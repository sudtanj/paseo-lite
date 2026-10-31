//! The built-in `paseo-lite` MCP server: `POST /mcp` on the daemon's own
//! HTTP listener (server.rs), given to every Claude Code and Codex agent
//! (mcp.rs) so an agent can inspect and debug the daemon that runs it.
//!
//! Streamable HTTP, minimally: one JSON-RPC message (or a batch) per POST,
//! answered with plain `application/json` (never an event stream); no
//! sessions, no server-initiated messages. Only this machine's peers with the
//! per-start bearer token get in.
//!
//! Tools that read agent state go through the manager actor (`Cmd::Debug`),
//! so they see a consistent snapshot and never lock anything; `/proc` scans
//! run on the blocking pool so the single-threaded runtime stays free.

use crate::manager::{Agent, Cmd, Manager};
use crate::mcp::DebugMcp;
use crate::session::Ctx;
use crate::store::Row;
use crate::util::truncate;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::{mpsc, oneshot};

/// Supported protocol revisions, newest first (the default).
const PROTOCOL_VERSIONS: [&str; 3] = ["2025-06-18", "2025-03-26", "2024-11-05"];

const INSTRUCTIONS: &str = "Inspect the paseo-lite daemon that runs this agent: its status, capacity and memory (daemon_status), agents and their queues (list_agents, agent_timeline), the claude/codex processes (agent_processes), its recent log (daemon_logs) and projects (list_projects). `rpc` sends one app-protocol request as the Paseo app would and can change state.";

/// How long a manager-backed tool waits for the manager.
const MANAGER_WAIT: Duration = Duration::from_secs(15);
/// `rpc`: wait for the first reply, then this long for each further one.
const RPC_FIRST_WAIT: Duration = Duration::from_secs(20);
const RPC_MORE_WAIT: Duration = Duration::from_millis(300);
const RPC_MAX_MESSAGES: usize = 50;
/// `rpc`: replies larger than this have their long strings cut.
const RPC_MAX_BYTES: usize = 200_000;
/// Long text fields in timeline items and replies are cut to this.
const TEXT_MAX: usize = 2000;

/// Answer one POST to `/mcp`.
pub async fn handle(ctx: &Arc<Ctx>, mcp: &DebugMcp, peer: SocketAddr, headers: &HeaderMap, body: &[u8]) -> Response {
    if let Err(resp) = check_access(mcp, peer, headers) {
        return resp;
    }
    let msg: Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => return json_response(StatusCode::BAD_REQUEST, &rpc_error(Value::Null, -32700, &format!("Parse error: {e}"))),
    };
    match msg {
        Value::Array(batch) if batch.is_empty() => json_response(StatusCode::BAD_REQUEST, &rpc_error(Value::Null, -32600, "Invalid Request: empty batch")),
        Value::Array(batch) => {
            let mut replies = Vec::new();
            for m in batch {
                if let Some(r) = dispatch(ctx, m).await {
                    replies.push(r);
                }
            }
            if replies.is_empty() {
                StatusCode::ACCEPTED.into_response()
            } else {
                json_response(StatusCode::OK, &Value::Array(replies))
            }
        }
        m => match dispatch(ctx, m).await {
            Some(r) => json_response(StatusCode::OK, &r),
            None => StatusCode::ACCEPTED.into_response(),
        },
    }
}

/// Loopback (or the daemon's own address) and the bearer token.
fn check_access(mcp: &DebugMcp, peer: SocketAddr, headers: &HeaderMap) -> Result<(), Response> {
    if !mcp.peer_allowed(peer.ip()) {
        return Err((StatusCode::FORBIDDEN, "MCP is only served to this machine").into_response());
    }
    let auth = headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok());
    if !mcp.authorized(auth) {
        return Err(json_response(StatusCode::UNAUTHORIZED, &json!({ "error": "missing or invalid bearer token" })));
    }
    Ok(())
}

fn json_response(status: StatusCode, body: &Value) -> Response {
    (status, [(header::CONTENT_TYPE, "application/json")], body.to_string()).into_response()
}

fn rpc_error(id: Value, code: i64, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}

/// One JSON-RPC message. None for notifications and client responses (202).
pub async fn dispatch(ctx: &Arc<Ctx>, msg: Value) -> Option<Value> {
    let Some(obj) = msg.as_object() else { return Some(rpc_error(Value::Null, -32600, "Invalid Request")) };
    let id = obj.get("id").cloned();
    let Some(method) = obj.get("method").and_then(Value::as_str) else {
        // A response to a server request (this server sends none): ignore it.
        if id.is_some() && (obj.contains_key("result") || obj.contains_key("error")) {
            return None;
        }
        return Some(rpc_error(id.unwrap_or(Value::Null), -32600, "Invalid Request"));
    };
    let id = match id {
        Some(id) if !method.starts_with("notifications/") => id,
        _ => return None,
    };
    let params = obj.get("params").cloned().unwrap_or_else(|| json!({}));
    let result = match method {
        "initialize" => Ok(initialize_result(&params)),
        "ping" => Ok(json!({})),
        "tools/list" => Ok(json!({ "tools": tools() })),
        "tools/call" => call_tool(ctx, &params).await,
        _ => Err((-32601, format!("Method not found: {method}"))),
    };
    Some(match result {
        Ok(result) => json!({ "jsonrpc": "2.0", "id": id, "result": result }),
        Err((code, message)) => rpc_error(id, code, &message),
    })
}

fn initialize_result(params: &Value) -> Value {
    let requested = params.get("protocolVersion").and_then(Value::as_str).unwrap_or("");
    let version = if PROTOCOL_VERSIONS.contains(&requested) { requested } else { PROTOCOL_VERSIONS[0] };
    json!({
        "protocolVersion": version,
        "capabilities": { "tools": {} },
        "serverInfo": { "name": crate::mcp::DEBUG_NAME, "version": env!("CARGO_PKG_VERSION") },
        "instructions": INSTRUCTIONS,
    })
}

fn tool(name: &str, description: &str, properties: Value, required: &[&str], read_only: bool) -> Value {
    json!({
        "name": name,
        "description": description,
        "inputSchema": { "type": "object", "properties": properties, "required": required },
        "annotations": { "readOnlyHint": read_only, "destructiveHint": !read_only, "openWorldHint": false },
    })
}

fn tools() -> Vec<Value> {
    vec![
        tool("daemon_status", "paseo-lite daemon status: version, pid, uptime, config (secrets redacted), turn capacity (limit, running, queued, ceiling and what bounds it, last grow/shrink decision), cgroup memory/swap/pids, memory and CPU pressure (PSI), load average, daemon RSS.", json!({}), &[], true),
        tool("list_agents", "All agents the daemon knows (most recently updated first): id, provider, title, cwd, status, active turn, whether it holds a CLI slot or waits in the queue, queued follow-up messages, retry/usage-limit state, last activity and error, pending permission requests.", json!({ "includeArchived": { "type": "boolean", "description": "Include archived agents (default false)." } }), &[], true),
        tool("agent_timeline", "Recent timeline rows of one agent (user messages, assistant text, reasoning, tool calls, notifications), oldest first, long text cut to ~2000 chars.", json!({ "agentId": { "type": "string", "description": "Agent id, a unique id prefix (4+ chars) or the exact title." }, "limit": { "type": "integer", "description": "Rows from the end (default 50, max 500).", "minimum": 1, "maximum": 500 } }), &["agentId"], true),
        tool("agent_processes", "Provider child processes: each live `claude` process (agent id, pid, busy, holding background sub-agents, idle time, RSS/swap from /proc and its descendant processes), prompts waiting for a Claude slot, and the shared `codex app-server` (pid, active turns, memory), plus the daemon's own memory.", json!({}), &[], true),
        tool("daemon_logs", "Recent daemon log lines (info and warnings since start; the last 2000 are kept, long lines cut), oldest first.", json!({ "lines": { "type": "integer", "description": "How many lines from the end (default 200, max 2000).", "minimum": 1, "maximum": 2000 }, "filter": { "type": "string", "description": "Only lines containing this text (case-insensitive)." } }), &[], true),
        tool("list_projects", "Projects (registered directories) and workspaces as the daemon knows them, with agent counts, plus agent directories that belong to no project.", json!({}), &[], true),
        tool("rpc", "Escape hatch: send one Paseo app-protocol session message (the inner `message` of a `session` frame, e.g. {\"type\":\"fetch_agents_request\"} or {\"type\":\"daemon.get_status.request\"}) through the same handler the app's websocket uses, and return the reply messages it produces. A requestId is added when missing. CAN MUTATE STATE (create/archive agents, send prompts, change config): prefer the read-only tools.", json!({ "message": { "type": "object", "description": "The session message, with at least `type`." } }), &["message"], false),
    ]
}

/// `tools/call`. Unknown tools are a protocol error; failures inside a tool
/// come back as a result with `isError`.
async fn call_tool(ctx: &Arc<Ctx>, params: &Value) -> Result<Value, (i64, String)> {
    let name = params.get("name").and_then(Value::as_str).unwrap_or("");
    let args = params.get("arguments").filter(|a| a.is_object()).cloned().unwrap_or_else(|| json!({}));
    let outcome = match name {
        "daemon_logs" => Ok(daemon_logs(&args)),
        "rpc" => rpc_tool(ctx, &args).await,
        "agent_processes" => match manager_tool(ctx, name, args).await {
            Ok(v) => tokio::task::spawn_blocking(move || add_proc_info(v)).await.map_err(|e| format!("reading /proc failed: {e}")),
            Err(e) => Err(e),
        },
        "daemon_status" | "list_agents" | "agent_timeline" | "list_projects" => manager_tool(ctx, name, args).await,
        _ => return Err((-32602, format!("Unknown tool: {name}"))),
    };
    Ok(tool_result(outcome))
}

fn tool_result(outcome: Result<Value, String>) -> Value {
    let (value, is_error) = match outcome {
        Ok(v) => (v, false),
        Err(e) => (json!({ "error": e }), true),
    };
    let text = serde_json::to_string_pretty(&value).unwrap_or_default();
    json!({ "content": [{ "type": "text", "text": text }], "isError": is_error })
}

async fn manager_tool(ctx: &Arc<Ctx>, name: &str, args: Value) -> Result<Value, String> {
    let (tx, rx) = oneshot::channel();
    ctx.cmds.send(Cmd::Debug { tool: name.to_string(), args, reply: tx }).map_err(|_| "the daemon is shutting down".to_string())?;
    match tokio::time::timeout(MANAGER_WAIT, rx).await {
        Ok(Ok(result)) => result,
        Ok(Err(_)) => Err("the manager dropped the request".into()),
        Err(_) => Err(format!("the manager did not answer within {}s (stuck or very busy)", MANAGER_WAIT.as_secs())),
    }
}

fn daemon_logs(args: &Value) -> Value {
    let lines = args.get("lines").and_then(Value::as_u64).unwrap_or(200).clamp(1, 2000) as usize;
    let filter = args.get("filter").and_then(Value::as_str);
    let logs = crate::util::recent_logs(lines, filter);
    json!({ "returned": logs.len(), "lines": logs })
}

/// The `rpc` tool: one session message through `Manager::handle_rpc`, with a
/// throwaway reply channel in place of a socket session.
async fn rpc_tool(ctx: &Arc<Ctx>, args: &Value) -> Result<Value, String> {
    let mut msg = args.get("message").cloned().unwrap_or(Value::Null);
    // Accept the whole `session` envelope too.
    if msg.get("type").and_then(Value::as_str) == Some("session") {
        msg = msg.get_mut("message").map(Value::take).unwrap_or(Value::Null);
    }
    let Some(kind) = msg.get("type").and_then(Value::as_str).map(str::to_string) else {
        return Err("`message` must be a session message object with a `type`, e.g. {\"type\":\"fetch_agents_request\"}".into());
    };
    if kind == "file.upload.request" {
        return Err("uploads need a socket session and are not available here".into());
    }
    if msg.get("requestId").map(Value::is_null).unwrap_or(true) {
        msg["requestId"] = json!(format!("mcp-{}", crate::util::new_id()));
    }
    crate::log!("mcp: rpc {kind}");
    let (out, mut rx) = mpsc::unbounded_channel::<Arc<str>>();
    ctx.cmds.send(Cmd::Rpc { msg, out }).map_err(|_| "the daemon is shutting down".to_string())?;
    let mut responses = Vec::new();
    let mut wait = RPC_FIRST_WAIT;
    while responses.len() < RPC_MAX_MESSAGES {
        match tokio::time::timeout(wait, rx.recv()).await {
            Ok(Some(frame)) => responses.push(unwrap_frame(&frame)),
            _ => break,
        }
        wait = RPC_MORE_WAIT;
    }
    let note = responses.is_empty().then(|| format!("no reply within {}s (some requests answer only through broadcasts, or not at all)", RPC_FIRST_WAIT.as_secs()));
    Ok(json!({ "requestType": kind, "responses": responses, "note": note }))
}

/// A reply frame as JSON, without the `session` envelope; huge ones get
/// their long strings cut.
fn unwrap_frame(frame: &str) -> Value {
    let Ok(mut v) = serde_json::from_str::<Value>(frame) else { return json!(truncate(frame, TEXT_MAX)) };
    if v.get("type").and_then(Value::as_str) == Some("session") {
        v = v.get_mut("message").map(Value::take).unwrap_or(Value::Null);
    }
    if frame.len() > RPC_MAX_BYTES {
        return json!({ "truncated": true, "bytes": frame.len(), "message": cut_strings(&v, TEXT_MAX) });
    }
    v
}

/// A copy of `v` with every string longer than `max` bytes cut.
fn cut_strings(v: &Value, max: usize) -> Value {
    match v {
        Value::String(s) if s.len() > max => Value::String(truncate(s, max)),
        Value::Array(items) => Value::Array(items.iter().map(|i| cut_strings(i, max)).collect()),
        Value::Object(map) => Value::Object(map.iter().map(|(k, i)| (k.clone(), cut_strings(i, max))).collect()),
        other => other.clone(),
    }
}

// ---- manager-side tools (run inside the manager actor) ----

impl Manager {
    pub fn debug_tool(&self, tool: &str, args: &Value) -> Result<Value, String> {
        match tool {
            "daemon_status" => Ok(self.debug_status()),
            "list_agents" => Ok(self.debug_agents(args)),
            "agent_timeline" => self.debug_timeline(args),
            "agent_processes" => Ok(self.debug_processes()),
            "list_projects" => Ok(self.debug_projects()),
            _ => Err(format!("unknown tool {tool}")),
        }
    }

    fn debug_status(&self) -> Value {
        let uptime = humantime::parse_rfc3339(&self.started_at).ok().and_then(|t| SystemTime::now().duration_since(t).ok()).map(|d| d.as_secs());
        let sent = crate::egress::snapshot();
        let egress: Vec<Value> = sent.iter().take(8).map(|(kind, n, bytes)| json!({ "type": kind, "messages": n, "bytes": bytes })).collect();
        json!({
            "version": env!("CARGO_PKG_VERSION"),
            "pid": std::process::id(),
            "serverId": self.server_id,
            "startedAt": self.started_at,
            "uptimeSecs": uptime,
            "daemonRssKib": crate::rpc_ext::rss_kib(),
            "config": redacted_config(&self.cfg),
            "runtime": self.runtime_status(),
            "capacity": self.capacity.debug_status(),
            "machine": machine_status(),
            "connectedClients": self.clients.iter().filter(|c| !c.out.is_closed()).count(),
            "sentToApps": egress,
        })
    }

    fn debug_agents(&self, args: &Value) -> Value {
        let include_archived = args.get("includeArchived").and_then(Value::as_bool).unwrap_or(false);
        let mut list: Vec<&Agent> = self.agents.values().filter(|a| include_archived || a.rec.archived_at.is_none()).collect();
        list.sort_by(|a, b| b.rec.updated_at.cmp(&a.rec.updated_at));
        let now = Instant::now();
        let agents: Vec<Value> = list
            .iter()
            .map(|a| {
                let r = &a.rec;
                let permissions: Vec<Value> = a.permissions.iter().map(|p| cut_strings(&p.request, 500)).collect();
                json!({
                    "id": r.id,
                    "provider": r.provider,
                    "title": r.title,
                    "cwd": r.cwd,
                    "workspaceId": r.workspace_id,
                    "model": r.model,
                    "modeId": r.mode_id,
                    "thinking": r.thinking_option_id,
                    "status": a.status,
                    "activeTurn": a.active_turn.as_ref().map(|(id, at)| json!({ "turnId": id, "startedAt": at })),
                    "holdsCliSlot": self.running.contains(&r.id),
                    "cliQueuePosition": self.cli_queue.iter().position(|(id, _)| *id == r.id),
                    "waitingForClaudeSlot": r.provider != "codex" && self.claude.is_waiting(&r.id),
                    "queuedMessages": a.queue.len(),
                    "compacting": a.compacting,
                    "attempts": a.attempts,
                    "retryInSecs": a.retry_at.map(|t| t.saturating_duration_since(now).as_secs()),
                    "limitWait": a.limit_wait,
                    "secsSinceProviderEvent": a.last_event.elapsed().as_secs(),
                    "createdAt": r.created_at,
                    "updatedAt": r.updated_at,
                    "lastUserMessageAt": r.last_user_message_at,
                    "sessionId": r.session_id,
                    "archivedAt": r.archived_at,
                    "attentionReason": r.attention_reason,
                    "lastError": r.last_error,
                    "pendingPermissions": permissions,
                    "timelineInMemory": a.timeline.is_some(),
                })
            })
            .collect();
        json!({ "count": agents.len(), "agents": agents })
    }

    fn debug_timeline(&self, args: &Value) -> Result<Value, String> {
        let key = args.get("agentId").and_then(Value::as_str).filter(|k| !k.is_empty()).ok_or("agentId is required")?;
        let id = self.resolve(key).ok_or_else(|| format!("agent {key} not found"))?;
        let a = self.agents.get(&id).ok_or_else(|| format!("agent {key} not found"))?;
        let limit = args.get("limit").and_then(Value::as_u64).unwrap_or(50).clamp(1, 500) as usize;
        // An unloaded timeline is read for this call only, not kept in memory.
        let loaded: Vec<Row>;
        let rows: &[Row] = match &a.timeline {
            Some(rows) => rows,
            None => {
                loaded = self.store.load_timeline(&id);
                &loaded
            }
        };
        let start = rows.len().saturating_sub(limit);
        let items: Vec<Value> = rows[start..]
            .iter()
            .map(|r| json!({ "seqStart": r.seq_start, "seqEnd": r.seq_end, "timestamp": r.timestamp, "turnId": r.turn_id, "item": cut_strings(&r.item, TEXT_MAX) }))
            .collect();
        Ok(json!({ "agentId": id, "title": a.rec.title, "status": a.status, "totalRows": rows.len(), "returned": items.len(), "items": items }))
    }

    fn debug_processes(&self) -> Value {
        let running: Vec<&String> = self.running.iter().collect();
        let queue: Vec<&String> = self.cli_queue.iter().map(|(id, _)| id).collect();
        json!({
            "activeCli": self.active_cli,
            "turnsHoldingSlots": running,
            "cliQueue": queue,
            "claude": self.claude.debug_status(),
            "codex": self.codex.debug_status(),
        })
    }

    /// Unarchived agents matching `pred`.
    fn count_live(&self, pred: impl Fn(&Agent) -> bool) -> usize {
        self.agents.values().filter(|a| a.rec.archived_at.is_none() && pred(*a)).count()
    }

    fn debug_projects(&self) -> Value {
        let projects: Vec<Value> = self
            .projects
            .iter()
            .map(|p| {
                json!({
                    "rootPath": p.root_path,
                    "customName": p.custom_name,
                    "createdAt": p.created_at,
                    "owned": p.owned,
                    "exists": std::path::Path::new(&p.root_path).is_dir(),
                    "agents": self.count_live(|a| under(a.rec.cwd.as_str(), p.root_path.as_str())),
                })
            })
            .collect();
        let workspaces: Vec<Value> = self
            .workspaces
            .iter()
            .map(|w| {
                json!({
                    "id": w.id,
                    "projectRoot": w.project_root,
                    "directory": w.directory,
                    "title": w.title,
                    "pinnedAt": w.pinned_at,
                    "createdAt": w.created_at,
                    "archivedAt": w.archived_at,
                    "agents": self.count_live(|a| a.rec.workspace_id.as_deref() == Some(w.id.as_str())),
                    "runningAgents": self.count_live(|a| a.status == "running" && a.rec.workspace_id.as_deref() == Some(w.id.as_str())),
                })
            })
            .collect();
        let mut orphans: Vec<&str> = self.agents.values().map(|a| a.rec.cwd.as_str()).filter(|cwd| !self.projects.iter().any(|p| under(cwd, p.root_path.as_str()))).collect();
        orphans.sort_unstable();
        orphans.dedup();
        json!({ "projects": projects, "workspaces": workspaces, "agentDirsWithoutProject": orphans })
    }
}

/// `dir` is `root` or inside it.
fn under(dir: &str, root: &str) -> bool {
    dir == root || dir.strip_prefix(root).map(|rest| rest.starts_with('/')).unwrap_or(false)
}

/// Config without secrets (password, API keys, the MCP token).
fn redacted_config(cfg: &crate::config::Config) -> Value {
    let secs = |d: Duration| d.as_secs();
    json!({
        "home": cfg.home.display().to_string(),
        "listen": cfg.listen,
        "hostnames": cfg.hostnames,
        "passwordSet": cfg.password.is_some(),
        "relay": { "enabled": cfg.relay_enabled, "endpoint": cfg.relay_endpoint, "useTls": cfg.relay_use_tls, "publicEndpoint": cfg.relay_public_endpoint, "publicUseTls": cfg.relay_public_use_tls },
        "claude": {
            "bin": cfg.claude_bin,
            "maxLive": cfg.claude_max_live.label(),
            "keepWarm": cfg.claude_keep_warm,
            "idleSecs": secs(cfg.claude_idle),
            "maxSubagents": cfg.claude_max_subagents,
            "contextWindow": cfg.claude_context_window,
            "subagentPromptSet": cfg.claude_subagent_prompt.is_some(),
        },
        "codex": { "bin": cfg.codex_bin, "maxActive": cfg.codex_max_active.label(), "keepWarm": cfg.codex_keep_warm, "idleSecs": secs(cfg.codex_idle) },
        "capacity": { "agentMb": cfg.agent_mb, "autoMaxAgents": cfg.auto_max_agents, "autoStableSecs": secs(cfg.auto_stable) },
        "agentPriority": { "nice": cfg.agent_nice, "oomScoreAdj": cfg.agent_oom_score_adj, "lowIo": cfg.agent_low_io },
        "turns": {
            "autoCompactPct": cfg.auto_compact_pct,
            "autoCompactTokens": cfg.auto_compact_tokens,
            "retryMax": cfg.retry_max,
            "retryBaseSecs": secs(cfg.retry_base),
            "stallTimeoutSecs": secs(cfg.stall_timeout),
            "resumeAfterLimit": cfg.resume_after_limit,
            "limitPollSecs": secs(cfg.limit_poll),
        },
        "deleteProjectFiles": format!("{:?}", cfg.delete_project_files),
        "timelineUnloadSecs": secs(cfg.timeline_unload),
        "defaultSystemPromptSet": cfg.default_system_prompt.is_some(),
        "mcp": {
            "zilongRouterUrl": cfg.zilong_mcp_url.as_deref().map(redact_url),
            "zilongApiKeySet": cfg.zilong_api_key.is_some(),
            "debugServerUrl": cfg.debug_mcp.as_ref().map(|d| d.url.clone()),
        },
    })
}

/// A URL without credentials, query or fragment.
fn redact_url(url: &str) -> String {
    let url = url.split(|c: char| c == '?' || c == '#').next().unwrap_or("");
    let Some((scheme, rest)) = url.split_once("://") else { return url.to_string() };
    let (authority, path) = match rest.find('/') {
        Some(i) => rest.split_at(i),
        None => (rest, ""),
    };
    let host = authority.rsplit('@').next().unwrap_or(authority);
    format!("{scheme}://{host}{path}")
}

fn read_trimmed(path: &str) -> Option<String> {
    std::fs::read_to_string(path).ok().map(|s| s.trim().to_string())
}

/// The daemon's cgroup (v2) and the host's memory, pressure and load. Small
/// files only.
fn machine_status() -> Value {
    // `0::/path` in /proc/self/cgroup; `/` inside a container's namespace.
    let own = std::fs::read_to_string("/proc/self/cgroup").ok().and_then(|t| t.lines().find_map(|l| l.strip_prefix("0::").map(|p| p.trim().trim_end_matches('/').to_string())));
    let dir = own.map(|p| format!("/sys/fs/cgroup{p}")).filter(|d| std::path::Path::new(&format!("{d}/memory.current")).exists()).unwrap_or_else(|| "/sys/fs/cgroup".to_string());
    let cg = |name: &str| read_trimmed(&format!("{dir}/{name}"));
    let mut cgroup = json!({
        "path": dir,
        "memory.current": cg("memory.current"),
        "memory.max": cg("memory.max"),
        "memory.high": cg("memory.high"),
        "memory.swap.current": cg("memory.swap.current"),
        "memory.swap.max": cg("memory.swap.max"),
        "memory.pressure": cg("memory.pressure"),
        "cpu.pressure": cg("cpu.pressure"),
        "cpu.max": cg("cpu.max"),
        "pids.current": cg("pids.current"),
        "pids.max": cg("pids.max"),
    });
    if dir != "/sys/fs/cgroup" {
        cgroup["root"] = json!({ "memory.pressure": read_trimmed("/sys/fs/cgroup/memory.pressure"), "cpu.pressure": read_trimmed("/sys/fs/cgroup/cpu.pressure") });
    }
    let meminfo: HashMap<String, u64> = std::fs::read_to_string("/proc/meminfo")
        .unwrap_or_default()
        .lines()
        .filter_map(|l| {
            let (k, v) = l.split_once(':')?;
            if !["MemTotal", "MemAvailable", "Cached", "SwapTotal", "SwapFree", "SwapCached"].contains(&k) {
                return None;
            }
            let kib = v.split_whitespace().next()?.parse::<u64>().ok()?;
            Some((format!("{k}Kib"), kib))
        })
        .collect();
    json!({
        "cgroup": cgroup,
        "meminfo": meminfo,
        "hostPressure": { "memory": read_trimmed("/proc/pressure/memory"), "cpu": read_trimmed("/proc/pressure/cpu") },
        "loadavg": read_trimmed("/proc/loadavg"),
    })
}

// ---- /proc (on the blocking pool) ----

/// Memory and state of a process from `/proc/<pid>/status`.
fn proc_status(pid: u32) -> Value {
    let Ok(text) = std::fs::read_to_string(format!("/proc/{pid}/status")) else { return json!({ "gone": true }) };
    let field = |key: &str| text.lines().find_map(|l| l.strip_prefix(key)?.strip_prefix(':').map(str::trim));
    let kib = |key: &str| field(key).and_then(|v| v.split_whitespace().next()?.parse::<u64>().ok());
    json!({
        "name": field("Name"),
        "state": field("State"),
        "threads": field("Threads").and_then(|v| v.parse::<u64>().ok()),
        "rssKib": kib("VmRSS"),
        "swapKib": kib("VmSwap"),
        "peakRssKib": kib("VmHWM"),
    })
}

/// Parent of every process, from `/proc/<pid>/stat` (the command name may
/// hold spaces and parentheses, so parse after the last `)`).
fn parent_map() -> HashMap<u32, Vec<(u32, String)>> {
    let mut children: HashMap<u32, Vec<(u32, String)>> = HashMap::new();
    let Ok(dir) = std::fs::read_dir("/proc") else { return children };
    for entry in dir.flatten() {
        let Some(pid) = entry.file_name().to_str().and_then(|n| n.parse::<u32>().ok()) else { continue };
        let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else { continue };
        let (Some(open), Some(close)) = (stat.find('('), stat.rfind(')')) else { continue };
        let comm = stat.get(open + 1..close).unwrap_or("").to_string();
        let Some(ppid) = stat.get(close + 1..).and_then(|rest| rest.split_whitespace().nth(1)).and_then(|p| p.parse::<u32>().ok()) else { continue };
        children.entry(ppid).or_default().push((pid, comm));
    }
    children
}

/// Every process below `root` (at most 100).
fn descendants(root: u32, children: &HashMap<u32, Vec<(u32, String)>>) -> Vec<Value> {
    let mut out = Vec::new();
    let mut stack = vec![root];
    while let Some(pid) = stack.pop() {
        for (child, comm) in children.get(&pid).map(Vec::as_slice).unwrap_or(&[]) {
            if out.len() >= 100 {
                return out;
            }
            let status = proc_status(*child);
            out.push(json!({ "pid": child, "parent": pid, "command": comm, "rssKib": status["rssKib"], "swapKib": status["swapKib"] }));
            stack.push(*child);
        }
    }
    out
}

/// Adds `/proc` details to the `agent_processes` snapshot.
fn add_proc_info(mut v: Value) -> Value {
    let children = parent_map();
    let mut enrich = |p: &mut Value| {
        if let Some(pid) = p.get("pid").and_then(Value::as_u64).map(|p| p as u32) {
            p["proc"] = proc_status(pid);
            p["descendants"] = json!(descendants(pid, &children));
        }
    };
    if let Some(procs) = v.pointer_mut("/claude/processes").and_then(Value::as_array_mut) {
        procs.iter_mut().for_each(&mut enrich);
    }
    if let Some(codex) = v.get_mut("codex") {
        enrich(codex);
    }
    v["daemon"] = json!({ "pid": std::process::id(), "proc": proc_status(std::process::id()) });
    v
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;
    use tokio::sync::broadcast;

    fn ctx() -> (Arc<Ctx>, mpsc::UnboundedReceiver<Cmd>) {
        let (cmds, rx) = mpsc::unbounded_channel();
        let (bcast, _) = broadcast::channel(4);
        (Arc::new(Ctx { cmds, bcast, server_id: "s".into(), hostname: "h".into(), password: None, home: std::env::temp_dir() }), rx)
    }

    fn server() -> DebugMcp {
        DebugMcp::for_listen("0.0.0.0:6768").unwrap()
    }

    fn bearer(token: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(header::AUTHORIZATION, HeaderValue::from_str(&format!("Bearer {token}")).unwrap());
        h
    }

    fn local() -> SocketAddr {
        "127.0.0.1:50000".parse().unwrap()
    }

    async fn body_json(resp: Response) -> Value {
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn rejects_missing_or_wrong_token_and_remote_peers() {
        let (ctx, _rx) = ctx();
        let mcp = server();
        let ping = br#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#;
        assert_eq!(handle(&ctx, &mcp, local(), &HeaderMap::new(), ping).await.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(handle(&ctx, &mcp, local(), &bearer("nope"), ping).await.status(), StatusCode::UNAUTHORIZED);
        let remote: SocketAddr = "192.0.2.7:50000".parse().unwrap();
        assert_eq!(handle(&ctx, &mcp, remote, &bearer(&mcp.token), ping).await.status(), StatusCode::FORBIDDEN);
        let ok = handle(&ctx, &mcp, local(), &bearer(&mcp.token), ping).await;
        assert_eq!(ok.status(), StatusCode::OK);
        assert_eq!(ok.headers()[header::CONTENT_TYPE], "application/json");
        assert_eq!(body_json(ok).await, json!({ "jsonrpc": "2.0", "id": 1, "result": {} }));
    }

    #[tokio::test]
    async fn notifications_are_accepted_and_bad_json_is_a_parse_error() {
        let (ctx, _rx) = ctx();
        let mcp = server();
        let note = br#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#;
        let resp = handle(&ctx, &mcp, local(), &bearer(&mcp.token), note).await;
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        let resp = handle(&ctx, &mcp, local(), &bearer(&mcp.token), b"{nope").await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert_eq!(body_json(resp).await["error"]["code"], -32700);
        let batch = br#"[{"jsonrpc":"2.0","id":"a","method":"ping"},{"jsonrpc":"2.0","method":"notifications/cancelled"}]"#;
        let resp = handle(&ctx, &mcp, local(), &bearer(&mcp.token), batch).await;
        assert_eq!(body_json(resp).await, json!([{ "jsonrpc": "2.0", "id": "a", "result": {} }]));
    }

    #[tokio::test]
    async fn initialize_negotiates_the_protocol_version() {
        let (ctx, _rx) = ctx();
        let init = |v: &str| json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize", "params": { "protocolVersion": v, "capabilities": {}, "clientInfo": { "name": "t", "version": "1" } } });
        let r = dispatch(&ctx, init("2025-03-26")).await.unwrap();
        assert_eq!(r["result"]["protocolVersion"], "2025-03-26");
        assert_eq!(r["result"]["serverInfo"]["name"], "paseo-lite");
        assert_eq!(r["result"]["serverInfo"]["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(r["result"]["capabilities"], json!({ "tools": {} }));
        let r = dispatch(&ctx, init("1999-01-01")).await.unwrap();
        assert_eq!(r["result"]["protocolVersion"], "2025-06-18");
    }

    #[tokio::test]
    async fn lists_tools_and_rejects_unknown_methods_and_tools() {
        let (ctx, _rx) = ctx();
        let r = dispatch(&ctx, json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/list" })).await.unwrap();
        let names: Vec<&str> = r["result"]["tools"].as_array().unwrap().iter().map(|t| t["name"].as_str().unwrap()).collect();
        assert_eq!(names, ["daemon_status", "list_agents", "agent_timeline", "agent_processes", "daemon_logs", "list_projects", "rpc"]);
        for t in r["result"]["tools"].as_array().unwrap() {
            assert_eq!(t["inputSchema"]["type"], "object");
        }
        let r = dispatch(&ctx, json!({ "jsonrpc": "2.0", "id": 3, "method": "resources/list" })).await.unwrap();
        assert_eq!(r["error"]["code"], -32601);
        let r = dispatch(&ctx, json!({ "jsonrpc": "2.0", "id": 4, "method": "tools/call", "params": { "name": "nope" } })).await.unwrap();
        assert_eq!(r["error"]["code"], -32602);
        assert!(dispatch(&ctx, json!({ "jsonrpc": "2.0", "id": 5, "result": {} })).await.is_none());
        assert_eq!(dispatch(&ctx, json!([1])).await.unwrap()["error"]["code"], -32600);
    }

    #[tokio::test]
    async fn tool_calls_return_text_content() {
        let (ctx, mut rx) = ctx();
        crate::util::emit_log("", "mcp-server-test log line".to_string());
        let r = dispatch(&ctx, json!({ "jsonrpc": "2.0", "id": 6, "method": "tools/call", "params": { "name": "daemon_logs", "arguments": { "filter": "mcp-server-test" } } })).await.unwrap();
        assert_eq!(r["result"]["isError"], false);
        let text: Value = serde_json::from_str(r["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
        assert_eq!(text["returned"], 1);
        // Manager-backed tools go through Cmd::Debug.
        tokio::spawn(async move {
            if let Some(Cmd::Debug { tool, reply, .. }) = rx.recv().await {
                let _ = reply.send(if tool == "list_agents" { Ok(json!({ "count": 0 })) } else { Err("boom".into()) });
            }
        });
        let r = dispatch(&ctx, json!({ "jsonrpc": "2.0", "id": 7, "method": "tools/call", "params": { "name": "list_agents" } })).await.unwrap();
        assert_eq!(r["result"]["content"][0]["type"], "text");
        assert_eq!(r["result"]["isError"], false);
        assert!(r["result"]["content"][0]["text"].as_str().unwrap().contains("\"count\": 0"));
    }

    #[test]
    fn tool_errors_set_is_error() {
        let r = tool_result(Err("no such agent".into()));
        assert_eq!(r["isError"], true);
        assert!(r["content"][0]["text"].as_str().unwrap().contains("no such agent"));
    }

    #[test]
    fn urls_lose_credentials_and_queries() {
        assert_eq!(redact_url("http://user:pw@pi:20128/mcp?key=x#f"), "http://pi:20128/mcp");
        assert_eq!(redact_url("http://pi:20128"), "http://pi:20128");
    }

    #[test]
    fn long_strings_are_cut() {
        let v = cut_strings(&json!({ "a": ["x".repeat(3000)], "b": "short", "n": 1 }), 2000);
        assert_eq!(v["a"][0].as_str().unwrap().len(), 2000 + "…".len());
        assert_eq!(v["b"], "short");
        assert_eq!(v["n"], 1);
    }
}
