//! Upstream protocol requests beyond the core subset: agent controls
//! (rename/labels, thinking, refresh/resume, slash commands, wait-for-finish,
//! prompt list) and daemon/project management (status, config, diagnostics,
//! project rename/remove with folder cleanup).

use crate::manager::{reply, Agent, Manager, Out, Waiter, EPOCH};
use crate::providers::{binary_available, is_supported};
use crate::rpc::{normalize_dir, rpc_error, s};
use crate::snapshot::project_descriptor;
use crate::store::{AgentRecord, ProjectRecord};
use crate::util::{new_id, now_iso, truncate};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

/// `{ requestId, agentId, accepted, error }` responses (AgentActionResponsePayload).
fn action_reply(out: &Out, kind: &str, rid: &str, agent_id: &str, error: Option<&str>) {
    let response_type = kind.replace("_request", "_response");
    reply(out, json!({ "type": response_type, "payload": { "requestId": rid, "agentId": agent_id, "accepted": error.is_none(), "error": error } }));
}

fn non_default(v: Option<String>) -> Option<String> {
    v.map(|t| t.trim().to_string()).filter(|t| !t.is_empty() && t != "default")
}

impl Manager {
    /// Handle a request from the extended set. Returns false when `kind` is not one of them.
    pub fn handle_ext_rpc(&mut self, kind: &str, msg: &Value, rid: &str, out: &Out) -> bool {
        let agent_key = s(msg, "agentId").unwrap_or_default();
        match kind {
            "update_agent_request" => self.update_agent(msg, &agent_key, kind, rid, out),
            "set_agent_thinking_request" => self.set_thinking(msg, &agent_key, kind, rid, out),
            "set_agent_feature_request" => {
                let id = self.resolve(&agent_key).unwrap_or(agent_key);
                action_reply(out, kind, rid, &id, Some("paseo-lite has no configurable agent features"));
            }
            "refresh_agent_request" => self.refresh_agent(&agent_key, kind, rid, out),
            "resume_agent_request" => self.resume_agent(msg, kind, rid, out),
            "list_commands_request" => self.list_commands(&agent_key, rid, out),
            "list_provider_features_request" => {
                let provider = msg.get("draftConfig").and_then(|c| s(c, "provider")).unwrap_or_else(|| "claude".into());
                reply(out, json!({ "type": "list_provider_features_response", "payload": { "provider": provider, "features": [], "error": null, "fetchedAt": now_iso(), "requestId": rid } }));
            }
            "wait_for_finish_request" => self.wait_for_finish(msg, &agent_key, rid, out),
            "agent.timeline.list_prompts.request" => self.list_prompts(&agent_key, rid, out),
            "daemon.get_status.request" => self.daemon_status(rid, out),
            "usage.list_reports.request" => self.usage_reports(msg, rid, out),
            "provider.usage.list.request" => self.legacy_usage(rid, out),
            "get_daemon_config_request" => self.daemon_config(rid, out),
            "set_daemon_config_request" => self.set_daemon_config(msg, rid, out),
            "diagnostics.request" => {
                reply(out, json!({ "type": "diagnostics.response", "payload": { "requestId": rid, "diagnostic": self.diagnostics() } }));
            }
            "project.rename.request" => self.rename_project(msg, rid, out),
            "project.remove.request" => self.remove_project(msg, rid, out),
            _ => return false,
        }
        true
    }
}

// ---- agent controls ----

impl Manager {
    fn update_agent(&mut self, msg: &Value, agent_key: &str, kind: &str, rid: &str, out: &Out) {
        let Some(id) = self.resolve(agent_key) else { return action_reply(out, kind, rid, agent_key, Some("agent not found")) };
        if let Some(a) = self.agents.get_mut(&id) {
            if let Some(name) = s(msg, "name") {
                let name = name.trim();
                a.rec.title = (!name.is_empty()).then(|| truncate(name, 200));
            }
            if let Some(labels) = msg.get("labels").and_then(|l| l.as_object()) {
                // Merge; an empty value removes the label.
                for (k, v) in labels {
                    match v.as_str().filter(|v| !v.is_empty()) {
                        Some(v) => {
                            a.rec.labels.insert(k.clone(), v.to_string());
                        }
                        None => {
                            a.rec.labels.remove(k);
                        }
                    }
                }
            }
        }
        self.touch(&id);
        self.publish_agent(&id);
        action_reply(out, kind, rid, &id, None);
    }

    fn set_thinking(&mut self, msg: &Value, agent_key: &str, kind: &str, rid: &str, out: &Out) {
        let Some(id) = self.resolve(agent_key) else { return action_reply(out, kind, rid, agent_key, Some("agent not found")) };
        if let Some(a) = self.agents.get_mut(&id) {
            a.rec.thinking_option_id = non_default(s(msg, "thinkingOptionId"));
        }
        // `--effort` is a spawn flag; the next prompt resumes with the new level.
        self.claude.restart_on_next_prompt(&id);
        self.touch(&id);
        self.publish_agent(&id);
        action_reply(out, kind, rid, &id, None);
    }

    /// Reload the agent's timeline from disk and re-publish it.
    fn refresh_agent(&mut self, agent_key: &str, kind: &str, rid: &str, out: &Out) {
        let Some(id) = self.resolve(agent_key) else { return rpc_error(out, rid, kind, "agent not found") };
        let store = &self.store;
        let size = match self.agents.get_mut(&id) {
            Some(a) => {
                if a.dirty_rows.is_empty() && a.status != "running" {
                    a.timeline = None;
                }
                a.ensure_timeline(store);
                a.timeline.as_ref().map(|t| t.len()).unwrap_or(0)
            }
            None => 0,
        };
        self.publish_agent(&id);
        reply(out, json!({ "type": "status", "payload": { "status": "agent_refreshed", "agentId": id, "requestId": rid, "timelineSize": size } }));
    }

    /// Reattach to a provider session: reuse the agent that owns it, or
    /// create one bound to it (the CLI resumes the session on the next prompt).
    fn resume_agent(&mut self, msg: &Value, kind: &str, rid: &str, out: &Out) {
        let handle = msg.get("handle").cloned().unwrap_or(Value::Null);
        let overrides = msg.get("overrides").cloned().unwrap_or(Value::Null);
        let provider = s(&handle, "provider").unwrap_or_default();
        let Some(session_id) = s(&handle, "sessionId").filter(|x| !x.is_empty()) else { return rpc_error(out, rid, kind, "resume handle has no sessionId") };
        let existing = self.agents.values().find(|a| a.rec.provider == provider && a.rec.session_id.as_deref() == Some(session_id.as_str())).map(|a| a.rec.id.clone());
        let id = match existing {
            Some(id) => {
                if let Some(a) = self.agents.get_mut(&id) {
                    a.rec.archived_at = None;
                    if let Some(m) = non_default(s(&overrides, "model")) {
                        a.rec.model = Some(m);
                    }
                    if let Some(m) = s(&overrides, "modeId") {
                        a.rec.mode_id = Some(m);
                    }
                }
                id
            }
            None => {
                if !is_supported(&provider) {
                    return rpc_error(out, rid, kind, &format!("provider '{provider}' is not supported (claude, codex)"));
                }
                let raw_cwd = s(&overrides, "cwd").or_else(|| handle.get("metadata").and_then(|m| s(m, "cwd"))).unwrap_or_default();
                let cwd = match normalize_dir(&raw_cwd) {
                    Ok(c) => c,
                    Err(e) => return rpc_error(out, rid, kind, &format!("cannot resume without a valid cwd: {e}")),
                };
                let workspace_id = self.find_or_create_workspace(&cwd);
                let now = now_iso();
                let rec = AgentRecord {
                    id: new_id(),
                    provider,
                    cwd,
                    workspace_id: Some(workspace_id),
                    model: non_default(s(&overrides, "model")),
                    mode_id: s(&overrides, "modeId"),
                    title: s(&overrides, "title"),
                    created_at: now.clone(),
                    updated_at: now,
                    last_user_message_at: None,
                    session_id: Some(session_id),
                    archived_at: None,
                    last_error: None,
                    last_usage: None,
                    system_prompt: s(&overrides, "systemPrompt"),
                    labels: BTreeMap::new(),
                    next_seq: 0,
                    attention_reason: None,
                    attention_at: None,
                    thinking_option_id: non_default(s(&overrides, "thinkingOptionId")),
                    pending_turn: None,
                    pending_queue: Vec::new(),
                };
                let id = rec.id.clone();
                let mut agent = Agent::new(rec);
                agent.timeline = Some(Vec::new());
                self.agents.insert(id.clone(), agent);
                crate::log!("agent {id} resumed from {} session", self.agents[&id].rec.provider);
                id
            }
        };
        self.touch(&id);
        let store = &self.store;
        let size = self.agents.get_mut(&id).map(|a| {
            a.ensure_timeline(store);
            a.timeline.as_ref().map(|t| t.len()).unwrap_or(0)
        });
        let snapshot = self.agents[&id].snapshot();
        reply(out, json!({ "type": "status", "payload": { "status": "agent_resumed", "agent": snapshot, "agentId": id, "requestId": rid, "timelineSize": size.unwrap_or(0) } }));
        self.publish_agent(&id);
    }
}

// ---- slash commands ----

fn command(name: &str, description: &str, hint: &str, kind: &str) -> Value {
    json!({ "name": name, "description": description, "argumentHint": hint, "kind": kind })
}

/// `description:` / `argument-hint:` / `name:` from a Markdown file's YAML front matter.
fn front_matter(path: &std::path::Path) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let Ok(text) = std::fs::read_to_string(path) else { return out };
    let mut lines = text.lines();
    if lines.next().map(str::trim) != Some("---") {
        return out;
    }
    for line in lines {
        if line.trim() == "---" {
            break;
        }
        if let Some((k, v)) = line.split_once(':') {
            out.insert(k.trim().to_string(), v.trim().trim_matches(|c| c == '"' || c == '\'').to_string());
        }
    }
    out
}

/// Custom commands: `<dir>/**/*.md`, subdirectories joined with `:` like Claude Code.
fn scan_commands(dir: &std::path::Path, prefix: &str, depth: usize, found: &mut Vec<Value>) {
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    for entry in entries.flatten().take(500) {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        if path.is_dir() && depth < 3 {
            scan_commands(&path, &format!("{prefix}{name}:"), depth + 1, found);
        } else if let Some(stem) = name.strip_suffix(".md") {
            let meta = front_matter(&path);
            let desc = meta.get("description").cloned().unwrap_or_else(|| "Custom command".into());
            found.push(command(&format!("{prefix}{stem}"), &desc, meta.get("argument-hint").map(String::as_str).unwrap_or(""), "command"));
        }
    }
}

/// Skills: `<dir>/<name>/SKILL.md`.
fn scan_skills(dir: &std::path::Path, found: &mut Vec<Value>) {
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    for entry in entries.flatten().take(500) {
        let skill = entry.path().join("SKILL.md");
        if !skill.is_file() {
            continue;
        }
        let meta = front_matter(&skill);
        let name = meta.get("name").cloned().unwrap_or_else(|| entry.file_name().to_string_lossy().into_owned());
        found.push(command(&name, meta.get("description").map(String::as_str).unwrap_or("Skill"), "", "skill"));
    }
}

fn claude_commands(cwd: &str) -> Vec<Value> {
    let mut found = vec![
        command("compact", "Summarize the conversation to free up context", "[instructions]", "command"),
        command("context", "Show context window usage", "", "command"),
        command("init", "Create a CLAUDE.md with codebase documentation", "", "command"),
    ];
    let home = std::env::var("HOME").unwrap_or_default();
    // Project entries first so they win over user-level ones with the same name.
    for base in [std::path::PathBuf::from(cwd), std::path::PathBuf::from(&home)] {
        scan_commands(&base.join(".claude/commands"), "", 0, &mut found);
        scan_skills(&base.join(".claude/skills"), &mut found);
    }
    let mut seen = std::collections::HashSet::new();
    found.retain(|c| seen.insert(c["name"].as_str().unwrap_or("").to_string()));
    found
}

impl Manager {
    fn list_commands(&mut self, agent_key: &str, rid: &str, out: &Out) {
        let Some(a) = self.resolve(agent_key).and_then(|id| self.agents.get(&id)) else {
            return reply(out, json!({ "type": "list_commands_response", "payload": { "agentId": agent_key, "commands": [], "error": "agent not found", "requestId": rid } }));
        };
        let commands = if a.rec.provider == "codex" {
            vec![command("compact", "Summarize the thread to free up context", "", "command")]
        } else {
            claude_commands(&a.rec.cwd)
        };
        reply(out, json!({ "type": "list_commands_response", "payload": { "agentId": a.rec.id, "commands": commands, "error": null, "requestId": rid } }));
    }
}

// ---- wait for finish / prompts ----

impl Manager {
    fn wait_for_finish(&mut self, msg: &Value, agent_key: &str, rid: &str, out: &Out) {
        let Some(id) = self.resolve(agent_key) else {
            return reply(out, json!({ "type": "wait_for_finish_response", "payload": { "requestId": rid, "status": "error", "final": null, "error": "agent not found", "lastMessage": null } }));
        };
        let deadline = msg.get("timeoutMs").and_then(|v| v.as_u64()).map(|ms| Instant::now() + Duration::from_millis(ms));
        self.waiters.push(Waiter { agent_id: id, request_id: rid.to_string(), out: out.clone(), deadline });
        self.resolve_waiters();
    }

    /// Answer every waiter whose agent stopped, needs permission, or timed out.
    /// Cheap when nobody waits; the manager calls it after each event.
    pub fn resolve_waiters(&mut self) {
        if self.waiters.is_empty() {
            return;
        }
        let now = Instant::now();
        for w in std::mem::take(&mut self.waiters) {
            let state = match self.agents.get(&w.agent_id) {
                None => Some(("error", Some("agent not found".to_string()))),
                Some(a) if !a.permissions.is_empty() => Some(("permission", None)),
                Some(a) if a.status == "running" => None,
                Some(a) if a.status == "error" => Some(("error", a.rec.last_error.clone())),
                Some(_) => Some(("idle", None)),
            };
            let state = state.or_else(|| w.deadline.filter(|d| now >= *d).map(|_| ("timeout", None)));
            let Some((status, error)) = state else {
                self.waiters.push(w);
                continue;
            };
            let last = self.last_assistant_message(&w.agent_id);
            let snapshot = self.agents.get(&w.agent_id).map(|a| a.snapshot());
            reply(&w.out, json!({ "type": "wait_for_finish_response", "payload": { "requestId": w.request_id, "status": status, "final": snapshot, "error": error, "lastMessage": last } }));
        }
    }

    pub(crate) fn last_assistant_message(&mut self, agent_id: &str) -> Option<String> {
        let store = &self.store;
        let a = self.agents.get_mut(agent_id)?;
        a.ensure_timeline(store);
        a.timeline.as_ref()?.iter().rev().find(|r| r.item.get("type").and_then(|t| t.as_str()) == Some("assistant_message")).and_then(|r| s(&r.item, "text"))
    }

    fn list_prompts(&mut self, agent_key: &str, rid: &str, out: &Out) {
        let not_found = || json!({ "type": "agent.timeline.list_prompts.response", "payload": { "requestId": rid, "agentId": agent_key, "epoch": EPOCH, "prompts": [], "error": "agent not found" } });
        let Some(id) = self.resolve(agent_key) else { return reply(out, not_found()) };
        let store = &self.store;
        let Some(a) = self.agents.get_mut(&id) else { return reply(out, not_found()) };
        a.ensure_timeline(store);
        let prompts: Vec<Value> = a
            .timeline
            .as_deref()
            .unwrap_or_default()
            .iter()
            .filter(|r| r.item.get("type").and_then(|t| t.as_str()) == Some("user_message"))
            .map(|r| json!({ "seq": r.seq_start, "timestamp": r.timestamp, "preview": truncate(r.item.get("text").and_then(|t| t.as_str()).unwrap_or("").trim(), 160) }))
            .collect();
        reply(out, json!({ "type": "agent.timeline.list_prompts.response", "payload": { "requestId": rid, "agentId": a.rec.id, "epoch": EPOCH, "prompts": prompts, "error": null } }));
    }
}

// ---- daemon ----

/// Resident memory of the daemon process in KiB (Linux), for status/diagnostics.
fn rss_kib() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|l| l.starts_with("VmRSS:"))?;
    line.split_whitespace().nth(1)?.parse().ok()
}

impl Manager {
    /// Agent process / queue state, shared by status and diagnostics.
    fn runtime_status(&self) -> Value {
        json!({
            "activeCli": self.active_cli,
            "runningTurns": self.running.len(),
            "queuedTurns": self.cli_queue.len(),
            "claude": { "liveProcesses": self.claude.live_count(), "queued": self.claude.waiting_count(), "maxLive": self.claude.max_live(), "maxLiveMode": self.cfg.claude_max_live.label(), "keepWarm": self.cfg.claude_keep_warm },
            "codex": { "appServerRunning": self.codex.is_running(), "maxActive": self.slots("codex"), "maxActiveMode": self.cfg.codex_max_active.label(), "keepWarm": self.cfg.codex_keep_warm },
            "capacity": self.capacity.as_ref().map(|c| c.status()),
            "agents": { "total": self.agents.len(), "running": self.agents.values().filter(|a| a.status == "running").count() },
            "autoCompactPct": self.cfg.auto_compact_pct,
            "autoCompactTokens": self.cfg.auto_compact_tokens,
            "agentNice": self.cfg.agent_nice,
            "daemonRssKib": rss_kib(),
        })
    }

    fn daemon_status(&self, rid: &str, out: &Out) {
        let cfg = &self.cfg;
        let providers = json!([
            { "provider": "claude", "available": binary_available(&cfg.claude_bin) },
            { "provider": "codex", "available": binary_available(&cfg.codex_bin) },
        ]);
        reply(out, json!({ "type": "daemon.get_status.response", "payload": {
            "requestId": rid,
            "serverId": self.server_id,
            "version": env!("CARGO_PKG_VERSION"),
            "pid": std::process::id(),
            // Upstream reports the Node binary; the closest equivalent is this executable.
            "nodePath": std::env::current_exe().map(|p| p.to_string_lossy().into_owned()).unwrap_or_default(),
            "startedAt": self.started_at,
            "listen": cfg.listen,
            "relay": { "enabled": cfg.relay_enabled, "endpoint": cfg.relay_endpoint, "publicEndpoint": cfg.relay_public_endpoint, "useTls": cfg.relay_use_tls, "publicUseTls": cfg.relay_public_use_tls },
            "providers": providers,
            "runtime": self.runtime_status(),
        } }));
    }

    /// The app-editable config (custom models, appended prompt; see `daemon_config`).
    fn daemon_config(&self, rid: &str, out: &Out) {
        reply(out, json!({ "type": "get_daemon_config_response", "payload": { "requestId": rid, "config": self.daemon_config.to_json(&self.cfg) } }));
    }

    /// Apply a config patch, persist it, and push the result: the new config
    /// to every client and refreshed provider model lists (custom models).
    fn set_daemon_config(&mut self, msg: &Value, rid: &str, out: &Out) {
        let patch = msg.get("config").cloned().unwrap_or(Value::Null);
        self.daemon_config.apply_patch(&patch);
        let config = self.daemon_config.to_json(&self.cfg);
        reply(out, json!({ "type": "set_daemon_config_response", "payload": { "requestId": rid, "config": config } }));
        self.broadcast(json!({ "type": "status", "payload": { "status": "daemon_config_changed", "config": config } }));
        let entries = self.provider_entries_now();
        self.broadcast(json!({ "type": "providers_snapshot_update", "payload": { "entries": entries, "generatedAt": now_iso() } }));
        // The system prompt is a spawn flag for Claude; idle processes restart
        // on their next prompt, busy ones after their turn.
        if patch.get("appendSystemPrompt").is_some() {
            let ids: Vec<String> = self.agents.keys().cloned().collect();
            for id in ids {
                self.claude.restart_on_next_prompt(&id);
            }
        }
        crate::log!("daemon config updated");
    }

    fn diagnostics(&self) -> String {
        let cfg = &self.cfg;
        let rt = self.runtime_status();
        let mut lines = vec![
            format!("paseo-lite {} (pid {}, started {})", env!("CARGO_PKG_VERSION"), std::process::id(), self.started_at),
            format!("server id: {}", self.server_id),
            format!("listen: {}", cfg.listen),
            format!("relay: {}", if cfg.relay_enabled { format!("{} (tls {})", cfg.relay_endpoint, cfg.relay_use_tls) } else { "disabled".into() }),
            format!("daemon RSS: {}", rss_kib().map(|k| format!("{} MiB", k / 1024)).unwrap_or_else(|| "unknown".into())),
            format!("claude: bin {} ({}), {}", cfg.claude_bin, if binary_available(&cfg.claude_bin) { "found" } else { "missing" }, rt["claude"]),
            format!("codex: bin {} ({}), {}", cfg.codex_bin, if binary_available(&cfg.codex_bin) { "found" } else { "missing" }, rt["codex"]),
            format!("agents: {}", rt["agents"]),
            format!("projects: {}", self.projects.len()),
            format!("agent priority: nice {}, oom_score_adj {}, low io {}", cfg.agent_nice, cfg.agent_oom_score_adj, cfg.agent_low_io),
            format!("auto-compact: {}", if cfg.auto_compact_pct == 0 { "off".into() } else { format!("at {}% of context or {} tokens", cfg.auto_compact_pct, cfg.auto_compact_tokens) }),
        ];
        let sent = crate::egress::snapshot();
        let total: u64 = sent.iter().map(|s| s.2).sum();
        lines.push(format!("sent to apps since start: {} KiB in {} messages", total / 1024, sent.iter().map(|s| s.1).sum::<u64>()));
        for (kind, n, bytes) in sent.iter().take(8) {
            lines.push(format!("  {kind}: {} KiB, {n} messages", bytes / 1024));
        }
        for a in self.agents.values().filter(|a| a.status != "idle") {
            lines.push(format!("agent {} [{}] {}: {}", a.rec.id, a.rec.provider, a.status, a.rec.last_error.as_deref().unwrap_or("")));
        }
        lines.join("\n")
    }
}

// ---- projects / workspaces ----

impl Manager {
    /// Every directory the app can see: registered projects plus agent cwds.
    fn known_paths(&self) -> Vec<String> {
        let mut paths: Vec<String> = self.projects.iter().map(|p| p.root_path.clone()).collect();
        for a in self.agents.values() {
            if !paths.contains(&a.rec.cwd) {
                paths.push(a.rec.cwd.clone());
            }
        }
        paths
    }

    /// The stored record for a directory, registering it if needed.
    fn project_record(&mut self, path: &str) -> &mut ProjectRecord {
        if let Some(i) = self.projects.iter().position(|p| p.root_path == path) {
            return &mut self.projects[i];
        }
        self.projects.push(ProjectRecord { root_path: path.to_string(), created_at: now_iso(), custom_name: None, owned: false });
        self.projects.last_mut().expect("just pushed")
    }

    fn active_workspaces_of(&self, project_root: &str) -> Vec<String> {
        self.workspaces.iter().filter(|w| w.archived_at.is_none() && w.project_root == project_root).map(|w| w.id.clone()).collect()
    }

    fn rename_project(&mut self, msg: &Value, rid: &str, out: &Out) {
        let project_id = s(msg, "projectId").unwrap_or_default();
        let custom = msg.get("customName").and_then(|v| v.as_str()).map(str::trim).filter(|n| !n.is_empty()).map(String::from);
        if !self.known_paths().contains(&project_id) {
            return reply(out, json!({ "type": "project.rename.response", "payload": { "requestId": rid, "projectId": project_id, "accepted": false, "customName": null, "error": "project not found" } }));
        }
        self.project_record(&project_id).custom_name = custom.clone();
        self.store.save_projects(&self.projects);
        crate::snapshot::set_project_name(&project_id, custom.as_deref());
        self.broadcast(json!({ "type": "project.update", "payload": { "kind": "upsert", "project": project_descriptor(&project_id) } }));
        for ws in self.active_workspaces_of(&project_id) {
            self.publish_workspace(&ws);
        }
        reply(out, json!({ "type": "project.rename.response", "payload": { "requestId": rid, "projectId": project_id, "accepted": true, "customName": custom, "error": null } }));
    }

    /// Unregister a project: archive its workspaces and their agents
    /// (reversible: prompting an archived agent restores it) and forget it.
    /// Returns whether paseo-lite created the directory (None when it was
    /// not a registered project) and the archived workspace ids.
    fn drop_project(&mut self, path: &str) -> (Option<bool>, Vec<String>) {
        let owned = self.projects.iter().find(|p| p.root_path == path).map(|p| p.owned);
        let archived = self.active_workspaces_of(path);
        for ws in &archived {
            let _ = self.archive_workspace(ws);
        }
        self.projects.retain(|p| p.root_path != path);
        self.store.save_projects(&self.projects);
        crate::snapshot::set_project_name(path, None);
        self.broadcast(json!({ "type": "project.update", "payload": { "kind": "remove", "projectId": path } }));
        crate::log!("project removed: {path} ({} workspaces archived)", archived.len());
        (owned, archived)
    }

    /// Delete the removed project's directory in the background (see
    /// `cleanup`), then call `done` with a note when the files were kept.
    /// Only registered project folders are ever deleted (`owned` is None for a
    /// directory that was merely some agent's cwd).
    fn cleanup_dir(&self, path: String, owned: Option<bool>, done: impl FnOnce(Option<String>) + Send + 'static) {
        let Some(owned) = owned else { return done(None) };
        let (policy, home) = (self.cfg.delete_project_files, self.cfg.home.clone());
        // Every other directory we know (archived agents and workspaces too),
        // so a parent folder is never deleted out from under a nested project.
        let others: Vec<String> = self
            .projects
            .iter()
            .map(|p| p.root_path.clone())
            .chain(self.agents.values().map(|a| a.rec.cwd.clone()))
            .chain(self.workspaces.iter().filter(|w| w.project_root != path).map(|w| w.directory.clone()))
            .filter(|p| *p != path)
            .collect();
        tokio::spawn(async move {
            let note = match crate::cleanup::delete_project_dir(path, owned, policy, home, others).await {
                Ok(_) => None,
                Err(reason) => {
                    crate::warn!("project files {reason}");
                    Some(format!("Removed from Paseo, but the files were {reason}"))
                }
            };
            done(note);
        });
    }

    /// Remove a project: archive its workspaces and agents, unregister it and
    /// clean up its local directory.
    fn remove_project(&mut self, msg: &Value, rid: &str, out: &Out) {
        let project_id = s(msg, "projectId").unwrap_or_default();
        if !self.known_paths().contains(&project_id) {
            return reply(out, json!({ "type": "project.remove.response", "payload": { "requestId": rid, "projectId": project_id, "accepted": false, "removedWorkspaceIds": [], "error": "project not found" } }));
        }
        let (owned, removed) = self.drop_project(&project_id);
        let (out, rid) = (out.clone(), rid.to_string());
        self.cleanup_dir(project_id.clone(), owned, move |note| {
            reply(&out, json!({ "type": "project.remove.response", "payload": { "requestId": rid, "projectId": project_id, "accepted": true, "removedWorkspaceIds": removed, "error": note } }));
        });
    }

    /// Archive a workspace and its agents. When that was the project's last
    /// workspace, the project goes too and its local folder is cleaned up.
    pub(crate) fn archive_workspace_request(&mut self, msg: &Value, rid: &str, out: &Out) {
        let id = s(msg, "workspaceId").unwrap_or_default();
        let root = self.workspaces.iter().find(|w| w.id == id).map(|w| w.project_root.clone());
        let at = match self.archive_workspace(&id) {
            Ok(at) => at,
            Err(e) => return reply(out, json!({ "type": "archive_workspace_response", "payload": { "requestId": rid, "workspaceId": id, "archivedAt": null, "error": e } })),
        };
        reply(out, json!({ "type": "archive_workspace_response", "payload": { "requestId": rid, "workspaceId": id, "archivedAt": at, "error": null } }));
        let last = root.filter(|r| self.projects.iter().any(|p| &p.root_path == r) && self.active_workspaces_of(r).is_empty());
        if let Some(root) = last {
            let (owned, _) = self.drop_project(&root);
            // The archive already succeeded; a kept folder is only logged.
            self.cleanup_dir(root, owned, |_| {});
        }
    }
}

/// Usage: tokens and API-priced cost per provider (see `usage`).
impl Manager {
    const USAGE_SOURCES: [(&'static str, &'static str); 2] = [("claude", "Claude Code"), ("codex", "Codex")];

    /// The provider cards, then one card per model. An agent's view is its
    /// own provider's card only. `ids` picks cards (a refresh of one card).
    fn usage_entries(&self, agent: Option<&str>, ids: Option<Vec<String>>) -> Vec<Value> {
        if let Some(agent) = agent {
            let provider = self.agents.get(agent).map(|a| a.rec.provider.clone());
            return Self::USAGE_SOURCES.iter().filter(|(id, _)| provider.as_deref() == Some(*id)).map(|(id, label)| self.usage.report(id, label, Some(agent))).collect();
        }
        let mut cards: Vec<Value> = Self::USAGE_SOURCES.iter().map(|(id, label)| self.usage.report(id, label, None)).collect();
        for (id, label) in Self::USAGE_SOURCES {
            cards.extend(self.usage.model_reports(id, label));
        }
        if let Some(ids) = ids {
            cards.retain(|c| c["id"].as_str().map(|id| ids.iter().any(|i| i == id)).unwrap_or(false));
        }
        cards
    }

    fn usage_reports(&self, msg: &Value, rid: &str, out: &Out) {
        let agent_key = s(msg, "agentId");
        let agent = agent_key.map(|k| self.resolve(&k).unwrap_or(k));
        let ids = msg.get("reportIds").and_then(|v| v.as_array()).map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect());
        for report in self.usage_entries(agent.as_deref(), ids) {
            reply(out, json!({ "type": "usage.list_reports.update", "payload": { "requestId": rid, "report": report } }));
        }
        reply(out, json!({ "type": "usage.list_reports.response", "payload": { "requestId": rid, "error": null } }));
    }

    // COMPAT(providerUsageList): apps from before usage sources ask for this.
    fn legacy_usage(&self, rid: &str, out: &Out) {
        let fetched_at = now_iso();
        let providers: Vec<Value> = self
            .usage_entries(None, None)
            .into_iter()
            .map(|e| {
                json!({
                    "providerId": e["sourceId"],
                    "displayName": e["sourceLabel"],
                    "status": "available",
                    "planLabel": null,
                    "sourceLabel": e["account"]["label"],
                    "fetchedAt": e["fetchedAt"],
                    "windows": [],
                    "balances": e["report"]["balances"],
                    "details": e["report"]["details"],
                })
            })
            .collect();
        reply(out, json!({ "type": "provider.usage.list.response", "payload": { "requestId": rid, "fetchedAt": fetched_at, "providers": providers } }));
    }
}
