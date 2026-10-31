//! Parse Claude Code `--output-format stream-json` lines into provider events.

use super::tools;
use super::{EventTx, ProviderEvent};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};

pub struct StreamState {
    pub agent_id: String,
    pub events: EventTx,
    pub stdin: mpsc::UnboundedSender<String>,
    pub interrupted: Arc<AtomicBool>,
    pub session_id: Option<String>,
    /// tool_use_id -> (name, input), so results can rebuild the detail.
    tools: HashMap<String, (String, Value)>,
    /// Message ids whose text/thinking already arrived as stream deltas.
    streamed: HashSet<String>,
    current_message: Option<String>,
    /// Context window occupancy after the latest API call.
    context_tokens: Option<u64>,
    /// Latest `rate_limit_event` info of the running turn.
    limit_info: Option<Value>,
    /// The turn was refused by the usage limit (`api_error: usage_limit_reached`), with its reset when given.
    limit_refused: Option<Option<u64>>,
    /// A sub-agent of the running turn stopped on the usage or rate limit.
    sub_limited: bool,
    /// Task tool_use_id -> the running sub-agent's action log.
    sub_logs: HashMap<String, Vec<String>>,
    /// tool_use_id -> (name, input) of work running in the background:
    /// sub-agents, shell commands and workflows.
    background: HashMap<String, (String, Value)>,
    /// Whether background work is running and when it last showed activity.
    /// The pool keeps the process alive while it works.
    pub sub_active: Arc<Mutex<SubAgents>>,
    /// How many sub-agents may run at once, set by the pool from the
    /// machine's state (0 = start no new ones).
    pub sub_cap: Arc<AtomicUsize>,
    /// Sub-agent launches the gate let through, and when.
    admitted: HashMap<String, Instant>,
}

/// The `PreToolUse` hook callback id the daemon registers for Agent/Task
/// calls, so it decides how many sub-agents run at once.
pub const SUBAGENT_HOOK: &str = "paseo-subagent-gate";
/// An admitted launch counts as running this long even before its Task call
/// shows up in the stream.
const ADMIT_GRACE: Duration = Duration::from_secs(30);

/// Lines kept in a running sub-agent's log (the oldest are dropped).
const MAX_SUB_LOG_LINES: usize = 50;
/// Background tasks tracked at once.
const MAX_BACKGROUND: usize = 32;
/// A background sub-agent silent for this long is taken for hung: its
/// process may then be reaped or evicted like any idle one.
const SUBAGENT_QUIET_MAX: Duration = Duration::from_secs(30 * 60);
/// The same for background shells and workflows, which show no progress
/// until they report: as long as Bash lets a background command run.
const BACKGROUND_QUIET_MAX: Duration = Duration::from_secs(2 * 60 * 60);
/// Kept after the last sub-agent reports, while the manager opens the turn
/// the CLI runs on its result.
const SUBAGENT_GRACE: Duration = Duration::from_secs(15);

/// A process's background work (sub-agents, shells, workflows), as the pool
/// sees it.
#[derive(Default)]
pub struct SubAgents {
    pub running: bool,
    pub last_seen: Option<Instant>,
    /// Silence after which running work is taken for hung.
    pub quiet_max: Duration,
}

impl SubAgents {
    /// Whether the process must stay up for them.
    pub fn holding(&self) -> bool {
        let quiet = self.last_seen.map(|t| t.elapsed()).unwrap_or(Duration::MAX);
        quiet < if self.running { self.quiet_max } else { SUBAGENT_GRACE }
    }
}

fn is_agent_tool(name: &str) -> bool {
    matches!(name, "Task" | "Agent")
}

/// Does this successful tool result mean the work runs on in the background
/// and reports back with a `<task-notification>`? Workflows always do, shell
/// commands when asked to, sub-agents say so in their short launch answer
/// (a foreground sub-agent's final report must not be taken for one).
fn launched_in_background(name: &str, input: &Value, output: &str) -> bool {
    let asked = input.get("run_in_background").and_then(|v| v.as_bool()).unwrap_or(false);
    match name {
        "Workflow" => true,
        "Bash" | "PowerShell" => asked || output.starts_with("Command running in background with ID:"),
        "Task" | "Agent" => {
            if output.starts_with("Async agent launched") {
                return true;
            }
            // Forks, and sub-agents that run in the background by default,
            // may not set the flag, and the wording varies: a launch answer
            // is short, and its first line is about the agent starting.
            let asked = asked || s(input, "subagent_type").as_deref() == Some("fork");
            let text = output.trim_start().to_lowercase();
            let first = text.lines().next().unwrap_or("");
            let about_agent = ["agent", "fork", "background"].iter().any(|w| first.contains(w));
            let started = first.contains("launched") || first.contains("started");
            let notice = text.contains("in the background") || text.contains("will be notified");
            about_agent && ((started && text.len() < 2000) || (notice && text.len() < 1000) || (asked && text.len() < 300))
        }
        _ => false,
    }
}

fn s(v: &Value, key: &str) -> Option<String> {
    v.get(key).and_then(|x| x.as_str()).map(|x| x.to_string())
}

impl StreamState {
    pub fn new(agent_id: String, events: EventTx, stdin: mpsc::UnboundedSender<String>, interrupted: Arc<AtomicBool>, session_id: Option<String>) -> Self {
        StreamState { agent_id, events, stdin, interrupted, session_id, tools: HashMap::new(), streamed: HashSet::new(), current_message: None, context_tokens: None, limit_info: None, limit_refused: None, sub_limited: false, sub_logs: HashMap::new(), background: HashMap::new(), sub_active: Arc::default(), sub_cap: Arc::new(AtomicUsize::new(usize::MAX)), admitted: HashMap::new() }
    }

    fn emit(&self, ev: ProviderEvent) {
        let _ = self.events.send(ev);
    }

    fn timeline(&self, item: Value) {
        self.emit(ProviderEvent::Timeline { agent_id: self.agent_id.clone(), item });
    }

    pub fn handle_line(&mut self, line: &str) {
        let Ok(msg) = serde_json::from_str::<Value>(line) else { return };
        let is_sidechain = msg.get("parent_tool_use_id").map(|v| !v.is_null()).unwrap_or(false);
        match msg.get("type").and_then(|t| t.as_str()).unwrap_or("") {
            "system" if s(&msg, "subtype").as_deref() == Some("compact_boundary") => {
                let meta = msg.get("compact_metadata").or_else(|| msg.get("compactMetadata")).unwrap_or(&Value::Null);
                let pick = |a: &str, b: &str| meta.get(a).or_else(|| meta.get(b)).and_then(|v| v.as_u64());
                let post = pick("post_tokens", "postTokens");
                self.context_tokens = post;
                let trigger = if s(meta, "trigger").as_deref() == Some("manual") { "manual" } else { "auto" };
                let mut item = json!({ "type": "compaction", "status": "completed", "trigger": trigger });
                if let Some(pre) = pick("pre_tokens", "preTokens") {
                    item["preTokens"] = json!(pre);
                }
                self.timeline(item);
            }
            "system" => {
                if s(&msg, "subtype").as_deref() == Some("init") {
                    if let Some(sid) = s(&msg, "session_id") {
                        if self.session_id.as_deref() != Some(sid.as_str()) {
                            self.session_id = Some(sid.clone());
                            self.emit(ProviderEvent::SessionStarted { agent_id: self.agent_id.clone(), session_id: sid });
                        }
                    }
                }
            }
            "stream_event" if !is_sidechain => self.handle_stream_event(msg.get("event").unwrap_or(&Value::Null)),
            "assistant" if !is_sidechain => self.handle_assistant(&msg),
            "assistant" => {
                self.sub_limited |= is_limit_message(&msg);
                self.handle_sidechain(&msg);
            }
            "user" if !is_sidechain => self.handle_tool_results(&msg),
            "rate_limit_event" => self.limit_info = msg.get("rate_limit_info").cloned(),
            "result" => self.handle_result(&msg),
            "control_request" => self.handle_control_request(&msg),
            _ => {}
        }
    }

    fn handle_stream_event(&mut self, ev: &Value) {
        match ev.get("type").and_then(|t| t.as_str()).unwrap_or("") {
            "message_start" => {
                self.current_message = ev.get("message").and_then(|m| s(m, "id"));
            }
            "content_block_delta" => {
                let delta = ev.get("delta").unwrap_or(&Value::Null);
                let (kind, text) = match delta.get("type").and_then(|t| t.as_str()) {
                    Some("text_delta") => ("assistant_message", s(delta, "text")),
                    Some("thinking_delta") => ("reasoning", s(delta, "thinking")),
                    _ => return,
                };
                if let Some(text) = text.filter(|t| !t.is_empty()) {
                    if let Some(id) = &self.current_message {
                        self.streamed.insert(id.clone());
                    }
                    self.timeline(json!({ "type": kind, "text": text }));
                }
            }
            _ => {}
        }
    }

    fn handle_assistant(&mut self, msg: &Value) {
        if s(msg, "api_error").as_deref() == Some("usage_limit_reached") {
            self.limit_refused = Some(msg.pointer("/api_error_params/rate_limit_info/resetsAt").and_then(|v| v.as_u64()));
        }
        let message = msg.get("message").unwrap_or(&Value::Null);
        // Each API call reports the full prompt it was sent, so the latest one
        // is how much of the context window the conversation now occupies.
        if let Some(u) = message.get("usage") {
            let n = |k: &str| u.get(k).and_then(|v| v.as_u64()).unwrap_or(0);
            let used = n("input_tokens") + n("cache_creation_input_tokens") + n("cache_read_input_tokens") + n("output_tokens");
            if used > 0 {
                self.context_tokens = Some(used);
            }
        }
        let streamed = s(message, "id").map(|id| self.streamed.contains(&id)).unwrap_or(false);
        let Some(blocks) = message.get("content").and_then(|c| c.as_array()) else { return };
        for block in blocks {
            match block.get("type").and_then(|t| t.as_str()).unwrap_or("") {
                "text" if !streamed => {
                    if let Some(text) = s(block, "text").filter(|t| !t.is_empty()) {
                        self.timeline(json!({ "type": "assistant_message", "text": text }));
                    }
                }
                "thinking" if !streamed => {
                    if let Some(text) = s(block, "thinking").filter(|t| !t.is_empty()) {
                        self.timeline(json!({ "type": "reasoning", "text": text }));
                    }
                }
                "tool_use" | "server_tool_use" | "mcp_tool_use" => {
                    let id = s(block, "id").unwrap_or_else(crate::util::new_id);
                    let name = s(block, "name").unwrap_or_else(|| "tool".into());
                    let input = block.get("input").cloned().unwrap_or(Value::Null);
                    if name == "TodoWrite" {
                        if let Some(todo) = tools::claude_todo(&input) {
                            self.timeline(todo);
                        }
                        continue;
                    }
                    let detail = tools::claude_detail(&name, &input, None);
                    self.timeline(tools::tool_call(&id, &name, detail, "running", None));
                    self.tools.insert(id, (name, input));
                }
                _ => {}
            }
        }
    }

    /// Tell the pool whether background work is still running. Shells and
    /// workflows show no activity until they report, so they may stay quiet
    /// longer than sub-agents before they count as hung.
    fn note_background(&self) {
        let quiet_max = if self.background.values().all(|(name, _)| is_agent_tool(name)) { SUBAGENT_QUIET_MAX } else { BACKGROUND_QUIET_MAX };
        *self.sub_active.lock().unwrap() = SubAgents { running: !self.background.is_empty(), last_seen: Some(Instant::now()), quiet_max };
    }

    /// A Task card whose log is the sub-agent's actions so far.
    fn sub_agent_card(&self, id: &str, name: &str, input: &Value, status: &str) -> Value {
        let mut detail = tools::claude_detail(name, input, None);
        detail["log"] = json!(self.sub_logs.get(id).map(|l| l.join("\n")).unwrap_or_default());
        tools::tool_call(id, name, detail, status, None)
    }

    /// A sub-agent's own message: list its tool calls on the parent Task card
    /// while it runs (in the foreground or the background), so the app shows
    /// what it is doing.
    fn handle_sidechain(&mut self, msg: &Value) {
        let Some(parent) = s(msg, "parent_tool_use_id") else { return };
        let Some((name, input)) = self.tools.get(&parent).or_else(|| self.background.get(&parent)).cloned() else { return };
        if self.background.contains_key(&parent) {
            self.note_background();
        }
        let Some(blocks) = msg.pointer("/message/content").and_then(|c| c.as_array()) else { return };
        let lines: Vec<String> = blocks
            .iter()
            .filter(|b| matches!(b.get("type").and_then(|t| t.as_str()), Some("tool_use" | "server_tool_use" | "mcp_tool_use")))
            .map(|b| tools::claude_action_line(&s(b, "name").unwrap_or_else(|| "tool".into()), b.get("input").unwrap_or(&Value::Null)))
            .collect();
        if lines.is_empty() {
            return;
        }
        let log = self.sub_logs.entry(parent.clone()).or_default();
        log.extend(lines);
        let excess = log.len().saturating_sub(MAX_SUB_LOG_LINES);
        log.drain(..excess);
        self.timeline(self.sub_agent_card(&parent, &name, &input, "running"));
    }

    /// Background work that reported back (`<task-notification>`): its cards
    /// get the final result. The CLI then runs a turn of its own on
    /// them, which the manager is told to open.
    fn finish_background(&mut self, content: &Value) {
        let text = content_text(content);
        if !text.contains("<task-notification>") {
            return;
        }
        self.emit(ProviderEvent::BackgroundTurn { agent_id: self.agent_id.clone() });
        for note in text.split("<task-notification>").skip(1) {
            let Some(id) = tag(note, "tool-use-id") else { continue };
            self.admitted.remove(&id);
            let Some((name, input)) = self.background.remove(&id) else { continue };
            self.sub_logs.remove(&id);
            let summary = tag(note, "summary").unwrap_or_default();
            let result = tag(note, "result").unwrap_or_else(|| summary.clone());
            let detail = tools::claude_detail(&name, &input, Some(&result));
            let item = match tag(note, "status").as_deref() {
                Some("completed") => tools::tool_call(&id, &name, detail, "completed", None),
                Some("failed") => tools::tool_call(&id, &name, detail, "failed", Some(summary)),
                _ => tools::tool_call(&id, &name, detail, "canceled", None),
            };
            self.timeline(item);
        }
        self.note_background();
    }

    /// The process is gone and the background work it was running died with
    /// it: fail the cards instead of leaving them running.
    pub fn process_ended(&mut self, killed: bool) {
        let error = if killed { "Stopped before it reported back: its Claude Code process was shut down" } else { "Stopped before it reported back: its Claude Code process exited" };
        for (id, (name, input)) in std::mem::take(&mut self.background) {
            let mut detail = tools::claude_detail(&name, &input, None);
            let log = self.sub_logs.remove(&id);
            if log.is_some() || is_agent_tool(&name) {
                detail["log"] = json!(log.map(|l| l.join("\n")).unwrap_or_default());
            }
            self.timeline(tools::tool_call(&id, &name, detail, "failed", Some(error.into())));
        }
        self.note_background();
    }

    fn handle_tool_results(&mut self, msg: &Value) {
        let content = msg.pointer("/message/content").unwrap_or(&Value::Null);
        if let Some(summary) = failed_task_summary(content) {
            self.sub_limited |= super::is_usage_limit(&summary);
        }
        self.finish_background(content);
        let Some(blocks) = msg.get("message").and_then(|m| m.get("content")).and_then(|c| c.as_array()) else { return };
        for block in blocks {
            if !block.get("type").and_then(|t| t.as_str()).map(|t| t.ends_with("tool_result")).unwrap_or(false) {
                continue;
            }
            let Some(id) = s(block, "tool_use_id") else { continue };
            let Some((name, input)) = self.tools.remove(&id) else { continue };
            let output = content_text(block.get("content").unwrap_or(&Value::Null));
            let failed = block.get("is_error").and_then(|v| v.as_bool()).unwrap_or(false);
            if !failed && launched_in_background(&name, &input, &output) {
                // It runs on in the background: the card stays running (a
                // sub-agent's keeps listing what it does) until its
                // notification. The launch text is internal.
                let card = if is_agent_tool(&name) { self.sub_agent_card(&id, &name, &input, "running") } else { tools::tool_call(&id, &name, tools::claude_detail(&name, &input, None), "running", None) };
                self.timeline(card);
                if self.background.len() < MAX_BACKGROUND {
                    self.background.insert(id, (name, input));
                    self.note_background();
                }
                continue;
            }
            self.sub_logs.remove(&id);
            self.admitted.remove(&id);
            // A sub-agent that died on the limit reports it as its result.
            if is_agent_tool(&name) && output.len() < 2000 && output.contains("API Error") && super::is_usage_limit(&output) {
                self.sub_limited = true;
            }
            let detail = tools::claude_detail(&name, &input, Some(&output));
            let (status, error) = if failed { ("failed", Some(output)) } else { ("completed", None) };
            self.timeline(tools::tool_call(&id, &name, detail, status, error));
        }
    }

    fn handle_result(&mut self, msg: &Value) {
        let agent_id = self.agent_id.clone();
        if let Some(u) = msg.get("usage") {
            let usage = json!({
                "inputTokens": u.get("input_tokens"),
                "cachedInputTokens": u.get("cache_read_input_tokens"),
                "outputTokens": u.get("output_tokens"),
                "totalCostUsd": msg.get("total_cost_usd"),
                "contextWindowUsedTokens": self.context_tokens,
            });
            self.emit(ProviderEvent::Usage { agent_id: agent_id.clone(), usage });
        }
        if let Some(sample) = crate::usage::claude_sample(msg) {
            self.emit(ProviderEvent::Spend { agent_id: agent_id.clone(), sample });
        }
        // Drop any tool rows still running; their results will never arrive.
        for (id, (name, input)) in self.tools.drain() {
            let detail = tools::claude_detail(&name, &input, None);
            let _ = self.events.send(ProviderEvent::Timeline { agent_id: agent_id.clone(), item: tools::tool_call(&id, &name, detail, "canceled", None) });
        }
        self.streamed.clear();
        // Background work outlives the turn that launched it.
        let background = &self.background;
        self.sub_logs.retain(|id, _| background.contains_key(id));
        let ok = s(msg, "subtype").as_deref() == Some("success") && !msg.get("is_error").and_then(|v| v.as_bool()).unwrap_or(false);
        let error = || {
            msg.get("errors")
                .and_then(|e| e.as_array())
                .map(|e| e.iter().filter_map(|x| x.as_str()).collect::<Vec<_>>().join("; "))
                .filter(|e| !e.is_empty())
                .or_else(|| s(msg, "result"))
                .unwrap_or_else(|| s(msg, "subtype").unwrap_or_else(|| "turn failed".into()))
        };
        let sub_reset = self.limit_info.as_ref().filter(|i| s(i, "status").as_deref() == Some("rejected")).and_then(|i| i.get("resetsAt")).and_then(|v| v.as_u64());
        let sub_limited = std::mem::take(&mut self.sub_limited);
        let limit = self.usage_limit(msg, ok, &error());
        if ok && limit.is_none() && sub_limited && !self.interrupted.load(Ordering::SeqCst) {
            // The turn itself finished; the manager waits out the limit and has the work picked up.
            self.emit(ProviderEvent::LimitHit { agent_id: agent_id.clone(), resets_at: sub_reset });
        }
        if self.interrupted.swap(false, Ordering::SeqCst) {
            self.emit(ProviderEvent::TurnCanceled { agent_id, reason: "interrupted".into() });
        } else if let Some(resets_at) = limit {
            self.emit(ProviderEvent::LimitHit { agent_id: agent_id.clone(), resets_at });
            self.emit(ProviderEvent::TurnFailed { agent_id, error: error() });
        } else if ok {
            self.emit(ProviderEvent::TurnCompleted { agent_id });
        } else {
            self.emit(ProviderEvent::TurnFailed { agent_id, error: error() });
        }
    }

    /// Did the usage limit end this turn? Some(reset time, when known).
    /// Claude says so in a typed field, a `rejected` rate-limit event or, on
    /// older builds, only in the reply text (sometimes on a "successful" result).
    fn usage_limit(&mut self, msg: &Value, ok: bool, error: &str) -> Option<Option<u64>> {
        let refused = self.limit_refused.take();
        let info = self.limit_info.take();
        let rejected = info.filter(|i| s(i, "status").as_deref() == Some("rejected"));
        let text = s(msg, "result").unwrap_or_default();
        let lead = text.trim_start().to_ascii_lowercase();
        // A reply merely mentioning rate limits ("You can raise the rate limit...") is not one.
        let says_so = text.len() < 300 && (lead.starts_with("you") || lead.starts_with("claude ai usage limit")) && !lead.contains("rate limit") && super::is_usage_limit(&text);
        let hit = refused.is_some() || (!ok && (rejected.is_some() || super::is_usage_limit(error))) || (ok && says_so);
        if !hit {
            return None;
        }
        let event_reset = rejected.as_ref().and_then(|i| i.get("resetsAt")).and_then(|v| v.as_u64());
        Some(refused.flatten().or(event_reset).or_else(|| super::reset_in_text(&text)).or_else(|| super::reset_in_text(error)))
    }

    /// The `PreToolUse` hook for Agent/Task: let a sub-agent start while fewer
    /// than `sub_cap` are running (foreground or background), else deny it
    /// with a reason the model can act on. Deny wins over every permission
    /// mode, Bypass included. Only launches this hook let through count, so
    /// background shells and workflows never take a sub-agent's place.
    fn gate_subagent(&mut self, req: &Value) -> Value {
        let now = Instant::now();
        let (tools, background) = (&self.tools, &self.background);
        self.admitted.retain(|id, at| tools.contains_key(id) || background.contains_key(id) || now.duration_since(*at) < ADMIT_GRACE);
        let cap = self.sub_cap.load(Ordering::Relaxed);
        let running = self.admitted.len();
        if running < cap {
            let id = s(req, "tool_use_id").or_else(|| req.get("input").and_then(|i| s(i, "tool_use_id"))).unwrap_or_else(crate::util::new_id);
            self.admitted.insert(id, now);
            return json!({});
        }
        let reason = if cap == 0 {
            "Sub-agent not started: the machine is low on memory or CPU right now. Do this part yourself if it is small; otherwise tell the user it is on hold and launch it after a running sub-agent reports back or in a later turn.".to_string()
        } else {
            format!("Sub-agent not started: {running} sub-agent(s) are already running, the most this machine allows at once. Launch it after one of them reports back (its notification starts a new turn), or do this part yourself if it is small. Do not wait or poll for them.")
        };
        crate::log!("claude {}: sub-agent launch refused ({running} running, cap {cap})", self.agent_id);
        json!({ "hookSpecificOutput": { "hookEventName": "PreToolUse", "permissionDecision": "deny", "permissionDecisionReason": reason } })
    }

    fn handle_control_request(&mut self, msg: &Value) {
        let Some(request_id) = s(msg, "request_id") else { return };
        let req = msg.get("request").cloned().unwrap_or(Value::Null);
        if s(&req, "subtype").as_deref() == Some("hook_callback") && s(&req, "callback_id").as_deref() == Some(SUBAGENT_HOOK) {
            let resp = json!({ "type": "control_response", "response": { "subtype": "success", "request_id": request_id, "response": self.gate_subagent(&req) } });
            let _ = self.stdin.send(resp.to_string());
            return;
        }
        if s(&req, "subtype").as_deref() != Some("can_use_tool") {
            // Unknown host callbacks (hooks, MCP) are answered with an error so the CLI never stalls.
            let resp = json!({ "type": "control_response", "response": { "subtype": "error", "request_id": request_id, "error": "unsupported" } });
            let _ = self.stdin.send(resp.to_string());
            return;
        }
        let name = s(&req, "tool_name").unwrap_or_else(|| "tool".into());
        let input = req.get("input").cloned().unwrap_or_else(|| json!({}));
        let kind = match name.as_str() {
            "ExitPlanMode" => "plan",
            "AskUserQuestion" => "question",
            _ => "tool",
        };
        let mut request = json!({
            "id": format!("perm-{}", crate::util::new_id()),
            "provider": "claude",
            "name": name,
            "kind": kind,
            "title": name,
            "input": input.clone(),
            "detail": tools::claude_detail(&name, &input, None),
        });
        if kind == "question" {
            // The app renders its question form from `input`, not a tool detail.
            request["input"] = question_input(&input);
            if let Some(r) = request.as_object_mut() {
                r.remove("detail");
            }
            // Title and description also feed the push notification.
            let first = input.get("questions").and_then(|q| q.get(0));
            if let Some(text) = first.and_then(|q| s(q, "question")) {
                request["title"] = json!(text);
            }
            let labels: Vec<String> = first.and_then(|q| q.get("options")).and_then(|o| o.as_array()).map(|o| o.iter().filter_map(|o| s(o, "label")).collect()).unwrap_or_default();
            if !labels.is_empty() {
                request["description"] = json!(labels.join(" / "));
            }
        }
        let (tx, rx) = oneshot::channel::<Value>();
        self.emit(ProviderEvent::Permission { agent_id: self.agent_id.clone(), request, reply: tx });
        let stdin = self.stdin.clone();
        tokio::spawn(async move {
            // A dropped sender (agent canceled/archived) counts as a deny.
            let answer = rx.await.unwrap_or_else(|_| json!({ "behavior": "deny", "message": "Permission request canceled" }));
            let inner = if answer.get("behavior").and_then(|b| b.as_str()) == Some("allow") {
                let updated = if kind == "question" { question_answer(answer.get("updatedInput"), &input) } else { answer.get("updatedInput").cloned() };
                json!({ "behavior": "allow", "updatedInput": updated.unwrap_or(input) })
            } else {
                json!({
                    "behavior": "deny",
                    "message": answer.get("message").and_then(|m| m.as_str()).unwrap_or("The user denied this tool call"),
                    "interrupt": answer.get("interrupt").and_then(|i| i.as_bool()).unwrap_or(false),
                })
            };
            let resp = json!({ "type": "control_response", "response": { "subtype": "success", "request_id": request_id, "response": inner } });
            let _ = stdin.send(resp.to_string());
        });
    }
}

/// Is this assistant message the CLI's report of a usage or rate limit error?
fn is_limit_message(msg: &Value) -> bool {
    let flagged = ["error", "api_error"].iter().any(|k| matches!(s(msg, k).as_deref(), Some("rate_limit" | "usage_limit_reached")));
    let text = msg.pointer("/message/content/0/text").and_then(|t| t.as_str()).unwrap_or("");
    flagged || (text.starts_with("API Error") && super::is_usage_limit(text))
}

/// The summary of a background sub-agent's `<task-notification>` that says
/// it failed ("Agent \"...\" failed: Agent terminated early due to an API error: ...").
fn failed_task_summary(content: &Value) -> Option<String> {
    let text = content_text(content);
    if !text.contains("<task-notification>") || !text.contains("<status>failed</status>") {
        return None;
    }
    tag(&text, "summary")
}

/// The text of message or tool-result content (a string or text parts).
fn content_text(content: &Value) -> String {
    match content {
        Value::String(t) => t.clone(),
        Value::Array(parts) => parts.iter().filter_map(|p| s(p, "text")).collect::<Vec<_>>().join("\n"),
        _ => String::new(),
    }
}

/// The text inside the first `<name>…</name>` of a notification.
fn tag(text: &str, name: &str) -> Option<String> {
    let (_, rest) = text.split_once(&format!("<{name}>"))?;
    rest.split_once(&format!("</{name}>")).map(|(inner, _)| inner.trim().to_string())
}

/// AskUserQuestion leaves "Other" to the host; the app shows its free-text
/// box only for questions flagged `allowOther`.
fn question_input(input: &Value) -> Value {
    let mut out = input.clone();
    if let Some(questions) = out.get_mut("questions").and_then(|q| q.as_array_mut()) {
        for q in questions.iter_mut().filter(|q| q.is_object()) {
            q["allowOther"] = json!(true);
        }
    }
    out
}

/// The CLI input for an answered AskUserQuestion. The app keys `answers` by
/// each question's header, but the CLI matches them by question text, so
/// re-key them (either key is accepted). None when nothing was answered.
pub fn question_answer(updated: Option<&Value>, original: &Value) -> Option<Value> {
    let mut input = original.clone();
    if let (Some(dst), Some(src)) = (input.as_object_mut(), updated.and_then(|u| u.as_object())) {
        dst.extend(src.iter().map(|(k, v)| (k.clone(), v.clone())));
    }
    let given = input.get("answers").cloned().unwrap_or(Value::Null);
    let pick = |key: Option<String>| key.and_then(|k| given.get(&k)?.as_str().map(str::trim).filter(|a| !a.is_empty()).map(String::from));
    let mut answers = serde_json::Map::new();
    for q in input.get_mut("questions").and_then(|q| q.as_array_mut()).into_iter().flatten() {
        if let Some(o) = q.as_object_mut() {
            o.remove("allowOther");
        }
        let Some(text) = s(q, "question") else { continue };
        if let Some(answer) = pick(Some(text.clone())).or_else(|| pick(s(q, "header"))) {
            answers.insert(text, json!(answer));
        }
    }
    if answers.is_empty() {
        return None;
    }
    input["answers"] = Value::Object(answers);
    Some(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(lines: &[Value]) -> Vec<ProviderEvent> {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let (stdin, _keep) = mpsc::unbounded_channel();
        let mut st = StreamState::new("a1".into(), tx, stdin, Arc::new(AtomicBool::new(false)), Some("s1".into()));
        for line in lines {
            st.handle_line(&line.to_string());
        }
        let mut out = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            out.push(ev);
        }
        out
    }

    fn limit_of(events: &[ProviderEvent]) -> Option<Option<u64>> {
        events.iter().find_map(|e| match e {
            ProviderEvent::LimitHit { resets_at, .. } => Some(*resets_at),
            _ => None,
        })
    }

    #[test]
    fn usage_limit_is_reported_with_its_reset() {
        let events = run(&[
            json!({ "type": "rate_limit_event", "rate_limit_info": { "status": "rejected", "resetsAt": 1_760_000_000u64, "rateLimitType": "five_hour" } }),
            json!({ "type": "result", "subtype": "success", "is_error": true, "result": "You've hit your session limit \u{b7} resets 3am (UTC)" }),
        ]);
        assert_eq!(limit_of(&events), Some(Some(1_760_000_000)));
        assert!(events.iter().any(|e| matches!(e, ProviderEvent::TurnFailed { .. })));

        let refused = run(&[
            json!({ "type": "assistant", "api_error": "usage_limit_reached", "api_error_params": { "rate_limit_info": { "status": "rejected", "resetsAt": 1_770_000_000u64 } }, "message": { "content": [] } }),
            json!({ "type": "result", "subtype": "success", "is_error": true, "result": "limit" }),
        ]);
        assert_eq!(limit_of(&refused), Some(Some(1_770_000_000)));

        let legacy = run(&[json!({ "type": "result", "subtype": "success", "result": "Claude AI usage limit reached|1760000000" })]);
        assert_eq!(limit_of(&legacy), Some(Some(1_760_000_000)));
    }

    #[test]
    fn other_results_are_not_limits() {
        let ok = run(&[
            json!({ "type": "rate_limit_event", "rate_limit_info": { "status": "allowed_warning", "resetsAt": 1_760_000_000u64 } }),
            json!({ "type": "result", "subtype": "success", "result": "Done. The tests pass." }),
        ]);
        assert_eq!(limit_of(&ok), None);
        assert!(ok.iter().any(|e| matches!(e, ProviderEvent::TurnCompleted { .. })));
        let failed = run(&[json!({ "type": "result", "subtype": "error_during_execution", "is_error": true, "result": "API Error: 529 overloaded" })]);
        assert_eq!(limit_of(&failed), None);
    }

    #[test]
    fn questions_offer_free_text() {
        let input = json!({ "questions": [{ "question": "Which DB?", "header": "DB", "options": [{ "label": "Postgres" }], "multiSelect": false }] });
        assert_eq!(question_input(&input)["questions"][0]["allowOther"], true);
    }

    #[test]
    fn question_answers_are_keyed_by_question_text() {
        let input = json!({ "questions": [
            { "question": "Which DB?", "header": "DB", "options": [{ "label": "Postgres" }] },
            { "question": "ORM?", "header": "ORM", "options": [{ "label": "Diesel" }] },
        ] });
        // What the app sends back: the input it was given (with allowOther) and answers by header.
        let mut updated = question_input(&input);
        updated["answers"] = json!({ "DB": "SQLite, with WAL", "ORM?": "Diesel" });
        let out = question_answer(Some(&updated), &input).unwrap();
        assert_eq!(out["answers"], json!({ "Which DB?": "SQLite, with WAL", "ORM?": "Diesel" }));
        assert!(out["questions"][0].get("allowOther").is_none());

        assert!(question_answer(Some(&json!({ "answers": { "DB": "  " } })), &input).is_none());
        assert!(question_answer(None, &input).is_none());
    }

    #[test]
    fn rate_limit_rejection_is_a_limit() {
        let events = run(&[json!({ "type": "result", "subtype": "error_during_execution", "is_error": true, "result": "API Error: Request rejected (429) \u{b7} This request would exceed your account's rate limit. Please try again later." })]);
        assert_eq!(limit_of(&events), Some(None));
        // Merely mentioning rate limits in a reply is not one.
        let ok = run(&[json!({ "type": "result", "subtype": "success", "result": "You can raise the rate limit in config.toml." })]);
        assert_eq!(limit_of(&ok), None);
    }

    #[test]
    fn subagent_limit_is_reported_on_a_finished_turn() {
        let done = json!({ "type": "result", "subtype": "success", "result": "Done." });
        // A sub-agent's own API error (sidechain message).
        let sidechain = json!({ "type": "assistant", "parent_tool_use_id": "toolu_1", "error": "rate_limit", "message": { "content": [{ "type": "text", "text": "API Error: Request rejected (429)" }] } });
        let events = run(&[sidechain, done.clone()]);
        let at = events.iter().position(|e| matches!(e, ProviderEvent::LimitHit { .. })).unwrap();
        assert!(matches!(events[at + 1], ProviderEvent::TurnCompleted { .. }));

        // A foreground Task whose result is the limit error.
        let task = json!({ "type": "assistant", "message": { "content": [{ "type": "tool_use", "id": "toolu_2", "name": "Task", "input": { "description": "x" } }] } });
        let result = json!({ "type": "user", "message": { "content": [{ "type": "tool_result", "tool_use_id": "toolu_2", "is_error": true, "content": "Agent terminated early due to an API error: API Error: Request rejected (429) \u{b7} rate limit" }] } });
        assert_eq!(limit_of(&run(&[task, result, done.clone()])), Some(None));

        // A background sub-agent's failure notification.
        let note = "<task-notification>\n<status>failed</status>\n<summary>Agent \"Review\" failed: Agent terminated early due to an API error: API Error: Request rejected (429) (error type rate_limit, HTTP 429)</summary>\n<result>Reading the context first</result>\n</task-notification>";
        let notified = json!({ "type": "user", "message": { "content": note } });
        assert_eq!(limit_of(&run(&[notified, done.clone()])), Some(None));

        // The next turn starts clean.
        assert_eq!(limit_of(&run(&[done])), None);
    }

    #[test]
    fn subagent_actions_show_on_its_task_card() {
        let task = json!({ "type": "assistant", "message": { "content": [{ "type": "tool_use", "id": "toolu_1", "name": "Task", "input": { "subagent_type": "Explore", "description": "Find it" } }] } });
        let read = json!({ "type": "assistant", "parent_tool_use_id": "toolu_1", "message": { "content": [{ "type": "text", "text": "Looking" }, { "type": "tool_use", "id": "toolu_s1", "name": "Read", "input": { "file_path": "/src/main.rs" } }] } });
        let grep = json!({ "type": "assistant", "parent_tool_use_id": "toolu_1", "message": { "content": [{ "type": "tool_use", "id": "toolu_s2", "name": "Grep", "input": { "pattern": "fn  main" } }] } });
        let result = json!({ "type": "user", "message": { "content": [{ "type": "tool_result", "tool_use_id": "toolu_1", "content": "Found it." }] } });
        let items: Vec<Value> = run(&[task, read, grep, result])
            .into_iter()
            .filter_map(|e| match e {
                ProviderEvent::Timeline { item, .. } => Some(item),
                _ => None,
            })
            .collect();
        assert_eq!(items.len(), 4);
        assert!(items.iter().all(|i| i["callId"] == "toolu_1"));
        assert_eq!(items[1]["status"], "running");
        assert_eq!(items[1]["detail"]["log"], "[Read] /src/main.rs");
        assert_eq!(items[2]["detail"]["log"], "[Read] /src/main.rs\n[Grep] fn main");
        assert_eq!(items[2]["detail"]["subAgentType"], "Explore");
        assert_eq!(items[3]["status"], "completed");
        assert_eq!(items[3]["detail"]["log"], "Found it.");
    }

    #[test]
    fn background_subagent_card_follows_it_until_it_reports() {
        let timeline = |events: Vec<ProviderEvent>| -> Vec<Value> {
            events
                .into_iter()
                .filter_map(|e| match e {
                    ProviderEvent::Timeline { item, .. } => Some(item),
                    _ => None,
                })
                .collect()
        };
        let task = json!({ "type": "assistant", "message": { "content": [{ "type": "tool_use", "id": "toolu_1", "name": "Agent", "input": { "description": "Port it", "run_in_background": true } }] } });
        let launched = json!({ "type": "user", "message": { "content": [{ "type": "tool_result", "tool_use_id": "toolu_1", "content": [{ "type": "text", "text": "Async agent launched successfully. (internal)\nagentId: a123" }] }] } });
        let done = json!({ "type": "result", "subtype": "success", "result": "Started." });
        let bash = json!({ "type": "assistant", "parent_tool_use_id": "toolu_1", "message": { "content": [{ "type": "tool_use", "id": "toolu_s1", "name": "Bash", "input": { "command": "ls src" } }] } });
        let note = "<task-notification>\n<task-id>a123</task-id>\n<tool-use-id>toolu_1</tool-use-id>\n<status>completed</status>\n<summary>Agent \"Port it\" finished</summary>\n<result>Ported.</result>\n<usage><tool_uses>1</tool_uses></usage>\n</task-notification>";
        let notified = json!({ "type": "user", "message": { "content": note } });
        let events = run(&[task.clone(), launched.clone(), done, bash.clone(), notified]);
        // The CLI acts on the notification in a turn of its own.
        assert!(events.iter().any(|e| matches!(e, ProviderEvent::BackgroundTurn { .. })));
        let items = timeline(events);
        assert_eq!(items.len(), 4);
        // The launch answer is internal metadata: the card shows no log yet.
        assert_eq!(items[1]["status"], "running");
        assert_eq!(items[1]["detail"]["log"], "");
        // After the turn ended, the agent's actions still reach its card.
        assert_eq!(items[2]["callId"], "toolu_1");
        assert_eq!(items[2]["status"], "running");
        assert_eq!(items[2]["detail"]["log"], "[Bash] ls src");
        // Its notification puts the final result on the card.
        assert_eq!(items[3]["status"], "completed");
        assert_eq!(items[3]["detail"]["log"], "Ported.");

        // A process that exits first fails the cards of the agents it was running.
        let (tx, mut rx) = mpsc::unbounded_channel();
        let (stdin, _keep) = mpsc::unbounded_channel();
        let mut st = StreamState::new("a1".into(), tx, stdin, Arc::new(AtomicBool::new(false)), Some("s1".into()));
        for line in [task, launched, bash] {
            st.handle_line(&line.to_string());
        }
        assert!(st.sub_active.lock().unwrap().running);
        st.process_ended(true);
        assert!(!st.sub_active.lock().unwrap().running);
        let mut events = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            events.push(ev);
        }
        let last = timeline(events).pop().expect("a card");
        assert_eq!(last["status"], "failed");
        assert_eq!(last["detail"]["log"], "[Bash] ls src");
    }

    #[test]
    fn background_shells_and_workflows_hold_the_process_until_they_report() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let (stdin, _keep) = mpsc::unbounded_channel();
        let mut st = StreamState::new("a1".into(), tx, stdin, Arc::new(AtomicBool::new(false)), Some("s1".into()));
        let lines = [
            json!({ "type": "assistant", "message": { "content": [
                { "type": "tool_use", "id": "toolu_w", "name": "Workflow", "input": { "script": "review.js" } },
                { "type": "tool_use", "id": "toolu_b", "name": "Bash", "input": { "command": "cargo build", "run_in_background": true } },
            ] } }),
            json!({ "type": "user", "message": { "content": [
                { "type": "tool_result", "tool_use_id": "toolu_w", "content": "Workflow started in the background (task id wf1)." },
                { "type": "tool_result", "tool_use_id": "toolu_b", "content": "Command running in background with ID: bgmz5vjgy. Output is being written to: /tmp/bgmz5vjgy.out" },
            ] } }),
            json!({ "type": "result", "subtype": "success", "result": "Started." }),
        ];
        for line in &lines {
            st.handle_line(&line.to_string());
        }
        let held = |st: &StreamState| {
            let sub = st.sub_active.lock().unwrap();
            (sub.running, sub.quiet_max)
        };
        assert_eq!(held(&st), (true, BACKGROUND_QUIET_MAX));
        assert!(st.sub_active.lock().unwrap().holding());
        let shell = "<task-notification>\n<task-id>bgmz5vjgy</task-id>\n<tool-use-id>toolu_b</tool-use-id>\n<status>completed</status>\n<summary>Background command \"cargo build\" completed (exit code 0)</summary>\n</task-notification>";
        st.handle_line(&json!({ "type": "user", "message": { "content": shell } }).to_string());
        // The workflow still runs.
        assert!(held(&st).0);
        let flow = "<task-notification>\n<task-id>wf1</task-id>\n<tool-use-id>toolu_w</tool-use-id>\n<status>stopped</status>\n<summary>Workflow stopped</summary>\n</task-notification>";
        st.handle_line(&json!({ "type": "user", "message": { "content": flow } }).to_string());
        assert!(!held(&st).0);
        let mut items = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            if let ProviderEvent::Timeline { item, .. } = ev {
                items.push(item);
            }
        }
        let cards = |id: &str| -> Vec<Value> { items.iter().filter(|i| i["callId"] == id).cloned().collect() };
        let (bash, flow) = (cards("toolu_b"), cards("toolu_w"));
        // Call, launch (still running), notification.
        assert_eq!(bash.len(), 3);
        assert_eq!(bash[1]["status"], "running");
        assert_eq!(bash[1]["detail"]["type"], "shell");
        assert!(bash[1]["detail"].get("log").is_none());
        assert_eq!(bash[2]["status"], "completed");
        assert_eq!(bash[2]["detail"]["output"], "Background command \"cargo build\" completed (exit code 0)");
        assert_eq!(flow.len(), 3);
        assert_eq!(flow[1]["status"], "running");
        assert_eq!(flow[2]["status"], "canceled");
    }

    #[test]
    fn foreground_results_are_not_taken_for_background_launches() {
        let call = |id: &str, name: &str, input: Value| json!({ "type": "assistant", "message": { "content": [{ "type": "tool_use", "id": id, "name": name, "input": input }] } });
        let result = |id: &str, text: &str| json!({ "type": "user", "message": { "content": [{ "type": "tool_result", "tool_use_id": id, "content": text }] } });
        let (tx, _rx) = mpsc::unbounded_channel();
        let (stdin, _keep) = mpsc::unbounded_channel();
        let mut st = StreamState::new("a1".into(), tx, stdin, Arc::new(AtomicBool::new(false)), Some("s1".into()));
        let lines = [
            call("t1", "Agent", json!({ "description": "Find it" })),
            result("t1", "Found it: the agent loop starts in src/main.rs at line 40.\nagentId: a1 (for resuming)"),
            call("t2", "Task", json!({ "description": "Deploy" })),
            result("t2", "Launched the server and checked it answers on :8080."),
            call("t3", "Bash", json!({ "command": "echo started in the background" })),
            result("t3", "started in the background"),
        ];
        for line in &lines {
            st.handle_line(&line.to_string());
        }
        assert!(!st.sub_active.lock().unwrap().running);
        assert!(st.background.is_empty());

        // A fork's launch answer counts, whatever its wording.
        assert!(launched_in_background("Agent", &json!({ "subagent_type": "fork" }), "Fork started.\nagentId: f1"));
        assert!(launched_in_background("Agent", &json!({}), "Async agent launched successfully.\nagentId: a1"));
        assert!(!launched_in_background("Agent", &json!({ "subagent_type": "fork" }), &format!("The agent finished: {}", "x".repeat(400))));
    }

    #[test]
    fn subagent_gate_ignores_background_shells() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let (stdin, mut out) = mpsc::unbounded_channel();
        let mut st = StreamState::new("a1".into(), tx, stdin, Arc::new(AtomicBool::new(false)), Some("s1".into()));
        st.sub_cap.store(1, Ordering::Relaxed);
        for id in ["b1", "b2"] {
            st.handle_line(&json!({ "type": "assistant", "message": { "content": [{ "type": "tool_use", "id": id, "name": "Bash", "input": { "command": "make", "run_in_background": true } }] } }).to_string());
            st.handle_line(&json!({ "type": "user", "message": { "content": [{ "type": "tool_result", "tool_use_id": id, "content": format!("Command running in background with ID: {id}.") }] } }).to_string());
        }
        assert_eq!(st.background.len(), 2);
        let hook = json!({ "type": "control_request", "request_id": "r1", "request": { "subtype": "hook_callback", "callback_id": SUBAGENT_HOOK, "input": { "hook_event_name": "PreToolUse", "tool_name": "Agent" }, "tool_use_id": "t1" } });
        st.handle_line(&hook.to_string());
        let resp: Value = serde_json::from_str(&out.try_recv().expect("an answer")).unwrap();
        assert!(resp.pointer("/response/response/hookSpecificOutput").is_none());
    }

    #[test]
    fn subagent_gate_caps_running_subagents() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let (stdin, mut out) = mpsc::unbounded_channel();
        let mut st = StreamState::new("a1".into(), tx, stdin, Arc::new(AtomicBool::new(false)), Some("s1".into()));
        st.sub_cap.store(2, Ordering::Relaxed);
        let mut gate = |st: &mut StreamState, id: &str| -> Value {
            let hook = json!({ "type": "control_request", "request_id": format!("r-{id}"), "request": { "subtype": "hook_callback", "callback_id": SUBAGENT_HOOK, "input": { "hook_event_name": "PreToolUse", "tool_name": "Agent" }, "tool_use_id": id } });
            st.handle_line(&hook.to_string());
            let resp: Value = serde_json::from_str(&out.try_recv().expect("an answer")).unwrap();
            assert_eq!(resp["response"]["request_id"], format!("r-{id}"));
            resp["response"]["response"].clone()
        };
        let denied = |v: &Value| v.pointer("/hookSpecificOutput/permissionDecision").and_then(|d| d.as_str()) == Some("deny");
        let launch = |st: &mut StreamState, id: &str| {
            st.handle_line(&json!({ "type": "assistant", "message": { "content": [{ "type": "tool_use", "id": id, "name": "Agent", "input": { "description": "Part", "run_in_background": true } }] } }).to_string());
            st.handle_line(&json!({ "type": "user", "message": { "content": [{ "type": "tool_result", "tool_use_id": id, "content": "Async agent launched successfully." }] } }).to_string());
        };
        assert!(!denied(&gate(&mut st, "t1")));
        launch(&mut st, "t1");
        assert!(!denied(&gate(&mut st, "t2")));
        launch(&mut st, "t2");
        // Two running: the third waits.
        assert!(denied(&gate(&mut st, "t3")));
        // One reports back: its slot is free again.
        let note = "<task-notification>\n<tool-use-id>t1</tool-use-id>\n<status>completed</status>\n<result>Done.</result>\n</task-notification>";
        st.handle_line(&json!({ "type": "user", "message": { "content": note } }).to_string());
        assert!(!denied(&gate(&mut st, "t3")));
        // Under pressure no new sub-agent starts, whatever is running.
        st.sub_cap.store(0, Ordering::Relaxed);
        assert!(denied(&gate(&mut st, "t4")));
    }
}
