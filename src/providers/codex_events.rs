//! Codex app-server notifications and server->client requests (approvals).

use super::{s, Codex};
use crate::providers::{tools, ProviderEvent};
use serde_json::{json, Value};
use tokio::sync::oneshot;

impl Codex {
    pub(super) fn handle_notification(&self, method: &str, params: &Value) {
        if method == "account/rateLimits/updated" {
            // Account-wide, so it names no thread.
            if let Some(limits) = params.get("rateLimits") {
                self.inner.state.lock().unwrap().rate_limits = Some(limits.clone());
            }
            return;
        }
        let Some(agent_id) = self.agent_for(params) else { return };
        match method {
            "turn/started" => {
                let turn_id = params.get("turn").and_then(|t| s(t, "id"));
                if let Some(entry) = self.inner.state.lock().unwrap().active.get_mut(&agent_id) {
                    entry.1 = turn_id.clone();
                }
                self.emit(ProviderEvent::TurnStarted { agent_id, turn_id });
            }
            "item/agentMessage/delta" | "item/reasoning/summaryTextDelta" | "item/reasoning/textDelta" => {
                let Some(delta) = s(params, "delta").filter(|d| !d.is_empty()) else { return };
                if let Some(item_id) = s(params, "itemId") {
                    self.inner.state.lock().unwrap().streamed_items.insert(item_id);
                }
                let kind = if method == "item/agentMessage/delta" { "assistant_message" } else { "reasoning" };
                self.emit(ProviderEvent::Timeline { agent_id, item: json!({ "type": kind, "text": delta }) });
            }
            "item/started" | "item/completed" => {
                let Some(item) = params.get("item") else { return };
                let completed = method == "item/completed";
                let item_id = s(item, "id").unwrap_or_default();
                match item.get("type").and_then(|t| t.as_str()).unwrap_or("") {
                    "agentMessage" | "reasoning" if completed => {
                        let was_streamed = self.inner.state.lock().unwrap().streamed_items.remove(&item_id);
                        if was_streamed {
                            return;
                        }
                        let (kind, text) = if item.get("type").and_then(|t| t.as_str()) == Some("agentMessage") {
                            ("assistant_message", s(item, "text").unwrap_or_default())
                        } else {
                            let join = |k: &str| item.get(k).and_then(|v| v.as_array()).map(|a| a.iter().filter_map(|x| x.as_str()).collect::<Vec<_>>().join("\n")).unwrap_or_default();
                            let summary = join("summary");
                            ("reasoning", if summary.is_empty() { join("content") } else { summary })
                        };
                        if !text.is_empty() {
                            self.emit(ProviderEvent::Timeline { agent_id, item: json!({ "type": kind, "text": text }) });
                        }
                    }
                    "contextCompaction" if completed => {
                        self.emit(ProviderEvent::Timeline { agent_id, item: json!({ "type": "compaction", "status": "completed" }) });
                    }
                    _ => {
                        if let Some(tool) = tools::codex_tool_item(item, completed) {
                            self.emit(ProviderEvent::Timeline { agent_id, item: tool });
                        }
                    }
                }
            }
            "turn/plan/updated" => {
                let Some(plan) = params.get("plan").and_then(|p| p.as_array()) else { return };
                let items: Vec<Value> = plan
                    .iter()
                    .map(|p| {
                        let status = match s(p, "status").as_deref() {
                            Some("completed") => "completed",
                            Some("inProgress") | Some("in_progress") => "in_progress",
                            _ => "pending",
                        };
                        json!({ "text": s(p, "step").unwrap_or_default(), "completed": status == "completed", "status": status })
                    })
                    .collect();
                self.emit(ProviderEvent::Timeline { agent_id, item: json!({ "type": "todo", "items": items }) });
            }
            "thread/tokenUsage/updated" => {
                let usage = params.get("tokenUsage").or_else(|| params.get("usage")).cloned().unwrap_or(Value::Null);
                let total = usage.get("total").unwrap_or(&usage);
                let thread = s(params, "threadId").or_else(|| s(params, "conversationId")).unwrap_or_default();
                if let Some(sample) = crate::usage::codex_sample(&thread, total) {
                    self.emit(ProviderEvent::Spend { agent_id: agent_id.clone(), sample });
                }
                let pick = |k: &str| total.get(k).cloned().unwrap_or(Value::Null);
                let usage = json!({
                    "inputTokens": pick("inputTokens"),
                    "cachedInputTokens": pick("cachedInputTokens"),
                    "outputTokens": pick("outputTokens"),
                    "contextWindowMaxTokens": usage.get("modelContextWindow").cloned().unwrap_or(Value::Null),
                    // `last` is the most recent model call: its prompt is the live context.
                    "contextWindowUsedTokens": usage.get("last").and_then(|l| l.get("totalTokens").or_else(|| l.get("total_tokens"))).cloned().unwrap_or(Value::Null),
                });
                self.emit(ProviderEvent::Usage { agent_id, usage });
            }
            "thread/compacted" => {
                // Compaction normally runs as a turn and ends with turn/completed.
                // If no turn was announced for a compaction we started, this is
                // the only completion signal, so finish it here.
                let finished = {
                    let mut st = self.inner.state.lock().unwrap();
                    let unannounced = st.active.get(&agent_id).filter(|(_, turn)| turn.is_none()).map(|(thread, _)| thread.clone());
                    if let Some(thread) = &unannounced {
                        st.active.remove(&agent_id);
                        st.stray_turn_end.insert(thread.clone());
                    }
                    unannounced.is_some()
                };
                if finished {
                    self.emit(ProviderEvent::TurnCompleted { agent_id });
                }
            }
            "turn/completed" => {
                let turn = params.get("turn").cloned().unwrap_or(Value::Null);
                let turn_id = s(&turn, "id");
                // Only end the turn we are tracking: a late completion of an
                // earlier turn (e.g. a compaction already finished via
                // thread/compacted) must not end the prompt running now.
                let owned = {
                    let mut st = self.inner.state.lock().unwrap();
                    let thread = s(params, "threadId").unwrap_or_default();
                    let stray = st.stray_turn_end.remove(&thread);
                    let stale = match (st.active.get(&agent_id), &turn_id) {
                        (Some((_, Some(cur))), Some(done)) => cur != done,
                        // The compaction already ended via thread/compacted; this
                        // is its late completion, not the prompt now waiting.
                        (Some((_, None)), _) => stray,
                        _ => false,
                    };
                    !stale && st.active.remove(&agent_id).is_some()
                };
                if !owned {
                    return;
                }
                match s(&turn, "status").as_deref() {
                    Some("failed") => {
                        let error = turn.get("error").and_then(|e| s(e, "message")).unwrap_or_else(|| "turn failed".into());
                        let info = turn.get("error").and_then(|e| e.get("codexErrorInfo"));
                        let typed = info.map(|i| i.as_str() == Some("usageLimitExceeded") || i.get("usageLimitExceeded").is_some()).unwrap_or(false);
                        if typed || crate::providers::is_usage_limit(&error) {
                            let resets_at = self.inner.state.lock().unwrap().rate_limits.as_ref().and_then(exhausted_reset);
                            self.emit(ProviderEvent::LimitHit { agent_id: agent_id.clone(), resets_at });
                        }
                        self.emit(ProviderEvent::TurnFailed { agent_id, error });
                    }
                    Some("interrupted") => self.emit(ProviderEvent::TurnCanceled { agent_id, reason: "interrupted".into() }),
                    _ => self.emit(ProviderEvent::TurnCompleted { agent_id }),
                }
            }
            _ => {}
        }
    }

    pub(super) fn handle_server_request(&self, id: Value, method: &str, params: Value) {
        if method == "mcpServer/elicitation/request" {
            return self.handle_elicitation(id, params);
        }
        let is_approval = matches!(
            method,
            "item/commandExecution/requestApproval" | "item/fileChange/requestApproval" | "execCommandApproval" | "applyPatchApproval"
        );
        let agent_id = self.agent_for(&params);
        let (true, Some(agent_id)) = (is_approval, agent_id) else {
            self.send_raw(json!({ "id": id, "error": { "code": -32601, "message": format!("{method} is not supported by paseo-lite") } }));
            return;
        };
        let legacy = !method.starts_with("item/");
        let is_command = method.contains("ommand") || method == "execCommandApproval";
        let command = match params.get("command") {
            Some(Value::Array(parts)) => parts.iter().filter_map(|p| p.as_str()).collect::<Vec<_>>().join(" "),
            Some(Value::String(c)) => c.clone(),
            _ => String::new(),
        };
        let (name, detail) = if is_command {
            ("shell", json!({ "type": "shell", "command": command, "cwd": s(&params, "cwd") }))
        } else {
            ("apply_patch", json!({ "type": "plain_text", "label": "Apply file changes", "text": s(&params, "reason").unwrap_or_default() }))
        };
        let request = json!({
            "id": format!("perm-{}", crate::util::new_id()),
            "provider": "codex",
            "name": name,
            "kind": "tool",
            "title": if is_command { "Run command" } else { "Edit files" },
            "description": s(&params, "reason"),
            "input": params,
            "detail": detail,
        });
        let (tx, rx) = oneshot::channel::<Value>();
        self.inner.state.lock().unwrap().open_approvals += 1;
        self.emit(ProviderEvent::Permission { agent_id, request, reply: tx });
        let this = self.clone();
        tokio::spawn(async move {
            let answer = rx.await.unwrap_or_else(|_| json!({ "behavior": "deny", "interrupt": true }));
            let allow = answer.get("behavior").and_then(|b| b.as_str()) == Some("allow");
            let interrupt = answer.get("interrupt").and_then(|i| i.as_bool()).unwrap_or(false);
            let decision = match (legacy, allow, interrupt) {
                (true, true, _) => "approved",
                (true, false, true) => "abort",
                (true, false, false) => "denied",
                (false, true, _) => "accept",
                (false, false, true) => "cancel",
                (false, false, false) => "decline",
            };
            this.send_raw(json!({ "id": id, "result": { "decision": decision } }));
            let mut st = this.inner.state.lock().unwrap();
            st.open_approvals = st.open_approvals.saturating_sub(1);
        });
    }

    /// An MCP server (or Codex, to approve an MCP tool call) asks the user
    /// something. One with no fields to fill in is shown as a permission
    /// prompt; forms and URLs, which the app can't show, are declined so the
    /// turn never stalls.
    fn handle_elicitation(&self, id: Value, params: Value) {
        let request = params.get("request").unwrap_or(&params);
        let server = s(&params, "serverName").unwrap_or_else(|| "mcp".into());
        let message = s(request, "message").unwrap_or_default();
        let fields = request.pointer("/requestedSchema/properties").and_then(|p| p.as_object()).map(|p| !p.is_empty()).unwrap_or(false);
        let url = s(request, "mode").as_deref() == Some("url");
        let agent_id = self.agent_for(&params);
        let (false, false, Some(agent_id)) = (fields, url, agent_id) else {
            crate::log!("codex: declined an MCP request from {server:?} the app can't show: {}", crate::util::truncate(&message, 200));
            self.send_raw(json!({ "id": id, "result": { "action": "decline", "content": null } }));
            return;
        };
        let request = json!({
            "id": format!("perm-{}", crate::util::new_id()),
            "provider": "codex",
            "name": format!("mcp__{server}"),
            "kind": "tool",
            "title": format!("MCP: {server}"),
            "description": message,
            "input": params,
            "detail": { "type": "plain_text", "label": format!("MCP server {server}"), "text": message },
        });
        let (tx, rx) = oneshot::channel::<Value>();
        self.inner.state.lock().unwrap().open_approvals += 1;
        self.emit(ProviderEvent::Permission { agent_id, request, reply: tx });
        let this = self.clone();
        tokio::spawn(async move {
            let answer = rx.await.unwrap_or_else(|_| json!({ "behavior": "deny", "interrupt": true }));
            let allow = answer.get("behavior").and_then(|b| b.as_str()) == Some("allow");
            let interrupt = answer.get("interrupt").and_then(|i| i.as_bool()).unwrap_or(false);
            let result = match (allow, interrupt) {
                (true, _) => json!({ "action": "accept", "content": {} }),
                (false, true) => json!({ "action": "cancel", "content": null }),
                (false, false) => json!({ "action": "decline", "content": null }),
            };
            this.send_raw(json!({ "id": id, "result": result }));
            let mut st = this.inner.state.lock().unwrap();
            st.open_approvals = st.open_approvals.saturating_sub(1);
        });
    }
}

/// When the used-up rate-limit window resets (unix seconds): the latest of
/// the windows at 100%, since all of them must clear.
fn exhausted_reset(limits: &Value) -> Option<u64> {
    ["primary", "secondary"]
        .iter()
        .filter_map(|k| limits.get(*k))
        .filter(|w| w.get("usedPercent").and_then(|p| p.as_f64()).unwrap_or(0.0) >= 100.0)
        .filter_map(|w| w.get("resetsAt").and_then(|t| t.as_u64()))
        .max()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reset_comes_from_the_used_up_window() {
        let limits = json!({ "primary": { "usedPercent": 100.0, "windowDurationMins": 300, "resetsAt": 1_760_000_000u64 }, "secondary": { "usedPercent": 40.0, "resetsAt": 1_760_500_000u64 } });
        assert_eq!(exhausted_reset(&limits), Some(1_760_000_000));
        let both = json!({ "primary": { "usedPercent": 100, "resetsAt": 1_760_000_000u64 }, "secondary": { "usedPercent": 100, "resetsAt": 1_760_500_000u64 } });
        assert_eq!(exhausted_reset(&both), Some(1_760_500_000));
        assert_eq!(exhausted_reset(&json!({ "primary": { "usedPercent": 20, "resetsAt": 1 } })), None);
    }
}
