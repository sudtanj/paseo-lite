//! Session RPC handlers for the Paseo core protocol subset.

use crate::manager::{reply, wrap, Agent, Cmd, Manager, Out};
use crate::providers::{binary_available, claude_models, is_supported, modes_for};
use crate::snapshot::{cursor, project_descriptor, timeline_entry};
use crate::store::AgentRecord;
use crate::util::{new_id, now_iso};
use serde_json::{json, Value};
use std::collections::BTreeMap;

pub(crate) fn s(v: &Value, key: &str) -> Option<String> {
    v.get(key).and_then(|x| x.as_str()).map(|x| x.to_string())
}

fn expand_home(path: &str) -> String {
    if let Some(rest) = path.strip_prefix("~") {
        if let Ok(home) = std::env::var("HOME") {
            return format!("{home}{rest}");
        }
    }
    path.to_string()
}

pub(crate) fn normalize_dir(path: &str) -> Result<String, String> {
    let expanded = expand_home(path.trim());
    let canon = std::fs::canonicalize(&expanded).map_err(|e| format!("{expanded}: {e}"))?;
    if !canon.is_dir() {
        return Err(format!("{expanded} is not a directory"));
    }
    Ok(canon.to_string_lossy().into_owned())
}

impl Manager {
    pub(crate) fn resolve(&self, key: &str) -> Option<String> {
        if self.agents.contains_key(key) {
            return Some(key.to_string());
        }
        let by_prefix: Vec<&String> = self.agents.keys().filter(|id| id.starts_with(key)).collect();
        if by_prefix.len() == 1 && key.len() >= 4 {
            return Some(by_prefix[0].clone());
        }
        self.agents.values().find(|a| a.rec.title.as_deref() == Some(key)).map(|a| a.rec.id.clone())
    }

    fn directory_entries(&self, include_archived: bool) -> Vec<Value> {
        let mut list: Vec<&Agent> = self.agents.values().filter(|a| include_archived || a.rec.archived_at.is_none()).collect();
        list.sort_by(|a, b| b.rec.updated_at.cmp(&a.rec.updated_at));
        list.iter().map(|a| json!({ "agent": a.snapshot(), "project": a.project() })).collect()
    }

    pub fn handle_rpc(&mut self, msg: Value, out: &Out) {
        let kind = s(&msg, "type").unwrap_or_default();
        let request_id = s(&msg, "requestId");
        let rid = request_id.clone().unwrap_or_default();
        let agent_key = s(&msg, "agentId").unwrap_or_default();
        match kind.as_str() {
            "fetch_agents_request" | "fetch_agent_history_request" => {
                let include_archived = kind == "fetch_agent_history_request" || msg.pointer("/filter/includeArchived").and_then(|v| v.as_bool()).unwrap_or(false);
                let entries = self.directory_entries(include_archived);
                let response_type = kind.replace("_request", "_response");
                let mut payload = json!({ "requestId": rid, "entries": entries, "pageInfo": { "nextCursor": null, "prevCursor": null, "hasMore": false } });
                if msg.get("subscribe").map(|v| !v.is_null()).unwrap_or(false) {
                    // Every session already receives all agent updates.
                    payload["subscriptionId"] = json!(msg.pointer("/subscribe/subscriptionId").and_then(|v| v.as_str()).map(String::from).unwrap_or_else(new_id));
                }
                reply(out, json!({ "type": response_type, "payload": payload }));
            }
            "fetch_agent_request" => {
                let found = self.resolve(&agent_key).and_then(|id| self.agents.get(&id));
                let payload = match found {
                    Some(a) => json!({ "requestId": rid, "agent": a.snapshot(), "project": a.project(), "error": null }),
                    None => json!({ "requestId": rid, "agent": null, "error": "agent not found" }),
                };
                reply(out, json!({ "type": "fetch_agent_response", "payload": payload }));
            }
            "create_agent_request" => self.create_agent(&msg, &rid, out),
            "send_agent_message_request" | "send_agent_message" => {
                // Images and attachments are saved and described in the prompt (see uploads.rs).
                let text = crate::uploads::build_prompt(&self.cfg.home, &s(&msg, "text").unwrap_or_default(), msg.get("images"), msg.get("attachments"));
                let result = match self.resolve(&agent_key) {
                    Some(id) if !text.trim().is_empty() => self.submit_prompt(&id, text, s(&msg, "messageId")).map(|_| id),
                    Some(_) => Err("empty message".into()),
                    None => Err("agent not found".into()),
                };
                if kind == "send_agent_message_request" {
                    let (accepted, error) = match &result {
                        Ok(_) => (true, Value::Null),
                        Err(e) => (false, json!(e)),
                    };
                    let agent_id = result.clone().unwrap_or(agent_key);
                    reply(out, json!({ "type": "send_agent_message_response", "payload": { "requestId": rid, "agentId": agent_id, "accepted": accepted, "error": error } }));
                }
            }
            "cancel_agent_request" => {
                let id = self.resolve(&agent_key);
                if let Some(id) = &id {
                    self.cancel(id);
                }
                if request_id.is_some() {
                    let agent = id.as_ref().and_then(|i| self.agents.get(i)).map(|a| a.snapshot());
                    let error = if id.is_none() { json!("agent not found") } else { Value::Null };
                    reply(out, json!({ "type": "cancel_agent_response", "payload": { "requestId": rid, "agentId": agent_key, "agent": agent, "error": error } }));
                }
            }
            "agent_permission_response" => {
                // The app waits for `agent_permission_resolved`; say why it won't come.
                let Some(id) = self.resolve(&agent_key) else { return rpc_error(out, &rid, &kind, "agent not found") };
                let response = msg.get("response").cloned().unwrap_or_else(|| json!({ "behavior": "deny" }));
                if let Err(e) = self.respond_permission(&id, &rid, response) {
                    rpc_error(out, &rid, &kind, e);
                }
            }
            "archive_agent_request" => {
                let Some(id) = self.resolve(&agent_key) else {
                    return rpc_error(out, &rid, &kind, "agent not found");
                };
                self.release_runtime(&id);
                let now = now_iso();
                if let Some(a) = self.agents.get_mut(&id) {
                    a.rec.archived_at = Some(now.clone());
                    a.rec.attention_reason = None;
                }
                self.touch(&id);
                self.publish_agent(&id);
                reply(out, json!({ "type": "agent_archived", "payload": { "agentId": id, "archivedAt": now, "requestId": rid } }));
            }
            "delete_agent_request" => {
                let Some(id) = self.resolve(&agent_key) else {
                    return rpc_error(out, &rid, &kind, "agent not found");
                };
                self.release_runtime(&id);
                let workspace = self.agents.remove(&id).and_then(|a| a.rec.workspace_id);
                self.store.delete_timeline(&id);
                // Its spend stays in the provider totals.
                self.usage.forget_agent(&id);
                self.agents_dirty = true;
                self.broadcast(json!({ "type": "agent_update", "payload": { "kind": "remove", "agentId": id } }));
                self.broadcast(json!({ "type": "agent_deleted", "payload": { "agentId": id, "requestId": rid } }));
                if let Some(ws) = workspace {
                    self.publish_workspace(&ws);
                }
            }
            "clear_agent_attention" => {
                let ids: Vec<String> = match msg.get("agentId") {
                    Some(Value::Array(list)) => list.iter().filter_map(|v| v.as_str().map(String::from)).collect(),
                    Some(Value::String(one)) => vec![one.clone()],
                    _ => vec![],
                };
                for key in ids {
                    if let Some(id) = self.resolve(&key) {
                        if let Some(a) = self.agents.get_mut(&id) {
                            a.rec.attention_reason = None;
                            a.rec.attention_at = None;
                        }
                        self.agents_dirty = true;
                        self.publish_agent(&id);
                    }
                }
                if request_id.is_some() {
                    reply(out, json!({ "type": "clear_agent_attention_response", "payload": { "requestId": rid, "agentId": msg.get("agentId").cloned().unwrap_or(Value::Null), "ok": true } }));
                }
            }
            "fetch_agent_timeline_request" => self.fetch_timeline(&msg, &agent_key, &rid, out),
            "set_agent_mode_request" | "set_agent_model_request" => {
                let response_type = kind.replace("_request", "_response");
                let Some(id) = self.resolve(&agent_key) else {
                    return reply(out, json!({ "type": response_type, "payload": { "requestId": rid, "agentId": agent_key, "accepted": false, "error": "agent not found" } }));
                };
                if kind == "set_agent_mode_request" {
                    let mode = s(&msg, "modeId").unwrap_or_default();
                    let provider = self.agents[&id].rec.provider.clone();
                    if provider == "claude" {
                        self.claude.set_mode(&id, &mode);
                    }
                    if let Some(a) = self.agents.get_mut(&id) {
                        a.rec.mode_id = Some(mode);
                    }
                } else {
                    let model = s(&msg, "modelId");
                    if let Some(a) = self.agents.get_mut(&id) {
                        a.rec.model = model;
                    }
                    self.claude.restart_on_next_prompt(&id);
                }
                self.touch(&id);
                self.publish_agent(&id);
                reply(out, json!({ "type": response_type, "payload": { "requestId": rid, "agentId": id, "accepted": true, "error": null } }));
            }
            "list_provider_models_request" => {
                let provider = s(&msg, "provider").unwrap_or_default();
                let out = out.clone();
                let ov = self.daemon_config.overrides();
                if provider == "codex" && !self.codex_may_start() {
                    // Claude Code holds the machine: answer from the cache, never start Codex.
                    let (models, error) = match self.codex.cached_models() {
                        Some(m) => (m, Value::Null),
                        None => (json!([]), json!("Codex models load the first time Codex runs")),
                    };
                    reply(&out, json!({ "type": "list_provider_models_response", "payload": { "provider": "codex", "models": crate::snapshot::clean(ov.merge_models("codex", models)), "error": error, "fetchedAt": now_iso(), "requestId": rid } }));
                } else if provider == "codex" {
                    let codex = self.codex.clone();
                    tokio::spawn(async move {
                        let (models, error) = match codex.list_models().await {
                            Ok(m) => (m, Value::Null),
                            Err(e) => (json!([]), json!(e)),
                        };
                        reply(&out, json!({ "type": "list_provider_models_response", "payload": { "provider": "codex", "models": crate::snapshot::clean(ov.merge_models("codex", models)), "error": error, "fetchedAt": now_iso(), "requestId": rid } }));
                    });
                } else {
                    reply(&out, json!({ "type": "list_provider_models_response", "payload": { "provider": provider, "models": ov.merge_models("claude", claude_models()), "error": null, "fetchedAt": now_iso(), "requestId": rid } }));
                }
            }
            "list_provider_modes_request" => {
                let provider = s(&msg, "provider").unwrap_or_default();
                reply(out, json!({ "type": "list_provider_modes_response", "payload": { "provider": provider, "modes": modes_for(&provider), "error": null, "fetchedAt": now_iso(), "requestId": rid } }));
            }
            "list_available_providers_request" => {
                let providers = json!([
                    { "provider": "claude", "available": binary_available(&self.cfg.claude_bin) },
                    { "provider": "codex", "available": binary_available(&self.cfg.codex_bin) },
                ]);
                reply(out, json!({ "type": "list_available_providers_response", "payload": { "providers": providers, "error": null, "fetchedAt": now_iso(), "requestId": rid } }));
            }
            "get_providers_snapshot_request" => {
                // Answer right away with whatever models are known; a cold Codex
                // model list (app-server start + gateway call) can take longer
                // than the app waits, so it is fetched in the background and
                // pushed as `providers_snapshot_update`.
                let cached = self.codex.cached_models();
                let mut payload = json!({ "entries": self.provider_entries(cached.clone()), "generatedAt": now_iso(), "requestId": rid });
                if let Some(cwd) = s(&msg, "cwd") {
                    payload["cwd"] = json!(cwd);
                }
                reply(out, json!({ "type": "get_providers_snapshot_response", "payload": payload }));
                if cached.is_none() {
                    self.push_codex_models();
                }
            }
            "refresh_providers_snapshot_request" => {
                reply(out, json!({ "type": "refresh_providers_snapshot_response", "payload": { "requestId": rid, "acknowledged": true } }));
                if self.codex_may_start() {
                    self.codex.clear_models();
                    self.push_codex_models();
                }
            }
            "fetch_workspaces_request" => {
                let project = msg.pointer("/filter/projectId").and_then(|v| v.as_str()).map(String::from);
                let entries = self.workspace_entries(project.as_deref());
                let mut payload = json!({ "requestId": rid, "entries": entries, "emptyProjects": self.empty_projects(), "pageInfo": { "nextCursor": null, "prevCursor": null, "hasMore": false } });
                // Every session already receives every workspace_update.
                if let Some(sub) = msg.get("subscribe").filter(|v| !v.is_null()) {
                    payload["subscriptionId"] = json!(sub.get("subscriptionId").and_then(|v| v.as_str()).map(String::from).unwrap_or_else(new_id));
                }
                reply(out, json!({ "type": "fetch_workspaces_response", "payload": payload }));
            }
            "workspace.create.request" => {
                let source = msg.get("source").cloned().unwrap_or(Value::Null);
                let fail = |error: String, code: &str| json!({ "type": "workspace.create.response", "payload": { "requestId": rid, "workspace": null, "setupTerminalId": null, "error": error, "errorCode": code } });
                if s(&source, "kind").as_deref() != Some("directory") {
                    return reply(out, fail("paseo-lite does not create git worktrees yet; choose Local isolation".into(), "unsupported"));
                }
                let dir = match normalize_dir(&s(&source, "path").unwrap_or_default()) {
                    Ok(d) => d,
                    Err(e) => return reply(out, fail(format!("Directory not found: {e}"), "directory_not_found")),
                };
                // projectId is the project root path; fall back to the directory itself.
                let root = s(&source, "projectId").filter(|p| self.projects.iter().any(|r| &r.root_path == p)).unwrap_or_else(|| dir.clone());
                if !self.projects.iter().any(|r| r.root_path == root) {
                    self.register_project(&root, false);
                }
                let id = self.create_workspace(&root, &dir);
                self.publish_workspace(&id);
                crate::log!("workspace {id} created in {dir}");
                reply(out, json!({ "type": "workspace.create.response", "payload": { "requestId": rid, "workspace": self.workspace_json(&id), "setupTerminalId": null, "error": null } }));
            }
            "archive_workspace_request" => self.archive_workspace_request(&msg, &rid, out),
            "workspace.title.set.request" => {
                let id = s(&msg, "workspaceId").unwrap_or_default();
                let title = s(&msg, "title").map(|t| t.trim().to_string()).filter(|t| !t.is_empty());
                let payload = match self.update_workspace(&id, |w| w.title = title.clone()) {
                    Some(w) => json!({ "requestId": rid, "workspaceId": id, "accepted": true, "title": w.title, "error": null }),
                    None => json!({ "requestId": rid, "workspaceId": id, "accepted": false, "title": null, "error": "workspace not found" }),
                };
                reply(out, json!({ "type": "workspace.title.set.response", "payload": payload }));
            }
            "workspace.pin.set.request" => {
                let id = s(&msg, "workspaceId").unwrap_or_default();
                let pinned = msg.get("pinned").and_then(|v| v.as_bool()).unwrap_or(false);
                let payload = match self.update_workspace(&id, |w| w.pinned_at = if pinned { Some(now_iso()) } else { None }) {
                    Some(w) => json!({ "requestId": rid, "workspaceId": id, "accepted": true, "pinnedAt": w.pinned_at, "error": null }),
                    None => json!({ "requestId": rid, "workspaceId": id, "accepted": false, "pinnedAt": null, "error": "workspace not found" }),
                };
                reply(out, json!({ "type": "workspace.pin.set.response", "payload": payload }));
            }
            "workspace.clear_attention.request" => {
                let ids: Vec<String> = match msg.get("workspaceId") {
                    Some(Value::Array(list)) => list.iter().filter_map(|v| v.as_str().map(String::from)).collect(),
                    Some(Value::String(one)) => vec![one.clone()],
                    _ => vec![],
                };
                let mut results = Vec::new();
                let mut all_cleared: Vec<String> = Vec::new();
                for ws in &ids {
                    let cleared: Vec<String> = self.agents.values().filter(|a| a.rec.workspace_id.as_deref() == Some(ws.as_str()) && a.rec.attention_reason.is_some()).map(|a| a.rec.id.clone()).collect();
                    for agent_id in &cleared {
                        if let Some(a) = self.agents.get_mut(agent_id) {
                            a.rec.attention_reason = None;
                            a.rec.attention_at = None;
                        }
                        self.agents_dirty = true;
                        self.publish_agent(agent_id);
                    }
                    results.push(json!({ "workspaceId": ws, "clearedAgentIds": cleared, "success": true, "error": null }));
                    all_cleared.extend(cleared);
                }
                reply(out, json!({ "type": "workspace.clear_attention.response", "payload": { "requestId": rid, "workspaceId": msg.get("workspaceId").cloned().unwrap_or(Value::Null), "clearedAgentIds": all_cleared, "results": results, "success": true, "error": null } }));
            }
            "workspace_setup_status_request" => {
                // No setup scripts in paseo-lite: report "nothing to show".
                reply(out, json!({ "type": "workspace_setup_status_response", "payload": { "requestId": rid, "workspaceId": s(&msg, "workspaceId").unwrap_or_default(), "snapshot": null } }));
            }
            "project.list.request" => {
                let mut paths: Vec<String> = self.projects.iter().map(|p| p.root_path.clone()).collect();
                for a in self.agents.values().filter(|a| a.rec.archived_at.is_none()) {
                    if !paths.contains(&a.rec.cwd) {
                        paths.push(a.rec.cwd.clone());
                    }
                }
                let projects: Vec<Value> = paths.iter().map(|p| project_descriptor(p)).collect();
                reply(out, json!({ "type": "project.list.response", "payload": { "requestId": rid, "projects": projects } }));
            }
            "project.add.request" => {
                let requested = s(&msg, "cwd").unwrap_or_default();
                let payload = match normalize_dir(&requested) {
                    Ok(cwd) => json!({ "requestId": rid, "project": self.register_project(&cwd, false), "error": null }),
                    Err(e) => json!({ "requestId": rid, "project": null, "error": format!("Directory not found: {e}"), "errorCode": "directory_not_found" }),
                };
                reply(out, json!({ "type": "project.add.response", "payload": payload }));
            }
            "project.create_directory.request" => {
                let parent = s(&msg, "parentPath").unwrap_or_default();
                let name = s(&msg, "name").unwrap_or_default();
                let payload = match crate::projects::create_directory(&parent, &name) {
                    Ok(path) => json!({ "requestId": rid, "directoryPath": path, "project": self.register_project(&path, true), "error": null, "errorCode": null }),
                    Err((code, error)) => json!({ "requestId": rid, "directoryPath": null, "project": null, "error": error, "errorCode": code }),
                };
                reply(out, json!({ "type": "project.create_directory.response", "payload": payload }));
            }
            "workspace.github.search_repositories.request" => {
                let query = s(&msg, "query").unwrap_or_default();
                let limit = msg.get("limit").and_then(|v| v.as_u64()).unwrap_or(20).clamp(1, 50);
                let out = out.clone();
                let rid = rid.clone();
                tokio::spawn(async move {
                    let payload = crate::projects::search_repositories(&query, limit, &rid).await;
                    reply(&out, json!({ "type": "workspace.github.search_repositories.response", "payload": payload }));
                });
            }
            "project.github.clone.request" => {
                let repo = s(&msg, "repo").unwrap_or_default();
                let protocol = s(&msg, "cloneProtocol").unwrap_or_else(|| "https".into());
                let target = expand_home(s(&msg, "targetDirectory").unwrap_or_default().trim());
                let out = out.clone();
                let cmds = self.cmds_tx.clone();
                let rid = rid.clone();
                tokio::spawn(async move {
                    match crate::projects::clone_repository(&repo, &protocol, &target).await {
                        Ok((display, path)) => {
                            let response = json!({ "type": "project.github.clone.response", "payload": { "requestId": rid, "repo": display, "checkoutPath": path, "project": null, "error": null } });
                            let _ = cmds.send(Cmd::RegisterProject { path, out, response });
                        }
                        Err((display, path, error)) => {
                            crate::warn!("github clone of {repo} failed: {error}");
                            reply(&out, json!({ "type": "project.github.clone.response", "payload": { "requestId": rid, "repo": display, "checkoutPath": path, "project": null, "error": error } }));
                        }
                    }
                });
            }
            "directory_suggestions_request" => {
                let query = s(&msg, "query").unwrap_or_default();
                let limit = msg.get("limit").and_then(|v| v.as_u64()).unwrap_or(30) as usize;
                let dirs = suggest_dirs(&query, limit);
                let entries: Vec<Value> = dirs.iter().map(|d| json!({ "path": d, "kind": "directory" })).collect();
                reply(out, json!({ "type": "directory_suggestions_response", "payload": { "directories": dirs, "entries": entries, "error": null, "requestId": rid } }));
            }
            "checkout_status_request" => {
                let (out, rid, cwd) = (out.clone(), rid.clone(), s(&msg, "cwd").unwrap_or_default());
                tokio::spawn(async move {
                    reply(&out, json!({ "type": "checkout_status_response", "payload": crate::checkout::status(&cwd, &rid).await }));
                });
            }
            "checkout.diff.get.request" => {
                let (out, rid, cwd) = (out.clone(), rid.clone(), s(&msg, "cwd").unwrap_or_default());
                let compare = msg.get("compare").cloned().unwrap_or_else(|| json!({ "mode": "uncommitted" }));
                tokio::spawn(async move {
                    reply(&out, json!({ "type": "checkout.diff.get.response", "payload": crate::checkout::get(&cwd, &compare, &rid).await }));
                });
            }
            "subscribe_checkout_diff_request" => {
                let sub_id = s(&msg, "subscriptionId").unwrap_or_else(new_id);
                let compare = msg.get("compare").cloned().unwrap_or_else(|| json!({ "mode": "uncommitted" }));
                let cwd = s(&msg, "cwd").unwrap_or_default();
                tokio::spawn(crate::checkout::subscribe(self.checkout.clone(), out.clone(), rid.clone(), sub_id, cwd, compare));
            }
            "unsubscribe_checkout_diff_request" => {
                if let Some(id) = s(&msg, "subscriptionId") {
                    self.checkout.unsubscribe(&id);
                }
            }
            "daemon.get_pairing_offer.request" => {
                let (url, qr) = match &self.pairing_url {
                    Some(u) => {
                        let svg = qrcode::QrCode::new(u.as_bytes()).ok().map(|c| c.render::<qrcode::render::svg::Color>().min_dimensions(240, 240).quiet_zone(true).build());
                        (u.clone(), svg)
                    }
                    None => (String::new(), None),
                };
                reply(out, json!({ "type": "daemon.get_pairing_offer.response", "payload": { "requestId": rid, "url": url, "qr": qr, "relayEnabled": self.pairing_url.is_some() } }));
            }
            "client_heartbeat" => self.client_heartbeat(&msg, out),
            "register_push_token" => {
                if let Some(token) = s(&msg, "token") {
                    self.push_tokens.renew(&token);
                }
            }
            "push.unregister.request" => {
                if let Some(token) = s(&msg, "token") {
                    self.push_tokens.revoke(&token);
                }
                reply(out, json!({ "type": "push.unregister.response", "payload": { "requestId": rid } }));
            }
            "abort_request" => {}
            "ping" => {
                let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0);
                let mut payload = json!({ "requestId": rid, "serverReceivedAt": now, "serverSentAt": now });
                if let Some(sent) = msg.get("clientSentAt") {
                    payload["clientSentAt"] = sent.clone();
                }
                reply(out, json!({ "type": "pong", "payload": payload }));
            }
            _ => {
                let git = crate::git_actions::GitCtx { watcher: self.checkout.clone(), bcast: self.bcast.clone() };
                // File explorer, downloads, saves and entry edits (files.rs).
                if !crate::files::handle(&kind, &msg, &rid, out) && !crate::git_actions::handle(&git, &kind, &msg, &rid, out) && !self.handle_ext_rpc(&kind, &msg, &rid, out) && request_id.is_some() {
                    rpc_error(out, &rid, &kind, "not supported by paseo-lite");
                }
            }
        }
    }

    /// Provider snapshot entries from what is known right now (no fetch).
    pub(crate) fn provider_entries_now(&self) -> Value {
        self.provider_entries(self.codex.cached_models())
    }

    fn provider_entries(&self, codex_models: Option<Value>) -> Value {
        provider_entries(&self.cfg.claude_bin, &self.cfg.codex_bin, codex_models, &self.daemon_config.overrides())
    }

    /// Fetch the Codex model list in the background and push it to every session.
    fn push_codex_models(&self) {
        if !binary_available(&self.cfg.codex_bin) || !self.codex_may_start() {
            return;
        }
        let codex = self.codex.clone();
        let bcast = self.bcast.clone();
        let (claude_bin, codex_bin, ov) = (self.cfg.claude_bin.clone(), self.cfg.codex_bin.clone(), self.daemon_config.overrides());
        tokio::spawn(async move {
            match codex.list_models().await {
                Ok(models) if models.as_array().map(|m| !m.is_empty()).unwrap_or(false) => {
                    let entries = provider_entries(&claude_bin, &codex_bin, Some(models), &ov);
                    let _ = bcast.send(wrap(json!({ "type": "providers_snapshot_update", "payload": { "entries": entries, "generatedAt": now_iso() } })));
                }
                Ok(_) => crate::warn!("codex returned no models; keeping the default entry"),
                Err(e) => crate::warn!("codex model list failed: {e}"),
            }
        });
    }

    fn create_agent(&mut self, msg: &Value, rid: &str, out: &Out) {
        let config = msg.get("config").cloned().unwrap_or(Value::Null);
        let provider = s(&config, "provider").unwrap_or_default();
        let fail = |error: String| reply(out, json!({ "type": "status", "payload": { "status": "agent_create_failed", "requestId": rid, "error": error } }));
        if !is_supported(&provider) {
            return fail(format!("provider '{provider}' is not supported (claude, codex)"));
        }
        let cwd = match normalize_dir(&s(&config, "cwd").unwrap_or_default()) {
            Ok(c) => c,
            Err(e) => return fail(e),
        };
        let now = now_iso();
        let labels: BTreeMap<String, String> = msg
            .get("labels")
            .and_then(|l| l.as_object())
            .map(|m| m.iter().filter_map(|(k, v)| v.as_str().map(|v| (k.clone(), v.to_string()))).collect())
            .unwrap_or_default();
        // The app creates the workspace first (workspace.create) and passes its id;
        // older clients and the built-in UI get the directory's default workspace.
        let workspace_id = match s(msg, "workspaceId").filter(|id| self.workspaces.iter().any(|w| &w.id == id && w.archived_at.is_none())) {
            Some(id) => id,
            None => self.find_or_create_workspace(&cwd),
        };
        let rec = AgentRecord {
            id: new_id(),
            provider,
            cwd,
            workspace_id: Some(workspace_id),
            model: s(&config, "model").filter(|m| !m.is_empty()),
            mode_id: s(&config, "modeId"),
            title: s(&config, "title"),
            created_at: now.clone(),
            updated_at: now,
            last_user_message_at: None,
            session_id: None,
            archived_at: None,
            last_error: None,
            last_usage: None,
            system_prompt: s(&config, "systemPrompt"),
            labels,
            next_seq: 0,
            attention_reason: None,
            attention_at: None,
            thinking_option_id: s(&config, "thinkingOptionId").filter(|t| !t.is_empty() && t != "default"),
            pending_turn: None,
            pending_queue: Vec::new(),
        };
        let id = rec.id.clone();
        let mut agent = Agent::new(rec);
        agent.timeline = Some(Vec::new());
        self.agents.insert(id.clone(), agent);
        self.agents_dirty = true;
        crate::log!("agent {id} created ({} in {})", self.agents[&id].rec.provider, self.agents[&id].rec.cwd);
        let snapshot = self.agents[&id].snapshot();
        reply(out, json!({ "type": "status", "payload": { "status": "agent_created", "agentId": id, "requestId": rid, "agent": snapshot } }));
        self.publish_agent(&id);
        let initial = crate::uploads::build_prompt(&self.cfg.home, &s(msg, "initialPrompt").unwrap_or_default(), msg.get("images"), msg.get("attachments"));
        if let Some(prompt) = Some(initial).filter(|p| !p.trim().is_empty()) {
            let _ = self.submit_prompt(&id, prompt, s(msg, "clientMessageId"));
        }
    }

    fn fetch_timeline(&mut self, msg: &Value, agent_key: &str, rid: &str, out: &Out) {
        let direction = s(msg, "direction").unwrap_or_else(|| "tail".into());
        let projection = s(msg, "projection").unwrap_or_else(|| "projected".into());
        let limit = msg.get("limit").and_then(|v| v.as_u64()).unwrap_or(200) as usize;
        let cursor_seq = msg.pointer("/cursor/seq").and_then(|v| v.as_u64());
        let Some(id) = self.resolve(agent_key) else {
            return reply(out, json!({ "type": "fetch_agent_timeline_response", "payload": {
                "requestId": rid, "agentId": agent_key, "agent": null, "direction": direction, "projection": projection,
                "epoch": crate::manager::EPOCH, "reset": false, "staleCursor": false, "gap": false,
                "window": { "minSeq": 0, "maxSeq": 0, "nextSeq": 0 }, "startCursor": null, "endCursor": null,
                "hasOlder": false, "hasNewer": false, "entries": [], "error": "agent not found" } }));
        };
        let store = &self.store;
        let agent = self.agents.get_mut(&id).expect("resolved");
        agent.ensure_timeline(store);
        let rows = agent.timeline.as_ref().expect("loaded");
        let n = rows.len();
        let (start, end) = match (direction.as_str(), cursor_seq) {
            ("before", Some(c)) => {
                let end = rows.iter().position(|r| r.seq_start >= c).unwrap_or(n);
                (if limit == 0 { 0 } else { end.saturating_sub(limit) }, end)
            }
            // A row is updated in place (tool call running -> completed, streamed
            // text chunks), keeping its seqStart while seqEnd grows. Catching up
            // must return every row that changed after the cursor, not only rows
            // that started after it, or a backgrounded app never sees the update.
            ("after", Some(c)) => {
                let changed: Vec<usize> = rows.iter().enumerate().filter(|(_, r)| r.seq_end > c).map(|(i, _)| i).collect();
                match changed.first() {
                    None => (n, n),
                    Some(&first) => {
                        let take = if limit == 0 { changed.len() } else { limit.min(changed.len()) };
                        (first, changed[take - 1] + 1)
                    }
                }
            }
            _ => (if limit == 0 { 0 } else { n.saturating_sub(limit) }, n),
        };
        let provider = agent.rec.provider.clone();
        let entries: Vec<Value> = rows[start..end].iter().map(|r| timeline_entry(&provider, r)).collect();
        let min_seq = rows.first().map(|r| r.seq_start).unwrap_or(0);
        // Rows updated in place can end later than rows after them.
        let max_seq = rows.iter().map(|r| r.seq_end).max().unwrap_or(0);
        let start_cursor = rows.get(start).filter(|_| start < end).map(|r| cursor(r.seq_start));
        let end_cursor = rows[start..end].iter().map(|r| r.seq_end).max().map(cursor);
        let has_newer = match (direction.as_str(), cursor_seq) {
            ("after", Some(c)) => rows[end..].iter().any(|r| r.seq_end > c),
            _ => end < n,
        };
        let payload = json!({
            "requestId": rid,
            "agentId": id,
            "agent": agent.snapshot(),
            "direction": direction,
            "projection": projection,
            "epoch": crate::manager::EPOCH,
            "reset": false,
            "staleCursor": false,
            "gap": false,
            "window": { "minSeq": min_seq, "maxSeq": max_seq, "nextSeq": agent.rec.next_seq },
            "startCursor": start_cursor,
            "endCursor": end_cursor,
            "hasOlder": start > 0,
            "hasNewer": has_newer,
            "entries": entries,
            "error": null,
        });
        reply(out, json!({ "type": "fetch_agent_timeline_response", "payload": payload }));
    }
}

/// `ProviderSnapshotEntry` list. Codex falls back to a single "Default" model
/// (Codex's configured model) until its real list has been fetched.
fn provider_entries(claude_bin: &str, codex_bin: &str, codex_models: Option<Value>, ov: &crate::daemon_config::ProviderOverrides) -> Value {
    let now = now_iso();
    let entry = |provider: &str, label: &str, bin: &str, models: Value| {
        let ok = binary_available(bin);
        json!({
            "provider": provider,
            "status": if ok { "ready" } else { "unavailable" },
            "enabled": ov.enabled(provider),
            "source": "builtin",
            "label": label,
            "models": ov.merge_models(provider, models),
            "modes": modes_for(provider),
            "defaultModeId": crate::providers::default_mode(provider),
            "fetchedAt": now,
        })
    };
    let codex_models = codex_models.unwrap_or_else(|| json!([{ "provider": "codex", "id": "default", "label": "Default", "isDefault": true }]));
    crate::snapshot::clean(json!([
        entry("claude", "Claude Code", claude_bin, claude_models()),
        entry("codex", "Codex", codex_bin, codex_models),
    ]))
}

pub fn rpc_error(out: &Out, request_id: &str, request_type: &str, error: &str) {
    reply(out, json!({ "type": "rpc_error", "payload": { "requestId": request_id, "requestType": request_type, "error": error, "code": "unsupported" } }));
}

/// Directory completion: `~/pro` -> children of `~` starting with `pro`.
pub fn suggest_dirs(query: &str, limit: usize) -> Vec<String> {
    let q = expand_home(if query.trim().is_empty() { "~/" } else { query.trim() });
    let (dir, prefix) = match q.rfind('/') {
        Some(i) => (q[..=i].to_string(), q[i + 1..].to_lowercase()),
        None => (expand_home("~/"), q.to_lowercase()),
    };
    let Ok(read) = std::fs::read_dir(&dir) else { return vec![] };
    let mut out: Vec<String> = read
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
        .filter_map(|e| e.file_name().into_string().ok())
        .filter(|name| (!name.starts_with('.') || prefix.starts_with('.')) && name.to_lowercase().starts_with(&prefix))
        .map(|name| format!("{}{}", dir, name))
        .collect();
    out.sort();
    out.truncate(limit.max(1));
    out
}
