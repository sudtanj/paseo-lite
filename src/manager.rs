//! The manager actor: sole owner of agent state. Sessions and providers talk to
//! it over channels, so there are no locks around agent data and it never
//! awaits while handling a message.

use crate::config::Config;
use crate::providers::claude::{ClaudePool, SendOutcome};
use crate::providers::codex::Codex;
use crate::providers::{binary_available, AgentSpec, ProviderEvent};
use crate::store::{AgentRecord, ProjectRecord, Row, Store, WorkspaceRecord};
use crate::util::{new_id, now_iso};
use serde_json::{json, Value};
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc, oneshot};

pub type Out = mpsc::UnboundedSender<Arc<str>>;

pub const EPOCH: &str = "e1";

/// Minimum gap between attempts to (re)start a keep-warm process, so a
/// broken binary or a vanished session can't spin in a restart loop.
const WARM_RETRY: Duration = Duration::from_secs(60);

/// Prompt text that compacts the conversation (native in Claude Code; mapped
/// to `thread/compact/start` for Codex).
const COMPACT_COMMAND: &str = "/compact";

/// The CLI that serves a provider.
fn cli_of(provider: &str) -> &'static str {
    if provider == "codex" { "codex" } else { "claude" }
}

fn cli_label(cli: &str) -> &'static str {
    if cli == "codex" { "Codex" } else { "Claude Code" }
}

/// How long streamed text is held to be sent as one message.
const CHUNK_DELAY: Duration = Duration::from_millis(150);

async fn sleep_until_opt(deadline: Option<Instant>) {
    match deadline {
        Some(d) => tokio::time::sleep_until(d.into()).await,
        None => std::future::pending().await,
    }
}

/// The agent of an event that must not overtake that agent's buffered text
/// (everything but timeline items, which buffer or flush themselves).
fn other_event_agent(ev: &ProviderEvent) -> Option<&str> {
    match ev {
        ProviderEvent::Timeline { .. } => None,
        ProviderEvent::SessionStarted { agent_id, .. }
        | ProviderEvent::TurnStarted { agent_id, .. }
        | ProviderEvent::Permission { agent_id, .. }
        | ProviderEvent::Usage { agent_id, .. }
        | ProviderEvent::Spend { agent_id, .. }
        | ProviderEvent::TurnCompleted { agent_id }
        | ProviderEvent::TurnFailed { agent_id, .. }
        | ProviderEvent::LimitHit { agent_id, .. }
        | ProviderEvent::TurnCanceled { agent_id, .. }
        | ProviderEvent::ProcessExited { agent_id, .. } => Some(agent_id),
    }
}

pub enum Cmd {
    /// A session-level message (already unwrapped from the `session` envelope).
    Rpc { msg: Value, out: Out },
    /// Register a directory produced by background work (GitHub clone), then
    /// send `response` with `payload.project` filled in.
    RegisterProject { path: String, out: Out, response: Value },
    /// Push tokens Expo reported as dead.
    RevokePushTokens(Vec<String>),
}

/// What a connected client last reported via `client_heartbeat`.
pub struct Presence {
    pub out: Out,
    pub focused_agent: Option<String>,
    pub app_visible: bool,
    pub active_at: Instant,
}

pub struct Permission {
    pub request: Value,
    pub reply: Option<oneshot::Sender<Value>>,
}

pub struct Agent {
    pub rec: AgentRecord,
    pub status: &'static str,
    pub active_turn: Option<(String, String)>,
    /// Messages sent while a turn runs: (text, client message id).
    pub queue: VecDeque<(String, Option<String>)>,
    pub permissions: Vec<Permission>,
    pub timeline: Option<Vec<Row>>,
    pub dirty_rows: BTreeSet<u64>,
    pub last_touch: Instant,
    /// The active turn is a daemon-started compaction, not a user prompt.
    pub compacting: bool,
    /// The user prompt of the running turn (resent when a retry has nothing to continue).
    pub turn_text: String,
    /// Automatic retries used by the running turn.
    pub attempts: u32,
    /// The turn produced output (assistant text, reasoning or a tool call).
    pub produced: bool,
    /// Last sign of life from the provider, for the stall watchdog.
    pub last_event: Instant,
    /// A retry is scheduled: when to re-dispatch and with which prompt.
    pub retry_at: Option<Instant>,
    pub retry_text: String,
    /// The provider reported the usage limit for the running turn (reset time when known).
    pub limit_hit: Option<Option<u64>>,
    /// The scheduled retry is a wait for the usage limit to reset.
    pub limit_wait: bool,
    /// When the running turn first hit the usage limit.
    pub limit_since: Option<Instant>,
}

pub struct Manager {
    pub cfg: Config,
    pub server_id: String,
    pub store: Store,
    pub agents: HashMap<String, Agent>,
    pub claude: ClaudePool,
    pub codex: Codex,
    /// The one CLI allowed to have a process ("claude" / "codex"). Switching
    /// stops the other CLI first, so the two never run side by side.
    pub active_cli: Option<&'static str>,
    /// Agents whose turn currently holds a slot on the active CLI.
    pub running: HashSet<String>,
    /// Turns (agentId, prompt) waiting for the CLI, across all projects and
    /// both providers, strictly first come first served.
    pub cli_queue: VecDeque<(String, String)>,
    /// How many turns may run at once when a cap is `auto`, and the watch on
    /// the container's limits that frees memory before it runs out.
    pub capacity: crate::capacity::Capacity,
    claude_warm_at: Option<Instant>,
    codex_warm_at: Option<Instant>,
    pub bcast: broadcast::Sender<Arc<str>>,
    pub events_tx: mpsc::UnboundedSender<ProviderEvent>,
    pub agents_dirty: bool,
    /// Relay pairing link (None when the relay is disabled).
    pub pairing_url: Option<String>,
    /// Directories added as projects, in insertion order.
    pub projects: Vec<ProjectRecord>,
    /// Workspaces (including archived ones, kept for agent history).
    pub workspaces: Vec<WorkspaceRecord>,
    /// Lets background tasks hand results back to the manager.
    pub cmds_tx: mpsc::UnboundedSender<Cmd>,
    /// Live git diff subscriptions for the Changes panel.
    pub checkout: crate::checkout::Watcher,
    /// When the daemon started (`daemon.get_status`).
    pub started_at: String,
    /// Pending `wait_for_finish_request`s.
    pub waiters: Vec<Waiter>,
    /// Expo push tokens registered by phones.
    pub push_tokens: crate::push::TokenStore,
    /// Presence per connected client, for in-app vs push notifications.
    pub clients: Vec<Presence>,
    /// App-editable settings (custom models, appended system prompt).
    pub daemon_config: crate::daemon_config::DaemonConfig,
    /// Tokens and cost per provider and model.
    pub usage: crate::usage::Ledger,
    /// Streamed text not sent yet, per agent: (kind, text). Deltas arrive a few
    /// tokens at a time; sending them in ~150 ms batches cuts the messages (and
    /// their envelopes) sent to the app by an order of magnitude.
    chunks: HashMap<String, (String, String)>,
    chunk_deadline: Option<Instant>,
}

/// A client blocked in `wait_for_finish_request` until the agent stops
/// running, needs permission, or the deadline passes.
pub struct Waiter {
    pub agent_id: String,
    pub request_id: String,
    pub out: Out,
    pub deadline: Option<Instant>,
}

pub fn wrap(msg: Value) -> Arc<str> {
    Arc::from(json!({ "type": "session", "message": msg }).to_string())
}

pub fn reply(out: &Out, msg: Value) {
    let _ = out.send(wrap(msg));
}

impl Manager {
    pub fn new(
        cfg: Config,
        server_id: String,
        pairing_url: Option<String>,
        bcast: broadcast::Sender<Arc<str>>,
        cmds_tx: mpsc::UnboundedSender<Cmd>,
    ) -> (Manager, mpsc::UnboundedReceiver<ProviderEvent>) {
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let store = Store::new(cfg.home.clone());
        let mut agents = HashMap::new();
        for rec in store.load_agents() {
            agents.insert(rec.id.clone(), Agent::new(rec));
        }
        let projects = store.load_projects();
        let workspaces = store.load_workspaces();
        let push_tokens = crate::push::TokenStore::load(&cfg.home);
        let daemon_config = crate::daemon_config::DaemonConfig::load(&cfg.home);
        let usage = crate::usage::Ledger::load(&cfg.home);
        for p in &projects {
            crate::snapshot::set_project_name(&p.root_path, p.custom_name.as_deref());
        }
        crate::log!("loaded {} agents, {} projects, {} workspaces", agents.len(), projects.len(), workspaces.len());
        let mut claude = ClaudePool::new(cfg.clone(), events_tx.clone());
        let codex = Codex::new(cfg.clone(), events_tx.clone());
        let auto = [cfg.claude_max_live, cfg.codex_max_active].contains(&crate::capacity::Mode::Auto);
        let capacity = crate::capacity::Capacity::new(auto, cfg.agent_mb, cfg.auto_max_agents, cfg.auto_stable);
        if let crate::capacity::Mode::Fixed(n) = cfg.claude_max_live {
            claude.set_max_live(n);
        }
        let mut manager = Manager { cfg, server_id, store, agents, claude, codex, active_cli: None, running: HashSet::new(), cli_queue: VecDeque::new(), capacity, claude_warm_at: None, codex_warm_at: None, bcast, events_tx, agents_dirty: false, pairing_url, projects, workspaces, cmds_tx, checkout: crate::checkout::Watcher::start(), started_at: now_iso(), waiters: Vec::new(), push_tokens, clients: Vec::new(), daemon_config, usage, chunks: HashMap::new(), chunk_deadline: None };
        manager.migrate_agent_workspaces();
        (manager, events_rx)
    }

    /// Add a directory to the project registry (idempotent) and return its
    /// descriptor. `owned`: paseo-lite created the directory itself.
    pub fn register_project(&mut self, path: &str, owned: bool) -> Value {
        match self.projects.iter_mut().find(|p| p.root_path == path) {
            Some(p) => {
                if owned && !p.owned {
                    p.owned = true;
                    self.store.save_projects(&self.projects);
                }
            }
            None => {
                self.projects.push(ProjectRecord { root_path: path.to_string(), created_at: now_iso(), custom_name: None, owned });
                self.store.save_projects(&self.projects);
                crate::log!("project added: {path}");
            }
        }
        let project = crate::snapshot::project_descriptor(path);
        self.broadcast(json!({ "type": "project.update", "payload": { "kind": "upsert", "project": project } }));
        project
    }

    pub async fn run(mut self, mut cmds: mpsc::UnboundedReceiver<Cmd>, mut events: mpsc::UnboundedReceiver<ProviderEvent>, mut shutdown: oneshot::Receiver<()>) {
        self.resume_interrupted();
        let mut tick = tokio::time::interval(Duration::from_secs(2));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                Some(cmd) = cmds.recv() => match cmd {
                    Cmd::Rpc { msg, out } => self.handle_rpc(msg, &out),
                    Cmd::RegisterProject { path, out, mut response } => {
                        response["payload"]["project"] = self.register_project(&path, true);
                        reply(&out, response);
                    }
                    Cmd::RevokePushTokens(tokens) => {
                        for t in tokens {
                            self.push_tokens.revoke(&t);
                        }
                    }
                },
                Some(ev) = events.recv() => self.handle_event(ev),
                _ = tick.tick() => self.housekeeping(),
                _ = sleep_until_opt(self.chunk_deadline) => self.flush_chunks(),
                _ = &mut shutdown => break,
            }
            self.resolve_waiters();
        }
        self.flush_all();
        self.claude.shutdown();
        self.codex.shutdown();
    }

    fn housekeeping(&mut self) {
        self.claude.reap();
        self.codex.reap();
        self.update_capacity();
        self.keep_warm();
        self.run_retries_and_watchdog();
        self.flush_all();
        let unload = self.cfg.timeline_unload;
        for agent in self.agents.values_mut() {
            if agent.timeline.is_some() && agent.status != "running" && agent.dirty_rows.is_empty() && agent.last_touch.elapsed() >= unload {
                agent.timeline = None;
            }
        }
    }

    /// Keep the active CLI ready so prompts skip the cold start. Only the
    /// active CLI is ever warmed; the other one stays fully stopped.
    /// Runs every tick; the first tick fires right after startup.
    fn keep_warm(&mut self) {
        if self.active_cli.is_none() {
            // Startup: the CLI of the most recently used agent becomes active.
            self.active_cli = self
                .agents
                .values()
                .filter(|a| a.rec.archived_at.is_none() && a.rec.last_user_message_at.is_some())
                .max_by(|a, b| a.rec.last_user_message_at.cmp(&b.rec.last_user_message_at))
                .map(|a| cli_of(&a.rec.provider));
            if let Some(cli) = self.active_cli {
                crate::log!("active CLI: {cli} (last used)");
                if cli == "claude" {
                    self.codex.set_allowed(false);
                    self.codex.shutdown();
                }
            }
        }
        if self.active_cli.is_none() && self.codex.is_running() {
            // Started for model listing before any agent ran: it is the warm
            // CLI now, so a later Claude turn switches (and stops it) properly.
            self.active_cli = Some("codex");
        }
        let due = |t: Option<Instant>| t.map(|t| t.elapsed() >= WARM_RETRY).unwrap_or(true);
        match self.active_cli {
            Some("codex") => {
                if self.cfg.codex_keep_warm && !self.strained() && !self.codex.is_running() && due(self.codex_warm_at) && binary_available(&self.cfg.codex_bin) {
                    self.codex_warm_at = Some(Instant::now());
                    let codex = self.codex.clone();
                    tokio::spawn(async move {
                        if let Err(e) = codex.warm_up().await {
                            crate::warn!("codex warm-up failed: {e}");
                        }
                    });
                }
            }
            Some("claude") => {
                if self.cfg.claude_keep_warm && !self.strained() && self.claude.live_count() == 0 && !self.codex.is_running() && self.running.is_empty() && self.cli_queue.is_empty() && due(self.claude_warm_at) && binary_available(&self.cfg.claude_bin) {
                    // The process is tied to one session and cwd, so warm the Claude
                    // agent most likely to get the next prompt: the last one used.
                    let candidate = self
                        .agents
                        .values()
                        .filter(|a| a.rec.provider == "claude" && a.status != "running" && a.rec.archived_at.is_none() && a.rec.session_id.is_some())
                        .filter(|a| std::path::Path::new(&a.rec.cwd).is_dir())
                        .max_by(|a, b| a.rec.last_user_message_at.as_ref().unwrap_or(&a.rec.updated_at).cmp(b.rec.last_user_message_at.as_ref().unwrap_or(&b.rec.updated_at)))
                        .map(|a| a.rec.id.clone());
                    if let Some(spec) = candidate.and_then(|id| self.spec(&id)) {
                        self.claude_warm_at = Some(Instant::now());
                        if let Err(e) = self.claude.prewarm(&spec) {
                            crate::warn!("claude prewarm failed: {e}");
                        }
                    }
                }
            }
            _ => {}
        }
    }

    pub fn flush_all(&mut self) {
        self.flush_chunks();
        for agent in self.agents.values_mut() {
            agent.flush(&self.store);
        }
        if self.agents_dirty {
            self.agents_dirty = false;
            self.store.save_agents(self.agents.values().map(|a| &a.rec));
        }
        self.usage.save_if_dirty();
    }

    pub fn broadcast(&self, msg: Value) {
        let _ = self.bcast.send(wrap(msg));
    }

    pub fn touch(&mut self, agent_id: &str) {
        if let Some(a) = self.agents.get_mut(agent_id) {
            a.rec.updated_at = now_iso();
            a.last_touch = Instant::now();
        }
        self.agents_dirty = true;
    }

    pub fn publish_agent(&self, agent_id: &str) {
        if let Some(a) = self.agents.get(agent_id) {
            self.broadcast(json!({
                "type": "agent_update",
                "payload": { "kind": "upsert", "agent": a.snapshot(), "project": a.project() },
            }));
            // Workspace status/activity is derived from its agents.
            if let Some(ws) = &a.rec.workspace_id {
                self.publish_workspace(ws);
            }
        }
    }

    fn stream(&self, agent_id: &str, provider: &str, event: Value) {
        let _ = provider;
        self.broadcast(json!({
            "type": "agent_stream",
            "payload": { "agentId": agent_id, "event": event, "timestamp": now_iso() },
        }));
    }

    /// Append a timeline item, merging chunks / tool updates into existing rows.
    /// Append a timeline item, first sending any streamed text still buffered
    /// for the agent so rows keep their order.
    pub fn append_item(&mut self, agent_id: &str, item: Value) {
        self.flush_chunk(agent_id);
        self.append_now(agent_id, item);
    }

    /// Hold a streamed text delta briefly and merge it with the next ones;
    /// anything else is appended at once (after the held text).
    fn buffer_chunk(&mut self, agent_id: &str, item: Value) {
        let kind = item.get("type").and_then(|t| t.as_str()).unwrap_or("");
        let text = item.get("text").and_then(|t| t.as_str());
        let (Some(text), true) = (text, item.as_object().map(|o| o.len() == 2).unwrap_or(false) && matches!(kind, "assistant_message" | "reasoning")) else {
            return self.append_item(agent_id, item);
        };
        match self.chunks.get_mut(agent_id).filter(|(k, _)| k == kind) {
            Some((_, held)) => held.push_str(text),
            None => {
                self.flush_chunk(agent_id);
                self.chunks.insert(agent_id.to_string(), (kind.to_string(), text.to_string()));
            }
        }
        if self.chunk_deadline.is_none() {
            self.chunk_deadline = Some(Instant::now() + CHUNK_DELAY);
        }
    }

    fn flush_chunk(&mut self, agent_id: &str) {
        if let Some((kind, text)) = self.chunks.remove(agent_id) {
            self.append_now(agent_id, json!({ "type": kind, "text": text }));
        }
    }

    fn flush_chunks(&mut self) {
        self.chunk_deadline = None;
        let ids: Vec<String> = self.chunks.keys().cloned().collect();
        for id in ids {
            self.flush_chunk(&id);
        }
    }

    fn append_now(&mut self, agent_id: &str, item: Value) {
        let item = crate::snapshot::clean(item);
        let store = &self.store;
        let Some(agent) = self.agents.get_mut(agent_id) else { return };
        agent.ensure_timeline(store);
        let seq = agent.rec.next_seq;
        agent.rec.next_seq += 1;
        let ts = now_iso();
        let turn_id = agent.active_turn.as_ref().map(|t| t.0.clone());
        let provider = agent.rec.provider.clone();
        let kind = item.get("type").and_then(|t| t.as_str()).unwrap_or("").to_string();
        let rows = agent.timeline.as_mut().expect("loaded");

        let mut merged_into: Option<usize> = None;
        if kind == "assistant_message" || kind == "reasoning" {
            if let Some(last) = rows.last_mut() {
                if last.item.get("type").and_then(|t| t.as_str()) == Some(kind.as_str()) && last.turn_id == turn_id {
                    let add = item.get("text").and_then(|t| t.as_str()).unwrap_or("");
                    if let Some(Value::String(text)) = last.item.get_mut("text") {
                        text.push_str(add);
                    }
                    let tag = if kind == "assistant_message" { "assistant_merge" } else { "reasoning_merge" };
                    if !last.collapsed.iter().any(|c| c == tag) {
                        last.collapsed.push(tag.into());
                    }
                    merged_into = Some(rows.len() - 1);
                }
            }
        } else if kind == "tool_call" {
            let call_id = item.get("callId").and_then(|c| c.as_str()).unwrap_or("");
            let n = rows.len();
            if let Some(idx) = rows.iter().rev().take(256).position(|r| r.item.get("callId").and_then(|c| c.as_str()) == Some(call_id)).map(|p| n - 1 - p) {
                rows[idx].item = item.clone();
                if !rows[idx].collapsed.iter().any(|c| c == "tool_lifecycle") {
                    rows[idx].collapsed.push("tool_lifecycle".into());
                }
                merged_into = Some(idx);
            }
        }

        match merged_into {
            Some(idx) => {
                let row = &mut rows[idx];
                row.seq_end = seq;
                match row.ranges.last_mut() {
                    Some(r) if r.1 + 1 == seq => r.1 = seq,
                    _ => row.ranges.push((seq, seq)),
                }
                agent.dirty_rows.insert(row.seq_start);
            }
            None => {
                rows.push(Row { seq_start: seq, seq_end: seq, ranges: vec![(seq, seq)], collapsed: vec![], item: item.clone(), turn_id: turn_id.clone(), timestamp: ts.clone() });
                agent.dirty_rows.insert(seq);
            }
        }
        agent.last_touch = Instant::now();
        self.agents_dirty = true;
        let mut event = json!({ "type": "timeline", "provider": provider, "item": item });
        if let Some(t) = turn_id {
            event["turnId"] = json!(t);
        }
        self.broadcast(json!({
            "type": "agent_stream",
            "payload": { "agentId": agent_id, "event": event, "timestamp": ts, "seq": seq, "epoch": EPOCH },
        }));
    }

    fn spec(&self, agent_id: &str) -> Option<AgentSpec> {
        let a = self.agents.get(agent_id)?;
        Some(AgentSpec {
            id: a.rec.id.clone(),
            cwd: a.rec.cwd.clone(),
            model: a.rec.model.clone(),
            mode_id: a.rec.mode_id.clone(),
            session_id: a.rec.session_id.clone(),
            // Built-in default, then the app's appendSystemPrompt, then the agent's own.
            system_prompt: {
                let parts: Vec<String> = [self.cfg.default_system_prompt.clone(), self.daemon_config.append_system_prompt(), a.rec.system_prompt.clone()]
                    .into_iter()
                    .flatten()
                    .filter(|p| !p.trim().is_empty())
                    .collect();
                (!parts.is_empty()).then(|| parts.join("\n\n"))
            },
            thinking: a.rec.thinking_option_id.clone(),
        })
    }

    /// Accept a prompt: start now when idle, else queue behind the running turn.
    pub fn submit_prompt(&mut self, agent_id: &str, text: String, message_id: Option<String>) -> Result<(), String> {
        let agent = self.agents.get_mut(agent_id).ok_or("agent not found")?;
        if agent.rec.archived_at.is_some() {
            agent.rec.archived_at = None;
        }
        if agent.status == "running" {
            if agent.limit_wait && agent.retry_at.is_some() {
                // Waiting for the usage limit: a new message means "try now".
                // If the limit still holds, the wait is simply rescheduled.
                agent.retry_at = Some(Instant::now());
            }
            agent.queue.push_back((text, message_id));
            self.persist_pending(agent_id);
            return Ok(());
        }
        self.start_turn(agent_id, vec![(text, message_id)]);
        Ok(())
    }

    /// Start a turn for one or more user messages. Several messages (queued
    /// while the previous turn ran) each get their own bubble in the chat and
    /// are sent to the model together as one prompt.
    fn start_turn(&mut self, agent_id: &str, messages: Vec<(String, Option<String>)>) {
        let text = merged_prompt(&messages);
        let turn_id = new_id();
        let provider = {
            let Some(agent) = self.agents.get_mut(agent_id) else { return };
            agent.status = "running";
            agent.turn_text = text.clone();
            agent.attempts = 0;
            agent.produced = false;
            agent.retry_at = None;
            agent.limit_hit = None;
            agent.limit_wait = false;
            agent.limit_since = None;
            agent.last_event = Instant::now();
            agent.active_turn = Some((turn_id.clone(), now_iso()));
            agent.rec.last_user_message_at = Some(now_iso());
            agent.rec.last_error = None;
            agent.rec.attention_reason = None;
            if agent.rec.title.is_none() {
                let first = messages.first().map(|m| crate::uploads::bubble_text(&m.0)).unwrap_or_default();
                let first_line = first.lines().next().unwrap_or("").trim();
                agent.rec.title = Some(crate::util::truncate(first_line, 80));
            }
            agent.rec.provider.clone()
        };
        // One bubble per message, exactly as the user sent them.
        for (msg_text, message_id) in &messages {
            // The bubble shows what the user wrote (plus attachment names), not the notes for the agent.
            let mut user = json!({ "type": "user_message", "text": crate::uploads::bubble_text(msg_text), "messageId": message_id.clone().unwrap_or_else(new_id) });
            if let Some(id) = message_id {
                user["clientMessageId"] = json!(id);
            }
            self.append_item(agent_id, user);
        }
        self.stream(agent_id, &provider, json!({ "type": "turn_started", "provider": provider, "turnId": turn_id }));
        self.touch(agent_id);
        self.publish_agent(agent_id);
        self.persist_pending(agent_id);
        self.dispatch(agent_id, text);
    }

    /// Queue a turn for the shared CLI and start whatever can run now.
    fn dispatch(&mut self, agent_id: &str, text: String) {
        self.cli_queue.push_back((agent_id.to_string(), text));
        self.pump();
        if let Some(pos) = self.cli_queue.iter().position(|(id, _)| id == agent_id) {
            let note = json!({ "type": "notification", "level": "info", "message": format!("Queued: waiting for the agent CLI ({} ahead)", self.running.len() + pos) });
            self.append_item(agent_id, note);
        }
    }

    /// Start queued turns in strict FIFO order. Only one CLI is active at a
    /// time: a turn for the other CLI waits until the active CLI's running
    /// turns end, then the active CLI is shut down and the other one cold
    /// starts and stays warm until the next switch.
    fn pump(&mut self) {
        while let Some((front, _)) = self.cli_queue.front() {
            let id = front.clone();
            let Some(provider) = self.agents.get(&id).filter(|a| a.status == "running").map(|a| a.rec.provider.clone()) else {
                self.cli_queue.pop_front();
                continue;
            };
            let cli = cli_of(&provider);
            if self.active_cli != Some(cli) {
                if !self.running.is_empty() {
                    break;
                }
                self.switch_cli(cli, &id);
            }
            if self.running.len() >= self.slots(cli) {
                break;
            }
            let Some((id, text)) = self.cli_queue.pop_front() else { break };
            let Some(spec) = self.spec(&id) else { continue };
            self.running.insert(id.clone());
            if cli == "codex" {
                self.run_codex(spec, text);
            } else if let SendOutcome::Failed(error) = self.claude.send(spec, text) {
                self.finish_turn(&id, Err(error));
            }
        }
    }

    /// How many turns `cli` may run at once: its fixed cap, or what the
    /// machine allows right now.
    pub fn slots(&self, cli: &str) -> usize {
        let mode = if cli == "codex" { self.cfg.codex_max_active } else { self.cfg.claude_max_live };
        match mode {
            crate::capacity::Mode::Fixed(n) => n,
            crate::capacity::Mode::Auto => self.capacity.limit(),
        }
    }

    /// Auto capacity: read the machine, let the Claude pool keep as many
    /// processes as the new limit allows, and start queued turns when it
    /// went up. When the container is about to run out, release every idle
    /// process so the daemon keeps the room it needs to stay in control.
    /// Runs every tick.
    fn update_capacity(&mut self) {
        let queued = self.cli_queue.len() + self.claude.waiting_count();
        let running = self.running.len();
        let change = self.capacity.update(running, queued);
        if change == crate::capacity::Change::Critical {
            let freed = self.claude.release_idle();
            if freed > 0 {
                crate::warn!("capacity: released {freed} idle claude process(es) to free memory");
            }
        }
        let max = self.slots("claude");
        if max != self.claude.max_live() {
            let ready = self.claude.set_max_live(max);
            self.send_ready(ready);
        }
        if change == crate::capacity::Change::Grew {
            self.pump();
        }
    }

    /// Under pressure, optional work (warm-ups) waits.
    fn strained(&self) -> bool {
        self.capacity.strained()
    }

    /// Start Claude prompts the pool released from its waiting list.
    fn send_ready(&mut self, ready: Vec<(AgentSpec, String)>) {
        for (spec, text) in ready {
            let id = spec.id.clone();
            if let SendOutcome::Failed(e) = self.claude.send(spec, text) {
                self.finish_turn(&id, Err(e));
            }
        }
    }

    /// Whether side work (model listing) may start the Codex app-server:
    /// never while Claude Code is the active CLI.
    pub fn codex_may_start(&self) -> bool {
        self.active_cli != Some("claude")
    }

    /// Stop the active CLI's processes and hand the machine to `cli`.
    fn switch_cli(&mut self, cli: &'static str, for_agent: &str) {
        // Always stop the other CLI, even with no active one recorded yet.
        if cli == "codex" {
            self.claude.shutdown();
            self.codex.set_allowed(true);
        } else {
            // Block first so an in-flight warm-up can't restart it.
            self.codex.set_allowed(false);
            self.codex.shutdown();
        }
        if let Some(old) = self.active_cli {
            crate::log!("switching CLI: {old} -> {cli}");
            let note = json!({ "type": "notification", "level": "info", "message": format!("Switching from {} to {} (starting {})", cli_label(old), cli_label(cli), cli_label(cli)) });
            self.append_item(for_agent, note);
        }
        self.active_cli = Some(cli);
        self.claude_warm_at = None;
        self.codex_warm_at = None;
    }

    fn run_codex(&mut self, spec: AgentSpec, text: String) {
        let codex = self.codex.clone();
        let events = self.events_tx.clone();
        let id = spec.id.clone();
        tokio::spawn(async move {
            // `/compact` (typed, or queued by auto-compaction) maps to Codex's
            // native thread compaction; Claude handles the command itself.
            let result = if text.trim() == COMPACT_COMMAND { codex.compact(spec).await } else { codex.run_turn(spec, text).await };
            if let Err(error) = result {
                let _ = events.send(ProviderEvent::TurnFailed { agent_id: id, error });
            }
        });
    }

    /// True when the agent's last turn left its context at or past
    /// `auto_compact_pct` of the window or `auto_compact_tokens`.
    fn needs_compaction(&self, agent_id: &str) -> bool {
        let pct = self.cfg.auto_compact_pct as u64;
        let Some(a) = self.agents.get(agent_id) else { return false };
        if pct == 0 || a.rec.session_id.is_none() {
            return false;
        }
        let Some(usage) = &a.rec.last_usage else { return false };
        let get = |k: &str| usage.get(k).and_then(|v| v.as_u64()).filter(|n| *n > 0);
        let Some(used) = get("contextWindowUsedTokens") else { return false };
        let cap = self.cfg.auto_compact_tokens;
        (cap > 0 && used >= cap) || get("contextWindowMaxTokens").map(|max| used * 100 >= max * pct).unwrap_or(false)
    }

    /// Run `/compact` as its own turn, ahead of the agent's queued prompts,
    /// so the next prompt is sent with a summarised (smaller) context.
    fn start_compaction(&mut self, agent_id: &str) {
        let (provider, used) = {
            let Some(agent) = self.agents.get_mut(agent_id) else { return };
            let turn_id = new_id();
            agent.status = "running";
            agent.active_turn = Some((turn_id, now_iso()));
            agent.compacting = true;
            agent.last_event = Instant::now();
            agent.retry_at = None;
            // Wait for fresh numbers before considering another compaction.
            let used = match agent.rec.last_usage.as_mut() {
                Some(Value::Object(u)) => u.remove("contextWindowUsedTokens").and_then(|v| v.as_u64()),
                _ => None,
            };
            (agent.rec.provider.clone(), used)
        };
        crate::log!("auto-compacting context for agent {agent_id}");
        let turn_id = self.agents[agent_id].active_turn.as_ref().map(|t| t.0.clone());
        let message = match used {
            Some(n) => format!("Context is at {}K tokens; compacting to save tokens and data", n / 1000),
            None => "Compacting the context to save tokens and data".to_string(),
        };
        let note = json!({ "type": "notification", "level": "info", "message": message });
        self.append_item(agent_id, note);
        self.stream(agent_id, &provider, json!({ "type": "turn_started", "provider": provider, "turnId": turn_id }));
        self.touch(agent_id);
        self.publish_agent(agent_id);
        self.dispatch(agent_id, COMPACT_COMMAND.to_string());
    }

    /// End the active turn. `Ok(None)` completed, `Ok(Some(reason))` canceled, `Err` failed.
    pub fn finish_turn(&mut self, agent_id: &str, outcome: Result<Option<String>, String>) {
        self.flush_chunk(agent_id);
        let compacting = match self.agents.get_mut(agent_id) {
            // A scheduled retry owns the turn; late events from the dead attempt are ignored.
            Some(a) if a.status == "running" && a.retry_at.is_none() => std::mem::take(&mut a.compacting),
            _ => return,
        };
        // A failed auto-compaction is not the user's failure: note it and
        // carry on with the uncompacted context (the CLI's own limit still applies).
        let outcome = match outcome {
            Err(e) if compacting => {
                crate::warn!("auto-compaction failed for agent {agent_id}: {e}");
                self.append_item(agent_id, json!({ "type": "notification", "level": "warning", "message": format!("Auto-compaction failed: {e}") }));
                Ok(None)
            }
            other => other,
        };
        if !compacting {
            let hit = self.agents.get_mut(agent_id).and_then(|a| a.limit_hit.take());
            if self.cfg.resume_after_limit {
                let waiting = match &outcome {
                    Err(e) if hit.is_some() || crate::providers::is_usage_limit(e) => {
                        let resets_at = hit.flatten().or_else(|| crate::providers::reset_in_text(e));
                        self.schedule_limit_wait(agent_id, resets_at, false)
                    }
                    // The turn finished, but a sub-agent stopped on the limit with its work undone.
                    Ok(None) if hit.is_some() => self.schedule_limit_wait(agent_id, hit.flatten(), true),
                    _ => false,
                };
                if waiting {
                    return;
                }
            }
            if let Some(reason) = self.retry_reason(agent_id, &outcome) {
                if self.schedule_retry(agent_id, &reason) {
                    return;
                }
            }
        }
        let (provider, turn_id) = {
            let Some(agent) = self.agents.get_mut(agent_id) else { return };
            let turn_id = agent.active_turn.take().map(|t| t.0);
            // Dropping the reply senders makes providers deny outstanding requests.
            let pending: Vec<String> = agent.permissions.drain(..).filter_map(|p| p.request.get("id").and_then(|i| i.as_str()).map(String::from)).collect();
            agent.status = if outcome.is_err() { "error" } else { "idle" };
            let reason = match &outcome {
                Ok(None) => Some("finished"),
                Err(_) => Some("error"),
                Ok(Some(_)) => None,
            };
            // The user's turn already raised its attention; the compaction after it doesn't.
            if !compacting {
                agent.rec.attention_reason = reason.map(String::from);
                agent.rec.attention_at = reason.map(|_| now_iso());
            }
            if let Err(e) = &outcome {
                agent.rec.last_error = Some(e.clone());
            }
            let provider = agent.rec.provider.clone();
            for id in pending {
                self.broadcast(json!({ "type": "agent_permission_resolved", "payload": { "agentId": agent_id, "requestId": id, "resolution": { "behavior": "deny", "message": "turn ended" } } }));
            }
            (provider, turn_id)
        };
        self.persist_pending(agent_id);
        let event = match &outcome {
            Ok(None) => {
                let mut ev = json!({ "type": "turn_completed", "provider": provider, "turnId": turn_id });
                if let Some(u) = self.agents[agent_id].rec.last_usage.clone() {
                    ev["usage"] = crate::snapshot::strip_nulls(u);
                }
                ev
            }
            Ok(Some(reason)) => json!({ "type": "turn_canceled", "provider": provider, "turnId": turn_id, "reason": reason }),
            Err(error) => {
                self.append_item(agent_id, json!({ "type": "error", "message": error }));
                json!({ "type": "turn_failed", "provider": provider, "turnId": turn_id, "error": error })
            }
        };
        self.stream(agent_id, &provider, event);
        if let Some(a) = self.agents.get_mut(agent_id) {
            a.flush(&self.store);
        }
        self.touch(agent_id);
        self.publish_agent(agent_id);
        self.checkout.poke();
        if !compacting {
            match &outcome {
                Ok(None) => self.notify_attention(agent_id, "finished"),
                Err(_) => self.notify_attention(agent_id, "error"),
                Ok(Some(_)) => {}
            }
        }

        // Free the CLI slot (or the queue entry of a turn that never started).
        self.running.remove(agent_id);
        self.cli_queue.retain(|(id, _)| id != agent_id);
        if provider == "claude" {
            let ready = self.claude.turn_finished(agent_id);
            self.send_ready(ready);
        }
        self.pump();
        if !compacting && matches!(outcome, Ok(None)) && self.needs_compaction(agent_id) {
            // Queued prompts for this agent run after the compaction finishes.
            self.start_compaction(agent_id);
            return;
        }
        // Everything the user queued for this chat runs as one turn: same
        // conversation, one round trip, context sent once.
        let batch = self.agents.get_mut(agent_id).map(|a| take_batch(&mut a.queue)).unwrap_or_default();
        if !batch.is_empty() {
            self.start_turn(agent_id, batch);
        }
    }

    fn save_agents_now(&mut self) {
        self.agents_dirty = false;
        self.store.save_agents(self.agents.values().map(|a| &a.rec));
    }

    /// Record the running turn and queued follow-ups in the agent record and
    /// save at once, so a hard kill (container replaced, OOM) can't lose them.
    fn persist_pending(&mut self, agent_id: &str) {
        let Some(a) = self.agents.get_mut(agent_id) else { return };
        let pending = (a.status == "running" && !a.compacting && !a.turn_text.is_empty()).then(|| a.turn_text.clone());
        let queue: Vec<String> = a.queue.iter().map(|(t, _)| t.clone()).collect();
        if a.rec.pending_turn == pending && a.rec.pending_queue == queue {
            return;
        }
        a.rec.pending_turn = pending;
        a.rec.pending_queue = queue;
        self.save_agents_now();
    }

    /// After a restart: continue every turn that was interrupted and restore
    /// the queued follow-ups, so the agent carries on without anyone noticing.
    fn resume_interrupted(&mut self) {
        let ids: Vec<String> = self.agents.values().filter(|a| a.rec.archived_at.is_none() && (a.rec.pending_turn.is_some() || !a.rec.pending_queue.is_empty())).map(|a| a.rec.id.clone()).collect();
        for id in ids {
            let store = &self.store;
            let Some(a) = self.agents.get_mut(&id) else { continue };
            a.ensure_timeline(store);
            a.queue = std::mem::take(&mut a.rec.pending_queue).into_iter().map(|t| (t, None)).collect();
            let Some(prompt) = a.rec.pending_turn.take() else {
                // Only follow-ups were waiting: start the next one.
                let batch = take_batch(&mut a.queue);
                if !batch.is_empty() {
                    self.start_turn(&id, batch);
                }
                continue;
            };
            // Did the turn get anywhere before the restart? Rows after the last user message.
            let rows = a.timeline.as_deref().unwrap_or_default();
            let after_user = rows.iter().rposition(|r| r.item.get("type").and_then(|t| t.as_str()) == Some("user_message")).map(|i| &rows[i + 1..]).unwrap_or_default();
            let produced = after_user.iter().any(|r| matches!(r.item.get("type").and_then(|t| t.as_str()), Some("assistant_message" | "reasoning" | "tool_call")));
            a.status = "running";
            a.active_turn = Some((new_id(), now_iso()));
            a.turn_text = prompt.clone();
            a.attempts = 0;
            a.produced = produced;
            a.compacting = false;
            a.retry_at = None;
            a.limit_hit = None;
            a.limit_wait = false;
            a.limit_since = None;
            a.last_event = Instant::now();
            let provider = a.rec.provider.clone();
            let turn_id = a.active_turn.as_ref().map(|t| t.0.clone());
            crate::log!("resuming interrupted turn for agent {id}");
            let note = json!({ "type": "notification", "level": "info", "message": "paseo-lite restarted while this agent was working. Resuming where it left off." });
            self.append_item(&id, note);
            self.stream(&id, &provider, json!({ "type": "turn_started", "provider": provider, "turnId": turn_id }));
            self.publish_agent(&id);
            self.persist_pending(&id);
            // With progress already made, ask the model to pick up; otherwise resend the prompt.
            self.dispatch(&id, if produced { RESUME_PROMPT.to_string() } else { prompt });
        }
    }

    /// Why a finished turn should be retried automatically, if it should:
    /// a transient error, no output at all, or a stop in the middle of a
    /// tool call. Auth/quota errors and user cancels are never retried.
    fn retry_reason(&self, agent_id: &str, outcome: &Result<Option<String>, String>) -> Option<String> {
        if self.cfg.retry_max == 0 {
            return None;
        }
        let a = self.agents.get(agent_id)?;
        match outcome {
            Err(e) => retryable_error(e).then(|| e.clone()),
            Ok(None) if !a.produced => Some("the model returned no response".into()),
            Ok(None) => {
                let last = a.timeline.as_ref().and_then(|t| t.last())?;
                let kind = last.item.get("type").and_then(|t| t.as_str());
                let status = last.item.get("status").and_then(|t| t.as_str());
                (kind == Some("tool_call") && matches!(status, Some("canceled") | Some("running"))).then(|| "the model stopped in the middle of a tool call".into())
            }
            Ok(Some(_)) => None,
        }
    }

    /// Stop the failed attempt and re-dispatch after a backoff. False when the
    /// retries are used up (the failure is then reported as usual).
    fn schedule_retry(&mut self, agent_id: &str, reason: &str) -> bool {
        let (max, base) = (self.cfg.retry_max, self.cfg.retry_base);
        let Some(a) = self.agents.get_mut(agent_id) else { return false };
        if a.attempts >= max {
            return false;
        }
        a.attempts += 1;
        let attempt = a.attempts;
        let delay = (base * 2u32.saturating_pow(attempt - 1)).min(Duration::from_secs(60));
        a.retry_at = Some(Instant::now() + delay);
        a.limit_wait = false;
        // Output already streamed stays in the conversation: ask the model to
        // pick up from there. With nothing produced, resend the prompt.
        a.retry_text = if a.produced { CONTINUE_PROMPT.to_string() } else { a.turn_text.clone() };
        crate::warn!("agent {agent_id}: {reason}; retry {attempt}/{max} in {}s", delay.as_secs());
        let note = json!({ "type": "notification", "level": "warning", "message": format!("Something went wrong ({}). Retrying automatically in {}s (attempt {attempt} of {max})", crate::util::truncate(reason, 160), delay.as_secs()) });
        self.park_turn(agent_id, note);
        true
    }

    /// The account's usage or rate limit stopped the turn (or, with
    /// `subagents`, one of its sub-agents): keep the turn open, wait until the
    /// limit resets, then carry on. With no reset time (a rate limit), check
    /// back after a minute, then twice as long each time up to the poll
    /// interval. Queued messages run after it. False when the wait would be
    /// too long; the failure is then reported as usual.
    fn schedule_limit_wait(&mut self, agent_id: &str, resets_at: Option<u64>, subagents: bool) -> bool {
        let now = unix_now();
        let Some(a) = self.agents.get_mut(agent_id) else { return false };
        let since = *a.limit_since.get_or_insert_with(Instant::now);
        let poll = self.cfg.limit_poll;
        let (delay, known) = match resets_at {
            Some(t) if t > now => (Duration::from_secs(t - now) + LIMIT_GRACE, true),
            _ => (since.elapsed().clamp(LIMIT_FIRST_POLL.min(poll), poll), false),
        };
        if since.elapsed() + delay > LIMIT_MAX_WAIT {
            crate::warn!("agent {agent_id}: usage limit resets too far out ({}); not waiting", wait_text(delay));
            return false;
        }
        a.retry_at = Some(Instant::now() + delay);
        a.limit_wait = true;
        a.retry_text = if subagents { SUBAGENT_LIMIT_PROMPT.to_string() } else if a.produced { LIMIT_PROMPT.to_string() } else { a.turn_text.clone() };
        let what = if subagents { "A sub-agent stopped at the usage limit" } else { "Usage limit reached" };
        let message = if known {
            format!("{what}. Continuing the work automatically at {} (in {}). Send a message to try sooner, or stop to cancel.", reset_text(now + delay.as_secs()), wait_text(delay))
        } else {
            format!("{what}. Continuing the work automatically once it clears: next try in {}. Send a message to try sooner, or stop to cancel.", wait_text(delay))
        };
        crate::log!("agent {agent_id}: usage limit; continuing in {}", wait_text(delay));
        self.park_turn(agent_id, json!({ "type": "notification", "level": "warning", "message": message }));
        true
    }

    /// The CLI ran a turn of its own while none was open here (background
    /// sub-agents reporting back) and it hit the limit: open a turn that
    /// waits for the reset and then has the unfinished work picked up.
    fn wait_out_idle_limit(&mut self, agent_id: &str, resets_at: Option<u64>) {
        let turn_id = new_id();
        let provider = {
            let Some(a) = self.agents.get_mut(agent_id) else { return };
            a.status = "running";
            a.active_turn = Some((turn_id.clone(), now_iso()));
            a.turn_text = SUBAGENT_LIMIT_PROMPT.to_string();
            a.attempts = 0;
            a.produced = true;
            a.compacting = false;
            a.retry_at = None;
            a.limit_hit = None;
            a.limit_wait = false;
            a.limit_since = None;
            a.last_event = Instant::now();
            a.rec.provider.clone()
        };
        crate::log!("agent {agent_id}: limit hit outside a turn (background sub-agents)");
        self.stream(agent_id, &provider, json!({ "type": "turn_started", "provider": provider, "turnId": turn_id }));
        if self.schedule_limit_wait(agent_id, resets_at, true) {
            self.touch(agent_id);
            self.persist_pending(agent_id);
            return;
        }
        if let Some(a) = self.agents.get_mut(agent_id) {
            a.status = "idle";
            a.active_turn = None;
        }
        self.stream(agent_id, &provider, json!({ "type": "turn_canceled", "provider": provider, "turnId": turn_id, "reason": "usage limit" }));
        self.publish_agent(agent_id);
    }

    /// Keep the turn open while the failed attempt is torn down: free its CLI
    /// slot, deny its open permission requests and note why in the chat.
    fn park_turn(&mut self, agent_id: &str, note: Value) {
        let Some(a) = self.agents.get_mut(agent_id) else { return };
        // Dropping the reply senders makes providers deny outstanding requests.
        a.permissions.clear();
        let provider = a.rec.provider.clone();
        // A clean slate: a fresh CLI process resumes the session; Codex's turn is interrupted.
        self.running.remove(agent_id);
        self.cli_queue.retain(|(id, _)| id != agent_id);
        if provider == "codex" {
            self.codex.abandon(agent_id);
        } else {
            self.claude.stop(agent_id);
        }
        self.append_item(agent_id, note);
        self.publish_agent(agent_id);
        self.pump();
    }

    /// Housekeeping: re-dispatch due retries and unstick silent turns.
    fn run_retries_and_watchdog(&mut self) {
        let now = Instant::now();
        let due: Vec<(String, String)> = self.agents.values().filter(|a| a.retry_at.map(|t| now >= t).unwrap_or(false)).map(|a| (a.rec.id.clone(), a.retry_text.clone())).collect();
        for (id, text) in due {
            if let Some(a) = self.agents.get_mut(&id) {
                a.retry_at = None;
                a.limit_wait = false;
                a.last_event = Instant::now();
            }
            self.dispatch(&id, text);
        }
        let limit = self.cfg.stall_timeout;
        if limit.is_zero() || self.cfg.retry_max == 0 {
            return;
        }
        let stalled: Vec<String> = self
            .agents
            .values()
            .filter(|a| a.status == "running" && a.retry_at.is_none() && self.running.contains(&a.rec.id) && a.permissions.is_empty() && a.last_event.elapsed() >= limit)
            // A long-running tool call is quiet by nature.
            .filter(|a| !a.timeline.as_ref().and_then(|t| t.last()).map(|r| r.item.get("type").and_then(|t| t.as_str()) == Some("tool_call") && r.item.get("status").and_then(|s| s.as_str()) == Some("running")).unwrap_or(false))
            .map(|a| a.rec.id.clone())
            .collect();
        for id in stalled {
            crate::warn!("agent {id}: no output for {}s; treating the turn as stuck", limit.as_secs());
            self.finish_turn(&id, Err(format!("no output for {} minutes", (limit.as_secs() / 60).max(1))));
        }
    }

    pub fn cancel(&mut self, agent_id: &str) {
        let Some(agent) = self.agents.get_mut(agent_id) else { return };
        agent.queue.clear();
        self.persist_pending(agent_id);
        let Some(agent) = self.agents.get_mut(agent_id) else { return };
        if agent.status != "running" {
            return;
        }
        if agent.retry_at.take().is_some() {
            // Canceling while a retry is pending: the turn is simply over.
            self.finish_turn(agent_id, Ok(Some("canceled".into())));
            return;
        }
        let is_codex = agent.rec.provider == "codex";
        if self.cli_queue.iter().any(|(id, _)| id == agent_id) {
            self.finish_turn(agent_id, Ok(Some("canceled before start".into())));
            return;
        }
        if is_codex {
            let codex = self.codex.clone();
            let id = agent_id.to_string();
            tokio::spawn(async move { codex.interrupt(&id).await });
        } else if self.claude.interrupt(agent_id) {
            self.finish_turn(agent_id, Ok(Some("canceled before start".into())));
        }
    }

    /// The model Codex uses for agents that didn't pick one.
    fn codex_default_model(&self) -> Option<String> {
        let models = self.codex.cached_models()?;
        let list = models.as_array()?;
        list.iter().find(|m| m.get("isDefault").and_then(|v| v.as_bool()) == Some(true)).or(list.first()).and_then(|m| m.get("id")).and_then(|v| v.as_str()).map(String::from)
    }

    fn handle_event(&mut self, ev: ProviderEvent) {
        if let Some(id) = other_event_agent(&ev) {
            self.flush_chunk(id);
        }
        match ev {
            ProviderEvent::SessionStarted { agent_id, session_id } => {
                if let Some(a) = self.agents.get_mut(&agent_id) {
                    a.rec.session_id = Some(session_id.clone());
                    let provider = a.rec.provider.clone();
                    self.agents_dirty = true;
                    // Losing the session id on a hard kill would orphan the conversation.
                    self.save_agents_now();
                    self.stream(&agent_id, &provider, json!({ "type": "thread_started", "provider": provider, "sessionId": session_id }));
                }
            }
            ProviderEvent::TurnStarted { .. } => {}
            ProviderEvent::Timeline { agent_id, item } => {
                if let Some(a) = self.agents.get_mut(&agent_id).filter(|a| a.status == "running" && a.retry_at.is_none()) {
                    a.last_event = Instant::now();
                    if matches!(item.get("type").and_then(|t| t.as_str()), Some("assistant_message" | "reasoning" | "tool_call")) {
                        a.produced = true;
                    }
                    self.buffer_chunk(&agent_id, item);
                }
            }
            ProviderEvent::Permission { agent_id, request, reply } => {
                let request = crate::snapshot::clean(request);
                let Some(a) = self.agents.get_mut(&agent_id) else { return };
                a.permissions.push(Permission { request: request.clone(), reply: Some(reply) });
                a.rec.attention_reason = Some("permission".into());
                a.rec.attention_at = Some(now_iso());
                let provider = a.rec.provider.clone();
                self.broadcast(json!({ "type": "agent_permission_request", "payload": { "agentId": agent_id, "request": request } }));
                self.stream(&agent_id, &provider, json!({ "type": "permission_requested", "provider": provider, "request": request }));
                self.publish_agent(&agent_id);
                self.notify_attention(&agent_id, "permission");
            }
            ProviderEvent::Usage { agent_id, usage } => {
                if let Some(a) = self.agents.get_mut(&agent_id) {
                    let mut usage = usage;
                    // Claude doesn't report its window size; derive it from the model.
                    if a.rec.provider == "claude" && usage.get("contextWindowMaxTokens").map(|v| v.is_null()).unwrap_or(true) {
                        // From the model catalog; unknown or default model -> the configured window.
                        let model = a.rec.model.as_deref().unwrap_or("");
                        let window = crate::providers::claude_context_window(model).unwrap_or(if model.to_ascii_lowercase().contains("[1m]") { 1_000_000 } else { self.cfg.claude_context_window });
                        usage["contextWindowMaxTokens"] = json!(window);
                    }
                    // Show what the agent has spent overall, not just this session.
                    if let Some(total) = self.usage.agent_total(&agent_id).filter(|t| t.cost > 0.0) {
                        usage["totalCostUsd"] = json!(total.cost);
                    }
                    a.rec.last_usage = Some(usage);
                }
            }
            ProviderEvent::Spend { agent_id, sample } => {
                let Some(a) = self.agents.get(&agent_id) else { return };
                let provider = a.rec.provider.clone();
                let model = a.rec.model.clone().filter(|m| !m.is_empty()).or_else(|| (provider == "codex").then(|| self.codex_default_model()).flatten()).unwrap_or_else(|| provider.clone());
                self.usage.record(&agent_id, &provider, &model, sample);
                let total = self.usage.agent_total(&agent_id).unwrap_or_default();
                if let Some(Value::Object(u)) = self.agents.get_mut(&agent_id).and_then(|a| a.rec.last_usage.as_mut()) {
                    if total.cost > 0.0 {
                        u.insert("totalCostUsd".into(), json!(total.cost));
                    }
                }
            }
            ProviderEvent::TurnCompleted { agent_id } => self.finish_turn(&agent_id, Ok(None)),
            ProviderEvent::TurnCanceled { agent_id, reason } => self.finish_turn(&agent_id, Ok(Some(reason))),
            ProviderEvent::LimitHit { agent_id, resets_at } => {
                let Some(a) = self.agents.get_mut(&agent_id) else { return };
                if a.status == "running" {
                    if a.retry_at.is_none() {
                        a.limit_hit = Some(resets_at);
                    }
                } else if a.rec.provider == "claude" && a.rec.archived_at.is_none() && self.cfg.resume_after_limit {
                    // Only the Claude CLI runs turns of its own; late Codex events are from canceled turns.
                    self.wait_out_idle_limit(&agent_id, resets_at);
                }
            }
            ProviderEvent::TurnFailed { agent_id, error } => self.finish_turn(&agent_id, Err(error)),
            ProviderEvent::ProcessExited { agent_id, generation } => {
                let (current, ready) = self.claude.process_exited(&agent_id, generation);
                let queued = self.claude.is_waiting(&agent_id) || self.cli_queue.iter().any(|(id, _)| id == &agent_id);
                let failed = current && self.agents.get(&agent_id).map(|a| a.status == "running").unwrap_or(false) && !queued;
                // Start the prompts that were waiting on this slot before
                // finish_turn drains the pool again, so they keep their order.
                self.send_ready(ready);
                if failed {
                    self.finish_turn(&agent_id, Err("claude exited unexpectedly".into()));
                }
            }
        }
    }

    /// Record a `client_heartbeat` (focus, visibility, last activity).
    pub fn client_heartbeat(&mut self, msg: &Value, out: &Out) {
        self.clients.retain(|c| !c.out.is_closed());
        // Age of the client's last activity, from its own timestamp; never in the future.
        let age = msg
            .get("lastActivityAt")
            .and_then(|v| v.as_str())
            .and_then(|t| humantime::parse_rfc3339_weak(t).ok())
            .and_then(|t| std::time::SystemTime::now().duration_since(t).ok())
            .unwrap_or_default();
        let presence = Presence {
            out: out.clone(),
            focused_agent: msg.get("focusedAgentId").and_then(|v| v.as_str()).map(String::from),
            app_visible: msg.get("appVisible").and_then(|v| v.as_bool()).unwrap_or(false),
            active_at: Instant::now().checked_sub(age).unwrap_or_else(Instant::now),
        };
        match self.clients.iter_mut().find(|c| c.out.same_channel(out)) {
            Some(c) => *c = presence,
            None => self.clients.push(presence),
        }
    }

    /// Tell the user an agent needs them, like upstream: nothing if a present
    /// client is looking at that agent; an in-app notice to the most recently
    /// active present client; otherwise a phone push (not for errors).
    pub fn notify_attention(&mut self, agent_id: &str, reason: &'static str) {
        self.clients.retain(|c| !c.out.is_closed());
        // A backgrounded app (phone locked) is not present: it should get a push.
        let present: Vec<&Presence> = self.clients.iter().filter(|c| c.app_visible && c.active_at.elapsed() <= crate::push::PRESENCE_WINDOW).collect();
        if present.iter().any(|c| c.app_visible && c.focused_agent.as_deref() == Some(agent_id)) {
            return;
        }
        let recipient = present.iter().max_by_key(|c| c.active_at).map(|c| c.out.clone());
        // The workspace the app groups this agent under (same id as in its snapshot).
        let Some(workspace) = self.agents.get(agent_id).map(|a| a.rec.workspace_id.clone().unwrap_or_else(|| crate::snapshot::workspace_id(&a.rec.cwd))) else { return };
        let detail = match reason {
            "finished" => self.last_assistant_message(agent_id),
            "permission" => self.agents.get(agent_id).and_then(|a| a.permissions.last()).map(|p| {
                let title = p.request.get("title").and_then(|t| t.as_str()).unwrap_or("").trim().to_string();
                let desc = p.request.get("description").and_then(|t| t.as_str()).unwrap_or("").trim().to_string();
                match (title.is_empty(), desc.is_empty() || desc == title) {
                    (false, false) => format!("{title} - {desc}"),
                    (false, true) => title,
                    (true, false) => desc,
                    (true, true) => p.request.get("name").and_then(|n| n.as_str()).unwrap_or("tool").to_string(),
                }
            }),
            _ => None,
        };
        let payload = crate::push::attention_payload(reason, &self.server_id, &workspace, agent_id, detail.as_deref());
        match recipient {
            Some(out) => reply(&out, json!({ "type": "agent_attention_required", "payload": { "agentId": agent_id, "reason": reason, "timestamp": now_iso(), "shouldNotify": true, "notification": payload } })),
            None if reason != "error" => {
                let tokens = self.push_tokens.active();
                if tokens.is_empty() {
                    return;
                }
                crate::log!("push notification ({reason}) for agent {agent_id} to {} device(s)", tokens.len());
                let cmds = self.cmds_tx.clone();
                tokio::spawn(async move {
                    let dead = crate::push::send(tokens, payload).await;
                    if !dead.is_empty() {
                        let _ = cmds.send(Cmd::RevokePushTokens(dead));
                    }
                });
            }
            None => {}
        }
    }

    /// Errors are shown on the app's card, which stays open to try again.
    pub fn respond_permission(&mut self, agent_id: &str, request_id: &str, response: Value) -> Result<(), &'static str> {
        let Some(a) = self.agents.get_mut(agent_id) else { return Err("agent not found") };
        let Some(pos) = a.permissions.iter().position(|p| p.request.get("id").and_then(|i| i.as_str()) == Some(request_id)) else {
            return Err("This request is no longer pending (the turn ended or was restarted)");
        };
        let req = &a.permissions[pos].request;
        let allow = response.get("behavior").and_then(|b| b.as_str()) == Some("allow");
        if allow && req.get("kind").and_then(|k| k.as_str()) == Some("question") {
            let original = req.get("input").cloned().unwrap_or(Value::Null);
            if crate::providers::claude_stream::question_answer(response.get("updatedInput"), &original).is_none() {
                return Err("Answer at least one question");
            }
        }
        let mut perm = a.permissions.remove(pos);
        if let Some(tx) = perm.reply.take() {
            let _ = tx.send(response.clone());
        }
        if a.permissions.is_empty() && a.rec.attention_reason.as_deref() == Some("permission") {
            a.rec.attention_reason = None;
        }
        let provider = a.rec.provider.clone();
        self.broadcast(json!({ "type": "agent_permission_resolved", "payload": { "agentId": agent_id, "requestId": request_id, "resolution": response } }));
        self.stream(agent_id, &provider, json!({ "type": "permission_resolved", "provider": provider, "requestId": request_id, "resolution": response }));
        self.publish_agent(agent_id);
        Ok(())
    }

    /// Release every runtime resource held for an agent (process slot, thread route).
    pub fn release_runtime(&mut self, agent_id: &str) {
        if let Some(a) = self.agents.get_mut(agent_id) {
            a.queue.clear();
            a.permissions.clear();
            a.status = "idle";
            a.active_turn = None;
            a.compacting = false;
            a.retry_at = None;
        }
        self.persist_pending(agent_id);
        self.claude.stop(agent_id);
        self.cli_queue.retain(|(id, _)| id != agent_id);
        // Interrupt the turn in the shared app-server too, so the freed slot
        // doesn't overlap with an orphaned turn.
        self.codex.abandon(agent_id);
        if self.running.remove(agent_id) {
            self.pump();
        }
    }
}

impl Agent {
    pub fn new(rec: AgentRecord) -> Agent {
        Agent { rec, status: "idle", active_turn: None, queue: VecDeque::new(), permissions: Vec::new(), timeline: None, dirty_rows: BTreeSet::new(), last_touch: Instant::now(), compacting: false, turn_text: String::new(), attempts: 0, produced: false, last_event: Instant::now(), retry_at: None, retry_text: String::new(), limit_hit: None, limit_wait: false, limit_since: None }
    }

    pub fn ensure_timeline(&mut self, store: &Store) {
        if self.timeline.is_none() {
            let rows = store.load_timeline(&self.rec.id);
            if let Some(last) = rows.last() {
                self.rec.next_seq = self.rec.next_seq.max(last.seq_end + 1);
            }
            self.timeline = Some(rows);
        }
        self.last_touch = Instant::now();
    }

    pub fn flush(&mut self, store: &Store) {
        if self.dirty_rows.is_empty() {
            return;
        }
        let Some(rows) = &self.timeline else { return };
        let dirty = std::mem::take(&mut self.dirty_rows);
        let out: Vec<&Row> = rows.iter().filter(|r| dirty.contains(&r.seq_start)).collect();
        store.append_rows(&self.rec.id, &out);
    }
}

/// Sent when an interrupted turn is retried after it had already produced output.
const CONTINUE_PROMPT: &str = "Your previous response was interrupted unexpectedly. Continue from where you left off and finish the task.";

/// Sent when the daemon restarted in the middle of a turn that had already produced output.
const RESUME_PROMPT: &str = "The server was restarted while you were working, which interrupted your previous response. Continue from where you left off and finish the task.";

/// After a usage limit resets, wait a little longer before continuing, so the
/// first request doesn't race the reset.
const LIMIT_GRACE: Duration = Duration::from_secs(60);
/// Give up waiting for a usage limit after this long (weekly limits included).
const LIMIT_MAX_WAIT: Duration = Duration::from_secs(8 * 24 * 3600);
/// First check-back after a limit with no reset time (rate limits clear in minutes).
const LIMIT_FIRST_POLL: Duration = Duration::from_secs(60);

/// Sent when the usage limit has reset and a turn it stopped carries on.
const LIMIT_PROMPT: &str = "The usage or rate limit that stopped you has reset. Continue from where you left off and finish the task.";
/// Sent when the limit that stopped sub-agents (yours or background ones) has reset.
const SUBAGENT_LIMIT_PROMPT: &str = "Some of your sub-agents stopped early because of the usage or rate limit, which has now reset. Check what they left unfinished and finish it, re-launching them if needed. If nothing is left to do, say so briefly.";

/// Transient failures are retried; auth, quota and setup problems won't fix
/// themselves, so those surface immediately.
fn retryable_error(error: &str) -> bool {
    const PERMANENT: [&str; 12] = [
        "not logged in", "log in", "login", "authentication", "unauthorized", "invalid api key", "credit balance", "usage limit", "quota", "failed to start", "no such file", "not found",
    ];
    let e = error.to_ascii_lowercase();
    !PERMANENT.iter().any(|p| e.contains(p))
}

fn unix_now() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

/// "2026-10-12 03:01 UTC"
fn reset_text(unix: u64) -> String {
    let at = std::time::UNIX_EPOCH + Duration::from_secs(unix);
    let iso = humantime::format_rfc3339_seconds(at).to_string();
    format!("{} UTC", iso.get(..16).unwrap_or(&iso).replace('T', " "))
}

/// "2h 14m", "3d 4h", "45m"
fn wait_text(d: Duration) -> String {
    let m = (d.as_secs() + 59) / 60;
    match (m / 1440, m % 1440 / 60, m % 60) {
        (0, 0, m) => format!("{m}m"),
        (0, h, m) => format!("{h}h {m}m"),
        (d, h, _) => format!("{d}d {h}h"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limit_wait_is_described_plainly() {
        assert_eq!(reset_text(1_760_000_000), "2025-10-09 08:53 UTC");
        assert_eq!(wait_text(Duration::from_secs(59)), "1m");
        assert_eq!(wait_text(Duration::from_secs(45 * 60)), "45m");
        assert_eq!(wait_text(Duration::from_secs(2 * 3600 + 14 * 60)), "2h 14m");
        assert_eq!(wait_text(Duration::from_secs(3 * 86400 + 4 * 3600)), "3d 4h");
    }

    #[test]
    fn transient_errors_are_retryable() {
        assert!(retryable_error("claude exited unexpectedly"));
        assert!(retryable_error("codex app-server exited"));
        assert!(retryable_error("API Error: 529 overloaded"));
        assert!(retryable_error("no output for 10 minutes"));
    }

    #[test]
    fn permanent_errors_are_not_retried() {
        assert!(!retryable_error("Not logged in · Please run /login"));
        assert!(!retryable_error("Invalid API key"));
        assert!(!retryable_error("Credit balance is too low"));
        assert!(!retryable_error("failed to start claude: No such file or directory"));
    }

    #[test]
    fn cli_selection() {
        assert_eq!(cli_of("codex"), "codex");
        assert_eq!(cli_of("claude"), "claude");
        assert_eq!(cli_label("codex"), "Codex");
    }
}

/// The next run of queued messages to send as one turn. Slash commands
/// (`/compact`, `/init`, ...) only work as a whole message, so a command is
/// always a turn of its own and ends a batch.
fn take_batch(queue: &mut VecDeque<(String, Option<String>)>) -> Vec<(String, Option<String>)> {
    let is_command = |m: &(String, Option<String>)| m.0.trim_start().starts_with('/');
    match queue.front() {
        None => Vec::new(),
        Some(m) if is_command(m) => queue.pop_front().into_iter().collect(),
        Some(_) => {
            let mut batch = Vec::new();
            while queue.front().map(|m| !is_command(m)).unwrap_or(false) {
                batch.extend(queue.pop_front());
            }
            batch
        }
    }
}

/// One prompt for a batch of messages from the same chat.
fn merged_prompt(messages: &[(String, Option<String>)]) -> String {
    match messages {
        [] => String::new(),
        [(only, _)] => only.clone(),
        many => {
            let mut out = String::from("While you were working, the user sent these follow-up messages in this conversation. Treat them together, in order (later messages may refine or override earlier ones):\n");
            for (i, (text, _)) in many.iter().enumerate() {
                out.push_str(&format!("\n[{}] {}\n", i + 1, text.trim()));
            }
            out
        }
    }
}

#[cfg(test)]
mod batching_tests {
    use super::*;

    fn q(items: &[&str]) -> VecDeque<(String, Option<String>)> {
        items.iter().map(|s| (s.to_string(), None)).collect()
    }

    #[test]
    fn batches_plain_messages_and_isolates_commands() {
        let mut queue = q(&["fix the test", "also update docs", "/compact", "then push"]);
        let first = take_batch(&mut queue);
        assert_eq!(first.len(), 2);
        assert_eq!(take_batch(&mut queue)[0].0, "/compact");
        assert_eq!(take_batch(&mut queue)[0].0, "then push");
        assert!(take_batch(&mut queue).is_empty());
    }

    #[test]
    fn single_message_is_sent_unchanged() {
        assert_eq!(merged_prompt(&[("hello".into(), None)]), "hello");
    }

    #[test]
    fn merged_prompt_numbers_messages_in_order() {
        let p = merged_prompt(&[("one".into(), None), ("two".into(), None)]);
        assert!(p.contains("[1] one") && p.contains("[2] two"));
        assert!(p.find("[1]").unwrap() < p.find("[2]").unwrap());
    }
}
