//! Claude Code process pool.
//!
//! Upstream Paseo keeps one `claude` child alive per agent for the agent's whole
//! life. Here a child exists only while it is useful:
//! - spawned on the first prompt (with `--resume` when the session exists),
//! - kept warm for `claude_idle` after a turn so follow-ups are instant,
//!   and for as long as background sub-agents are still working in it,
//! - then its stdin is closed and it exits, freeing all of its memory.
//! At most `max_live` children run at once across all projects: the fixed
//! `claude_max_live`, or the manager's auto capacity (one on a small machine).
//! When the cap is hit the oldest idle child is evicted, or the prompt waits
//! in a FIFO queue until a running turn finishes. A child whose turn is over
//! and only background sub-agents work in it takes no slot, so another chat
//! can start alongside it (unless the machine is strained).
//! With `claude_keep_warm` the idle child is never reaped, and the manager
//! pre-starts one (`prewarm`) so the next prompt skips the cold start.

use super::claude_stream::{StreamState, SubAgents, SUBAGENT_HOOK};
use super::{AgentSpec, EventTx, ProviderEvent};
use crate::config::Config;
use serde_json::json;
use std::collections::{HashMap, VecDeque};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::{mpsc, oneshot};

struct Proc {
    stdin: mpsc::UnboundedSender<String>,
    kill: Option<oneshot::Sender<()>>,
    busy: bool,
    last_used: Instant,
    generation: u64,
    interrupted: Arc<AtomicBool>,
    /// Its background sub-agents, updated by the stream reader.
    sub_active: Arc<std::sync::Mutex<SubAgents>>,
    /// Spawn flags (model, effort) changed mid-turn: restart once the turn ends.
    restart: bool,
}

impl Proc {
    /// Idle between turns, but its background sub-agents are still working:
    /// stopping it would kill them.
    fn holding(&self) -> bool {
        self.sub_active.lock().unwrap().holding()
    }
}

pub enum SendOutcome {
    Started,
    Queued,
    Failed(String),
}

pub struct ClaudePool {
    cfg: Config,
    events: EventTx,
    procs: HashMap<String, Proc>,
    waiting: VecDeque<(AgentSpec, String)>,
    generation: u64,
    max_live: usize,
    /// Sub-agents each process may run at once, shared with every stream
    /// reader's gate (0 while the machine is strained).
    sub_cap: Arc<AtomicUsize>,
    /// The machine is under pressure: children holding sub-agents count
    /// against the cap again.
    strained: bool,
}

impl ClaudePool {
    pub fn new(cfg: Config, events: EventTx) -> Self {
        let sub_cap = Arc::new(AtomicUsize::new(cfg.claude_max_subagents));
        ClaudePool { cfg, events, procs: HashMap::new(), waiting: VecDeque::new(), generation: 0, max_live: 1, sub_cap, strained: false }
    }

    /// How many sub-agents a process may start now: the configured cap, or
    /// none while the machine is strained (running ones carry on).
    pub fn set_strained(&mut self, strained: bool) {
        self.strained = strained;
        let cap = if strained { 0 } else { self.cfg.claude_max_subagents };
        if self.sub_cap.swap(cap, Ordering::Relaxed) != cap {
            crate::log!("claude: up to {cap} sub-agent(s) per agent");
        }
    }

    pub fn live_count(&self) -> usize {
        self.procs.len()
    }

    pub fn max_live(&self) -> usize {
        self.max_live
    }

    /// Children that take a slot. One between turns whose background
    /// sub-agents are still working doesn't block other chats, unless the
    /// machine is strained.
    fn occupied(&self) -> usize {
        if self.strained {
            return self.procs.len();
        }
        self.procs.values().filter(|p| p.busy || !p.holding()).count()
    }

    /// Change the process cap. Lowering it stops warm idle processes above
    /// the new cap (busy ones finish their turn first); raising it returns
    /// waiting prompts that may start now.
    pub fn set_max_live(&mut self, max: usize) -> Vec<(AgentSpec, String)> {
        self.max_live = max.max(1);
        while self.occupied() > self.max_live && self.evict_one_idle() {}
        self.drain_waiting()
    }

    /// Prompts waiting for a free slot.
    pub fn waiting_count(&self) -> usize {
        self.waiting.len()
    }

    /// Start a process for an agent ahead of its next prompt. Only when the
    /// pool is completely empty, so it never competes with real work.
    /// Returns true when a process was started.
    pub fn prewarm(&mut self, spec: &AgentSpec) -> Result<bool, String> {
        if !self.procs.is_empty() || !self.waiting.is_empty() || spec.session_id.is_none() {
            return Ok(false);
        }
        self.spawn(spec)?;
        crate::log!("claude prewarmed for agent {}", spec.id);
        Ok(true)
    }

    fn user_message(text: &str, session_id: &Option<String>) -> String {
        let mut content = vec![json!({ "type": "text", "text": text })];
        // Attached images go in as image blocks so the model actually sees them.
        for path in crate::uploads::image_paths(text) {
            const MAX_INLINE: u64 = 5 * 1024 * 1024; // the API's per-image limit
            match std::fs::metadata(&path).map(|m| m.len()) {
                Ok(len) if len <= MAX_INLINE => {
                    if let Ok(bytes) = std::fs::read(&path) {
                        use base64::Engine;
                        let data = base64::engine::general_purpose::STANDARD.encode(bytes);
                        content.push(json!({ "type": "image", "source": { "type": "base64", "media_type": crate::uploads::mime_for_path(&path), "data": data } }));
                    }
                }
                _ => crate::warn!("image {path} too large to inline; the agent gets its path only"),
            }
        }
        json!({
            "type": "user",
            "message": { "role": "user", "content": content },
            "parent_tool_use_id": null,
            "session_id": session_id.clone().unwrap_or_default(),
        })
        .to_string()
    }

    /// Deliver a prompt. The caller guarantees the agent is not mid-turn.
    pub fn send(&mut self, spec: AgentSpec, text: String) -> SendOutcome {
        if let Some(p) = self.procs.get_mut(&spec.id) {
            p.busy = true;
            p.last_used = Instant::now();
            if p.stdin.send(Self::user_message(&text, &spec.session_id)).is_ok() {
                return SendOutcome::Started;
            }
            self.procs.remove(&spec.id);
        }
        if self.occupied() >= self.max_live && !self.evict_one_idle() {
            self.waiting.push_back((spec, text));
            return SendOutcome::Queued;
        }
        match self.spawn(&spec) {
            Ok(()) => {
                let p = self.procs.get_mut(&spec.id).expect("just spawned");
                p.busy = true;
                let _ = p.stdin.send(Self::user_message(&text, &spec.session_id));
                SendOutcome::Started
            }
            Err(e) => SendOutcome::Failed(e),
        }
    }

    /// Stop every process that isn't running a turn. Returns how many.
    pub fn release_idle(&mut self) -> usize {
        let idle: Vec<String> = self.procs.iter().filter(|(_, p)| !p.busy).map(|(id, _)| id.clone()).collect();
        for id in &idle {
            self.stop(id);
        }
        idle.len()
    }

    fn evict_one_idle(&mut self) -> bool {
        let victim = self.procs.iter().filter(|(_, p)| !p.busy && !p.holding()).min_by_key(|(_, p)| p.last_used).map(|(id, _)| id.clone());
        match victim {
            Some(id) => {
                self.stop(&id);
                true
            }
            None => false,
        }
    }

    fn spawn(&mut self, spec: &AgentSpec) -> Result<(), String> {
        self.generation += 1;
        let generation = self.generation;
        let mut cmd = Command::new(&self.cfg.claude_bin);
        cmd.args(["-p", "--input-format", "stream-json", "--output-format", "stream-json", "--verbose", "--include-partial-messages", "--permission-prompt-tool", "stdio"]);
        cmd.args(["--permission-mode", spec.mode_id.as_deref().unwrap_or("default")]);
        if let Some(model) = spec.model.as_deref().filter(|m| !m.is_empty() && *m != "default") {
            cmd.args(["--model", model]);
        }
        let session_id = match &spec.session_id {
            Some(sid) => {
                cmd.args(["--resume", sid.as_str()]);
                sid.clone()
            }
            None => {
                let sid = crate::util::new_id();
                cmd.args(["--session-id", sid.as_str()]);
                sid
            }
        };
        if let Some(effort) = spec.thinking.as_deref().filter(|e| !e.is_empty() && *e != "default") {
            cmd.args(["--effort", effort]);
        }
        let prompt: Vec<&str> = [spec.system_prompt.as_deref(), self.cfg.claude_subagent_prompt.as_deref()].into_iter().flatten().filter(|p| !p.trim().is_empty()).collect();
        if !prompt.is_empty() {
            cmd.args(["--append-system-prompt", prompt.join("\n\n").as_str()]);
        }
        // Skip telemetry, error reporting and update checks: they only cost
        // egress here (the binary is pinned in the image). Set it in the
        // daemon's environment to override.
        if std::env::var_os("CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC").is_none() {
            cmd.env("CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC", "1");
        }
        cmd.current_dir(&spec.cwd).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped()).kill_on_drop(true);
        super::deprioritize(&mut cmd, &self.cfg);
        let mut child = cmd.spawn().map_err(|e| format!("failed to start {}: {e}", self.cfg.claude_bin))?;
        crate::log!("claude started for agent {} (live={})", spec.id, self.procs.len() + 1);

        let mut stdin = child.stdin.take().ok_or("no stdin")?;
        let stdout = child.stdout.take().ok_or("no stdout")?;
        let mut stderr = child.stderr.take().ok_or("no stderr")?;
        let (in_tx, mut in_rx) = mpsc::unbounded_channel::<String>();
        let (kill_tx, kill_rx) = oneshot::channel::<()>();
        let interrupted = Arc::new(AtomicBool::new(false));

        if spec.session_id.is_none() {
            let _ = self.events.send(ProviderEvent::SessionStarted { agent_id: spec.id.clone(), session_id: session_id.clone() });
        }
        // Every sub-agent launch asks the daemon first (claude_stream's gate).
        let hooks = json!({ "PreToolUse": [{ "matcher": "Agent|Task", "hookCallbackIds": [SUBAGENT_HOOK] }] });
        let init = json!({ "type": "control_request", "request_id": format!("init-{generation}"), "request": { "subtype": "initialize", "hooks": hooks } });
        let _ = in_tx.send(init.to_string());

        tokio::spawn(async move {
            while let Some(line) = in_rx.recv().await {
                if stdin.write_all(line.as_bytes()).await.is_err() || stdin.write_all(b"\n").await.is_err() || stdin.flush().await.is_err() {
                    break;
                }
            }
            // Channel closed => stdin EOF => claude exits on its own.
        });

        let stderr_tail = Arc::new(std::sync::Mutex::new(String::new()));
        let tail = stderr_tail.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            while let Ok(n) = stderr.read(&mut buf).await {
                if n == 0 {
                    break;
                }
                let mut t = tail.lock().unwrap();
                t.push_str(&String::from_utf8_lossy(&buf[..n]));
                if t.len() > 2048 {
                    let cut = t.len() - 2048;
                    let mut start = cut;
                    while !t.is_char_boundary(start) {
                        start += 1;
                    }
                    t.drain(..start);
                }
            }
        });

        let mut state = StreamState::new(spec.id.clone(), self.events.clone(), in_tx.clone(), interrupted.clone(), Some(session_id));
        let sub_active = state.sub_active.clone();
        state.sub_cap = self.sub_cap.clone();
        let events = self.events.clone();
        let agent_id = spec.id.clone();
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            let mut kill_rx = kill_rx;
            let mut killed = false;
            loop {
                tokio::select! {
                    line = lines.next_line() => match line {
                        Ok(Some(line)) => state.handle_line(&line),
                        _ => break,
                    },
                    _ = &mut kill_rx => {
                        let _ = child.start_kill();
                        killed = true;
                        break;
                    }
                }
            }
            state.process_ended(killed);
            let status = child.wait().await.ok();
            let tail = stderr_tail.lock().unwrap().trim().to_string();
            if status.map(|s| !s.success()).unwrap_or(false) && !tail.is_empty() {
                crate::warn!("claude for {agent_id} exited: {}", crate::util::truncate(&tail, 400));
            }
            let _ = events.send(ProviderEvent::ProcessExited { agent_id, generation });
        });

        self.procs.insert(spec.id.clone(), Proc { stdin: in_tx, kill: Some(kill_tx), busy: false, last_used: Instant::now(), generation, interrupted, sub_active, restart: false });
        Ok(())
    }

    /// Called when a turn finishes. Returns queued prompts that may now start.
    pub fn turn_finished(&mut self, agent_id: &str) -> Vec<(AgentSpec, String)> {
        let restart = match self.procs.get_mut(agent_id) {
            Some(p) => {
                p.busy = false;
                p.last_used = Instant::now();
                p.restart
            }
            None => false,
        };
        if restart {
            self.stop(agent_id);
        }
        self.drain_waiting()
    }

    /// Returns true if the exiting process was the current one for the agent.
    pub fn process_exited(&mut self, agent_id: &str, generation: u64) -> (bool, Vec<(AgentSpec, String)>) {
        let current = self.procs.get(agent_id).map(|p| p.generation == generation).unwrap_or(false);
        if current {
            self.procs.remove(agent_id);
        }
        (current, self.drain_waiting())
    }

    fn drain_waiting(&mut self) -> Vec<(AgentSpec, String)> {
        let mut free = self.max_live.saturating_sub(self.occupied());
        // Make room for one waiting prompt by evicting a warm-but-idle process.
        if free == 0 && !self.waiting.is_empty() && self.evict_one_idle() {
            free = 1;
        }
        let take = free.min(self.waiting.len());
        self.waiting.drain(..take).collect()
    }

    pub fn is_waiting(&self, agent_id: &str) -> bool {
        self.waiting.iter().any(|(s, _)| s.id == agent_id)
    }

    /// Interrupt the running turn. Returns true when a queued (not yet
    /// started) prompt was dropped instead, so the caller reports cancel itself.
    pub fn interrupt(&mut self, agent_id: &str) -> bool {
        let before = self.waiting.len();
        self.waiting.retain(|(s, _)| s.id != agent_id);
        if self.waiting.len() != before {
            return true;
        }
        if let Some(p) = self.procs.get(agent_id) {
            p.interrupted.store(true, Ordering::SeqCst);
            let req = json!({ "type": "control_request", "request_id": format!("int-{}", crate::util::new_id()), "request": { "subtype": "interrupt" } });
            let _ = p.stdin.send(req.to_string());
        }
        false
    }

    pub fn set_mode(&mut self, agent_id: &str, mode: &str) {
        if let Some(p) = self.procs.get(agent_id) {
            let req = json!({ "type": "control_request", "request_id": format!("mode-{}", crate::util::new_id()), "request": { "subtype": "set_permission_mode", "mode": mode } });
            let _ = p.stdin.send(req.to_string());
        }
    }

    /// Model changes need a fresh process; the next prompt resumes the session.
    pub fn restart_on_next_prompt(&mut self, agent_id: &str) {
        let idle = match self.procs.get_mut(agent_id) {
            Some(p) if p.busy => {
                p.restart = true;
                false
            }
            Some(_) => true,
            None => false,
        };
        if idle {
            self.stop(agent_id);
        }
    }

    /// Stop the agent's process when background sub-agents work in it (an
    /// interrupt would leave them running). Returns waiting prompts that may
    /// start in the freed slot, or None when there was nothing to stop.
    pub fn stop_sub_agents(&mut self, agent_id: &str) -> Option<Vec<(AgentSpec, String)>> {
        if !self.procs.get(agent_id)?.holding() {
            return None;
        }
        crate::log!("claude: stopping agent {agent_id} and its background sub-agents");
        self.stop(agent_id);
        Some(self.drain_waiting())
    }

    pub fn stop(&mut self, agent_id: &str) {
        self.waiting.retain(|(s, _)| s.id != agent_id);
        if let Some(mut p) = self.procs.remove(agent_id) {
            if let Some(kill) = p.kill.take() {
                let _ = kill.send(());
            }
        }
    }

    /// Reap children idle past the keep-warm window (not while background
    /// sub-agents work in them). Returns waiting prompts that may now start,
    /// as a child whose sub-agents finished frees its slot here.
    pub fn reap(&mut self) -> Vec<(AgentSpec, String)> {
        if !self.cfg.claude_keep_warm {
            let idle = self.cfg.claude_idle;
            let stale: Vec<String> = self.procs.iter().filter(|(_, p)| !p.busy && !p.holding() && p.last_used.elapsed() >= idle).map(|(id, _)| id.clone()).collect();
            for id in stale {
                crate::log!("claude idle for agent {id}; releasing process");
                self.stop(&id);
            }
        }
        self.drain_waiting()
    }

    /// The CLI started a turn of its own (background sub-agents reported
    /// back). False when the agent's process is already gone.
    pub fn mark_busy(&mut self, agent_id: &str) -> bool {
        let Some(p) = self.procs.get_mut(agent_id) else { return false };
        p.busy = true;
        p.last_used = Instant::now();
        true
    }

    /// Stop every child. Returns how many were stopped.
    pub fn shutdown(&mut self) -> usize {
        let ids: Vec<String> = self.procs.keys().cloned().collect();
        for id in &ids {
            self.stop(id);
        }
        ids.len()
    }
}
