//! Provider adapters. Both report into the manager through one event channel,
//! so the manager is the single owner of agent state.

pub mod claude;
pub mod claude_stream;
pub mod codex;
pub mod tools;

use serde_json::Value;
use tokio::sync::{mpsc, oneshot};

pub type EventTx = mpsc::UnboundedSender<ProviderEvent>;

/// What a provider needs to run a turn for an agent.
#[derive(Clone, Debug)]
pub struct AgentSpec {
    pub id: String,
    pub cwd: String,
    pub model: Option<String>,
    pub mode_id: Option<String>,
    pub session_id: Option<String>,
    pub system_prompt: Option<String>,
    /// Reasoning effort; None = provider default.
    pub thinking: Option<String>,
}

pub enum ProviderEvent {
    SessionStarted { agent_id: String, session_id: String },
    TurnStarted { agent_id: String, turn_id: Option<String> },
    /// A timeline item. `assistant_message` / `reasoning` items are chunks the
    /// manager merges; `tool_call` items with a known `callId` replace that row.
    Timeline { agent_id: String, item: Value },
    Permission { agent_id: String, request: Value, reply: oneshot::Sender<Value> },
    Usage { agent_id: String, usage: Value },
    /// Running token/cost totals for the usage ledger.
    Spend { agent_id: String, sample: crate::usage::Sample },
    TurnCompleted { agent_id: String },
    TurnFailed { agent_id: String, error: String },
    /// The account's usage limit stopped the turn; sent just before its
    /// TurnFailed. `resets_at` (unix seconds) when the provider said.
    LimitHit { agent_id: String, resets_at: Option<u64> },
    TurnCanceled { agent_id: String, reason: String },
    /// The Claude CLI produced output while no turn was open: it runs a turn
    /// of its own (background work reported back), which the manager opens.
    /// Sent once per such turn, before its first timeline item.
    BackgroundTurn { agent_id: String },
    /// Process-level exit (Claude) so the pool can free the slot.
    ProcessExited { agent_id: String, generation: u64 },
}

pub fn claude_modes() -> Value {
    serde_json::json!([
        { "id": "default", "label": "Always ask", "description": "Prompt for every tool that needs permission" },
        { "id": "acceptEdits", "label": "Accept edits", "description": "Auto-approve file edits" },
        { "id": "plan", "label": "Plan", "description": "Read-only planning" },
        { "id": "bypassPermissions", "label": "Bypass", "description": "Never ask (dangerous)" }
    ])
}

pub fn codex_modes() -> Value {
    serde_json::json!([
        { "id": "read-only", "label": "Read only", "description": "Read-only sandbox, ask before changes" },
        { "id": "auto", "label": "Auto", "description": "Workspace-write sandbox, ask when needed" },
        { "id": "full-access", "label": "Full access", "description": "No sandbox, never ask (dangerous)" }
    ])
}

pub fn default_mode(provider: &str) -> &'static str {
    if provider == "codex" { "auto" } else { "default" }
}

pub fn modes_for(provider: &str) -> Value {
    if provider == "codex" { codex_modes() } else { claude_modes() }
}

/// Reasoning-effort levels passed to `claude --effort`.
pub fn claude_thinking_options() -> Value {
    serde_json::json!([
        { "id": "low", "label": "Low" },
        { "id": "medium", "label": "Medium" },
        { "id": "high", "label": "High" },
        { "id": "xhigh", "label": "Extra high" },
        { "id": "max", "label": "Max" }
    ])
}

/// Claude Code model catalog, ported from upstream's curated manifest
/// (providers/claude/model-manifest.ts). Upstream verifies it with a probe
/// process; paseo-lite skips the probe to avoid spawning `claude` just to
/// list models. Mythos 5.1 is omitted because upstream only shows it after
/// discovery confirms access.
pub fn claude_models() -> Value {
    const MODELS: &[(&str, &str, &str, u64)] = &[
        ("claude-opus-5-5", "Opus 5.5", "Opus 5.5 · Latest release", 1_000_000),
        ("claude-opus-5", "Opus 5", "Opus 5 · Previous release", 1_000_000),
        ("claude-fable-5-1", "Fable 5.1", "Fable 5.1 · Most powerful model", 1_000_000),
        ("claude-fable-5", "Fable 5", "Fable 5 · Previous release", 1_000_000),
        ("claude-opus-4-8[1m]", "Opus 4.8 1M", "Opus 4.8 with 1M context window", 1_000_000),
        ("claude-opus-4-8", "Opus 4.8", "Opus 4.8 · Previous release", 200_000),
        ("claude-sonnet-5-5", "Sonnet 5.5", "Sonnet 5.5 · Best for everyday tasks", 1_000_000),
        ("claude-sonnet-5", "Sonnet 5", "Sonnet 5 · Previous release", 200_000),
        ("claude-sonnet-5[1m]", "Sonnet 5 1M", "Sonnet 5 with 1M context window", 1_000_000),
        ("claude-opus-4-7[1m]", "Opus 4.7 1M", "Opus 4.7 with 1M context window", 1_000_000),
        ("claude-opus-4-7", "Opus 4.7", "Opus 4.7 · Previous release", 200_000),
        ("claude-opus-4-6[1m]", "Opus 4.6 1M", "Opus 4.6 with 1M context window", 1_000_000),
        ("claude-opus-4-6", "Opus 4.6", "Opus 4.6 · Most capable for complex work", 200_000),
        ("claude-sonnet-4-6[1m]", "Sonnet 4.6 1M", "Sonnet 4.6 with 1M context window", 1_000_000),
        ("claude-sonnet-4-6", "Sonnet 4.6", "Sonnet 4.6 · Best for everyday tasks", 200_000),
        ("claude-haiku-5-5", "Haiku 5.5", "Haiku 5.5 · Fastest for quick answers", 1_000_000),
        ("claude-haiku-4-5", "Haiku 4.5", "Haiku 4.5 · Previous release", 200_000),
    ];
    let thinking = claude_thinking_options();
    Value::Array(
        MODELS
            .iter()
            .map(|(id, label, description, context)| {
                let mut m = serde_json::json!({ "provider": "claude", "id": id, "label": label, "description": description, "contextWindowMaxTokens": context });
                if *id == "claude-opus-5-5" {
                    m["isDefault"] = serde_json::json!(true);
                }
                // Haiku models are offered without effort levels.
                if !id.contains("haiku") {
                    m["thinkingOptions"] = thinking.clone();
                }
                m
            })
            .collect(),
    )
}

/// Context window of a catalog model (None for unknown ids / the default).
pub fn claude_context_window(model: &str) -> Option<u64> {
    claude_models().as_array()?.iter().find(|m| m["id"].as_str() == Some(model)).and_then(|m| m["contextWindowMaxTokens"].as_u64())
}

pub fn is_supported(provider: &str) -> bool {
    provider == "claude" || provider == "codex"
}

/// Run an agent CLI below the daemon: lower CPU and I/O priority and a higher
/// OOM score. Children (tool commands, builds, test runs) inherit all three,
/// so a heavy agent task slows itself down instead of starving the daemon.
pub fn deprioritize(cmd: &mut tokio::process::Command, cfg: &crate::config::Config) {
    #[cfg(unix)]
    {
        let nice = cfg.agent_nice;
        let low_io = cfg.agent_low_io;
        // Relative to the daemon's own score: Docker's `--oom-score-adj` gives
        // the whole container one value, and agents must still rank above
        // the daemon so the kernel kills them first.
        let oom = (cfg.agent_oom_score_adj > 0).then(|| agent_oom_score(own_oom_score(), cfg.agent_oom_score_adj).to_string().into_bytes());
        // SAFETY: the closure runs between fork and exec, so it only makes
        // async-signal-safe libc calls and never allocates (`oom` is built
        // beforehand). Failures are ignored: priority is best effort.
        unsafe {
            cmd.pre_exec(move || {
                if nice > 0 {
                    libc::setpriority(libc::PRIO_PROCESS, 0, nice);
                }
                #[cfg(target_os = "linux")]
                {
                    if low_io {
                        // IOPRIO_WHO_PROCESS, self, best-effort class (2) at level 7 (lowest).
                        libc::syscall(libc::SYS_ioprio_set, 1, 0, (2 << 13) | 7);
                    }
                    if let Some(value) = &oom {
                        let fd = libc::open(c"/proc/self/oom_score_adj".as_ptr(), libc::O_WRONLY | libc::O_CLOEXEC);
                        if fd >= 0 {
                            libc::write(fd, value.as_ptr().cast(), value.len());
                            libc::close(fd);
                        }
                    }
                }
                #[cfg(not(target_os = "linux"))]
                let _ = (low_io, &oom);
                Ok(())
            });
        }
    }
    #[cfg(not(unix))]
    let _ = (cmd, cfg);
}

fn own_oom_score() -> i32 {
    std::fs::read_to_string("/proc/self/oom_score_adj").ok().and_then(|v| v.trim().parse().ok()).unwrap_or(0)
}

/// An agent's `oom_score_adj`: `extra` above the daemon's, capped at 1000.
/// At the cap the daemon is lowered where allowed (see `protect_daemon`).
fn agent_oom_score(own: i32, extra: i32) -> i32 {
    (own.max(0) + extra).min(1000)
}

/// Keep the daemon itself the last thing the OOM killer picks inside the
/// container. Lowering the score needs CAP_SYS_RESOURCE, so this usually
/// only works as root; otherwise agents still rank above it (or tie at 1000
/// when the container already starts at 1000).
pub fn protect_daemon() {
    #[cfg(target_os = "linux")]
    {
        let own = own_oom_score();
        if own > 0 && std::fs::write("/proc/self/oom_score_adj", "0").is_ok() {
            crate::log!("daemon oom_score_adj lowered from {own} to 0");
        } else if own >= 1000 {
            crate::warn!("daemon oom_score_adj is {own}: agents can't rank above it, so the kernel may pick the daemon under memory pressure");
        }
    }
}

/// Locate an executable on PATH without spawning anything.
pub fn binary_available(bin: &str) -> bool {
    let p = std::path::Path::new(bin);
    if p.components().count() > 1 {
        return p.is_file();
    }
    std::env::var_os("PATH")
        .map(|paths| std::env::split_paths(&paths).any(|dir| dir.join(bin).is_file()))
        .unwrap_or(false)
}


/// Does an error read like the account's usage or rate limit (HTTP 429),
/// which clears by itself? Spend caps and credit balances don't.
pub fn is_usage_limit(text: &str) -> bool {
    let t = text.to_ascii_lowercase();
    if ["spend limit", "usage credits", "credit balance", "budget", "context"].iter().any(|p| t.contains(p)) {
        return false;
    }
    let rate_limited = t.contains("rate limit") || t.contains("rate_limit") || t.contains("(429)") || t.contains("429 too many") || t.contains("too many requests");
    rate_limited || t.contains("usage limit") || t.contains("usagelimitexceeded") || t.contains("limit reached") || ((t.contains("hit your") || t.contains("reached your")) && t.contains("limit"))
}

/// The reset time in the older "Claude AI usage limit reached|1760000000" text.
pub fn reset_in_text(text: &str) -> Option<u64> {
    let (_, tail) = text.rsplit_once('|')?;
    tail.trim().parse().ok().filter(|t| *t > 1_000_000_000)
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_known_providers_are_supported() {
        assert!(is_supported("claude") && is_supported("codex"));
        assert!(!is_supported("gemini"));
    }

    #[test]
    fn default_modes() {
        assert_eq!(default_mode("codex"), "auto");
        assert_eq!(default_mode("claude"), "default");
    }

    #[test]
    fn claude_catalog_has_one_default_and_context_windows() {
        let models = claude_models();
        let defaults = models.as_array().unwrap().iter().filter(|m| m["isDefault"] == true).count();
        assert_eq!(defaults, 1);
        assert_eq!(claude_context_window("claude-opus-4-6"), Some(200_000));
        assert_eq!(claude_context_window("claude-opus-4-6[1m]"), Some(1_000_000));
        assert_eq!(claude_context_window("unknown"), None);
    }

    #[test]
    fn haiku_has_no_effort_levels() {
        let models = claude_models();
        let haiku = models.as_array().unwrap().iter().find(|m| m["id"] == "claude-haiku-4-5").unwrap();
        assert!(haiku.get("thinkingOptions").is_none());
    }

    #[test]
    fn missing_binary_is_unavailable() {
        assert!(!binary_available("definitely-not-a-real-binary-xyz"));
        assert!(binary_available("/bin/sh") || cfg!(windows));
    }

    #[test]
    fn agents_rank_above_the_daemon_for_the_oom_killer() {
        // Docker's --oom-score-adj 500 is inherited by the daemon.
        assert_eq!(agent_oom_score(500, 500), 1000);
        assert_eq!(agent_oom_score(0, 500), 500);
        assert_eq!(agent_oom_score(900, 500), 1000);
        assert_eq!(agent_oom_score(-200, 300), 300);
    }

    #[test]
    fn usage_limits_are_told_apart_from_other_limits() {
        assert!(is_usage_limit("You\u{2019}ve hit your session limit \u{b7} resets 3am (UTC)"));
        assert!(is_usage_limit("You've hit your usage limit. Upgrade to Plus to continue using Codex"));
        assert!(is_usage_limit("Claude AI usage limit reached|1760000000"));
        assert!(is_usage_limit("You've reached your Fable limit."));
        assert!(is_usage_limit("API Error: Request rejected (429) \u{b7} This request would exceed your account's rate limit. Please try again later."));
        assert!(is_usage_limit("exceeded retry limit, last status: 429 Too Many Requests"));
        assert!(!is_usage_limit("You've hit your monthly spend limit"));
        assert!(!is_usage_limit("Context limit reached \u{b7} /compact or /clear to continue"));
        assert!(!is_usage_limit("Credit balance is too low"));
        assert!(!is_usage_limit("API Error: 529 overloaded"));
        assert_eq!(reset_in_text("Claude AI usage limit reached|1760000000"), Some(1_760_000_000));
        assert_eq!(reset_in_text("You've hit your session limit"), None);
    }
}
