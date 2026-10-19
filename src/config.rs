//! Runtime configuration, read once from environment variables and CLI flags.

use std::path::PathBuf;
use std::time::Duration;

/// Appended to every agent's system prompt (Claude `--append-system-prompt`,
/// Codex `developerInstructions`). paseo-lite targets small VMs where an agent
/// starting dev servers, test suites or builds can starve or OOM the host.
pub const DEFAULT_SYSTEM_PROMPT: &str = "You are running on a low-resource VM (for example a 1 GB RAM free-tier instance) that is shared with the agent daemon itself. Do not run, start, or serve the application you are developing locally (no dev servers, `npm run dev`/`start`, `cargo run`, `docker compose up`, emulators or browsers), and do not run its test suites, end-to-end tests, benchmarks or full builds locally - they can exhaust the machine's memory and CPU and take it down. Verify changes by careful code reading and reasoning instead, and rely on CI or the user to build and test. Lightweight commands such as git, file inspection, searching, formatting or a single quick syntax check are fine. If verification genuinely requires running something heavy, stop and ask the user first.";

#[derive(Clone, Debug)]
pub struct Config {
    pub home: PathBuf,
    pub listen: String,
    pub password: Option<String>,
    pub relay_enabled: bool,
    /// Extra DNS names accepted in the Host header (`PASEO_HOSTNAMES`); a
    /// leading dot matches any subdomain. IPs and localhost are always allowed.
    pub hostnames: Vec<String>,
    pub relay_endpoint: String,
    pub relay_use_tls: bool,
    pub relay_public_endpoint: String,
    pub relay_public_use_tls: bool,
    pub app_base_url: String,
    pub claude_bin: String,
    pub codex_bin: String,
    /// How long an idle `claude` process is kept warm before it is reaped
    /// (ignored when `claude_keep_warm` is on).
    pub claude_idle: Duration,
    /// Cap on simultaneously live `claude` processes across all projects: a
    /// number, or `auto` to follow the machine (capacity.rs). Prompts beyond
    /// the cap wait in a FIFO queue.
    pub claude_max_live: crate::capacity::Mode,
    /// Never reap the idle `claude` process, and pre-start one for the most
    /// recently used Claude agent so the next prompt skips the cold start.
    pub claude_keep_warm: bool,
    /// How long the shared `codex app-server` stays up with no active turns
    /// (ignored when `codex_keep_warm` is on).
    pub codex_idle: Duration,
    /// Max Codex turns running at once across all projects (a number or
    /// `auto`); the rest queue.
    pub codex_max_active: crate::capacity::Mode,
    /// Auto capacity: memory budgeted per agent (MB), the most agents it may
    /// run at once, and how long the machine must stay calm before one more.
    pub agent_mb: u64,
    pub auto_max_agents: usize,
    pub auto_stable: Duration,
    /// Start the shared `codex app-server` with the daemon and keep it up,
    /// restarting it if it exits.
    pub codex_keep_warm: bool,
    /// Niceness for `claude` / `codex` processes (and everything they run),
    /// so heavy agent work yields the CPU to the daemon. 0 = unchanged.
    pub agent_nice: i32,
    /// Added to the daemon's `oom_score_adj` for agent processes: under memory pressure the kernel
    /// kills an agent before it touches the daemon. 0 = unchanged.
    pub agent_oom_score_adj: i32,
    /// Lowest best-effort I/O priority for agent processes.
    pub agent_low_io: bool,
    /// Compact an agent's context after a turn once it fills this share of
    /// the context window (percent). 0 disables daemon-triggered compaction.
    pub auto_compact_pct: u32,
    /// Also compact once the context reaches this many tokens, whatever the
    /// window: every model call uploads the whole context, so a smaller one
    /// saves egress (and cache-read cost). 0 = no cap.
    pub auto_compact_tokens: u64,
    /// Context window assumed for Claude models (1M for `[1m]` models).
    pub claude_context_window: u64,
    /// Automatic retries of a failed, empty or stalled turn (0 = off).
    pub retry_max: u32,
    /// First retry delay; doubles each attempt (capped at 60s).
    pub retry_base: Duration,
    /// A running turn with no output for this long is treated as stuck and
    /// retried (0 = off). Long-running tool calls are exempt.
    pub stall_timeout: Duration,
    /// When the account's usage limit stops a turn, wait for the reset and
    /// continue automatically (instead of failing the turn).
    pub resume_after_limit: bool,
    /// How often to try again when the limit's reset time is unknown.
    pub limit_poll: Duration,
    /// What happens to a project's directory when the project is removed.
    pub delete_project_files: crate::cleanup::Policy,
    /// Timelines of agents untouched for this long are dropped from memory.
    pub timeline_unload: Duration,
    /// Instructions added to every agent; `PASEO_DEFAULT_SYSTEM_PROMPT` overrides
    /// it, and setting it to an empty string disables it.
    pub default_system_prompt: Option<String>,
    /// Where edge machines' folders appear on main (`PASEO_EDGES_DIR`): an agent
    /// whose folder is under `<edges_dir>/<edge>/` runs on that edge.
    pub edges_dir: PathBuf,
}

fn env(key: &str) -> Option<String> {
    std::env::var(key).ok().map(|v| v.trim().to_string()).filter(|v| !v.is_empty())
}

fn env_bool(key: &str) -> Option<bool> {
    env(key).map(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
}

fn env_secs(key: &str, default: u64) -> Duration {
    Duration::from_secs(env(key).and_then(|v| v.parse().ok()).unwrap_or(default))
}

impl Config {
    pub fn load(args: &[String]) -> Config {
        let home = env("PASEO_LITE_HOME").map(PathBuf::from).unwrap_or_else(|| {
            let base = env("HOME").unwrap_or_else(|| ".".into());
            PathBuf::from(base).join(".paseo-lite")
        });
        let mut listen = env("PASEO_LISTEN").unwrap_or_else(|| "127.0.0.1:6768".into());
        let mut relay_enabled = env_bool("PASEO_RELAY_ENABLED").or_else(|| env_bool("PASEO_RELAY")).unwrap_or(true);
        let mut i = 0;
        while i < args.len() {
            match args[i].as_str() {
                "--listen" if i + 1 < args.len() => {
                    listen = args[i + 1].clone();
                    i += 1;
                }
                "--no-relay" => relay_enabled = false,
                _ => {}
            }
            i += 1;
        }
        let relay_endpoint = env("PASEO_RELAY_ENDPOINT").unwrap_or_else(|| "relay.paseo.sh:443".into());
        let default_tls = relay_endpoint.ends_with(":443");
        let relay_use_tls = env_bool("PASEO_RELAY_USE_TLS").unwrap_or(default_tls);
        let relay_public_endpoint = env("PASEO_RELAY_PUBLIC_ENDPOINT").unwrap_or_else(|| relay_endpoint.clone());
        let relay_public_use_tls = env_bool("PASEO_RELAY_PUBLIC_USE_TLS").unwrap_or(relay_use_tls);
        Config {
            listen,
            password: env("PASEO_PASSWORD"),
            relay_enabled,
            hostnames: env("PASEO_HOSTNAMES")
                .map(|v| v.split(',').map(|h| h.trim().to_ascii_lowercase()).filter(|h| !h.is_empty()).collect())
                .unwrap_or_default(),
            relay_endpoint,
            relay_use_tls,
            relay_public_endpoint,
            relay_public_use_tls,
            app_base_url: env("PASEO_APP_BASE_URL").unwrap_or_else(|| "https://app.paseo.sh".into()),
            claude_bin: env("PASEO_CLAUDE_BIN").unwrap_or_else(|| "claude".into()),
            codex_bin: env("PASEO_CODEX_BIN").unwrap_or_else(|| "codex".into()),
            claude_idle: env_secs("PASEO_CLAUDE_IDLE_SECS", 90),
            claude_max_live: crate::capacity::Mode::parse(env("PASEO_CLAUDE_MAX_LIVE").as_deref()),
            claude_keep_warm: env_bool("PASEO_CLAUDE_KEEP_WARM").unwrap_or(true),
            codex_idle: env_secs("PASEO_CODEX_IDLE_SECS", 300),
            codex_max_active: crate::capacity::Mode::parse(env("PASEO_CODEX_MAX_ACTIVE").as_deref()),
            agent_mb: env("PASEO_AGENT_MB").and_then(|v| v.parse().ok()).unwrap_or(300).max(50),
            auto_max_agents: env("PASEO_AUTO_MAX_AGENTS").and_then(|v| v.parse().ok()).unwrap_or(8).max(1),
            auto_stable: env_secs("PASEO_AUTO_STABLE_SECS", 60),
            codex_keep_warm: env_bool("PASEO_CODEX_KEEP_WARM").unwrap_or(true),
            agent_nice: env("PASEO_AGENT_NICE").and_then(|v| v.parse().ok()).unwrap_or(10).clamp(0, 19),
            agent_oom_score_adj: env("PASEO_AGENT_OOM_SCORE_ADJ").and_then(|v| v.parse().ok()).unwrap_or(500).clamp(0, 1000),
            agent_low_io: env_bool("PASEO_AGENT_LOW_IO").unwrap_or(true),
            auto_compact_pct: env("PASEO_AUTO_COMPACT_PCT").and_then(|v| v.parse().ok()).unwrap_or(70).min(99),
            auto_compact_tokens: env("PASEO_AUTO_COMPACT_TOKENS").and_then(|v| v.parse().ok()).unwrap_or(150_000),
            claude_context_window: env("PASEO_CLAUDE_CONTEXT_WINDOW").and_then(|v| v.parse().ok()).unwrap_or(200_000),
            retry_max: env("PASEO_RETRY_MAX").and_then(|v| v.parse().ok()).unwrap_or(3),
            retry_base: env_secs("PASEO_RETRY_BASE_SECS", 5),
            stall_timeout: env_secs("PASEO_STALL_SECS", 600),
            resume_after_limit: env_bool("PASEO_RESUME_AFTER_LIMIT").unwrap_or(true),
            limit_poll: env_secs("PASEO_LIMIT_POLL_SECS", 1800).max(Duration::from_secs(1)),
            delete_project_files: crate::cleanup::Policy::parse(env("PASEO_DELETE_PROJECT_FILES").as_deref()),
            timeline_unload: env_secs("PASEO_TIMELINE_UNLOAD_SECS", 600),
            default_system_prompt: match std::env::var("PASEO_DEFAULT_SYSTEM_PROMPT") {
                Ok(v) if v.trim().is_empty() => None,
                Ok(v) => Some(v),
                Err(_) => Some(DEFAULT_SYSTEM_PROMPT.to_string()),
            },
            edges_dir: env("PASEO_EDGES_DIR").map(PathBuf::from).unwrap_or_else(|| crate::edge::hub::default_edges_dir(&home)),
            home,
        }
    }
}
