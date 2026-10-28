//! Benchmark: the release daemon inside Docker containers sized like the small
//! machines it targets (memory, CPU quota, no swap), driven over WebSocket like
//! the app. paseo-lite reads its limits from the container's cgroup, so each
//! container also exercises the auto capacity it picks there.
//!
//! A fake `claude` (mawk, as in the image) stands in for Claude Code: it holds `BENCH_CLAUDE_MB`
//! of memory like a real process and answers each prompt with a streamed turn
//! (text deltas, one tool call, a final message) of about one second.
//!
//! Ignored by `cargo test`; run it with
//! `cargo test --release --test bench -- --ignored --nocapture` (Linux with
//! Docker, `ubuntu:24.04` pulled). `.github/workflows/bench.yml` runs it on
//! amd64 and arm64 runners and writes the tables into the README.
#![cfg(target_os = "linux")]

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fmt::Write as _;
use std::io::Write as _;
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_tungstenite::tungstenite::Message;

const BIN: &str = env!("CARGO_BIN_EXE_paseo-lite");
const IMAGE: &str = "ubuntu:24.04";
/// Chats started at once in the burst, and the turns each one runs.
const BURST_AGENTS: usize = 6;
const BURST_TURNS: usize = 3;
/// Turns of the single chat (the first one starts the process).
const SEQUENTIAL_TURNS: usize = 10;
/// Auto capacity waits this long with work queued before adding a worker
/// (the default is 60 s, longer than a whole run here).
const AUTO_STABLE_SECS: &str = "10";
/// Give up on a scenario after this long.
const SCENARIO_LIMIT: Duration = Duration::from_secs(15 * 60);

/// A machine to simulate: Docker `--memory` (swap off) and `--cpus`.
struct System {
    id: &'static str,
    label: &'static str,
    memory: &'static str,
    cpus: &'static str,
}

const SYSTEMS: &[System] = &[
    System { id: "gcp-e2-micro", label: "GCP e2-micro (free tier): 1 GB, 0.25 vCPU sustained", memory: "1g", cpus: "0.25" },
    System { id: "vps-512m", label: "512 MB VPS: 512 MB, 1 vCPU", memory: "512m", cpus: "1" },
    System { id: "vps-2c-4g", label: "Small VPS: 4 GB, 2 vCPU", memory: "4g", cpus: "2" },
    System { id: "sbc-1g", label: "1 GB single-board computer (Orange Pi Zero 3, Raspberry Pi 3): 1 GB, 4 cores", memory: "1g", cpus: "4" },
    System { id: "oci-a1", label: "OCI Ampere A1 (free tier, scaled to the runner): 12 GB, 4 OCPU", memory: "12g", cpus: "4" },
];

/// The fake Claude Code CLI. Every stdin line that is a user message gets one
/// turn; everything else (the initialize control request) is ignored.
fn fake_claude(dir: &Path) -> PathBuf {
    let path = dir.join("fake-claude");
    let script = r##"#!/bin/sh
exec awk -W interactive -v mb="${BENCH_CLAUDE_MB:-200}" '
function out(line) { print line; fflush() }
function turn(   i) {
  n++
  out("{\"type\":\"stream_event\",\"event\":{\"type\":\"message_start\",\"message\":{\"id\":\"m" n "a\"}}}")
  for (i = 0; i < 5; i++) {
    out("{\"type\":\"stream_event\",\"event\":{\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Looking at chunk " i ". \"}}}")
    system("sleep 0.1")
  }
  out("{\"type\":\"assistant\",\"message\":{\"id\":\"m" n "a\",\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"Looking.\"},{\"type\":\"tool_use\",\"id\":\"toolu_" n "\",\"name\":\"Bash\",\"input\":{\"command\":\"ls src\"}}]}}")
  system("sleep 0.3")
  out("{\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"tool_result\",\"tool_use_id\":\"toolu_" n "\",\"content\":\"main.rs\\nlib.rs\"}]}}")
  out("{\"type\":\"assistant\",\"message\":{\"id\":\"m" n "b\",\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"Done: two files.\"}],\"usage\":{\"input_tokens\":1200,\"output_tokens\":80}}}")
  out("{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"usage\":{\"input_tokens\":1200,\"output_tokens\":80},\"total_cost_usd\":0.001,\"session_id\":\"bench-session\",\"modelUsage\":{\"claude-bench\":{\"inputTokens\":1200,\"outputTokens\":80,\"cacheReadInputTokens\":0,\"cacheCreationInputTokens\":0,\"costUSD\":0.001}}}")
}
BEGIN {
  # Hold (and touch) about mb MiB, as a real CLI process would.
  s = "x"; while (length(s) < 1048576) s = s s
  chunk = s; while (length(chunk) < 16777216) chunk = chunk chunk
  for (i = 0; i * 16 < mb; i++) hold[i] = chunk i
}
/"type":"user"/ { turn() }
'
"##;
    std::fs::write(&path, script).unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path
}

fn docker(args: &[&str]) -> String {
    let out = Command::new("docker").args(args).output().expect("run docker");
    assert!(out.status.success(), "docker {args:?}: {}", String::from_utf8_lossy(&out.stderr));
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// The daemon in a resource-limited container; removed with its data on drop.
struct Container {
    name: String,
    port: u16,
    pid: u32,
    cgroup: PathBuf,
    dir: PathBuf,
}

impl Drop for Container {
    fn drop(&mut self) {
        let _ = Command::new("docker").args(["rm", "-f", &self.name]).output();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn start(sys: &System, claude_mb: &str) -> (Container, Duration) {
    let dir = std::env::temp_dir().join(format!("paseo-lite-bench-{}-{}", sys.id, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("home")).unwrap();
    std::fs::create_dir_all(dir.join("work")).unwrap();
    let claude = fake_claude(&dir);
    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let name = format!("paseo-lite-bench-{}-{}", sys.id, std::process::id());
    let user = format!("{}:{}", unsafe { libc::getuid() }, unsafe { libc::getgid() });
    let d = dir.display().to_string();
    let env = [
        format!("HOME={d}/home"),
        format!("PASEO_LITE_HOME={d}/home"),
        format!("PASEO_CLAUDE_BIN={}", claude.display()),
        "PASEO_CODEX_BIN=/nonexistent/codex".into(),
        "PASEO_DEFAULT_SYSTEM_PROMPT=".into(),
        "PASEO_CLAUDE_SUBAGENT_PROMPT=".into(),
        format!("PASEO_AUTO_STABLE_SECS={AUTO_STABLE_SECS}"),
        format!("BENCH_CLAUDE_MB={claude_mb}"),
    ];
    let mut args: Vec<String> = ["run", "-d", "--name", name.as_str(), "--network", "host", "--memory", sys.memory, "--memory-swap", sys.memory, "--cpus", sys.cpus, "--user", user.as_str()]
        .iter()
        .map(|s| s.to_string())
        .collect();
    args.extend(["-v".into(), format!("{d}:{d}"), "-v".into(), format!("{BIN}:/usr/local/bin/paseo-lite:ro")]);
    for e in &env {
        args.extend(["-e".into(), e.clone()]);
    }
    args.extend([IMAGE.into(), "paseo-lite".into(), "--listen".into(), format!("127.0.0.1:{port}"), "--no-relay".into()]);
    let started = Instant::now();
    docker(&args.iter().map(String::as_str).collect::<Vec<_>>());
    let pid: u32 = docker(&["inspect", "-f", "{{.State.Pid}}", name.as_str()]).parse().expect("container pid");
    // cgroup v2: "0::/system.slice/docker-<id>.scope"
    let cg = std::fs::read_to_string(format!("/proc/{pid}/cgroup")).unwrap();
    let rel = cg.lines().find_map(|l| l.strip_prefix("0::")).expect("cgroup v2").trim().trim_start_matches('/').to_string();
    let container = Container { name, port, pid, cgroup: Path::new("/sys/fs/cgroup").join(rel), dir };
    while started.elapsed() < Duration::from_secs(120) {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return (container, started.elapsed());
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let logs = Command::new("docker").args(["logs", "--tail", "40", container.name.as_str()]).output().map(|o| String::from_utf8_lossy(&o.stderr).to_string()).unwrap_or_default();
    panic!("{}: daemon did not start listening on {port}\n{logs}", sys.id);
}

fn read_num(path: &Path) -> u64 {
    std::fs::read_to_string(path).ok().and_then(|v| v.trim().parse().ok()).unwrap_or(0)
}

fn rss_kb(pid: u32) -> u64 {
    std::fs::read_to_string(format!("/proc/{pid}/status"))
        .ok()
        .and_then(|s| s.lines().find_map(|l| l.strip_prefix("VmRSS:")).and_then(|v| v.split_whitespace().next()).and_then(|v| v.parse().ok()))
        .unwrap_or(0)
}

/// `oom_kill` from the container's `memory.events`.
fn oom_kills(cgroup: &Path) -> u64 {
    std::fs::read_to_string(cgroup.join("memory.events"))
        .ok()
        .and_then(|t| t.lines().find_map(|l| l.strip_prefix("oom_kill ")).and_then(|v| v.trim().parse().ok()))
        .unwrap_or(0)
}

/// Polls the daemon's RSS and the whole container's memory every 20 ms on its
/// own thread and keeps the highest values.
struct Sampler {
    stop: Arc<AtomicBool>,
    daemon: Arc<AtomicU64>,
    total: Arc<AtomicU64>,
    thread: std::thread::JoinHandle<()>,
}

impl Sampler {
    fn start(c: &Container) -> Sampler {
        let (stop, daemon, total) = (Arc::new(AtomicBool::new(false)), Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));
        let (s, d, t, pid, current) = (stop.clone(), daemon.clone(), total.clone(), c.pid, c.cgroup.join("memory.current"));
        let thread = std::thread::spawn(move || {
            while !s.load(Ordering::Relaxed) {
                d.fetch_max(rss_kb(pid), Ordering::Relaxed);
                t.fetch_max(read_num(&current) / 1024, Ordering::Relaxed);
                std::thread::sleep(Duration::from_millis(20));
            }
        });
        Sampler { stop, daemon, total, thread }
    }

    /// Peak daemon RSS and peak container memory, in KiB.
    fn finish(self) -> (u64, u64) {
        self.stop.store(true, Ordering::Relaxed);
        self.thread.join().unwrap();
        (self.daemon.load(Ordering::Relaxed), self.total.load(Ordering::Relaxed))
    }
}

type Ws = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn connect(port: u16) -> Ws {
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/ws")).await.expect("ws connect");
    send(&mut ws, json!({ "type": "hello", "protocolVersion": 1, "clientId": "bench", "clientType": "cli" })).await;
    recv(&mut ws, Duration::from_secs(60)).await.expect("hello reply");
    ws
}

async fn send(ws: &mut Ws, v: Value) {
    ws.send(Message::Text(v.to_string())).await.unwrap();
}

fn session(msg: Value) -> Value {
    json!({ "type": "session", "message": msg })
}

async fn recv(ws: &mut Ws, wait: Duration) -> Option<Value> {
    loop {
        match tokio::time::timeout(wait, ws.next()).await.ok()? {
            Some(Ok(Message::Text(t))) => return serde_json::from_str(&t).ok(),
            Some(Ok(_)) => continue,
            _ => return None,
        }
    }
}

/// The daemon's own view: process counts and the capacity it chose.
async fn runtime(ws: &mut Ws) -> Value {
    send(ws, session(json!({ "type": "daemon.get_status.request", "requestId": "status" }))).await;
    while let Some(v) = recv(ws, Duration::from_secs(30)).await {
        if v["message"]["type"] == "daemon.get_status.response" {
            return v["message"]["payload"]["runtime"].clone();
        }
    }
    Value::Null
}

/// Polls the daemon status on its own connection and keeps the most `claude`
/// processes seen at once and the highest capacity limit.
fn watch(port: u16, stop: Arc<AtomicBool>) -> tokio::task::JoinHandle<(u64, u64)> {
    tokio::spawn(async move {
        let mut ws = connect(port).await;
        let (mut live, mut limit) = (0, 0);
        while !stop.load(Ordering::Relaxed) {
            let r = runtime(&mut ws).await;
            live = live.max(r["claude"]["liveProcesses"].as_u64().unwrap_or(0));
            limit = limit.max(r["capacity"]["limit"].as_u64().unwrap_or(0));
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        (live, limit)
    })
}

/// One scenario's results.
struct Run {
    name: String,
    latencies: Vec<Duration>,
    errors: Vec<String>,
    wall: Duration,
    peak_daemon_kb: u64,
    peak_total_kb: u64,
    peak_live: u64,
}

impl Run {
    fn percentile(&self, p: f64) -> Duration {
        let mut l = self.latencies.clone();
        l.sort();
        l.get(((l.len().max(1) - 1) as f64 * p).round() as usize).copied().unwrap_or_default()
    }
}

/// Start `agents` chats at once; each runs `turns` turns back to back.
/// Returns every turn's latency (prompt sent to turn finished, queueing included).
async fn drive(c: &Container, ws: &mut Ws, tag: &str, agents: usize, turns: usize) -> (Vec<Duration>, Vec<String>, Duration) {
    let cwd = c.dir.join("work").display().to_string();
    let started = Instant::now();
    let mut created: HashMap<String, Instant> = HashMap::new();
    for i in 0..agents {
        let rid = format!("{tag}-{i}");
        created.insert(rid.clone(), Instant::now());
        let msg = json!({
            "type": "create_agent_request",
            "requestId": rid,
            "config": { "provider": "claude", "cwd": cwd, "modeId": "bypassPermissions" },
            "initialPrompt": "List the source files and summarize them.",
        });
        send(ws, session(msg)).await;
    }
    // agent id -> (when its current prompt went out, turns finished)
    let mut chats: HashMap<String, (Instant, usize)> = HashMap::new();
    let (mut latencies, mut errors) = (Vec::new(), Vec::new());
    while latencies.len() < agents * turns {
        let left = SCENARIO_LIMIT.saturating_sub(started.elapsed());
        let Some(v) = recv(ws, left).await else {
            panic!("{tag}: {} of {} turns finished in {:?}", latencies.len(), agents * turns, started.elapsed());
        };
        let (kind, p) = (v["message"]["type"].as_str().unwrap_or(""), &v["message"]["payload"]);
        if kind == "status" && p["status"] == "agent_created" {
            if let (Some(sent), Some(id)) = (p["requestId"].as_str().and_then(|r| created.get(r)), p["agentId"].as_str()) {
                chats.insert(id.to_string(), (*sent, 0));
            }
            continue;
        }
        let event = &p["event"];
        let end = event["type"].as_str().unwrap_or("");
        if kind != "agent_stream" || !matches!(end, "turn_completed" | "turn_failed" | "turn_canceled") {
            continue;
        }
        let Some(id) = p["agentId"].as_str() else { continue };
        let Some((sent, done)) = chats.get_mut(id) else { continue };
        latencies.push(sent.elapsed());
        if end != "turn_completed" {
            errors.push(format!("{end}: {}", event["error"].as_str().or(event["reason"].as_str()).unwrap_or("")));
        }
        *done += 1;
        if *done < turns {
            *sent = Instant::now();
            let msg = json!({ "type": "send_agent_message_request", "requestId": format!("{tag}-{id}-{done}"), "agentId": id, "text": format!("Now check part {done}.") });
            send(ws, session(msg)).await;
        }
    }
    (latencies, errors, started.elapsed())
}

async fn scenario(c: &Container, ws: &mut Ws, name: &str, tag: &str, agents: usize, turns: usize) -> Run {
    let sampler = Sampler::start(c);
    let stop = Arc::new(AtomicBool::new(false));
    let watcher = watch(c.port, stop.clone());
    let (latencies, errors, wall) = drive(c, ws, tag, agents, turns).await;
    stop.store(true, Ordering::Relaxed);
    let (peak_live, _) = watcher.await.unwrap_or_default();
    let (peak_daemon_kb, peak_total_kb) = sampler.finish();
    Run { name: name.to_string(), latencies, errors, wall, peak_daemon_kb, peak_total_kb, peak_live }
}

fn mib(kb: u64) -> String {
    format!("{:.1} MiB", kb as f64 / 1024.0)
}

fn secs(d: Duration) -> String {
    format!("{:.2} s", d.as_secs_f64())
}

/// What the README's summary quotes from the first system benchmarked.
struct Summary {
    label: String,
    ready: Duration,
    idle_kb: u64,
    warm_p50: Duration,
    burst: String,
    burst_wall: Duration,
    peak_daemon_kb: u64,
    errors: usize,
}

async fn bench(sys: &System, claude_mb: &str) -> (String, usize, Summary) {
    let (c, ready) = start(sys, claude_mb);
    let mut ws = connect(c.port).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let idle_kb = rss_kb(c.pid);
    let idle_total_kb = read_num(&c.cgroup.join("memory.current")) / 1024;

    let mut runs = Vec::new();
    let single = scenario(&c, &mut ws, "", "seq", 1, SEQUENTIAL_TURNS).await;
    // The first turn starts the CLI process; report it apart from the warm ones.
    let mut warm = single.latencies.clone();
    let cold = warm.remove(0);
    runs.push(Run { name: "1 chat, first turn (starts the CLI)".into(), latencies: vec![cold], errors: Vec::new(), wall: cold, peak_daemon_kb: single.peak_daemon_kb, peak_total_kb: single.peak_total_kb, peak_live: single.peak_live });
    runs.push(Run { name: format!("1 chat, {} more turns", warm.len()), wall: warm.iter().sum(), latencies: warm, ..single });
    let burst_name = format!("{BURST_AGENTS} chats at once, {BURST_TURNS} turns each");
    runs.push(scenario(&c, &mut ws, &burst_name, "burst", BURST_AGENTS, BURST_TURNS).await);

    tokio::time::sleep(Duration::from_secs(2)).await;
    let rt = runtime(&mut ws).await;
    let cap = &rt["capacity"];
    let ooms = oom_kills(&c.cgroup);

    let mut md = String::new();
    let _ = writeln!(md, "### {}\n", sys.label);
    let _ = writeln!(md, "`--memory {} --cpus {}` (`{}`). Listening {} after `docker run`; idle 3 s later: daemon {} RSS, container {}.\n", sys.memory, sys.cpus, sys.id, secs(ready), mib(idle_kb), mib(idle_total_kb));
    let _ = writeln!(md, "| Scenario | Turns | Errors | Wall | p50 turn | p99 turn | Peak daemon RSS | Peak container memory | Peak `claude` processes |");
    let _ = writeln!(md, "|---|---:|---:|---:|---:|---:|---:|---:|---:|");
    for r in &runs {
        let _ = writeln!(
            md,
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} |",
            r.name,
            r.latencies.len(),
            r.errors.len(),
            secs(r.wall),
            secs(r.percentile(0.5)),
            secs(r.percentile(0.99)),
            mib(r.peak_daemon_kb),
            mib(r.peak_total_kb),
            r.peak_live,
        );
    }
    let _ = writeln!(
        md,
        "\nAuto capacity: limit {} (ceiling {}) from {} MB and {} CPU(s) seen in the cgroup. OOM kills: {ooms}.",
        cap["limit"],
        cap["ceiling"],
        cap["memLimitMb"],
        cap["cpus"],
    );
    let errors: Vec<&String> = runs.iter().flat_map(|r| &r.errors).collect();
    if let Some(first) = errors.first() {
        let _ = writeln!(md, "\nFirst error: `{first}`");
    }
    md.push('\n');
    let burst = runs.last().unwrap();
    let summary = Summary {
        label: sys.label.split(':').next().unwrap_or(sys.label).to_string(),
        ready,
        idle_kb,
        warm_p50: runs[1].percentile(0.5),
        burst: burst_name,
        burst_wall: burst.wall,
        peak_daemon_kb: runs.iter().map(|r| r.peak_daemon_kb).max().unwrap_or(0),
        errors: errors.len() + ooms as usize,
    };
    (md, errors.len() + ooms as usize, summary)
}

#[tokio::test]
#[ignore = "benchmark; needs Docker: cargo test --release --test bench -- --ignored --nocapture"]
async fn small_machines() {
    let claude_mb = std::env::var("BENCH_CLAUDE_MB").unwrap_or_else(|_| "200".into());
    let wanted = std::env::var("BENCH_SYSTEMS").unwrap_or_else(|_| "gcp-e2-micro,vps-512m,vps-2c-4g".into());
    let systems: Vec<&System> = wanted
        .split(',')
        .map(str::trim)
        .filter(|w| !w.is_empty())
        .map(|w| SYSTEMS.iter().find(|s| s.id == w).unwrap_or_else(|| panic!("unknown system {w}")))
        .collect();
    let size_mib = std::fs::metadata(BIN).map(|m| m.len()).unwrap_or(0) as f64 / (1024.0 * 1024.0);
    let sha = std::env::var("GITHUB_SHA").unwrap_or_default();
    let commit = if sha.is_empty() { String::new() } else { format!(" at commit `{}`", &sha[..sha.len().min(7)]) };
    let date = humantime::format_rfc3339_seconds(std::time::SystemTime::now()).to_string();

    let mut md = String::new();
    let _ = writeln!(
        md,
        "#### {} runner\n\nMeasured {} on {}{commit}, binary {size_mib:.1} MiB. The fake `claude` holds {claude_mb} MiB and streams \
         each turn in about 1 s (5 text deltas, a tool call, a final message).\n",
        std::env::consts::ARCH,
        &date[..10],
        std::env::consts::ARCH,
    );
    let mut errors = 0;
    let mut first = None;
    for sys in systems {
        let (section, e, summary) = bench(sys, &claude_mb).await;
        print!("{section}");
        md.push_str(&section);
        errors += e;
        first.get_or_insert(summary);
    }

    println!("{md}");
    if let Ok(path) = std::env::var("GITHUB_STEP_SUMMARY") {
        let mut f = std::fs::OpenOptions::new().append(true).create(true).open(path).unwrap();
        writeln!(f, "## paseo-lite benchmark\n\n{md}").unwrap();
    }
    // The workflow collects these into the README.
    if let Ok(path) = std::env::var("BENCH_OUT") {
        std::fs::write(path, md.trim()).unwrap();
    }
    if let (Ok(path), Some(s)) = (std::env::var("BENCH_SUMMARY_OUT"), first) {
        let line = format!(
            "On a simulated {} the daemon is listening {} after start and idles at {} RSS; a follow-up turn takes {} end to end, \
             and {} finish in {} with the daemon peaking at {} RSS ({} errors; see [Benchmark](#benchmark)).",
            s.label,
            secs(s.ready),
            mib(s.idle_kb),
            secs(s.warm_p50),
            s.burst,
            secs(s.burst_wall),
            mib(s.peak_daemon_kb),
            s.errors,
        );
        std::fs::write(path, line).unwrap();
    }
    assert_eq!(errors, 0, "some turns failed or a process was OOM-killed; see the report above");
}
