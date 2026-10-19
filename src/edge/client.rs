//! `paseo-lite edge run`: the edge machine side.
//!
//! Connects out to main through the relay (no open ports), runs the agent
//! processes main asks for in this machine's workspace, and gives them a
//! local model proxy. Nothing secret lives here: the agents get a dummy API
//! key, and their model calls cross the relay to main, which adds the real
//! one. When main is unreachable, every agent process is stopped.

use super::wire::{self, JoinCode, Reassembler};
use crate::e2ee::{Channel, DaemonIdentity};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const READY_TIMEOUT: Duration = Duration::from_secs(20);
const PING_EVERY: Duration = Duration::from_secs(15);
/// No answer from main for this long: the link is dead, reconnect.
const STALE_AFTER: Duration = Duration::from_secs(60);
const STATUS_EVERY: Duration = Duration::from_secs(60);
const BACKOFF_MAX: Duration = Duration::from_secs(30);
/// Placeholder key for agents here; main swaps in the real credentials.
const DUMMY_KEY: &str = "paseo-edge-no-key-here";
/// Credentials an agent here must never inherit from this machine.
const STRIPPED_ENV: &[&str] = &["ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN", "CLAUDE_CODE_OAUTH_TOKEN", "ANTHROPIC_BASE_URL", "PASEO_EDGE_JOIN"];
/// Reasons main gives for refusing this edge that retrying cannot fix.
const FATAL: &[&str] = &["not_registered", "invalid_or_expired_code", "name_taken", "not_an_edge"];

struct Settings {
    workspace: PathBuf,
    claude_bin: String,
}

enum HttpEvent {
    Data(Vec<u8>),
    End(Option<String>),
}

#[derive(Default)]
struct Shared {
    link: Option<mpsc::UnboundedSender<String>>,
    http: HashMap<String, mpsc::UnboundedSender<HttpEvent>>,
    next_rid: u64,
}

type SharedRef = Arc<Mutex<Shared>>;

enum Stop {
    Retry(String),
    Fatal(String),
}

fn env(key: &str) -> Option<String> {
    std::env::var(key).ok().map(|v| v.trim().to_string()).filter(|v| !v.is_empty())
}

fn code_path(home: &Path) -> PathBuf {
    home.join("edge-join.json")
}

/// Run as an edge until main removes it. `code`: a join code from
/// `paseo-lite edge invite` (or `PASEO_EDGE_JOIN`); later runs reuse the saved one.
pub async fn run(cfg: crate::config::Config, code: Option<String>) -> Result<(), String> {
    let home = cfg.home.clone();
    let code = match code.or_else(|| env("PASEO_EDGE_JOIN")) {
        // Saved for later runs. Passing the same code again is harmless: once this
        // edge's key has joined, main recognises the key and ignores the used token.
        Some(c) => {
            let code = JoinCode::decode(&c)?;
            let json = serde_json::to_vec_pretty(&code).unwrap_or_default();
            crate::util::write_atomic(&code_path(&home), &json, true).map_err(|e| format!("cannot save the join code: {e}"))?;
            code
        }
        None => {
            let raw = std::fs::read(code_path(&home)).map_err(|_| "no join code: run `paseo-lite edge invite <name>` on main and pass the code here".to_string())?;
            serde_json::from_slice(&raw).map_err(|_| "the saved join code is damaged; join again with a new code".to_string())?
        }
    };
    let identity = Arc::new(DaemonIdentity::load_or_create(&home.join("edge-client")).map_err(|e| format!("cannot create this edge's key: {e}"))?);
    let workspace = env("PASEO_EDGE_WORKSPACE").map(PathBuf::from).unwrap_or_else(|| {
        let w = Path::new("/workspace");
        if w.is_dir() { w.to_path_buf() } else { PathBuf::from(env("HOME").unwrap_or_else(|| ".".into())).join("workspace") }
    });
    std::fs::create_dir_all(&workspace).map_err(|e| format!("cannot use the workspace {}: {e}", workspace.display()))?;
    let settings = Arc::new(Settings { workspace, claude_bin: cfg.claude_bin.clone() });

    let shared: SharedRef = Arc::new(Mutex::new(Shared::default()));
    let proxy_port = start_proxy(shared.clone()).await?;
    crate::log!("edge: workspace {}, model proxy on 127.0.0.1:{proxy_port}", settings.workspace.display());

    let mut backoff = Duration::from_secs(1);
    loop {
        let started = Instant::now();
        match session(&code, &identity, &settings, &shared, proxy_port).await {
            Stop::Fatal(reason) => return Err(format!("main refused this edge ({reason}). Ask main for a new code: paseo-lite edge invite <name>")),
            Stop::Retry(why) => {
                if started.elapsed() > Duration::from_secs(30) {
                    backoff = Duration::from_secs(1);
                }
                crate::warn!("edge: link to main lost ({why}); retrying in {}s", backoff.as_secs());
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(BACKOFF_MAX);
            }
        }
    }
}

fn relay_url(code: &JoinCode) -> String {
    let scheme = if code.tls { "wss" } else { "ws" };
    let enc = |s: &str| s.chars().filter(|c| c.is_ascii_alphanumeric() || "-_.~".contains(*c)).collect::<String>();
    format!("{scheme}://{}/ws?serverId={}&role=client&v=2", code.relay, enc(&code.server_id))
}

/// Workspace folders main may show as projects: names only.
fn list_dirs(workspace: &Path) -> Vec<String> {
    let mut dirs: Vec<String> = std::fs::read_dir(workspace)
        .map(|rd| rd.filter_map(|e| e.ok()).filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false)).filter_map(|e| wire::safe_name(&e.file_name().to_string_lossy())).collect())
        .unwrap_or_default();
    dirs.sort();
    dirs.truncate(200);
    dirs
}

struct LocalProc {
    stdin: Option<mpsc::UnboundedSender<String>>,
    kill: Option<oneshot::Sender<()>>,
}

async fn session(code: &JoinCode, identity: &Arc<DaemonIdentity>, settings: &Arc<Settings>, shared: &SharedRef, proxy_port: u16) -> Stop {
    let ws = match crate::relay::connect(&relay_url(code), CONNECT_TIMEOUT).await {
        Ok(ws) => ws,
        Err(e) => return Stop::Retry(e),
    };
    let (mut sink, mut stream) = ws.split();
    let hello = json!({ "type": "e2ee_hello", "key": identity.public_b64 });
    if let Err(e) = sink.send(Message::Text(hello.to_string())).await {
        return Stop::Retry(e.to_string());
    }
    let ready = tokio::time::timeout(READY_TIMEOUT, async {
        while let Some(frame) = stream.next().await {
            if let Ok(Message::Text(t)) = frame {
                if serde_json::from_str::<Value>(&t).ok().and_then(|v| v.get("type").and_then(|t| t.as_str()).map(|t| t == "e2ee_ready")).unwrap_or(false) {
                    return true;
                }
            }
        }
        false
    })
    .await;
    if !matches!(ready, Ok(true)) {
        return Stop::Retry("main did not answer (is its daemon running?)".into());
    }
    let channel = match Channel::new(&identity.secret, &code.key) {
        Ok(c) => c,
        Err(e) => return Stop::Fatal(format!("bad key in the join code: {e}")),
    };

    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<String>();
    shared.lock().unwrap().link = Some(out_tx.clone());
    let edge_hello = json!({ "type": "edge.hello", "v": 1, "token": code.token, "dirs": list_dirs(&settings.workspace), "version": env!("CARGO_PKG_VERSION") });
    let _ = out_tx.send(edge_hello.to_string());

    let mut procs: HashMap<u64, LocalProc> = HashMap::new();
    let mut reasm = Reassembler::default();
    let mut frag_id = 0u64;
    let mut last_heard = Instant::now();
    let mut ping = tokio::time::interval(PING_EVERY);
    let mut status = tokio::time::interval(STATUS_EVERY);
    let stop = loop {
        tokio::select! {
            frame = stream.next() => {
                let text = match frame {
                    Some(Ok(Message::Text(t))) => match channel.decrypt_text(&t) {
                        Ok(t) => t,
                        Err(e) => break Stop::Retry(e),
                    },
                    Some(Ok(Message::Binary(b))) => match channel.decrypt(&b).and_then(|p| String::from_utf8(p).map_err(|e| e.to_string())) {
                        Ok(t) => t,
                        Err(e) => break Stop::Retry(e),
                    },
                    Some(Ok(Message::Close(_))) | None => break Stop::Retry("closed".into()),
                    Some(Ok(_)) => continue,
                    Some(Err(e)) => break Stop::Retry(e.to_string()),
                };
                last_heard = Instant::now();
                let msg = match reasm.push(text) {
                    Ok(Some(v)) => v,
                    Ok(None) => continue,
                    Err(e) => break Stop::Retry(e),
                };
                if let Some(stop) = handle(msg, &mut procs, settings, shared, &out_tx, proxy_port) {
                    break stop;
                }
            }
            Some(text) = out_rx.recv() => {
                let mut failed = None;
                for frame in wire::split(&text, &mut frag_id) {
                    if let Err(e) = sink.send(Message::Text(channel.encrypt_text(&frame))).await {
                        failed = Some(e.to_string());
                        break;
                    }
                }
                if let Some(e) = failed {
                    break Stop::Retry(e);
                }
            }
            _ = ping.tick() => {
                if last_heard.elapsed() > STALE_AFTER {
                    break Stop::Retry("main stopped answering".into());
                }
                let _ = out_tx.send(r#"{"type":"ping"}"#.to_string());
            }
            _ = status.tick() => {
                let _ = out_tx.send(json!({ "type": "edge.status", "dirs": list_dirs(&settings.workspace) }).to_string());
            }
        }
    };

    // Without main nobody can see or steer these agents: stop them.
    for (_, mut p) in procs.drain() {
        if let Some(kill) = p.kill.take() {
            let _ = kill.send(());
        }
    }
    let mut sh = shared.lock().unwrap();
    sh.link = None;
    for (_, tx) in sh.http.drain() {
        let _ = tx.send(HttpEvent::End(Some("lost the link to main".into())));
    }
    stop
}

/// A message from main. Returns Some to end the session.
fn handle(msg: Value, procs: &mut HashMap<u64, LocalProc>, settings: &Arc<Settings>, shared: &SharedRef, out: &mpsc::UnboundedSender<String>, proxy_port: u16) -> Option<Stop> {
    let pid = msg.get("pid").and_then(|p| p.as_u64());
    match msg.get("type").and_then(|t| t.as_str()).unwrap_or("") {
        "edge.welcome" => {
            let name = msg.get("name").and_then(|n| n.as_str()).unwrap_or("?");
            crate::log!("edge: connected to main as {name}");
            if msg.get("modelProxy").and_then(|m| m.as_bool()) == Some(false) {
                crate::warn!("edge: main has no model credentials, so agents here cannot reach a model");
            }
        }
        "edge.rejected" => {
            let reason = msg.get("reason").and_then(|r| r.as_str()).unwrap_or("unknown").to_string();
            return Some(if FATAL.contains(&reason.as_str()) { Stop::Fatal(reason) } else { Stop::Retry(reason) });
        }
        "proc.spawn" => {
            let pid = pid?;
            match spawn_local(pid, &msg, settings, out, proxy_port) {
                Ok(p) => {
                    procs.insert(pid, p);
                }
                Err(e) => {
                    crate::warn!("edge: cannot start process {pid}: {e}");
                    let _ = out.send(json!({ "type": "proc.exit", "pid": pid, "ok": false, "detail": e }).to_string());
                }
            }
        }
        "proc.stdin" => {
            if let (Some(p), Some(line)) = (pid.and_then(|pid| procs.get(&pid)), msg.get("line").and_then(|l| l.as_str())) {
                if let Some(stdin) = &p.stdin {
                    let _ = stdin.send(line.to_string());
                }
            }
        }
        "proc.eof" => {
            if let Some(p) = pid.and_then(|pid| procs.get_mut(&pid)) {
                p.stdin = None;
            }
        }
        "proc.kill" => {
            if let Some(mut p) = pid.and_then(|pid| procs.remove(&pid)) {
                if let Some(kill) = p.kill.take() {
                    let _ = kill.send(());
                }
            }
        }
        "http.data" | "http.end" => {
            let rid = msg.get("rid").and_then(|r| r.as_str())?;
            let mut sh = shared.lock().unwrap();
            if msg.get("type").and_then(|t| t.as_str()) == Some("http.end") {
                if let Some(tx) = sh.http.remove(rid) {
                    let _ = tx.send(HttpEvent::End(msg.get("error").and_then(|e| e.as_str()).map(str::to_string)));
                }
            } else if let Some(tx) = sh.http.get(rid) {
                use base64::Engine;
                if let Some(Ok(bytes)) = msg.get("data").and_then(|d| d.as_str()).map(|d| base64::engine::general_purpose::STANDARD.decode(d)) {
                    let _ = tx.send(HttpEvent::Data(bytes));
                }
            }
        }
        _ => {}
    }
    // Forget processes that have exited.
    procs.retain(|_, p| p.kill.as_ref().map(|k| !k.is_closed()).unwrap_or(false));
    None
}

fn spawn_local(pid: u64, msg: &Value, settings: &Settings, out: &mpsc::UnboundedSender<String>, proxy_port: u16) -> Result<LocalProc, String> {
    // Main only starts Claude Code here (Codex comes later).
    if msg.get("program").and_then(|p| p.as_str()) != Some("claude") {
        return Err("only claude runs on edge machines for now".into());
    }
    let rel = msg.get("cwd").and_then(|c| c.as_str()).unwrap_or("");
    if Path::new(rel).components().any(|c| !matches!(c, Component::Normal(_))) {
        return Err(format!("bad folder {rel:?}"));
    }
    let cwd = if rel.is_empty() { settings.workspace.clone() } else { settings.workspace.join(rel) };
    std::fs::create_dir_all(&cwd).map_err(|e| format!("cannot create {}: {e}", cwd.display()))?;
    let args: Vec<String> = msg.get("args").and_then(|a| a.as_array()).map(|a| a.iter().filter_map(|v| v.as_str().map(str::to_string)).collect()).unwrap_or_default();

    let mut cmd = tokio::process::Command::new(&settings.claude_bin);
    cmd.args(&args).current_dir(&cwd);
    for k in STRIPPED_ENV {
        cmd.env_remove(k);
    }
    // Only harmless switches from main; never anything that could carry a secret.
    if let Some(pairs) = msg.get("env").and_then(|e| e.as_array()) {
        for p in pairs {
            if let (Some(k), Some(v)) = (p.get(0).and_then(|k| k.as_str()), p.get(1).and_then(|v| v.as_str())) {
                if (k.starts_with("CLAUDE_CODE_DISABLE_") || k.starts_with("DISABLE_")) && !STRIPPED_ENV.contains(&k) {
                    cmd.env(k, v);
                }
            }
        }
    }
    cmd.env("ANTHROPIC_BASE_URL", format!("http://127.0.0.1:{proxy_port}"))
        .env("ANTHROPIC_API_KEY", DUMMY_KEY)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = cmd.spawn().map_err(|e| format!("failed to start {}: {e}", settings.claude_bin))?;
    crate::log!("edge: started claude (process {pid}) in {}", cwd.display());

    let mut stdin = child.stdin.take().ok_or("no stdin")?;
    let stdout = child.stdout.take().ok_or("no stdout")?;
    let mut stderr = child.stderr.take().ok_or("no stderr")?;
    let (in_tx, mut in_rx) = mpsc::unbounded_channel::<String>();
    let (kill_tx, mut kill_rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        while let Some(line) = in_rx.recv().await {
            if stdin.write_all(line.as_bytes()).await.is_err() || stdin.write_all(b"\n").await.is_err() || stdin.flush().await.is_err() {
                break;
            }
        }
    });
    let tail = Arc::new(Mutex::new(String::new()));
    let tail_w = tail.clone();
    tokio::spawn(async move {
        let mut buf = [0u8; 1024];
        while let Ok(n) = stderr.read(&mut buf).await {
            if n == 0 {
                break;
            }
            let mut t = tail_w.lock().unwrap();
            t.push_str(&String::from_utf8_lossy(&buf[..n]));
            if t.len() > 2048 {
                let mut cut = t.len() - 2048;
                while !t.is_char_boundary(cut) {
                    cut += 1;
                }
                t.drain(..cut);
            }
        }
    });
    let out = out.clone();
    tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();
        loop {
            tokio::select! {
                line = lines.next_line() => match line {
                    Ok(Some(line)) => {
                        if out.send(json!({ "type": "proc.out", "pid": pid, "line": line }).to_string()).is_err() {
                            let _ = child.start_kill();
                            break;
                        }
                    }
                    _ => break,
                },
                _ = &mut kill_rx => {
                    let _ = child.start_kill();
                    break;
                }
            }
        }
        let ok = child.wait().await.map(|s| s.success()).unwrap_or(false);
        let detail = tail.lock().unwrap().trim().to_string();
        let _ = out.send(json!({ "type": "proc.exit", "pid": pid, "ok": ok, "detail": detail }).to_string());
    });
    Ok(LocalProc { stdin: Some(in_tx), kill: Some(kill_tx) })
}

/// The model proxy agents here talk to (`ANTHROPIC_BASE_URL`). Loopback only.
async fn start_proxy(shared: SharedRef) -> Result<u16, String> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.map_err(|e| format!("cannot start the model proxy: {e}"))?;
    let port = listener.local_addr().map_err(|e| e.to_string())?.port();
    tokio::spawn(async move {
        loop {
            let Ok((sock, _)) = listener.accept().await else { continue };
            tokio::spawn(proxy_conn(sock, shared.clone()));
        }
    });
    Ok(port)
}

async fn reply_error(sock: &mut tokio::net::TcpStream, status: &str, message: &str) {
    let body = json!({ "type": "error", "error": { "type": "api_error", "message": message } }).to_string();
    let head = format!("HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len());
    let _ = sock.write_all(head.as_bytes()).await;
    let _ = sock.write_all(body.as_bytes()).await;
    let _ = sock.shutdown().await;
}

/// One request per connection: read it, send it to main, stream main's
/// answer back, close.
async fn proxy_conn(mut sock: tokio::net::TcpStream, shared: SharedRef) {
    let mut buf = Vec::new();
    let head_end = loop {
        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break i + 4;
        }
        if buf.len() > 64 * 1024 {
            return reply_error(&mut sock, "431 Request Header Fields Too Large", "request head too large").await;
        }
        let mut chunk = [0u8; 8192];
        match tokio::time::timeout(Duration::from_secs(30), sock.read(&mut chunk)).await {
            Ok(Ok(n)) if n > 0 => buf.extend_from_slice(&chunk[..n]),
            _ => return,
        }
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
    let mut lines = head.split("\r\n");
    let mut first = lines.next().unwrap_or("").split(' ');
    let (method, path) = (first.next().unwrap_or("").to_string(), first.next().unwrap_or("").to_string());
    let mut headers = Vec::new();
    let mut length = 0usize;
    for line in lines.filter(|l| !l.is_empty()) {
        let Some((k, v)) = line.split_once(':') else { continue };
        let (k, v) = (k.trim().to_string(), v.trim().to_string());
        match k.to_ascii_lowercase().as_str() {
            "content-length" => length = v.parse().unwrap_or(usize::MAX),
            "transfer-encoding" => return reply_error(&mut sock, "411 Length Required", "chunked request bodies are not supported").await,
            _ => {}
        }
        headers.push(json!([k, v]));
    }
    if length > super::upstream::MAX_BODY {
        return reply_error(&mut sock, "413 Payload Too Large", "request body too large").await;
    }
    let mut body = buf[head_end..].to_vec();
    while body.len() < length {
        let mut chunk = vec![0u8; 64 * 1024];
        match tokio::time::timeout(Duration::from_secs(60), sock.read(&mut chunk)).await {
            Ok(Ok(n)) if n > 0 => body.extend_from_slice(&chunk[..n]),
            _ => return,
        }
    }
    body.truncate(length);

    let (tx, mut rx) = mpsc::unbounded_channel::<HttpEvent>();
    let (rid, link) = {
        let mut sh = shared.lock().unwrap();
        sh.next_rid += 1;
        let rid = format!("r{}", sh.next_rid);
        sh.http.insert(rid.clone(), tx);
        (rid, sh.link.clone())
    };
    let Some(link) = link else {
        shared.lock().unwrap().http.remove(&rid);
        return reply_error(&mut sock, "503 Service Unavailable", "this edge machine has lost its link to main").await;
    };
    use base64::Engine;
    let request = json!({ "type": "http.request", "rid": rid, "method": method, "path": path, "headers": headers, "body": base64::engine::general_purpose::STANDARD.encode(&body) });
    if link.send(request.to_string()).is_err() {
        shared.lock().unwrap().http.remove(&rid);
        return reply_error(&mut sock, "503 Service Unavailable", "this edge machine has lost its link to main").await;
    }

    // The response head gets `Connection: close`, so the client never tries
    // to reuse this socket for a second request.
    let mut pending_head: Option<Vec<u8>> = Some(Vec::new());
    let mut error = None;
    while let Some(ev) = rx.recv().await {
        match ev {
            HttpEvent::Data(bytes) => {
                let to_write = match pending_head.as_mut() {
                    Some(h) => {
                        h.extend_from_slice(&bytes);
                        match h.windows(4).position(|w| w == b"\r\n\r\n") {
                            Some(i) => {
                                let h = pending_head.take().unwrap_or_default();
                                let mut out = close_head(&h[..i + 4]);
                                out.extend_from_slice(&h[i + 4..]);
                                out
                            }
                            None => continue,
                        }
                    }
                    None => bytes,
                };
                if sock.write_all(&to_write).await.is_err() {
                    break;
                }
            }
            HttpEvent::End(e) => {
                error = e;
                break;
            }
        }
    }
    shared.lock().unwrap().http.remove(&rid);
    if pending_head.is_some() {
        let message = error.unwrap_or_else(|| "main sent no response".into());
        return reply_error(&mut sock, "502 Bad Gateway", &format!("edge model proxy: {message}")).await;
    }
    let _ = sock.shutdown().await;
}

/// Response head with `Connection: close` instead of whatever it said.
fn close_head(head: &[u8]) -> Vec<u8> {
    let text = String::from_utf8_lossy(head);
    let mut out = String::new();
    for line in text.split("\r\n").filter(|l| !l.is_empty()) {
        let lower = line.to_ascii_lowercase();
        if lower.starts_with("connection:") || lower.starts_with("keep-alive:") {
            continue;
        }
        out.push_str(line);
        out.push_str("\r\n");
    }
    out.push_str("Connection: close\r\n\r\n");
    out.into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn response_heads_are_closed() {
        let head = close_head(b"HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nKeep-Alive: timeout=5\r\nContent-Type: text/event-stream\r\n\r\n");
        assert_eq!(String::from_utf8(head).unwrap(), "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n");
    }
}
