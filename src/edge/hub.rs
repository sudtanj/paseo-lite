//! Main side of edge machines.
//!
//! Edges reach main through the relay on a second identity (`edge-hub/`),
//! separate from the one the app pairs with. Every connection on it lands
//! here and nowhere else, so an edge never gets near the app session, its
//! RPCs, the password or the pairing key.
//!
//! The gate is deny-by-default. After a valid `edge.hello` an edge may send
//! only: `ping`, `edge.status`, `proc.out` / `proc.exit` for processes main
//! started on it, and `http.request` for the model proxy. Anything else ends
//! the connection.

use super::registry::Registry;
use super::upstream::Upstream;
use super::wire::{self, Reassembler};
use crate::session::Inbound;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

static HUB: OnceLock<Arc<Hub>> = OnceLock::new();

const HELLO_TIMEOUT: Duration = Duration::from_secs(20);
/// Model requests one edge may have in flight.
const MAX_HTTP_PER_EDGE: usize = 16;
/// Workspace folders an edge may announce.
const MAX_DIRS: usize = 200;
/// How soon `paseo-lite edge rm` cuts a connected edge off.
const REVOKE_CHECK: Duration = Duration::from_secs(10);

pub struct Hub {
    home: PathBuf,
    edges_dir: PathBuf,
    upstream: Option<Upstream>,
    state: Mutex<State>,
    /// Invite redemption is a read-modify-write of edges.json.
    registry_lock: Mutex<()>,
}

#[derive(Default)]
struct State {
    next_conn: u64,
    next_pid: u64,
    links: HashMap<String, Link>,
    procs: HashMap<u64, ProcLink>,
}

struct Link {
    conn: u64,
    out: mpsc::UnboundedSender<String>,
}

struct ProcLink {
    edge: String,
    conn: u64,
    stdout: mpsc::UnboundedSender<String>,
    exit: Option<oneshot::Sender<Exit>>,
}

pub struct Exit {
    pub ok: bool,
    pub detail: String,
}

/// An agent process running on an edge, used like a local child: lines in,
/// lines out, an exit.
pub struct RemoteProc {
    pub stdin: mpsc::UnboundedSender<String>,
    pub stdout: mpsc::UnboundedReceiver<String>,
    pub exit: oneshot::Receiver<Exit>,
    pub kill: RemoteKill,
}

pub struct RemoteKill {
    pid: u64,
}

impl RemoteKill {
    pub fn kill(&self) {
        if let Some(hub) = HUB.get() {
            hub.send_to_proc(self.pid, json!({ "type": "proc.kill", "pid": self.pid }));
        }
    }
}

/// Where an agent with this working directory runs: on an edge when the
/// directory is under `edges_dir/<edge>/`, with the rest of the path relative
/// to that edge's workspace.
pub struct Target {
    pub edge: String,
    pub rel: String,
}

pub fn route(edges_dir: &Path, cwd: &str) -> Option<Result<Target, String>> {
    let rel = Path::new(cwd).strip_prefix(edges_dir).ok()?;
    let mut parts = Vec::new();
    for c in rel.components() {
        match c {
            Component::Normal(p) => parts.push(p.to_string_lossy().into_owned()),
            _ => return Some(Err(format!("{cwd} is not a valid folder on an edge machine"))),
        }
    }
    if parts.is_empty() {
        return Some(Err(format!("pick a folder inside {}/<edge name>/", edges_dir.display())));
    }
    let edge = parts.remove(0);
    Some(Ok(Target { edge, rel: parts.join("/") }))
}

/// Start an agent process on an edge.
pub fn spawn(target: &Target, program: &str, args: &[String], env: &[(String, String)]) -> Result<RemoteProc, String> {
    let hub = HUB.get().ok_or("edge machines are not set up on this daemon (paseo-lite edge invite)")?;
    let (pid, out, stdout_rx, exit_rx) = {
        let mut st = hub.state.lock().unwrap();
        let link = st.links.get(&target.edge).ok_or_else(|| format!("edge machine {} is offline", target.edge))?;
        let (conn, out) = (link.conn, link.out.clone());
        st.next_pid += 1;
        let pid = st.next_pid;
        let (stdout_tx, stdout_rx) = mpsc::unbounded_channel();
        let (exit_tx, exit_rx) = oneshot::channel();
        st.procs.insert(pid, ProcLink { edge: target.edge.clone(), conn, stdout: stdout_tx, exit: Some(exit_tx) });
        (pid, out, stdout_rx, exit_rx)
    };
    let env: Vec<Value> = env.iter().map(|(k, v)| json!([k, v])).collect();
    let spawn = json!({ "type": "proc.spawn", "pid": pid, "program": program, "args": args, "env": env, "cwd": target.rel });
    if out.send(spawn.to_string()).is_err() {
        hub.state.lock().unwrap().procs.remove(&pid);
        return Err(format!("edge machine {} is offline", target.edge));
    }
    let (stdin_tx, mut stdin_rx) = mpsc::unbounded_channel::<String>();
    tokio::spawn(async move {
        while let Some(line) = stdin_rx.recv().await {
            if out.send(json!({ "type": "proc.stdin", "pid": pid, "line": line }).to_string()).is_err() {
                return;
            }
        }
        // Every sender dropped: stdin EOF, as for a local child.
        let _ = out.send(json!({ "type": "proc.eof", "pid": pid }).to_string());
    });
    Ok(RemoteProc { stdin: stdin_tx, stdout: stdout_rx, exit: exit_rx, kill: RemoteKill { pid } })
}

/// Default for `PASEO_EDGES_DIR`.
pub fn default_edges_dir(home: &Path) -> PathBuf {
    let workspace = Path::new("/workspace");
    if workspace.is_dir() { workspace.join("edges") } else { home.join("edges") }
}

/// The edge relay identity (separate from the app's): `<home>/edge-hub/`.
pub fn identity(home: &Path) -> std::io::Result<crate::e2ee::DaemonIdentity> {
    crate::e2ee::DaemonIdentity::load_or_create(&home.join("edge-hub"))
}

/// Serve edges once there is something to serve: an invite or a joined edge
/// (`edges.json`), or `PASEO_EDGE_HUB=1`. Checked every few seconds, so an
/// invite made while the daemon runs is picked up without a restart.
pub async fn run(cfg: crate::config::Config) {
    let forced = std::env::var("PASEO_EDGE_HUB").map(|v| matches!(v.trim(), "1" | "true" | "yes" | "on")).unwrap_or(false);
    while !forced && !super::registry::exists(&cfg.home) {
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    let identity = match identity(&cfg.home) {
        Ok(id) => Arc::new(id),
        Err(e) => {
            crate::warn!("edge machines disabled: cannot create the edge identity: {e}");
            return;
        }
    };
    let upstream = Upstream::from_env();
    if upstream.is_none() {
        crate::warn!("edge machines: no ANTHROPIC_API_KEY, ANTHROPIC_AUTH_TOKEN or CLAUDE_CODE_OAUTH_TOKEN here, so agents on edges cannot reach a model");
    }
    let hub = Arc::new(Hub { home: cfg.home.clone(), edges_dir: cfg.edges_dir.clone(), upstream, state: Mutex::new(State::default()), registry_lock: Mutex::new(()) });
    if HUB.set(hub.clone()).is_err() {
        return;
    }
    crate::log!("edge machines: serving on the relay (folders under {})", cfg.edges_dir.display());
    let handler: crate::relay::Handler = Arc::new(move |inbound: mpsc::Receiver<Inbound>, outbound: mpsc::Sender<Arc<str>>, peer_key: String| -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        Box::pin(serve(hub.clone(), inbound, outbound, peer_key))
    });
    crate::relay::run_with(handler, cfg, identity).await;
}

fn reject(reason: &str) -> Arc<str> {
    Arc::from(json!({ "type": "edge.rejected", "reason": reason }).to_string())
}

/// One edge connection, from the handshake to the end.
async fn serve(hub: Arc<Hub>, mut inbound: mpsc::Receiver<Inbound>, outbound: mpsc::Sender<Arc<str>>, peer_key: String) {
    let mut reasm = Reassembler::default();
    let hello = tokio::time::timeout(HELLO_TIMEOUT, async {
        loop {
            match inbound.recv().await {
                Some(Inbound::Text(t)) => match reasm.push(t) {
                    Ok(Some(v)) => return Some(v),
                    Ok(None) => continue,
                    Err(_) => return None,
                },
                _ => return None,
            }
        }
    })
    .await
    .ok()
    .flatten();
    let Some(hello) = hello else { return };
    if hello.get("type").and_then(|t| t.as_str()) != Some("edge.hello") {
        // e.g. an app `hello`: this identity only ever talks to edges.
        let _ = outbound.send(reject("not_an_edge")).await;
        return;
    }
    let name = match hub.admit(&peer_key, hello.get("token").and_then(|t| t.as_str())) {
        Ok(name) => name,
        Err(reason) => {
            crate::warn!("edge refused: {reason}");
            let _ = outbound.send(reject(reason)).await;
            return;
        }
    };
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<String>();
    let conn = hub.attach(&name, out_tx.clone());
    hub.make_dirs(&name, hello.get("dirs"));
    let _ = out_tx.send(json!({ "type": "edge.welcome", "name": name, "modelProxy": hub.upstream.is_some() }).to_string());
    crate::log!("edge {name} connected");

    let mut http: Vec<JoinHandle<()>> = Vec::new();
    let mut frag_id = 0u64;
    // `paseo-lite edge rm` edits edges.json: notice it and cut the edge off.
    let mut recheck = tokio::time::interval(REVOKE_CHECK);
    recheck.tick().await;
    let why = loop {
        tokio::select! {
            _ = recheck.tick() => {
                if Registry::load(&hub.home).name_for_key(&peer_key).as_deref() != Some(name.as_str()) {
                    let _ = outbound.send(reject("not_registered")).await;
                    break "removed with paseo-lite edge rm".to_string();
                }
            }
            frame = inbound.recv() => {
                let text = match frame {
                    Some(Inbound::Text(t)) => t,
                    Some(Inbound::Binary(_)) => break "binary frame".to_string(),
                    None => break "disconnected".to_string(),
                };
                let msg = match reasm.push(text) {
                    Ok(Some(v)) => v,
                    Ok(None) => continue,
                    Err(e) => break e,
                };
                if let Err(e) = hub.handle(&name, conn, msg, &out_tx, &mut http) {
                    break e;
                }
            }
            Some(text) = out_rx.recv() => {
                let mut gone = false;
                for frame in wire::split(&text, &mut frag_id) {
                    if outbound.send(Arc::from(frame)).await.is_err() {
                        gone = true;
                        break;
                    }
                }
                if gone {
                    break "disconnected".to_string();
                }
            }
        }
    };
    for h in http {
        h.abort();
    }
    hub.detach(&name, conn);
    crate::log!("edge {name} left: {why}");
}

impl Hub {
    fn admit(&self, peer_key: &str, token: Option<&str>) -> Result<String, &'static str> {
        let _guard = self.registry_lock.lock().unwrap();
        let mut reg = Registry::load(&self.home);
        if let Some(name) = reg.name_for_key(peer_key) {
            return Ok(name);
        }
        let token = token.ok_or("not_registered")?;
        let name = reg.redeem(token, peer_key)?;
        reg.save(&self.home).map_err(|_| "cannot_save")?;
        crate::log!("edge {name} joined");
        Ok(name)
    }

    /// A new connection for this edge replaces the old one. The edge kills its
    /// processes when it loses main, so the old connection's processes are gone.
    fn attach(&self, name: &str, out: mpsc::UnboundedSender<String>) -> u64 {
        let mut st = self.state.lock().unwrap();
        st.next_conn += 1;
        let conn = st.next_conn;
        st.links.insert(name.to_string(), Link { conn, out });
        fail_procs(&mut *st, |p| p.edge == name && p.conn != conn, "edge machine reconnected");
        conn
    }

    fn detach(&self, name: &str, conn: u64) {
        let mut st = self.state.lock().unwrap();
        if st.links.get(name).map(|l| l.conn == conn).unwrap_or(false) {
            st.links.remove(name);
        }
        fail_procs(&mut *st, |p| p.edge == name && p.conn == conn, "edge machine disconnected");
    }

    fn send_to_proc(&self, pid: u64, msg: Value) {
        let st = self.state.lock().unwrap();
        let Some(p) = st.procs.get(&pid) else { return };
        if let Some(link) = st.links.get(&p.edge).filter(|l| l.conn == p.conn) {
            let _ = link.out.send(msg.to_string());
        }
    }

    /// Placeholder folders on main for the edge's workspace folders, so the
    /// app can add them as projects. The files themselves stay on the edge.
    fn make_dirs(&self, name: &str, dirs: Option<&Value>) {
        let root = self.edges_dir.join(name);
        if let Err(e) = std::fs::create_dir_all(&root) {
            crate::warn!("edge {name}: cannot create {}: {e}", root.display());
            return;
        }
        let names = dirs.and_then(|d| d.as_array()).map(|a| a.iter().filter_map(|v| v.as_str()).filter_map(wire::safe_name).take(MAX_DIRS).collect::<Vec<_>>()).unwrap_or_default();
        for d in names {
            let _ = std::fs::create_dir_all(root.join(d));
        }
    }

    /// The deny-by-default gate for everything an edge sends after joining.
    fn handle(self: &Arc<Self>, name: &str, conn: u64, msg: Value, out: &mpsc::UnboundedSender<String>, http: &mut Vec<JoinHandle<()>>) -> Result<(), String> {
        let kind = msg.get("type").and_then(|t| t.as_str()).unwrap_or("");
        match kind {
            "ping" => {
                let _ = out.send(r#"{"type":"pong"}"#.to_string());
            }
            "edge.status" => self.make_dirs(name, msg.get("dirs")),
            "proc.out" => {
                let pid = msg.get("pid").and_then(|p| p.as_u64()).ok_or("proc.out without pid")?;
                let line = msg.get("line").and_then(|l| l.as_str()).ok_or("proc.out without line")?;
                let st = self.state.lock().unwrap();
                let p = st.procs.get(&pid).filter(|p| p.edge == name && p.conn == conn).ok_or("proc.out for a process this edge does not run")?;
                let _ = p.stdout.send(line.to_string());
            }
            "proc.exit" => {
                let pid = msg.get("pid").and_then(|p| p.as_u64()).ok_or("proc.exit without pid")?;
                let mut st = self.state.lock().unwrap();
                if !st.procs.get(&pid).map(|p| p.edge == name && p.conn == conn).unwrap_or(false) {
                    return Err("proc.exit for a process this edge does not run".into());
                }
                if let Some(mut p) = st.procs.remove(&pid) {
                    let ok = msg.get("ok").and_then(|o| o.as_bool()).unwrap_or(false);
                    let detail = msg.get("detail").and_then(|d| d.as_str()).map(|d| crate::util::truncate(d, 2048)).unwrap_or_default();
                    if let Some(tx) = p.exit.take() {
                        let _ = tx.send(Exit { ok, detail });
                    }
                }
            }
            "http.request" => {
                let rid = msg.get("rid").and_then(|r| r.as_str()).filter(|r| !r.is_empty() && r.len() <= 64).ok_or("http.request without rid")?.to_string();
                http.retain(|h| !h.is_finished());
                let refuse = |error: &str| {
                    let _ = out.send(json!({ "type": "http.end", "rid": rid, "error": error }).to_string());
                };
                let Some(upstream) = self.upstream.clone() else {
                    refuse("main has no model credentials");
                    return Ok(());
                };
                if http.len() >= MAX_HTTP_PER_EDGE {
                    refuse("too many model requests at once");
                    return Ok(());
                }
                let method = msg.get("method").and_then(|m| m.as_str()).unwrap_or("").to_string();
                let path = msg.get("path").and_then(|p| p.as_str()).unwrap_or("").to_string();
                if let Err(e) = Upstream::check(&method, &path) {
                    crate::warn!("edge {name}: refused a model request ({e}): {method} {}", crate::util::truncate(&path, 120));
                    refuse(e);
                    return Ok(());
                }
                let headers: Vec<(String, String)> = msg
                    .get("headers")
                    .and_then(|h| h.as_array())
                    .map(|a| a.iter().filter_map(|p| Some((p.get(0)?.as_str()?.to_string(), p.get(1)?.as_str()?.to_string()))).collect())
                    .unwrap_or_default();
                use base64::Engine;
                let body = match msg.get("body").and_then(|b| b.as_str()).map(|b| base64::engine::general_purpose::STANDARD.decode(b)) {
                    None => Vec::new(),
                    Some(Ok(b)) => b,
                    Some(Err(_)) => return Err("http.request body is not base64".into()),
                };
                let out = out.clone();
                http.push(tokio::spawn(async move {
                    let data_out = out.clone();
                    let rid2 = rid.clone();
                    let result = upstream
                        .forward(&method, &path, &headers, &body, move |bytes| {
                            let data = base64::engine::general_purpose::STANDARD.encode(bytes);
                            data_out.send(json!({ "type": "http.data", "rid": rid2, "data": data }).to_string()).is_ok()
                        })
                        .await;
                    let end = match result {
                        Ok(()) => json!({ "type": "http.end", "rid": rid }),
                        Err(e) => json!({ "type": "http.end", "rid": rid, "error": e }),
                    };
                    let _ = out.send(end.to_string());
                }));
            }
            _ => return Err(format!("message type {:?} is not allowed from an edge", crate::util::truncate(kind, 40))),
        }
        Ok(())
    }
}

fn fail_procs(st: &mut State, pick: impl Fn(&ProcLink) -> bool, why: &str) {
    let gone: Vec<u64> = st.procs.iter().filter(|(_, p)| pick(p)).map(|(pid, _)| *pid).collect();
    for pid in gone {
        if let Some(mut p) = st.procs.remove(&pid) {
            if let Some(tx) = p.exit.take() {
                let _ = tx.send(Exit { ok: false, detail: why.to_string() });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes_only_folders_under_the_edges_dir() {
        let dir = Path::new("/workspace/edges");
        assert!(route(dir, "/workspace/project").is_none());
        let t = route(dir, "/workspace/edges/laptop/repo/sub").unwrap().unwrap();
        assert_eq!((t.edge.as_str(), t.rel.as_str()), ("laptop", "repo/sub"));
        let t = route(dir, "/workspace/edges/laptop").unwrap().unwrap();
        assert_eq!((t.edge.as_str(), t.rel.as_str()), ("laptop", ""));
        assert!(route(dir, "/workspace/edges/laptop/../../etc").unwrap().is_err());
        assert!(route(dir, "/workspace/edges").unwrap().is_err());
    }
}
