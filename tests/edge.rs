//! Edge machines, end to end: the real binary runs as main and as an edge,
//! both talking through a fake relay (the same v2 protocol as relay.paseo.sh),
//! with a fake model API behind main.
//!
//! Checks the promises: an agent on an edge folder runs on the edge; the model
//! API sees main's key while the edge only ever has a dummy one; the proxy
//! refuses anything but model endpoints; and a hostile edge gets nothing from
//! main (no app session, no other processes, no reuse of a used code).

use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use base64::Engine;
use crypto_box::aead::{Aead, AeadCore, OsRng};
use crypto_box::{PublicKey, SalsaBox, SecretKey};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

const REAL_KEY: &str = "REAL-KEY-123";
const EDGE_LOCAL_KEY: &str = "EDGE-LOCAL-SECRET";

struct Proc(Child);

impl Drop for Proc {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn unique_dir(tag: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("paseo-lite-edge-{tag}-{}-{nanos}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn executable(path: &Path, body: &str) {
    std::fs::write(path, body).unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

// ---------------------------------------------------------------- fake relay

type Tx = mpsc::UnboundedSender<Message>;

#[derive(Default)]
struct RelayState {
    controls: HashMap<String, Tx>,
    /// connectionId -> (serverId, client socket)
    clients: HashMap<String, (String, Tx)>,
    servers: HashMap<String, Tx>,
    pending: HashMap<String, Vec<Message>>,
    next: u64,
}

fn query(uri: &str) -> HashMap<String, String> {
    uri.split_once('?').map(|(_, q)| q.split('&').filter_map(|kv| kv.split_once('=')).map(|(k, v)| (k.to_string(), v.to_string())).collect()).unwrap_or_default()
}

/// The relay's v2 routing: a control socket per server, one client socket per
/// connection (the relay assigns its id), one server data socket per connection.
fn start_relay() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    listener.set_nonblocking(true).unwrap();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async move {
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();
            let state = Arc::new(Mutex::new(RelayState::default()));
            loop {
                let Ok((sock, _)) = listener.accept().await else { continue };
                tokio::spawn(relay_conn(sock, state.clone()));
            }
        });
    });
    port
}

async fn relay_conn(sock: tokio::net::TcpStream, state: Arc<Mutex<RelayState>>) {
    let uri = Arc::new(Mutex::new(String::new()));
    let uri_w = uri.clone();
    use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
    let cb = move |req: &Request, resp: Response| -> Result<Response, ErrorResponse> {
        *uri_w.lock().unwrap() = req.uri().to_string();
        Ok(resp)
    };
    let Ok(ws) = tokio_tungstenite::accept_hdr_async(sock, cb).await else { return };
    let q = query(&uri.lock().unwrap());
    let (mut sink, mut stream) = ws.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<Message>();
    tokio::spawn(async move {
        while let Some(m) = rx.recv().await {
            let close = matches!(m, Message::Close(_));
            if sink.send(m).await.is_err() || close {
                break;
            }
        }
    });
    let server_id = q.get("serverId").cloned().unwrap_or_default();
    match (q.get("role").map(String::as_str), q.get("connectionId").cloned()) {
        (Some("server"), None) => {
            {
                let mut st = state.lock().unwrap();
                let ids: Vec<String> = st.clients.iter().filter(|(_, (s, _))| *s == server_id).map(|(id, _)| id.clone()).collect();
                let _ = tx.send(Message::Text(json!({ "type": "sync", "connectionIds": ids }).to_string()));
                st.controls.insert(server_id.clone(), tx);
            }
            while let Some(Ok(_)) = stream.next().await {}
            state.lock().unwrap().controls.remove(&server_id);
        }
        (Some("client"), _) => {
            let id = {
                let mut st = state.lock().unwrap();
                st.next += 1;
                let id = format!("c{}", st.next);
                st.clients.insert(id.clone(), (server_id.clone(), tx));
                if let Some(control) = st.controls.get(&server_id) {
                    let _ = control.send(Message::Text(json!({ "type": "connected", "connectionId": id }).to_string()));
                }
                id
            };
            while let Some(Ok(m)) = stream.next().await {
                if !matches!(m, Message::Text(_) | Message::Binary(_)) {
                    continue;
                }
                let mut st = state.lock().unwrap();
                match st.servers.get(&id) {
                    Some(server) => {
                        let _ = server.send(m);
                    }
                    None => st.pending.entry(id.clone()).or_default().push(m),
                }
            }
            let mut st = state.lock().unwrap();
            st.clients.remove(&id);
            st.pending.remove(&id);
            if let Some(server) = st.servers.remove(&id) {
                let _ = server.send(Message::Close(None));
            }
            if let Some(control) = st.controls.get(&server_id) {
                let _ = control.send(Message::Text(json!({ "type": "disconnected", "connectionId": id }).to_string()));
            }
        }
        (Some("server"), Some(id)) => {
            {
                let mut st = state.lock().unwrap();
                for m in st.pending.remove(&id).unwrap_or_default() {
                    let _ = tx.send(m);
                }
                st.servers.insert(id.clone(), tx);
            }
            while let Some(Ok(m)) = stream.next().await {
                if !matches!(m, Message::Text(_) | Message::Binary(_)) {
                    continue;
                }
                if let Some((_, client)) = state.lock().unwrap().clients.get(&id) {
                    let _ = client.send(m);
                }
            }
            let mut st = state.lock().unwrap();
            st.servers.remove(&id);
            if let Some((_, client)) = st.clients.remove(&id) {
                let _ = client.send(Message::Close(None));
            }
        }
        _ => {}
    }
}

// ------------------------------------------------------------ fake model API

/// Answers every request with the API key it was given and the path, as plain text.
fn start_model_api(seen: Arc<Mutex<Vec<String>>>) -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        for sock in listener.incoming() {
            let Ok(mut sock) = sock else { continue };
            let seen = seen.clone();
            std::thread::spawn(move || {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                let head_end = loop {
                    if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        break i + 4;
                    }
                    match sock.read(&mut chunk) {
                        Ok(n) if n > 0 => buf.extend_from_slice(&chunk[..n]),
                        _ => return,
                    }
                };
                let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
                let path = head.split(' ').nth(1).unwrap_or("").to_string();
                let keys: Vec<String> = head.lines().filter_map(|l| l.split_once(':')).filter(|(k, _)| k.eq_ignore_ascii_case("x-api-key")).map(|(_, v)| v.trim().to_string()).collect();
                let length: usize = head.lines().filter_map(|l| l.split_once(':')).find(|(k, _)| k.eq_ignore_ascii_case("content-length")).and_then(|(_, v)| v.trim().parse().ok()).unwrap_or(0);
                while buf.len() < head_end + length {
                    match sock.read(&mut chunk) {
                        Ok(n) if n > 0 => buf.extend_from_slice(&chunk[..n]),
                        _ => break,
                    }
                }
                seen.lock().unwrap().push(head.clone());
                let body = format!("key={} path={path}", keys.join("+"));
                let resp = format!("HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n{body}", body.len());
                let _ = sock.write_all(resp.as_bytes());
            });
        }
    });
    port
}

// ------------------------------------------------------------------ helpers

fn bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_paseo-lite"))
}

/// What the edge runs instead of Claude Code: calls the model through its
/// proxy (and one path the proxy must refuse), then reports what it saw.
fn edge_claude(dir: &Path) -> PathBuf {
    let path = dir.join("edge-claude.sh");
    let script = r#"#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *'"type":"user"'*)
      up=$(curl -s -X POST "$ANTHROPIC_BASE_URL/v1/messages?beta=true" -H "x-api-key: $ANTHROPIC_API_KEY" -H 'content-type: application/json' -d '{}')
      denied=$(curl -s -o /dev/null -w '%{http_code}' "$ANTHROPIC_BASE_URL/v1/organizations")
      printf '{"type":"assistant","message":{"id":"m1","role":"assistant","content":[{"type":"text","text":"upstream=[%s] denied=%s local=%s pwd=%s"}]}}\n' "$up" "$denied" "$ANTHROPIC_API_KEY" "$(basename "$PWD")"
      printf '%s\n' '{"type":"result","subtype":"success","is_error":false}'
      ;;
  esac
done
"#;
    executable(&path, script);
    path
}

fn wait_for(what: &str, timeout: Duration, mut ok: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    panic!("timed out waiting for {what}");
}

type Ws = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn recv_json(ws: &mut Ws) -> Value {
    loop {
        match tokio::time::timeout(Duration::from_secs(30), ws.next()).await.expect("timed out waiting for a frame") {
            Some(Ok(Message::Text(t))) => return serde_json::from_str(&t).unwrap(),
            Some(Ok(_)) => continue,
            other => panic!("socket closed: {other:?}"),
        }
    }
}

async fn recv_until<T>(ws: &mut Ws, mut pick: impl FnMut(&Value) -> Option<T>) -> T {
    loop {
        let v = recv_json(ws).await;
        if let Some(found) = pick(&v) {
            return found;
        }
    }
}

fn session(msg: Value) -> Value {
    json!({ "type": "session", "message": msg })
}

/// A hand-made edge connection, to play a hostile edge.
struct RawEdge {
    ws: Ws,
    cipher: SalsaBox,
}

impl RawEdge {
    async fn connect(relay: u16, server_id: &str, main_key: &str, secret: &SecretKey) -> RawEdge {
        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{relay}/ws?serverId={server_id}&role=client&v=2")).await.unwrap();
        let mine = STANDARD.encode(secret.public_key().as_bytes());
        ws.send(Message::Text(json!({ "type": "e2ee_hello", "key": mine }).to_string())).await.unwrap();
        loop {
            match tokio::time::timeout(Duration::from_secs(30), ws.next()).await.expect("no e2ee_ready") {
                Some(Ok(Message::Text(t))) if t.contains("e2ee_ready") => break,
                Some(Ok(_)) => continue,
                other => panic!("closed before e2ee_ready: {other:?}"),
            }
        }
        let main: [u8; 32] = STANDARD.decode(main_key).unwrap().try_into().unwrap();
        RawEdge { ws, cipher: SalsaBox::new(&PublicKey::from(main), secret) }
    }

    async fn send(&mut self, v: Value) {
        let nonce = SalsaBox::generate_nonce(&mut OsRng);
        let mut bundle = nonce.to_vec();
        bundle.extend(self.cipher.encrypt(&nonce, v.to_string().as_bytes()).unwrap());
        self.ws.send(Message::Text(STANDARD.encode(bundle))).await.unwrap();
    }

    /// The next message from main, or None once main closed the connection.
    async fn recv(&mut self) -> Option<Value> {
        loop {
            match tokio::time::timeout(Duration::from_secs(30), self.ws.next()).await {
                Ok(Some(Ok(Message::Text(t)))) => {
                    let raw = STANDARD.decode(t.trim()).ok()?;
                    let (nonce, sealed) = raw.split_at(24);
                    let plain = self.cipher.decrypt(crypto_box::aead::generic_array::GenericArray::from_slice(nonce), sealed).ok()?;
                    return Some(serde_json::from_slice(&plain).unwrap());
                }
                Ok(Some(Ok(Message::Close(_)))) | Ok(None) | Ok(Some(Err(_))) | Err(_) => return None,
                Ok(Some(Ok(_))) => continue,
            }
        }
    }

    /// Everything main still sends before closing; panics if it never closes.
    async fn drain_until_closed(&mut self) -> Vec<Value> {
        let mut got = Vec::new();
        while let Some(v) = self.recv().await {
            got.push(v);
            assert!(got.len() < 50, "main kept the connection open: {got:?}");
        }
        got
    }
}

fn secret_from(path: &Path) -> SecretKey {
    let stored: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    let bytes: [u8; 32] = STANDARD.decode(stored["secretKeyB64"].as_str().unwrap()).unwrap().try_into().unwrap();
    SecretKey::from(bytes)
}

// -------------------------------------------------------------------- test

#[tokio::test]
async fn edge_runs_agents_without_main_secrets_and_a_hostile_edge_gets_nothing() {
    let dir = unique_dir("main");
    let relay = start_relay();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let model_api = start_model_api(seen.clone());
    let main_home = dir.join("main-home");
    let edges_dir = dir.join("edges");
    let main_claude = dir.join("main-claude.sh");
    executable(&main_claude, "#!/bin/sh\necho 'main must not run edge agents' >&2\nexit 1\n");

    // Main: relay on, real-looking credentials, nothing secret in the edge's reach.
    let port = free_port();
    let main = Proc(
        bin()
            .args(["--listen", &format!("127.0.0.1:{port}")])
            .env("PASEO_LITE_HOME", &main_home)
            .env("PASEO_RELAY_ENDPOINT", format!("127.0.0.1:{relay}"))
            .env("PASEO_EDGES_DIR", &edges_dir)
            .env("PASEO_CLAUDE_BIN", &main_claude)
            .env("PASEO_CODEX_BIN", "/nonexistent/codex")
            .env("PASEO_DEFAULT_SYSTEM_PROMPT", "")
            .env("PASEO_RETRY_MAX", "0")
            .env("ANTHROPIC_API_KEY", REAL_KEY)
            .env("ANTHROPIC_BASE_URL", format!("http://127.0.0.1:{model_api}"))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    wait_for("main to listen", Duration::from_secs(20), || std::net::TcpStream::connect(("127.0.0.1", port)).is_ok());

    // A one-time code, made on main.
    let out = bin().args(["edge", "invite", "laptop"]).env("PASEO_LITE_HOME", &main_home).env("PASEO_RELAY_ENDPOINT", format!("127.0.0.1:{relay}")).output().unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    let code_text = String::from_utf8(out.stdout).unwrap().trim().to_string();
    let code: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(code_text.strip_prefix("pse1_").unwrap()).unwrap()).unwrap();
    let (edge_server, main_edge_key, token) = (code["server_id"].as_str().unwrap().to_string(), code["key"].as_str().unwrap().to_string(), code["token"].as_str().unwrap().to_string());
    // The code is not the app's pairing identity.
    let app_server_id = std::fs::read_to_string(main_home.join("server-id")).unwrap();
    assert_ne!(edge_server, app_server_id.trim());

    // The edge: its own home and workspace, and a local API key it must not pass on.
    let edge_home = dir.join("edge-home");
    let workspace = dir.join("edge-workspace");
    std::fs::create_dir_all(workspace.join("repo")).unwrap();
    let mut edge = Proc(
        bin()
            .args(["edge", "run", &code_text])
            .env("PASEO_LITE_HOME", &edge_home)
            .env("PASEO_EDGE_WORKSPACE", &workspace)
            .env("PASEO_CLAUDE_BIN", edge_claude(&dir))
            .env("ANTHROPIC_API_KEY", EDGE_LOCAL_KEY)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let folder = edges_dir.join("laptop").join("repo");
    wait_for("the edge's folder on main", Duration::from_secs(40), || folder.is_dir());

    // An agent on that folder runs on the edge, and its model call carries main's key.
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/ws")).await.unwrap();
    ws.send(Message::Text(json!({ "type": "hello", "protocolVersion": 1, "clientId": "test", "clientType": "cli" }).to_string())).await.unwrap();
    recv_json(&mut ws).await;
    let create = session(json!({
        "type": "create_agent_request",
        "requestId": "a1",
        "config": { "provider": "claude", "cwd": folder.to_string_lossy(), "modeId": "bypassPermissions" },
        "initialPrompt": "say hi",
    }));
    ws.send(Message::Text(create.to_string())).await.unwrap();
    let agent_id = recv_until(&mut ws, |v| {
        let p = &v["message"]["payload"];
        (v["message"]["type"] == "status" && p["status"] == "agent_created").then(|| p["agentId"].as_str().unwrap().to_string())
    })
    .await;
    recv_until(&mut ws, |v| {
        let ev = &v["message"]["payload"]["event"];
        assert_ne!(ev["type"], "turn_failed", "{v}");
        (v["message"]["type"] == "agent_stream" && ev["type"] == "turn_completed").then_some(())
    })
    .await;
    ws.send(Message::Text(session(json!({ "type": "fetch_agent_timeline_request", "requestId": "t1", "agentId": agent_id, "direction": "tail", "limit": 50 })).to_string())).await.unwrap();
    let tl = recv_until(&mut ws, |v| (v["message"]["type"] == "fetch_agent_timeline_response").then(|| v["message"]["payload"].clone())).await;
    let text = tl["entries"].as_array().unwrap().iter().find(|e| e["item"]["type"] == "assistant_message").expect("assistant text")["item"]["text"].as_str().unwrap().to_string();
    assert!(text.contains(&format!("upstream=[key={REAL_KEY} path=/v1/messages?beta=true]")), "{text}");
    assert!(text.contains("denied=502"), "{text}");
    assert!(text.contains("local=paseo-edge-no-key-here"), "{text}");
    assert!(text.contains("pwd=repo"), "{text}");
    let seen_heads = seen.lock().unwrap().join("\n---\n");
    assert!(!seen_heads.contains(EDGE_LOCAL_KEY) && !seen_heads.contains("paseo-edge-no-key-here"), "{seen_heads}");
    assert!(!seen_heads.contains("/v1/organizations"), "a refused path reached the model API: {seen_heads}");

    // Hostile edges. A stranger without a code is refused.
    let stranger = SecretKey::generate(&mut OsRng);
    let mut e = RawEdge::connect(relay, &edge_server, &main_edge_key, &stranger).await;
    e.send(json!({ "type": "edge.hello", "v": 1 })).await;
    assert_eq!(e.recv().await.unwrap()["reason"], "not_registered");
    // The code was used up by the real edge.
    let mut e = RawEdge::connect(relay, &edge_server, &main_edge_key, &stranger).await;
    e.send(json!({ "type": "edge.hello", "v": 1, "token": token })).await;
    assert_eq!(e.recv().await.unwrap()["reason"], "invalid_or_expired_code");
    // The edge identity never serves the app protocol, even to a known key.
    let edge_key = secret_from(&edge_home.join("edge-client").join("daemon-keypair.json"));
    let mut e = RawEdge::connect(relay, &edge_server, &main_edge_key, &edge_key).await;
    e.send(json!({ "type": "hello", "protocolVersion": 1, "clientId": "x", "clientType": "cli" })).await;
    let got = e.drain_until_closed().await;
    assert_eq!(got.len(), 1, "{got:?}");
    assert_eq!(got[0]["reason"], "not_an_edge");

    // A joined edge (its real key) still cannot use the proxy for other paths, ask
    // for app data, or touch processes it does not run: each ends the connection.
    let mut e = RawEdge::connect(relay, &edge_server, &main_edge_key, &edge_key).await;
    e.send(json!({ "type": "edge.hello", "v": 1 })).await;
    assert_eq!(e.recv().await.unwrap()["type"], "edge.welcome");
    let headers = json!([["Host", "127.0.0.1:9222"]]);
    e.send(json!({ "type": "http.request", "rid": "r1", "method": "GET", "path": "/json/version", "headers": headers, "body": "" })).await;
    let end = e.recv().await.unwrap();
    assert_eq!((end["type"].as_str(), end["rid"].as_str()), (Some("http.end"), Some("r1")), "{end}");
    assert!(end["error"].as_str().unwrap_or("").contains("not allowed"), "{end}");
    e.send(session(json!({ "type": "fetch_agents_request", "requestId": "x" }))).await;
    let got = e.drain_until_closed().await;
    assert!(got.iter().all(|v| v["type"] != "session"), "main answered an app request on the edge link: {got:?}");

    let mut e = RawEdge::connect(relay, &edge_server, &main_edge_key, &edge_key).await;
    e.send(json!({ "type": "edge.hello", "v": 1 })).await;
    assert_eq!(e.recv().await.unwrap()["type"], "edge.welcome");
    e.send(json!({ "type": "proc.out", "pid": 1, "line": "{\"type\":\"result\"}" })).await;
    let got = e.drain_until_closed().await;
    assert!(got.iter().all(|v| v["type"] != "session"), "{got:?}");

    // Removing the edge on main cuts it off; the real edge gives up.
    let out = bin().args(["edge", "rm", "laptop"]).env("PASEO_LITE_HOME", &main_home).output().unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    wait_for("the removed edge to stop", Duration::from_secs(60), || edge.0.try_wait().unwrap().is_some());
    assert!(!edge.0.try_wait().unwrap().unwrap().success());

    drop(main);
    let _ = std::fs::remove_dir_all(&dir);
}
