//! Outbound relay transport, protocol v2 (same as `relay-transport.ts`):
//! - one control socket: `/ws?serverId=..&role=server&v=2`, carrying
//!   `sync` / `connected` / `disconnected` / `ping` JSON messages;
//! - one data socket per client connection (`&connectionId=..`), on which the
//!   client performs the `e2ee_hello` / `e2ee_ready` handshake and then sends
//!   NaCl-box encrypted frames. The relay only ever sees ciphertext.

use crate::config::Config;
use crate::e2ee::{Channel, DaemonIdentity};
use crate::session::{self, Ctx, Inbound};
use std::future::Future;
use std::pin::Pin;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::Message;

/// Short ping/stale windows so a dead control socket (NAT timeout, network
/// switch) is noticed and replaced within seconds instead of half a minute.
const PING_INTERVAL: Duration = Duration::from_secs(5);
const STALE_AFTER: Duration = Duration::from_secs(15);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(8);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(15);
/// Reconnect backoff: immediate first retry, then doubling up to the cap.
const BACKOFF_START: Duration = Duration::from_millis(250);
const BACKOFF_MAX: Duration = Duration::from_secs(10);
/// Max queued outbound frames written per flush.
const WRITE_BATCH: usize = 64;

/// One TLS config for every relay socket: the root store is built once and
/// rustls' in-memory session cache lets reconnects and per-client data
/// sockets resume TLS sessions (one round trip less, much less CPU).
/// Also used for outbound HTTPS (push notifications).
pub fn tls_config() -> Arc<rustls::ClientConfig> {
    static CONFIG: std::sync::OnceLock<Arc<rustls::ClientConfig>> = std::sync::OnceLock::new();
    CONFIG
        .get_or_init(|| {
            let mut roots = rustls::RootCertStore::empty();
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            Arc::new(rustls::ClientConfig::builder().with_root_certificates(roots).with_no_client_auth())
        })
        .clone()
}

fn tls_connector() -> tokio_tungstenite::Connector {
    tokio_tungstenite::Connector::Rustls(tls_config())
}

pub(crate) type Ws = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// Open a relay socket with Nagle disabled (small JSON frames go out
/// immediately instead of waiting up to 40ms for an ACK) and the shared TLS config.
pub(crate) async fn connect(target: &str, timeout: Duration) -> Result<Ws, String> {
    let fut = tokio_tungstenite::connect_async_tls_with_config(target, None, true, Some(tls_connector()));
    let (ws, _) = tokio::time::timeout(timeout, fut).await.map_err(|_| "connect timeout".to_string())?.map_err(|e| e.to_string())?;
    Ok(ws)
}

/// Add up to ±20% jitter so many daemons don't reconnect in lockstep after a relay restart.
fn jitter(d: Duration) -> Duration {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|t| t.subsec_nanos()).unwrap_or(0);
    let pct = 80 + (nanos % 41) as u64;
    d * pct as u32 / 100
}

fn url(cfg: &Config, server_id: &str, connection_id: Option<&str>) -> String {
    let scheme = if cfg.relay_use_tls { "wss" } else { "ws" };
    let enc = |s: &str| s.chars().filter(|c| c.is_ascii_alphanumeric() || "-_.~".contains(*c)).collect::<String>();
    let mut u = format!("{scheme}://{}/ws?serverId={}&role=server&v=2", cfg.relay_endpoint, enc(server_id));
    if let Some(c) = connection_id {
        u.push_str("&connectionId=");
        u.push_str(&enc(c));
    }
    u
}

type DataSockets = Arc<Mutex<HashMap<String, JoinHandle<()>>>>;

/// What runs one decrypted client connection: plaintext frames in, frames
/// out, and the client's public key (already proven by the E2EE handshake).
pub type Handler = Arc<dyn Fn(tokio::sync::mpsc::Receiver<Inbound>, tokio::sync::mpsc::Sender<Arc<str>>, String) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// The app's relay identity: every connection is an app session.
pub async fn run(ctx: Arc<Ctx>, cfg: Config, identity: Arc<DaemonIdentity>) {
    let handler: Handler = Arc::new(
        move |inbound: tokio::sync::mpsc::Receiver<Inbound>, outbound: tokio::sync::mpsc::Sender<Arc<str>>, _peer_key: String| -> Pin<Box<dyn Future<Output = ()> + Send>> {
            Box::pin(session::run(ctx.clone(), inbound, outbound, true))
        },
    );
    run_with(handler, cfg, identity).await
}

/// Serve one relay identity, handing each client connection to `handler`.
pub async fn run_with(handler: Handler, cfg: Config, identity: Arc<DaemonIdentity>) {
    let data: DataSockets = Arc::new(Mutex::new(HashMap::new()));
    let mut backoff = Duration::ZERO;
    loop {
        let mut connected = false;
        let started = Instant::now();
        let err = control_session(&handler, &cfg, &identity, &data, &mut connected).await.err().unwrap_or_else(|| "closed".into());
        // A session that stayed up was healthy, so the next attempt starts
        // fresh: a drop after hours of uptime reconnects at once instead of
        // inheriting an old backoff. A relay that accepts and instantly
        // closes still backs off.
        if connected && started.elapsed() >= Duration::from_secs(5) {
            backoff = Duration::ZERO;
        }
        crate::warn!("relay control disconnected: {err}; reconnecting in {}ms", backoff.as_millis());
        if !backoff.is_zero() {
            tokio::time::sleep(jitter(backoff)).await;
        }
        backoff = if backoff.is_zero() { BACKOFF_START } else { (backoff * 2).min(BACKOFF_MAX) };
    }
}

async fn control_session(handler: &Handler, cfg: &Config, identity: &Arc<DaemonIdentity>, data: &DataSockets, connected: &mut bool) -> Result<(), String> {
    let target = url(cfg, &identity.server_id, None);
    let ws = connect(&target, CONNECT_TIMEOUT).await?;
    *connected = true;
    crate::log!("relay connected ({})", cfg.relay_endpoint);
    let (mut sink, mut stream) = ws.split();
    let mut last_seen = Instant::now();
    let mut ping = tokio::time::interval(PING_INTERVAL);
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = ping.tick() => {
                if last_seen.elapsed() > STALE_AFTER {
                    return Err("control socket stale".into());
                }
                sink.send(Message::Ping(Vec::new())).await.map_err(|e| e.to_string())?;
            }
            frame = stream.next() => {
                let frame = match frame {
                    Some(Ok(f)) => f,
                    Some(Err(e)) => return Err(e.to_string()),
                    None => return Err("closed".into()),
                };
                last_seen = Instant::now();
                let text = match frame {
                    Message::Text(t) => t,
                    Message::Binary(b) => String::from_utf8_lossy(&b).into_owned(),
                    Message::Close(_) => return Err("closed by relay".into()),
                    _ => continue,
                };
                let Ok(msg) = serde_json::from_str::<Value>(&text) else { continue };
                match msg.get("type").and_then(|t| t.as_str()) {
                    Some("ping") => {
                        let pong = json!({ "type": "pong", "ts": std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0) });
                        sink.send(Message::Text(pong.to_string())).await.map_err(|e| e.to_string())?;
                    }
                    Some("sync") => {
                        let ids: Vec<String> = msg.get("connectionIds").and_then(|v| v.as_array()).map(|a| a.iter().filter_map(|v| v.as_str()).map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect()).unwrap_or_default();
                        for id in ids {
                            ensure_data_socket(handler, cfg, identity, data, id);
                        }
                    }
                    Some("connected") => {
                        if let Some(id) = msg.get("connectionId").and_then(|v| v.as_str()).map(|s| s.trim().to_string()).filter(|s| !s.is_empty()) {
                            // A fresh client connection: any socket we still hold for this id is a
                            // zombie (e.g. a backgrounded phone that never closed it), so replace it.
                            if let Some(old) = data.lock().unwrap().remove(&id) {
                                old.abort();
                            }
                            ensure_data_socket(handler, cfg, identity, data, id);
                        }
                    }
                    Some("disconnected") => {
                        if let Some(id) = msg.get("connectionId").and_then(|v| v.as_str()) {
                            if let Some(handle) = data.lock().unwrap().remove(id.trim()) {
                                handle.abort();
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}

fn ensure_data_socket(handler: &Handler, cfg: &Config, identity: &Arc<DaemonIdentity>, data: &DataSockets, connection_id: String) {
    let mut map = data.lock().unwrap();
    map.retain(|_, h| !h.is_finished());
    if map.contains_key(&connection_id) {
        return;
    }
    let (handler, cfg, identity, data2, id) = (handler.clone(), cfg.clone(), identity.clone(), data.clone(), connection_id.clone());
    let handle = tokio::spawn(async move {
        if let Err(e) = data_socket(handler, &cfg, identity, &id).await {
            crate::warn!("relay data socket {id} ended: {e}");
        }
        data2.lock().unwrap().remove(&id);
    });
    map.insert(connection_id, handle);
}

fn parse_handshake(text: &str) -> Option<(String, Option<String>)> {
    if !text.trim_start().starts_with('{') {
        return None;
    }
    let v: Value = serde_json::from_str(text).ok()?;
    let kind = v.get("type")?.as_str()?.to_string();
    let key = v.get("key").and_then(|k| k.as_str()).map(|k| k.to_string());
    (kind == "e2ee_hello" || kind == "e2ee_ready").then_some((kind, key))
}

fn ready_frame(binary_ciphertext: bool) -> Message {
    let ready = if binary_ciphertext {
        json!({ "type": "e2ee_ready", "capabilities": { "binaryCiphertext": true } })
    } else {
        json!({ "type": "e2ee_ready" })
    };
    Message::Text(ready.to_string())
}

async fn data_socket(handler: Handler, cfg: &Config, identity: Arc<DaemonIdentity>, connection_id: &str) -> Result<(), String> {
    let target = url(cfg, &identity.server_id, Some(connection_id));
    let ws = connect(&target, HANDSHAKE_TIMEOUT).await?;
    let (mut sink, mut stream) = ws.split();

    // Handshake: first text frame must be the client's e2ee_hello.
    let (channel, peer_key, binary_ciphertext) = tokio::time::timeout(HANDSHAKE_TIMEOUT, async {
        while let Some(frame) = stream.next().await {
            let Message::Text(text) = frame.map_err(|e| e.to_string())? else { continue };
            let v: Value = serde_json::from_str(&text).map_err(|_| "invalid hello".to_string())?;
            if v.get("type").and_then(|t| t.as_str()) != Some("e2ee_hello") {
                return Err("invalid hello".to_string());
            }
            let key = v.get("key").and_then(|k| k.as_str()).ok_or("hello without key")?.trim().to_string();
            let binary = v.pointer("/capabilities/binaryCiphertext").and_then(|b| b.as_bool()).unwrap_or(false);
            let channel = Channel::new(&identity.secret, &key)?;
            return Ok((channel, key, binary));
        }
        Err("closed during handshake".to_string())
    })
    .await
    .map_err(|_| "handshake timeout".to_string())??;
    sink.send(ready_frame(binary_ciphertext)).await.map_err(|e| e.to_string())?;

    let (in_tx, in_rx) = tokio::sync::mpsc::channel::<Inbound>(64);
    let (out_tx, mut out_rx) = tokio::sync::mpsc::channel::<Arc<str>>(256);
    let session = tokio::spawn(handler(in_rx, out_tx, peer_key.clone()));
    let mut last_seen = Instant::now();
    let mut keepalive = tokio::time::interval(PING_INTERVAL);
    keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let result = loop {
        tokio::select! {
            out = out_rx.recv() => {
                let Some(out) = out else { break Ok(()) };
                // JSON is text plaintext => base64 ciphertext in a text frame.
                // Queue whatever else is already waiting and flush once, so a
                // burst of stream updates costs one write instead of one each.
                let mut res = sink.feed(Message::Text(channel.encrypt_text(&out))).await;
                let mut n = 1;
                while res.is_ok() && n < WRITE_BATCH {
                    let Ok(more) = out_rx.try_recv() else { break };
                    res = sink.feed(Message::Text(channel.encrypt_text(&more))).await;
                    n += 1;
                }
                // A dead TCP connection never errors on write: bound the flush so it is noticed.
                let flushed = match tokio::time::timeout(Duration::from_secs(20), sink.flush()).await {
                    Ok(r) => r,
                    Err(_) => break Err("relay write stalled".into()),
                };
                if let Err(e) = res.and(flushed) {
                    break Err(e.to_string());
                }
            }
            _ = keepalive.tick() => {
                if last_seen.elapsed() > STALE_AFTER * 6 {
                    break Err("relay data socket stale".into());
                }
                if tokio::time::timeout(Duration::from_secs(10), sink.send(Message::Ping(Vec::new()))).await.is_err() {
                    break Err("relay ping stalled".into());
                }
            }
            frame = stream.next() => {
                last_seen = Instant::now();
                let frame = match frame {
                    Some(Ok(f)) => f,
                    Some(Err(e)) => break Err(e.to_string()),
                    None => break Ok(()),
                };
                let plaintext = match frame {
                    Message::Text(text) => {
                        if let Some((kind, key)) = parse_handshake(&text) {
                            if kind == "e2ee_hello" {
                                if key.as_deref().map(str::trim) == Some(peer_key.as_str()) {
                                    let _ = sink.send(ready_frame(binary_ciphertext)).await;
                                    continue;
                                }
                                let _ = sink.send(Message::Close(Some(CloseFrame { code: CloseCode::Policy, reason: "E2EE re-handshake key mismatch".into() }))).await;
                                break Err("re-handshake key mismatch".into());
                            }
                            continue;
                        }
                        if text.trim_start().starts_with('{') {
                            break Err("plaintext frame on encrypted channel".into());
                        }
                        channel.decrypt_text_bytes(&text).and_then(inbound_frame)
                    }
                    Message::Binary(bytes) => channel.decrypt(&bytes).and_then(inbound_frame),
                    Message::Close(_) => break Ok(()),
                    _ => continue,
                };
                match plaintext {
                    Ok(frame) => {
                        if in_tx.send(frame).await.is_err() {
                            break Ok(());
                        }
                    }
                    Err(e) => {
                        let _ = sink.send(Message::Close(Some(CloseFrame { code: CloseCode::Error, reason: e.clone().into() }))).await;
                        break Err(e);
                    }
                }
            }
        }
    };
    session.abort();
    result
}

/// Decrypted plaintext -> session frame: file-transfer frames stay binary,
/// everything else must be UTF-8 JSON.
fn inbound_frame(plain: Vec<u8>) -> Result<Inbound, String> {
    if crate::uploads::is_file_frame(&plain) {
        Ok(Inbound::Binary(plain))
    } else {
        String::from_utf8(plain).map(Inbound::Text).map_err(|e| e.to_string())
    }
}
