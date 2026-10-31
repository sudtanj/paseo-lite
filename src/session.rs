//! One client connection, independent of transport (direct WS or E2EE relay).
//! The transport adapter feeds plaintext frames in and drains frames out.

use crate::manager::Cmd;
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};

pub const WS_PROTOCOL_VERSION: i64 = 1;

/// A frame from the client: JSON text, or binary (file-transfer frames).
pub enum Inbound {
    Text(String),
    Binary(Vec<u8>),
}

pub struct Ctx {
    pub cmds: mpsc::UnboundedSender<Cmd>,
    pub bcast: broadcast::Sender<Arc<str>>,
    pub server_id: String,
    pub hostname: String,
    pub password: Option<String>,
    /// paseo-lite home: uploads are stored under `uploads/`.
    pub home: std::path::PathBuf,
}

pub(crate) fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn server_info(ctx: &Ctx) -> Arc<str> {
    Arc::from(
        json!({ "type": "session", "message": { "type": "status", "payload": {
            "status": "server_info",
            "protocolVersion": WS_PROTOCOL_VERSION,
            "serverId": ctx.server_id,
            "hostname": ctx.hostname,
            "version": concat!("paseo-lite-", env!("CARGO_PKG_VERSION")),
            // The app gates screens on these flags; only list what rpc.rs implements.
            // Add Project needs both projectAdd and stableProjectIdentity.
            "features": {
                "providersSnapshot": true,
                "workspaceMultiplicity": true,
                "workspacePinning": true,
                "projectAdd": true,
                "projectList": true,
                "stableProjectIdentity": true,
                "projectCreateDirectory": true,
                "projectGithubClone": true,
                "workspaceGithubRepositorySearch": true,
                "projectRemove": true,
                "agentThinkingUpdate": true,
                "agentTimelinePromptIndex": true,
                "daemonStatusRpc": true,
                "daemonDiagnostics": true,
                "pushTokenRevocation": true,
                "providerRemoval": true,
                "checkoutRefresh": true,
                "checkoutDiscardChanges": true,
                "commitsList": true,
                "commitBaseClassification": true,
                "usageSources": true,
                "providerUsageList": true,
            },
        } } })
        .to_string(),
    )
}

/// Returns when the client goes away or is rejected.
pub async fn run(ctx: Arc<Ctx>, mut inbound: mpsc::Receiver<Inbound>, outbound: mpsc::Sender<Arc<str>>, via_relay: bool) {
    // Phase 1: wait for hello. Relay sockets are already authenticated by the
    // E2EE key exchange (same policy as the Node daemon).
    loop {
        let Some(frame) = inbound.recv().await else { return };
        let Inbound::Text(text) = frame else { continue };
        let Ok(msg) = serde_json::from_str::<Value>(&text) else { continue };
        match msg.get("type").and_then(|t| t.as_str()) {
            Some("ping") => {
                let _ = outbound.send(Arc::from(r#"{"type":"pong"}"#)).await;
            }
            Some("hello") => {
                let version = msg.get("protocolVersion").and_then(|v| v.as_i64()).unwrap_or(0);
                let reject = |reason: &str| -> Arc<str> { Arc::from(json!({ "type": "hello.rejected", "reason": reason, "accepts": ["password"] }).to_string()) };
                if version != WS_PROTOCOL_VERSION {
                    let _ = outbound.send(reject("incompatible_protocol")).await;
                    return;
                }
                if let (Some(expected), false) = (&ctx.password, via_relay) {
                    match msg.pointer("/auth/password").and_then(|p| p.as_str()) {
                        None => {
                            let _ = outbound.send(reject("password_required")).await;
                            return;
                        }
                        Some(given) if !constant_time_eq(given.as_bytes(), expected.as_bytes()) => {
                            let _ = outbound.send(reject("incorrect_password")).await;
                            return;
                        }
                        _ => {}
                    }
                }
                if outbound.send(server_info(&ctx)).await.is_err() {
                    return;
                }
                break;
            }
            _ => {}
        }
    }

    // Phase 2: live session.
    let mut events = ctx.bcast.subscribe();
    let (reply_tx, mut reply_rx) = mpsc::unbounded_channel::<Arc<str>>();
    let mut uploads = crate::uploads::Uploads::default();
    loop {
        tokio::select! {
            frame = inbound.recv() => {
                let text = match frame {
                    None => return,
                    Some(Inbound::Text(text)) => text,
                    // File-transfer frames of an upload this session started.
                    Some(Inbound::Binary(bytes)) => {
                        if let Some(resp) = uploads.frame(&bytes).await {
                            if outbound.send(crate::manager::wrap(resp)).await.is_err() { return; }
                        }
                        continue;
                    }
                };
                let Ok(mut msg) = serde_json::from_str::<Value>(&text) else { continue };
                match msg.get("type").and_then(|t| t.as_str()) {
                    Some("ping") => {
                        if outbound.send(Arc::from(r#"{"type":"pong"}"#)).await.is_err() { return; }
                    }
                    Some("hello") => {
                        if outbound.send(server_info(&ctx)).await.is_err() { return; }
                    }
                    Some("session") => {
                        let inner = msg.get_mut("message").map(Value::take).unwrap_or(Value::Null);
                        // Uploads are per connection (the bytes follow on this socket).
                        if inner.get("type").and_then(|t| t.as_str()) == Some("file.upload.request") {
                            if let Some(resp) = uploads.begin(&ctx.home, &inner) {
                                if outbound.send(crate::manager::wrap(resp)).await.is_err() { return; }
                            }
                            continue;
                        }
                        if ctx.cmds.send(Cmd::Rpc { msg: inner, out: reply_tx.clone() }).is_err() { return; }
                    }
                    _ => {}
                }
            }
            Some(out) = reply_rx.recv() => {
                crate::egress::count(&out);
                if outbound.send(out).await.is_err() { return; }
            }
            ev = events.recv() => match ev {
                Ok(out) => {
                    crate::egress::count(&out);
                    if outbound.send(out).await.is_err() { return; }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    // A slow or just-resumed client missed events it cannot know about. End the
                    // session: the app reconnects and re-syncs everything, instead of showing a
                    // stale chat until it is killed.
                    crate::warn!("session lagged, dropped {n} events; closing so the client resyncs");
                    return;
                }
                Err(broadcast::error::RecvError::Closed) => return,
            },
        }
    }
}
