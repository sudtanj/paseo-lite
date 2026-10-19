//! What travels between main and an edge, shared by both sides.
//!
//! Every message is one JSON object. The hosted relay runs on Cloudflare,
//! which drops WebSocket messages over 1 MiB, so a message larger than
//! `MAX_FRAME` goes out as `frag` pieces and is put back together on the
//! other side before anything looks at it.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// Plaintext bytes per relay message. Encrypted and base64'd it stays well
/// under Cloudflare's 1 MiB limit.
pub const MAX_FRAME: usize = 384 * 1024;
/// Largest message accepted after reassembly (a model request with a long
/// context and images can be tens of MB).
pub const MAX_MESSAGE: usize = 64 * 1024 * 1024;

/// Split one JSON message into relay-sized frames.
pub fn split(message: &str, next_id: &mut u64) -> Vec<String> {
    if message.len() <= MAX_FRAME {
        return vec![message.to_string()];
    }
    *next_id += 1;
    let id = *next_id;
    let mut frames = Vec::new();
    let mut rest = message;
    while !rest.is_empty() {
        let mut cut = rest.len().min(MAX_FRAME);
        while !rest.is_char_boundary(cut) {
            cut -= 1;
        }
        let (part, tail) = rest.split_at(cut);
        rest = tail;
        frames.push(json!({ "type": "frag", "id": id, "last": rest.is_empty(), "data": part }).to_string());
    }
    frames
}

/// Puts `frag` pieces back together. One message at a time per connection:
/// each side writes a message's pieces back to back.
#[derive(Default)]
pub struct Reassembler {
    id: Option<u64>,
    buf: String,
}

impl Reassembler {
    /// Returns the complete message once its last piece arrived (or the
    /// frame itself when it was not a piece).
    pub fn push(&mut self, frame: String) -> Result<Option<Value>, String> {
        let v: Value = serde_json::from_str(&frame).map_err(|_| "invalid JSON frame".to_string())?;
        if v.get("type").and_then(|t| t.as_str()) != Some("frag") {
            if self.id.is_some() {
                return Err("message interleaved with an unfinished fragmented message".into());
            }
            return Ok(Some(v));
        }
        let id = v.get("id").and_then(|i| i.as_u64()).ok_or("frag without id")?;
        let data = v.get("data").and_then(|d| d.as_str()).ok_or("frag without data")?;
        match self.id {
            None => self.id = Some(id),
            Some(cur) if cur != id => return Err("fragments of two messages interleaved".into()),
            _ => {}
        }
        if self.buf.len() + data.len() > MAX_MESSAGE {
            return Err("fragmented message too large".into());
        }
        self.buf.push_str(data);
        if v.get("last").and_then(|l| l.as_bool()).unwrap_or(false) {
            self.id = None;
            let whole = std::mem::take(&mut self.buf);
            let v: Value = serde_json::from_str(&whole).map_err(|_| "invalid fragmented message".to_string())?;
            if v.get("type").and_then(|t| t.as_str()) == Some("frag") {
                return Err("nested fragments".into());
            }
            return Ok(Some(v));
        }
        Ok(None)
    }
}

/// Everything an edge needs to find main and join it once. It holds the
/// edge relay identity only: never the app's pairing key or the password.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct JoinCode {
    pub v: u8,
    /// Relay server id of main's edge identity.
    pub server_id: String,
    /// Public key of main's edge identity.
    pub key: String,
    pub relay: String,
    pub tls: bool,
    /// One-time token; useless once redeemed or expired.
    pub token: String,
}

const PREFIX: &str = "pse1_";

impl JoinCode {
    pub fn encode(&self) -> String {
        format!("{PREFIX}{}", URL_SAFE_NO_PAD.encode(serde_json::to_vec(self).unwrap_or_default()))
    }

    pub fn decode(code: &str) -> Result<JoinCode, String> {
        let body = code.trim().strip_prefix(PREFIX).ok_or("not a paseo-lite edge join code")?;
        let raw = URL_SAFE_NO_PAD.decode(body.as_bytes()).map_err(|_| "join code is damaged".to_string())?;
        let code: JoinCode = serde_json::from_slice(&raw).map_err(|_| "join code is damaged".to_string())?;
        if code.v != 1 || code.server_id.is_empty() || code.key.is_empty() || code.relay.is_empty() || code.token.is_empty() {
            return Err("join code is incomplete".into());
        }
        Ok(code)
    }
}

/// Names of edges and of their workspace folders become directory names on
/// main, so only a safe subset is allowed.
pub fn safe_name(name: &str) -> Option<String> {
    let name = name.trim();
    let ok = !name.is_empty()
        && name.len() <= 64
        && !name.starts_with('.')
        && name.chars().all(|c| c.is_ascii_alphanumeric() || "-_.".contains(c));
    ok.then(|| name.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_messages_pass_through() {
        let mut id = 0;
        let frames = split(r#"{"type":"ping"}"#, &mut id);
        assert_eq!(frames, vec![r#"{"type":"ping"}"#.to_string()]);
        let mut r = Reassembler::default();
        assert_eq!(r.push(frames[0].clone()).unwrap().unwrap()["type"], "ping");
    }

    #[test]
    fn large_messages_round_trip_in_pieces() {
        let text = "é".repeat(MAX_FRAME); // multi-byte chars: cuts must land on char boundaries
        let message = json!({ "type": "proc.out", "pid": 1, "line": text }).to_string();
        let mut id = 0;
        let frames = split(&message, &mut id);
        assert!(frames.len() > 1);
        assert!(frames.iter().all(|f| f.len() < MAX_FRAME + 200));
        let mut r = Reassembler::default();
        let mut out = None;
        for f in frames {
            out = r.push(f).unwrap();
        }
        assert_eq!(out.unwrap()["line"].as_str().unwrap(), text);
    }

    #[test]
    fn interleaved_pieces_are_refused() {
        let mut r = Reassembler::default();
        r.push(json!({ "type": "frag", "id": 1, "last": false, "data": "{" }).to_string()).unwrap();
        assert!(r.push(json!({ "type": "frag", "id": 2, "last": true, "data": "}" }).to_string()).is_err());
    }

    #[test]
    fn join_code_round_trip() {
        let code = JoinCode { v: 1, server_id: "srv_x".into(), key: "k".into(), relay: "relay.paseo.sh:443".into(), tls: true, token: "t".into() };
        assert_eq!(JoinCode::decode(&code.encode()).unwrap(), code);
        assert!(JoinCode::decode("https://app.paseo.sh/#offer=abc").is_err());
    }

    #[test]
    fn names_are_restricted() {
        assert_eq!(safe_name("laptop-1").as_deref(), Some("laptop-1"));
        assert!(safe_name("../etc").is_none());
        assert!(safe_name("a/b").is_none());
        assert!(safe_name(".hidden").is_none());
        assert!(safe_name("").is_none());
    }
}
