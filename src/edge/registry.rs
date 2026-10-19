//! Main's list of edges (`edges.json` in the paseo-lite home, mode 0600):
//! pending one-time invites and the public key of every edge that joined.
//! The `paseo-lite edge` CLI and the daemon share the file; the daemon
//! re-reads it on every edge handshake, so invites and removals apply at once.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Registry {
    #[serde(default)]
    pub invites: Vec<Invite>,
    #[serde(default)]
    pub edges: Vec<Edge>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Invite {
    pub token: String,
    pub name: String,
    pub expires_at_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Edge {
    pub name: String,
    /// The edge's own public key: the only thing that identifies it after joining.
    pub key: String,
    pub added_at: String,
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

fn path(home: &Path) -> PathBuf {
    home.join("edges.json")
}

pub fn exists(home: &Path) -> bool {
    path(home).exists()
}

fn eq(a: &str, b: &str) -> bool {
    a.len() == b.len() && a.bytes().zip(b.bytes()).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// 256 random bits (two v4 UUIDs, from the OS CSPRNG), URL-safe.
pub fn new_token() -> String {
    use base64::Engine;
    let mut raw = [0u8; 32];
    raw[..16].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
    raw[16..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw)
}

impl Registry {
    pub fn load(home: &Path) -> Registry {
        std::fs::read_to_string(path(home)).ok().and_then(|s| serde_json::from_str(&s).ok()).unwrap_or_default()
    }

    pub fn save(&self, home: &Path) -> std::io::Result<()> {
        let json = serde_json::to_string_pretty(self).unwrap_or_default() + "\n";
        crate::util::write_atomic(&path(home), json.as_bytes(), true)
    }

    fn prune(&mut self) {
        let now = now_ms();
        self.invites.retain(|i| i.expires_at_ms > now);
    }

    pub fn invite(&mut self, name: &str, ttl_ms: u64) -> Result<String, String> {
        self.prune();
        let name = super::wire::safe_name(name).ok_or("edge names may use letters, digits, '-', '_' and '.' (64 at most)")?;
        if self.edges.iter().any(|e| e.name == name) {
            return Err(format!("an edge named {name} already joined; remove it first (paseo-lite edge rm {name})"));
        }
        let token = new_token();
        self.invites.retain(|i| i.name != name);
        self.invites.push(Invite { token: token.clone(), name, expires_at_ms: now_ms() + ttl_ms });
        Ok(token)
    }

    pub fn name_for_key(&self, key: &str) -> Option<String> {
        self.edges.iter().find(|e| eq(&e.key, key)).map(|e| e.name.clone())
    }

    /// Use up an invite: the edge's key joins under the invite's name.
    pub fn redeem(&mut self, token: &str, key: &str) -> Result<String, &'static str> {
        self.prune();
        let idx = self.invites.iter().position(|i| eq(&i.token, token)).ok_or("invalid_or_expired_code")?;
        let invite = self.invites.remove(idx);
        if self.edges.iter().any(|e| e.name == invite.name) {
            return Err("name_taken");
        }
        self.edges.push(Edge { name: invite.name.clone(), key: key.to_string(), added_at: crate::util::now_iso() });
        Ok(invite.name)
    }

    pub fn remove(&mut self, name: &str) -> bool {
        let before = self.edges.len() + self.invites.len();
        self.edges.retain(|e| e.name != name);
        self.invites.retain(|i| i.name != name);
        before != self.edges.len() + self.invites.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_invite_is_used_once() {
        let mut r = Registry::default();
        let token = r.invite("laptop", 60_000).unwrap();
        assert_eq!(r.redeem(&token, "key1").unwrap(), "laptop");
        assert_eq!(r.name_for_key("key1").as_deref(), Some("laptop"));
        assert_eq!(r.redeem(&token, "key2"), Err("invalid_or_expired_code"));
        assert!(r.name_for_key("key2").is_none());
    }

    #[test]
    fn expired_invites_are_refused() {
        let mut r = Registry::default();
        let token = r.invite("laptop", 0).unwrap();
        assert_eq!(r.redeem(&token, "key1"), Err("invalid_or_expired_code"));
    }

    #[test]
    fn removing_an_edge_forgets_its_key() {
        let mut r = Registry::default();
        let token = r.invite("laptop", 60_000).unwrap();
        r.redeem(&token, "key1").unwrap();
        assert!(r.remove("laptop"));
        assert!(r.name_for_key("key1").is_none());
        assert!(r.invite("bad/name", 60_000).is_err());
    }
}
