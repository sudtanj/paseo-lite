//! MCP servers given to every Claude Code and Codex agent (sub-agents inherit
//! them), next to the user's own (`~/.claude.json`, `.mcp.json`,
//! `~/.codex/config.toml`), as Streamable HTTP servers:
//!
//! - `zilong-router`: the external gateway (`PASEO_ZILONG_MCP_URL`), when set.
//!   Its API key (`PASEO_ZILONG_API_KEY`) reaches Codex through the
//!   environment it inherits (`bearer_token_env_var`).
//! - `paseo-lite`: the daemon's own debug server (`/mcp`, mcp_server.rs), on
//!   unless `PASEO_DEBUG_MCP=0`. Its bearer token is random per daemon start
//!   and reaches Codex as `PASEO_LITE_MCP_TOKEN` on the app-server's
//!   environment.
//!
//! Secrets never go on a command line: Claude reads them from a private
//! (0600) `--mcp-config` file, Codex from its environment.

use crate::config::Config;
use serde_json::{json, Value};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;

pub const NAME: &str = "zilong-router";
pub const KEY_ENV: &str = "PASEO_ZILONG_API_KEY";
pub const DEBUG_NAME: &str = "paseo-lite";
pub const DEBUG_TOKEN_ENV: &str = "PASEO_LITE_MCP_TOKEN";

/// The daemon's own MCP server: where agents reach it and the token they need.
#[derive(Clone)]
pub struct DebugMcp {
    pub url: String,
    pub token: String,
    /// The address agents connect to (loopback unless the daemon listens on
    /// one specific address); requests from other peers are refused.
    pub host: IpAddr,
}

/// Never print the token (Config derives Debug).
impl std::fmt::Debug for DebugMcp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DebugMcp").field("url", &self.url).field("token", &"<redacted>").field("host", &self.host).finish()
    }
}

impl DebugMcp {
    /// The server for a daemon listening on `listen` (`PASEO_LISTEN`), with a
    /// fresh token; None (with a warning) when no local URL can be derived.
    pub fn for_listen(listen: &str) -> Option<DebugMcp> {
        match local_endpoint(listen) {
            Ok((host, port)) => Some(DebugMcp { url: format!("http://{}/mcp", SocketAddr::new(host, port)), token: new_token(), host }),
            Err(why) => {
                crate::warn!("mcp: {DEBUG_NAME} debug server disabled: {why}");
                None
            }
        }
    }

    /// Requests only from this machine.
    pub fn peer_allowed(&self, peer: IpAddr) -> bool {
        let peer = match peer {
            IpAddr::V6(v6) => v6.to_ipv4_mapped().map(IpAddr::V4).unwrap_or(peer),
            v4 => v4,
        };
        peer.is_loopback() || peer == self.host
    }

    /// `Authorization: Bearer <token>`, compared in constant time.
    pub fn authorized(&self, authorization: Option<&str>) -> bool {
        let Some(value) = authorization.map(str::trim) else { return false };
        let Some((scheme, token)) = value.split_once(' ') else { return false };
        scheme.eq_ignore_ascii_case("bearer") && crate::session::constant_time_eq(token.trim().as_bytes(), self.token.as_bytes())
    }
}

/// 64 random hex characters (two v4 UUIDs: 244 random bits).
fn new_token() -> String {
    format!("{}{}", uuid::Uuid::new_v4().simple(), uuid::Uuid::new_v4().simple())
}

/// The address and port agents on this machine reach the daemon at:
/// 127.0.0.1 for a wildcard or loopback listen address (or `localhost`),
/// the address itself when the daemon listens on one specific address.
fn local_endpoint(listen: &str) -> Result<(IpAddr, u16), String> {
    let listen = listen.trim();
    if listen.starts_with('/') || listen.starts_with("unix:") {
        return Err(format!("listen address {listen} is a unix socket"));
    }
    let (ip, port) = match listen.parse::<SocketAddr>() {
        Ok(addr) => (Some(addr.ip()), addr.port()),
        Err(_) => {
            let (host, port) = listen.rsplit_once(':').ok_or_else(|| format!("listen address {listen} has no port"))?;
            let port = port.parse::<u16>().map_err(|_| format!("listen address {listen} has no valid port"))?;
            if !host.is_empty() && !host.eq_ignore_ascii_case("localhost") {
                return Err(format!("cannot derive a local address from listen address {listen}"));
            }
            (None, port)
        }
    };
    if port == 0 {
        return Err(format!("listen address {listen} has no fixed port"));
    }
    let host = match ip {
        Some(ip) if ip.is_loopback() => ip,
        Some(ip) if !ip.is_unspecified() => ip,
        _ => IpAddr::V4(Ipv4Addr::LOCALHOST),
    };
    Ok((host, port))
}

fn claude_path(cfg: &Config) -> PathBuf {
    cfg.home.join("claude-mcp.json")
}

/// The `--mcp-config` contents, None when no server is configured.
fn claude_config_json(cfg: &Config) -> Option<Value> {
    let mut servers = serde_json::Map::new();
    if let Some(url) = &cfg.zilong_mcp_url {
        let mut server = json!({ "type": "http", "url": url });
        if let Some(key) = &cfg.zilong_api_key {
            server["headers"] = json!({ "Authorization": format!("Bearer {key}") });
        }
        servers.insert(NAME.to_string(), server);
    }
    if let Some(debug) = &cfg.debug_mcp {
        let server = json!({ "type": "http", "url": debug.url, "headers": { "Authorization": format!("Bearer {}", debug.token) } });
        servers.insert(DEBUG_NAME.to_string(), server);
    }
    (!servers.is_empty()).then(|| json!({ "mcpServers": servers }))
}

/// Writes Claude's `--mcp-config` file at startup (0600, it holds the
/// secrets), or removes a stale one when no server is configured.
pub fn write_claude_config(cfg: &Config) {
    let path = claude_path(cfg);
    let Some(config) = claude_config_json(cfg) else {
        let _ = std::fs::remove_file(&path);
        return;
    };
    match crate::util::write_atomic(&path, config.to_string().as_bytes(), true) {
        Ok(()) => {
            if let Some(url) = &cfg.zilong_mcp_url {
                crate::log!("mcp: {NAME} at {url} for Claude Code and Codex");
            }
            if let Some(debug) = &cfg.debug_mcp {
                crate::log!("mcp: {DEBUG_NAME} debug server at {} for Claude Code and Codex", debug.url);
            }
        }
        Err(e) => crate::warn!("mcp: cannot write {}: {e}; Claude Code starts without its MCP servers", path.display()),
    }
}

/// `--mcp-config <file>` for `claude`, when any server is configured.
pub fn claude_config(cfg: &Config) -> Option<PathBuf> {
    if cfg.zilong_mcp_url.is_none() && cfg.debug_mcp.is_none() {
        return None;
    }
    Some(claude_path(cfg)).filter(|p| p.exists())
}

/// `-c` overrides for `codex app-server`.
pub fn codex_args(cfg: &Config) -> Vec<String> {
    let mut args = Vec::new();
    if let Some(url) = &cfg.zilong_mcp_url {
        args.extend(["-c".to_string(), format!("mcp_servers.{NAME}.url={}", toml_str(url))]);
        if cfg.zilong_api_key.is_some() {
            args.extend(["-c".to_string(), format!("mcp_servers.{NAME}.bearer_token_env_var={}", toml_str(KEY_ENV))]);
        }
    }
    if let Some(debug) = &cfg.debug_mcp {
        args.extend(["-c".to_string(), format!("mcp_servers.{DEBUG_NAME}.url={}", toml_str(&debug.url))]);
        args.extend(["-c".to_string(), format!("mcp_servers.{DEBUG_NAME}.bearer_token_env_var={}", toml_str(DEBUG_TOKEN_ENV))]);
    }
    args
}

/// Environment for `codex app-server` (the debug server's token).
pub fn codex_env(cfg: &Config) -> Vec<(&'static str, String)> {
    cfg.debug_mcp.iter().map(|d| (DEBUG_TOKEN_ENV, d.token.clone())).collect()
}

/// A TOML basic string (JSON string escaping is valid TOML for these).
fn toml_str(s: &str) -> String {
    serde_json::to_string(s).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn debug(url: &str) -> DebugMcp {
        DebugMcp { url: url.into(), token: "t0ken".into(), host: IpAddr::V4(Ipv4Addr::LOCALHOST) }
    }

    #[test]
    fn codex_gets_url_and_key_variable_only() {
        let mut cfg = Config::load(&[]);
        cfg.zilong_mcp_url = None;
        cfg.debug_mcp = None;
        assert!(codex_args(&cfg).is_empty());
        assert!(codex_env(&cfg).is_empty());
        cfg.zilong_mcp_url = Some("http://pi:20128/mcp".into());
        cfg.zilong_api_key = Some("sk-secret".into());
        let args = codex_args(&cfg).join(" ");
        assert_eq!(args, r#"-c mcp_servers.zilong-router.url="http://pi:20128/mcp" -c mcp_servers.zilong-router.bearer_token_env_var="PASEO_ZILONG_API_KEY""#);
        assert!(!args.contains("sk-secret"));
    }

    #[test]
    fn codex_gets_the_debug_server_with_its_token_in_the_environment() {
        let mut cfg = Config::load(&[]);
        cfg.zilong_mcp_url = None;
        cfg.debug_mcp = Some(debug("http://127.0.0.1:6768/mcp"));
        let args = codex_args(&cfg).join(" ");
        assert_eq!(args, r#"-c mcp_servers.paseo-lite.url="http://127.0.0.1:6768/mcp" -c mcp_servers.paseo-lite.bearer_token_env_var="PASEO_LITE_MCP_TOKEN""#);
        assert!(!args.contains("t0ken"));
        assert_eq!(codex_env(&cfg), vec![(DEBUG_TOKEN_ENV, "t0ken".to_string())]);
    }

    #[test]
    fn claude_config_holds_both_servers_or_none() {
        let mut cfg = Config::load(&[]);
        cfg.zilong_mcp_url = None;
        cfg.debug_mcp = None;
        assert!(claude_config_json(&cfg).is_none());
        cfg.debug_mcp = Some(debug("http://127.0.0.1:6768/mcp"));
        let only_debug = claude_config_json(&cfg).unwrap();
        assert_eq!(only_debug["mcpServers"].as_object().unwrap().len(), 1);
        assert_eq!(only_debug["mcpServers"]["paseo-lite"], json!({ "type": "http", "url": "http://127.0.0.1:6768/mcp", "headers": { "Authorization": "Bearer t0ken" } }));
        cfg.zilong_mcp_url = Some("http://pi:20128/mcp".into());
        cfg.zilong_api_key = Some("sk-secret".into());
        let both = claude_config_json(&cfg).unwrap();
        assert_eq!(both["mcpServers"]["zilong-router"]["headers"]["Authorization"], "Bearer sk-secret");
        assert_eq!(both["mcpServers"]["paseo-lite"]["url"], "http://127.0.0.1:6768/mcp");
    }

    #[test]
    fn debug_url_follows_the_listen_address() {
        let url = |l: &str| DebugMcp::for_listen(l).map(|d| d.url);
        assert_eq!(url("0.0.0.0:6768").as_deref(), Some("http://127.0.0.1:6768/mcp"));
        assert_eq!(url("127.0.0.1:7000").as_deref(), Some("http://127.0.0.1:7000/mcp"));
        assert_eq!(url("[::]:6768").as_deref(), Some("http://127.0.0.1:6768/mcp"));
        assert_eq!(url("[::1]:6768").as_deref(), Some("http://[::1]:6768/mcp"));
        assert_eq!(url("localhost:6768").as_deref(), Some("http://127.0.0.1:6768/mcp"));
        assert_eq!(url("192.168.1.5:6768").as_deref(), Some("http://192.168.1.5:6768/mcp"));
        assert_eq!(url("/run/paseo.sock"), None);
        assert_eq!(url("example.com:6768"), None);
        assert_eq!(url("127.0.0.1:0"), None);
    }

    #[test]
    fn debug_auth_needs_the_exact_bearer_token() {
        let d = DebugMcp::for_listen("127.0.0.1:6768").unwrap();
        assert_eq!(d.token.len(), 64);
        assert!(d.authorized(Some(&format!("Bearer {}", d.token))));
        assert!(d.authorized(Some(&format!("bearer {}", d.token))));
        assert!(!d.authorized(None));
        assert!(!d.authorized(Some(&d.token)));
        assert!(!d.authorized(Some("Bearer wrong")));
        assert!(!d.authorized(Some(&format!("Basic {}", d.token))));
        assert!(!format!("{d:?}").contains(&d.token));
    }

    #[test]
    fn debug_server_only_answers_this_machine() {
        let d = DebugMcp::for_listen("0.0.0.0:6768").unwrap();
        assert!(d.peer_allowed("127.0.0.1".parse().unwrap()));
        assert!(d.peer_allowed("::1".parse().unwrap()));
        assert!(d.peer_allowed("::ffff:127.0.0.1".parse().unwrap()));
        assert!(!d.peer_allowed("10.0.0.2".parse().unwrap()));
        let specific = DebugMcp::for_listen("10.0.0.2:6768").unwrap();
        assert!(specific.peer_allowed("10.0.0.2".parse().unwrap()));
        assert!(!specific.peer_allowed("10.0.0.3".parse().unwrap()));
    }
}
