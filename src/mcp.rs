//! The zilong-router MCP gateway (`PASEO_ZILONG_MCP_URL`), given to Claude
//! Code and Codex alike as one Streamable HTTP server named `zilong-router`.
//! The API key (`PASEO_ZILONG_API_KEY`) never goes on a command line: Claude
//! reads it from a private `--mcp-config` file, Codex from the environment it
//! inherits (`bearer_token_env_var`). The user's own servers (`~/.claude.json`,
//! `.mcp.json`, `~/.codex/config.toml`) still load.

use crate::config::Config;
use serde_json::json;
use std::path::PathBuf;

pub const NAME: &str = "zilong-router";
pub const KEY_ENV: &str = "PASEO_ZILONG_API_KEY";

fn claude_path(cfg: &Config) -> PathBuf {
    cfg.home.join("claude-mcp.json")
}

/// Writes Claude's `--mcp-config` file at startup (0600, it holds the key),
/// or removes a stale one when the gateway is not configured.
pub fn write_claude_config(cfg: &Config) {
    let path = claude_path(cfg);
    let Some(url) = &cfg.zilong_mcp_url else {
        let _ = std::fs::remove_file(&path);
        return;
    };
    let mut server = json!({ "type": "http", "url": url });
    if let Some(key) = &cfg.zilong_api_key {
        server["headers"] = json!({ "Authorization": format!("Bearer {key}") });
    }
    let config = json!({ "mcpServers": { NAME: server } });
    match crate::util::write_atomic(&path, config.to_string().as_bytes(), true) {
        Ok(()) => crate::log!("mcp: {NAME} at {url} for Claude Code and Codex"),
        Err(e) => crate::warn!("mcp: cannot write {}: {e}; Claude Code starts without {NAME}", path.display()),
    }
}

/// `--mcp-config <file>` for `claude`, when the gateway is configured.
pub fn claude_config(cfg: &Config) -> Option<PathBuf> {
    cfg.zilong_mcp_url.as_ref()?;
    Some(claude_path(cfg)).filter(|p| p.exists())
}

/// `-c` overrides for `codex app-server`.
pub fn codex_args(cfg: &Config) -> Vec<String> {
    let Some(url) = &cfg.zilong_mcp_url else { return Vec::new() };
    let mut args = vec!["-c".to_string(), format!("mcp_servers.{NAME}.url={}", toml_str(url))];
    if cfg.zilong_api_key.is_some() {
        args.extend(["-c".to_string(), format!("mcp_servers.{NAME}.bearer_token_env_var={}", toml_str(KEY_ENV))]);
    }
    args
}

/// A TOML basic string (JSON string escaping is valid TOML for these).
fn toml_str(s: &str) -> String {
    serde_json::to_string(s).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codex_gets_url_and_key_variable_only() {
        let mut cfg = Config::load(&[]);
        cfg.zilong_mcp_url = None;
        assert!(codex_args(&cfg).is_empty());
        cfg.zilong_mcp_url = Some("http://pi:20128/mcp".into());
        cfg.zilong_api_key = Some("sk-secret".into());
        let args = codex_args(&cfg).join(" ");
        assert_eq!(args, r#"-c mcp_servers.zilong-router.url="http://pi:20128/mcp" -c mcp_servers.zilong-router.bearer_token_env_var="PASEO_ZILONG_API_KEY""#);
        assert!(!args.contains("sk-secret"));
    }
}
