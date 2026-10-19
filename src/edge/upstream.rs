//! Main side of the edge model proxy: Claude Code on an edge talks to a local
//! proxy there, the request crosses the relay, and main sends it on to the one
//! fixed model API with the real credentials added here. Edges never get the
//! key, and cannot pick another host or path.
//!
//! The response goes back as raw bytes (`Connection: close`), so streaming,
//! chunked encoding and compression pass through untouched.

use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Largest request body forwarded (a long context with images).
pub const MAX_BODY: usize = 32 * 1024 * 1024;
const IDLE: Duration = Duration::from_secs(300);
const TOTAL: Duration = Duration::from_secs(30 * 60);
const OAUTH_BETA: &str = "oauth-2025-04-20";

#[derive(Clone)]
enum Auth {
    ApiKey(String),
    Bearer(String),
    /// A Claude subscription login (`CLAUDE_CODE_OAUTH_TOKEN`).
    OAuth(String),
}

#[derive(Clone)]
pub struct Upstream {
    tls: bool,
    host: String,
    port: u16,
    prefix: String,
    auth: Auth,
}

trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

fn env(key: &str) -> Option<String> {
    std::env::var(key).ok().map(|v| v.trim().to_string()).filter(|v| !v.is_empty())
}

impl Upstream {
    /// The daemon's own Claude credentials and base URL. None when it has no
    /// credentials, in which case edges cannot reach a model.
    pub fn from_env() -> Option<Upstream> {
        let auth = if let Some(k) = env("ANTHROPIC_API_KEY") {
            Auth::ApiKey(k)
        } else if let Some(t) = env("ANTHROPIC_AUTH_TOKEN") {
            Auth::Bearer(t)
        } else {
            Auth::OAuth(env("CLAUDE_CODE_OAUTH_TOKEN")?)
        };
        let base = env("ANTHROPIC_BASE_URL").unwrap_or_else(|| "https://api.anthropic.com".into());
        match Upstream::parse(&base, auth) {
            Ok(u) => Some(u),
            Err(e) => {
                crate::warn!("edge model proxy disabled: ANTHROPIC_BASE_URL {e}");
                None
            }
        }
    }

    fn parse(base: &str, auth: Auth) -> Result<Upstream, String> {
        let (tls, rest) = if let Some(r) = base.strip_prefix("https://") {
            (true, r)
        } else if let Some(r) = base.strip_prefix("http://") {
            (false, r)
        } else {
            return Err("must start with https:// or http://".into());
        };
        let (authority, prefix) = rest.split_once('/').map(|(a, p)| (a, format!("/{}", p.trim_end_matches('/')))).unwrap_or((rest, String::new()));
        if authority.is_empty() || authority.contains('@') {
            return Err("has no usable host".into());
        }
        let (host, port) = match authority.rsplit_once(':') {
            Some((h, p)) => (h.to_string(), p.parse::<u16>().map_err(|_| "has a bad port".to_string())?),
            None => (authority.to_string(), if tls { 443 } else { 80 }),
        };
        let prefix = if prefix == "/" { String::new() } else { prefix };
        Ok(Upstream { tls, host, port, prefix, auth })
    }

    /// Only the model endpoints Claude Code needs, with a plain path.
    pub fn check(method: &str, path: &str) -> Result<(), &'static str> {
        if method != "POST" && method != "GET" {
            return Err("method not allowed");
        }
        let plain = path.starts_with('/') && path.bytes().all(|b| b.is_ascii_graphic() && !matches!(b, b'\\' | b'#' | b'%')) && !path.contains("..") && !path.contains("//");
        if !plain {
            return Err("path not allowed");
        }
        let allowed = ["/v1/messages", "/v1/models"].iter().any(|p| path.strip_prefix(p).map(|rest| rest.is_empty() || rest.starts_with('/') || rest.starts_with('?')).unwrap_or(false));
        if allowed { Ok(()) } else { Err("path not allowed") }
    }

    fn head(&self, method: &str, path: &str, headers: &[(String, String)], body_len: usize) -> String {
        const DROP: &[&str] = &[
            "host", "authorization", "x-api-key", "content-length", "connection", "keep-alive", "proxy-authorization",
            "proxy-connection", "transfer-encoding", "te", "trailer", "upgrade", "cookie", "expect",
        ];
        let host = if (self.tls && self.port == 443) || (!self.tls && self.port == 80) { self.host.clone() } else { format!("{}:{}", self.host, self.port) };
        let mut head = format!("{method} {}{path} HTTP/1.1\r\nHost: {host}\r\n", self.prefix);
        let mut beta: Option<String> = None;
        for (k, v) in headers.iter().take(64) {
            let lower = k.to_ascii_lowercase();
            let name_ok = !k.is_empty() && k.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-');
            if !name_ok || v.contains(&['\r', '\n'][..]) || DROP.contains(&lower.as_str()) {
                continue;
            }
            if lower == "anthropic-beta" {
                beta = Some(v.clone());
                continue;
            }
            head.push_str(&format!("{k}: {v}\r\n"));
        }
        match &self.auth {
            Auth::ApiKey(k) => head.push_str(&format!("x-api-key: {k}\r\n")),
            Auth::Bearer(t) => head.push_str(&format!("Authorization: Bearer {t}\r\n")),
            Auth::OAuth(t) => {
                head.push_str(&format!("Authorization: Bearer {t}\r\n"));
                let b = beta.take().unwrap_or_default();
                beta = Some(if b.split(',').any(|x| x.trim() == OAUTH_BETA) { b } else if b.is_empty() { OAUTH_BETA.into() } else { format!("{b},{OAUTH_BETA}") });
            }
        }
        if let Some(b) = beta {
            head.push_str(&format!("anthropic-beta: {b}\r\n"));
        }
        if method == "POST" || body_len > 0 {
            head.push_str(&format!("Content-Length: {body_len}\r\n"));
        }
        head.push_str("Connection: close\r\n\r\n");
        head
    }

    async fn open(&self) -> Result<Box<dyn Io>, String> {
        let tcp = tokio::net::TcpStream::connect((self.host.as_str(), self.port)).await.map_err(|e| e.to_string())?;
        let _ = tcp.set_nodelay(true);
        if !self.tls {
            return Ok(Box::new(tcp));
        }
        let name = rustls::pki_types::ServerName::try_from(self.host.clone()).map_err(|e| e.to_string())?;
        let tls = tokio_rustls::TlsConnector::from(crate::relay::tls_config()).connect(name, tcp).await.map_err(|e| e.to_string())?;
        Ok(Box::new(tls))
    }

    /// Send one request upstream; every piece of the raw response goes to `chunk`.
    pub async fn forward(&self, method: &str, path: &str, headers: &[(String, String)], body: &[u8], mut chunk: impl FnMut(Vec<u8>) -> bool) -> Result<(), String> {
        Upstream::check(method, path)?;
        if body.len() > MAX_BODY {
            return Err("request body too large".into());
        }
        let work = async {
            let mut io = self.open().await?;
            io.write_all(self.head(method, path, headers, body.len()).as_bytes()).await.map_err(|e| e.to_string())?;
            io.write_all(body).await.map_err(|e| e.to_string())?;
            io.flush().await.map_err(|e| e.to_string())?;
            let mut buf = vec![0u8; 64 * 1024];
            loop {
                let n = match tokio::time::timeout(IDLE, io.read(&mut buf)).await {
                    Ok(Ok(n)) => n,
                    // Servers may skip TLS close_notify on Connection: close.
                    Ok(Err(_)) => 0,
                    Err(_) => return Err("model API went quiet".to_string()),
                };
                if n == 0 || !chunk(buf[..n].to_vec()) {
                    return Ok(());
                }
            }
        };
        tokio::time::timeout(TOTAL, work).await.map_err(|_| "model request took too long".to_string())?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_model_endpoints_pass() {
        assert!(Upstream::check("POST", "/v1/messages").is_ok());
        assert!(Upstream::check("POST", "/v1/messages?beta=true").is_ok());
        assert!(Upstream::check("POST", "/v1/messages/count_tokens").is_ok());
        assert!(Upstream::check("GET", "/v1/models").is_ok());
        assert!(Upstream::check("POST", "/v1/messagesx").is_err());
        assert!(Upstream::check("GET", "/v1/organizations").is_err());
        assert!(Upstream::check("POST", "/v1/messages/../../admin").is_err());
        assert!(Upstream::check("POST", "/v1/messages/%2e%2e/admin").is_err());
        assert!(Upstream::check("POST", "http://127.0.0.1:9222/json").is_err());
        assert!(Upstream::check("DELETE", "/v1/messages").is_err());
    }

    #[test]
    fn credentials_are_added_and_the_edge_cannot_set_its_own() {
        let u = Upstream::parse("https://gw.example.com/anthropic/", Auth::ApiKey("REAL".into())).unwrap();
        let headers = vec![
            ("x-api-key".to_string(), "edge-dummy".to_string()),
            ("Host".to_string(), "127.0.0.1:9222".to_string()),
            ("anthropic-version".to_string(), "2023-06-01".to_string()),
            ("x-evil".to_string(), "a\r\nx-api-key: injected".to_string()),
        ];
        let head = u.head("POST", "/v1/messages", &headers, 2);
        assert!(head.starts_with("POST /anthropic/v1/messages HTTP/1.1\r\nHost: gw.example.com\r\n"), "{head}");
        assert!(head.contains("x-api-key: REAL\r\n"));
        assert!(!head.contains("edge-dummy") && !head.contains("9222") && !head.contains("injected"), "{head}");
        assert!(head.contains("anthropic-version: 2023-06-01\r\n"));
        assert!(head.ends_with("Content-Length: 2\r\nConnection: close\r\n\r\n"));
    }

    #[test]
    fn oauth_adds_its_beta_flag_once() {
        let u = Upstream::parse("https://api.anthropic.com", Auth::OAuth("tok".into())).unwrap();
        let head = u.head("POST", "/v1/messages", &[("anthropic-beta".into(), "a,oauth-2025-04-20".into())], 0);
        assert!(head.contains("Authorization: Bearer tok\r\n"));
        assert!(head.contains("anthropic-beta: a,oauth-2025-04-20\r\n"));
        assert_eq!(head.matches(OAUTH_BETA).count(), 1);
    }

    #[test]
    fn base_urls_parse() {
        let u = Upstream::parse("http://127.0.0.1:8080", Auth::ApiKey("k".into())).unwrap();
        assert_eq!((u.tls, u.host.as_str(), u.port, u.prefix.as_str()), (false, "127.0.0.1", 8080, ""));
        assert!(Upstream::parse("ftp://x", Auth::ApiKey("k".into())).is_err());
        assert!(Upstream::parse("https://user:pw@x", Auth::ApiKey("k".into())).is_err());
    }
}
