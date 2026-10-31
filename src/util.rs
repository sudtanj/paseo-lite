use std::collections::VecDeque;
use std::io::Write;
use std::path::Path;
use std::sync::Mutex;
use std::time::SystemTime;

/// Info-level log line. Printed unless `PASEO_LOG_CONSOLE_LEVEL=warn|error`;
/// always kept for the `daemon_logs` MCP tool.
#[macro_export]
macro_rules! log {
    ($($arg:tt)*) => {
        $crate::util::emit_log("", $crate::util::log_enabled(2), format!($($arg)*))
    };
}

/// Warning-level log line, for failures the operator should see.
#[macro_export]
macro_rules! warn {
    ($($arg:tt)*) => {
        $crate::util::emit_log("WARN ", $crate::util::log_enabled(3), format!($($arg)*))
    };
}

/// Recent log lines kept in memory for the `daemon_logs` MCP tool (mcp_server.rs):
/// at most `LOG_KEEP` lines of at most `LOG_LINE_MAX` bytes (~1 MB worst case).
const LOG_KEEP: usize = 2000;
const LOG_LINE_MAX: usize = 512;
static LOG_RING: Mutex<VecDeque<String>> = Mutex::new(VecDeque::new());

/// Print a log line to stderr (when `print`) and remember it in the ring buffer.
pub fn emit_log(level: &str, print: bool, msg: String) {
    let line = format!("[{}] {level}{msg}", now_iso());
    if print {
        eprintln!("{line}");
    }
    let line = if line.len() > LOG_LINE_MAX { truncate(&line, LOG_LINE_MAX) } else { line };
    if let Ok(mut ring) = LOG_RING.lock() {
        if ring.len() >= LOG_KEEP {
            ring.pop_front();
        }
        ring.push_back(line);
    }
}

/// The last `lines` log lines (oldest first), optionally only those
/// containing `filter` (case-insensitive).
pub fn recent_logs(lines: usize, filter: Option<&str>) -> Vec<String> {
    let Ok(ring) = LOG_RING.lock() else { return Vec::new() };
    let filter = filter.map(str::to_ascii_lowercase).filter(|f| !f.is_empty());
    let mut out: Vec<String> = ring.iter().rev().filter(|l| filter.as_ref().map(|f| l.to_ascii_lowercase().contains(f.as_str())).unwrap_or(true)).take(lines).cloned().collect();
    out.reverse();
    out
}

/// 1=debug 2=info 3=warn 4=error, from `PASEO_LOG_CONSOLE_LEVEL` (default info).
pub fn log_enabled(level: u8) -> bool {
    static MIN: std::sync::OnceLock<u8> = std::sync::OnceLock::new();
    let min = *MIN.get_or_init(|| match std::env::var("PASEO_LOG_CONSOLE_LEVEL").unwrap_or_default().trim().to_ascii_lowercase().as_str() {
        "trace" | "debug" => 1,
        "warn" | "warning" => 3,
        "error" | "fatal" | "silent" => 4,
        _ => 2,
    });
    level >= min
}

pub fn now_iso() -> String {
    humantime::format_rfc3339_millis(SystemTime::now()).to_string()
}

pub fn new_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Write via temp file + rename so a crash never leaves a torn file behind.
pub fn write_atomic(path: &Path, data: &[u8], private: bool) -> std::io::Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let tmp = path.with_extension(format!("tmp{}", std::process::id()));
    {
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create(true).truncate(true);
        #[cfg(unix)]
        if private {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        #[cfg(not(unix))]
        let _ = private;
        let mut f = opts.open(&tmp)?;
        f.write_all(data)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)
}

pub fn basename(path: &str) -> String {
    Path::new(path)
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| path.to_string())
}

/// Truncate to at most `max` bytes on a char boundary.
pub fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &s[..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_keeps_short_strings() {
        assert_eq!(truncate("hello", 10), "hello");
    }

    #[test]
    fn truncate_cuts_on_char_boundary() {
        // "é" is two bytes; cutting at byte 1 must back up to 0.
        assert_eq!(truncate("éa", 1), "…");
        assert_eq!(truncate("abcdef", 3), "abc…");
    }

    #[test]
    fn basename_of_paths() {
        assert_eq!(basename("/home/u/project"), "project");
    }

    #[test]
    fn ids_are_unique() {
        assert_ne!(new_id(), new_id());
    }

    #[test]
    fn recent_logs_are_kept_and_filtered() {
        let marker = format!("ring-test-{}", new_id());
        emit_log("", false, format!("{marker} first"));
        emit_log("WARN ", true, format!("{marker} second {}", "x".repeat(2000)));
        let lines = recent_logs(10, Some(&marker.to_ascii_uppercase()));
        assert_eq!(lines.len(), 2);
        assert!(lines[0].ends_with("first"));
        assert!(lines[1].contains("WARN ") && lines[1].len() <= LOG_LINE_MAX + "…".len());
        assert_eq!(recent_logs(1, Some(&marker)).len(), 1);
    }
}
