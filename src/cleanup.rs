//! Deleting a removed project's local directory.
//!
//! The rule is "never destroy work that can't be recovered": a directory is
//! deleted when git shows everything in it already exists on a remote (or it
//! is a still-empty folder paseo-lite created). Anything else is kept and the
//! reason is reported back. `PASEO_DELETE_PROJECT_FILES=all` drops the git
//! check, `never` disables deletion. Whatever the policy, only directories
//! strictly inside the user's home are deleted, never one holding
//! paseo-lite's data or another known project.

use crate::checkout::git;
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Policy {
    Safe,
    All,
    Never,
}

impl Policy {
    pub fn parse(v: Option<&str>) -> Policy {
        match v.map(|v| v.trim().to_ascii_lowercase()).as_deref() {
            Some("all") => Policy::All,
            Some("never") | Some("off") | Some("0") | Some("false") => Policy::Never,
            _ => Policy::Safe,
        }
    }
}

/// The user's home directory: `$HOME`, else the password database (systemd
/// services often run without `HOME`).
fn home_dir() -> Option<PathBuf> {
    match std::env::var_os("HOME").filter(|h| !h.is_empty()) {
        Some(h) => Some(PathBuf::from(h)),
        None => passwd_home(),
    }
}

#[cfg(unix)]
fn passwd_home() -> Option<PathBuf> {
    // SAFETY: getpwuid returns a pointer into static storage (or null); the
    // string is copied out immediately and the daemon calls it from one thread.
    unsafe {
        let pw = libc::getpwuid(libc::getuid());
        if pw.is_null() || (*pw).pw_dir.is_null() {
            return None;
        }
        let dir = std::ffi::CStr::from_ptr((*pw).pw_dir).to_string_lossy().into_owned();
        (!dir.is_empty()).then(|| PathBuf::from(dir))
    }
}

#[cfg(not(unix))]
fn passwd_home() -> Option<PathBuf> {
    None
}

/// Why `path` must never be deleted, if it mustn't. `others` are every other
/// directory paseo-lite knows about (projects and agent cwds).
fn protected(path: &Path, daemon_home: &Path, others: &[String]) -> Option<String> {
    if !path.is_absolute() {
        return Some("not an absolute path".into());
    }
    let Ok(meta) = std::fs::symlink_metadata(path) else { return Some("it cannot be inspected".into()) };
    if meta.file_type().is_symlink() {
        return Some("it is a symlink".into());
    }
    if !meta.is_dir() {
        return Some("not a directory".into());
    }
    let Ok(canon) = std::fs::canonicalize(path) else { return Some("it cannot be resolved".into()) };
    // Only ever delete strictly inside the user's home directory: never the
    // home itself, another user's home, or system locations.
    let Some(home) = home_dir().and_then(|h| std::fs::canonicalize(h).ok()) else {
        return Some("the home directory is unknown".into());
    };
    if canon == home || !canon.starts_with(&home) {
        return Some("it is not inside your home directory".into());
    }
    let inside = |other: &Path| std::fs::canonicalize(other).map(|o| o.starts_with(&canon)).unwrap_or(false);
    if inside(daemon_home) {
        return Some("it contains paseo-lite's data".into());
    }
    if let Some(other) = others.iter().find(|o| Path::new(o.as_str()) != path && inside(Path::new(o.as_str()))) {
        return Some(format!("it contains another project ({other})"));
    }
    None
}

/// Ignored paths that are safe to lose because a build or install recreates them.
fn regenerable(rel: &str) -> bool {
    const DIRS: [&str; 16] = [
        "node_modules", "target", "dist", "build", "out", ".next", ".nuxt", ".turbo", ".cache", "coverage", ".venv", "venv", "__pycache__", ".gradle", ".pytest_cache", ".mypy_cache",
    ];
    let rel = rel.trim_end_matches('/');
    rel.split('/').any(|part| DIRS.contains(&part)) || rel.ends_with(".pyc") || rel.ends_with(".log") || rel.ends_with(".DS_Store")
}

/// Files Claude Code writes under `.claude/` while it runs (it lists them in
/// `.git/info/exclude` itself): state, locks and caches, never user work.
/// Worktrees and local agent memory are not among them.
fn claude_runtime(rel: &str) -> bool {
    const FILES: [&str; 5] = ["scheduled_tasks.lock", "scheduled_tasks.json", "agent-registry.json", "first-run", "assistant-daemon-state.json"];
    const DIRS: [&str; 3] = ["routines/.state", "checkpoints", "mailbox"];
    let rel = rel.trim_end_matches('/');
    let Some((_, inner)) = rel.split_once(".claude/").filter(|(before, _)| before.is_empty() || before.ends_with('/')) else { return false };
    FILES.contains(&inner) || DIRS.iter().any(|d| inner == *d || inner.starts_with(&format!("{d}/")))
}

/// An ignored path git reports as a whole (`.claude/`): safe to lose when
/// every file in it is regenerable or Claude Code runtime state.
fn regenerable_tree(root: &Path, rel: &str) -> bool {
    let rel = rel.trim_end_matches('/');
    let path = root.join(rel);
    let Ok(meta) = std::fs::symlink_metadata(&path) else { return true };
    if !meta.is_dir() {
        return regenerable(rel) || claude_runtime(rel);
    }
    if regenerable(rel) || claude_runtime(rel) {
        return true;
    }
    let Ok(entries) = std::fs::read_dir(&path) else { return false };
    let mut seen = 0;
    for entry in entries {
        seen += 1;
        // A big tree is not Claude's runtime state: keep it.
        if seen > 200 {
            return false;
        }
        let Ok(entry) = entry else { return false };
        let child = format!("{rel}/{}", entry.file_name().to_string_lossy());
        if !regenerable_tree(root, &child) {
            return false;
        }
    }
    true
}

/// Why the work in `dir` can't be recovered from a remote; None when it can.
/// Any git error counts as unrecoverable.
async fn unrecoverable(dir: &Path) -> Option<String> {
    let Ok(top) = git(dir, &["rev-parse", "--show-toplevel"]).await else {
        return Some("it is not a git repository, so there is no remote copy".into());
    };
    let same = std::fs::canonicalize(top.trim()).ok() == std::fs::canonicalize(dir).ok();
    if !same {
        return Some("it is a folder inside a larger git repository".into());
    }
    let status = match git(dir, &["status", "--porcelain", "--ignored=matching"]).await {
        Ok(s) => s,
        Err(e) => return Some(format!("git status failed: {e}")),
    };
    for line in status.lines().filter(|l| !l.trim().is_empty()) {
        match line.strip_prefix("!! ") {
            Some(rel) if regenerable(rel) || regenerable_tree(dir, rel.trim()) => {}
            Some(rel) => return Some(format!("it has ignored files git can't restore (e.g. {})", rel.trim())),
            None => return Some("it has uncommitted changes".into()),
        }
    }
    // HEAD too, so commits on a detached HEAD count.
    match git(dir, &["log", "HEAD", "--branches", "--not", "--remotes", "--oneline"]).await {
        Ok(s) if !s.trim().is_empty() => return Some(format!("{} commit(s) are not on any remote", s.trim().lines().count())),
        Err(e) => return Some(format!("git log failed: {e}")),
        _ => {}
    }
    match git(dir, &["stash", "list"]).await {
        Ok(s) if !s.trim().is_empty() => return Some("it has stashed changes".into()),
        Err(e) => return Some(format!("git stash list failed: {e}")),
        _ => {}
    }
    match git(dir, &["worktree", "list", "--porcelain"]).await {
        Ok(s) if s.lines().filter(|l| l.starts_with("worktree ")).count() > 1 => return Some("other git worktrees depend on it".into()),
        Err(e) => return Some(format!("git worktree list failed: {e}")),
        _ => {}
    }
    None
}

fn is_empty_dir(path: &Path) -> bool {
    std::fs::read_dir(path).map(|mut d| d.next().is_none()).unwrap_or(false)
}

/// Delete a removed project's directory if the policy allows it.
/// `Ok(true)` deleted, `Ok(false)` nothing to delete, `Err(reason)` kept.
pub async fn delete_project_dir(dir: String, owned: bool, policy: Policy, daemon_home: PathBuf, others: Vec<String>) -> Result<bool, String> {
    let path = Path::new(&dir);
    if policy == Policy::Never {
        return Err("kept: deleting project files is disabled (PASEO_DELETE_PROJECT_FILES=never)".into());
    }
    if std::fs::symlink_metadata(path).is_err() {
        return Ok(false);
    }
    if let Some(reason) = protected(path, &daemon_home, &others) {
        return Err(format!("kept {dir}: {reason}"));
    }
    // A folder paseo-lite created and nobody used yet goes without checks;
    // everything else (even our own clones, which may hold weeks of work)
    // must be recoverable from a remote.
    let fresh = owned && is_empty_dir(path);
    if policy == Policy::Safe && !fresh {
        if let Some(reason) = unrecoverable(path).await {
            return Err(format!("kept {dir}: {reason}"));
        }
    }
    // std's remove_dir_all never follows symlinks out of the tree; it runs on
    // the blocking pool so a big node_modules doesn't stall the daemon.
    tokio::fs::remove_dir_all(path).await.map_err(|e| format!("could not delete {dir}: {e}"))?;
    crate::log!("deleted project directory {dir}");
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policy_parsing() {
        assert_eq!(Policy::parse(None), Policy::Safe);
        assert_eq!(Policy::parse(Some("ALL")), Policy::All);
        assert_eq!(Policy::parse(Some("never")), Policy::Never);
        assert_eq!(Policy::parse(Some("0")), Policy::Never);
        assert_eq!(Policy::parse(Some("bogus")), Policy::Safe);
    }

    #[test]
    fn regenerable_ignored_paths() {
        assert!(regenerable("node_modules/"));
        assert!(regenerable("web/node_modules/"));
        assert!(regenerable("target/"));
        assert!(regenerable("app.log"));
        assert!(!regenerable(".env"));
        assert!(!regenerable("data/local.sqlite"));
    }

    #[test]
    fn claude_runtime_state_is_regenerable() {
        assert!(claude_runtime(".claude/scheduled_tasks.lock"));
        assert!(claude_runtime("web/.claude/mailbox/x.json"));
        assert!(!claude_runtime(".claude/settings.local.json"));
        assert!(!claude_runtime(".claude/worktrees/feature/src/main.rs"));
        assert!(!claude_runtime("not.claude/first-run"));
        let root = std::env::temp_dir().join(format!("paseo-lite-claude-{}", crate::util::new_id()));
        std::fs::create_dir_all(root.join(".claude/checkpoints")).unwrap();
        std::fs::write(root.join(".claude/scheduled_tasks.lock"), "{}").unwrap();
        std::fs::write(root.join(".claude/checkpoints/a"), "x").unwrap();
        assert!(regenerable_tree(&root, ".claude/"));
        std::fs::write(root.join(".claude/settings.local.json"), "{}").unwrap();
        assert!(!regenerable_tree(&root, ".claude/"));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn protected_rejects_relative_missing_and_outside_home() {
        let home = std::env::temp_dir();
        assert!(protected(Path::new("relative/dir"), &home, &[]).is_some());
        assert!(protected(Path::new("/definitely/not/here"), &home, &[]).is_some());
        // The filesystem root and /tmp-level system paths are outside any home.
        assert!(protected(Path::new("/"), &home, &[]).is_some());
    }

    #[tokio::test]
    async fn never_policy_refuses_and_missing_dir_is_ok() {
        let home = std::env::temp_dir();
        let r = delete_project_dir("/tmp".into(), true, Policy::Never, home.clone(), vec![]).await;
        assert!(r.is_err());
        let gone = format!("/tmp/paseo-lite-missing-{}", crate::util::new_id());
        assert_eq!(delete_project_dir(gone, true, Policy::Safe, home, vec![]).await, Ok(false));
    }
}
