//! Filesystem and GitHub helpers behind the app's Add Project flow
//! (`project.create_directory`, `workspace.github.search_repositories`,
//! `project.github.clone`). Error codes and validation follow the Node daemon.

use serde_json::{json, Value};
use std::path::{Component, Path, PathBuf};
use std::time::Duration;
use tokio::process::Command;

fn expand_home(path: &str) -> String {
    match (path.strip_prefix('~'), std::env::var("HOME")) {
        (Some(rest), Ok(home)) if rest.is_empty() || rest.starts_with('/') => format!("{home}{rest}"),
        _ => path.to_string(),
    }
}

/// Lexical absolute path (no symlink resolution), like Node's `path.resolve`.
fn resolve(path: &str) -> PathBuf {
    let p = Path::new(path);
    let base = if p.is_absolute() { PathBuf::new() } else { std::env::current_dir().unwrap_or_default() };
    let mut out = PathBuf::new();
    for c in base.join(p).components() {
        match c {
            Component::ParentDir => {
                out.pop();
            }
            Component::CurDir => {}
            other => out.push(other.as_os_str()),
        }
    }
    out
}

fn validate_name(name: &str) -> Result<(), String> {
    if name.trim().is_empty() {
        return Err("Directory name cannot be empty".into());
    }
    if name != name.trim() {
        return Err("Directory name cannot start or end with whitespace".into());
    }
    if name == "." || name == ".." {
        return Err("Directory name cannot be . or ..".into());
    }
    if name.contains('/') || name.contains('\\') || name.contains('\0') {
        return Err("Directory name must be a single name without path separators".into());
    }
    Ok(())
}

/// Create `<parent>/<name>`. Errors are `(errorCode, message)`.
pub fn create_directory(parent: &str, name: &str) -> Result<String, (String, String)> {
    validate_name(name).map_err(|e| ("invalid_name".to_string(), e))?;
    let parent = parent.trim();
    if parent.is_empty() {
        return Err(("parent_directory_not_found".into(), "Parent directory is required".into()));
    }
    let parent_dir = resolve(&expand_home(parent));
    match std::fs::metadata(&parent_dir) {
        Ok(m) if m.is_dir() => {}
        Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
            return Err(("permission_denied".into(), format!("Permission denied reading parent directory: {}", parent_dir.display())));
        }
        _ => return Err(("parent_directory_not_found".into(), format!("Parent directory not found: {}", parent_dir.display()))),
    }
    let dir = parent_dir.join(name);
    match std::fs::create_dir(&dir) {
        Ok(()) => Ok(dir.to_string_lossy().into_owned()),
        Err(e) => {
            let (code, what) = match e.kind() {
                std::io::ErrorKind::AlreadyExists => ("directory_exists", "Directory already exists"),
                std::io::ErrorKind::PermissionDenied => ("permission_denied", "Permission denied creating directory"),
                std::io::ErrorKind::NotFound => ("parent_directory_not_found", "Parent directory not found"),
                _ => ("filesystem_error", "Failed to create directory"),
            };
            Err((code.into(), format!("{what}: {} ({e})", dir.display())))
        }
    }
}

pub(crate) async fn run(program: &str, args: &[&str], cwd: Option<&Path>, timeout: Duration) -> Result<String, String> {
    let mut cmd = Command::new(program);
    cmd.args(args).kill_on_drop(true).stdin(std::process::Stdio::null());
    if let Some(dir) = cwd {
        cmd.current_dir(dir);
    }
    let output = tokio::time::timeout(timeout, cmd.output())
        .await
        .map_err(|_| format!("{program} timed out"))?
        .map_err(|e| format!("failed to run {program}: {e}"))?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    } else {
        let err = String::from_utf8_lossy(&output.stderr).trim().to_string();
        Err(if err.is_empty() { format!("{program} exited with {}", output.status) } else { crate::util::truncate(&err, 600) })
    }
}

fn repo_summary(r: &Value, name_with_owner: &str, clone_url: String) -> Option<Value> {
    if !name_with_owner.contains('/') {
        return None;
    }
    let id = match r.get("id") {
        Some(Value::String(s)) => s.clone(),
        Some(other) if !other.is_null() => other.to_string(),
        _ => name_with_owner.to_string(),
    };
    let private = r.get("isPrivate").and_then(|v| v.as_bool()).unwrap_or(false);
    Some(json!({
        "id": id,
        "name": r.get("name").and_then(|v| v.as_str()).unwrap_or(name_with_owner),
        "nameWithOwner": name_with_owner,
        "description": r.get("description").and_then(|v| v.as_str()),
        "visibility": if private { "private" } else { "public" },
        "updatedAt": r.get("updatedAt").and_then(|v| v.as_str()).unwrap_or(""),
        "cloneUrl": clone_url,
    }))
}

/// `workspace.github.search_repositories.response` payload, via the `gh` CLI.
pub async fn search_repositories(query: &str, limit: u64, request_id: &str) -> Value {
    let fail = |status: &str, error: String| {
        let mut p = json!({ "status": status, "requestId": request_id, "repositories": [], "available": status == "error", "error": error });
        if status == "unavailable" {
            p["reason"] = json!("gh_missing");
        }
        p
    };
    if !crate::providers::binary_available("gh") {
        return fail("unavailable", "GitHub CLI (gh) is not installed on this host".into());
    }
    let limit = limit.to_string();
    let query = query.trim();
    let home = std::env::var("HOME").ok().map(PathBuf::from);
    let result = if query.is_empty() {
        run("gh", &["repo", "list", "--json", "id,name,nameWithOwner,description,isPrivate,updatedAt,url", "--limit", &limit], home.as_deref(), Duration::from_secs(30)).await
    } else {
        run("gh", &["search", "repos", query, "--json", "id,name,fullName,description,isPrivate,updatedAt,url", "--sort", "updated", "--order", "desc", "--limit", &limit], home.as_deref(), Duration::from_secs(30)).await
    };
    let stdout = match result {
        Ok(s) => s,
        Err(e) => {
            let lower = e.to_ascii_lowercase();
            let status = if lower.contains("auth login") || lower.contains("not logged") || lower.contains("gh_token") { "unauthenticated" } else { "error" };
            return fail(status, e);
        }
    };
    let list: Vec<Value> = serde_json::from_str(if stdout.trim().is_empty() { "[]" } else { stdout.trim() }).unwrap_or_default();
    let repositories: Vec<Value> = list
        .iter()
        .filter_map(|r| {
            let nwo = r.get("nameWithOwner").or_else(|| r.get("fullName")).and_then(|v| v.as_str())?.trim().to_string();
            let url = r.get("url").and_then(|v| v.as_str()).map(String::from).unwrap_or_else(|| format!("https://github.com/{nwo}"));
            repo_summary(r, &nwo, url)
        })
        .collect();
    json!({ "status": "success", "requestId": request_id, "repositories": repositories, "available": true, "error": null })
}

fn valid_segment(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
}

/// `(checkout directory name, display name, clone url)`.
fn normalize_repo(repo: &str, protocol: &str) -> Result<(String, String, String), String> {
    let trimmed = repo.trim();
    if trimmed.is_empty() {
        return Err("Repository is required".into());
    }
    // A remote URL (https://host/owner/repo(.git) or git@host:owner/repo(.git)).
    let remote_path = if let Some(rest) = trimmed.strip_prefix("https://").or_else(|| trimmed.strip_prefix("http://")).or_else(|| trimmed.strip_prefix("ssh://")) {
        rest.split_once('/').map(|(_, p)| p.to_string())
    } else if trimmed.starts_with("git@") {
        trimmed.split_once(':').map(|(_, p)| p.to_string())
    } else {
        None
    };
    if let Some(path) = remote_path {
        let path = path.trim_end_matches('/').trim_end_matches(".git").to_string();
        let name = path.rsplit('/').next().unwrap_or("").to_string();
        if !valid_segment(&name) {
            return Err("Repository name contains invalid characters".into());
        }
        return Ok((name, path, trimmed.to_string()));
    }
    let parts: Vec<&str> = trimmed.split('/').collect();
    if parts.len() != 2 {
        return Err("Repository must use owner/repo format or a git remote URL".into());
    }
    let owner = parts[0];
    let name = parts[1].trim_end_matches(".git");
    if !valid_segment(owner) || !valid_segment(name) {
        return Err("Repository contains invalid characters".into());
    }
    let url = if protocol == "ssh" { format!("git@github.com:{owner}/{name}.git") } else { format!("https://github.com/{owner}/{name}.git") };
    Ok((name.to_string(), format!("{owner}/{name}"), url))
}

/// Is `dir` the top of a git checkout whose `origin` is the repository
/// `display` (`owner/repo`, compared case-insensitively)?
async fn same_repository(dir: &Path, display: &str, protocol: &str) -> bool {
    let Ok(top) = run("git", &["rev-parse", "--show-toplevel"], Some(dir), Duration::from_secs(10)).await else { return false };
    if std::fs::canonicalize(top.trim()).ok() != std::fs::canonicalize(dir).ok() {
        return false;
    }
    let Ok(origin) = run("git", &["remote", "get-url", "origin"], Some(dir), Duration::from_secs(10)).await else { return false };
    match normalize_repo(origin.trim(), protocol) {
        Ok((_, existing, _)) => existing.eq_ignore_ascii_case(display),
        Err(_) => false,
    }
}

/// Clone into `<target>/<repo name>` via a staging dir, so a failed clone
/// never leaves a half-populated checkout behind. Uses `git` (which picks up
/// `gh` as credential helper for private repos).
/// Ok: `(display name, checkout path)`. Err: `(display name, checkout path if known, message)`.
pub async fn clone_repository(repo: &str, protocol: &str, target: &str) -> Result<(String, String), (String, Option<String>, String)> {
    let display_fallback = if repo.trim().len() >= 3 { repo.trim().to_string() } else { "unknown".to_string() };
    let (name, display, url) = normalize_repo(repo, protocol).map_err(|e| (display_fallback.clone(), None, e))?;
    if target.trim().is_empty() {
        return Err((display, None, "Target directory is required".into()));
    }
    let parent = resolve(&expand_home(target.trim()));
    let checkout = parent.join(&name);
    let checkout_str = checkout.to_string_lossy().into_owned();
    let fail = |e: String| (display.clone(), Some(checkout_str.clone()), e);
    std::fs::create_dir_all(&parent).map_err(|e| fail(format!("Cannot create {}: {e}", parent.display())))?;
    if std::fs::symlink_metadata(&checkout).is_ok() {
        // Cloning again what is already there (a removed project whose files
        // were kept): reuse that checkout instead of failing.
        if same_repository(&checkout, &display, protocol).await {
            crate::log!("github clone of {display}: reusing the existing checkout {checkout_str}");
            return Ok((display, checkout_str));
        }
        return Err(fail(format!("Checkout path already exists: {checkout_str}")));
    }
    let staging = parent.join(format!(".paseo-clone-{}", crate::util::new_id()));
    let staging_str = staging.to_string_lossy().into_owned();
    let cloned = run("git", &["clone", "--", &url, &staging_str], Some(&parent), Duration::from_secs(300)).await;
    if let Err(e) = cloned {
        let _ = std::fs::remove_dir_all(&staging);
        return Err(fail(e));
    }
    if let Err(e) = std::fs::rename(&staging, &checkout) {
        let _ = std::fs::remove_dir_all(&staging);
        return Err(fail(format!("Failed to move clone into place: {e}")));
    }
    Ok((display, checkout_str))
}
