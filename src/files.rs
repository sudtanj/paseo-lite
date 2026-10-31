//! Workspace files for the app's file explorer and file pane, following
//! upstream's `file-explorer/service.ts` and `session/files/workspace-files-session.ts`:
//!
//! - `file_explorer_request`: list a directory (newest first, dangling links
//!   skipped) or read a file (text inline, images base64, binaries as
//!   metadata only). paseo-lite has no outbound binary channel, so
//!   `acceptBinary` gets the inline JSON fallback, as upstream does for such clients;
//! - `file_download_token_request`, redeemed once within 60 s at
//!   `GET /api/files/download?token=…` on the direct HTTP listener (see server.rs);
//! - `fs.file.write.request`: revision-checked atomic save of a text file;
//! - `fs.entry.create` / `fs.entry.rename` (`git mv` for tracked files) / `fs.entry.delete`.
//!
//! Every path is resolved against the request's `cwd` and has to stay inside
//! it, both lexically (`..`, absolute paths) and after resolving symlinks.
//! Files are opened with `O_NOFOLLOW`. All filesystem work runs on tokio's
//! blocking pool, and inline content is capped so a big file can't balloon memory.

use crate::manager::{reply, Out};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime};

/// Inline text above this is refused (upstream's editable-file limit).
const MAX_TEXT_BYTES: u64 = 1024 * 1024;
/// Inline images above this are refused: base64, JSON and relay encryption each copy them.
const MAX_IMAGE_BYTES: u64 = 2 * 1024 * 1024;
/// Upstream's `MAX_EDITABLE_FILE_BYTES`.
const MAX_EDITABLE_BYTES: u64 = 1024 * 1024;
const SAMPLE_BYTES: usize = 8192;
/// Upstream's default `downloadTokenTtlMs`.
const DOWNLOAD_TTL: Duration = Duration::from_secs(60);
const OUTSIDE: &str = "Access outside of workspace is not allowed";
const TOO_LARGE: &str = "File is too large to display";

/// Handles the file request types (work runs in the background). Returns
/// false when `kind` isn't one of them.
pub fn handle(kind: &str, msg: &Value, rid: &str, out: &Out) -> bool {
    let response = match kind {
        "file_explorer_request" => "file_explorer_response",
        "file_download_token_request" => "file_download_token_response",
        "fs.file.write.request" => "fs.file.write.response",
        "fs.entry.create.request" => "fs.entry.create.response",
        "fs.entry.rename.request" => "fs.entry.rename.response",
        "fs.entry.delete.request" => "fs.entry.delete.response",
        _ => return false,
    };
    let (kind, msg, rid, out) = (kind.to_string(), msg.clone(), rid.to_string(), out.clone());
    tokio::spawn(async move {
        let job = {
            let rid = rid.clone();
            tokio::task::spawn_blocking(move || payload(&kind, &msg, &rid))
        };
        let payload = job.await.unwrap_or_else(|e| json!({ "requestId": rid, "error": format!("file request failed: {e}") }));
        reply(&out, json!({ "type": response, "payload": payload }));
    });
    true
}

fn payload(kind: &str, msg: &Value, rid: &str) -> Value {
    let s = |k: &str| msg.get(k).and_then(|v| v.as_str()).unwrap_or("").to_string();
    let (cwd, path) = (s("cwd"), s("path"));
    match kind {
        "file_explorer_request" => explore(msg, rid),
        "file_download_token_request" => download_token(&cwd, &path, rid),
        "fs.file.write.request" => {
            let result = write_file(&cwd, &path, &s("content"), &s("expectedModifiedAt"), msg.get("expectedRevision").and_then(|v| v.as_str()));
            json!({ "result": result, "requestId": rid })
        }
        "fs.entry.create.request" => {
            let parent = s("parentPath");
            let (path, success, error) = outcome(create_entry(&cwd, &parent, &s("name"), &s("kind")));
            json!({ "cwd": cwd, "parentPath": parent, "path": path, "success": success, "error": error, "requestId": rid })
        }
        "fs.entry.rename.request" => {
            let (renamed, success, error) = outcome(rename_entry(&cwd, &path, &s("name")));
            json!({ "cwd": cwd, "path": path, "renamedPath": renamed, "success": success, "error": error, "requestId": rid })
        }
        _ => {
            let (_, success, error) = outcome(delete_entry(&cwd, &path));
            json!({ "cwd": cwd, "path": path, "success": success, "error": error, "requestId": rid })
        }
    }
}

fn outcome(result: Result<String, String>) -> (Option<String>, bool, Option<String>) {
    match result {
        Ok(path) => (Some(path), true, None),
        Err(error) => (None, false, Some(error)),
    }
}

// ---------------------------------------------------------------------------
// Path scoping

fn expand_home(path: &str) -> String {
    match (path.strip_prefix('~'), std::env::var("HOME")) {
        (Some(rest), Ok(home)) if rest.is_empty() || rest.starts_with('/') => format!("{home}{rest}"),
        _ => path.to_string(),
    }
}

/// `base.join(path)` with `.` and `..` folded away, no symlink resolution
/// (Node's `path.resolve`). An absolute `path` replaces `base`.
fn normalize(base: &Path, path: &str) -> PathBuf {
    let mut out = PathBuf::from("/");
    for c in base.join(path).components() {
        match c {
            Component::ParentDir => {
                out.pop();
            }
            Component::Normal(name) => out.push(name),
            _ => {}
        }
    }
    out
}

/// The workspace root as given (`lexical`) and with symlinks resolved (`canon`).
struct Root {
    lexical: PathBuf,
    canon: PathBuf,
}

fn root(cwd: &str) -> Result<Root, String> {
    let cwd = cwd.trim();
    if cwd.is_empty() {
        return Err("cwd is required".into());
    }
    let base = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("/"));
    let lexical = normalize(&base, &expand_home(cwd));
    let canon = std::fs::canonicalize(&lexical).map_err(|e| format!("{}: {e}", lexical.display()))?;
    Ok(Root { lexical, canon })
}

struct Scoped {
    /// Root-relative path ("." for the root), as the protocol reports it.
    rel: String,
    /// Root joined with the request, symlinks unresolved (upstream's `requestedPath`).
    requested: PathBuf,
    /// Symlinks resolved, checked to be inside the root (upstream's `resolvedPath`).
    real: PathBuf,
}

fn scope(root: &Root, rel: &str) -> Result<Scoped, String> {
    let rel = rel.trim();
    let requested = normalize(&root.lexical, &expand_home(if rel.is_empty() { "." } else { rel }));
    let inside = requested.strip_prefix(&root.lexical).map_err(|_| OUTSIDE.to_string())?;
    let rel = if inside.as_os_str().is_empty() { ".".to_string() } else { inside.to_string_lossy().into_owned() };
    let real = resolve_inside(&requested, &root.canon)?;
    Ok(Scoped { rel, requested, real })
}

/// Canonical form of `path`, which has to lie under `canon_root`. A path that
/// doesn't exist (yet) resolves through its nearest existing ancestor, so a
/// missing name behind a symlink that leaves the root is refused too.
fn resolve_inside(path: &Path, canon_root: &Path) -> Result<PathBuf, String> {
    let mut existing = path;
    let mut missing: Vec<&std::ffi::OsStr> = Vec::new();
    loop {
        match std::fs::canonicalize(existing) {
            Ok(mut real) => {
                if !real.starts_with(canon_root) {
                    return Err(OUTSIDE.into());
                }
                for name in missing.iter().rev() {
                    real.push(name);
                }
                return Ok(real);
            }
            Err(e) if e.kind() == ErrorKind::NotFound || e.raw_os_error() == Some(libc::ENOTDIR) => match (existing.parent(), existing.file_name()) {
                (Some(parent), Some(name)) => {
                    missing.push(name);
                    existing = parent;
                }
                _ => return Err(e.to_string()),
            },
            Err(e) => return Err(e.to_string()),
        }
    }
}

fn open_read(path: &Path) -> std::io::Result<File> {
    OpenOptions::new().read(true).custom_flags(libc::O_NOFOLLOW).open(path)
}

// ---------------------------------------------------------------------------
// Metadata helpers

/// `Date.toISOString()`: UTC, milliseconds.
fn iso(t: SystemTime) -> String {
    humantime::format_rfc3339_millis(t.max(SystemTime::UNIX_EPOCH)).to_string()
}

fn modified(meta: &std::fs::Metadata) -> SystemTime {
    meta.modified().unwrap_or(SystemTime::UNIX_EPOCH)
}

/// Upstream's `fileRevision`: `dev:ino:size:mtimeNs`.
fn revision(meta: &std::fs::Metadata) -> String {
    let mtime_ns = meta.mtime() as i128 * 1_000_000_000 + meta.mtime_nsec() as i128;
    format!("{}:{}:{}:{}", meta.dev(), meta.ino(), meta.size(), mtime_ns)
}

fn extension(path: &Path) -> String {
    path.extension().and_then(|e| e.to_str()).unwrap_or("").to_ascii_lowercase()
}

fn image_mime(ext: &str) -> Option<&'static str> {
    Some(match ext {
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "svg" => "image/svg+xml",
        _ => return None,
    })
}

fn text_mime(ext: &str) -> &'static str {
    if ext == "json" {
        "application/json"
    } else {
        "text/plain"
    }
}

/// Upstream's `isLikelyBinary`: any NUL, or more than 30% control bytes.
fn likely_binary(bytes: &[u8]) -> bool {
    if bytes.contains(&0) {
        return true;
    }
    let suspicious = bytes.iter().filter(|&&b| (b < 32 && b != b'\t' && b != b'\n' && b != b'\r') || b == 127).count();
    !bytes.is_empty() && suspicious * 10 > bytes.len() * 3
}

/// Reads up to `buf.len()` bytes (fewer only at end of file).
fn read_fill(file: &mut File, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        match file.read(&mut buf[filled..]) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(e) if e.kind() == ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(filled)
}

/// Binary verdict on the first bytes of a file that may continue past them:
/// a multi-byte character cut off at the end of the sample is fine.
fn sample_is_binary(sample: &[u8]) -> bool {
    likely_binary(sample) || std::str::from_utf8(sample).err().is_some_and(|e| e.error_len().is_some())
}

// ---------------------------------------------------------------------------
// file_explorer_request

/// Full `file_explorer_response` payload.
pub fn explore(msg: &Value, request_id: &str) -> Value {
    let s = |k: &str| msg.get(k).and_then(|v| v.as_str()).unwrap_or("").to_string();
    let (cwd, path) = (s("cwd"), s("path"));
    let mode = if s("mode") == "file" { "file" } else { "list" };
    let requested = if path.is_empty() { ".".to_string() } else { path.clone() };
    let mut payload = json!({ "cwd": cwd.trim(), "path": requested, "mode": mode, "directory": null, "file": null, "error": null, "requestId": request_id });
    if cwd.trim().is_empty() {
        payload["cwd"] = json!(cwd);
    }
    let max_bytes = msg.get("maxBytes").and_then(|v| v.as_u64()).filter(|&n| n > 0);
    let result = root(&cwd).and_then(|root| if mode == "list" { list(&root, &path).map(|d| ("directory", d)) } else { read(&root, &path, max_bytes).map(|f| ("file", f)) });
    match result {
        Ok((key, value)) => {
            payload["path"] = value["path"].clone();
            payload[key] = value;
        }
        Err(e) => payload["error"] = json!(e),
    }
    payload
}

fn list(root: &Root, rel: &str) -> Result<Value, String> {
    let dir = scope(root, rel)?;
    let meta = std::fs::metadata(&dir.real).map_err(|e| e.to_string())?;
    if !meta.is_dir() {
        return Err("Requested path is not a directory".into());
    }
    let mut entries: Vec<(u128, String, Value)> = Vec::new();
    for dirent in std::fs::read_dir(&dir.real).map_err(|e| e.to_string())?.flatten() {
        let Ok(file_type) = dirent.file_type() else { continue };
        // Only a symlink can lead elsewhere: follow it, keep it inside the root,
        // and skip it when dangling (upstream does the same).
        let meta = if file_type.is_symlink() {
            match resolve_inside(&dirent.path(), &root.canon).and_then(|real| std::fs::metadata(real).map_err(|e| e.to_string())) {
                Ok(m) => m,
                Err(_) => continue,
            }
        } else {
            match dirent.metadata() {
                Ok(m) => m,
                Err(_) => continue,
            }
        };
        let name = dirent.file_name().to_string_lossy().into_owned();
        let path = if dir.rel == "." { name.clone() } else { format!("{}/{}", dir.rel, name) };
        let mtime = modified(&meta);
        let millis = mtime.duration_since(SystemTime::UNIX_EPOCH).map(|d| d.as_millis()).unwrap_or(0);
        let entry = json!({ "name": name, "path": path, "kind": if meta.is_dir() { "directory" } else { "file" }, "size": meta.len(), "modifiedAt": iso(mtime) });
        entries.push((millis, name, entry));
    }
    entries.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
    Ok(json!({ "path": dir.rel, "entries": entries.into_iter().map(|e| e.2).collect::<Vec<_>>() }))
}

fn read(root: &Root, rel: &str, max_bytes: Option<u64>) -> Result<Value, String> {
    let scoped = scope(root, rel)?;
    let mut file = open_read(&scoped.real).map_err(|e| e.to_string())?;
    let meta = file.metadata().map_err(|e| e.to_string())?;
    if !meta.is_file() {
        return Err("Requested path is not a file".into());
    }
    let size = meta.len();
    if max_bytes.is_some_and(|max| size > max) {
        return Err(TOO_LARGE.into());
    }
    let ext = extension(&scoped.real);
    let mut out = json!({ "path": scoped.rel, "size": size, "modifiedAt": iso(modified(&meta)), "revision": revision(&meta) });
    let mut set = |kind: &str, encoding: &str, mime: &str, content: Option<String>| {
        out["kind"] = json!(kind);
        out["encoding"] = json!(encoding);
        out["mimeType"] = json!(mime);
        if let Some(content) = content {
            out["content"] = Value::String(content);
        }
    };

    if let Some(mime) = image_mime(&ext) {
        if size > MAX_IMAGE_BYTES {
            return Err(TOO_LARGE.into());
        }
        let mut bytes = Vec::with_capacity(size as usize);
        (&mut file).take(MAX_IMAGE_BYTES + 1).read_to_end(&mut bytes).map_err(|e| e.to_string())?;
        if bytes.len() as u64 > MAX_IMAGE_BYTES {
            return Err(TOO_LARGE.into());
        }
        use base64::Engine;
        set("image", "base64", mime, Some(base64::engine::general_purpose::STANDARD.encode(&bytes)));
        return Ok(out);
    }

    // Sniff a sample first: a large binary then costs no more than the sample.
    let mut bytes = vec![0u8; SAMPLE_BYTES.min(size as usize)];
    let n = read_fill(&mut file, &mut bytes).map_err(|e| e.to_string())?;
    bytes.truncate(n);
    if sample_is_binary(&bytes) {
        set("binary", "none", "application/octet-stream", None);
        return Ok(out);
    }
    if size > MAX_TEXT_BYTES {
        return Err(TOO_LARGE.into());
    }
    (&mut file).take(MAX_TEXT_BYTES + 1 - bytes.len() as u64).read_to_end(&mut bytes).map_err(|e| e.to_string())?;
    if bytes.len() as u64 > MAX_TEXT_BYTES {
        return Err(TOO_LARGE.into());
    }
    if likely_binary(&bytes) {
        set("binary", "none", "application/octet-stream", None);
        return Ok(out);
    }
    match String::from_utf8(bytes) {
        Ok(text) => set("text", "utf-8", text_mime(&ext), Some(text)),
        Err(_) => set("binary", "none", "application/octet-stream", None),
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Downloads

pub struct Download {
    pub path: PathBuf,
    pub file_name: String,
    pub mime_type: String,
    expires: Instant,
}

fn downloads() -> &'static Mutex<HashMap<String, Download>> {
    static TOKENS: OnceLock<Mutex<HashMap<String, Download>>> = OnceLock::new();
    TOKENS.get_or_init(Default::default)
}

/// Redeems a download token (single use). None when unknown or expired.
pub fn take_download(token: &str) -> Option<Download> {
    let entry = downloads().lock().ok()?.remove(token)?;
    (entry.expires > Instant::now()).then_some(entry)
}

/// Upstream's `getDownloadableFileInfo`: (path, absolute path, file name, mime type, size).
fn downloadable(cwd: &str, rel: &str) -> Result<(String, PathBuf, String, String, u64), String> {
    let root = root(cwd)?;
    let scoped = scope(&root, rel)?;
    let mut file = open_read(&scoped.real).map_err(|e| e.to_string())?;
    let meta = file.metadata().map_err(|e| e.to_string())?;
    if !meta.is_file() {
        return Err("Requested path is not a file".into());
    }
    let ext = extension(&scoped.real);
    let mime = match image_mime(&ext) {
        Some(mime) => mime,
        None => {
            let mut sample = vec![0u8; SAMPLE_BYTES];
            let n = read_fill(&mut file, &mut sample).map_err(|e| e.to_string())?;
            if likely_binary(&sample[..n]) {
                "application/octet-stream"
            } else {
                text_mime(&ext)
            }
        }
    };
    let file_name = scoped.requested.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
    Ok((scoped.rel, scoped.real, file_name, mime.to_string(), meta.len()))
}

fn download_token(cwd: &str, rel: &str, rid: &str) -> Value {
    let reported_cwd = if cwd.trim().is_empty() { cwd } else { cwd.trim() };
    match downloadable(cwd, rel) {
        Ok((path, real, file_name, mime_type, size)) => {
            let token = uuid::Uuid::new_v4().to_string();
            if let Ok(mut tokens) = downloads().lock() {
                let now = Instant::now();
                tokens.retain(|_, t| t.expires > now);
                tokens.insert(token.clone(), Download { path: real, file_name: file_name.clone(), mime_type: mime_type.clone(), expires: now + DOWNLOAD_TTL });
            }
            json!({ "cwd": reported_cwd, "path": path, "token": token, "fileName": file_name, "mimeType": mime_type, "size": size, "error": null, "requestId": rid })
        }
        Err(error) => json!({ "cwd": reported_cwd, "path": rel, "token": null, "fileName": null, "mimeType": null, "size": null, "error": error, "requestId": rid }),
    }
}

/// Upstream's `formatAttachmentContentDisposition` (RFC 6266): an ASCII
/// `filename`, plus `filename*=UTF-8''…` for non-ASCII names or names that
/// contain a percent escape.
pub fn content_disposition(file_name: &str) -> String {
    let ascii: String = file_name.chars().map(|c| if (' '..='~').contains(&c) { c } else { '?' }).collect();
    let quoted = ascii.replace('\\', "\\\\").replace('"', "\\\"");
    let disposition = format!("attachment; filename=\"{quoted}\"");
    let has_escape = file_name.as_bytes().windows(3).any(|w| w[0] == b'%' && w[1].is_ascii_hexdigit() && w[2].is_ascii_hexdigit());
    if ascii == file_name && !has_escape {
        return disposition;
    }
    let mut encoded = String::new();
    for &b in file_name.as_bytes() {
        if b.is_ascii_alphanumeric() || b"-_.!~".contains(&b) {
            encoded.push(b as char);
        } else {
            encoded.push_str(&format!("%{b:02X}"));
        }
    }
    format!("{disposition}; filename*=UTF-8''{encoded}")
}

// ---------------------------------------------------------------------------
// fs.file.write.request

fn version_ready(cwd: &str, rel: &str, meta: &std::fs::Metadata) -> Value {
    json!({ "status": "ready", "cwd": cwd, "path": rel, "size": meta.len(), "modifiedAt": iso(modified(meta)), "revision": revision(meta) })
}

fn matches_expected(meta: &std::fs::Metadata, expected_modified_at: &str, expected_revision: Option<&str>) -> bool {
    match expected_revision.filter(|r| !r.is_empty()) {
        Some(expected) => revision(meta) == expected,
        None => iso(modified(meta)) == expected_modified_at,
    }
}

/// Upstream's `writeExplorerFile`: `written`, `conflict` (with the file's
/// current version) or `error`. Only existing UTF-8 text files up to 1 MiB.
fn write_file(cwd: &str, rel: &str, content: &str, expected_modified_at: &str, expected_revision: Option<&str>) -> Value {
    let error = |e: String| json!({ "status": "error", "error": e });
    if content.len() as u64 > MAX_EDITABLE_BYTES {
        return error("File is too large to edit".into());
    }
    let root = match root(cwd) {
        Ok(r) => r,
        Err(e) => return error(e),
    };
    let expanded_cwd = root.lexical.to_string_lossy().into_owned();
    let scoped = match scope(&root, rel) {
        Ok(s) => s,
        Err(e) => return error(e),
    };
    let mut file = match open_read(&scoped.real) {
        Ok(f) => f,
        Err(e) if e.kind() == ErrorKind::NotFound => return json!({ "status": "conflict", "version": { "status": "missing", "cwd": expanded_cwd, "path": rel } }),
        Err(e) => return error(e.to_string()),
    };
    let meta = match file.metadata() {
        Ok(m) => m,
        Err(e) => return error(e.to_string()),
    };
    if !meta.is_file() {
        return error("Requested path is not a file".into());
    }
    if meta.len() > MAX_EDITABLE_BYTES {
        return error("File is too large to edit".into());
    }
    let mut current = Vec::with_capacity(meta.len() as usize);
    if let Err(e) = (&mut file).take(MAX_EDITABLE_BYTES + 1).read_to_end(&mut current) {
        return error(e.to_string());
    }
    if likely_binary(&current) || std::str::from_utf8(&current).is_err() {
        return error("Binary files cannot be edited".into());
    }
    drop(current);
    drop(file);
    if !matches_expected(&meta, expected_modified_at, expected_revision) {
        return json!({ "status": "conflict", "version": version_ready(&expanded_cwd, &scoped.rel, &meta) });
    }

    let mode = meta.mode() & 0o7777;
    let parent = scoped.real.parent().unwrap_or(Path::new("/"));
    let temporary = parent.join(format!(".paseo-{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| -> std::io::Result<Value> {
        let mut tmp = OpenOptions::new().write(true).create_new(true).mode(mode).open(&temporary)?;
        tmp.set_permissions(std::fs::Permissions::from_mode(mode))?;
        tmp.write_all(content.as_bytes())?;
        tmp.sync_all()?;
        drop(tmp);
        let latest = std::fs::metadata(&scoped.real)?;
        if !matches_expected(&latest, expected_modified_at, expected_revision) {
            return Ok(json!({ "status": "conflict", "version": version_ready(&expanded_cwd, &scoped.rel, &latest) }));
        }
        std::fs::rename(&temporary, &scoped.real)?;
        let written = std::fs::metadata(&scoped.real)?;
        Ok(json!({ "status": "written", "modifiedAt": iso(modified(&written)), "size": written.len(), "revision": revision(&written) }))
    })();
    let _ = std::fs::remove_file(&temporary);
    result.unwrap_or_else(|e| error(e.to_string()))
}

// ---------------------------------------------------------------------------
// fs.entry.*

fn valid_name(name: &str) -> Result<&str, String> {
    let name = name.trim();
    if name.is_empty() || name == "." || name == ".." {
        return Err("Invalid name".into());
    }
    if name.contains('/') || name.contains('\\') {
        return Err("Name cannot contain path separators".into());
    }
    Ok(name)
}

fn join_rel(parent: &str, name: &str) -> String {
    if parent == "." {
        name.to_string()
    } else {
        format!("{parent}/{name}")
    }
}

fn gone(e: std::io::Error) -> String {
    if e.kind() == ErrorKind::NotFound {
        "File or folder no longer exists".into()
    } else {
        e.to_string()
    }
}

fn create_entry(cwd: &str, parent_path: &str, name: &str, kind: &str) -> Result<String, String> {
    let name = valid_name(name)?;
    let root = root(cwd)?;
    let parent = scope(&root, parent_path)?;
    if !std::fs::metadata(&parent.real).map_err(|e| e.to_string())?.is_dir() {
        return Err("Parent path is not a directory".into());
    }
    let target = parent.real.join(name);
    // mkdir and O_EXCL both refuse an existing name, symlinks included.
    let created = if kind == "directory" { std::fs::create_dir(&target) } else { OpenOptions::new().write(true).create_new(true).mode(0o644).open(&target).map(drop) };
    match created {
        Ok(()) => Ok(join_rel(&parent.rel, name)),
        Err(e) if e.kind() == ErrorKind::AlreadyExists => Err(format!("\"{name}\" already exists")),
        Err(e) => Err(e.to_string()),
    }
}

fn rename_entry(cwd: &str, rel: &str, name: &str) -> Result<String, String> {
    let name = valid_name(name)?;
    let root = root(cwd)?;
    let source = scope(&root, rel)?;
    if source.real == root.canon {
        return Err("Cannot rename the workspace root".into());
    }
    let parent = source.requested.parent().unwrap_or(Path::new("/"));
    let target = parent.join(name);
    let source_meta = std::fs::symlink_metadata(&source.requested).map_err(gone)?;
    if let Ok(existing) = std::fs::symlink_metadata(&target) {
        // Only a case-only rename of the same entry may land on an existing name.
        let same = existing.dev() == source_meta.dev() && existing.ino() == source_meta.ino();
        if !(same && source.requested.to_string_lossy().to_lowercase() == target.to_string_lossy().to_lowercase()) {
            return Err(format!("\"{name}\" already exists"));
        }
    }
    let parent_rel = Path::new(&source.rel).parent().map(|p| p.to_string_lossy().into_owned()).filter(|p| !p.is_empty()).unwrap_or_else(|| ".".into());
    let renamed = join_rel(&parent_rel, name);
    if renamed == source.rel {
        return Ok(renamed);
    }
    if git_tracked(&root.canon, &source.rel) {
        git(&root.canon, &["mv", "--", &source.rel, &renamed])?;
    } else {
        std::fs::rename(&source.requested, &target).map_err(gone)?;
    }
    Ok(renamed)
}

fn git(dir: &Path, args: &[&str]) -> Result<String, String> {
    let out = std::process::Command::new("git")
        .args(args)
        .current_dir(dir)
        .env("GIT_TERMINAL_PROMPT", "0")
        .env_remove("GIT_LITERAL_PATHSPECS")
        .stdin(std::process::Stdio::null())
        .output()
        .map_err(|e| format!("failed to run git: {e}"))?;
    if out.status.success() {
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    } else {
        Err(crate::util::truncate(String::from_utf8_lossy(&out.stderr).trim(), 800))
    }
}

/// Whether `rel` (relative to `dir`) is tracked by git, so a rename keeps the index in step.
fn git_tracked(dir: &Path, rel: &str) -> bool {
    git(dir, &["rev-parse", "--is-inside-work-tree"]).is_ok() && git(dir, &["ls-files", "-z", "--", &format!(":(literal){rel}")]).is_ok_and(|out| !out.is_empty())
}

fn delete_entry(cwd: &str, rel: &str) -> Result<String, String> {
    let root = root(cwd)?;
    let scoped = scope(&root, rel)?;
    if scoped.real == root.canon {
        return Err("Cannot delete the workspace root".into());
    }
    // Remove the requested path, never what a symlink points to.
    // (`remove_dir_all` doesn't follow symlinks inside the tree either.)
    let meta = std::fs::symlink_metadata(&scoped.requested).map_err(gone)?;
    if meta.is_dir() {
        std::fs::remove_dir_all(&scoped.requested).map_err(gone)?;
    } else {
        std::fs::remove_file(&scoped.requested).map_err(gone)?;
    }
    Ok(scoped.rel)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;

    struct Tmp(PathBuf);
    impl Drop for Tmp {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// A temp dir holding `ws/` (the workspace) and `outside/`.
    fn setup() -> (Tmp, String) {
        let dir = std::env::temp_dir().join(format!("paseo-lite-files-{}", crate::util::new_id()));
        std::fs::create_dir_all(dir.join("ws/src")).unwrap();
        std::fs::create_dir_all(dir.join("outside")).unwrap();
        std::fs::write(dir.join("outside/secret.txt"), "secret").unwrap();
        let ws = dir.join("ws").to_string_lossy().into_owned();
        (Tmp(dir), ws)
    }

    fn set_mtime(path: &Path, secs: u64) {
        // Read-only so directories work too; futimens only needs ownership.
        let file = File::open(path).unwrap();
        file.set_modified(SystemTime::UNIX_EPOCH + Duration::from_secs(secs)).unwrap();
    }

    fn explore_req(cwd: &str, path: &str, mode: &str) -> Value {
        explore(&json!({ "type": "file_explorer_request", "cwd": cwd, "path": path, "mode": mode, "requestId": "r1" }), "r1")
    }

    #[test]
    fn normalize_folds_dots_and_absolute_paths() {
        assert_eq!(normalize(Path::new("/a/b"), "c/../d/./e"), PathBuf::from("/a/b/d/e"));
        assert_eq!(normalize(Path::new("/a/b"), "../../.."), PathBuf::from("/"));
        assert_eq!(normalize(Path::new("/a/b"), "/etc/passwd"), PathBuf::from("/etc/passwd"));
    }

    #[test]
    fn scope_rejects_escapes() {
        let (tmp, ws) = setup();
        symlink(tmp.0.join("outside"), tmp.0.join("ws/out")).unwrap();
        symlink(tmp.0.join("ws/src"), tmp.0.join("ws/inner")).unwrap();
        let root = root(&ws).unwrap();
        for bad in ["..", "../outside/secret.txt", "src/../../outside", "/etc/passwd", "out", "out/secret.txt", "out/missing/new.txt"] {
            assert_eq!(scope(&root, bad).err().as_deref(), Some(OUTSIDE), "{bad}");
        }
        assert_eq!(scope(&root, "").unwrap().rel, ".");
        assert_eq!(scope(&root, "./src/../src").unwrap().rel, "src");
        assert_eq!(scope(&root, &format!("{ws}/src")).unwrap().rel, "src");
        let inner = scope(&root, "inner/new.txt").unwrap();
        assert_eq!(inner.rel, "inner/new.txt");
        assert_eq!(inner.real, root.canon.join("src/new.txt"));
    }

    #[test]
    fn list_is_newest_first_and_skips_links_out_or_dangling() {
        let (tmp, ws) = setup();
        let w = tmp.0.join("ws");
        std::fs::write(w.join("a.txt"), "aa").unwrap();
        std::fs::write(w.join("b.txt"), "b").unwrap();
        set_mtime(&w.join("a.txt"), 1_000);
        set_mtime(&w.join("b.txt"), 1_000);
        set_mtime(&w.join("src"), 2_000);
        symlink(tmp.0.join("outside/secret.txt"), w.join("leak")).unwrap();
        symlink(w.join("gone"), w.join("dangling")).unwrap();
        let p = explore_req(&ws, ".", "list");
        assert_eq!(p["error"], Value::Null);
        assert_eq!(p["path"], ".");
        assert_eq!(p["file"], Value::Null);
        let entries = p["directory"]["entries"].as_array().unwrap();
        let names: Vec<&str> = entries.iter().map(|e| e["name"].as_str().unwrap()).collect();
        assert_eq!(names, ["src", "a.txt", "b.txt"]);
        assert_eq!(entries[0]["kind"], "directory");
        assert_eq!(entries[1], json!({ "name": "a.txt", "path": "a.txt", "kind": "file", "size": 2, "modifiedAt": "1970-01-01T00:16:40.000Z" }));

        std::fs::write(w.join("src/main.rs"), "fn main() {}").unwrap();
        let p = explore_req(&ws, "src", "list");
        assert_eq!(p["path"], "src");
        assert_eq!(p["directory"]["entries"][0]["path"], "src/main.rs");

        let p = explore_req(&ws, "../outside", "list");
        assert_eq!(p["error"], OUTSIDE);
        assert_eq!(p["directory"], Value::Null);
        assert_eq!(explore_req("  ", ".", "list")["error"], "cwd is required");
    }

    #[test]
    fn read_text_image_binary() {
        let (tmp, ws) = setup();
        let w = tmp.0.join("ws");
        std::fs::write(w.join("notes.md"), "héllo\n").unwrap();
        std::fs::write(w.join("data.json"), "{}").unwrap();
        std::fs::write(w.join("pic.PNG"), [0x89, b'P', b'N', b'G', 0]).unwrap();
        std::fs::write(w.join("blob.bin"), [1u8, 0, 2, 3]).unwrap();
        std::fs::write(w.join("big.txt"), vec![b'x'; MAX_TEXT_BYTES as usize + 1]).unwrap();

        let f = &explore_req(&ws, "notes.md", "file")["file"];
        assert_eq!((f["kind"].as_str(), f["encoding"].as_str(), f["content"].as_str()), (Some("text"), Some("utf-8"), Some("héllo\n")));
        assert_eq!(f["mimeType"], "text/plain");
        assert_eq!(f["size"], 7);
        assert!(f["revision"].as_str().unwrap().split(':').count() == 4);
        assert_eq!(explore_req(&ws, "data.json", "file")["file"]["mimeType"], "application/json");

        let f = &explore_req(&ws, "pic.PNG", "file")["file"];
        assert_eq!((f["kind"].as_str(), f["encoding"].as_str(), f["mimeType"].as_str()), (Some("image"), Some("base64"), Some("image/png")));
        assert_eq!(f["content"], "iVBORwA=");

        let f = &explore_req(&ws, "blob.bin", "file")["file"];
        assert_eq!((f["kind"].as_str(), f["encoding"].as_str()), (Some("binary"), Some("none")));
        assert!(f.get("content").is_none());

        assert_eq!(explore_req(&ws, "big.txt", "file")["error"], TOO_LARGE);
        let capped = explore(&json!({ "cwd": ws, "path": "notes.md", "mode": "file", "maxBytes": 3 }), "r");
        assert_eq!(capped["error"], TOO_LARGE);
        assert_eq!(explore_req(&ws, "src", "file")["error"], "Requested path is not a file");
        assert_eq!(explore_req(&ws, "../outside/secret.txt", "file")["error"], OUTSIDE);
    }

    #[test]
    fn write_checks_revision_and_saves() {
        let (tmp, ws) = setup();
        let path = tmp.0.join("ws/a.txt");
        std::fs::write(&path, "one").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
        let rev = revision(&std::fs::metadata(&path).unwrap());

        let stale = write_file(&ws, "a.txt", "two", "", Some("0:0:0:0"));
        assert_eq!(stale["status"], "conflict");
        assert_eq!(stale["version"]["revision"], rev.as_str());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "one");

        let ok = write_file(&ws, "a.txt", "two", "", Some(rev.as_str()));
        assert_eq!(ok["status"], "written");
        assert_eq!(ok["size"], 3);
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "two");
        assert_eq!(std::fs::metadata(&path).unwrap().mode() & 0o777, 0o640);
        assert_eq!(std::fs::read_dir(tmp.0.join("ws")).unwrap().count(), 2, "no temp file left behind");

        let missing = write_file(&ws, "nope.txt", "x", "", None);
        assert_eq!(missing["version"]["status"], "missing");
        assert_eq!(write_file(&ws, "../outside/secret.txt", "x", "", None)["error"], OUTSIDE);
        assert_eq!(std::fs::read_to_string(tmp.0.join("outside/secret.txt")).unwrap(), "secret");
    }

    #[test]
    fn create_rename_delete_stay_inside() {
        let (tmp, ws) = setup();
        let w = tmp.0.join("ws");
        assert_eq!(create_entry(&ws, "src", "new.txt", "file").unwrap(), "src/new.txt");
        assert_eq!(create_entry(&ws, ".", "docs", "directory").unwrap(), "docs");
        assert_eq!(create_entry(&ws, "src", "new.txt", "file").unwrap_err(), "\"new.txt\" already exists");
        assert_eq!(create_entry(&ws, ".", "a/b", "file").unwrap_err(), "Name cannot contain path separators");
        assert_eq!(create_entry(&ws, "..", "x", "file").unwrap_err(), OUTSIDE);

        assert_eq!(rename_entry(&ws, "src/new.txt", "renamed.txt").unwrap(), "src/renamed.txt");
        assert!(w.join("src/renamed.txt").is_file());
        assert_eq!(rename_entry(&ws, "src/renamed.txt", "..").unwrap_err(), "Invalid name");
        assert_eq!(rename_entry(&ws, ".", "x").unwrap_err(), "Cannot rename the workspace root");

        symlink(tmp.0.join("outside"), w.join("docs/link")).unwrap();
        assert_eq!(delete_entry(&ws, "docs/link/secret.txt").unwrap_err(), OUTSIDE);
        assert_eq!(delete_entry(&ws, ".").unwrap_err(), "Cannot delete the workspace root");
        assert_eq!(delete_entry(&ws, "docs").unwrap(), "docs");
        assert!(!w.join("docs").exists());
        assert!(tmp.0.join("outside/secret.txt").is_file());
        assert_eq!(delete_entry(&ws, "docs").unwrap_err(), "File or folder no longer exists");
    }

    #[test]
    fn download_tokens_are_single_use() {
        let (tmp, ws) = setup();
        std::fs::write(tmp.0.join("ws/report.csv"), "a,b\n").unwrap();
        let p = download_token(&ws, "report.csv", "r");
        assert_eq!((p["fileName"].as_str(), p["mimeType"].as_str(), p["size"].as_u64()), (Some("report.csv"), Some("text/plain"), Some(4)));
        let token = p["token"].as_str().unwrap();
        assert_eq!(take_download(token).unwrap().path, std::fs::canonicalize(tmp.0.join("ws/report.csv")).unwrap());
        assert!(take_download(token).is_none());
        let bad = download_token(&ws, "../outside/secret.txt", "r");
        assert_eq!((bad["token"].clone(), bad["error"].as_str()), (Value::Null, Some(OUTSIDE)));
    }

    #[test]
    fn content_disposition_matches_upstream() {
        assert_eq!(content_disposition("a.txt"), "attachment; filename=\"a.txt\"");
        assert_eq!(content_disposition("say \"hi\".txt"), "attachment; filename=\"say \\\"hi\\\".txt\"");
        assert_eq!(content_disposition("é (1).txt"), "attachment; filename=\"? (1).txt\"; filename*=UTF-8''%C3%A9%20%281%29.txt");
        assert_eq!(content_disposition("100%41.txt"), "attachment; filename=\"100%41.txt\"; filename*=UTF-8''100%2541.txt");
    }
}
