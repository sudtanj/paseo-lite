//! End-to-end tests: start the real daemon binary on a free port with a
//! throwaway home, then talk to it over HTTP and WebSocket exactly like the
//! app does. A fake `claude` script stands in for the Claude Code CLI.

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use tokio_tungstenite::tungstenite::Message;

struct Daemon {
    child: Child,
    port: u16,
    dir: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn unique_dir(tag: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("paseo-lite-e2e-{tag}-{}-{nanos}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// A stand-in for `claude -p --input-format stream-json`: answers every user
/// message with one assistant message and a successful result.
fn fake_claude(dir: &std::path::Path, mode: &str) -> PathBuf {
    let path = dir.join(format!("fake-claude-{mode}.sh"));
    let script = r#"#!/bin/sh
n=0
while IFS= read -r line; do
  case "$line" in
    *'"type":"user"'*)
      printf '%s\n' '{"type":"assistant","message":{"id":"m1","role":"assistant","content":[{"type":"text","text":"hello from fake claude"}],"usage":{"input_tokens":3,"output_tokens":4}}}'
      n=$((n+1))
      printf '{"type":"result","subtype":"success","is_error":false,"usage":{"input_tokens":3,"output_tokens":4},"total_cost_usd":0.0,"session_id":"fake-session","modelUsage":{"claude-test":{"inputTokens":%d,"outputTokens":%d,"cacheReadInputTokens":0,"cacheCreationInputTokens":0,"costUSD":%d}}}\n' $((100*n)) $((10*n)) $n
      ;;
  esac
done
"#;
    let hanging = "#!/bin/sh\nwhile IFS= read -r line; do\n  case \"$line\" in\n    *'\"type\":\"user\"'*) sleep 600 ;;\n  esac\ndone\n";
    // One tool call (running, then its result), then text: exercises rows updated in place.
    let tool = concat!(
        "#!/bin/sh\nwhile IFS= read -r line; do\n  case \"$line\" in\n    *'\"type\":\"user\"'*)\n",
        "      printf '%s\\n' '{\"type\":\"assistant\",\"message\":{\"id\":\"m1\",\"role\":\"assistant\",\"content\":[{\"type\":\"tool_use\",\"id\":\"toolu_1\",\"name\":\"Bash\",\"input\":{\"command\":\"ls\"}}]}}'\n",
        "      printf '%s\\n' '{\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"tool_result\",\"tool_use_id\":\"toolu_1\",\"content\":\"file.txt\"}]}}'\n",
        "      printf '%s\\n' '{\"type\":\"assistant\",\"message\":{\"id\":\"m2\",\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"done\"}]}}'\n",
        "      printf '%s\\n' '{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false}'\n",
        "      ;;\n  esac\ndone\n"
    );
    // Twenty streamed text deltas in quick succession: exercises delta batching.
    let mut stream = String::from("#!/bin/sh\nwhile IFS= read -r line; do\n  case \"$line\" in\n    *'\"type\":\"user\"'*)\n");
    stream.push_str("      printf '%s\\n' '{\"type\":\"stream_event\",\"event\":{\"type\":\"message_start\",\"message\":{\"id\":\"m1\"}}}'\n");
    for i in 0..20 {
        stream.push_str(&format!("      printf '%s\\n' '{{\"type\":\"stream_event\",\"event\":{{\"type\":\"content_block_delta\",\"index\":0,\"delta\":{{\"type\":\"text_delta\",\"text\":\"w{i} \"}}}}}}'\n"));
    }
    stream.push_str("      printf '%s\\n' '{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false}'\n      ;;\n  esac\ndone\n");
    // Refused by the usage limit the first time (state kept next to the script,
    // since the daemon restarts the CLI), answered normally after that.
    let limit = format!(
        concat!(
            "#!/bin/sh\nwhile IFS= read -r line; do\n  case \"$line\" in\n    *'\"type\":\"user\"'*)\n",
            "      if [ ! -e '{marker}' ]; then\n        : > '{marker}'\n",
            "        printf '%s\\n' '{{\"type\":\"rate_limit_event\",\"rate_limit_info\":{{\"status\":\"rejected\",\"resetsAt\":1,\"rateLimitType\":\"five_hour\"}}}}'\n",
            "        printf '%s\\n' '{{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":true,\"result\":\"Session limit reached\"}}'\n",
            "      else\n",
            "        printf '%s\\n' '{{\"type\":\"assistant\",\"message\":{{\"id\":\"m1\",\"role\":\"assistant\",\"content\":[{{\"type\":\"text\",\"text\":\"back to work\"}}]}}}}'\n",
            "        printf '%s\\n' '{{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false}}'\n",
            "      fi\n      ;;\n  esac\ndone\n"
        ),
        marker = dir.join("limit-hit").display()
    );
    let body = match mode {
        "hang" => hanging.to_string(),
        "tool" => tool.to_string(),
        "stream" => stream,
        "limit" => limit,
        _ => script.to_string(),
    };
    std::fs::write(&path, body).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    path
}

fn start(tag: &str, password: Option<&str>) -> Daemon {
    start_at(unique_dir(tag), password, "ok")
}

/// Start (or restart) the daemon on an existing directory. `hang`: the fake
/// claude accepts a prompt and never answers, to simulate a turn in flight.
fn start_at(dir: PathBuf, password: Option<&str>, mode: &str) -> Daemon {
    let port = free_port();
    let claude = fake_claude(&dir, mode);
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_paseo-lite"));
    cmd.args(["--listen", &format!("127.0.0.1:{port}"), "--no-relay"])
        .env("PASEO_LITE_HOME", dir.join("home"))
        .env("PASEO_CLAUDE_BIN", &claude)
        .env("PASEO_CODEX_BIN", "/nonexistent/codex")
        .env("PASEO_DEFAULT_SYSTEM_PROMPT", "")
        .env("PASEO_RETRY_MAX", "0")
        .env("PASEO_LIMIT_POLL_SECS", "1")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let Some(p) = password {
        cmd.env("PASEO_PASSWORD", p);
    }
    let child = cmd.spawn().expect("start paseo-lite");
    let daemon = Daemon { child, port, dir };
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return daemon;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("daemon did not start listening on {port}");
}

type Ws = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn connect(port: u16) -> Ws {
    let (ws, _) = tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/ws")).await.expect("ws connect");
    ws
}

async fn send(ws: &mut Ws, v: Value) {
    ws.send(Message::Text(v.to_string())).await.unwrap();
}

async fn recv(ws: &mut Ws) -> Value {
    loop {
        match tokio::time::timeout(Duration::from_secs(20), ws.next()).await.expect("timed out waiting for a frame") {
            Some(Ok(Message::Text(t))) => return serde_json::from_str(&t).unwrap(),
            Some(Ok(_)) => continue,
            other => panic!("socket closed: {other:?}"),
        }
    }
}

/// Read frames until `pick` returns something.
async fn recv_until<T>(ws: &mut Ws, mut pick: impl FnMut(&Value) -> Option<T>) -> T {
    loop {
        let v = recv(ws).await;
        if let Some(found) = pick(&v) {
            return found;
        }
    }
}

async fn hello(ws: &mut Ws, auth: Option<&str>) -> Value {
    let mut msg = json!({ "type": "hello", "protocolVersion": 1, "clientId": "test", "clientType": "cli" });
    if let Some(p) = auth {
        msg["auth"] = json!({ "password": p });
    }
    send(ws, msg).await;
    recv(ws).await
}

fn session(msg: Value) -> Value {
    json!({ "type": "session", "message": msg })
}

#[test]
fn serves_the_web_ui() {
    let d = start("http", None);
    let mut s = TcpStream::connect(("127.0.0.1", d.port)).unwrap();
    s.write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n").unwrap();
    let mut body = String::new();
    s.read_to_string(&mut body).unwrap();
    assert!(body.starts_with("HTTP/1.1 200"), "{body}");
    let lower = body.to_ascii_lowercase();
    assert!(lower.contains("<html"));
    // Pairing page only: behind the password, never framed or cached, no agent/project RPCs.
    assert!(lower.contains("x-frame-options: deny"), "{body}");
    assert!(lower.contains("cache-control: no-store"), "{body}");
    assert!(lower.contains("type=\"password\""));
    assert!(lower.contains("daemon.get_pairing_offer.request"));
    assert!(!lower.contains("fetch_agents_request"));
}

#[tokio::test]
async fn handshake_announces_features() {
    let d = start("hello", None);
    let mut ws = connect(d.port).await;
    let info = hello(&mut ws, None).await;
    let payload = &info["message"]["payload"];
    assert_eq!(payload["status"], "server_info");
    assert_eq!(payload["features"]["projectAdd"], true);
    assert_eq!(payload["features"]["providerRemoval"], true);
    assert_eq!(payload["features"]["usageSources"], true);
}

#[tokio::test]
async fn rejects_incompatible_protocol_and_bad_password() {
    let d = start("auth", Some("secret"));
    let mut ws = connect(d.port).await;
    send(&mut ws, json!({ "type": "hello", "protocolVersion": 99 })).await;
    assert_eq!(recv(&mut ws).await["type"], "hello.rejected");

    let mut ws = connect(d.port).await;
    let r = hello(&mut ws, None).await;
    assert_eq!(r["type"], "hello.rejected");
    assert_eq!(r["reason"], "password_required");

    let mut ws = connect(d.port).await;
    let r = hello(&mut ws, Some("wrong")).await;
    assert_eq!(r["reason"], "incorrect_password");

    let mut ws = connect(d.port).await;
    let r = hello(&mut ws, Some("secret")).await;
    assert_eq!(r["message"]["payload"]["status"], "server_info");
}

/// The app offers its password as a `paseo.bearer.<password>` subprotocol and
/// fails the connection (1006) when the server does not select it.
#[tokio::test]
async fn selects_the_bearer_subprotocol_the_app_offers() {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let d = start("bearer", Some("secret"));
    let mut req = format!("ws://127.0.0.1:{}/ws", d.port).into_client_request().unwrap();
    req.headers_mut().insert("Sec-WebSocket-Protocol", "paseo.bearer.secret".parse().unwrap());
    let (mut ws, resp) = tokio_tungstenite::connect_async(req).await.expect("ws connect with subprotocol");
    assert_eq!(resp.headers()["sec-websocket-protocol"], "paseo.bearer.secret");
    let r = hello(&mut ws, Some("secret")).await;
    assert_eq!(r["message"]["payload"]["status"], "server_info");
}

#[tokio::test]
async fn unsupported_requests_get_an_rpc_error() {
    let d = start("rpcerr", None);
    let mut ws = connect(d.port).await;
    hello(&mut ws, None).await;
    send(&mut ws, session(json!({ "type": "create_terminal_request", "requestId": "r1" }))).await;
    let err = recv_until(&mut ws, |v| (v["message"]["type"] == "rpc_error").then(|| v["message"]["payload"].clone())).await;
    assert_eq!(err["requestId"], "r1");
}

#[tokio::test]
async fn daemon_config_custom_models_round_trip() {
    let d = start("config", None);
    let mut ws = connect(d.port).await;
    hello(&mut ws, None).await;
    let patch = json!({ "providers": { "claude": { "additionalModels": [{ "id": "my-custom-model", "label": "Mine" }] } } });
    send(&mut ws, session(json!({ "type": "set_daemon_config_request", "requestId": "c1", "config": patch }))).await;
    recv_until(&mut ws, |v| (v["message"]["type"] == "set_daemon_config_response").then_some(())).await;

    send(&mut ws, session(json!({ "type": "list_provider_models_request", "requestId": "c2", "provider": "claude" }))).await;
    let models = recv_until(&mut ws, |v| (v["message"]["type"] == "list_provider_models_response").then(|| v["message"]["payload"]["models"].clone())).await;
    assert!(models.as_array().unwrap().iter().any(|m| m["id"] == "my-custom-model" && m["label"] == "Mine"));

    // Persisted: a second connection sees it in get_daemon_config.
    let mut ws2 = connect(d.port).await;
    hello(&mut ws2, None).await;
    send(&mut ws2, session(json!({ "type": "get_daemon_config_request", "requestId": "c3" }))).await;
    let cfg = recv_until(&mut ws2, |v| (v["message"]["type"] == "get_daemon_config_response").then(|| v["message"]["payload"]["config"].clone())).await;
    assert_eq!(cfg["providers"]["claude"]["additionalModels"][0]["id"], "my-custom-model");
}

#[tokio::test]
async fn project_add_and_list() {
    let d = start("project", None);
    let proj = d.dir.join("myproject");
    std::fs::create_dir_all(&proj).unwrap();
    let mut ws = connect(d.port).await;
    hello(&mut ws, None).await;
    send(&mut ws, session(json!({ "type": "project.add.request", "requestId": "p1", "cwd": proj.to_string_lossy() }))).await;
    let added = recv_until(&mut ws, |v| (v["message"]["type"] == "project.add.response").then(|| v["message"]["payload"].clone())).await;
    assert!(added["error"].is_null(), "{added}");
    send(&mut ws, session(json!({ "type": "project.list.request", "requestId": "p2" }))).await;
    let list = recv_until(&mut ws, |v| (v["message"]["type"] == "project.list.response").then(|| v["message"]["payload"]["projects"].clone())).await;
    assert_eq!(list.as_array().unwrap().len(), 1);
    assert_eq!(list[0]["projectDisplayName"], "myproject");
}

#[tokio::test]
async fn agent_turn_with_fake_claude() {
    let d = start("agent", None);
    let cwd = d.dir.join("work");
    std::fs::create_dir_all(&cwd).unwrap();
    let mut ws = connect(d.port).await;
    hello(&mut ws, None).await;
    send(
        &mut ws,
        session(json!({
            "type": "create_agent_request",
            "requestId": "a1",
            "config": { "provider": "claude", "cwd": cwd.to_string_lossy(), "modeId": "bypassPermissions" },
            "initialPrompt": "say hi",
        })),
    )
    .await;
    let agent_id = recv_until(&mut ws, |v| {
        let p = &v["message"]["payload"];
        (v["message"]["type"] == "status" && p["status"] == "agent_created").then(|| p["agentId"].as_str().unwrap().to_string())
    })
    .await;

    // The turn completes, and the assistant text lands in the timeline.
    recv_until(&mut ws, |v| (v["message"]["type"] == "agent_stream" && v["message"]["payload"]["event"]["type"] == "turn_completed").then_some(())).await;
    send(&mut ws, session(json!({ "type": "fetch_agent_timeline_request", "requestId": "t1", "agentId": agent_id, "direction": "tail", "limit": 50 }))).await;
    let tl = recv_until(&mut ws, |v| (v["message"]["type"] == "fetch_agent_timeline_response").then(|| v["message"]["payload"].clone())).await;
    assert!(tl["error"].is_null(), "{tl}");
    let items: Vec<String> = tl["entries"].as_array().unwrap().iter().map(|e| e["item"]["type"].as_str().unwrap().to_string()).collect();
    assert!(items.contains(&"user_message".to_string()), "{items:?}");
    assert!(items.contains(&"assistant_message".to_string()), "{items:?}");
    let text = tl["entries"].as_array().unwrap().iter().find(|e| e["item"]["type"] == "assistant_message").unwrap()["item"]["text"].as_str().unwrap().to_string();
    assert!(text.contains("hello from fake claude"));

    // Usage: the turn's tokens and cost are booked for Claude, streamed per provider.
    send(&mut ws, session(json!({ "type": "usage.list_reports.request", "requestId": "u1" }))).await;
    let mut reports = Vec::new();
    loop {
        let v = recv_until(&mut ws, |v| {
            let t = v["message"]["type"].as_str().unwrap_or("");
            (t.starts_with("usage.list_reports.") && v["message"]["payload"]["requestId"] == "u1").then(|| v["message"].clone())
        })
        .await;
        if v["type"] == "usage.list_reports.response" {
            assert!(v["payload"]["error"].is_null(), "{v}");
            break;
        }
        reports.push(v["payload"]["report"].clone());
    }
    let claude = reports.iter().find(|r| r["id"] == "claude").expect("claude report");
    let balance = |r: &Value, id: &str| r["report"]["balances"].as_array().unwrap().iter().find(|b| b["id"] == id).unwrap()["used"].clone();
    assert_eq!(balance(claude, "tokens-today"), 110, "{claude}");
    assert_eq!(balance(claude, "cost-today").as_f64(), Some(1.0));
    assert!(claude["report"]["details"].as_array().unwrap().iter().any(|d| d["label"] == "claude-test"), "{claude}");
    let model = reports.iter().find(|r| r["id"] == "claude:claude-test").expect("per-model card");
    assert_eq!(balance(model, "tokens-all"), 110, "{model}");
    assert_eq!(balance(model, "cost-7d").as_f64(), Some(1.0));
    // An agent sees its own provider, with its own spend first.
    send(&mut ws, session(json!({ "type": "usage.list_reports.request", "requestId": "u2", "agentId": agent_id }))).await;
    let mine = recv_until(&mut ws, |v| (v["message"]["type"] == "usage.list_reports.update" && v["message"]["payload"]["requestId"] == "u2").then(|| v["message"]["payload"]["report"].clone())).await;
    assert_eq!(mine["id"], "claude");
    assert_eq!(balance(&mine, "agent-tokens"), 110);

    // History lists the agent, and it can be archived.
    send(&mut ws, session(json!({ "type": "fetch_agent_history_request", "requestId": "h1" }))).await;
    let hist = recv_until(&mut ws, |v| (v["message"]["type"] == "fetch_agent_history_response").then(|| v["message"]["payload"].clone())).await;
    assert_eq!(hist["entries"].as_array().unwrap().len(), 1);
    send(&mut ws, session(json!({ "type": "archive_agent_request", "requestId": "z1", "agentId": agent_id }))).await;
    recv_until(&mut ws, |v| (v["message"]["type"] == "agent_archived").then_some(())).await;
}

fn git(dir: &std::path::Path, args: &[&str]) -> String {
    let out = Command::new("git").args(args).current_dir(dir).output().expect("git");
    assert!(out.status.success(), "git {args:?}: {}", String::from_utf8_lossy(&out.stderr));
    String::from_utf8_lossy(&out.stdout).into_owned()
}

async fn rpc(ws: &mut Ws, msg: Value, response: &str) -> Value {
    send(ws, session(msg)).await;
    recv_until(ws, |v| (v["message"]["type"] == response).then(|| v["message"]["payload"].clone())).await
}

#[tokio::test]
async fn git_actions_commit_branch_stash_history() {
    let d = start("git", None);
    let repo = d.dir.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "-q", "-b", "main"]);
    git(&repo, &["config", "user.email", "test@example.com"]);
    git(&repo, &["config", "user.name", "Test"]);
    std::fs::write(repo.join("a.txt"), "one\n").unwrap();
    git(&repo, &["add", "-A"]);
    git(&repo, &["commit", "-q", "-m", "initial"]);
    let cwd = repo.to_string_lossy().to_string();
    let mut ws = connect(d.port).await;
    hello(&mut ws, None).await;

    // Commit with an explicit message, and with none (auto-generated).
    std::fs::write(repo.join("a.txt"), "two\n").unwrap();
    let r = rpc(&mut ws, json!({ "type": "checkout_commit_request", "requestId": "g1", "cwd": cwd, "message": "Change a" }), "checkout_commit_response").await;
    assert_eq!(r["success"], true, "{r}");
    std::fs::write(repo.join("b.txt"), "new\n").unwrap();
    let r = rpc(&mut ws, json!({ "type": "checkout_commit_request", "requestId": "g2", "cwd": cwd }), "checkout_commit_response").await;
    assert_eq!(r["success"], true, "{r}");
    assert!(git(&repo, &["log", "-1", "--format=%s"]).contains("b.txt"));

    // Rename the branch, then check suggestions and validation see it.
    let r = rpc(&mut ws, json!({ "type": "checkout.rename_branch.request", "requestId": "g3", "cwd": cwd, "branch": "feat/x" }), "checkout.rename_branch.response").await;
    assert_eq!(r["currentBranch"], "feat/x", "{r}");
    let r = rpc(&mut ws, json!({ "type": "branch_suggestions_request", "requestId": "g4", "cwd": cwd, "query": "feat" }), "branch_suggestions_response").await;
    assert_eq!(r["branches"], json!(["feat/x"]), "{r}");
    let r = rpc(&mut ws, json!({ "type": "validate_branch_request", "requestId": "g5", "cwd": cwd, "branchName": "feat/x" }), "validate_branch_response").await;
    assert_eq!(r["exists"], true);
    let r = rpc(&mut ws, json!({ "type": "validate_branch_request", "requestId": "g6", "cwd": cwd, "branchName": "nope" }), "validate_branch_response").await;
    assert_eq!(r["exists"], false);

    // Stash a change, list it, pop it back.
    std::fs::write(repo.join("a.txt"), "three\n").unwrap();
    let r = rpc(&mut ws, json!({ "type": "stash_save_request", "requestId": "g7", "cwd": cwd, "branch": "feat/x" }), "stash_save_response").await;
    assert_eq!(r["success"], true, "{r}");
    let r = rpc(&mut ws, json!({ "type": "stash_list_request", "requestId": "g8", "cwd": cwd }), "stash_list_response").await;
    assert_eq!(r["entries"][0]["branch"], "feat/x", "{r}");
    assert_eq!(r["entries"][0]["isPaseo"], true);
    let r = rpc(&mut ws, json!({ "type": "stash_pop_request", "requestId": "g9", "cwd": cwd, "stashIndex": 0 }), "stash_pop_response").await;
    assert_eq!(r["success"], true, "{r}");
    assert_eq!(std::fs::read_to_string(repo.join("a.txt")).unwrap(), "three\n");

    // Discard brings the file back to HEAD; untracked files are removed.
    std::fs::write(repo.join("junk.txt"), "x").unwrap();
    let r = rpc(&mut ws, json!({ "type": "checkout.discard_changes.request", "requestId": "g10", "cwd": cwd, "paths": ["a.txt", "junk.txt"] }), "checkout.discard_changes.response").await;
    assert_eq!(r["success"], true, "{r}");
    assert_eq!(std::fs::read_to_string(repo.join("a.txt")).unwrap(), "two\n");
    assert!(!repo.join("junk.txt").exists());

    // Switch back to a new main and merge feat/x into it from the branch.
    git(&repo, &["branch", "main"]);
    let r = rpc(&mut ws, json!({ "type": "checkout_switch_branch_request", "requestId": "g11", "cwd": cwd, "branch": "main" }), "checkout_switch_branch_response").await;
    assert_eq!(r["success"], true, "{r}");
    assert_eq!(r["source"], "local");

    // History with file stats, and a single commit's file diff.
    let r = rpc(&mut ws, json!({ "type": "checkout.commits.list.request", "requestId": "g12", "cwd": cwd }), "checkout.commits.list.response").await;
    let commits = r["commits"].as_array().unwrap();
    assert!(commits.len() >= 3, "{r}");
    let change_a = commits.iter().find(|c| c["subject"] == "Change a").unwrap();
    assert_eq!(change_a["files"][0]["path"], "a.txt");
    let sha = change_a["sha"].as_str().unwrap();
    let r = rpc(&mut ws, json!({ "type": "checkout.commits.file_diff.request", "requestId": "g13", "cwd": cwd, "sha": sha, "path": "a.txt" }), "checkout.commits.file_diff.response").await;
    assert_eq!(r["file"]["path"], "a.txt", "{r}");
    assert_eq!(r["file"]["additions"], 1);

    // Not a repository -> NOT_GIT_REPO; PR status without a remote -> no_remote.
    let r = rpc(&mut ws, json!({ "type": "checkout_push_request", "requestId": "g14", "cwd": d.dir.to_string_lossy() }), "checkout_push_response").await;
    assert_eq!(r["error"]["code"], "NOT_GIT_REPO", "{r}");
    let r = rpc(&mut ws, json!({ "type": "checkout_pr_status_request", "requestId": "g15", "cwd": cwd }), "checkout_pr_status_response").await;
    assert_eq!(r["authState"], "no_remote", "{r}");
    assert!(r["status"].is_null());
}

/// A turn in flight when the daemon is killed (container replaced) resumes by
/// itself after the restart, in the same conversation.
#[tokio::test]
async fn interrupted_turn_resumes_after_hard_kill() {
    let dir = unique_dir("resume");
    let cwd = dir.join("work");
    std::fs::create_dir_all(&cwd).unwrap();

    // First life: the "model" takes the prompt and never answers.
    let mut first = start_at(dir.clone(), None, "hang");
    let mut ws = connect(first.port).await;
    hello(&mut ws, None).await;
    send(
        &mut ws,
        session(json!({ "type": "create_agent_request", "requestId": "r1", "config": { "provider": "claude", "cwd": cwd.to_string_lossy(), "modeId": "bypassPermissions" }, "initialPrompt": "do the thing" })),
    )
    .await;
    let agent_id = recv_until(&mut ws, |v| {
        let p = &v["message"]["payload"];
        (v["message"]["type"] == "status" && p["status"] == "agent_created").then(|| p["agentId"].as_str().unwrap().to_string())
    })
    .await;
    // Wait until the agent record with its running turn is on disk, then SIGKILL (no graceful flush).
    let agents_file = dir.join("home").join("agents.json");
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let saved = std::fs::read_to_string(&agents_file).unwrap_or_default();
        if saved.contains("\"pendingTurn\":\"do the thing\"") {
            break;
        }
        assert!(Instant::now() < deadline, "pending turn never saved: {saved}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    first.child.kill().unwrap();
    let _ = first.child.wait();
    std::mem::forget(first); // keep the directory: it is the "volume"

    // Second life: a working model. The daemon resumes the turn on its own.
    let second = start_at(dir, None, "ok");
    let mut ws = connect(second.port).await;
    hello(&mut ws, None).await;
    let deadline = Instant::now() + Duration::from_secs(30);
    let tl = loop {
        let tl = rpc(&mut ws, json!({ "type": "fetch_agent_timeline_request", "requestId": "r2", "agentId": agent_id, "direction": "tail", "limit": 50 }), "fetch_agent_timeline_response").await;
        let has_answer = tl["entries"].as_array().unwrap().iter().any(|e| e["item"]["type"] == "assistant_message");
        if has_answer {
            break tl;
        }
        assert!(Instant::now() < deadline, "turn was not resumed: {tl}");
        tokio::time::sleep(Duration::from_millis(300)).await;
    };
    let texts: Vec<String> = tl["entries"].as_array().unwrap().iter().filter(|e| e["item"]["type"] == "notification").map(|e| e["item"]["message"].as_str().unwrap().to_string()).collect();
    assert!(texts.iter().any(|t| t.contains("restarted")), "{texts:?}");
}

/// An app that was in the background catches up with `direction: "after"`.
/// A tool call row is created "running" and later updated in place to
/// "completed": catching up from a cursor at its start must return the update.
#[tokio::test]
async fn catch_up_returns_rows_updated_in_place() {
    let d = start_at(unique_dir("catchup"), None, "tool");
    let cwd = d.dir.join("work");
    std::fs::create_dir_all(&cwd).unwrap();
    let mut ws = connect(d.port).await;
    hello(&mut ws, None).await;
    send(
        &mut ws,
        session(json!({ "type": "create_agent_request", "requestId": "c1", "config": { "provider": "claude", "cwd": cwd.to_string_lossy(), "modeId": "bypassPermissions" }, "initialPrompt": "list files" })),
    )
    .await;
    let agent_id = recv_until(&mut ws, |v| {
        let p = &v["message"]["payload"];
        (v["message"]["type"] == "status" && p["status"] == "agent_created").then(|| p["agentId"].as_str().unwrap().to_string())
    })
    .await;
    recv_until(&mut ws, |v| (v["message"]["type"] == "agent_stream" && v["message"]["payload"]["event"]["type"] == "turn_completed").then_some(())).await;

    let tail = rpc(&mut ws, json!({ "type": "fetch_agent_timeline_request", "requestId": "c2", "agentId": agent_id, "direction": "tail", "limit": 50 }), "fetch_agent_timeline_response").await;
    let tool = tail["entries"].as_array().unwrap().iter().find(|e| e["item"]["type"] == "tool_call").cloned().expect("tool call row");
    assert_eq!(tool["item"]["status"], "completed");
    let start = tool["seqStart"].as_u64().unwrap();
    assert!(tool["seqEnd"].as_u64().unwrap() > start, "the row was updated in place: {tool}");

    // The app last saw the row when it was created (cursor = its seqStart).
    let after = rpc(
        &mut ws,
        json!({ "type": "fetch_agent_timeline_request", "requestId": "c3", "agentId": agent_id, "direction": "after", "cursor": { "epoch": tail["epoch"], "seq": start }, "limit": 50 }),
        "fetch_agent_timeline_response",
    )
    .await;
    let entries = after["entries"].as_array().unwrap();
    let updated = entries.iter().find(|e| e["item"]["type"] == "tool_call").expect("updated tool call must be returned");
    assert_eq!(updated["item"]["status"], "completed");
    assert!(entries.iter().any(|e| e["item"]["type"] == "assistant_message"));
    assert_eq!(after["hasNewer"], false);
    assert_eq!(after["endCursor"]["seq"], after["window"]["maxSeq"]);
}

fn file_frame(op: u8, request_id: &str, body: &[u8]) -> Vec<u8> {
    let mut v = vec![op, request_id.len() as u8];
    v.extend_from_slice(request_id.as_bytes());
    v.extend_from_slice(body);
    v
}

/// The app's upload protocol: file.upload.request, then binary frames
/// (begin / chunks / end); the file is then attached to a message.
#[tokio::test]
async fn upload_file_and_attach_it_with_an_image() {
    let d = start("upload", None);
    let cwd = d.dir.join("work");
    std::fs::create_dir_all(&cwd).unwrap();
    let mut ws = connect(d.port).await;
    hello(&mut ws, None).await;

    let content = b"line one\nline two\n";
    send(&mut ws, session(json!({ "type": "file.upload.request", "requestId": "up1", "fileName": "../notes.txt", "mimeType": "text/plain", "size": content.len(), "modifiedAt": "2026-10-11T00:00:00Z" }))).await;
    let meta = br#"{"mime":"text/plain","size":18,"encoding":"binary","modifiedAt":"2026-10-11T00:00:00Z"}"#;
    let mut begin_body = (meta.len() as u16).to_be_bytes().to_vec();
    begin_body.extend_from_slice(meta);
    ws.send(Message::Binary(file_frame(0x10, "up1", &begin_body))).await.unwrap();
    ws.send(Message::Binary(file_frame(0x11, "up1", &content[..9]))).await.unwrap();
    ws.send(Message::Binary(file_frame(0x11, "up1", &content[9..]))).await.unwrap();
    ws.send(Message::Binary(file_frame(0x12, "up1", b""))).await.unwrap();
    let up = recv_until(&mut ws, |v| (v["message"]["type"] == "file.upload.response").then(|| v["message"]["payload"].clone())).await;
    assert!(up["error"].is_null(), "{up}");
    let file = up["file"].clone();
    assert_eq!(file["fileName"], "notes.txt", "path components are stripped");
    let saved = std::fs::read(file["path"].as_str().unwrap()).unwrap();
    assert_eq!(saved, content);

    // Create an agent and send a message with the file and an inline image.
    send(&mut ws, session(json!({ "type": "create_agent_request", "requestId": "a1", "config": { "provider": "claude", "cwd": cwd.to_string_lossy(), "modeId": "bypassPermissions" } }))).await;
    let agent_id = recv_until(&mut ws, |v| {
        let p = &v["message"]["payload"];
        (v["message"]["type"] == "status" && p["status"] == "agent_created").then(|| p["agentId"].as_str().unwrap().to_string())
    })
    .await;
    let png = "iVBORw0KGgo=";
    send(
        &mut ws,
        session(json!({ "type": "send_agent_message_request", "requestId": "m1", "agentId": agent_id, "text": "what is in these?", "images": [{ "data": png, "mimeType": "image/png" }], "attachments": [file] })),
    )
    .await;
    recv_until(&mut ws, |v| (v["message"]["type"] == "agent_stream" && v["message"]["payload"]["event"]["type"] == "turn_completed").then_some(())).await;
    let tl = rpc(&mut ws, json!({ "type": "fetch_agent_timeline_request", "requestId": "t1", "agentId": agent_id, "direction": "tail", "limit": 50 }), "fetch_agent_timeline_response").await;
    let bubble = tl["entries"].as_array().unwrap().iter().find(|e| e["item"]["type"] == "user_message").unwrap()["item"]["text"].as_str().unwrap().to_string();
    assert!(bubble.starts_with("what is in these?"), "{bubble}");
    assert!(bubble.contains("📎") && bubble.contains("notes.txt") && bubble.contains("image.png"), "{bubble}");
    assert!(!bubble.contains("Path:"), "agent notes stay out of the bubble: {bubble}");
}

#[tokio::test]
async fn streamed_text_is_sent_in_batches() {
    let d = start_at(unique_dir("stream"), None, "stream");
    let cwd = d.dir.join("work");
    std::fs::create_dir_all(&cwd).unwrap();
    let mut ws = connect(d.port).await;
    hello(&mut ws, None).await;
    send(
        &mut ws,
        session(json!({ "type": "create_agent_request", "requestId": "s1", "config": { "provider": "claude", "cwd": cwd.to_string_lossy(), "modeId": "bypassPermissions" }, "initialPrompt": "talk" })),
    )
    .await;
    let agent_id = recv_until(&mut ws, |v| {
        let p = &v["message"]["payload"];
        (v["message"]["type"] == "status" && p["status"] == "agent_created").then(|| p["agentId"].as_str().unwrap().to_string())
    })
    .await;
    // Count the assistant text messages until the turn ends.
    let mut texts = Vec::new();
    recv_until(&mut ws, |v| {
        let ev = &v["message"]["payload"]["event"];
        if v["message"]["type"] != "agent_stream" {
            return None;
        }
        if ev["type"] == "timeline" && ev["item"]["type"] == "assistant_message" {
            texts.push(ev["item"]["text"].as_str().unwrap_or("").to_string());
        }
        (ev["type"] == "turn_completed").then_some(())
    })
    .await;
    let expected: String = (0..20).map(|i| format!("w{i} ")).collect();
    assert_eq!(texts.concat(), expected);
    assert!(texts.len() < 20, "deltas were not batched: {} messages", texts.len());
    // The stored row has the whole text, before the turn ended.
    let tail = rpc(&mut ws, json!({ "type": "fetch_agent_timeline_request", "requestId": "s2", "agentId": agent_id, "direction": "tail", "limit": 50 }), "fetch_agent_timeline_response").await;
    let rows: Vec<&Value> = tail["entries"].as_array().unwrap().iter().filter(|e| e["item"]["type"] == "assistant_message").collect();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["item"]["text"], expected);
}

#[tokio::test]
async fn usage_limit_waits_and_continues() {
    let d = start_at(unique_dir("limit"), None, "limit");
    let cwd = d.dir.join("work");
    std::fs::create_dir_all(&cwd).unwrap();
    let mut ws = connect(d.port).await;
    hello(&mut ws, None).await;
    send(
        &mut ws,
        session(json!({ "type": "create_agent_request", "requestId": "l1", "config": { "provider": "claude", "cwd": cwd.to_string_lossy(), "modeId": "bypassPermissions" }, "initialPrompt": "build it" })),
    )
    .await;
    // The limit is announced, then the same turn carries on and completes: never fails.
    let mut noted = false;
    recv_until(&mut ws, |v| {
        let ev = &v["message"]["payload"]["event"];
        if v["message"]["type"] != "agent_stream" {
            return None;
        }
        assert_ne!(ev["type"], "turn_failed", "{ev}");
        if ev["type"] == "timeline" && ev["item"]["type"] == "notification" {
            noted |= ev["item"]["message"].as_str().unwrap_or("").starts_with("Usage limit reached");
        }
        (ev["type"] == "turn_completed").then_some(())
    })
    .await;
    assert!(noted, "the wait was not announced");
    assert!(d.dir.join("limit-hit").exists());
}
