//! Map provider tool calls onto Paseo `tool_call` timeline items.

use serde_json::{json, Value};

fn s(v: &Value, key: &str) -> Option<String> {
    v.get(key).and_then(|x| x.as_str()).map(|x| x.to_string())
}

/// Bound tool output kept in memory / on disk / on the wire.
const MAX_OUTPUT: usize = 16 * 1024;

pub fn clip(text: &str) -> String {
    crate::util::truncate(text, MAX_OUTPUT)
}

/// Detail for a Claude Code tool invocation.
pub fn claude_detail(name: &str, input: &Value, output: Option<&str>) -> Value {
    let out = output.map(clip);
    match name {
        "Bash" => json!({ "type": "shell", "command": s(input, "command").unwrap_or_default(), "output": out }),
        "Read" => json!({ "type": "read", "filePath": s(input, "file_path").unwrap_or_default() }),
        "Edit" | "MultiEdit" => json!({
            "type": "edit",
            "filePath": s(input, "file_path").unwrap_or_default(),
            "oldString": s(input, "old_string").map(|x| clip(&x)),
            "newString": s(input, "new_string").map(|x| clip(&x)),
        }),
        "Write" => json!({
            "type": "write",
            "filePath": s(input, "file_path").unwrap_or_default(),
            "content": s(input, "content").map(|x| clip(&x)),
        }),
        "Grep" => json!({ "type": "search", "query": s(input, "pattern").unwrap_or_default(), "toolName": "grep", "content": out }),
        "Glob" => json!({ "type": "search", "query": s(input, "pattern").unwrap_or_default(), "toolName": "glob", "content": out }),
        "WebSearch" => json!({ "type": "search", "query": s(input, "query").unwrap_or_default(), "toolName": "web_search", "content": out }),
        "WebFetch" => json!({ "type": "fetch", "url": s(input, "url").unwrap_or_default(), "prompt": s(input, "prompt"), "result": out }),
        "Task" | "Agent" => json!({
            "type": "sub_agent",
            "subAgentType": s(input, "subagent_type"),
            "description": s(input, "description"),
            "log": out.unwrap_or_default(),
            "actions": [],
        }),
        "ExitPlanMode" => json!({ "type": "plan", "text": s(input, "plan").unwrap_or_default() }),
        _ => json!({ "type": "unknown", "input": input, "output": out.map(Value::String).unwrap_or(Value::Null) }),
    }
}

/// One line of a sub-agent's activity log: `[Tool] what it touched`.
pub fn claude_action_line(name: &str, input: &Value) -> String {
    let key = match name {
        "Bash" => "command",
        "Read" | "Edit" | "MultiEdit" | "Write" | "NotebookEdit" => "file_path",
        "Grep" | "Glob" => "pattern",
        "WebSearch" => "query",
        "WebFetch" => "url",
        "Task" | "Agent" => "description",
        _ => "",
    };
    let summary = s(input, key).map(|x| x.split_whitespace().collect::<Vec<_>>().join(" ")).filter(|x| !x.is_empty());
    match summary {
        Some(x) => format!("[{name}] {}", crate::util::truncate(&x, 160)),
        None => format!("[{name}]"),
    }
}

pub fn tool_call(call_id: &str, name: &str, detail: Value, status: &str, error: Option<String>) -> Value {
    let error = match (status, error) {
        ("failed", Some(e)) => Value::String(clip(&e)),
        ("failed", None) => Value::String("failed".into()),
        _ => Value::Null,
    };
    json!({
        "type": "tool_call",
        "callId": call_id,
        "name": name,
        "detail": detail,
        "status": status,
        "error": error,
    })
}

/// Claude `TodoWrite` becomes a `todo` item instead of a tool row.
pub fn claude_todo(input: &Value) -> Option<Value> {
    let todos = input.get("todos")?.as_array()?;
    let items: Vec<Value> = todos
        .iter()
        .map(|t| {
            let status = s(t, "status").unwrap_or_else(|| "pending".into());
            json!({
                "text": s(t, "content").unwrap_or_default(),
                "completed": status == "completed",
                "status": status,
                "activeForm": s(t, "activeForm"),
            })
        })
        .collect();
    Some(json!({ "type": "todo", "items": items }))
}

fn codex_command(item: &Value) -> String {
    match item.get("command") {
        Some(Value::String(c)) => c.clone(),
        Some(Value::Array(parts)) => parts.iter().filter_map(|p| p.as_str()).collect::<Vec<_>>().join(" "),
        _ => String::new(),
    }
}

fn codex_status(item: &Value, completed: bool) -> &'static str {
    let raw = item.get("status").and_then(|v| v.as_str()).unwrap_or("");
    let lower = raw.to_ascii_lowercase();
    if lower.contains("fail") || lower.contains("error") || lower.contains("declin") {
        "failed"
    } else if lower.contains("cancel") || lower.contains("interrupt") {
        "canceled"
    } else if completed || lower.contains("complet") || lower.contains("success") {
        "completed"
    } else {
        "running"
    }
}

/// Map a Codex app-server thread item to a timeline item (tool kinds only;
/// messages and reasoning are handled by the caller).
pub fn codex_tool_item(item: &Value, completed: bool) -> Option<Value> {
    let id = s(item, "id")?;
    let kind = s(item, "type")?;
    let mut status = codex_status(item, completed);
    let error = item.get("error").filter(|e| !e.is_null()).map(|e| match e {
        Value::String(x) => x.clone(),
        other => other.get("message").and_then(|m| m.as_str()).map(|m| m.to_string()).unwrap_or_else(|| other.to_string()),
    });
    if error.is_some() && completed {
        status = "failed";
    }
    let (name, detail) = match kind.as_str() {
        "commandExecution" => {
            let exit = item.get("exitCode").and_then(|v| v.as_i64());
            if completed && exit.map(|c| c != 0).unwrap_or(false) {
                status = "failed";
            }
            (
                "shell".to_string(),
                json!({
                    "type": "shell",
                    "command": codex_command(item),
                    "cwd": s(item, "cwd"),
                    "output": s(item, "aggregatedOutput").map(|o| clip(&o)),
                    "exitCode": exit,
                }),
            )
        }
        "fileChange" => {
            let changes = item.get("changes").and_then(|c| c.as_array()).cloned().unwrap_or_default();
            let first = changes.first().cloned().unwrap_or(Value::Null);
            let path = s(&first, "path").unwrap_or_default();
            let diff = s(&first, "diff").or_else(|| s(&first, "unified_diff"));
            let detail = if changes.len() <= 1 {
                json!({ "type": "edit", "filePath": path, "unifiedDiff": diff.map(|d| clip(&d)) })
            } else {
                let files: Vec<String> = changes.iter().filter_map(|c| s(c, "path")).collect();
                json!({ "type": "plain_text", "label": format!("{} files changed", files.len()), "text": files.join("\n") })
            };
            ("apply_patch".to_string(), detail)
        }
        "mcpToolCall" => {
            let tool = s(item, "tool").unwrap_or_else(|| "mcp".into());
            let server = s(item, "server").unwrap_or_default();
            (
                format!("mcp__{server}__{tool}"),
                json!({ "type": "unknown", "input": item.get("arguments").cloned().unwrap_or(Value::Null), "output": item.get("result").cloned().unwrap_or(Value::Null) }),
            )
        }
        "webSearch" => (
            "web_search".to_string(),
            json!({ "type": "search", "query": s(item, "query").unwrap_or_default(), "toolName": "web_search" }),
        ),
        _ => return None,
    };
    let error = if status == "failed" { error.or_else(|| Some("failed".into())) } else { None };
    Some(tool_call(&id, &name, detail, status, error))
}
