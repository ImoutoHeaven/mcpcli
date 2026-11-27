use serde::ser::SerializeMap;
use serde_json::{Map, Value};
use std::io::{self, Write};

pub const BUILTIN_SKILL_NAME: &str = "mcpcli";
pub const BUILTIN_SKILL_VERSION: &str = env!("CARGO_PKG_VERSION");

pub const BUILTIN_SKILL_MD: &str = concat!(
    "---\n",
    "name: mcpcli\n",
    "description: Discover configured MCP servers and skills, inspect tool schemas, call tools with typed KEY:=VALUE arguments, manage async MCP tasks, and read remote server skills.\n",
    "version: \"",
    env!("CARGO_PKG_VERSION"),
    "\"\n",
    "---\n\n",
    "# mcpcli\n\n",
    "Default output is a single JSON value on stdout. Pass `--text` for escaped terminal text.\n\n",
    "## Discovery\n\n",
    "1. Run `mcpcli catalog` to read the configured global `AGENTS.md`, local skills, and server list without starting or connecting to any server.\n",
    "2. Run `mcpcli tools <server> --summary` to list tool names and first-line descriptions. Narrow results with `--filter <substring>`.\n",
    "3. Run `mcpcli inspect <server> <tool>` to read the full input and output schema before calling a tool.\n\n",
    "## Schema gate\n\n",
    "Pin an expected tool snapshot or raw input schema file with `--schema <file>` on `inspect` or `call`:\n\n",
    "```bash\n",
    "mcpcli inspect <server> <tool> --schema tool.json\n",
    "mcpcli call <server> <tool> --schema tool.json --schema-mode exact query:=rust\n",
    "```\n\n",
    "`--schema-mode` defaults to `compatible`, which allows backward-compatible changes such as new optional input fields or widening `integer` to `number`. Use `exact` for normalized deep equality. A mismatch exits with code `2` before sending `tools/call`.\n\n",
    "## Calling tools\n\n",
    "Pass arguments as `KEY:=VALUE` pairs, `--args <json>`, `--args-file <path>`, or `--stdin`. Each `VALUE` parses as lossless JSON when valid and falls back to a string otherwise. Use `--` before keys that start with `-`:\n\n",
    "```bash\n",
    "mcpcli call <server> <tool> query:=mcpcli limit:=10 enabled:=true\n",
    "mcpcli call <server> <tool> -- -flag:=1\n",
    "```\n\n",
    "## Async MCP tasks\n\n",
    "`mcpcli call` polls `resultType: \"task\"` responses until completion by default. Use `--detach` to return the task handle immediately, then manage it with `tasks` (alias `task`):\n\n",
    "```bash\n",
    "mcpcli call <server> <tool> --detach query:=long_job\n",
    "mcpcli tasks get <server> <task_id> --wait --max-timeout 60\n",
    "mcpcli tasks update <server> <task_id> --args '{\"confirm\":true}'\n",
    "mcpcli tasks cancel <server> <task_id>\n",
    "```\n\n",
    "## Skills\n\n",
    "Omit `--server` to inspect local configured skills, or pass `--server <server>` to read remote skills exposed over MCP:\n\n",
    "```bash\n",
    "mcpcli skills list --server <server>\n",
    "mcpcli skills show --server <server> <name_or_uri>\n",
    "mcpcli skills files --server <server> [--dir <rel_dir>] <name_or_uri>\n",
    "mcpcli skills read --server <server> [--inline-media] <name_or_uri> <rel_path>\n",
    "mcpcli skills builtin\n",
    "```\n\n",
    "## Exit codes\n\n",
    "| Code | Meaning |\n",
    "| --- | --- |\n",
    "| `0` | Success |\n",
    "| `1` | Internal, configuration, or memory limit error |\n",
    "| `2` | CLI usage, argument, path, task ID, or schema mismatch error |\n",
    "| `3` | Tool completed with `isError: true` |\n",
    "| `4` | MCP protocol, skill integrity, task failure, or cancellation error |\n",
    "| `5` | Timeout |\n",
    "| `6` | Session lost or daemon protocol mismatch |\n",
);

/// Serialize selected fields by reference without cloning their values.
pub struct ObjectWithout<'a>(pub &'a Map<String, Value>, pub &'a [&'a str]);

impl serde::Serialize for ObjectWithout<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let count = self
            .0
            .keys()
            .filter(|key| !self.1.contains(&key.as_str()))
            .count();
        let mut map = serializer.serialize_map(Some(count))?;
        for (key, value) in self.0 {
            if !self.1.contains(&key.as_str()) {
                map.serialize_entry(key, value)?;
            }
        }
        map.end()
    }
}

/// Inspect exact schemas while retaining their existing owned payload.
pub fn write_tool(stdout: &mut impl Write, tool: &Value) -> io::Result<()> {
    let Some(object) = tool.as_object() else {
        write_text_json(stdout, tool)?;
        return writeln!(stdout);
    };
    let title = single_line(
        object
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("<unnamed>"),
        128,
    );
    stdout.write_all(title.as_bytes())?;
    let mut emitted = !title.is_empty();
    let mut ends_with_lf = false;
    if let Some(description) = object.get("description").and_then(Value::as_str) {
        stdout.write_all(b"\n\n")?;
        write_safe_text(stdout, description)?;
        emitted = true;
        ends_with_lf = description.is_empty() || description.ends_with('\n');
    }
    if let Some(schema) = object.get("inputSchema") {
        stdout.write_all(b"\n\nInput schema:\n")?;
        write_text_json(stdout, schema)?;
        emitted = true;
        ends_with_lf = false;
    }
    let excluded = ["name", "description", "inputSchema"];
    if object.keys().any(|key| !excluded.contains(&key.as_str())) {
        stdout.write_all(b"\n\nMetadata:\n")?;
        write_text_json(stdout, &ObjectWithout(object, &excluded))?;
        emitted = true;
        ends_with_lf = false;
    }
    if emitted && !ends_with_lf {
        stdout.write_all(b"\n")?;
    }
    Ok(())
}

/// Write borrowed text spans, escaping controls without a second full string.
pub fn write_safe_text(stdout: &mut impl Write, text: &str) -> io::Result<()> {
    let mut start = 0;
    for (index, character) in text.char_indices() {
        if character == '\r' && text.as_bytes().get(index + 1) == Some(&b'\n') {
            stdout.write_all(&text.as_bytes()[start..index])?;
            start = index + 1;
        } else if character.is_control() && !matches!(character, '\n' | '\t') {
            stdout.write_all(&text.as_bytes()[start..index])?;
            write!(stdout, "{}", character.escape_default())?;
            start = index + character.len_utf8();
        }
    }
    stdout.write_all(&text.as_bytes()[start..])
}

/// Preserve CRLF normalization when adjacent borrowed spans meet at a newline.
pub fn write_text_parts<'a>(
    stdout: &mut impl Write,
    parts: impl IntoIterator<Item = &'a str>,
) -> io::Result<()> {
    let mut parts = parts.into_iter().filter(|part| !part.is_empty()).peekable();
    let mut emitted = false;
    let mut ends_with_lf = false;
    while let Some(part) = parts.next() {
        let part = if parts.peek().is_some_and(|next| next.starts_with('\n')) {
            part.strip_suffix('\r').unwrap_or(part)
        } else {
            part
        };
        write_safe_text(stdout, part)?;
        emitted |= !part.is_empty();
        ends_with_lf = part.ends_with('\n');
    }
    if emitted && !ends_with_lf {
        stdout.write_all(b"\n")?;
    }
    Ok(())
}

pub fn write_text_json(stdout: &mut impl Write, value: &impl serde::Serialize) -> io::Result<()> {
    struct SafeWriter<'a, W>(&'a mut W);
    impl<W: Write> Write for SafeWriter<'_, W> {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            // Serde JSON writes complete UTF-8 string fragments and ASCII syntax.
            let text = std::str::from_utf8(bytes).map_err(io::Error::other)?;
            write_safe_text(self.0, text)?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            self.0.flush()
        }
    }
    serde_json::to_writer_pretty(SafeWriter(stdout), value).map_err(io::Error::other)
}

pub fn safe_text(text: &str) -> String {
    let mut result = String::with_capacity(text.len());
    let mut characters = text.chars().peekable();
    while let Some(character) = characters.next() {
        if character == '\r' && characters.peek() == Some(&'\n') {
            continue;
        }
        if character.is_control() && !matches!(character, '\n' | '\t') {
            result.extend(character.escape_default());
        } else {
            result.push(character);
        }
    }
    result
}

pub fn summary_description(text: &str) -> Option<String> {
    let mut result = String::with_capacity(256 * 4 + '…'.len_utf8());
    for line in text.split_inclusive('\n') {
        let line = match line.strip_suffix('\n') {
            Some(line) => line.strip_suffix('\r').unwrap_or(line),
            None => line,
        };
        result.clear();
        let mut count = 0usize;
        let mut last_nonspace = 0usize;
        let mut truncated = false;
        let mut push = |character: char| {
            if count == 0 && character.is_whitespace() {
                return true;
            }
            count = count.saturating_add(1);
            if count <= 256 {
                result.push(character);
                if !character.is_whitespace() {
                    last_nonspace = result.len();
                }
            }
            count <= 256 || character.is_whitespace()
        };
        for character in line.chars() {
            let keep = if character.is_control() && character != '\t' {
                character.escape_default().all(&mut push)
            } else {
                push(character)
            };
            if !keep {
                truncated = true;
                break;
            }
        }
        if truncated {
            result.push('…');
        } else {
            result.truncate(last_nonspace);
        }
        if !result.is_empty() {
            result.shrink_to_fit();
            return Some(result);
        }
    }
    None
}

pub fn summarize_tool(mut tool: Value) -> Value {
    let Some(object) = tool.as_object_mut() else {
        return tool;
    };
    let mut summary = Map::new();
    if let Some(name) = object.remove("name").filter(Value::is_string) {
        summary.insert("name".into(), name);
    }
    if let Some(Value::String(description)) = object.remove("description")
        && let Some(description) = summary_description(&description)
    {
        summary.insert("description".into(), Value::String(description));
    }
    if let Some(annotations) = object.remove("annotations").filter(Value::is_object) {
        summary.insert("annotations".into(), annotations);
    }
    Value::Object(summary)
}

pub fn write_remote_skills_list(stdout: &mut impl Write, skills: &[Value]) -> io::Result<()> {
    for skill in skills {
        let frontmatter = skill.get("frontmatter").and_then(Value::as_object);
        let name = skill
            .get("name")
            .and_then(Value::as_str)
            .or_else(|| {
                frontmatter
                    .and_then(|f| f.get("name"))
                    .and_then(Value::as_str)
            })
            .unwrap_or("");
        let uri = skill.get("uri").and_then(Value::as_str).unwrap_or("");
        let desc = skill
            .get("description")
            .and_then(Value::as_str)
            .or_else(|| {
                frontmatter
                    .and_then(|f| f.get("description"))
                    .and_then(Value::as_str)
            })
            .and_then(summary_description)
            .unwrap_or_default();
        writeln!(
            stdout,
            "{}\t{}\t{}",
            single_line(name, 256),
            single_line(uri, 2048),
            desc
        )?;
    }
    Ok(())
}

pub fn write_remote_skill_show(
    stdout: &mut impl Write,
    server: &str,
    uri: &str,
    dynamic: bool,
    content: &str,
) -> io::Result<()> {
    write!(
        stdout,
        "Origin: {} ({})",
        single_line(server, 256),
        single_line(uri, 2048)
    )?;
    if dynamic {
        stdout.write_all(b" [dynamic, unverified]")?;
    }
    stdout.write_all(b"\n\n")?;
    write_safe_text(stdout, content)?;
    if !content.is_empty() && !content.ends_with('\n') {
        stdout.write_all(b"\n")?;
    }
    Ok(())
}

pub fn write_remote_skill_files(stdout: &mut impl Write, entries: &[Value]) -> io::Result<()> {
    for entry in entries {
        let label = entry
            .get("path")
            .and_then(Value::as_str)
            .or_else(|| entry.get("uri").and_then(Value::as_str))
            .unwrap_or("");
        stdout.write_all(single_line(label, 2048).as_bytes())?;
        if let Some(size) = entry.get("size").and_then(Value::as_u64) {
            write!(stdout, "\t{size}")?;
        }
        if let Some(extra) = entry
            .get("digest")
            .and_then(Value::as_str)
            .or_else(|| entry.get("mimeType").and_then(Value::as_str))
        {
            write!(stdout, "\t{}", single_line(extra, 256))?;
        }
        stdout.write_all(b"\n")?;
    }
    Ok(())
}

pub fn write_task_status(
    stdout: &mut impl Write,
    task_id: &str,
    status: &str,
    status_message: Option<&str>,
) -> io::Result<()> {
    writeln!(
        stdout,
        "task {} [{}]",
        single_line(task_id, 512),
        single_line(status, 64)
    )?;
    if let Some(message) = status_message.filter(|m| !m.is_empty()) {
        write_safe_text(stdout, message)?;
        if !message.ends_with('\n') {
            stdout.write_all(b"\n")?;
        }
    }
    Ok(())
}

pub fn write_task_get(stdout: &mut impl Write, value: &Value) -> io::Result<()> {
    let task = value.get("task").filter(|v| v.is_object()).unwrap_or(value);
    let task_id = task.get("taskId").and_then(Value::as_str).unwrap_or("");
    let status = task.get("status").and_then(Value::as_str).unwrap_or("");
    let status_message = task.get("statusMessage").and_then(Value::as_str);
    write_task_status(stdout, task_id, status, status_message)?;

    match status {
        "completed" => {
            if let Some(result) = value.get("result").or_else(|| task.get("result")) {
                stdout.write_all(b"\n")?;
                if let Some(content) = result.get("content").and_then(Value::as_array) {
                    let texts: Vec<&str> = content
                        .iter()
                        .filter(|b| b.get("type").and_then(Value::as_str) == Some("text"))
                        .filter_map(|b| b.get("text").and_then(Value::as_str))
                        .collect();
                    if !texts.is_empty() {
                        write_text_parts(stdout, texts)?;
                        return Ok(());
                    }
                }
                write_text_json(stdout, result)?;
                stdout.write_all(b"\n")?;
            }
        }
        "failed" => {
            if let Some(error) = value.get("error").or_else(|| task.get("error")) {
                stdout.write_all(b"\n")?;
                write_text_json(stdout, error)?;
                stdout.write_all(b"\n")?;
            }
        }
        "input_required" => {
            if let Some(requests) = value
                .get("inputRequests")
                .or_else(|| task.get("inputRequests"))
            {
                stdout.write_all(b"\n")?;
                write_text_json(stdout, requests)?;
                stdout.write_all(b"\n")?;
            }
        }
        _ => {}
    }
    Ok(())
}

pub fn write_task_ack(stdout: &mut impl Write, task_id: &str) -> io::Result<()> {
    writeln!(stdout, "acknowledged {}", single_line(task_id, 512))
}

pub fn write_task_cancelled(stdout: &mut impl Write, task_id: &str) -> io::Result<()> {
    writeln!(stdout, "cancelled {}", single_line(task_id, 512))
}

fn single_line(text: &str, limit: usize) -> String {
    let mut result = String::new();
    let mut count = 0;
    let mut space = false;
    let mut push = |character: char| {
        if character.is_whitespace() {
            space = count > 0;
            return true;
        }
        if space {
            if count == limit {
                return false;
            }
            result.push(' ');
            count += 1;
            space = false;
        }
        if count == limit {
            return false;
        }
        result.push(character);
        count += 1;
        true
    };
    let mut characters = text.chars().peekable();
    let mut truncated = false;
    while let Some(character) = characters.next() {
        if character == '\r' && characters.peek() == Some(&'\n') {
            continue;
        }
        let complete = if character.is_control() && !matches!(character, '\n' | '\t') {
            character.escape_default().all(&mut push)
        } else {
            push(character)
        };
        if !complete {
            truncated = true;
            break;
        }
    }
    if truncated {
        result.push('…');
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn render_tool(tool: &Value) -> String {
        let mut bytes = Vec::new();
        write_tool(&mut bytes, tool).unwrap();
        String::from_utf8(bytes).unwrap()
    }

    #[test]
    fn inspect_preserves_exact_complex_schema_and_other_metadata() {
        let schema = json!({"type":"object","properties":{"fooBar":{"$ref":"#/$defs/item"},"foo_bar":{"type":["string","null"]}},"$defs":{"item":{"oneOf":[{"type":"integer"},{"type":"string"}]}}});
        let tool = json!({"name":"complex","description":"Works with complex values.","inputSchema":schema,"outputSchema":{"type":"object"},"annotations":{"readOnlyHint":true},"_meta":{"vendor":"value"}});
        let rendered = render_tool(&tool);
        assert!(rendered.contains(&serde_json::to_string_pretty(&schema).unwrap()));
        assert!(rendered.contains("outputSchema"));
        assert!(rendered.contains("readOnlyHint"));
        assert!(rendered.contains("vendor"));
        assert!(!rendered.contains("--foo-bar"));
    }

    #[test]
    fn bounded_name_rendering_preserves_whitespace_and_escape_truncation() {
        for name in [
            "  one\t two\r\n  three  ",
            "é\u{85}  😀\r",
            "one\u{a0}two",
            "",
            "  ",
        ] {
            let expected = safe_text(name)
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ");
            for limit in [0, 1, 5, 128] {
                let mut chars = expected.chars();
                let mut shortened: String = chars.by_ref().take(limit).collect();
                if chars.next().is_some() {
                    shortened.push('…');
                }
                assert_eq!(single_line(name, limit), shortened);
            }
        }
        assert_eq!(
            single_line(&"name".repeat(64 << 10), 128).chars().count(),
            129
        );
    }

    #[test]
    fn safe_text_preserves_crlf_lines_while_escaping_bare_cr() {
        assert_eq!(safe_text("one\r\ntwo\r\n"), "one\ntwo\n");
        assert_eq!(safe_text("spoof\roverwrite"), "spoof\\roverwrite");
        assert_eq!(safe_text("mixed\r\r\nline"), "mixed\\r\nline");
    }

    #[test]
    fn summary_bounds_work_before_normalizing_long_descriptions() {
        assert_eq!(
            summary_description(&format!("word{}\nignored", " ".repeat(64 << 10))),
            Some("word".into())
        );
        assert_eq!(
            summary_description(&format!("word{}end", " ".repeat(64 << 10))),
            Some(format!("word{}…", " ".repeat(252)))
        );
        assert_eq!(summary_description("\u{85}\nnext"), Some("\\u{85}".into()));
        assert_eq!(
            summary_description("\r\n\t \nlast\r"),
            Some("last\\r".into())
        );
        assert_eq!(
            summary_description(&"😀".repeat(64 << 10))
                .unwrap()
                .chars()
                .count(),
            257
        );
    }

    #[test]
    fn summarize_tool_projects_first_non_empty_line_and_annotations() {
        let long_line = "😀".repeat(260);
        let tool = json!({
            "name": "search_docs",
            "description": format!("\n  \r\n  {long_line}\nSecond line ignored"),
            "inputSchema": {"type": "object"},
            "annotations": {"readOnlyHint": true, "openWorldHint": false},
            "_meta": {"internal": 1}
        });
        let summary = summarize_tool(tool);
        assert_eq!(summary["name"], "search_docs");
        let desc = summary["description"].as_str().unwrap();
        assert_eq!(desc.chars().count(), 257);
        assert!(desc.ends_with('…'));
        assert_eq!(
            summary["annotations"],
            json!({"readOnlyHint": true, "openWorldHint": false})
        );
        assert!(summary.get("inputSchema").is_none());
        assert!(summary.get("_meta").is_none());

        let blank_desc = summarize_tool(json!({
            "name": "noop",
            "description": "  \r\n\t  ",
            "inputSchema": {}
        }));
        assert_eq!(blank_desc, json!({"name": "noop"}));

        let control_desc = summarize_tool(json!({
            "name": "ctrl",
            "description": "first\u{1b}[31mred\rnext\nsecond"
        }));
        assert_eq!(control_desc["description"], "first\\u{1b}[31mred\\rnext");
    }

    #[test]
    fn builtin_skill_guide_has_frontmatter_version_and_core_commands() {
        assert!(BUILTIN_SKILL_MD.starts_with("---\nname: mcpcli\n"));
        assert!(BUILTIN_SKILL_MD.contains(BUILTIN_SKILL_VERSION));
        for needle in [
            "catalog",
            "tools <server> --summary",
            "--filter",
            "inspect <server> <tool> --schema",
            "KEY:=VALUE",
            "--detach",
            "tasks get",
            "tasks update",
            "tasks cancel",
            "skills list --server",
            "| `0` |",
            "| `6` |",
        ] {
            assert!(BUILTIN_SKILL_MD.contains(needle), "missing {needle}");
        }
        assert!(!BUILTIN_SKILL_MD.contains('—'));
        assert!(!BUILTIN_SKILL_MD.contains('–'));
    }

    #[test]
    fn remote_skill_and_task_text_renderers_escape_control_chars() {
        let mut out = Vec::new();
        write_remote_skills_list(
            &mut out,
            &[json!({
                "name": "demo\u{1b}[31m",
                "uri": "skill://demo/SKILL.md\r",
                "description": "first\u{1b}[0m line\nsecond"
            })],
        )
        .unwrap();
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "demo\\u{1b}[31m\tskill://demo/SKILL.md\\r\tfirst\\u{1b}[0m line\n"
        );

        let mut out = Vec::new();
        write_remote_skill_show(
            &mut out,
            "srv",
            "skill://demo/SKILL.md",
            true,
            "body\u{1b}[31m\r\nnext",
        )
        .unwrap();
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "Origin: srv (skill://demo/SKILL.md) [dynamic, unverified]\n\nbody\\u{1b}[31m\nnext\n"
        );

        let mut out = Vec::new();
        write_remote_skill_files(
            &mut out,
            &[json!({"path": "refs/a.md", "size": 42, "digest": "sha256:abc"})],
        )
        .unwrap();
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "refs/a.md\t42\tsha256:abc\n"
        );

        let mut out = Vec::new();
        write_task_get(
            &mut out,
            &json!({
                "taskId": "t-1",
                "status": "failed",
                "statusMessage": "boom\u{1b}[31m",
                "error": {"code": -32000, "message": "err\u{1b}[0m\u{85}"}
            }),
        )
        .unwrap();
        let rendered = String::from_utf8(out).unwrap();
        assert!(rendered.starts_with("task t-1 [failed]\nboom\\u{1b}[31m\n\n"));
        assert!(!rendered.contains('\u{1b}'));
        assert!(rendered.contains("\\u001b[0m\\u{85}"));

        let mut out = Vec::new();
        write_task_ack(&mut out, "t-1").unwrap();
        write_task_cancelled(&mut out, "t-2").unwrap();
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "acknowledged t-1\ncancelled t-2\n"
        );
    }
}
