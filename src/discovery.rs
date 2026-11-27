use serde::ser::SerializeMap;
use serde_json::{Map, Value};
use std::io::{self, Write};

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
}
