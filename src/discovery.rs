use serde_json::Value;

/// Inspect the exact schemas rather than synthesizing an incomplete flag interface.
pub fn render_tool(tool: &Value) -> String {
    let Some(object) = tool.as_object() else {
        return serde_json::to_string_pretty(tool).unwrap_or_default();
    };
    let mut sections = vec![single_line(
        object
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("<unnamed>"),
        128,
    )];
    if let Some(description) = object.get("description").and_then(Value::as_str) {
        sections.push(safe_text(description));
    }
    if let Some(schema) = object.get("inputSchema") {
        sections.push(format!(
            "Input schema:\n{}",
            serde_json::to_string_pretty(schema).unwrap()
        ));
    }
    let mut metadata = object.clone();
    for field in ["name", "description", "inputSchema"] {
        metadata.remove(field);
    }
    if !metadata.is_empty() {
        sections.push(format!(
            "Metadata:\n{}",
            serde_json::to_string_pretty(&metadata).unwrap()
        ));
    }
    sections.join("\n\n")
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

fn single_line(text: &str, limit: usize) -> String {
    let normalized = safe_text(text)
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    let mut characters = normalized.chars();
    let mut shortened: String = characters.by_ref().take(limit).collect();
    if characters.next().is_some() {
        shortened.push('…');
    }
    shortened
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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
    fn safe_text_preserves_crlf_lines_while_escaping_bare_cr() {
        assert_eq!(safe_text("one\r\ntwo\r\n"), "one\ntwo\n");
        assert_eq!(safe_text("spoof\roverwrite"), "spoof\\roverwrite");
        assert_eq!(safe_text("mixed\r\r\nline"), "mixed\\r\nline");
    }
}
