use super::args::usage;
use anyhow::{Context, Result};
use mcpcli::{
    budget::{ByteBudget, Leased},
    discovery::{self, write_safe_text, write_text_json},
    ipc::Failure,
    mcp::McpConnection,
    media::InflightBatch,
};
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use std::io::Write;

pub(crate) struct Output {
    pub(crate) value: Option<Value>,
    pub(crate) text: Option<String>,
    pub(crate) code: i32,
    pub(crate) diagnostic: Option<Failure>,
    pub(crate) connection: Option<McpConnection>,
    pub(crate) inflight: Option<InflightBatch>,
    pub(crate) lease: Option<mcpcli::budget::ByteLease>,
}
impl Output {
    pub(crate) fn json(value: Value) -> Self {
        Self {
            value: Some(value),
            text: None,
            code: 0,
            diagnostic: None,
            connection: None,
            inflight: None,
            lease: None,
        }
    }
    pub(crate) fn with_text(value: Value, text: String) -> Self {
        Self {
            value: Some(value),
            text: Some(text),
            code: 0,
            diagnostic: None,
            connection: None,
            inflight: None,
            lease: None,
        }
    }
    pub(crate) fn failure(error: anyhow::Error) -> Self {
        let failure = error
            .downcast::<Failure>()
            .unwrap_or_else(|error| Failure::new(1, "runtime", format!("{error:#}")));
        Self {
            value: None,
            text: None,
            code: failure.code,
            diagnostic: Some(failure),
            connection: None,
            inflight: None,
            lease: None,
        }
    }
    pub(crate) fn with_connection(mut self, connection: Option<McpConnection>) -> Self {
        self.connection = connection;
        self
    }
    pub(crate) fn with_inflight(mut self, inflight: Option<InflightBatch>) -> Self {
        self.inflight = inflight;
        self
    }
    pub(crate) fn with_lease(mut self, lease: Option<mcpcli::budget::ByteLease>) -> Self {
        match (&mut self.lease, lease) {
            (Some(current), Some(extra)) => current.merge(extra),
            (None, lease) => self.lease = lease,
            _ => {}
        }
        self
    }
    pub(crate) fn cleanup(mut self, error: Option<Failure>) -> Self {
        if let Some(error) = error {
            if let Some(original) = &mut self.diagnostic {
                original.message = format!("{}; additionally: {}", original.message, error.message);
            } else {
                if self.code == 0 {
                    self.code = error.code;
                }
                self.diagnostic = Some(error);
            }
        }
        self
    }
}

pub(crate) struct RemoteOutput {
    pub(crate) value: Value,
    pub(crate) connection: Option<McpConnection>,
    pub(crate) inflight: Option<InflightBatch>,
    pub(crate) lease: Option<mcpcli::budget::ByteLease>,
}
impl RemoteOutput {
    pub(crate) fn into_output(self, render: impl FnOnce(Value) -> Result<Output>) -> Output {
        render(self.value)
            .unwrap_or_else(Output::failure)
            .with_connection(self.connection)
            .with_inflight(self.inflight)
            .with_lease(self.lease)
    }
}

pub(crate) fn brief(value: &str) -> String {
    let line = value.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut chars = line.chars();
    let mut result: String = chars.by_ref().take(240).collect();
    if chars.next().is_some() {
        result.push('…');
    }
    result
}

pub(crate) struct ServerView<'a> {
    root: &'a Map<String, Value>,
    result: &'a Map<String, Value>,
}

impl serde::Serialize for ServerView<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(self.root.len()))?;
        for (key, value) in self.root {
            if key == "result" {
                map.serialize_entry(
                    key,
                    &discovery::ObjectWithout(self.result, &["instructions"]),
                )?;
            } else {
                map.serialize_entry(key, value)?;
            }
        }
        map.end()
    }
}

pub(crate) fn server_view(value: &Value) -> Result<ServerView<'_>> {
    let root = value.as_object().context("invalid server information")?;
    let result = root
        .get("result")
        .and_then(Value::as_object)
        .context("invalid MCP server result")?;
    Ok(ServerView { root, result })
}

pub(crate) fn write_server_text(stdout: &mut impl Write, value: &Value) -> std::io::Result<()> {
    let view = server_view(value).map_err(std::io::Error::other)?;
    write_text_json(stdout, &view)?;
    if let Some(instructions) = view.result.get("instructions").and_then(Value::as_str) {
        stdout.write_all(b"\n\nInstructions:\n")?;
        write_safe_text(stdout, instructions)?;
        if !instructions.is_empty() && !instructions.ends_with('\n') {
            stdout.write_all(b"\n")?;
        }
    } else {
        stdout.write_all(b"\n")?;
    }
    Ok(())
}

pub(crate) fn listed_tools(value: &Value) -> Result<impl Iterator<Item = &Value>> {
    let pages = value["pages"]
        .as_array()
        .context("invalid tool list pages")?;
    anyhow::ensure!(
        pages.iter().all(|page| page["tools"].is_array()),
        "invalid tool list page"
    );
    Ok(pages
        .iter()
        .flat_map(|page| page["tools"].as_array().unwrap()))
}

pub(crate) fn write_tools_text(stdout: &mut impl Write, value: &Value) -> std::io::Result<()> {
    fn render_tools<'a>(
        stdout: &mut impl Write,
        tools: impl Iterator<Item = &'a Value>,
        first_line: bool,
    ) -> std::io::Result<()> {
        let parts = tools.enumerate().flat_map(|(index, tool)| {
            let name = tool["name"].as_str().unwrap_or("<unnamed>");
            let description = tool["description"]
                .as_str()
                .filter(|description| !description.is_empty())
                .and_then(|description| {
                    if first_line {
                        description
                            .lines()
                            .map(str::trim)
                            .find(|line| !line.is_empty())
                    } else {
                        Some(description)
                    }
                });
            [
                (index > 0).then_some("\n\n"),
                Some(name),
                description.map(|_| "\n"),
                description,
            ]
            .into_iter()
            .flatten()
        });
        discovery::write_text_parts(stdout, parts)
    }

    if let Some(tools) = value.get("tools").and_then(Value::as_array) {
        render_tools(stdout, tools.iter(), true)
    } else {
        let tools = listed_tools(value).map_err(std::io::Error::other)?;
        render_tools(stdout, tools, false)
    }
}

pub(crate) fn project_tools(
    mut value: Value,
    filter: Option<&str>,
    summary: bool,
    budget: &std::sync::Arc<ByteBudget>,
) -> Result<Leased<Value>> {
    let _ = listed_tools(&value)?;
    let mut projection_lease = budget.empty_lease();
    let needle = filter.map(str::to_lowercase);
    let mut selected = Vec::new();
    let Value::Array(pages) = value["pages"].take() else {
        unreachable!("validated pages")
    };
    for mut page in pages {
        let Value::Array(tools) = page["tools"].take() else {
            unreachable!("validated tools")
        };
        for tool in tools {
            let mut matched = needle.is_none();
            if let Some(needle) = &needle {
                for field in ["name", "description"] {
                    if let Some(text) = tool.get(field).and_then(Value::as_str) {
                        // Cover UTF-8 lowercasing and its allocation growth before copying.
                        let _scratch = budget
                            .try_acquire(text.len().saturating_mul(4))
                            .ok_or_else(|| {
                                Failure::new(
                                    1,
                                    "memory_limit_exceeded",
                                    "Tool filtering exceeds the active memory limit",
                                )
                            })?;
                        if text.to_lowercase().contains(needle) {
                            matched = true;
                            break;
                        }
                    }
                }
            }
            if matched {
                let tool = if summary {
                    let mut description_lease =
                        budget.try_acquire(2 * 4 * 257).ok_or_else(|| {
                            Failure::new(
                                1,
                                "memory_limit_exceeded",
                                "Tool summary exceeds the active memory limit",
                            )
                        })?;
                    let tool = discovery::summarize_tool(tool);
                    let bytes = match tool.get("description") {
                        Some(Value::String(description)) => description.capacity(),
                        _ => 0,
                    };
                    description_lease.shrink_to(bytes);
                    projection_lease.merge(description_lease);
                    tool
                } else {
                    tool
                };
                selected.push(tool);
            }
        }
    }
    let mut output = Map::new();
    output.insert("tools".into(), Value::Array(selected));
    Ok(Leased::new(Value::Object(output), projection_lease))
}

pub(crate) fn take_listed_tool(value: &mut Value, name: &str) -> Result<Value> {
    let _ = listed_tools(value)?;
    for page in value["pages"].as_array_mut().expect("validated pages") {
        for tool in page["tools"].as_array_mut().expect("validated tools") {
            if tool["name"].as_str() == Some(name) {
                return Ok(tool.take());
            }
        }
    }
    Err(usage("unknown_tool", format!("unknown tool {name:?}")))
}

pub(crate) fn write_tool_text(
    stdout: &mut impl Write,
    result: &Value,
    inline_media: bool,
) -> std::io::Result<()> {
    let mut saved_by_index = BTreeMap::new();
    if !inline_media && let Some(entries) = result.get("savedMedia").and_then(Value::as_array) {
        for entry in entries {
            if let Some(index) = entry.get("contentIndex").and_then(Value::as_u64) {
                saved_by_index.insert(index as usize, entry);
            }
        }
    }
    let remote = if inline_media {
        result
    } else {
        &result["result"]
    };
    enum Part<'a> {
        Text(&'a str),
        ResourceText {
            uri: &'a str,
            mime: Option<&'a str>,
            text: &'a str,
        },
        ResourceLink {
            name: Option<&'a str>,
            mime: Option<&'a str>,
            uri: &'a str,
        },
        Saved(&'a Value),
    }
    let mut omitted = 0;
    let mut emitted = false;
    let mut ends_with_lf = false;
    let content = remote
        .get("content")
        .and_then(Value::as_array)
        .map_or(&[][..], Vec::as_slice);
    let mut parts = content
        .iter()
        .enumerate()
        .filter_map(|(index, block)| {
            if block["type"] == "text" {
                block["text"]
                    .as_str()
                    .filter(|text| !text.is_empty())
                    .map(Part::Text)
            } else if let Some(saved) = saved_by_index.get(&index) {
                Some(Part::Saved(saved))
            } else if block["type"] == "resource"
                && let Some(res) = block.get("resource").and_then(Value::as_object)
                && res.get("blob").is_none_or(Value::is_null)
                && let Some(text) = res.get("text").and_then(Value::as_str)
            {
                let uri = res.get("uri").and_then(Value::as_str).unwrap_or("");
                let mime = res
                    .get("mimeType")
                    .and_then(Value::as_str)
                    .filter(|m| !m.is_empty());
                if uri.is_empty() && text.is_empty() {
                    None
                } else {
                    Some(Part::ResourceText { uri, mime, text })
                }
            } else if block["type"] == "resource_link"
                && let Some(uri) = block
                    .get("uri")
                    .and_then(Value::as_str)
                    .filter(|u| !u.is_empty())
            {
                let name = block
                    .get("name")
                    .and_then(Value::as_str)
                    .filter(|n| !n.is_empty());
                let mime = block
                    .get("mimeType")
                    .and_then(Value::as_str)
                    .filter(|m| !m.is_empty());
                Some(Part::ResourceLink { name, mime, uri })
            } else {
                omitted += 1;
                None
            }
        })
        .peekable();
    while let Some(part) = parts.next() {
        if emitted {
            stdout.write_all(b"\n")?;
        }
        emitted = true;
        let followed_by_lf = parts.peek().is_some();
        let tail = match part {
            Part::Text(text) => text,
            Part::ResourceText { uri, mime, text } => {
                if !uri.is_empty() {
                    stdout.write_all(b"Resource ")?;
                    if let Some(mime) = mime {
                        stdout.write_all(b"[")?;
                        write_safe_text(stdout, mime)?;
                        stdout.write_all(b"] ")?;
                    }
                    write_safe_text(stdout, uri)?;
                    if !text.is_empty() {
                        stdout.write_all(b":\n")?;
                        text
                    } else {
                        ""
                    }
                } else {
                    text
                }
            }
            Part::ResourceLink { name, mime, uri } => {
                stdout.write_all(b"Resource link")?;
                if let Some(mime) = mime {
                    stdout.write_all(b" [")?;
                    write_safe_text(stdout, mime)?;
                    stdout.write_all(b"]")?;
                }
                if let Some(name) = name {
                    stdout.write_all(b" ")?;
                    write_safe_text(stdout, name)?;
                }
                stdout.write_all(b": ")?;
                uri
            }
            Part::Saved(saved) => {
                let kind = saved["kind"].as_str().unwrap_or("media");
                let mime = saved["mimeType"]
                    .as_str()
                    .unwrap_or("application/octet-stream");
                let bytes = saved["bytes"].as_u64().unwrap_or(0);
                stdout.write_all(b"Saved ")?;
                write_safe_text(stdout, kind)?;
                stdout.write_all(b" [")?;
                write_safe_text(stdout, mime)?;
                write!(stdout, "] ({bytes} B): ")?;
                saved["savedPath"].as_str().unwrap_or("")
            }
        };
        // Joining text blocks inserts LF. Preserve CRLF normalization even when
        // the CR belongs to the preceding block and the LF is the separator.
        let tail = if followed_by_lf {
            tail.strip_suffix('\r').unwrap_or(tail)
        } else {
            tail
        };
        write_safe_text(stdout, tail)?;
        ends_with_lf = tail.ends_with('\n');
    }
    if omitted > 0 {
        eprintln!(
            "mcpcli: {omitted} non-text content block(s) are available in default JSON output"
        );
    }
    if let Some(structured) = remote.get("structuredContent") {
        if !emitted {
            write_text_json(stdout, structured)?;
            emitted = true;
            ends_with_lf = false;
        } else {
            eprintln!("mcpcli: structuredContent is available in default JSON output");
        }
    }
    if emitted && !ends_with_lf {
        stdout.write_all(b"\n")?;
    }
    Ok(())
}

/// Terminal boundary for text-mode stdout.
pub(crate) fn write_text(stdout: &mut impl Write, text: &str) -> std::io::Result<()> {
    write_safe_text(stdout, text)?;
    if !text.is_empty() && !text.ends_with('\n') {
        stdout.write_all(b"\n")?;
    }
    Ok(())
}

pub(crate) fn write_diagnostic(stderr: &mut impl Write, error: &Failure) -> std::io::Result<()> {
    #[derive(serde::Serialize)]
    struct Diagnostic<'a> {
        error: &'a Failure,
    }
    serde_json::to_writer(&mut *stderr, &Diagnostic { error }).map_err(std::io::Error::other)?;
    writeln!(stderr)
}
