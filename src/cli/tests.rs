use super::args::{
    AuthCommand, Cli, CliExecMode, CliStreamMode, Command, EventsCommand, TopLevelHelpProbe,
    validate_call_max_timeout,
};
use super::input::{arguments, read_argument_bytes, retain_cli_input};
use super::offline::catalog;
use super::output::{
    Output, RemoteOutput, project_tools, take_listed_tool, write_diagnostic, write_server_text,
    write_text, write_tool_text, write_tools_text,
};
use super::remote::check_dispatch_budgets;
use super::run;
use crate::entry;
use anyhow::Result;
use clap::{CommandFactory, Parser};
use mcpcli::{
    budget::ByteBudget,
    config::LoadedConfig,
    discovery::{self, safe_text, write_text_json},
    ipc::Failure,
    json as input,
};
use serde_json::{Value, json};
use std::io::Write;
use std::sync::atomic::AtomicBool;
use std::time::Duration;
use tokio::time::timeout;

fn server_text(value: &Value) -> Result<String> {
    let mut bytes = Vec::new();
    write_server_text(&mut bytes, value)?;
    Ok(String::from_utf8(bytes)?)
}

fn tools_text(value: &Value) -> Result<String> {
    let mut bytes = Vec::new();
    write_tools_text(&mut bytes, value)?;
    Ok(String::from_utf8(bytes)?)
}

fn tool_text(result: &Value, inline_media: bool) -> String {
    let mut output = Vec::new();
    write_tool_text(&mut output, result, inline_media).unwrap();
    String::from_utf8(output).unwrap()
}

#[test]
fn tool_text_streams_large_borrowed_spans_and_normalizes_joined_crlf() {
    struct BorrowedWriter {
        start: usize,
        end: usize,
        borrowed: usize,
        written: usize,
    }
    impl Write for BorrowedWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > 1024 {
                let ptr = bytes.as_ptr() as usize;
                assert!(
                    ptr >= self.start && ptr + bytes.len() <= self.end,
                    "large text was copied"
                );
                self.borrowed += bytes.len();
            }
            self.written += bytes.len();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let value = json!({"content": [{"type":"text", "text":"x".repeat(128 << 10)}]});
    let text = value["content"][0]["text"].as_str().unwrap();
    let mut writer = BorrowedWriter {
        start: text.as_ptr() as usize,
        end: text.as_ptr() as usize + text.len(),
        borrowed: 0,
        written: 0,
    };
    write_tool_text(&mut writer, &value, true).unwrap();
    assert_eq!(writer.borrowed, text.len());
    assert_eq!(writer.written, text.len() + 1);
    assert_eq!(
        tool_text(
            &json!({"content": [
                {"type":"text", "text":"first\r"}, {"type":"text", "text":"second\r\n"}
            ]}),
            true
        ),
        "first\nsecond\n"
    );
}

#[test]
fn diagnostic_streams_shared_rpc_error_without_copying_its_value() {
    struct BorrowedWriter {
        start: usize,
        end: usize,
        borrowed: usize,
    }
    impl Write for BorrowedWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > 1024 {
                let ptr = bytes.as_ptr() as usize;
                assert!(
                    ptr >= self.start && ptr + bytes.len() <= self.end,
                    "large diagnostic was copied"
                );
                self.borrowed += bytes.len();
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut error = Failure::new(4, "mcp_protocol", "failed");
    error.rpc_error = Some(std::sync::Arc::new(
        json!({"data":{"text":"x".repeat(128 << 10)}}),
    ));
    let text = error.rpc_error.as_ref().unwrap()["data"]["text"]
        .as_str()
        .unwrap();
    let mut writer = BorrowedWriter {
        start: text.as_ptr() as usize,
        end: text.as_ptr() as usize + text.len(),
        borrowed: 0,
    };
    write_diagnostic(&mut writer, &error).unwrap();
    assert_eq!(writer.borrowed, text.len());
}

#[test]
fn structured_and_fallback_text_streams_escape_c1_without_changing_json() {
    let structured = json!({"key\u{85}": "normal é😀\u{85}\u{9b}\n"});
    let expected = safe_text(&serde_json::to_string_pretty(&structured).unwrap());
    assert_eq!(
        tool_text(&json!({"structuredContent": structured}), true),
        format!("{expected}\n")
    );
    let mut bytes = Vec::new();
    write_text_json(&mut bytes, &structured).unwrap();
    assert_eq!(String::from_utf8(bytes).unwrap(), expected);
    let json = serde_json::to_vec(&structured).unwrap();
    assert_eq!(serde_json::from_slice::<Value>(&json).unwrap(), structured);
    assert_eq!(structured["key\u{85}"], "normal é😀\u{85}\u{9b}\n");
}

#[test]
fn empty_tool_text_blocks_preserve_structured_output_without_blank_lines() {
    for count in [1, 2] {
        let mut result = json!({
            "content": vec![json!({"type": "text", "text": ""}); count]
        });
        assert_eq!(tool_text(&result, true), "");
        result["structuredContent"] = json!({"answer": 42});
        assert_eq!(
            tool_text(&result, true),
            format!(
                "{}\n",
                serde_json::to_string_pretty(&result["structuredContent"]).unwrap()
            )
        );
    }
    assert_eq!(
        tool_text(&json!({"content": [{"type": "text", "text": " \n"}]}), true),
        " \n"
    );
}

#[test]
fn tool_text_renders_saved_media_summary_and_ignores_remote_field_when_inline() {
    let saved = json!({
        "content": [
            {"type": "text", "text": "Read video"},
            {"type": "resource", "resource": {"uri": "file:///x.mp4", "mimeType": "video/mp4"}}
        ],
        "savedMedia": [{
            "contentIndex": 1,
            "kind": "resource",
            "savedPath": "/tmp/media/abc.mp4",
            "mimeType": "video/mp4",
            "bytes": 1234
        }]
    });
    let local = json!({"result": saved, "savedMedia": saved["savedMedia"]});
    assert_eq!(
        tool_text(&local, false),
        "Read video\nSaved resource [video/mp4] (1234 B): /tmp/media/abc.mp4\n"
    );
    // When --inline-media is set, any remote savedMedia field is ignored.
    assert_eq!(tool_text(&saved, true), "Read video\n");
}

#[test]
fn tool_text_renders_inline_text_resources_and_resource_links_without_omitted_warning() {
    let payload = json!({
        "content": [
            {"type": "text", "text": "Header"},
            {"type": "resource", "resource": {
                "uri": "file:///notes/todo.txt",
                "mimeType": "text/plain",
                "text": "line 1\r\nline 2"
            }},
            {"type": "resource_link", "uri": "https://example.test/spec.md", "name": "spec.md", "mimeType": "text/markdown"}
        ]
    });
    assert_eq!(
        tool_text(&payload, true),
        "Header\nResource [text/plain] file:///notes/todo.txt:\nline 1\nline 2\nResource link [text/markdown] spec.md: https://example.test/spec.md\n"
    );

    let auth_login = Cli::try_parse_from([
        "mcpcli",
        "auth",
        "login",
        "remote",
        "--scope",
        "mcp:read",
        "--scope",
        "mcp:write",
        "--client-id",
        "my-client",
        "--callback-port",
        "8765",
        "--callback-path",
        "/oauth/cb",
        "--client-name",
        "my-cli",
        "--no-browser",
    ])
    .unwrap();
    assert!(matches!(
        auth_login.command,
        Command::Auth {
            command: AuthCommand::Login {
                ref server,
                ref scopes,
                ref client_id,
                callback_port: Some(8765),
                ref callback_path,
                ref client_name,
                no_browser: true,
            }
        } if server == "remote"
            && scopes == &["mcp:read", "mcp:write"]
            && client_id.as_deref() == Some("my-client")
            && callback_path.as_deref() == Some("/oauth/cb")
            && client_name.as_deref() == Some("my-cli")
    ));
    assert!(
        Cli::try_parse_from(["mcpcli", "auth", "login", "remote", "--callback-port", "0"]).is_err()
    );
    assert!(Cli::try_parse_from(["mcpcli", "auth", "status"]).is_ok());
    assert!(Cli::try_parse_from(["mcpcli", "auth", "status", "remote"]).is_ok());
    assert!(Cli::try_parse_from(["mcpcli", "auth", "logout", "remote"]).is_ok());
}

#[test]
fn server_text_preserves_full_instruction_body() {
    let value = json!({
        "source":"initialize",
        "protocolVersion":"2025-11-25",
        "result":{
            "serverInfo":{"name":"sample","version":"1.0"},
            "capabilities":{"tools":{}},
            "instructions":"    first line\nsecond line  \n\n"
        }
    });
    let rendered = server_text(&value).unwrap();
    assert!(rendered.contains("\"capabilities\""));
    assert!(rendered.ends_with("\n\nInstructions:\n    first line\nsecond line  \n\n"));
    assert_eq!(
        value["result"]["instructions"],
        "    first line\nsecond line  \n\n"
    );
}

#[test]
fn tools_text_keeps_multiline_and_long_descriptions() {
    let description = format!("    first\n{}  ", "x".repeat(300));
    let tools =
        json!({"pages":[{"tools":[{"name":"sample","description":description,"inputSchema":{}}]}]});
    let rendered = tools_text(&tools).unwrap();
    assert_eq!(rendered, format!("sample\n{description}\n"));
}

#[test]
fn text_stdout_sanitizes_server_tools_and_call_results_without_changing_json() {
    let raw = " \tindented  \nnext\r\nbare\r\u{1b}\u{9b}\u{0}  \n\n";
    let server = json!({"result":{"instructions":raw}});
    let tools = json!({"pages":[{"tools":[{"name":"sample","description":raw}]}]});
    let call = json!({"content":[{"type":"text","text":raw}]});
    for rendered in [
        server_text(&server).unwrap(),
        tools_text(&tools).unwrap(),
        tool_text(&call, true),
    ] {
        let mut bytes = Vec::new();
        write_text(&mut bytes, &rendered).unwrap();
        let text = String::from_utf8(bytes).unwrap();
        assert!(text.ends_with(" \tindented  \nnext\nbare\\r\\u{1b}\\u{9b}\\u{0}  \n\n"));
        assert!(
            !text
                .chars()
                .any(|c| c.is_control() && !matches!(c, '\n' | '\t'))
        );
    }
    assert_eq!(server["result"]["instructions"], raw);
    assert_eq!(tools["pages"][0]["tools"][0]["description"], raw);
    assert_eq!(call["content"][0]["text"], raw);
}

#[tokio::test]
async fn argument_reads_and_parse_retain_the_source_value_owner() {
    let input = serde_json::to_string(&json!({"text":"ordinary ".repeat(256) + "\n"})).unwrap();
    let peak = input.len() * 2 + input::scratch_bytes(input.as_bytes());
    for limit in [peak - 1, peak] {
        let budget = ByteBudget::new(limit);
        let result = arguments(
            &Some(input.clone()),
            &None,
            false,
            Duration::from_secs(1),
            &budget,
        )
        .await;
        if limit < peak {
            assert_eq!(
                result.unwrap_err().downcast_ref::<Failure>().unwrap().kind,
                "memory_limit_exceeded"
            );
        } else {
            let arguments = result.unwrap();
            assert_eq!(arguments["text"], "ordinary ".repeat(256) + "\n");
            assert_eq!(budget.used(), input.len());
            drop(arguments);
        }
        assert_eq!(budget.used(), 0);
    }
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("arguments.json");
    std::fs::write(&file, &input).unwrap();
    let budget = ByteBudget::new(peak);
    let result = arguments(&None, &Some(file), false, Duration::from_secs(1), &budget)
        .await
        .unwrap();
    assert_eq!(budget.used(), input.len());
    drop(result);
    assert_eq!(budget.used(), 0);

    let budget = ByteBudget::new(64);
    let (mut sender, reader) = tokio::io::duplex(64);
    tokio::io::AsyncWriteExt::write_all(&mut sender, b"part")
        .await
        .unwrap();
    let mut reading = Box::pin(read_argument_bytes(reader, &budget));
    assert!(
        timeout(Duration::from_millis(10), &mut reading)
            .await
            .is_err()
    );
    assert_eq!(budget.used(), 4);
    drop(reading);
    assert_eq!(budget.used(), 0);
}

#[test]
fn noncall_text_borrows_payloads_and_inspect_moves_the_selected_value() {
    struct BorrowedWriter {
        ranges: Vec<std::ops::Range<usize>>,
        borrowed: usize,
    }
    impl Write for BorrowedWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > 1024 {
                let start = bytes.as_ptr() as usize;
                assert!(
                    self.ranges
                        .iter()
                        .any(|range| range.start <= start && start + bytes.len() <= range.end),
                    "large text was copied"
                );
                self.borrowed += bytes.len();
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    fn ranges(value: &Value, output: &mut Vec<std::ops::Range<usize>>) {
        match value {
            Value::String(text) if text.len() > 1024 => {
                let start = text.as_ptr() as usize;
                output.push(start..start + text.len());
            }
            Value::Array(items) => {
                for item in items {
                    ranges(item, output);
                }
            }
            Value::Object(fields) => {
                for value in fields.values() {
                    ranges(value, output);
                }
            }
            _ => {}
        }
    }
    let server =
        json!({"result":{"instructions":"i".repeat(128 << 10), "extra":"m".repeat(128 << 10)}});
    let tools =
        json!({"pages":[{"tools":[{"name":"sample", "description":"d".repeat(128 << 10)}]}]});
    let tool = json!({"name":"sample", "description":"d".repeat(128 << 10),
        "inputSchema":{"description":"s".repeat(128 << 10)}, "_meta":{"long":"m".repeat(128 << 10)}});
    for (kind, value) in [(0, &server), (1, &tools), (2, &tool)] {
        let mut writer = BorrowedWriter {
            ranges: Vec::new(),
            borrowed: 0,
        };
        ranges(value, &mut writer.ranges);
        let expected: usize = writer.ranges.iter().map(|range| range.len()).sum();
        match kind {
            0 => write_server_text(&mut writer, value).unwrap(),
            1 => write_tools_text(&mut writer, value).unwrap(),
            _ => discovery::write_tool(&mut writer, value).unwrap(),
        }
        assert_eq!(writer.borrowed, expected);
    }
    let mut pages = json!({"pages":[{"tools":[{"name":"other"}, tool]}]});
    let pointer = pages["pages"][0]["tools"][1]["description"]
        .as_str()
        .unwrap()
        .as_ptr();
    let selected = take_listed_tool(&mut pages, "sample").unwrap();
    assert_eq!(selected["description"].as_str().unwrap().as_ptr(), pointer);
    assert!(pages["pages"][0]["tools"][1].is_null());
    assert_eq!(selected["_meta"]["long"].as_str().unwrap().len(), 128 << 10);
}

#[test]
fn noncall_text_keeps_control_and_separator_semantics() {
    let tools = json!({"pages":[{"tools":[
        {"name":"first\r","description":"body\u{85}é\r"},
        {"name":"next"}
    ]}]});
    assert_eq!(tools_text(&tools).unwrap(), "first\nbody\\u{85}é\n\nnext\n");
    assert_eq!(
        tools_text(&json!({"pages":[{"tools":[{"name":""},{"name":""}]}]})).unwrap(),
        "\n\n"
    );
    assert_eq!(tools_text(&json!({"pages":[{"tools":[]}]})).unwrap(), "");
    let server = json!({"result":{"instructions":"","extra":"é\u{85}"}});
    assert!(
        server_text(&server)
            .unwrap()
            .ends_with("\n\nInstructions:\n")
    );
    assert!(server_text(&server).unwrap().contains("é\\u{85}"));
    assert_eq!(server["result"]["extra"], "é\u{85}");
    let tool = json!({"name":"  name\tpart ", "description":"description\r",
        "inputSchema":{"title":"é\u{9b}"}, "extra":null});
    let mut text = Vec::new();
    discovery::write_tool(&mut text, &tool).unwrap();
    let text = String::from_utf8(text).unwrap();
    assert!(text.starts_with("name part\n\ndescription\\r\n\nInput schema:\n"));
    assert!(text.contains("é\\u{9b}"));
    assert!(text.contains("\"extra\": null"));
    assert_eq!(tool["inputSchema"]["title"], "é\u{9b}");
}

#[tokio::test]
async fn cli_argument_string_capacity_survives_reader_and_value_handoffs() {
    for exact in [false, true] {
        let mut cli = Cli::try_parse_from(["mcpcli", "call", "srv", "sample"]).unwrap();
        let mut text = String::with_capacity(512);
        text.push_str("{\"text\":\"ordinary\\ntext\"}");
        let capacity = text.capacity();
        let length = text.len();
        let peak = capacity + 2 * length + input::scratch_bytes(text.as_bytes());
        let Command::Call { args, .. } = &mut cli.command else {
            unreachable!()
        };
        *args = Some(text);
        let budget = ByteBudget::new(if exact { peak } else { peak - 1 });
        let cli = retain_cli_input(cli, &budget).unwrap();
        assert_eq!(budget.used(), capacity);
        let Command::Call { args, .. } = &cli.command else {
            unreachable!()
        };
        let parsed = arguments(args, &None, false, Duration::from_secs(1), &budget).await;
        if exact {
            let parsed = parsed.unwrap();
            assert_eq!(parsed["text"], "ordinary\ntext");
            assert_eq!(budget.used(), capacity + length);
            drop(parsed);
        } else {
            assert_eq!(
                parsed.unwrap_err().downcast_ref::<Failure>().unwrap().kind,
                "memory_limit_exceeded"
            );
        }
        assert_eq!(budget.used(), capacity);
        drop(cli);
        assert_eq!(budget.used(), 0);
    }
}

#[test]
fn cleanup_failure_does_not_replace_known_tool_error_exit() {
    let mut output = Output::json(json!({"isError":true}));
    output.code = 3;
    let output = output.cleanup(Some(Failure::new(1, "cleanup", "cleanup failed")));
    assert_eq!(output.code, 3);
    assert_eq!(output.value, Some(json!({"isError":true})));
    assert_eq!(output.diagnostic.unwrap().kind, "cleanup");
}

#[test]
fn cli_and_help_probe_definitions_are_consistent() {
    Cli::command().debug_assert();
    TopLevelHelpProbe::command().debug_assert();
    let parsed = Cli::try_parse_from([
        "mcpcli",
        "call",
        "srv",
        "tool",
        "--inline-media",
        "--max-timeout",
        "300",
    ])
    .unwrap();
    assert!(!parsed.has_explicit_timeout());
    assert!(matches!(
        parsed.command,
        Command::Call {
            inline_media: true,
            max_timeout: Some(300),
            ..
        }
    ));
    assert!(Cli::try_parse_from(["mcpcli", "config", "validate"]).is_ok());
    let tools_cmd =
        Cli::try_parse_from(["mcpcli", "tools", "srv", "--filter", "search", "--summary"]).unwrap();
    assert!(matches!(
        tools_cmd.command,
        Command::Tools {
            ref server,
            ref filter,
            summary: true,
        } if server == "srv" && filter.as_deref() == Some("search")
    ));
}

#[test]
fn validate_call_max_timeout_enforces_bounds_and_order() {
    assert_eq!(validate_call_max_timeout(60, None).unwrap(), None);
    assert_eq!(
        validate_call_max_timeout(60, Some(60)).unwrap(),
        Some(Duration::from_secs(60))
    );
    assert_eq!(
        validate_call_max_timeout(60, Some(86_400)).unwrap(),
        Some(Duration::from_secs(86_400))
    );
    let too_small = validate_call_max_timeout(60, Some(59))
        .unwrap_err()
        .downcast::<Failure>()
        .unwrap();
    assert_eq!(too_small.code, 2);
    assert_eq!(too_small.kind, "invalid_timeout");
    let too_large = validate_call_max_timeout(60, Some(86_401))
        .unwrap_err()
        .downcast::<Failure>()
        .unwrap();
    assert_eq!(too_large.code, 2);
    assert_eq!(too_large.kind, "invalid_timeout");
}

#[test]
fn write_tools_text_renders_flattened_tools_array() {
    let flat = json!({
        "tools": [
            {"name": "alpha", "description": "First line"},
            {"name": "beta"}
        ]
    });
    assert_eq!(tools_text(&flat).unwrap(), "alpha\nFirst line\n\nbeta\n");
}

#[test]
fn tools_projection_moves_payloads_and_retains_new_description_lease() {
    for summary in [false, true] {
        let value = json!({"pages":[{"tools":[{
            "name":"sample", "description":"First line\nSecond line",
            "inputSchema":{"type":"object", "description":"s".repeat(64 << 10)},
            "annotations":{"note":"a".repeat(64 << 10)}
        }]}]});
        let annotation = value["pages"][0]["tools"][0]["annotations"]["note"]
            .as_str()
            .unwrap()
            .as_ptr();
        let schema = value["pages"][0]["tools"][0]["inputSchema"]["description"]
            .as_str()
            .unwrap()
            .as_ptr();
        let bytes = input::serialized_len(&value).unwrap();
        let budget = ByteBudget::new(bytes + 2 * 4 * 257);
        let source_lease = budget.try_acquire(bytes).unwrap();
        let remote = RemoteOutput {
            value,
            connection: None,
            inflight: None,
            lease: Some(source_lease),
        };
        let output = remote.into_output(|value| {
            let (value, lease) =
                project_tools(value, Some("SAMPLE"), summary, &budget)?.into_parts();
            Ok(Output::json(value).with_lease(Some(lease)))
        });
        assert_eq!(output.code, 0);
        let selected = &output.value.as_ref().unwrap()["tools"][0];
        assert_eq!(
            selected["annotations"]["note"].as_str().unwrap().as_ptr(),
            annotation
        );
        if summary {
            assert_eq!(selected["description"], "First line");
            assert!(selected.get("inputSchema").is_none());
            assert!(budget.used() > bytes);
        } else {
            assert_eq!(
                selected["inputSchema"]["description"]
                    .as_str()
                    .unwrap()
                    .as_ptr(),
                schema
            );
            assert_eq!(selected["description"], "First line\nSecond line");
            assert_eq!(budget.used(), bytes);
        }
        assert_eq!(output.lease.as_ref().unwrap().bytes(), budget.used());
        drop(output);
        assert_eq!(budget.used(), 0);
    }
}

#[test]
fn filtered_text_uses_first_nonempty_line_without_changing_json() {
    let flat = json!({"tools":[{"name":"sample", "description":"\n First line \nSecond line"}]});
    assert_eq!(tools_text(&flat).unwrap(), "sample\nFirst line\n");
    assert_eq!(
        flat["tools"][0]["description"],
        "\n First line \nSecond line"
    );
    let pages = json!({"pages":[{"tools":flat["tools"].clone()}]});
    assert_eq!(
        tools_text(&pages).unwrap(),
        "sample\n\n First line \nSecond line\n"
    );
}

#[test]
fn catalog_includes_enabled_flag_and_marks_disabled_in_text() {
    let dir = tempfile::tempdir().unwrap();
    let cfg_path = dir.path().join("mcpcli.json");
    std::fs::write(
        &cfg_path,
        serde_json::to_string(&json!({
            "version": 1,
            "servers": {
                "active": {
                    "transport": "stdio",
                    "command": "echo",
                    "description": "Active server",
                    "timeout": 15
                },
                "off": {
                    "transport": "stdio",
                    "command": "~/nonexistent/bin",
                    "cwd": "/definitely/missing/dir",
                    "env": {"TOKEN": "${MISSING_SECRET_VAR}"},
                    "enabled": false,
                    "description": "Disabled server"
                }
            }
        }))
        .unwrap(),
    )
    .unwrap();
    let loaded = LoadedConfig::load(&cfg_path).unwrap();
    let out = catalog(&loaded, None).unwrap();
    let servers = out.value.as_ref().unwrap()["servers"].as_array().unwrap();
    assert_eq!(servers.len(), 2);
    let active = servers.iter().find(|s| s["name"] == "active").unwrap();
    let off = servers.iter().find(|s| s["name"] == "off").unwrap();
    assert_eq!(active["enabled"], true);
    assert_eq!(off["enabled"], false);
    let text = out.text.unwrap();
    assert!(text.contains("server active\tActive server"));
    assert!(text.contains("server off [disabled]\tDisabled server"));

    let cli = Cli::try_parse_from(["mcpcli", "tools", "active"]).unwrap();
    assert_eq!(cli.timeout_secs_for(&loaded.config.servers["active"]), 15);
    let cli_override =
        Cli::try_parse_from(["mcpcli", "--timeout", "9", "tools", "active"]).unwrap();
    assert_eq!(
        cli_override.timeout_secs_for(&loaded.config.servers["active"]),
        9
    );
}

#[test]
fn events_cli_commands_parse_all_flags_and_reject_conflicts() {
    let list = Cli::try_parse_from(["mcpcli", "events", "list", "github"]).unwrap();
    assert!(matches!(
        list.command,
        Command::Events {
            command: EventsCommand::List { ref server }
        } if server == "github"
    ));

    let poll = Cli::try_parse_from([
        "mcpcli",
        "events",
        "poll",
        "github",
        "pr.merged",
        "--args",
        r#"{"repo":"org/app"}"#,
        "--cursor",
        "1:5",
        "--max-age-ms",
        "60000",
        "--max-events",
        "10",
    ])
    .unwrap();
    assert!(matches!(
        poll.command,
        Command::Events {
            command: EventsCommand::Poll {
                ref server,
                ref event,
                args: Some(_),
                cursor: Some(_),
                max_age_ms: Some(60_000),
                max_events: Some(10),
                ..
            }
        } if server == "github" && event == "pr.merged"
    ));

    let stream = Cli::try_parse_from([
        "mcpcli",
        "--oneshot",
        "events",
        "stream",
        "github",
        "pr.merged",
        "--args",
        r#"{"repo":"org/app"}"#,
        "--mode",
        "poll",
        "--cursor-file",
        "/tmp/pr.cursor",
        "--max-age-ms",
        "120000",
        "--heartbeat-ms",
        "15000",
        "--once",
        "--timeout",
        "300000",
        "--exec-mode",
        "stream",
        "--exec-timeout",
        "5000",
        "--",
        "fastexec",
        "daemon",
        "emit",
        "--stream",
    ])
    .unwrap();
    assert!(stream.oneshot);
    assert_eq!(stream.timeout, Some(300_000));
    assert!(matches!(
        stream.command,
        Command::Events {
            command: EventsCommand::Stream {
                ref server,
                ref event,
                mode: CliStreamMode::Poll,
                cursor_file: Some(_),
                max_age_ms: Some(120_000),
                heartbeat_ms: Some(15_000),
                once: true,
                exec_mode: CliExecMode::Stream,
                exec_timeout: Some(5_000),
                ref callback,
                ..
            }
        } if server == "github" && event == "pr.merged" && callback == &["fastexec", "daemon", "emit", "--stream"]
    ));

    // --cursor and --cursor-file must conflict.
    assert!(
        Cli::try_parse_from([
            "mcpcli",
            "events",
            "stream",
            "github",
            "pr.merged",
            "--cursor",
            "1:1",
            "--cursor-file",
            "/tmp/c",
        ])
        .is_err()
    );
}

#[test]
fn sub_millisecond_dispatch_budget_returns_exit_5_timeout() {
    let err = check_dispatch_budgets(
        Duration::from_millis(1000),
        None,
        Duration::from_micros(999_500),
    )
    .unwrap_err();
    assert_eq!(err.code, 5);
    assert_eq!(err.kind, "timeout");

    let hard_err = check_dispatch_budgets(
        Duration::from_secs(5),
        Some(Duration::from_millis(1000)),
        Duration::from_micros(999_999),
    )
    .unwrap_err();
    assert_eq!(hard_err.code, 5);
    assert_eq!(hard_err.kind, "timeout");

    let zero_err =
        check_dispatch_budgets(Duration::from_secs(1), None, Duration::from_secs(2)).unwrap_err();
    assert_eq!(zero_err.code, 5);
    assert_eq!(zero_err.kind, "timeout");

    let (rem, rem_hard) = check_dispatch_budgets(
        Duration::from_millis(1000),
        Some(Duration::from_millis(2000)),
        Duration::from_millis(999),
    )
    .unwrap();
    assert_eq!(rem, Duration::from_millis(1));
    assert_eq!(rem_hard, Some(Duration::from_millis(1001)));
}

#[test]
fn top_level_futures_stay_within_windows_stack_budget() {
    let cli = Cli::try_parse_from(["mcpcli", "catalog"]).unwrap();
    let uncertain = AtomicBool::new(false);
    let run_size = std::mem::size_of_val(&run(&cli, &uncertain, None));
    let entry_size = std::mem::size_of_val(&entry(cli));
    assert!(
        entry_size < 32 * 1024,
        "entry future is {entry_size} bytes; keep under 32 KiB for Windows 1 MiB stacks"
    );
    assert!(
        run_size < 64 * 1024,
        "run future is {run_size} bytes; keep under 64 KiB for Windows 1 MiB stacks"
    );
}
