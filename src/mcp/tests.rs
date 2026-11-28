use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use rmcp::{
    RoleClient,
    model::{
        ClientJsonRpcMessage, ClientRequest, ErrorCode, ErrorData, ExtensionCapabilities,
        ProtocolVersion, ServerJsonRpcMessage, ServerRequest,
    },
    service::{RequestContext, RunningService, Service, ServiceError},
    transport::{Transport, streamable_http_client::StreamableHttpPostResponse},
};
use serde_json::{Map, Value};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::Command,
    time::{Instant, timeout},
};
#[cfg(unix)]
use tokio_util::sync::CancellationToken;

use super::{
    connection::*, events::*, events_client::*, http::*, ingress::*, stdio::*, types::*, *,
};
use crate::{
    budget::{ByteBudget, Leased},
    config::ServerConfig,
};

#[cfg(all(test, unix))]
mod unix_child_transport_tests {
    use super::*;

    #[tokio::test]
    async fn observing_exit_does_not_reap_leader_before_group_cleanup() {
        let mut command = Command::new("sh");
        command.args(["-c", "exit 0"]);
        let (mut transport, _) = ChildTransport::spawn_observed(
            command,
            CleanupState::default(),
            Ingress::default(),
            Arc::new(TransportTermination::default()),
            MAX_RESPONSE_BYTES,
            0,
            ByteBudget::new(MAX_RESPONSE_BYTES),
        )
        .unwrap();
        assert!(transport.wait_unreaped().await.unwrap());
        // A second WNOWAIT observation must still find our waitable leader.
        assert!(transport.exited_unreaped().unwrap());
        assert!(transport.group.is_some());
        transport.shutdown().await.unwrap();
        assert!(transport.group.is_none());
        assert!(transport.child.is_none());
    }

    #[tokio::test]
    async fn blocked_stdio_writer_cannot_prevent_owned_child_shutdown() {
        let mut command = Command::new("sh");
        command.args(["-c", "trap '' TERM; sleep 60"]);
        let (mut transport, _) = ChildTransport::spawn_observed(
            command,
            CleanupState::default(),
            Ingress::default(),
            Arc::new(TransportTermination::default()),
            MAX_RESPONSE_BYTES,
            0,
            ByteBudget::new(MAX_RESPONSE_BYTES),
        )
        .unwrap();
        let message = serde_json::from_value(serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {"name": "blocked", "arguments": {"data": "x".repeat(2 * 1024 * 1024)}}
        }))
        .unwrap();
        let send = tokio::spawn(transport.send(message));
        tokio::time::sleep(Duration::from_millis(30)).await;
        timeout(Duration::from_secs(3), transport.shutdown())
            .await
            .unwrap()
            .unwrap();
        assert!(transport.group.is_none());
        assert!(transport.child.is_none());
        assert!(send.await.unwrap().is_err());
    }

    #[tokio::test]
    async fn cancelling_shutdown_keeps_group_owned_until_drop() {
        let mut command = Command::new("sh");
        command.args(["-c", "trap '' TERM; cat >/dev/null; sleep 60"]);
        let (mut transport, _) = ChildTransport::spawn_observed(
            command,
            CleanupState::default(),
            Ingress::default(),
            Arc::new(TransportTermination::default()),
            MAX_RESPONSE_BYTES,
            0,
            ByteBudget::new(MAX_RESPONSE_BYTES),
        )
        .unwrap();
        let pid = transport.group.unwrap();
        assert!(
            timeout(Duration::from_millis(25), transport.shutdown())
                .await
                .is_err()
        );
        assert_eq!(transport.group, Some(pid));
        drop(transport);
        timeout(Duration::from_secs(3), async {
            loop {
                let alive = unsafe { libc::kill(pid, 0) } == 0;
                if !alive {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
}

#[cfg(test)]
mod call_budget_tests {
    use super::*;

    #[test]
    fn dispatch_checks_pre_dispatch_deadline_with_and_without_hard_cap() {
        let now = Instant::now();
        for hard in [None, Some(Duration::from_secs(10))] {
            let budgets = CallBudgets {
                pre_dispatch_deadline: now - Duration::from_millis(1),
                idle: Duration::from_secs(1),
                hard,
            };
            let error = budgets.request_options(now).unwrap_err();
            let failure = error.downcast_ref::<McpFailure>().unwrap();
            assert_eq!(failure.kind, FailureKind::Timeout);
            assert!(!failure.outcome_unknown);
        }

        let budgets = CallBudgets {
            pre_dispatch_deadline: now + Duration::from_secs(10),
            idle: Duration::from_secs(1),
            hard: Some(Duration::from_secs(1)),
        };
        let error = budgets
            .request_options(now - Duration::from_secs(2))
            .unwrap_err();
        let failure = error.downcast_ref::<McpFailure>().unwrap();
        assert_eq!(failure.kind, FailureKind::Timeout);
        assert!(!failure.outcome_unknown);
    }

    #[test]
    fn dispatch_keeps_progress_idle_independent_of_admission_time() {
        let now = Instant::now();
        let budgets = CallBudgets {
            pre_dispatch_deadline: now + Duration::from_secs(10),
            idle: Duration::from_secs(60),
            hard: None,
        };
        let options = budgets.request_options(now).unwrap();
        assert_eq!(options.timeout, Some(Duration::from_secs(60)));
        assert!(options.reset_timeout_on_progress);
        assert_eq!(options.max_total_timeout, None);

        let options = CallBudgets {
            hard: Some(Duration::from_secs(30)),
            ..budgets
        }
        .request_options(now)
        .unwrap();
        assert!(options.max_total_timeout.unwrap() <= Duration::from_secs(30));
        assert_eq!(options.timeout, options.max_total_timeout);
        assert!(options.reset_timeout_on_progress);
    }
}

#[cfg(test)]
mod stdio_transport_tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn complete_tool_responses_remain_successful_after_transport_eof() {
        use rmcp::model::{InitializeResult, ServerCapabilities, ServerPeerInfo};
        use serde_json::json;

        async fn request(server: &mut BufReader<tokio::io::DuplexStream>) -> Value {
            let mut line = String::new();
            assert!(server.read_line(&mut line).await.unwrap() > 0);
            serde_json::from_str(&line).unwrap()
        }

        async fn respond(
            server: &mut BufReader<tokio::io::DuplexStream>,
            request: &Value,
            result: &Value,
        ) {
            let mut bytes = serde_json::to_vec(&json!({
                "jsonrpc": "2.0", "id": request["id"], "result": result
            }))
            .unwrap();
            bytes.push(b'\n');
            server.get_mut().write_all(&bytes).await.unwrap();
        }

        for method in ["tools/list", "tools/call"] {
            let budget = ByteBudget::new(64 * 1024);
            let ingress = Ingress::default();
            let termination = Arc::new(TransportTermination::default());
            let router = EventRouter::new(budget.clone());
            let tools_changed = Arc::new(AtomicBool::new(false));
            let (client, server) = tokio::io::duplex(4096);
            let (read, write) = tokio::io::split(client);
            let mut capabilities = ServerCapabilities::default();
            capabilities.tools = Some(Default::default());
            let mut info = ServerPeerInfo::from(InitializeResult::new(capabilities));
            info.protocol_version = ProtocolVersion::V_2026_07_28;
            let service = rmcp::service::serve_directly(
                Handler {
                    tools_changed: tools_changed.clone(),
                },
                EventNotificationTransport::new(
                    ChildTransport::from_parts(
                        read,
                        write,
                        ingress.clone(),
                        termination.clone(),
                        4096,
                        budget.clone(),
                    ),
                    router.clone(),
                    termination.clone(),
                ),
                Some(info),
            );
            let mut connection = McpConnection {
                peer: service.peer().clone(),
                service: Some(service),
                ingress,
                event_router: router,
                event_schemas: EventSchemaCache::default(),
                termination: termination.clone(),
                stderr_capture: None,
                limits: McpLimits {
                    budget: budget.clone(),
                    ..McpLimits::default()
                },
                server: Leased::new(Vec::new(), budget.empty_lease()),
                closing: None,
                cleanup: CleanupState::default(),
                tools_changed,
                http: false,
            };
            let mut server = BufReader::new(server);
            let catalog = json!({
                "resultType": "complete",
                "tools": [{"name": "echo", "inputSchema": {"type": "object"}}]
            });
            let mut operation = Box::pin(async {
                if method == "tools/list" {
                    connection
                        .list_tool_pages(Duration::from_secs(5))
                        .await
                        .map(|pages| Value::Array(pages.to_vec()))
                } else {
                    connection
                        .call("echo", Map::new(), Duration::from_secs(5))
                        .await
                        .map(|result| (*result).clone())
                }
            });
            let mut received = tokio::select! {
                result = &mut operation => panic!("request completed before its reply: {result:?}"),
                request = request(&mut server) => request,
            };
            assert_eq!(received["method"], "tools/list");
            if method == "tools/call" {
                respond(&mut server, &received, &catalog).await;
                received = tokio::select! {
                    result = &mut operation => panic!("call completed before its reply: {result:?}"),
                    request = request(&mut server) => request,
                };
                assert_eq!(received["method"], "tools/call");
            }
            let result = if method == "tools/list" {
                catalog
            } else {
                json!({"content": [{"type": "text", "text": "finished"}], "isError": false})
            };
            respond(&mut server, &received, &result).await;
            server.get_mut().shutdown().await.unwrap();

            // Leave the application future unpolled until the SDK has received EOF.
            timeout(Duration::from_secs(5), async {
                while !termination.is_closed() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert!(!termination.is_terminated());
            let result = operation.await.unwrap();
            if method == "tools/list" {
                assert_eq!(result[0]["tools"][0]["name"], "echo");
            } else {
                assert_eq!(result["content"][0]["text"], "finished");
            }
            assert!(connection.is_closed());
            assert!(connection.events_client().is_closed());
            connection.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn fragmented_stdio_line_charges_capacity_until_release() {
        let budget = ByteBudget::new(256);
        let (client_io, mut server_io) = tokio::io::duplex(8);
        let (read, write) = tokio::io::split(client_io);
        let mut transport = ChildTransport::from_parts(
            read,
            write,
            Ingress::default(),
            Arc::new(TransportTermination::default()),
            128,
            budget.clone(),
        );
        let sender = tokio::spawn(async move {
            server_io
                .write_all(b"abcdefghijklmnopqrstuvwxyz012345\n")
                .await
                .unwrap();
        });
        assert!(transport.read_line().await);
        sender.await.unwrap();
        assert_eq!(transport.line_lease.bytes(), transport.line_buf.capacity());
        assert_eq!(budget.used(), transport.line_buf.capacity());
        transport.clear_line_buf();
        assert_eq!(budget.used(), 0);
    }

    #[tokio::test]
    async fn stdio_transport_shrinks_large_line_buf_and_enforces_exact_bounds_and_leases() {
        let budget = ByteBudget::new(512 * 1024);
        let ingress = Ingress::default();
        let _capture = ingress.capture().unwrap();
        let termination = Arc::new(TransportTermination::default());
        let (client_io, mut server_io) = tokio::io::duplex(64 * 1024);
        let (read, write) = tokio::io::split(client_io);
        let mut transport = ChildTransport::from_parts(
            read,
            write,
            ingress.clone(),
            termination.clone(),
            200_000,
            budget.clone(),
        );

        let req: ClientJsonRpcMessage = serde_json::from_value(serde_json::json!({
            "jsonrpc": "2.0", "id": 7, "method": "tools/call",
            "params": {"name": "big", "arguments": {}}
        }))
        .unwrap();
        ingress.track(&req);

        let big_payload = "a".repeat(100_000);
        let mut frame = format!(
            "\u{feff}{{\"jsonrpc\":\"2.0\",\"id\":7,\"result\":{{\"content\":[{{\"type\":\"text\",\"text\":\"{big_payload}\"}}],\"x\":null}}}}\r\n"
        )
        .into_bytes();
        let write_fut = tokio::spawn(async move {
            server_io.write_all(&frame).await.unwrap();
            server_io.shutdown().await.unwrap();
            frame.clear();
            server_io
        });

        let received = transport.receive().await.expect("valid response");
        let _server_io = write_fut.await.unwrap();
        assert!(matches!(received, ServerJsonRpcMessage::Response(_)));
        assert!(transport.line_buf.capacity() <= LINE_SHRINK_CAP);
        assert_eq!(transport.line_lease.bytes(), 0);
        let (_, captured, lease) = ingress.take().unwrap();
        let captured = captured.unwrap();
        assert_eq!(captured["result"].get("x"), Some(&Value::Null));
        drop(captured);
        drop(received);
        drop(lease);
        assert_eq!(budget.used(), 0);

        // Exact N-1, N pass and N+1 reject including LF / CRLF.
        for crlf in [false, true] {
            let line_ending = if crlf { "\r\n" } else { "\n" };
            let sample = |pad: usize| {
                format!(
                    "{{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{{\"tools\":[],\"p\":\"{}\"}}}}{line_ending}",
                    "x".repeat(pad)
                )
            };
            let exact = sample(10);
            let limit = exact.len();
            let under = sample(9);
            assert_eq!(under.len(), limit - 1);
            let over = sample(11);
            assert_eq!(over.len(), limit + 1);

            for valid in [under, exact] {
                let term = Arc::new(TransportTermination::default());
                let ingress = Ingress::default();
                let _capture = ingress.capture().unwrap();
                let request: ClientJsonRpcMessage = serde_json::from_value(serde_json::json!({
                    "jsonrpc": "2.0", "id": 1, "method": "tools/list"
                }))
                .unwrap();
                ingress.track(&request);
                let (c, mut s) = tokio::io::duplex(4096);
                let (r, w) = tokio::io::split(c);
                let mut t = ChildTransport::from_parts(
                    r,
                    w,
                    ingress,
                    term.clone(),
                    limit,
                    ByteBudget::new(4096),
                );
                s.write_all(valid.as_bytes()).await.unwrap();
                // EOF also makes a broken correlation fail promptly instead of hanging CI.
                s.shutdown().await.unwrap();
                assert!(t.receive().await.is_some());
                assert!(!term.is_terminated());
            }

            let term = Arc::new(TransportTermination::default());
            let small_budget = ByteBudget::new(4096);
            let (c, mut s) = tokio::io::duplex(4096);
            let (r, w) = tokio::io::split(c);
            let mut t = ChildTransport::from_parts(
                r,
                w,
                Ingress::default(),
                term.clone(),
                limit,
                small_budget.clone(),
            );
            s.write_all(over.as_bytes()).await.unwrap();
            assert!(t.receive().await.is_none());
            assert!(term.is_terminated());
            let err = term.failure(true);
            assert!(err.to_string().contains("exceeds the"));
            assert!(err.to_string().contains("outcome unknown"));
            assert_eq!(small_budget.used(), 0);
        }

        // Non-blocking memory lease failure terminates and releases partial bytes.
        let tight = ByteBudget::new(32);
        let term = Arc::new(TransportTermination::default());
        let (c, mut s) = tokio::io::duplex(4096);
        let (r, w) = tokio::io::split(c);
        let mut t =
            ChildTransport::from_parts(r, w, Ingress::default(), term.clone(), 4096, tight.clone());
        s.write_all(b"{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"tools\":[]}}\n")
            .await
            .unwrap();
        assert!(t.receive().await.is_none());
        assert!(term.is_terminated());
        assert!(
            term.failure(false)
                .to_string()
                .contains("active memory limit")
        );
        assert_eq!(tight.used(), 0);
        // Sending after termination refuses to write to dead stdin.
        let send_req: ClientJsonRpcMessage = serde_json::from_value(serde_json::json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/list"
        }))
        .unwrap();
        assert!(t.send(send_req).await.is_err());

        // Half-line cancellation safety: dropping receive() mid-line retains buffered bytes and lease.
        let half_budget = ByteBudget::new(4096);
        let half_ingress = Ingress::default();
        let _half_cap = half_ingress.capture().unwrap();
        let half_term = Arc::new(TransportTermination::default());
        let (c, mut s) = tokio::io::duplex(4096);
        let (r, w) = tokio::io::split(c);
        let mut ht = ChildTransport::from_parts(
            r,
            w,
            half_ingress.clone(),
            half_term.clone(),
            4096,
            half_budget.clone(),
        );
        let req99: ClientJsonRpcMessage = serde_json::from_value(serde_json::json!({
            "jsonrpc": "2.0",
            "id": 99,
            "method": "tools/list"
        }))
        .unwrap();
        ht.send(req99).await.unwrap();
        let mut echoed = Vec::new();
        let mut buf = [0u8; 256];
        let n = s.read(&mut buf).await.unwrap();
        echoed.extend_from_slice(&buf[..n]);
        assert!(echoed.ends_with(b"\n"));

        let first_half = b"{\"jsonrpc\":\"2.0\",\"id\":99,";
        s.write_all(first_half).await.unwrap();
        s.flush().await.unwrap();
        assert!(
            timeout(Duration::from_millis(30), ht.receive())
                .await
                .is_err()
        );
        assert_eq!(half_budget.used(), first_half.len());
        assert!(!half_term.is_terminated());

        let second_half = b"\"result\":{\"tools\":[],\"tag\":\"intact\"}}\n";
        s.write_all(second_half).await.unwrap();
        s.shutdown().await.unwrap();
        let full_msg = ht.receive().await.expect("completed line after cancel");
        assert!(matches!(full_msg, ServerJsonRpcMessage::Response(_)));
        assert_eq!(ht.line_lease.bytes(), 0);
        let (_, val, handed) = half_ingress.take().unwrap();
        assert_eq!(val.unwrap()["result"]["tag"], "intact");
        drop(handed);
        // The local frame and lossless capture are released. Ownership after
        // returning the typed message to the official SDK is outside this budget.
        assert_eq!(half_budget.used(), 0);
        drop(full_msg);
        assert_eq!(half_budget.used(), 0);
    }
}

#[cfg(test)]
mod http_tests {
    use super::*;
    use std::collections::BTreeMap;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    #[test]
    fn outgoing_body_capacity_follows_bytes_clones() {
        let message: ClientJsonRpcMessage = serde_json::from_value(serde_json::json!({
            "jsonrpc":"2.0", "id":1, "method":"tools/call",
            "params":{"name":"sample", "arguments":{"text":"ordinary\ntext"}}
        }))
        .unwrap();
        let length = crate::json::serialized_len(&message).unwrap();
        let budget = ByteBudget::new(length * 4);
        let termination = TransportTermination::default();
        let encoded = encode_mcp_message(&message, length, &budget, &termination).unwrap();
        let charged = encoded.capacity();
        assert_eq!(budget.used(), charged);
        let bytes = bytes::Bytes::from_owner(encoded);
        let other = bytes.clone();
        drop(bytes);
        assert_eq!(budget.used(), charged);
        assert_eq!(
            serde_json::from_slice::<Value>(&other).unwrap()["params"]["arguments"]["text"],
            "ordinary\ntext"
        );
        drop(other);
        assert_eq!(budget.used(), 0);
        let error = encode_mcp_message(&message, length - 1, &budget, &termination).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(termination.is_terminated());
        assert_eq!(budget.used(), 0);
        let budget = ByteBudget::new(1);
        let termination = TransportTermination::default();
        assert_eq!(
            encode_mcp_message(&message, length, &budget, &termination)
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::OutOfMemory
        );
        assert!(termination.is_terminated());
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn media_type_essence_is_exact_and_case_insensitive() {
        assert!(has_media_type(
            "Application/JSON; charset=UTF-8",
            "application/json"
        ));
        assert!(has_media_type(
            " TEXT/EVENT-STREAM ; charset=UTF-8",
            "text/event-stream"
        ));
        for value in [
            "application/jsonp",
            "application/json, text/plain",
            "",
            "application/jsonx",
        ] {
            assert!(!has_media_type(value, "application/json"));
        }
        assert!(!has_media_type(
            "text/event-stream-INVALID",
            "text/event-stream"
        ));
    }

    async fn read_framed_http(
        sock: &mut tokio::net::TcpStream,
        buf: &mut Vec<u8>,
    ) -> Option<(String, Vec<u8>)> {
        let mut scratch = [0u8; 2048];
        let header_end = loop {
            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break pos + 4;
            }
            let n = sock.read(&mut scratch).await.ok()?;
            if n == 0 {
                return None;
            }
            buf.extend_from_slice(&scratch[..n]);
        };
        let header_str = String::from_utf8_lossy(&buf[..header_end]).into_owned();
        let mut lines = header_str.split("\r\n");
        lines.next();
        let mut content_len = 0usize;
        for line in lines {
            if let Some((name, val)) = line.split_once(':')
                && name.trim().eq_ignore_ascii_case("content-length")
            {
                content_len = val.trim().parse().unwrap_or(0);
            }
        }
        while buf.len() < header_end + content_len {
            let n = sock.read(&mut scratch).await.ok()?;
            if n == 0 {
                return None;
            }
            buf.extend_from_slice(&scratch[..n]);
        }
        let body = buf[header_end..header_end + content_len].to_vec();
        buf.drain(..header_end + content_len);
        Some((header_str, body))
    }

    #[tokio::test]
    async fn event_http_headers_keep_their_lease_during_send() {
        let schema = serde_json::from_value(serde_json::json!({
            "type": "object",
            "properties": {
                "repo": { "type": "string", "x-mcp-header": "Repo" }
            }
        }))
        .unwrap();
        let event_schemas = Arc::new(std::sync::RwLock::new(Some(Leased::new(
            HashMap::from([("task.exited".into(), schema)]),
            ByteBudget::new(0).empty_lease(),
        ))));
        let message: ClientJsonRpcMessage = serde_json::from_value(serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "events/poll",
            "params": {"name": "task.exited", "arguments": {"repo": " leading"}}
        }))
        .unwrap();
        let mut http = BoundedHttp {
            client: reqwest::Client::new(),
            auth_manager: None,
            cleanup: CleanupState::default(),
            ingress: Ingress::default(),
            event_schemas,
            termination: Arc::new(TransportTermination::default()),
            max_response_bytes: 4096,
            budget: ByteBudget::new(16 * 1024),
        };
        let encoded = encode_mcp_message(&message, 4096, &http.budget, &http.termination).unwrap();
        let body_charge = encoded.capacity();
        drop(encoded);

        for completion in ["accepted", "closed", "cancelled"] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (received, observed) = tokio::sync::oneshot::channel();
            let (finish, wait) = tokio::sync::oneshot::channel();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let (headers, _) = read_framed_http(&mut socket, &mut Vec::new())
                    .await
                    .unwrap();
                received.send(headers).unwrap();
                if wait.await.unwrap_or(false) {
                    socket
                        .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
                        .await
                        .unwrap();
                }
            });
            let sender = http.clone();
            let request = message.clone();
            let sent = tokio::spawn(async move {
                sender
                    .post(
                        format!("http://{address}/mcp").into(),
                        request,
                        None,
                        None,
                        HashMap::new(),
                        4096,
                    )
                    .await
            });
            let headers = timeout(Duration::from_secs(5), observed)
                .await
                .unwrap()
                .unwrap();
            assert!(headers.contains("mcp-name: task.exited"));
            assert!(headers.contains("mcp-param-repo: =?base64?IGxlYWRpbmc=?="));
            assert!(
                http.budget.used() > body_charge,
                "event headers must remain charged while POST waits for its response"
            );
            if completion == "cancelled" {
                sent.abort();
                assert!(sent.await.unwrap_err().is_cancelled());
                finish.send(false).unwrap();
            } else {
                finish.send(completion == "accepted").unwrap();
                let result = sent.await.unwrap();
                if completion == "accepted" {
                    assert!(matches!(result, Ok(StreamableHttpPostResponse::Accepted)));
                } else {
                    assert!(result.is_err());
                }
            }
            server.await.unwrap();
            assert_eq!(http.budget.used(), 0);
        }

        http.budget = ByteBudget::new(1);
        let error = http
            .post(
                "http://127.0.0.1:1/mcp".into(),
                message,
                None,
                None,
                HashMap::new(),
                4096,
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            HttpError::Io(ref error) if error.kind() == std::io::ErrorKind::OutOfMemory
        ));
        assert!(error.to_string().contains("event headers"));
        assert!(http.termination.is_terminated());
        assert_eq!(
            http.termination
                .failure(false)
                .downcast_ref::<McpFailure>()
                .unwrap()
                .kind,
            FailureKind::Transport
        );
        assert_eq!(http.budget.used(), 0);
    }

    #[tokio::test]
    async fn chunked_http_body_charges_capacity_until_release() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_framed_http(&mut socket, &mut Vec::new())
                .await
                .unwrap();
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n8\r\nabcdefgh\r\n",
                )
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(10)).await;
            socket
                .write_all(b"9\r\nijklmnopq\r\n0\r\n\r\n")
                .await
                .unwrap();
        });
        let budget = ByteBudget::new(128);
        let client = reqwest::Client::new();
        let http = BoundedHttp {
            client: client.clone(),
            auth_manager: None,
            cleanup: CleanupState::default(),
            ingress: Ingress::default(),
            event_schemas: EventSchemaCache::default(),
            termination: Arc::new(TransportTermination::default()),
            max_response_bytes: 64,
            budget: budget.clone(),
        };
        let response = client
            .get(format!("http://{address}/"))
            .send()
            .await
            .unwrap();
        let (body, lease) = http.bounded_body(response, 64).await.unwrap();
        assert_eq!(body, b"abcdefghijklmnopq");
        assert_eq!(budget.used(), body.capacity());
        assert_eq!(lease.bytes(), body.capacity());
        drop(body);
        drop(lease);
        assert_eq!(budget.used(), 0);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn discover_oversize_error_body_clears_transient_before_initialize() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let srv = tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut pending = Vec::new();
                    while let Some((req_line, body)) =
                        read_framed_http(&mut sock, &mut pending).await
                    {
                        if req_line.starts_with("GET ") {
                            let _ = sock
                                .write_all(
                                    b"HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n",
                                )
                                .await;
                            continue;
                        }
                        let body_str = String::from_utf8_lossy(&body);
                        if body_str.contains("\"server/discover\"") {
                            let big_err = "e".repeat(MAX_ERROR_BYTES + 4096);
                            let resp = format!(
                                "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n{}",
                                big_err.len(),
                                big_err
                            );
                            let _ = sock.write_all(resp.as_bytes()).await;
                        } else if body_str.contains("\"initialize\"") {
                            let _ = sock
                                .write_all(
                                    b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n",
                                )
                                .await;
                        } else {
                            let _ = sock
                                .write_all(
                                    b"HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n",
                                )
                                .await;
                        }
                    }
                });
            }
        });

        let cfg = ServerConfig::Http {
            url: format!("http://127.0.0.1:{port}/mcp"),
            headers: BTreeMap::new(),
            description: None,
            enabled: true,
            timeout: None,
            oauth: None,
        };
        let err = McpConnection::connect(&cfg, Duration::from_secs(5))
            .await
            .err()
            .unwrap();
        let msg = err.to_string();
        assert!(
            !msg.contains("65536") && !msg.contains("exceeds the"),
            "stale discover transient leaked into initialize failure: {msg}"
        );
        assert_eq!(
            err.downcast_ref::<McpFailure>().unwrap().kind,
            FailureKind::AuthRequired
        );
        assert!(msg.contains("401"), "unexpected: {msg}");
        srv.abort();
    }

    #[tokio::test]
    async fn http_events_list_poll_header_retry_and_stream_terminated_cancel() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let saw_get_reconnect = Arc::new(AtomicBool::new(false));
        let saw_cancelled = Arc::new(AtomicBool::new(false));
        let poll_attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let list_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let saw_get_srv = saw_get_reconnect.clone();
        let saw_cancel_srv = saw_cancelled.clone();
        let poll_attempts_srv = poll_attempts.clone();
        let list_calls_srv = list_calls.clone();

        let srv = tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                let saw_get_srv = saw_get_srv.clone();
                let saw_cancel_srv = saw_cancel_srv.clone();
                let poll_attempts_srv = poll_attempts_srv.clone();
                let list_calls_srv = list_calls_srv.clone();
                tokio::spawn(async move {
                    let mut pending = Vec::new();
                    while let Some((header_str, body)) =
                        read_framed_http(&mut sock, &mut pending).await
                    {
                        if header_str.starts_with("GET ") {
                            if header_str.to_ascii_lowercase().contains("last-event-id:") {
                                saw_get_srv.store(true, Ordering::Release);
                            }
                            let _ = sock
                                .write_all(
                                    b"HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n",
                                )
                                .await;
                            continue;
                        }
                        let msg: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
                        let method = msg.get("method").and_then(Value::as_str).unwrap_or("");
                        let id = msg.get("id").cloned().unwrap_or(Value::Null);
                        let lower_headers = header_str.to_ascii_lowercase();

                        let send_json = |status: &str, payload: Value| {
                            let encoded = serde_json::to_string(&payload).unwrap();
                            format!(
                                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                                encoded.len(),
                                encoded
                            )
                        };

                        match method {
                            "server/discover" => {
                                let resp = send_json(
                                    "200 OK",
                                    serde_json::json!({
                                        "jsonrpc": "2.0",
                                        "id": id,
                                        "result": {
                                            "resultType": "complete",
                                            "supportedVersions": ["2026-07-28"],
                                            "capabilities": {
                                                "extensions": {
                                                    "io.modelcontextprotocol/events": { "listChanged": false }
                                                }
                                            },
                                            "ttlMs": 60000,
                                            "cacheScope": "public",
                                            "_meta": {
                                                "io.modelcontextprotocol/serverInfo": {
                                                    "name": "events-demo",
                                                    "version": "1.0"
                                                }
                                            }
                                        }
                                    }),
                                );
                                let _ = sock.write_all(resp.as_bytes()).await;
                            }
                            "events/list" => {
                                let call_num = list_calls_srv.fetch_add(1, Ordering::AcqRel);
                                let cursor = msg
                                    .get("params")
                                    .and_then(|p| p.get("cursor"))
                                    .and_then(Value::as_str);
                                let payload = if cursor.is_none() && call_num == 0 {
                                    serde_json::json!({
                                        "jsonrpc": "2.0",
                                        "id": id,
                                        "result": {
                                            "resultType": "complete",
                                            "events": [
                                                {
                                                    "name": "bad.event",
                                                    "inputSchema": {
                                                        "type": "object",
                                                        "properties": {
                                                            "x": { "type": "number", "x-mcp-header": "InvalidNumber" }
                                                        }
                                                    }
                                                }
                                            ],
                                            "nextCursor": "page-2"
                                        }
                                    })
                                } else {
                                    serde_json::json!({
                                        "jsonrpc": "2.0",
                                        "id": id,
                                        "result": {
                                            "resultType": "complete",
                                            "events": [
                                                {
                                                    "name": "pr.merged",
                                                    "inputSchema": {
                                                        "type": "object",
                                                        "properties": {
                                                            "repo": { "type": "string", "x-mcp-header": "Repo" }
                                                        }
                                                    }
                                                }
                                            ]
                                        }
                                    })
                                };
                                let resp = send_json("200 OK", payload);
                                let _ = sock.write_all(resp.as_bytes()).await;
                            }
                            "events/poll" => {
                                let attempt = poll_attempts_srv.fetch_add(1, Ordering::AcqRel);
                                let has_name = lower_headers.contains("mcp-name: pr.merged");
                                let has_param = lower_headers.contains("mcp-param-repo: org/app");
                                if attempt == 0 {
                                    // Simulate stale schema header mismatch (-32020) on first attempt.
                                    let resp = send_json(
                                        "400 Bad Request",
                                        serde_json::json!({
                                            "jsonrpc": "2.0",
                                            "id": id,
                                            "error": { "code": -32020, "message": "header mismatch" }
                                        }),
                                    );
                                    let _ = sock.write_all(resp.as_bytes()).await;
                                } else if has_name && has_param {
                                    let resp = send_json(
                                        "200 OK",
                                        serde_json::json!({
                                            "jsonrpc": "2.0",
                                            "id": id,
                                            "result": {
                                                "resultType": "complete",
                                                "events": [{"eventId": "ev-1", "data": {"ok": true}}],
                                                "cursor": "1:1",
                                                "truncated": false,
                                                "hasMore": false,
                                                "nextPollMs": 2000
                                            }
                                        }),
                                    );
                                    let _ = sock.write_all(resp.as_bytes()).await;
                                } else {
                                    let resp = send_json(
                                        "400 Bad Request",
                                        serde_json::json!({
                                            "jsonrpc": "2.0",
                                            "id": id,
                                            "error": { "code": -32020, "message": "missing headers" }
                                        }),
                                    );
                                    let _ = sock.write_all(resp.as_bytes()).await;
                                }
                            }
                            "events/stream" => {
                                assert!(lower_headers.contains("mcp-name: pr.merged"));
                                assert!(lower_headers.contains("mcp-param-repo: org/app"));
                                let active = serde_json::json!({
                                    "jsonrpc": "2.0",
                                    "method": "notifications/events/active",
                                    "params": {
                                        "cursor": "1:1",
                                        "truncated": false,
                                        "heartbeatMs": 30000,
                                        "_meta": { "io.modelcontextprotocol/subscriptionId": id }
                                    }
                                });
                                let terminated = serde_json::json!({
                                    "jsonrpc": "2.0",
                                    "method": "notifications/events/terminated",
                                    "params": {
                                        "error": { "code": -32025, "message": "done" },
                                        "_meta": { "io.modelcontextprotocol/subscriptionId": id }
                                    }
                                });
                                let sse_body = format!(
                                    "id: sse-1\ndata: {active}\n\nid: sse-2\ndata: {terminated}\n\n"
                                );
                                let resp = format!(
                                    "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\n\r\n{}",
                                    sse_body.len(),
                                    sse_body
                                );
                                let _ = sock.write_all(resp.as_bytes()).await;
                            }
                            "notifications/cancelled" => {
                                saw_cancel_srv.store(true, Ordering::Release);
                                let _ = sock
                                    .write_all(
                                        b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\n\r\n",
                                    )
                                    .await;
                            }
                            _ => {
                                let _ = sock
                                    .write_all(
                                        b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\n\r\n",
                                    )
                                    .await;
                            }
                        }
                    }
                });
            }
        });

        let cfg = ServerConfig::Http {
            url: format!("http://127.0.0.1:{port}/mcp"),
            headers: BTreeMap::new(),
            description: None,
            enabled: true,
            timeout: None,
            oauth: None,
        };
        let mut conn = McpConnection::connect(&cfg, Duration::from_secs(5))
            .await
            .unwrap();
        assert!(conn.supports_events());

        let events = conn.list_events(Duration::from_secs(5)).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["name"], "pr.merged");
        drop(events);

        let mut args = Map::new();
        args.insert("repo".into(), Value::String("org/app".into()));
        let poll = conn
            .poll_events(
                PollEventsParams {
                    name: "pr.merged".into(),
                    arguments: Some(args.clone()),
                    cursor: None,
                    max_age_ms: None,
                    max_events: Some(10),
                },
                Duration::from_secs(5),
            )
            .await
            .unwrap();
        assert_eq!(poll["cursor"], "1:1");
        assert_eq!(poll_attempts.load(Ordering::Acquire), 2);
        drop(poll);

        let (handle, mut rx) = conn
            .open_event_stream(
                StreamEventsParams {
                    name: "pr.merged".into(),
                    arguments: Some(args),
                    cursor: Some("1:1".into()),
                    max_age_ms: None,
                    heartbeat_ms: Some(30_000),
                },
                Duration::from_secs(5),
            )
            .await
            .unwrap();
        let first = rx.recv().await.unwrap();
        assert_eq!(first.method, "notifications/events/active");
        let second = rx.recv().await.unwrap();
        assert_eq!(second.method, "notifications/events/terminated");
        drop(first);
        drop(second);

        // Verify RequestHandle was automatically cancelled upon notifications/events/terminated.
        timeout(Duration::from_secs(2), async {
            while !handle.is_cancelled().await {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(!saw_get_reconnect.load(Ordering::Acquire));
        let _ = saw_cancelled.load(Ordering::Acquire);

        handle.cancel().await.unwrap();
        conn.close().await.unwrap();
        srv.abort();
    }
}

#[cfg(test)]
mod rpc_fidelity_tests {
    use super::*;

    #[tokio::test]
    async fn default_request_handler_drops_payload_before_context_and_bounds_errors() {
        use rmcp::model::GetExtensions;
        #[derive(Clone)]
        struct Payload(Arc<AtomicBool>);
        impl Drop for Payload {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }
        struct Guard(Arc<AtomicBool>);
        impl Drop for Guard {
            fn drop(&mut self) {
                assert!(self.0.load(Ordering::Acquire));
            }
        }
        struct Idle;
        impl Transport<RoleClient> for Idle {
            type Error = std::io::Error;
            fn send(
                &mut self,
                _: ClientJsonRpcMessage,
            ) -> impl Future<Output = std::io::Result<()>> + Send + 'static {
                std::future::ready(Ok(()))
            }
            async fn receive(&mut self) -> Option<ServerJsonRpcMessage> {
                std::future::pending().await
            }
            async fn close(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let handler = Handler {
            tools_changed: Arc::new(AtomicBool::new(false)),
        };
        let mut service = rmcp::service::serve_directly(handler.clone(), Idle, None);
        for method in ["ping".to_owned(), "local/".to_owned() + &"x".repeat(4096)] {
            let dropped = Arc::new(AtomicBool::new(false));
            let mut request: ServerRequest = serde_json::from_value(
                serde_json::json!({"method":method,"params":{"note":"ordinary text"}}),
            )
            .unwrap();
            request.extensions_mut().insert(Payload(dropped.clone()));
            let mut context =
                RequestContext::new(rmcp::model::RequestId::Number(7), service.peer().clone());
            context.extensions.insert(Arc::new(Guard(dropped.clone())));
            let result = handler.handle_request(request, context).await;
            assert!(dropped.load(Ordering::Acquire));
            if method == "ping" {
                assert!(result.is_ok());
            } else {
                let error = result.unwrap_err();
                assert_eq!(error.code, ErrorCode::METHOD_NOT_FOUND);
                assert_eq!(error.message, "Unsupported server request");
                assert!(error.data.is_none());
            }
        }
        service.close().await.unwrap();
    }

    #[test]
    fn rpc_error_fidelity_and_non_lossy_rejection() {
        let budget = ByteBudget::new(4096);
        let ingress = Ingress::default();
        let _capture = ingress.capture().unwrap();
        assert!(ingress.capture().is_none());
        let request = serde_json::from_value(serde_json::json!({
            "jsonrpc":"2.0","id":17,"method":"tools/list"
        }))
        .unwrap();
        ingress.track(&request);
        let ok_err = br#"{"jsonrpc":"2.0","id":17,"error":{"code":-32603,"message":"failed","data":null,"x-extra":7}}"#;
        let _ = ingress.observe_bytes(ok_err, 4096, &budget, false).unwrap();
        let error = rpc_failure(-32603, "diagnostic".into(), true, &ingress);
        let failure = error.downcast_ref::<McpFailure>().unwrap();
        let raw = failure.rpc_error.as_ref().unwrap();
        assert_eq!(raw.get("data"), Some(&Value::Null));
        assert_eq!(raw["x-extra"], 7);
        assert!(failure.outcome_unknown);
        assert_eq!(budget.used(), ok_err.len());
        let cloned = failure.clone();
        assert!(Arc::ptr_eq(
            failure.rpc_error.as_ref().unwrap(),
            cloned.rpc_error.as_ref().unwrap()
        ));
        drop(error);
        assert_eq!(budget.used(), ok_err.len());
        let ipc_failure = crate::daemon::mcp_error(cloned.into(), true);
        assert_eq!(budget.used(), ok_err.len());
        let copied_failure = ipc_failure.clone();
        drop(ipc_failure);
        assert_eq!(budget.used(), ok_err.len());
        drop(copied_failure);
        assert_eq!(budget.used(), 0);

        ingress.track(&request);
        let bad_err = br#"{"jsonrpc":"2.0","id":17,"error":{"code":-32603,"message":"failed","data":{"x":1,"x":2}}}"#;
        assert!(matches!(
            ingress.observe_bytes(bad_err, 4096, &budget, false),
            Err(IngressError::InvalidMetadata)
        ));
        assert!(ingress.take().is_none());
        assert_eq!(budget.used(), 0);
    }
}

#[cfg(test)]
mod event_lifecycle_tests {
    use super::*;
    use rmcp::model::{RequestId, ServerCapabilities, ServerPeerInfo};
    use serde_json::json;
    use tokio::sync::mpsc;

    struct Wire {
        sent: mpsc::Sender<ClientJsonRpcMessage>,
        incoming: mpsc::Receiver<ServerJsonRpcMessage>,
        ingress: Ingress,
    }

    impl Transport<RoleClient> for Wire {
        type Error = std::io::Error;

        fn send(
            &mut self,
            message: ClientJsonRpcMessage,
        ) -> impl Future<Output = std::io::Result<()>> + Send + 'static {
            self.ingress.track(&message);
            let sent = self.sent.clone();
            async move { sent.send(message).await.map_err(std::io::Error::other) }
        }

        async fn receive(&mut self) -> Option<ServerJsonRpcMessage> {
            let message = self.incoming.recv().await?;
            match &message {
                ServerJsonRpcMessage::Response(response) => self.ingress.untrack(&response.id),
                ServerJsonRpcMessage::Error(error) => {
                    if let Some(id) = &error.id {
                        self.ingress.untrack(id);
                    }
                }
                _ => {}
            }
            Some(message)
        }

        async fn close(&mut self) -> std::io::Result<()> {
            self.incoming.close();
            Ok(())
        }
    }

    struct Fixture {
        client: McpEventsClient,
        service: RunningService<RoleClient, Handler>,
        sent: mpsc::Receiver<ClientJsonRpcMessage>,
        incoming: mpsc::Sender<ServerJsonRpcMessage>,
    }

    impl Fixture {
        fn new(budget: Arc<ByteBudget>) -> Self {
            let (sent_tx, sent) = mpsc::channel(16);
            let (incoming, incoming_rx) = mpsc::channel(16);
            let ingress = Ingress::default();
            let router = EventRouter::new(budget.clone());
            let termination = Arc::new(TransportTermination::default());
            let mut extensions = ExtensionCapabilities::new();
            extensions.insert(EVENTS_EXTENSION_ID.into(), Map::new());
            let mut capabilities = ServerCapabilities::default();
            capabilities.extensions = Some(extensions);
            let mut info = ServerPeerInfo::from(rmcp::model::InitializeResult::new(capabilities));
            info.protocol_version = ProtocolVersion::V_2026_07_28;
            let service = rmcp::service::serve_directly(
                Handler {
                    tools_changed: Arc::new(AtomicBool::new(false)),
                },
                EventNotificationTransport::new(
                    Wire {
                        sent: sent_tx,
                        incoming: incoming_rx,
                        ingress: ingress.clone(),
                    },
                    router.clone(),
                    termination.clone(),
                ),
                Some(info),
            );
            let client = McpEventsClient {
                peer: service.peer().clone(),
                service_cancel: Arc::new(Mutex::new(None)),
                ingress,
                event_router: router,
                event_schemas: EventSchemaCache::default(),
                termination,
                stderr_capture: None,
                limits: McpLimits {
                    budget,
                    ..McpLimits::default()
                },
                http: true,
            };
            Self {
                client,
                service,
                sent,
                incoming,
            }
        }

        async fn request(&mut self, method: &str) -> RequestId {
            let message = timeout(Duration::from_secs(2), self.sent.recv())
                .await
                .unwrap()
                .unwrap();
            let ClientJsonRpcMessage::Request(request) = message else {
                panic!("expected request: {message:?}")
            };
            assert_eq!(request.request.method(), method);
            request.id
        }

        async fn respond(&self, message: Value) {
            self.incoming
                .send(serde_json::from_value(message).unwrap())
                .await
                .unwrap();
        }
    }

    fn stream_params() -> StreamEventsParams {
        StreamEventsParams {
            name: "sample.changed".into(),
            arguments: None,
            cursor: None,
            max_age_ms: None,
            heartbeat_ms: Some(30_000),
        }
    }

    fn poll_params() -> PollEventsParams {
        PollEventsParams {
            name: "sample.changed".into(),
            arguments: None,
            cursor: None,
            max_age_ms: None,
            max_events: Some(1),
        }
    }

    #[tokio::test]
    async fn dropping_stream_handshake_cancels_the_upstream_request() {
        let budget = ByteBudget::new(4096);
        let mut fixture = Fixture::new(budget.clone());
        let client = fixture.client.clone();
        let mut opening =
            Box::pin(client.open_event_stream(stream_params(), Duration::from_secs(5)));
        let id = tokio::select! {
            biased;
            _ = &mut opening => panic!("stream started without a server response"),
            id = fixture.request("events/stream") => id,
        };
        drop(opening);
        let cancelled = timeout(Duration::from_secs(2), fixture.sent.recv())
            .await
            .unwrap()
            .unwrap();
        let encoded = serde_json::to_value(cancelled).unwrap();
        assert_eq!(encoded["method"], "notifications/cancelled");
        assert_eq!(
            encoded["params"]["requestId"],
            serde_json::to_value(&id).unwrap()
        );
        let late_response =
            serde_json::to_vec(&json!({"jsonrpc":"2.0","id":id,"result":{}})).unwrap();
        assert!(matches!(
            fixture
                .client
                .ingress
                .observe_bytes(&late_response, 4096, &budget, false)
                .unwrap(),
            ObserveOutcome::NotMatched
        ));
        assert_eq!(budget.used(), 0);
        fixture.service.close().await.unwrap();
    }

    #[tokio::test]
    async fn final_stream_response_drains_queued_notifications_and_closes_only_that_stream() {
        let budget = ByteBudget::new(4096);
        let mut fixture = Fixture::new(budget.clone());
        let client = fixture.client.clone();
        let opening = tokio::spawn(async move {
            client
                .open_event_stream(stream_params(), Duration::from_secs(5))
                .await
        });
        let id = fixture.request("events/stream").await;
        for (method, fields) in [
            ("active", json!({"heartbeatMs":30000,"cursor":null})),
            (
                "event",
                json!({"eventId":"one","cursor":"1","data":{"value":1}}),
            ),
            (
                "event",
                json!({"eventId":"two","cursor":"2","data":{"value":2}}),
            ),
        ] {
            let mut params = fields.as_object().unwrap().clone();
            params.insert(
                "_meta".into(),
                json!({"io.modelcontextprotocol/subscriptionId":id}),
            );
            fixture.respond(json!({"jsonrpc":"2.0","method":format!("notifications/events/{method}"),"params":params})).await;
        }
        fixture
            .respond(json!({"jsonrpc":"2.0","id":id,"result":{"resultType":"complete"}}))
            .await;
        let (handle, mut receiver) = opening.await.unwrap().unwrap();
        for method in ["active", "event", "event"] {
            let notification = timeout(Duration::from_secs(2), receiver.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                notification.method,
                format!("notifications/events/{method}")
            );
        }
        assert!(
            timeout(Duration::from_secs(2), receiver.recv())
                .await
                .unwrap()
                .is_none()
        );
        assert!(handle.is_cancelled().await);
        assert!(!fixture.client.is_closed());
        drop(handle);
        drop(receiver);
        assert_eq!(budget.used(), 0);

        let client = fixture.client.clone();
        let poll = tokio::spawn(async move {
            client
                .poll_events(poll_params(), Duration::from_secs(5))
                .await
        });
        let id = fixture.request("events/poll").await;
        fixture.respond(json!({"jsonrpc":"2.0","id":id,"result":{"resultType":"complete","events":[],"cursor":null}})).await;
        drop(poll.await.unwrap().unwrap());
        assert!(fixture.sent.try_recv().is_err());
        fixture.service.close().await.unwrap();
    }

    #[tokio::test]
    async fn event_rpc_errors_preserve_code_data_and_tool_capture() {
        let budget = ByteBudget::new(16 * 1024);
        let mut fixture = Fixture::new(budget.clone());
        let ingress = fixture.client.ingress.clone();
        let _capture = ingress.capture().unwrap();
        let tool_request =
            serde_json::from_value(json!({"jsonrpc":"2.0","id":900,"method":"tools/list"}))
                .unwrap();
        ingress.track(&tool_request);
        let tool_error =
            br#"{"jsonrpc":"2.0","id":900,"error":{"code":-32603,"message":"tool error"}}"#;
        let _ = ingress
            .observe_bytes(tool_error, 4096, &budget, false)
            .unwrap();
        let captured_bytes = budget.used();
        for (method, code) in [
            ("events/list", -32602),
            ("events/poll", -32023),
            ("events/stream", -32026),
        ] {
            let client = fixture.client.clone();
            let operation = tokio::spawn(async move {
                match method {
                    "events/list" => client.list_events(Duration::from_secs(5)).await.map(|_| ()),
                    "events/poll" => client
                        .poll_events(poll_params(), Duration::from_secs(5))
                        .await
                        .map(|_| ()),
                    _ => client
                        .open_event_stream(stream_params(), Duration::from_secs(5))
                        .await
                        .map(|_| ()),
                }
            });
            let id = fixture.request(method).await;
            let expected = json!({"code":code,"message":"subscription unavailable","data":{"modes":["poll"],"detail":null}});
            fixture
                .respond(json!({"jsonrpc":"2.0","id":id,"error":expected}))
                .await;
            let error = operation.await.unwrap().unwrap_err();
            let failure = error.downcast_ref::<McpFailure>().unwrap();
            assert_eq!(failure.rpc_error.as_deref(), Some(&expected));
            assert_eq!(
                budget.used(),
                captured_bytes
                    + crate::json::serialized_len(&expected).unwrap()
                    + failure.message.capacity()
            );
            let retained = failure.clone();
            drop(error);
            assert!(budget.used() > captured_bytes);
            drop(retained);
            assert_eq!(budget.used(), captured_bytes);
        }
        let (_, captured, lease) = ingress
            .take()
            .expect("Events must leave the ordinary capture untouched");
        assert_eq!(captured.unwrap()["error"]["message"], "tool error");
        drop(lease);
        assert_eq!(budget.used(), 0);
        fixture.service.close().await.unwrap();
    }

    #[tokio::test]
    async fn event_error_moves_data_and_accounts_for_retained_diagnostic() {
        let budget = ByteBudget::new(4096);
        let mut fixture = Fixture::new(budget.clone());
        let rpc_message = "m".repeat(512);
        let message_ptr = rpc_message.as_ptr();
        let data = json!({"items":[{"details":{"note":"ordinary nested value","attempts":[1,2]}}]});
        let data_ptr = data["items"][0]["details"]["note"]
            .as_str()
            .unwrap()
            .as_ptr();
        let rpc = ErrorData::new(ErrorCode(-32026), rpc_message, Some(data));
        let rpc_bytes = crate::json::serialized_len(&rpc).unwrap();
        let error = fixture.client.service_failure(ServiceError::McpError(rpc));
        let failure = error.downcast_ref::<McpFailure>().unwrap();
        let value = failure.rpc_error.as_ref().unwrap();
        assert_eq!(value["code"], -32026);
        assert_eq!(value["message"].as_str().unwrap().as_ptr(), message_ptr);
        assert_eq!(
            value["data"]["items"][0]["details"]["note"]
                .as_str()
                .unwrap()
                .as_ptr(),
            data_ptr
        );
        assert_eq!(
            failure.message,
            format!("MCP error -32026: {}", "m".repeat(512))
        );
        let charged = rpc_bytes + failure.message.capacity();
        assert_eq!(budget.used(), charged);
        let retained = failure.clone();
        assert!(Arc::ptr_eq(retained.rpc_error.as_ref().unwrap(), value));
        drop(error);
        assert_eq!(budget.used(), charged);
        let ipc_failure = crate::daemon::mcp_error(retained.into(), false);
        assert_eq!(budget.used(), charged);
        drop(ipc_failure);
        assert_eq!(budget.used(), 0);
        fixture.service.close().await.unwrap();
    }

    #[tokio::test]
    async fn event_error_rejects_insufficient_diagnostic_budget_without_retaining_json() {
        let rpc = ErrorData::new(
            ErrorCode(-32026),
            "m".repeat(512),
            Some(json!({"items":[{"details":{"note":"ordinary value"}}]})),
        );
        let rpc_bytes = crate::json::serialized_len(&rpc).unwrap();
        let diagnostic_bytes = "MCP error -32026: ".len() + rpc.message.len();
        let budget = ByteBudget::new(rpc_bytes + diagnostic_bytes - 1);
        let mut fixture = Fixture::new(budget.clone());
        let error = fixture.client.service_failure(ServiceError::McpError(rpc));
        let failure = error.downcast_ref::<McpFailure>().unwrap();
        assert_eq!(failure.kind, FailureKind::Transport);
        assert!(failure.rpc_error.is_none());
        assert!(failure.lease.is_none());
        assert_eq!(budget.used(), 0);
        fixture.service.close().await.unwrap();
    }

    #[tokio::test]
    async fn event_schema_cache_keeps_its_own_lease_after_catalog_is_dropped() {
        let budget = ByteBudget::new(4096);
        let mut fixture = Fixture::new(budget.clone());
        let client = fixture.client.clone();
        let listing = tokio::spawn(async move { client.list_events(Duration::from_secs(5)).await });
        let id = fixture.request("events/list").await;
        let schema = json!({"type":"object","properties":{"label":{"type":"string","x-mcp-header":"Label"}}});
        fixture.respond(json!({"jsonrpc":"2.0","id":id,"result":{"resultType":"complete","events":[{"name":"sample.changed","inputSchema":schema}]}})).await;
        let catalog = listing.await.unwrap().unwrap();
        let held_with_catalog = budget.used();
        drop(catalog);
        assert_eq!(
            budget.used(),
            crate::json::serialized_len(&schema).unwrap() + "sample.changed".len()
        );
        assert!(budget.used() < held_with_catalog);
        fixture.client.event_schemas.write().unwrap().take();
        assert_eq!(budget.used(), 0);
        fixture.service.close().await.unwrap();
    }

    #[tokio::test]
    async fn stdio_events_list_ignores_x_mcp_header_while_http_filters_invalid_annotations() {
        let budget = ByteBudget::new(8192);
        let bad_schema = json!({
            "type": "object",
            "properties": {
                "ratio": { "type": "number", "x-mcp-header": "InvalidFloatHeader" }
            }
        });
        for is_http in [false, true] {
            let mut fixture = Fixture::new(budget.clone());
            fixture.client.http = is_http;
            let client = fixture.client.clone();
            let listing =
                tokio::spawn(async move { client.list_events(Duration::from_secs(5)).await });
            let id = fixture.request("events/list").await;
            fixture
                .respond(json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": {
                        "resultType": "complete",
                        "events": [
                            { "name": "bad.annotated", "inputSchema": bad_schema },
                            { "name": "ok.plain", "inputSchema": { "type": "object" } }
                        ]
                    }
                }))
                .await;
            let listed = listing.await.unwrap().unwrap();
            if is_http {
                assert_eq!(listed.len(), 1);
                assert_eq!(listed[0]["name"], "ok.plain");
            } else {
                assert_eq!(listed.len(), 2);
                assert_eq!(listed[0]["name"], "bad.annotated");
                assert!(fixture.client.event_schemas.read().unwrap().is_none());
            }
            drop(listed);
            fixture.client.event_schemas.write().unwrap().take();
            fixture.service.close().await.unwrap();
        }
        assert_eq!(budget.used(), 0);
    }

    #[tokio::test]
    async fn event_request_budget_precedes_the_json_copy() {
        let budget = ByteBudget::new(32);
        let mut fixture = Fixture::new(budget.clone());
        let mut params = stream_params();
        params.arguments = Some(
            json!({"label":"ordinary value"})
                .as_object()
                .unwrap()
                .clone(),
        );
        assert!(
            fixture
                .client
                .event_request("events/stream", &params)
                .is_err()
        );
        assert_eq!(budget.used(), 0);
        assert!(fixture.sent.try_recv().is_err());
        fixture.service.close().await.unwrap();
    }
}

#[cfg(test)]
mod stderr_capture_tests {
    use super::*;

    #[test]
    fn format_stderr_tail_skips_leading_utf8_continuation_bytes_and_sanitizes() {
        assert_eq!(format_stderr_tail(b""), None);
        // Simulate slicing inside a 3-byte UTF-8 sequence ("你" = E4 BD A0) followed by valid text and control bytes.
        let mut raw = vec![0xBD, 0xA0];
        raw.extend_from_slice("你好\x07\n".as_bytes());
        let formatted = format_stderr_tail(&raw).unwrap();
        assert_eq!(formatted, "你好\\u{7}\n");

        // Ring slicing at 4096 bytes across 3-byte characters produces valid UTF-8 with no replacement char.
        let budget = ByteBudget::new(32 * 1024);
        let lease = budget.try_acquire(DEFAULT_STDERR_TAIL_BYTES).unwrap();
        let mut ring = StderrRing::try_new(DEFAULT_STDERR_TAIL_BYTES, lease).unwrap();
        let payload = "你".repeat(2000); // 6000 bytes (> 4096)
        ring.push(payload.as_bytes());
        let tail = format_stderr_tail(&ring.tail_bytes(MAX_STDERR_SNAPSHOT_BYTES)).unwrap();
        assert!(!tail.contains('\u{FFFD}'));
        assert!(tail.ends_with("你好".chars().next().unwrap()));
        assert!(tail.len() <= MAX_STDERR_SNAPSHOT_BYTES);
    }

    #[test]
    fn stderr_ring_preallocates_charged_capacity_and_never_grows() {
        for cap in [31, 4_097, DEFAULT_STDERR_TAIL_BYTES] {
            let budget = ByteBudget::new(64 * 1024);
            let lease = budget.try_acquire(cap).unwrap();
            let mut ring = StderrRing::try_new(cap, lease).unwrap();
            let allocated = ring.buf.capacity();
            assert!(allocated >= cap);
            assert_eq!(ring.lease.bytes(), allocated);
            assert_eq!(budget.used(), allocated);
            for _ in 0..8 {
                ring.push(&[b'a'; 4_096]);
                ring.push(b"tail");
                assert!(ring.buf.len() <= cap);
                assert_eq!(ring.buf.capacity(), allocated);
                assert_eq!(budget.used(), allocated);
                assert_eq!(ring.tail_bytes(4), b"tail");
            }
            drop(ring);
            assert_eq!(budget.used(), 0);
        }
    }

    #[tokio::test]
    async fn stderr_capture_budget_fallback_keeps_child_stdio_working() {
        let budget = ByteBudget::new(4_096);
        let python = std::env::var("MCPCLI_TEST_PYTHON")
            .unwrap_or_else(|_| if cfg!(windows) { "python" } else { "python3" }.into());
        let mut command = Command::new(python);
        command.args([
            "-c",
            "import sys; sys.stderr.write('fallback diagnostic\\n'); sys.stderr.flush(); print('ready', flush=True)",
        ]);
        let (mut transport, capture) = ChildTransport::spawn_observed(
            command,
            CleanupState::default(),
            Ingress::default(),
            Arc::new(TransportTermination::default()),
            MAX_RESPONSE_BYTES,
            4_097,
            budget.clone(),
        )
        .unwrap();
        assert!(capture.is_none());
        assert_eq!(budget.used(), 0);
        let mut output = String::new();
        timeout(
            Duration::from_secs(5),
            transport.read.read_line(&mut output),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(output.trim_end(), "ready");
        transport.shutdown().await.unwrap();
        assert_eq!(budget.used(), 0);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn stderr_capture_fast_exit_large_output_and_abort_lease_release() {
        // 1. Fast exit returns immediately on EOF without waiting the 500ms snapshot window.
        let budget = ByteBudget::new(64 * 1024);
        let mut cmd = Command::new("sh");
        cmd.args(["-c", "printf 'fast_err_line\\n' >&2; exit 1"]);
        let (mut transport, capture) = ChildTransport::spawn_observed(
            cmd,
            CleanupState::default(),
            Ingress::default(),
            Arc::new(TransportTermination::default()),
            MAX_RESPONSE_BYTES,
            DEFAULT_STDERR_TAIL_BYTES,
            budget.clone(),
        )
        .unwrap();
        let capture = capture.expect("stderr capture enabled");
        let _ = transport.wait_unreaped().await.unwrap();
        let started = Instant::now();
        let tail = capture.snapshot_tail(Duration::from_millis(500)).await;
        assert!(
            started.elapsed() < Duration::from_millis(150),
            "fast exit should not wait full timeout, elapsed={:?}",
            started.elapsed()
        );
        assert_eq!(tail.as_deref(), Some("fast_err_line\n"));
        capture.abort();
        assert_eq!(budget.used(), 0);
        transport.shutdown().await.unwrap();

        // 2. Large (>1 MiB) stderr output drains continuously without deadlocking the child.
        let mut big_cmd = Command::new("python3");
        big_cmd.args([
            "-c",
            "import sys; sys.stderr.buffer.write(b'A'*1200000 + b'TAIL_END\\n'); sys.stderr.flush()",
        ]);
        let (mut big_transport, big_capture) = ChildTransport::spawn_observed(
            big_cmd,
            CleanupState::default(),
            Ingress::default(),
            Arc::new(TransportTermination::default()),
            MAX_RESPONSE_BYTES,
            DEFAULT_STDERR_TAIL_BYTES,
            budget.clone(),
        )
        .unwrap();
        let big_capture = big_capture.expect("stderr capture enabled");
        assert!(
            timeout(Duration::from_secs(5), big_transport.wait_unreaped())
                .await
                .expect("noisy child must not deadlock on stderr pipe")
                .unwrap()
        );
        let big_tail = big_capture
            .snapshot_tail(Duration::from_millis(500))
            .await
            .unwrap();
        assert!(big_tail.ends_with("TAIL_END\n"));
        assert!(big_tail.len() <= MAX_STDERR_SNAPSHOT_BYTES);
        big_capture.abort();
        assert_eq!(budget.used(), 0);
        big_transport.shutdown().await.unwrap();

        // 3. Detached grandchild holding stderr open does not leak ByteLease after abort().
        let mut grand_cmd = Command::new("sh");
        grand_cmd.args(["-c", "printf 'parent_msg\\n' >&2; (sleep 30) &"]);
        let (mut grand_transport, grand_capture) = ChildTransport::spawn_observed(
            grand_cmd,
            CleanupState::default(),
            Ingress::default(),
            Arc::new(TransportTermination::default()),
            MAX_RESPONSE_BYTES,
            DEFAULT_STDERR_TAIL_BYTES,
            budget.clone(),
        )
        .unwrap();
        let grand_capture = grand_capture.expect("stderr capture enabled");
        assert_eq!(budget.used(), DEFAULT_STDERR_TAIL_BYTES);
        grand_capture.abort();
        assert_eq!(budget.used(), 0);
        grand_transport.shutdown().await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn stdio_events_list_failure_surfaces_stderr_tail_and_close_releases_lease() {
        let script = r#"
import json, sys
for line in sys.stdin:
    msg = json.loads(line)
    method = msg.get("method")
    if method == "server/discover":
        resp = {
            "jsonrpc": "2.0",
            "id": msg["id"],
            "result": {
                "resultType": "complete",
                "supportedVersions": ["2026-07-28"],
                "capabilities": {
                    "extensions": {
                        "io.modelcontextprotocol/events": {"listChanged": False}
                    }
                },
                "ttlMs": 60000,
                "cacheScope": "public",
                "_meta": {
                    "io.modelcontextprotocol/serverInfo": {
                        "name": "events-crash",
                        "version": "0.1.0"
                    }
                }
            }
        }
        sys.stdout.write(json.dumps(resp) + "\n")
        sys.stdout.flush()
    elif method == "events/list":
        sys.stderr.write("events_list_boom_stderr\n")
        sys.stderr.flush()
        sys.exit(1)
"#;
        let config = ServerConfig::Stdio {
            command: "python3".into(),
            args: vec!["-u".into(), "-c".into(), script.into()],
            env: std::collections::BTreeMap::new(),
            cwd: None,
            description: None,
            enabled: true,
            timeout: None,
        };
        let budget = ByteBudget::new(1024 * 1024);
        let limits = McpLimits {
            budget: budget.clone(),
            ..McpLimits::default()
        };
        let mut conn = McpConnection::connect_with_limits(&config, Duration::from_secs(5), limits)
            .await
            .expect("connect to stdio events server");
        let events_client = conn.events_client();
        let err = events_client
            .list_events(Duration::from_secs(5))
            .await
            .unwrap_err();
        let failure = err
            .downcast_ref::<McpFailure>()
            .expect("expected McpFailure");
        assert_eq!(failure.kind, FailureKind::Transport);
        assert!(
            failure
                .stderr_tail
                .as_deref()
                .is_some_and(|tail| tail.contains("events_list_boom_stderr")),
            "expected stderr_tail to contain crash diagnostic, got {:?}",
            failure.stderr_tail
        );
        let _ = conn.close().await;
        drop(conn);
        // ByteLease must already be released even while a cloned McpEventsClient is still alive.
        assert_eq!(budget.used(), 0);
        drop(events_client);
    }

    #[test]
    fn handler_declares_events_and_tasks_and_strips_for_legacy_initialize() {
        let handler = Handler {
            tools_changed: Arc::new(AtomicBool::new(false)),
        };
        let info = handler.get_info();
        assert!(info.capabilities.elicitation.is_none());
        let exts = info.capabilities.extensions.as_ref().expect("extensions");
        assert!(exts.contains_key(EVENTS_EXTENSION_ID));
        assert!(exts.contains_key(TASKS_EXTENSION_ID));

        let mut legacy_init: ClientJsonRpcMessage = serde_json::from_value(serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-11-25",
                "capabilities": {
                    "extensions": {
                        "io.modelcontextprotocol/events": {},
                        "io.modelcontextprotocol/tasks": {}
                    }
                },
                "clientInfo": {"name": "mcpcli", "version": "0.7.0"}
            }
        }))
        .unwrap();
        strip_modern_extensions_for_legacy_initialize(&mut legacy_init);
        let ClientJsonRpcMessage::Request(req) = &legacy_init else {
            panic!("expected request");
        };
        let ClientRequest::InitializeRequest(init) = &req.request else {
            panic!("expected initialize");
        };
        assert!(init.params.capabilities.extensions.is_none());

        assert!(is_standard_notification("notifications/tasks"));
        for method in [
            "skills/list",
            "skills/get",
            "resources/directory/read",
            "tasks/get",
            "tasks/update",
            "tasks/cancel",
        ] {
            assert!(is_standard_method(method), "{method}");
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn call_with_options_leased_gates_schema_mismatch_and_unnegotiated_task() {
        let script = r#"
import json, sys
for line in sys.stdin:
    msg = json.loads(line)
    method = msg.get("method")
    if method == "server/discover":
        resp = {
            "jsonrpc": "2.0",
            "id": msg["id"],
            "result": {
                "resultType": "complete",
                "supportedVersions": ["2026-07-28"],
                "capabilities": {"tools": {}},
                "ttlMs": 60000,
                "cacheScope": "public",
                "_meta": {
                    "io.modelcontextprotocol/serverInfo": {
                        "name": "no-tasks-srv",
                        "version": "0.1.0"
                    }
                }
            }
        }
        sys.stdout.write(json.dumps(resp) + "\n")
        sys.stdout.flush()
    elif method == "tools/list":
        resp = {
            "jsonrpc": "2.0",
            "id": msg["id"],
            "result": {
                "resultType": "complete",
                "tools": [{
                    "name": "echo",
                    "description": "echo input",
                    "inputSchema": {
                        "type": "object",
                        "required": ["q"],
                        "properties": {"q": {"type": "string"}}
                    }
                }]
            }
        }
        sys.stdout.write(json.dumps(resp) + "\n")
        sys.stdout.flush()
    elif method == "tools/call":
        resp = {
            "jsonrpc": "2.0",
            "id": msg["id"],
            "result": {
                "resultType": "task",
                "taskId": "unneg-task-42",
                "status": "working",
                "createdAt": "2026-01-01T00:00:00Z",
                "lastUpdatedAt": "2026-01-01T00:00:00Z",
                "ttlMs": 60000
            }
        }
        sys.stdout.write(json.dumps(resp) + "\n")
        sys.stdout.flush()
"#;
        let config = ServerConfig::Stdio {
            command: "python3".into(),
            args: vec!["-u".into(), "-c".into(), script.into()],
            env: std::collections::BTreeMap::new(),
            cwd: None,
            description: None,
            enabled: true,
            timeout: None,
        };
        let budget = ByteBudget::new(1024 * 1024);
        let limits = McpLimits {
            budget: budget.clone(),
            ..McpLimits::default()
        };
        let mut conn = McpConnection::connect_with_limits(&config, Duration::from_secs(5), limits)
            .await
            .expect("connect to stdio server");

        let mut args = Map::new();
        args.insert("q".into(), Value::String("hello".into()));
        let budgets = CallBudgets {
            pre_dispatch_deadline: Instant::now() + Duration::from_secs(5),
            idle: Duration::from_secs(5),
            hard: None,
        };

        // 1. Schema mismatch fails pre-dispatch with exit code 2 and outcome_unknown: false.
        let mismatch = crate::schema::parse_expected_schema(
            serde_json::json!({
                "type": "object",
                "required": ["q"],
                "properties": {"q": {"type": "integer"}}
            }),
            crate::schema::SchemaMode::Compatible,
        )
        .unwrap();
        let dispatched = AtomicBool::new(false);
        let err = conn
            .call_with_options_leased(
                "echo",
                &args,
                CallOptions {
                    expected_schema: Some(&mismatch),
                },
                budgets,
                &dispatched,
                CancellationToken::new(),
            )
            .await
            .err()
            .expect("expected schema mismatch");
        assert_eq!(err.code, 2);
        assert_eq!(err.kind, "schema_mismatch");
        assert!(!err.outcome_unknown);
        assert!(!dispatched.load(Ordering::Acquire));

        // 2. Unnegotiated CreateTaskResult returns exit code 4, mcp_protocol, outcome_unknown: true, and data.taskId.
        let dispatched2 = AtomicBool::new(false);
        let err = conn
            .call_with_options_leased(
                "echo",
                &args,
                CallOptions::default(),
                budgets,
                &dispatched2,
                CancellationToken::new(),
            )
            .await
            .err()
            .expect("expected unnegotiated task rejection");
        assert_eq!(err.code, 4);
        assert_eq!(err.kind, "mcp_protocol");
        assert!(err.outcome_unknown);
        assert!(dispatched2.load(Ordering::Acquire));
        assert_eq!(
            err.data
                .as_deref()
                .and_then(|d| d.get("taskId"))
                .and_then(Value::as_str),
            Some("unneg-task-42")
        );

        let _ = conn.close().await;
        drop(conn);
        assert_eq!(budget.used(), 0);
    }
}
