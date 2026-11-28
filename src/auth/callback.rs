use super::challenge::{canonical_resource_url, constant_time_eq};
use crate::{discovery::safe_text, ipc::Failure};
use anyhow::Result;
use std::{io, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::timeout,
};

pub(super) fn validate_callback_params(
    url: &reqwest::Url,
    expected_state: &str,
    canonical_issuer: &str,
) -> Result<String> {
    let mut code = None;
    let mut state = None;
    let mut iss = None;
    let mut error = None;
    let mut error_desc = None;
    for (k, v) in url.query_pairs() {
        match k.as_ref() {
            "code" if code.is_none() => code = Some(v.into_owned()),
            "state" if state.is_none() => state = Some(v.into_owned()),
            "iss" if iss.is_none() => iss = Some(v.into_owned()),
            "error" if error.is_none() => error = Some(v.into_owned()),
            "error_description" if error_desc.is_none() => error_desc = Some(v.into_owned()),
            _ => {}
        }
    }
    let Some(got_state) = state else {
        return Err(Failure::new(4, "mcp_protocol", "OAuth callback is missing state").into());
    };
    if !constant_time_eq(got_state.as_bytes(), expected_state.as_bytes()) {
        return Err(Failure::new(4, "mcp_protocol", "OAuth callback state mismatch").into());
    }
    if let Some(got_iss) = iss {
        let normalized = canonical_resource_url(&got_iss).map_err(|_| {
            Failure::new(4, "mcp_protocol", "OAuth callback returned an invalid iss")
        })?;
        if normalized != canonical_issuer {
            return Err(Failure::new(
                4,
                "mcp_protocol",
                format!(
                    "OAuth callback issuer mismatch: expected {}, got {}",
                    safe_text(canonical_issuer),
                    safe_text(&normalized)
                ),
            )
            .into());
        }
    }
    if let Some(err_code) = error {
        let msg = match error_desc {
            Some(desc) if !desc.is_empty() => format!(
                "OAuth authorization failed: {} ({})",
                safe_text(&err_code),
                safe_text(&desc)
            ),
            _ => format!("OAuth authorization failed: {}", safe_text(&err_code)),
        };
        return Err(Failure::new(4, "mcp_protocol", msg).into());
    }
    let code = code.filter(|c| !c.is_empty()).ok_or_else(|| {
        Failure::new(
            4,
            "mcp_protocol",
            "OAuth callback did not include an authorization code",
        )
    })?;
    Ok(code)
}

pub(super) fn parse_pasted_callback(
    input: &str,
    expected_state: &str,
    canonical_issuer: &str,
) -> Result<String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(Failure::new(4, "mcp_protocol", "empty OAuth callback input").into());
    }
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        let parsed = reqwest::Url::parse(trimmed)
            .map_err(|_| Failure::new(4, "mcp_protocol", "invalid OAuth callback URL on stdin"))?;
        return validate_callback_params(&parsed, expected_state, canonical_issuer);
    }
    if trimmed.starts_with('?') || trimmed.contains('=') {
        let query = trimmed.trim_start_matches('?');
        let synthetic = reqwest::Url::parse(&format!("http://127.0.0.1/callback?{query}"))
            .map_err(|_| {
                Failure::new(4, "mcp_protocol", "invalid OAuth callback query on stdin")
            })?;
        if synthetic.query_pairs().any(|(k, _)| k == "state") {
            return validate_callback_params(&synthetic, expected_state, canonical_issuer);
        }
        if let Some((_, code)) = synthetic
            .query_pairs()
            .find(|(k, v)| k == "code" && !v.is_empty())
        {
            return Ok(code.into_owned());
        }
        return Err(Failure::new(4, "mcp_protocol", "OAuth callback query is missing code").into());
    }
    Ok(trimmed.to_owned())
}

async fn write_loopback_http(socket: &mut tokio::net::TcpStream, status: &str, body: &str) {
    let resp = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/plain; charset=utf-8\r\nCache-Control: no-store\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    let _ = socket.write_all(resp.as_bytes()).await;
    let _ = socket.flush().await;
    let _ = socket.shutdown().await;
}

pub(super) async fn accept_loopback_callback(
    listener: &TcpListener,
    callback_path: &str,
    expected_state: &str,
    canonical_issuer: &str,
) -> Result<String> {
    let callback_url = reqwest::Url::parse(&format!("http://127.0.0.1{callback_path}"))?;
    loop {
        let (mut socket, _) = listener
            .accept()
            .await
            .map_err(|e| Failure::new(1, "runtime", format!("loopback accept failed: {e}")))?;
        let mut buf = Vec::with_capacity(1024);
        let read_header = timeout(Duration::from_secs(5), async {
            let mut chunk = [0u8; 1024];
            loop {
                if buf.windows(4).any(|w| w == b"\r\n\r\n") || buf.windows(2).any(|w| w == b"\n\n")
                {
                    return Ok::<_, io::Error>(true);
                }
                if buf.len() >= 16 * 1024 {
                    return Ok(false);
                }
                let n = socket.read(&mut chunk).await?;
                if n == 0 {
                    return Ok(false);
                }
                buf.extend_from_slice(&chunk[..n]);
            }
        })
        .await;
        if !matches!(read_header, Ok(Ok(true))) {
            continue;
        }
        let header_text = String::from_utf8_lossy(&buf);
        let first_line = header_text.lines().next().unwrap_or("");
        let mut parts = first_line.split_whitespace();
        let method = parts.next().unwrap_or("");
        let target = parts.next().unwrap_or("");
        if method != "GET" || !target.starts_with('/') {
            write_loopback_http(&mut socket, "400 Bad Request", "Bad Request\n").await;
            continue;
        }
        let Ok(parsed) = reqwest::Url::parse(&format!("http://127.0.0.1{target}")) else {
            write_loopback_http(&mut socket, "400 Bad Request", "Bad Request\n").await;
            continue;
        };
        if parsed.path() != callback_url.path() {
            write_loopback_http(&mut socket, "404 Not Found", "Not Found\n").await;
            continue;
        }
        match validate_callback_params(&parsed, expected_state, canonical_issuer) {
            Ok(code) => {
                write_loopback_http(
                    &mut socket,
                    "200 OK",
                    "Authentication complete. You can close this window.\n",
                )
                .await;
                return Ok(code);
            }
            Err(err) => {
                write_loopback_http(
                    &mut socket,
                    "400 Bad Request",
                    "Authentication failed. Check the terminal for details.\n",
                )
                .await;
                return Err(err);
            }
        }
    }
}

#[cfg(unix)]
pub(super) async fn read_stdin_callback(
    expected_state: &str,
    canonical_issuer: &str,
) -> Result<String> {
    let mut line = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        let mut pfd = libc::pollfd {
            fd: libc::STDIN_FILENO,
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: `pfd` is a valid stack `pollfd` polling `STDIN_FILENO` with zero timeout.
        let ready = unsafe { libc::poll(&mut pfd, 1, 0) };
        if ready < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return std::future::pending().await;
        }
        if ready == 0
            || (pfd.revents & (libc::POLLIN | libc::POLLHUP | libc::POLLERR | libc::POLLNVAL)) == 0
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
            continue;
        }
        if pfd.revents & libc::POLLNVAL != 0 {
            return std::future::pending().await;
        }
        // SAFETY: `byte` is a valid 1-byte writable stack buffer.
        let n = unsafe { libc::read(libc::STDIN_FILENO, byte.as_mut_ptr().cast(), 1) };
        if n < 0 {
            let err = io::Error::last_os_error();
            if matches!(
                err.kind(),
                io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock
            ) {
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }
            return std::future::pending().await;
        }
        if n == 0 {
            if line.is_empty() {
                return std::future::pending().await;
            }
            break;
        }
        if byte[0] == b'\n' {
            break;
        }
        if line.len() >= 8 * 1024 {
            return Err(Failure::new(
                2,
                "arguments",
                "OAuth callback input on stdin exceeds 8 KiB",
            )
            .into());
        }
        line.push(byte[0]);
    }
    let text = String::from_utf8(line)
        .map_err(|_| Failure::new(2, "arguments", "OAuth callback input on stdin is not UTF-8"))?;
    if text.trim().is_empty() {
        return std::future::pending().await;
    }
    parse_pasted_callback(&text, expected_state, canonical_issuer)
}

#[cfg(not(unix))]
pub(super) async fn read_stdin_callback(
    expected_state: &str,
    canonical_issuer: &str,
) -> Result<String> {
    use std::io::Read as _;

    enum StdinLine {
        Line(Vec<u8>),
        TooLong,
        Pending,
    }

    let (tx, rx) = tokio::sync::oneshot::channel();
    let spawned = std::thread::Builder::new()
        .name("oauth-stdin".into())
        .spawn(move || {
            let mut stdin = io::stdin();
            let mut line = Vec::new();
            let mut byte = [0u8; 1];
            let outcome = loop {
                match stdin.read(&mut byte) {
                    Ok(0) => {
                        if line.is_empty() {
                            break StdinLine::Pending;
                        }
                        break StdinLine::Line(line);
                    }
                    Ok(_) => {
                        if byte[0] == b'\n' {
                            break StdinLine::Line(line);
                        }
                        if line.len() >= 8 * 1024 {
                            break StdinLine::TooLong;
                        }
                        line.push(byte[0]);
                    }
                    Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                    Err(_) => break StdinLine::Pending,
                }
            };
            let _ = tx.send(outcome);
        });
    if spawned.is_err() {
        return std::future::pending().await;
    }
    let line = match rx.await {
        Ok(StdinLine::Line(line)) => line,
        Ok(StdinLine::TooLong) => {
            return Err(Failure::new(
                2,
                "arguments",
                "OAuth callback input on stdin exceeds 8 KiB",
            )
            .into());
        }
        Ok(StdinLine::Pending) | Err(_) => return std::future::pending().await,
    };
    let text = String::from_utf8(line)
        .map_err(|_| Failure::new(2, "arguments", "OAuth callback input on stdin is not UTF-8"))?;
    if text.trim().is_empty() {
        return std::future::pending().await;
    }
    parse_pasted_callback(&text, expected_state, canonical_issuer)
}
