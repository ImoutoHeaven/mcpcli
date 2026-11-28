use super::callback::{accept_loopback_callback, parse_pasted_callback};
use super::challenge::{
    AuthChallengeInfo, canonical_resource_url, credential_storage_key, now_unix_secs,
    parse_www_authenticate,
};
use super::discovery::{
    authorization_server_metadata_urls, base64_standard, build_oauth_http_client,
    form_urlencode_component, protected_resource_metadata_urls, register_public_client,
    select_token_endpoint_auth_method,
};
use super::manager::{HttpAuthManager, LoginOptions, login, logout, status};
use super::store::{
    CredentialsStoreFile, StoredCredential, credential_lock, mutate_store, read_store_at,
};
#[cfg(unix)]
use super::store::{read_store_readonly, resolve_store_location_readonly};
use crate::{
    config::{OAuthConfig, ServerConfig},
    ipc::Failure,
    oauth_url::validate_oauth_endpoint_url,
    platform,
};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    io::Write,
    process::Stdio,
    sync::{Arc, OnceLock, atomic::Ordering},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::{Instant, timeout},
};

#[test]
fn canonical_resource_preserves_distinct_paths_and_queries() {
    assert_eq!(
        canonical_resource_url("HTTPS://Example.COM:443/mcp/#frag").unwrap(),
        "https://example.com/mcp/"
    );
    assert_eq!(
        canonical_resource_url("https://example.com/").unwrap(),
        "https://example.com"
    );
    assert_eq!(
        canonical_resource_url("http://127.0.0.1:8080/v1/mcp///").unwrap(),
        "http://127.0.0.1:8080/v1/mcp///"
    );
    let resources = [
        "https://example.com/mcp",
        "https://example.com/mcp/",
        "https://example.com/mcp//",
        "https://example.com/mcp?tenant=a",
        "https://example.com/mcp?tenant=b",
    ];
    let keys: std::collections::HashSet<_> = resources
        .iter()
        .map(|url| canonical_resource_url(url).unwrap())
        .collect();
    assert_eq!(keys.len(), resources.len());
    assert!(canonical_resource_url("https://user:pass@example.com/mcp").is_err());
    assert!(canonical_resource_url("ftp://example.com/mcp").is_err());
}

fn test_env_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

#[test]
fn oauth_endpoint_requires_https_or_literal_loopback() {
    for ok in [
        "https://auth.example.com/oauth/token",
        "http://127.0.0.1:9000/token",
        "http://127.0.0.99:9000/token",
        "http://[::1]:9000/token",
        "http://[::ffff:127.0.0.1]:9000/token",
        "http://localhost:9000/token",
        "http://sub.localhost:9000/token",
    ] {
        assert!(
            validate_oauth_endpoint_url(ok, "test", true).is_ok(),
            "{ok}"
        );
    }
    for bad in [
        "http://auth.example.com/token",
        "http://192.168.1.10:9000/token",
        "https://user:secret@auth.example.com/token",
        "https://auth.example.com/token#frag",
    ] {
        assert!(
            validate_oauth_endpoint_url(bad, "test", true).is_err(),
            "{bad}"
        );
    }

    assert!(
        validate_oauth_endpoint_url("https://auth.example.com/oauth/token", "test", false).is_ok()
    );
    for ssrf in [
        "http://auth.example.com/oauth/token",
        "http://127.0.0.1:9000/token",
        "https://127.0.0.1:9000/token",
        "https://127.255.255.254:9000/token",
        "http://[::1]:9000/token",
        "https://[::1]:9000/token",
        "https://[::ffff:127.0.0.1]:9000/token",
        "http://localhost:9000/token",
        "https://localhost:9000/token",
        "https://api.localhost/token",
        "https://10.0.0.1/token",
        "https://172.16.0.1/token",
        "https://192.168.1.1/token",
        "https://169.254.169.254/latest/meta-data",
        "https://100.100.100.200/latest/meta-data",
        "https://[fd00:ec2::254]/latest/meta-data",
        "https://[fe80::1]/token",
        "https://metadata.google.internal/computeMetadata/v1",
    ] {
        assert!(
            validate_oauth_endpoint_url(ssrf, "test", false).is_err(),
            "expected SSRF block for {ssrf}"
        );
    }

    let remote_srv = reqwest::Url::parse("https://remote.example/mcp").unwrap();
    assert!(
        protected_resource_metadata_urls(
            &remote_srv,
            Some("http://127.0.0.1:9000/.well-known/oauth-protected-resource"),
        )
        .is_err()
    );
    assert!(
        protected_resource_metadata_urls(
            &remote_srv,
            Some("https://127.0.0.1:9000/.well-known/oauth-protected-resource"),
        )
        .is_err()
    );
}

#[test]
fn www_authenticate_parser_extracts_bearer_params_and_ignores_realm() {
    let mut headers = http::HeaderMap::new();
    headers.append(
        http::header::WWW_AUTHENTICATE,
        http::HeaderValue::from_static(
            r#"Basic realm="secret-realm", Bearer realm="also-secret", resource_metadata="https://api.example.com/.well-known/oauth-protected-resource/mcp", scope="mcp:tools mcp:events", error="insufficient_scope", error_description="Need \"mcp:events\"""#,
        ),
    );
    let parsed = parse_www_authenticate(&headers);
    assert_eq!(
        parsed.resource_metadata.as_deref(),
        Some("https://api.example.com/.well-known/oauth-protected-resource/mcp")
    );
    assert_eq!(parsed.scope.as_deref(), Some("mcp:tools mcp:events"));
    assert_eq!(parsed.error.as_deref(), Some("insufficient_scope"));
    assert_eq!(
        parsed.error_description.as_deref(),
        Some("Need \"mcp:events\"")
    );

    let info = AuthChallengeInfo::from_response(http::StatusCode::FORBIDDEN, &headers);
    let msg = info.format_message(Some("github"));
    assert!(msg.contains("403"));
    assert!(msg.contains("run: mcpcli auth login github"));
    assert!(!msg.contains("secret-realm") && !msg.contains("also-secret"));
    let data = info.to_data_value(Some("github"));
    assert_eq!(data["status"], 403);
    assert_eq!(data["requiredScopes"], json!(["mcp:tools", "mcp:events"]));
    assert_eq!(data["hint"], "run: mcpcli auth login github");
}

#[test]
fn discovery_url_sequences_follow_rfc9728_and_rfc8414() {
    let srv = reqwest::Url::parse("https://api.example.com/v1/mcp").unwrap();
    let prm_urls: Vec<String> = protected_resource_metadata_urls(&srv, None)
        .unwrap()
        .into_iter()
        .map(|u| u.into())
        .collect();
    assert_eq!(
        prm_urls,
        vec![
            "https://api.example.com/.well-known/oauth-protected-resource/v1/mcp",
            "https://api.example.com/.well-known/oauth-protected-resource",
        ]
    );

    let resource = reqwest::Url::parse("https://api.example.com/mcp/?tenant=a%2Fb").unwrap();
    let metadata_urls: Vec<String> = protected_resource_metadata_urls(&resource, None)
        .unwrap()
        .into_iter()
        .map(Into::into)
        .collect();
    assert_eq!(
        metadata_urls,
        [
            "https://api.example.com/.well-known/oauth-protected-resource/mcp/?tenant=a%2Fb",
            "https://api.example.com/.well-known/oauth-protected-resource?tenant=a%2Fb",
        ]
    );
    let root = reqwest::Url::parse("https://api.example.com/?tenant=a").unwrap();
    assert_eq!(
        protected_resource_metadata_urls(&root, None).unwrap()[0].as_str(),
        "https://api.example.com/.well-known/oauth-protected-resource?tenant=a",
    );

    let tenant_issuer = reqwest::Url::parse("https://auth.example.com/tenant").unwrap();
    let asm_urls: Vec<String> = authorization_server_metadata_urls(&tenant_issuer)
        .into_iter()
        .map(|u| u.into())
        .collect();
    assert_eq!(
        asm_urls,
        vec![
            "https://auth.example.com/.well-known/oauth-authorization-server/tenant",
            "https://auth.example.com/.well-known/openid-configuration/tenant",
            "https://auth.example.com/tenant/.well-known/openid-configuration",
        ]
    );
}

#[test]
fn pasted_callback_validates_state_and_issuer() {
    let code = parse_pasted_callback(
        "http://127.0.0.1:43210/callback?code=ac_123&state=st_ok&iss=https%3A%2F%2Fauth.example.com%2F",
        "st_ok",
        "https://auth.example.com",
    )
    .unwrap();
    assert_eq!(code, "ac_123");

    assert!(
        parse_pasted_callback(
            "http://127.0.0.1:43210/callback?code=ac_123&state=wrong",
            "st_ok",
            "https://auth.example.com",
        )
        .is_err()
    );
    assert!(
        parse_pasted_callback(
            "http://127.0.0.1:43210/callback?code=ac_123&state=st_ok&iss=https%3A%2F%2Fevil.example.com",
            "st_ok",
            "https://auth.example.com",
        )
        .is_err()
    );
    assert_eq!(
        parse_pasted_callback("raw_code_only", "st_ok", "https://auth.example.com").unwrap(),
        "raw_code_only"
    );

    let mut spnego = http::HeaderMap::new();
    spnego.append(
        http::header::WWW_AUTHENTICATE,
        http::HeaderValue::from_static(r#"Negotiate YII/abc+def==, Bearer scope="mcp:read""#),
    );
    assert_eq!(
        parse_www_authenticate(&spnego).scope.as_deref(),
        Some("mcp:read")
    );
}

async fn read_http_body(socket: &mut tokio::net::TcpStream) -> Vec<u8> {
    use tokio::io::{AsyncBufReadExt, BufReader};
    let mut reader = BufReader::new(socket);
    let mut length = 0;
    loop {
        let mut line = String::new();
        assert_ne!(reader.read_line(&mut line).await.unwrap(), 0);
        if line == "\r\n" {
            break;
        }
        if let Some((name, value)) = line.split_once(':')
            && name.eq_ignore_ascii_case("content-length")
        {
            length = value.trim().parse().unwrap();
        }
    }
    let mut body = vec![0; length];
    reader.read_exact(&mut body).await.unwrap();
    body
}

#[tokio::test]
async fn login_deadline_includes_metadata_response_body() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let resource = format!("http://{}/mcp", listener.local_addr().unwrap());
    let (headers_sent, received_headers) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        for is_metadata in [false, true] {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_http_body(&mut socket).await;
            if is_metadata {
                socket
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n")
                    .await
                    .unwrap();
                headers_sent.send(()).unwrap();
                std::future::pending::<()>().await;
                return;
            }
            socket
                .write_all(
                    b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await
                .unwrap();
        }
    });
    let login = tokio::spawn(async move {
        super::login(LoginOptions {
            server_name: "slow-metadata",
            server_url: &resource,
            scopes: &[],
            configured_scopes: &[],
            client_id: None,
            configured_client_id: None,
            client_secret: None,
            client_name: None,
            configured_client_name: None,
            callback_port: None,
            callback_path: None,
            auth_server_metadata_url: None,
            account: None,
            no_browser: true,
            budget: Duration::from_secs(1),
        })
        .await
    });
    timeout(Duration::from_secs(5), received_headers)
        .await
        .unwrap()
        .unwrap();
    let error = timeout(Duration::from_secs(5), login)
        .await
        .expect("login did not enforce its total deadline")
        .unwrap()
        .unwrap_err();
    assert_eq!(error.downcast_ref::<Failure>().unwrap().kind, "timeout");
    server.abort();
}

#[tokio::test]
async fn refresh_deadline_includes_token_response_body() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let token_endpoint = format!("http://{}/token", listener.local_addr().unwrap());
    let (headers_sent, received_headers) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_http_body(&mut socket).await;
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        headers_sent.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    let refresh = tokio::spawn(async move {
        let manager = HttpAuthManager::for_server("http://127.0.0.1/mcp", None).unwrap();
        let credential = StoredCredential {
            resource: "http://127.0.0.1/mcp".into(),
            issuer: "http://127.0.0.1".into(),
            token_endpoint,
            revocation_endpoint: None,
            client_id: "client".into(),
            token_endpoint_auth_method: None,
            access_token: "access".into(),
            refresh_token: Some("refresh".into()),
            expires_at: Some(0),
            scopes: vec![],
        };
        manager
            .exchange_refresh_token(&reqwest::Client::new(), &credential, false)
            .await
    });
    timeout(Duration::from_secs(5), received_headers)
        .await
        .unwrap()
        .unwrap();
    let error = timeout(Duration::from_secs(20), refresh)
        .await
        .expect("refresh did not enforce its total deadline")
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("timed out"));
    server.abort();
}

#[tokio::test]
async fn singleflight_refresh_rotates_once_and_writes_private_credentials() {
    use std::sync::atomic::AtomicUsize;

    let _env_guard = test_env_lock().lock().await;
    let refresh_hits = Arc::new(AtomicUsize::new(0));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let token_endpoint = format!("http://127.0.0.1:{}/oauth/token", addr.port());
    let hits_srv = refresh_hits.clone();
    let srv = tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let hits_srv = hits_srv.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 4096];
                let n = sock.read(&mut buf).await.unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]);
                assert!(req.contains("grant_type=refresh_token"));
                assert!(req.contains("client_id=cli-test"));
                assert!(req.contains("resource=http%3A%2F%2F127.0.0.1%3A7777%2Fmcp"));
                tokio::time::sleep(Duration::from_millis(40)).await;
                let n_call = hits_srv.fetch_add(1, Ordering::SeqCst) + 1;
                let body = serde_json::to_string(&json!({
                    "access_token": format!("fresh_tok_{n_call}"),
                    "token_type": "Bearer",
                    "refresh_token": format!("fresh_rt_{n_call}"),
                    "expires_in": 3600,
                    "scope": "mcp:read mcp:write"
                }))
                .unwrap();
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(resp.as_bytes()).await;
            });
        }
    });

    #[cfg(unix)]
    let dir = tempfile::tempdir_in("/tmp").unwrap();
    #[cfg(windows)]
    let dir = tempfile::tempdir().unwrap();
    let cred_path = dir.path().canonicalize().unwrap().join("credentials.json");
    let initial = CredentialsStoreFile {
        version: 1,
        resources: BTreeMap::from([(
            "http://127.0.0.1:7777/mcp".into(),
            StoredCredential {
                resource: "http://127.0.0.1:7777/mcp".into(),
                issuer: format!("http://127.0.0.1:{}", addr.port()),
                token_endpoint,
                revocation_endpoint: None,
                client_id: "cli-test".into(),
                token_endpoint_auth_method: None,
                access_token: "expired_tok".into(),
                refresh_token: Some("initial_rt".into()),
                expires_at: Some(now_unix_secs().saturating_add(5)),
                scopes: vec!["mcp:read".into()],
            },
        )]),
    };
    {
        let mut f = platform::new_private_file(&cred_path).unwrap();
        f.write_all(&serde_json::to_vec(&initial).unwrap()).unwrap();
        f.sync_all().unwrap();
    }
    // SAFETY: serialized across tests in this module via `test_env_lock`.
    unsafe {
        std::env::set_var("MCPCLI_CREDENTIALS", &cred_path);
    }

    let mgr = HttpAuthManager::for_server("http://127.0.0.1:7777/mcp", None).unwrap();
    let client =
        build_oauth_http_client("http://127.0.0.1:7777/mcp", true, Duration::from_secs(5)).unwrap();
    let mut tasks = Vec::new();
    for _ in 0..6 {
        let mgr = mgr.clone();
        let client = client.clone();
        tasks.push(tokio::spawn(
            async move { mgr.prepare_token(&client).await },
        ));
    }
    for task in tasks {
        let (tok, attempted) = task.await.unwrap();
        assert!(attempted);
        assert_eq!(tok.as_deref(), Some("fresh_tok_1"));
    }
    assert_eq!(refresh_hits.load(Ordering::SeqCst), 1);

    let persisted = read_store_at(&cred_path).unwrap();
    let saved = &persisted.resources["http://127.0.0.1:7777/mcp"];
    assert_eq!(saved.access_token, "fresh_tok_1");
    assert_eq!(saved.refresh_token.as_deref(), Some("fresh_rt_1"));
    assert_eq!(saved.scopes, vec!["mcp:read", "mcp:write"]);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&cred_path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }

    // SAFETY: serialized across tests in this module via `test_env_lock`.
    unsafe {
        std::env::remove_var("MCPCLI_CREDENTIALS");
    }
    srv.abort();
}

#[tokio::test]
async fn credential_lock_is_stable_across_store_filename_case() {
    let _env_guard = test_env_lock().lock().await;
    #[cfg(unix)]
    let dir = tempfile::tempdir_in("/tmp").unwrap();
    #[cfg(windows)]
    let dir = tempfile::tempdir().unwrap();
    let parent = dir.path().canonicalize().unwrap();
    let previous = std::env::var_os("MCPCLI_CREDENTIALS");
    // SAFETY: serialized across tests in this module via `test_env_lock`.
    unsafe { std::env::set_var("MCPCLI_CREDENTIALS", parent.join("Credentials.json")) };
    let first = credential_lock("https://example.com/mcp").await.unwrap();
    // SAFETY: serialized across tests in this module via `test_env_lock`.
    unsafe { std::env::set_var("MCPCLI_CREDENTIALS", parent.join("credentials.json")) };
    let second = credential_lock("https://example.com/mcp");
    tokio::pin!(second);
    assert!(
        timeout(Duration::from_millis(100), &mut second)
            .await
            .is_err()
    );
    drop(first);
    let second = timeout(Duration::from_secs(5), second)
        .await
        .unwrap()
        .unwrap();
    drop(second);
    // SAFETY: serialized across tests in this module via `test_env_lock`.
    unsafe {
        match previous {
            Some(value) => std::env::set_var("MCPCLI_CREDENTIALS", value),
            None => std::env::remove_var("MCPCLI_CREDENTIALS"),
        }
    }
}

#[tokio::test]
async fn logout_deadline_bounds_locks_and_best_effort_revocation() {
    let _env_guard = test_env_lock().lock().await;
    #[cfg(unix)]
    let dir = tempfile::tempdir_in("/tmp").unwrap();
    #[cfg(windows)]
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().canonicalize().unwrap().join("credentials.json");
    let previous = std::env::var_os("MCPCLI_CREDENTIALS");
    // SAFETY: serialized across tests in this module via `test_env_lock`.
    unsafe { std::env::set_var("MCPCLI_CREDENTIALS", &path) };
    let resource = "http://127.0.0.1/mcp";
    let guard = credential_lock(resource).await.unwrap();
    // An empty store makes logout a no-op even while a first login is in progress.
    let empty = timeout(
        Duration::from_secs(1),
        logout("server", resource, None, true, Duration::from_secs(1)),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(empty.0["loggedOut"], true);
    assert!(!path.exists());
    drop(guard);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let revocation = format!("http://{}/revoke", listener.local_addr().unwrap());
    mutate_store(|store| {
        store.resources.insert(
            resource.into(),
            StoredCredential {
                resource: resource.into(),
                issuer: "http://127.0.0.1".into(),
                token_endpoint: "http://127.0.0.1/token".into(),
                revocation_endpoint: Some(revocation),
                client_id: "client".into(),
                token_endpoint_auth_method: None,
                access_token: "access".into(),
                refresh_token: Some("refresh".into()),
                expires_at: None,
                scopes: vec![],
            },
        );
        Ok(())
    })
    .await
    .unwrap();
    let guard = credential_lock(resource).await.unwrap();
    let waiting = timeout(
        Duration::from_secs(2),
        logout("server", resource, None, true, Duration::from_millis(100)),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(waiting.downcast_ref::<Failure>().unwrap().code, 5);
    assert!(
        read_store_at(&path)
            .unwrap()
            .resources
            .contains_key(resource)
    );
    drop(guard);

    let (sent, received) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_http_body(&mut socket).await;
        sent.send(()).unwrap();
        tokio::time::sleep(Duration::from_secs(10)).await;
        let _ = socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .await;
    });
    let logged_out = timeout(
        Duration::from_secs(3),
        logout("server", resource, None, true, Duration::from_secs(1)),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(logged_out.0["loggedOut"], true);
    received.await.unwrap();
    assert!(read_store_at(&path).unwrap().resources.is_empty());
    server.abort();
    // SAFETY: serialized across tests in this module via `test_env_lock`.
    unsafe {
        match previous {
            Some(value) => std::env::set_var("MCPCLI_CREDENTIALS", value),
            None => std::env::remove_var("MCPCLI_CREDENTIALS"),
        }
    }
}

#[tokio::test]
async fn logout_waits_for_refresh_then_removes_the_rotated_credential() {
    let _env_guard = test_env_lock().lock().await;
    #[cfg(unix)]
    let dir = tempfile::tempdir_in("/tmp").unwrap();
    #[cfg(windows)]
    let dir = tempfile::tempdir().unwrap();
    let credential_path = dir.path().canonicalize().unwrap().join("credentials.json");
    let previous = std::env::var_os("MCPCLI_CREDENTIALS");
    // SAFETY: serialized across tests in this module via `test_env_lock`.
    unsafe { std::env::set_var("MCPCLI_CREDENTIALS", &credential_path) };

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/token", listener.local_addr().unwrap());
    let resource = "http://127.0.0.1/mcp";
    mutate_store(|store| {
        store.resources.insert(
            resource.into(),
            StoredCredential {
                resource: resource.into(),
                issuer: "http://127.0.0.1".into(),
                token_endpoint: endpoint,
                revocation_endpoint: None,
                client_id: "client".into(),
                token_endpoint_auth_method: None,
                access_token: "expired-access".into(),
                refresh_token: Some("refresh".into()),
                expires_at: Some(0),
                scopes: vec![],
            },
        );
        Ok(())
    })
    .await
    .unwrap();
    let (requested, request_received) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_http_body(&mut socket).await;
        requested.send(()).unwrap();
        released.await.unwrap();
        let body =
            r#"{"access_token":"fresh-access","refresh_token":"fresh-refresh","expires_in":3600}"#;
        socket.write_all(format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        ).as_bytes()).await.unwrap();
    });
    let refresh = tokio::spawn(async move {
        HttpAuthManager::for_server(resource, None)
            .unwrap()
            .prepare_token(&reqwest::Client::new())
            .await
    });
    timeout(Duration::from_secs(5), request_received)
        .await
        .unwrap()
        .unwrap();
    let mut logout = tokio::spawn(super::logout(
        "server",
        resource,
        None,
        true,
        Duration::from_secs(5),
    ));
    let finished_early = timeout(Duration::from_millis(100), &mut logout)
        .await
        .is_ok();
    release.send(()).unwrap();
    let refreshed = refresh.await.unwrap();
    if !finished_early {
        logout.await.unwrap().unwrap();
    }
    let saved = read_store_at(&credential_path).unwrap();
    server.await.unwrap();
    // SAFETY: serialized across tests in this module via `test_env_lock`.
    unsafe {
        match previous {
            Some(value) => std::env::set_var("MCPCLI_CREDENTIALS", value),
            None => std::env::remove_var("MCPCLI_CREDENTIALS"),
        }
    }
    assert!(
        !finished_early,
        "logout must serialize with an ongoing refresh"
    );
    assert_eq!(refreshed.0.as_deref(), Some("fresh-access"));
    assert!(
        saved.resources.is_empty(),
        "a completed refresh must not restore logged-out credentials"
    );
}

#[tokio::test]
async fn remote_server_never_contacts_loopback_during_refresh_or_logout() {
    use std::sync::atomic::AtomicUsize;

    let _env_guard = test_env_lock().lock().await;
    let canary_hits = Arc::new(AtomicUsize::new(0));
    let canary = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = canary.local_addr().unwrap().port();
    let hits_clone = canary_hits.clone();
    let srv = tokio::spawn(async move {
        while canary.accept().await.is_ok() {
            hits_clone.fetch_add(1, Ordering::SeqCst);
        }
    });

    #[cfg(unix)]
    let dir = tempfile::tempdir_in("/tmp").unwrap();
    #[cfg(windows)]
    let dir = tempfile::tempdir().unwrap();
    let cred_path = dir.path().canonicalize().unwrap().join("credentials.json");
    let initial = CredentialsStoreFile {
        version: 1,
        resources: BTreeMap::from([(
            "https://remote.example/mcp".into(),
            StoredCredential {
                resource: "https://remote.example/mcp".into(),
                issuer: "https://auth.remote.example".into(),
                token_endpoint: format!("https://127.0.0.1:{port}/oauth/token"),
                revocation_endpoint: Some(format!("https://127.0.0.1:{port}/oauth/revoke")),
                client_id: "cli-remote".into(),
                token_endpoint_auth_method: None,
                access_token: "stale_at".into(),
                refresh_token: Some("stale_rt".into()),
                expires_at: Some(now_unix_secs().saturating_sub(10)),
                scopes: vec!["mcp:read".into()],
            },
        )]),
    };
    {
        let mut f = platform::new_private_file(&cred_path).unwrap();
        f.write_all(&serde_json::to_vec(&initial).unwrap()).unwrap();
        f.sync_all().unwrap();
    }
    // SAFETY: serialized across tests in this module via `test_env_lock`.
    unsafe {
        std::env::set_var("MCPCLI_CREDENTIALS", &cred_path);
    }

    let mgr = HttpAuthManager::for_server("https://remote.example/mcp", None).unwrap();
    let fallback = reqwest::Client::new();
    assert_eq!(mgr.prepare_token(&fallback).await, (None, true));
    assert_eq!(
        mgr.refresh_after_401(&fallback, Some("stale_at")).await,
        None
    );
    let (logout_json, _) = logout(
        "remote",
        "https://remote.example/mcp",
        None,
        true,
        Duration::from_secs(2),
    )
    .await
    .unwrap();
    assert_eq!(logout_json["loggedOut"], true);
    assert_eq!(canary_hits.load(Ordering::SeqCst), 0);

    // SAFETY: serialized across tests in this module via `test_env_lock`.
    unsafe {
        std::env::remove_var("MCPCLI_CREDENTIALS");
    }
    srv.abort();
}

#[tokio::test]
async fn invalid_grant_refresh_clears_refresh_token_and_withholds_expired_access_token() {
    use std::sync::atomic::AtomicUsize;

    let _env_guard = test_env_lock().lock().await;
    let refresh_hits = Arc::new(AtomicUsize::new(0));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let token_endpoint = format!("http://127.0.0.1:{port}/oauth/token");
    let hits_srv = refresh_hits.clone();
    let srv = tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let hits_srv = hits_srv.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 4096];
                let _ = sock.read(&mut buf).await;
                hits_srv.fetch_add(1, Ordering::SeqCst);
                let body = r#"{"error":"invalid_grant","error_description":"revoked"}"#;
                let resp = format!(
                    "HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(resp.as_bytes()).await;
            });
        }
    });

    #[cfg(unix)]
    let dir = tempfile::tempdir_in("/tmp").unwrap();
    #[cfg(windows)]
    let dir = tempfile::tempdir().unwrap();
    let cred_path = dir.path().canonicalize().unwrap().join("credentials.json");
    let resource_url = "http://127.0.0.1:7778/mcp";
    let initial = CredentialsStoreFile {
        version: 1,
        resources: BTreeMap::from([(
            resource_url.into(),
            StoredCredential {
                resource: resource_url.into(),
                issuer: format!("http://127.0.0.1:{port}"),
                token_endpoint: token_endpoint.clone(),
                revocation_endpoint: None,
                client_id: "cli-test".into(),
                token_endpoint_auth_method: None,
                access_token: "expired_access_tok".into(),
                refresh_token: Some("revoked_refresh_tok".into()),
                expires_at: Some(now_unix_secs().saturating_sub(15)),
                scopes: vec!["mcp:read".into()],
            },
        )]),
    };
    {
        let mut f = platform::new_private_file(&cred_path).unwrap();
        f.write_all(&serde_json::to_vec(&initial).unwrap()).unwrap();
        f.sync_all().unwrap();
    }
    // SAFETY: serialized across tests in this module via `test_env_lock`.
    unsafe {
        std::env::set_var("MCPCLI_CREDENTIALS", &cred_path);
    }

    let servers = BTreeMap::from([(
        "srv".to_owned(),
        ServerConfig::Http {
            url: resource_url.to_owned(),
            headers: BTreeMap::new(),
            description: None,
            enabled: true,
            timeout: None,
            oauth: None,
        },
    )]);
    assert_eq!(
        status(&servers, Some("srv")).unwrap().0["authenticated"],
        true
    );

    let mgr = HttpAuthManager::for_server(resource_url, None).unwrap();
    let client = build_oauth_http_client(resource_url, true, Duration::from_secs(5)).unwrap();
    assert_eq!(mgr.prepare_token(&client).await, (None, true));
    assert_eq!(refresh_hits.load(Ordering::SeqCst), 1);

    let saved = &read_store_at(&cred_path).unwrap().resources[resource_url];
    assert!(saved.refresh_token.is_none());
    assert_eq!(
        status(&servers, Some("srv")).unwrap().0["authenticated"],
        false
    );

    assert_eq!(mgr.prepare_token(&client).await, (None, false));
    assert_eq!(refresh_hits.load(Ordering::SeqCst), 1);

    // Reactive 401 refresh with future expires_at must also flip status to unauthenticated.
    let reactive = CredentialsStoreFile {
        version: 1,
        resources: BTreeMap::from([(
            resource_url.into(),
            StoredCredential {
                resource: resource_url.into(),
                issuer: format!("http://127.0.0.1:{port}"),
                token_endpoint,
                revocation_endpoint: None,
                client_id: "cli-test".into(),
                token_endpoint_auth_method: None,
                access_token: "revoked_upstream_tok".into(),
                refresh_token: Some("revoked_refresh_tok_2".into()),
                expires_at: Some(now_unix_secs().saturating_add(3600)),
                scopes: vec!["mcp:read".into()],
            },
        )]),
    };
    {
        std::fs::remove_file(&cred_path).unwrap();
        let mut f = platform::new_private_file(&cred_path).unwrap();
        f.write_all(&serde_json::to_vec(&reactive).unwrap())
            .unwrap();
        f.sync_all().unwrap();
    }
    assert_eq!(
        mgr.refresh_after_401(&client, Some("revoked_upstream_tok"))
            .await,
        None
    );
    assert_eq!(
        status(&servers, Some("srv")).unwrap().0["authenticated"],
        false
    );

    // SAFETY: serialized across tests in this module via `test_env_lock`.
    unsafe {
        std::env::remove_var("MCPCLI_CREDENTIALS");
    }
    srv.abort();
}

#[cfg(unix)]
#[tokio::test]
async fn xdg_state_path_selection_is_consistent_for_reads_and_writes() {
    const CHILD_MARKER: &str = "MCPCLI_TEST_XDG_STATE_CHILD";
    if std::env::var_os(CHILD_MARKER).as_deref() != Some(std::ffi::OsStr::new("1")) {
        let output = tokio::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "auth::tests::xdg_state_path_selection_is_consistent_for_reads_and_writes",
                "--nocapture",
            ])
            .env(CHILD_MARKER, "1")
            .output()
            .await
            .unwrap();
        assert!(
            output.status.success(),
            "isolated XDG state test failed: {}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    let home_dir = tempfile::tempdir_in("/tmp").unwrap();
    let canon_home = home_dir.path().canonicalize().unwrap();

    let prev_creds = std::env::var_os("MCPCLI_CREDENTIALS");
    let prev_rt = std::env::var_os("MCPCLI_RUNTIME_DIR");
    let prev_xdg = std::env::var_os("XDG_STATE_HOME");
    let prev_home = std::env::var_os("HOME");

    // SAFETY: this test runs alone in a child process with its own environment.
    unsafe {
        std::env::remove_var("MCPCLI_CREDENTIALS");
        std::env::remove_var("MCPCLI_RUNTIME_DIR");
        std::env::set_var("XDG_STATE_HOME", "relative/xdg-state");
        std::env::set_var("HOME", &canon_home);
    }

    mutate_store(|store| {
        store.resources.insert(
            "https://example.com/mcp".into(),
            StoredCredential {
                resource: "https://example.com/mcp".into(),
                issuer: "https://auth.example.com".into(),
                token_endpoint: "https://auth.example.com/token".into(),
                revocation_endpoint: None,
                client_id: "cid".into(),
                token_endpoint_auth_method: None,
                access_token: "tok".into(),
                refresh_token: None,
                expires_at: Some(now_unix_secs().saturating_add(600)),
                scopes: vec!["mcp:read".into()],
            },
        );
        Ok(())
    })
    .await
    .unwrap();

    let expected = canon_home
        .join(".local")
        .join("state")
        .join("mcpcli")
        .join("credentials.json");
    assert!(expected.is_file());
    assert_eq!(
        resolve_store_location_readonly().unwrap().as_deref(),
        Some(expected.as_path())
    );

    let unusable_state = canon_home.join("state-file");
    std::fs::write(&unusable_state, b"regular file").unwrap();
    let original = std::fs::read(&expected).unwrap();
    // SAFETY: this test runs alone in a child process with its own environment.
    unsafe {
        std::env::set_var("XDG_STATE_HOME", &unusable_state);
    }
    assert!(
        mutate_store(|store| {
            store.resources.clear();
            Ok(())
        })
        .await
        .is_err()
    );
    assert_eq!(std::fs::read(&expected).unwrap(), original);
    assert!(read_store_readonly().unwrap().resources.is_empty());

    // SAFETY: this test runs alone in a child process with its own environment.
    unsafe {
        match prev_creds {
            Some(v) => std::env::set_var("MCPCLI_CREDENTIALS", v),
            None => std::env::remove_var("MCPCLI_CREDENTIALS"),
        }
        match prev_rt {
            Some(v) => std::env::set_var("MCPCLI_RUNTIME_DIR", v),
            None => std::env::remove_var("MCPCLI_RUNTIME_DIR"),
        }
        match prev_xdg {
            Some(v) => std::env::set_var("XDG_STATE_HOME", v),
            None => std::env::remove_var("XDG_STATE_HOME"),
        }
        match prev_home {
            Some(v) => std::env::set_var("HOME", v),
            None => std::env::remove_var("HOME"),
        }
    }
}

async fn read_http_request(
    socket: &mut tokio::net::TcpStream,
) -> (String, Vec<(String, String)>, Vec<u8>) {
    use tokio::io::{AsyncBufReadExt, BufReader};
    let mut reader = BufReader::new(socket);
    let mut request_line = String::new();
    reader.read_line(&mut request_line).await.unwrap();
    let mut headers = Vec::new();
    let mut length = 0usize;
    loop {
        let mut line = String::new();
        let n = reader.read_line(&mut line).await.unwrap();
        if n == 0 || line == "\r\n" || line == "\n" {
            break;
        }
        if let Some((k, v)) = line.split_once(':') {
            let key = k.trim().to_ascii_lowercase();
            let val = v.trim().to_owned();
            if key == "content-length" {
                length = val.parse().unwrap_or(0);
            }
            headers.push((key, val));
        }
    }
    let mut body = vec![0u8; length];
    if length > 0 {
        reader.read_exact(&mut body).await.unwrap();
    }
    (request_line.trim().to_owned(), headers, body)
}

#[tokio::test]
async fn confidential_login_helper_entry() {
    let Ok(metadata_url) = std::env::var("MCPCLI_TEST_AUTH_METADATA_URL") else {
        return;
    };
    let (result, _) = login(LoginOptions {
        server_name: "conf-srv",
        server_url: "http://127.0.0.1:9999/mcp",
        scopes: &["mcp:tools".into(), "mcp:write".into()],
        configured_scopes: &["mcp:read".into(), "mcp:tools".into()],
        client_id: None,
        configured_client_id: Some(" cid-conf "),
        client_secret: Some("sec:ret+1"),
        client_name: None,
        configured_client_name: None,
        callback_port: None,
        callback_path: Some("/oauth/./callback"),
        auth_server_metadata_url: Some(&metadata_url),
        account: Some("acct1"),
        no_browser: true,
        budget: Duration::from_secs(10),
    })
    .await
    .unwrap();
    assert_eq!(result["authenticated"], true);
}

#[tokio::test]
async fn confidential_client_basic_and_post_across_login_refresh_and_logout() {
    use tokio::io::AsyncBufReadExt;
    let _env_guard = test_env_lock().lock().await;
    #[cfg(unix)]
    let dir = tempfile::tempdir_in("/tmp").unwrap();
    #[cfg(windows)]
    let dir = tempfile::tempdir().unwrap();
    let cred_path = dir.path().canonicalize().unwrap().join("credentials.json");
    let previous = std::env::var_os("MCPCLI_CREDENTIALS");
    // SAFETY: serialized across tests in this module via `test_env_lock`.
    unsafe { std::env::set_var("MCPCLI_CREDENTIALS", &cred_path) };

    for (auth_method, supported_methods) in [
        ("client_secret_basic", None::<Vec<&str>>),
        ("client_secret_post", Some(vec!["client_secret_post"])),
    ] {
        let _ = std::fs::remove_file(&cred_path);
        let as_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let as_port = as_listener.local_addr().unwrap().port();
        let issuer = format!("http://127.0.0.1:{as_port}/tenant");
        let client_id = " cid-conf ";
        let expected_basic = format!(
            "Basic {}",
            base64_standard(
                format!(
                    "{}:{}",
                    form_urlencode_component(client_id),
                    form_urlencode_component("sec:ret+1")
                )
                .as_bytes()
            )
        );
        let seen_token_auth =
            Arc::new(std::sync::Mutex::new(
                Vec::<(String, Option<String>, String)>::new(),
            ));
        let seen_clone = seen_token_auth.clone();
        let server_issuer = issuer.clone();
        let as_srv = tokio::spawn(async move {
            while let Ok((mut sock, _)) = as_listener.accept().await {
                let seen_clone = seen_clone.clone();
                let issuer = server_issuer.clone();
                let supported_methods = supported_methods.clone();
                tokio::spawn(async move {
                    let (req_line, headers, body) = read_http_request(&mut sock).await;
                    let auth_hdr = headers
                        .iter()
                        .find(|(k, _)| k == "authorization")
                        .map(|(_, v)| v.clone());
                    let body_str = String::from_utf8_lossy(&body).into_owned();
                    let (status, resp_body) =
                        if req_line.starts_with("GET /tenant/.well-known/openid-configuration ") {
                            let mut metadata = json!({
                                "issuer": issuer,
                                "authorization_endpoint": format!("{issuer}/authorize"),
                                "token_endpoint": format!("{issuer}/token"),
                                "revocation_endpoint": format!("{issuer}/revoke"),
                                "code_challenge_methods_supported": ["S256"]
                            });
                            if let Some(methods) = supported_methods {
                                metadata["token_endpoint_auth_methods_supported"] = json!(methods);
                            }
                            ("200 OK", metadata.to_string())
                        } else if req_line.starts_with("POST /tenant/token") {
                            seen_clone.lock().unwrap().push((
                                "token".into(),
                                auth_hdr,
                                body_str.clone(),
                            ));
                            let tok = if body_str.contains("grant_type=refresh_token") {
                                "refreshed_tok"
                            } else {
                                "initial_tok"
                            };
                            (
                                "200 OK",
                                json!({
                                    "access_token": tok,
                                    "token_type": "Bearer",
                                    "refresh_token": "rt_1",
                                    "expires_in": 10
                                })
                                .to_string(),
                            )
                        } else if req_line.starts_with("POST /tenant/revoke") {
                            seen_clone
                                .lock()
                                .unwrap()
                                .push(("revoke".into(), auth_hdr, body_str));
                            ("200 OK", String::new())
                        } else {
                            ("404 Not Found", String::new())
                        };
                    let resp = format!(
                        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{resp_body}",
                        resp_body.len()
                    );
                    let _ = sock.write_all(resp.as_bytes()).await;
                });
            }
        });

        let resource = "http://127.0.0.1:9999/mcp";

        let mut login_child = tokio::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "auth::tests::confidential_login_helper_entry",
                "--nocapture",
            ])
            .env(
                "MCPCLI_TEST_AUTH_METADATA_URL",
                format!("{issuer}/.well-known/openid-configuration"),
            )
            .env("MCPCLI_CREDENTIALS", &cred_path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut lines = tokio::io::BufReader::new(login_child.stderr.take().unwrap()).lines();
        let auth_url = timeout(Duration::from_secs(10), async {
            while let Some(line) = lines.next_line().await.unwrap() {
                if line.starts_with("http://") {
                    return reqwest::Url::parse(&line).unwrap();
                }
            }
            panic!("login exited without an authorization URL");
        })
        .await
        .unwrap();
        let query: BTreeMap<_, _> = auth_url.query_pairs().into_owned().collect();
        assert_eq!(query["client_id"], client_id);
        assert_eq!(query["scope"], "mcp:read mcp:tools mcp:write");
        assert!(query["redirect_uri"].ends_with("/oauth/./callback"));
        let mut callback = reqwest::Url::parse(&query["redirect_uri"]).unwrap();
        assert_eq!(callback.path(), "/oauth/callback");
        callback
            .query_pairs_mut()
            .append_pair("code", "authorization_code_1")
            .append_pair("state", &query["state"]);
        let client = build_oauth_http_client(resource, true, Duration::from_secs(5)).unwrap();
        assert_eq!(
            client.get(callback).send().await.unwrap().status(),
            http::StatusCode::OK
        );
        assert!(
            timeout(Duration::from_secs(10), login_child.wait())
                .await
                .unwrap()
                .unwrap()
                .success()
        );
        let storage_key = credential_storage_key(resource, Some("acct1"));
        let store = read_store_at(&cred_path).unwrap();
        let stored = &store.resources[&storage_key];
        assert_eq!(stored.client_id, client_id);
        assert_eq!(
            stored.token_endpoint_auth_method.as_deref(),
            Some(auth_method)
        );
        assert_eq!(stored.scopes, ["mcp:read", "mcp:tools", "mcp:write"]);

        // Verify client_secret is never on disk.
        let raw_disk = std::fs::read_to_string(&cred_path).unwrap();
        assert!(!raw_disk.contains("sec:ret+1"));

        // Mismatched client_id withholds client_secret on refresh (no request sent).
        let mismatched_oauth = OAuthConfig {
            client_id: Some("other-cid".into()),
            client_secret: Some("other-secret".into()),
            account: Some("acct1".into()),
            ..OAuthConfig::default()
        };
        let mgr_mismatch = HttpAuthManager::for_server(resource, Some(&mismatched_oauth)).unwrap();
        let (tok_mismatch, attempted_mismatch) = mgr_mismatch.prepare_token(&client).await;
        assert!(attempted_mismatch);
        assert_eq!(tok_mismatch.as_deref(), Some("initial_tok"));
        assert_eq!(seen_token_auth.lock().unwrap().len(), 1);

        // Refresh uses runtime client_secret with matching Basic or Post auth.
        let oauth_cfg = OAuthConfig {
            client_id: Some(client_id.into()),
            client_secret: Some("sec:ret+1".into()),
            account: Some("acct1".into()),
            ..OAuthConfig::default()
        };
        let mgr = HttpAuthManager::for_server(resource, Some(&oauth_cfg)).unwrap();
        let (fresh, attempted) = mgr.prepare_token(&client).await;
        assert!(attempted);
        assert_eq!(fresh.as_deref(), Some("refreshed_tok"));

        // Logout sends RFC 7009 revocation with matching Basic or Post auth.
        let (out_json, _) = logout(
            "conf-srv",
            resource,
            Some(&oauth_cfg),
            true,
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        assert_eq!(out_json["loggedOut"], true);
        assert!(read_store_at(&cred_path).unwrap().resources.is_empty());

        let recorded = seen_token_auth.lock().unwrap().clone();
        assert!(recorded.iter().any(
            |(kind, _, body)| kind == "token" && body.contains("grant_type=authorization_code")
        ));
        assert!(recorded.iter().any(
            |(kind, _, body)| kind == "token" && body.contains("grant_type=refresh_token")
        ));
        assert!(recorded.iter().any(|(kind, _, _)| kind == "revoke"));
        for (_, hdr, body) in &recorded {
            if auth_method == "client_secret_basic" {
                assert_eq!(hdr.as_deref(), Some(expected_basic.as_str()));
                assert!(!body.contains("client_secret="));
                assert!(!body.contains("client_id="));
            } else {
                assert!(hdr.is_none());
                assert!(body.contains("client_id=+cid-conf+"));
                assert!(body.contains("client_secret=sec%3Aret%2B1"));
            }
        }

        as_srv.abort();
    }

    // Unsupported token_endpoint_auth_methods_supported fails with mcp_protocol.
    let err =
        select_token_endpoint_auth_method(true, Some(&["private_key_jwt".into()])).unwrap_err();
    assert_eq!(err.downcast_ref::<Failure>().unwrap().kind, "mcp_protocol");

    // SAFETY: serialized across tests in this module via `test_env_lock`.
    unsafe {
        match previous {
            Some(v) => std::env::set_var("MCPCLI_CREDENTIALS", v),
            None => std::env::remove_var("MCPCLI_CREDENTIALS"),
        }
    }
}

#[tokio::test]
async fn login_conflict_rules_and_explicit_metadata_url_validation() {
    // --client-name conflicts with CLI --client-id or configured oauth.client_id.
    for (cli_cid, cfg_cid) in [(Some("cli-cid"), None), (None, Some("cfg-cid"))] {
        let err = super::login(LoginOptions {
            server_name: "srv",
            server_url: "http://127.0.0.1:9000/mcp",
            scopes: &[],
            configured_scopes: &[],
            client_id: cli_cid,
            configured_client_id: cfg_cid,
            client_secret: None,
            client_name: Some("my-app"),
            configured_client_name: None,
            callback_port: None,
            callback_path: None,
            auth_server_metadata_url: None,
            account: None,
            no_browser: true,
            budget: Duration::from_secs(2),
        })
        .await
        .unwrap_err();
        let failure = err.downcast_ref::<Failure>().unwrap();
        assert_eq!(failure.code, 2);
        assert_eq!(failure.kind, "arguments");
    }

    // Non-loopback MCP server rejects loopback/private auth_server_metadata_url.
    for bad_meta in [
        "http://127.0.0.1:8080/.well-known/openid-configuration",
        "https://169.254.169.254/.well-known/openid-configuration",
    ] {
        let err = super::login(LoginOptions {
            server_name: "remote",
            server_url: "https://mcp.example.com/mcp",
            scopes: &[],
            configured_scopes: &[],
            client_id: Some("cid"),
            configured_client_id: None,
            client_secret: None,
            client_name: None,
            configured_client_name: None,
            callback_port: None,
            callback_path: None,
            auth_server_metadata_url: Some(bad_meta),
            account: None,
            no_browser: true,
            budget: Duration::from_secs(2),
        })
        .await
        .unwrap_err();
        assert_eq!(err.downcast_ref::<Failure>().unwrap().code, 2);
    }

    // Explicit auth_server_metadata_url rejects mismatched issuer from server.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let srv = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let _ = read_http_request(&mut sock).await;
        let body = json!({
            "issuer": format!("http://127.0.0.1:{port}/other-tenant"),
            "authorization_endpoint": format!("http://127.0.0.1:{port}/authorize"),
            "token_endpoint": format!("http://127.0.0.1:{port}/token"),
            "code_challenge_methods_supported": ["S256"]
        })
        .to_string();
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let _ = sock.write_all(resp.as_bytes()).await;
    });
    let meta_url = format!("http://127.0.0.1:{port}/tenant/.well-known/openid-configuration");
    let err = super::login(LoginOptions {
        server_name: "srv",
        server_url: "http://127.0.0.1:9999/mcp",
        scopes: &[],
        configured_scopes: &[],
        client_id: Some("cid"),
        configured_client_id: None,
        client_secret: None,
        client_name: None,
        configured_client_name: None,
        callback_port: None,
        callback_path: None,
        auth_server_metadata_url: Some(&meta_url),
        account: None,
        no_browser: true,
        budget: Duration::from_secs(3),
    })
    .await
    .unwrap_err();
    assert_eq!(err.downcast_ref::<Failure>().unwrap().kind, "mcp_protocol");
    srv.abort();
}

#[tokio::test]
async fn multi_account_credentials_locks_and_status_are_isolated() {
    let _env_guard = test_env_lock().lock().await;
    #[cfg(unix)]
    let dir = tempfile::tempdir_in("/tmp").unwrap();
    #[cfg(windows)]
    let dir = tempfile::tempdir().unwrap();
    let cred_path = dir.path().canonicalize().unwrap().join("credentials.json");
    let previous = std::env::var_os("MCPCLI_CREDENTIALS");
    // SAFETY: serialized across tests in this module via `test_env_lock`.
    unsafe { std::env::set_var("MCPCLI_CREDENTIALS", &cred_path) };

    let url = "http://127.0.0.1:8123/mcp";
    assert_eq!(credential_storage_key(url, None), url);
    assert_eq!(
        credential_storage_key(url, Some("work")),
        "http://127.0.0.1:8123/mcp#account=work"
    );

    let exp = now_unix_secs().saturating_add(3600);
    mutate_store(|store| {
        for (acct, tok) in [
            (None, "tok_default"),
            (Some("work"), "tok_work"),
            (Some("personal"), "tok_personal"),
        ] {
            store.resources.insert(
                credential_storage_key(url, acct),
                StoredCredential {
                    resource: url.into(),
                    issuer: "http://127.0.0.1:9000".into(),
                    token_endpoint: "http://127.0.0.1:9000/token".into(),
                    revocation_endpoint: None,
                    client_id: "cid".into(),
                    token_endpoint_auth_method: Some("none".into()),
                    access_token: tok.into(),
                    refresh_token: None,
                    expires_at: Some(exp),
                    scopes: vec!["mcp:read".into()],
                },
            );
        }
        Ok(())
    })
    .await
    .unwrap();

    // Independent token retrieval per account.
    let client = reqwest::Client::new();
    let mgr_default = HttpAuthManager::for_server(url, None).unwrap();
    let work_oauth = OAuthConfig {
        account: Some("work".into()),
        ..OAuthConfig::default()
    };
    let personal_oauth = OAuthConfig {
        account: Some("personal".into()),
        ..OAuthConfig::default()
    };
    let mgr_work = HttpAuthManager::for_server(url, Some(&work_oauth)).unwrap();
    let mgr_personal = HttpAuthManager::for_server(url, Some(&personal_oauth)).unwrap();
    assert_eq!(
        mgr_default.prepare_token(&client).await.0.as_deref(),
        Some("tok_default")
    );
    assert_eq!(
        mgr_work.prepare_token(&client).await.0.as_deref(),
        Some("tok_work")
    );
    assert_eq!(
        mgr_personal.prepare_token(&client).await.0.as_deref(),
        Some("tok_personal")
    );

    // Locks are isolated by storage_key.
    let lock_work = credential_lock(&credential_storage_key(url, Some("work")))
        .await
        .unwrap();
    let lock_personal = timeout(
        Duration::from_secs(1),
        credential_lock(&credential_storage_key(url, Some("personal"))),
    )
    .await
    .expect("personal lock should not block on work lock")
    .unwrap();
    drop(lock_personal);
    drop(lock_work);

    // auth::status includes account when present and omits it when absent.
    let servers = BTreeMap::from([
        (
            "default".to_owned(),
            ServerConfig::Http {
                url: url.into(),
                headers: BTreeMap::new(),
                description: None,
                enabled: true,
                timeout: None,
                oauth: None,
            },
        ),
        (
            "work".to_owned(),
            ServerConfig::Http {
                url: url.into(),
                headers: BTreeMap::new(),
                description: None,
                enabled: true,
                timeout: None,
                oauth: Some(work_oauth.clone()),
            },
        ),
    ]);
    let (default_json, default_text) = status(&servers, Some("default")).unwrap();
    assert!(default_json.get("account").is_none());
    assert!(!default_text.contains("account="));
    let (work_json, work_text) = status(&servers, Some("work")).unwrap();
    assert_eq!(work_json["account"], "work");
    assert!(work_text.contains("account=work"));

    // Logging out work leaves personal and default intact.
    logout("work", url, Some(&work_oauth), true, Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(mgr_work.prepare_token(&client).await.0, None);
    assert_eq!(
        mgr_personal.prepare_token(&client).await.0.as_deref(),
        Some("tok_personal")
    );
    assert_eq!(
        mgr_default.prepare_token(&client).await.0.as_deref(),
        Some("tok_default")
    );

    // SAFETY: serialized across tests in this module via `test_env_lock`.
    unsafe {
        match previous {
            Some(v) => std::env::set_var("MCPCLI_CREDENTIALS", v),
            None => std::env::remove_var("MCPCLI_CREDENTIALS"),
        }
    }
}

#[tokio::test]
async fn disabled_server_logout_skips_confidential_remote_revocation() {
    use std::sync::atomic::AtomicUsize;

    let _env_guard = test_env_lock().lock().await;
    #[cfg(unix)]
    let dir = tempfile::tempdir_in("/tmp").unwrap();
    #[cfg(windows)]
    let dir = tempfile::tempdir().unwrap();
    let cred_path = dir.path().canonicalize().unwrap().join("credentials.json");
    let previous = std::env::var_os("MCPCLI_CREDENTIALS");
    // SAFETY: serialized across tests in this module via `test_env_lock`.
    unsafe { std::env::set_var("MCPCLI_CREDENTIALS", &cred_path) };

    let rev_hits = Arc::new(AtomicUsize::new(0));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let rev_url = format!("http://{}/revoke", listener.local_addr().unwrap());
    let hits_clone = rev_hits.clone();
    let srv = tokio::spawn(async move {
        while listener.accept().await.is_ok() {
            hits_clone.fetch_add(1, Ordering::SeqCst);
        }
    });

    let resource = "http://127.0.0.1:8124/mcp";
    mutate_store(|store| {
        store.resources.insert(
            resource.into(),
            StoredCredential {
                resource: resource.into(),
                issuer: "http://127.0.0.1:9000".into(),
                token_endpoint: "http://127.0.0.1:9000/token".into(),
                revocation_endpoint: Some(rev_url),
                client_id: "cid".into(),
                token_endpoint_auth_method: Some("client_secret_basic".into()),
                access_token: "at".into(),
                refresh_token: Some("rt".into()),
                expires_at: None,
                scopes: vec![],
            },
        );
        Ok(())
    })
    .await
    .unwrap();

    let oauth = OAuthConfig {
        client_id: Some("cid".into()),
        client_secret: Some("${UNEXPANDED_SECRET}".into()),
        ..OAuthConfig::default()
    };
    let (out, _) = logout(
        "disabled-srv",
        resource,
        Some(&oauth),
        false,
        Duration::from_secs(2),
    )
    .await
    .unwrap();
    assert_eq!(out["loggedOut"], true);
    assert!(read_store_at(&cred_path).unwrap().resources.is_empty());
    assert_eq!(rev_hits.load(Ordering::SeqCst), 0);

    srv.abort();
    // SAFETY: serialized across tests in this module via `test_env_lock`.
    unsafe {
        match previous {
            Some(v) => std::env::set_var("MCPCLI_CREDENTIALS", v),
            None => std::env::remove_var("MCPCLI_CREDENTIALS"),
        }
    }
}

#[tokio::test]
async fn custom_dcr_client_name_callback_path_and_client_id_override_clears_secret() {
    // 1. Custom DCR client_name and custom redirect_uri path.
    let reg_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let reg_url = format!("http://{}/register", reg_listener.local_addr().unwrap());
    let seen_dcr = Arc::new(std::sync::Mutex::new(None::<Value>));
    let seen_dcr_srv = seen_dcr.clone();
    let reg_srv = tokio::spawn(async move {
        let (mut sock, _) = reg_listener.accept().await.unwrap();
        let (_, _, body) = read_http_request(&mut sock).await;
        *seen_dcr_srv.lock().unwrap() = Some(serde_json::from_slice(&body).unwrap());
        let resp_body = r#"{"client_id":"dcr_custom_id"}"#;
        let resp = format!(
            "HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{resp_body}",
            resp_body.len()
        );
        let _ = sock.write_all(resp.as_bytes()).await;
    });
    let client = reqwest::Client::new();
    let cid = register_public_client(
        &client,
        &reg_url,
        "http://127.0.0.1:8765/custom/cb",
        "My Custom App",
        true,
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert_eq!(cid, "dcr_custom_id");
    let payload = seen_dcr.lock().unwrap().clone().unwrap();
    assert_eq!(payload["client_name"], "My Custom App");
    assert_eq!(
        payload["redirect_uris"],
        json!(["http://127.0.0.1:8765/custom/cb"])
    );
    reg_srv.abort();

    // 2. Custom callback_path rejects /callback with 404 and accepts configured path.
    let cb_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cb_port = cb_listener.local_addr().unwrap().port();
    let cb_task = tokio::spawn(async move {
        accept_loopback_callback(&cb_listener, "/custom/cb", "st_1", "http://127.0.0.1:9000").await
    });
    let wrong_resp = client
        .get(format!(
            "http://127.0.0.1:{cb_port}/callback?code=c1&state=st_1"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(wrong_resp.status(), http::StatusCode::NOT_FOUND);
    let ok_resp = client
        .get(format!(
            "http://127.0.0.1:{cb_port}/custom/cb?code=c_ok&state=st_1"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(ok_resp.status(), http::StatusCode::OK);
    assert_eq!(cb_task.await.unwrap().unwrap(), "c_ok");

    // 3. CLI --client-id overriding configured oauth.client_id clears client_secret:
    //    AS only supports private_key_jwt (which fails if client_secret is still active),
    //    so overriding client_id proceeds past method selection to timeout waiting for callback.
    let as_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let as_port = as_listener.local_addr().unwrap().port();
    let issuer = format!("http://127.0.0.1:{as_port}");
    let meta_url = format!("{issuer}/.well-known/openid-configuration");
    let as_srv = tokio::spawn(async move {
        while let Ok((mut sock, _)) = as_listener.accept().await {
            let issuer = issuer.clone();
            tokio::spawn(async move {
                let _ = read_http_request(&mut sock).await;
                let body = json!({
                    "issuer": issuer,
                    "authorization_endpoint": format!("{issuer}/authorize"),
                    "token_endpoint": format!("{issuer}/token"),
                    "code_challenge_methods_supported": ["S256"],
                    "token_endpoint_auth_methods_supported": ["private_key_jwt"]
                })
                .to_string();
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(resp.as_bytes()).await;
            });
        }
    });
    // Same client_id keeps client_secret -> fails with mcp_protocol due to private_key_jwt.
    let same_err = super::login(LoginOptions {
        server_name: "srv",
        server_url: "http://127.0.0.1:9999/mcp",
        scopes: &[],
        configured_scopes: &[],
        client_id: Some(" cfg-cid "),
        configured_client_id: Some("cfg-cid"),
        client_secret: Some("cfg-secret"),
        client_name: None,
        configured_client_name: None,
        callback_port: None,
        callback_path: None,
        auth_server_metadata_url: Some(&meta_url),
        account: None,
        no_browser: true,
        budget: Duration::from_millis(500),
    })
    .await
    .unwrap_err();
    assert_eq!(
        same_err.downcast_ref::<Failure>().unwrap().kind,
        "mcp_protocol"
    );

    // Different CLI client_id clears client_secret -> passes method selection and times out waiting for callback.
    let diff_err = super::login(LoginOptions {
        server_name: "srv",
        server_url: "http://127.0.0.1:9999/mcp",
        scopes: &[],
        configured_scopes: &[],
        client_id: Some("override-cid"),
        configured_client_id: Some(" override-cid "),
        client_secret: Some("cfg-secret"),
        client_name: None,
        configured_client_name: None,
        callback_port: None,
        callback_path: None,
        auth_server_metadata_url: Some(&meta_url),
        account: None,
        no_browser: true,
        budget: Duration::from_millis(300),
    })
    .await
    .unwrap_err();
    assert_eq!(diff_err.downcast_ref::<Failure>().unwrap().kind, "timeout");
    as_srv.abort();
}
