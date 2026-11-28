use super::callback::{accept_loopback_callback, read_stdin_callback};
use super::challenge::{
    BearerChallenge, canonical_resource_url, credential_storage_key, now_unix_secs, split_scopes,
    validate_mcp_server_url,
};
use super::discovery::{
    TokenEndpointResponse, apply_client_auth, build_oauth_http_client,
    discover_authorization_server, discover_protected_resource, encode_form,
    ensure_resolved_host_allowed, fetch_explicit_authorization_server_metadata,
    probe_mcp_server_challenge, protected_resource_metadata_urls, read_bounded_json,
    register_public_client, select_token_endpoint_auth_method, validate_bearer_token_string,
};
use super::store::{
    CredentialsStoreFile, StoredCredential, credential_lock, mutate_store, read_store_readonly,
    resolve_store_location_readonly,
};
use crate::{
    config::{OAuthConfig, ServerConfig, validate_client_name},
    discovery::safe_text,
    ipc::Failure,
    oauth_url::{is_loopback_literal_host, validate_callback_path, validate_oauth_endpoint_url},
};
use anyhow::{Context, Result, bail};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    io::{self, Write},
    process::{Command, Stdio},
    sync::Arc,
    time::Duration,
};
use tokio::{
    net::TcpListener,
    time::{Instant, timeout, timeout_at},
};

const PROACTIVE_REFRESH_WINDOW_SECS: u64 = 30;

fn csprng_fill(buf: &mut [u8]) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::io::Read;
        let mut urandom = std::fs::File::open("/dev/urandom")?;
        urandom.read_exact(buf)
    }
    #[cfg(windows)]
    {
        #[link(name = "bcrypt")]
        unsafe extern "system" {
            fn BCryptGenRandom(
                algorithm: *mut core::ffi::c_void,
                buffer: *mut u8,
                len: u32,
                flags: u32,
            ) -> i32;
        }
        const BCRYPT_USE_SYSTEM_PREFERRED_RNG: u32 = 0x0000_0002;
        // SAFETY: `buf` points to valid writable storage of `buf.len()` bytes.
        let status = unsafe {
            BCryptGenRandom(
                core::ptr::null_mut(),
                buf.as_mut_ptr(),
                buf.len() as u32,
                BCRYPT_USE_SYSTEM_PREFERRED_RNG,
            )
        };
        if status == 0 {
            Ok(())
        } else {
            Err(io::Error::other(format!(
                "BCryptGenRandom failed with NTSTATUS {status:#x}"
            )))
        }
    }
}

fn base64url_nopad(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = u32::from(chunk[0]);
        let b1 = u32::from(*chunk.get(1).unwrap_or(&0));
        let b2 = u32::from(*chunk.get(2).unwrap_or(&0));
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(TABLE[((triple >> 18) & 0x3f) as usize] as char);
        out.push(TABLE[((triple >> 12) & 0x3f) as usize] as char);
        if chunk.len() > 1 {
            out.push(TABLE[((triple >> 6) & 0x3f) as usize] as char);
        }
        if chunk.len() > 2 {
            out.push(TABLE[(triple & 0x3f) as usize] as char);
        }
    }
    out
}

fn pkce_s256_challenge(verifier: &str) -> String {
    let digest = Sha256::digest(verifier.as_bytes());
    base64url_nopad(&digest)
}

pub struct HttpAuthManager {
    storage_key: String,
    allow_loopback: bool,
    client_id: Option<String>,
    client_secret: Option<String>,
    oauth_client: Option<reqwest::Client>,
}

impl HttpAuthManager {
    pub fn for_server(url: &str, oauth: Option<&OAuthConfig>) -> Result<Arc<Self>> {
        let parsed =
            reqwest::Url::parse(url).map_err(|_| anyhow::anyhow!("invalid resource URL"))?;
        let allow_loopback = is_loopback_literal_host(parsed.host_str());
        let canonical = canonical_resource_url(url)?;
        let storage_key =
            credential_storage_key(&canonical, oauth.and_then(|o| o.account.as_deref()));
        let client_id = oauth.and_then(|o| o.client_id.clone());
        let client_secret = oauth.and_then(|o| o.client_secret.clone());
        let oauth_client = if allow_loopback {
            None
        } else {
            Some(build_oauth_http_client(
                url,
                false,
                Duration::from_secs(10),
            )?)
        };
        Ok(Arc::new(Self {
            storage_key,
            allow_loopback,
            client_id,
            client_secret,
            oauth_client,
        }))
    }

    pub async fn prepare_token(&self, client: &reqwest::Client) -> (Option<String>, bool) {
        let Ok(store) = read_store_readonly() else {
            return (None, false);
        };
        let Some(cred) = store.resources.get(&self.storage_key) else {
            return (None, false);
        };
        if validate_bearer_token_string(&cred.access_token).is_err() {
            return (None, false);
        }
        let now = now_unix_secs();
        if cred.refresh_token.is_none() {
            if cred.expires_at.is_some_and(|exp| exp <= now) {
                return (None, false);
            }
            return (Some(cred.access_token.clone()), false);
        }
        let needs_proactive = cred
            .expires_at
            .is_some_and(|exp| exp <= now.saturating_add(PROACTIVE_REFRESH_WINDOW_SECS));
        if !needs_proactive {
            return (Some(cred.access_token.clone()), false);
        }
        let Ok(_guard) = credential_lock(&self.storage_key).await else {
            return (
                cred.expires_at
                    .is_none_or(|exp| exp > now_unix_secs())
                    .then(|| cred.access_token.clone()),
                true,
            );
        };
        let Ok(reloaded_store) = read_store_readonly() else {
            if cred.expires_at.is_some_and(|exp| exp <= now_unix_secs()) {
                return (None, true);
            }
            return (Some(cred.access_token.clone()), true);
        };
        let Some(reloaded) = reloaded_store.resources.get(&self.storage_key) else {
            return (None, true);
        };
        let now_locked = now_unix_secs();
        if reloaded.refresh_token.is_none() {
            if reloaded.expires_at.is_some_and(|exp| exp <= now_locked) {
                return (None, true);
            }
            return (Some(reloaded.access_token.clone()), true);
        }
        let still_expiring = reloaded
            .expires_at
            .is_some_and(|exp| exp <= now_locked.saturating_add(PROACTIVE_REFRESH_WINDOW_SECS));
        if !still_expiring {
            return (Some(reloaded.access_token.clone()), true);
        }
        match self.exchange_refresh_token(client, reloaded, false).await {
            Ok(fresh) => (Some(fresh), true),
            Err(_)
                if reloaded
                    .expires_at
                    .is_some_and(|exp| exp <= now_unix_secs()) =>
            {
                (None, true)
            }
            Err(_) => (Some(reloaded.access_token.clone()), true),
        }
    }

    pub async fn refresh_after_401(
        &self,
        client: &reqwest::Client,
        rejected_token: Option<&str>,
    ) -> Option<String> {
        if !read_store_readonly()
            .ok()?
            .resources
            .contains_key(&self.storage_key)
        {
            return None;
        }
        let _guard = credential_lock(&self.storage_key).await.ok()?;
        let store = read_store_readonly().ok()?;
        let cred = store.resources.get(&self.storage_key)?;
        if validate_bearer_token_string(&cred.access_token).is_err() {
            return None;
        }
        if let Some(old) = rejected_token
            && old != cred.access_token
        {
            return Some(cred.access_token.clone());
        }
        cred.refresh_token.as_ref()?;
        self.exchange_refresh_token(client, cred, true).await.ok()
    }

    pub(super) async fn exchange_refresh_token(
        &self,
        fallback_client: &reqwest::Client,
        cred: &StoredCredential,
        rejected_by_resource: bool,
    ) -> Result<String> {
        timeout(
            Duration::from_secs(15),
            self.exchange_refresh_token_inner(fallback_client, cred, rejected_by_resource),
        )
        .await
        .map_err(|_| anyhow::anyhow!("OAuth token refresh timed out"))?
    }

    async fn exchange_refresh_token_inner(
        &self,
        fallback_client: &reqwest::Client,
        cred: &StoredCredential,
        rejected_by_resource: bool,
    ) -> Result<String> {
        let refresh_token = cred
            .refresh_token
            .as_deref()
            .context("no refresh_token available")?;
        let resource_loopback = reqwest::Url::parse(&cred.resource)
            .ok()
            .is_some_and(|u| is_loopback_literal_host(u.host_str()));
        let allow_loopback = self.allow_loopback && resource_loopback;
        let endpoint =
            validate_oauth_endpoint_url(&cred.token_endpoint, "token_endpoint", allow_loopback)?;
        let owned_client;
        let http_client = if allow_loopback {
            fallback_client
        } else if let Some(ref guarded) = self.oauth_client {
            guarded
        } else {
            owned_client =
                build_oauth_http_client(&cred.token_endpoint, false, Duration::from_secs(10))?;
            &owned_client
        };
        let mut form_pairs = vec![
            ("grant_type", "refresh_token"),
            ("refresh_token", refresh_token),
            ("resource", cred.resource.as_str()),
        ];
        let runtime_secret = if self.client_id.as_deref() == Some(cred.client_id.as_str()) {
            self.client_secret.as_deref()
        } else {
            None
        };
        let request = apply_client_auth(
            http_client
                .post(endpoint)
                .header(
                    http::header::CONTENT_TYPE,
                    "application/x-www-form-urlencoded",
                )
                .header(http::header::ACCEPT, "application/json"),
            &mut form_pairs,
            &cred.client_id,
            runtime_secret,
            cred.token_endpoint_auth_method.as_deref(),
        )?;
        let form = encode_form(&form_pairs);
        let response = timeout(Duration::from_secs(15), request.body(form).send())
            .await
            .map_err(|_| anyhow::anyhow!("OAuth token refresh timed out"))?
            .map_err(|e| anyhow::anyhow!("OAuth token refresh failed: {}", e.without_url()))?;
        let status = response.status();
        if !status.is_success() {
            if let Ok(err_json) = read_bounded_json(response).await
                && let Some(err_code) = err_json.get("error").and_then(Value::as_str)
                && matches!(
                    err_code,
                    "invalid_grant" | "invalid_client" | "unauthorized_client"
                )
            {
                let key = self.storage_key.clone();
                let expected_rt = refresh_token.to_owned();
                let _ = mutate_store(|store| {
                    if let Some(entry) = store.resources.get_mut(&key)
                        && entry.refresh_token.as_deref() == Some(expected_rt.as_str())
                    {
                        entry.refresh_token = None;
                        if rejected_by_resource {
                            entry.expires_at = Some(0);
                        }
                    }
                    Ok(())
                })
                .await;
            }
            bail!("OAuth token refresh rejected with HTTP {}", status.as_u16());
        }
        let body = read_bounded_json(response).await?;
        let parsed: TokenEndpointResponse =
            serde_json::from_value(body).context("invalid OAuth refresh response")?;
        if let Some(ref tt) = parsed.token_type
            && !tt.eq_ignore_ascii_case("bearer")
        {
            bail!("unsupported OAuth token_type");
        }
        validate_bearer_token_string(&parsed.access_token)?;
        let updated = StoredCredential {
            resource: cred.resource.clone(),
            issuer: cred.issuer.clone(),
            token_endpoint: cred.token_endpoint.clone(),
            revocation_endpoint: cred.revocation_endpoint.clone(),
            client_id: cred.client_id.clone(),
            token_endpoint_auth_method: cred.token_endpoint_auth_method.clone(),
            access_token: parsed.access_token.clone(),
            refresh_token: parsed.refresh_token.or_else(|| cred.refresh_token.clone()),
            expires_at: parsed
                .expires_in
                .map(|secs| now_unix_secs().saturating_add(secs)),
            scopes: parsed
                .scope
                .as_deref()
                .map(split_scopes)
                .unwrap_or_else(|| cred.scopes.clone()),
        };
        let key = self.storage_key.clone();
        mutate_store(|store| {
            store.resources.insert(key, updated);
            Ok(())
        })
        .await?;
        Ok(parsed.access_token)
    }
}

fn try_open_browser(url: &str) {
    #[cfg(target_os = "macos")]
    let mut cmd = {
        let mut c = Command::new("open");
        c.arg(url);
        c
    };
    #[cfg(all(unix, not(target_os = "macos")))]
    let mut cmd = {
        let mut c = Command::new("xdg-open");
        c.arg(url);
        c
    };
    #[cfg(windows)]
    let mut cmd = {
        let mut c = Command::new("rundll32.exe");
        c.args(["url.dll,FileProtocolHandler", url]);
        c
    };
    cmd.stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let Ok(mut child) = cmd.spawn() {
        std::thread::spawn(move || {
            let _ = child.wait();
        });
    }
}

pub struct LoginOptions<'a> {
    pub server_name: &'a str,
    pub server_url: &'a str,
    pub scopes: &'a [String],
    pub configured_scopes: &'a [String],
    pub client_id: Option<&'a str>,
    pub configured_client_id: Option<&'a str>,
    pub client_secret: Option<&'a str>,
    pub client_name: Option<&'a str>,
    pub configured_client_name: Option<&'a str>,
    pub callback_port: Option<u16>,
    pub callback_path: Option<&'a str>,
    pub auth_server_metadata_url: Option<&'a str>,
    pub account: Option<&'a str>,
    pub no_browser: bool,
    pub budget: Duration,
}

pub async fn login(opts: LoginOptions<'_>) -> Result<(Value, String)> {
    timeout(opts.budget, login_inner(opts))
        .await
        .map_err(|_| Failure::new(5, "timeout", "OAuth login timed out"))?
}

async fn login_inner(opts: LoginOptions<'_>) -> Result<(Value, String)> {
    let deadline = Instant::now() + opts.budget;
    let (server_url, allow_loopback) = validate_mcp_server_url(opts.server_url)
        .map_err(|e| Failure::new(2, "configuration", e.to_string()))?;
    let canonical_server_resource = canonical_resource_url(opts.server_url)
        .map_err(|e| Failure::new(2, "configuration", e.to_string()))?;

    let cli_client_id = opts.client_id.map(str::trim).filter(|s| !s.is_empty());
    let cfg_client_id = opts.configured_client_id.filter(|s| !s.trim().is_empty());
    if let Some(cli_name) = opts.client_name {
        validate_client_name(cli_name).map_err(|e| Failure::new(2, "arguments", e.to_string()))?;
        if cli_client_id.is_some() || cfg_client_id.is_some() {
            return Err(Failure::new(
                2,
                "arguments",
                "--client-name cannot be used when client_id is set",
            )
            .into());
        }
    }
    let effective_client_id = cli_client_id.or(cfg_client_id);
    // Switching client_id via CLI clears any configured client_secret so a secret
    // for one client is never sent under another client ID.
    let effective_client_secret = if cli_client_id.is_some() && cli_client_id != cfg_client_id {
        None
    } else {
        opts.client_secret.filter(|s| !s.is_empty())
    };
    if effective_client_secret.is_some() && effective_client_id.is_none() {
        return Err(
            Failure::new(2, "configuration", "oauth.client_secret requires client_id").into(),
        );
    }
    let effective_client_name = opts
        .client_name
        .or(opts.configured_client_name)
        .unwrap_or("mcpcli");
    validate_client_name(effective_client_name)
        .map_err(|e| Failure::new(2, "arguments", e.to_string()))?;
    if let Some(0) = opts.callback_port {
        return Err(Failure::new(2, "arguments", "callback_port must be 1..=65535").into());
    }
    let callback_path = opts.callback_path.unwrap_or("/callback");
    validate_callback_path(callback_path)
        .map_err(|e| Failure::new(2, "arguments", e.to_string()))?;

    let connect_cap = opts.budget.min(Duration::from_secs(15));
    let oauth_client = build_oauth_http_client(opts.server_url, allow_loopback, connect_cap)
        .map_err(|e| Failure::new(4, "mcp_transport", e.to_string()))?;

    let (asm, canonical_issuer, resource, challenge) = if let Some(meta_raw) =
        opts.auth_server_metadata_url
    {
        let meta_url =
            validate_oauth_endpoint_url(meta_raw, "auth_server_metadata_url", allow_loopback)
                .map_err(|e| Failure::new(2, "configuration", e.to_string()))?;
        let (asm, canonical_issuer) = fetch_explicit_authorization_server_metadata(
            &oauth_client,
            &meta_url,
            allow_loopback,
            deadline,
        )
        .await?;
        (
            asm,
            canonical_issuer,
            canonical_server_resource.clone(),
            BearerChallenge::default(),
        )
    } else {
        let probe_client = build_oauth_http_client(opts.server_url, true, connect_cap)
            .map_err(|e| Failure::new(4, "mcp_transport", e.to_string()))?;
        let probe_budget = deadline
            .saturating_duration_since(Instant::now())
            .min(Duration::from_secs(10));
        if probe_budget.is_zero() {
            return Err(Failure::new(5, "timeout", "OAuth login timed out").into());
        }
        let challenge =
            probe_mcp_server_challenge(&probe_client, &server_url, probe_budget).await?;

        let prm_urls =
            protected_resource_metadata_urls(&server_url, challenge.resource_metadata.as_deref())
                .map_err(|e| Failure::new(4, "mcp_protocol", e.to_string()))?;
        let prm = discover_protected_resource(
            &oauth_client,
            &prm_urls,
            &canonical_server_resource,
            deadline,
        )
        .await?;

        let issuer_raw = &prm.authorization_servers[0];
        let issuer_url =
            validate_oauth_endpoint_url(issuer_raw, "authorization server issuer", allow_loopback)
                .map_err(|e| Failure::new(4, "mcp_protocol", e.to_string()))?;
        let canonical_issuer = canonical_resource_url(issuer_raw)
            .map_err(|e| Failure::new(4, "mcp_protocol", e.to_string()))?;
        let asm = discover_authorization_server(
            &oauth_client,
            &issuer_url,
            &canonical_issuer,
            allow_loopback,
            deadline,
        )
        .await?;
        (asm, canonical_issuer, prm.resource, challenge)
    };

    let token_auth_method = select_token_endpoint_auth_method(
        effective_client_secret.is_some(),
        asm.token_endpoint_auth_methods_supported.as_deref(),
    )?;

    let bind_port = opts.callback_port.unwrap_or(0);
    let listener = TcpListener::bind(("127.0.0.1", bind_port))
        .await
        .map_err(|e| Failure::new(1, "runtime", format!("cannot bind loopback port: {e}")))?;
    let callback_port = listener
        .local_addr()
        .map_err(|e| Failure::new(1, "runtime", format!("cannot inspect loopback port: {e}")))?
        .port();
    let redirect_uri = format!("http://127.0.0.1:{callback_port}{callback_path}");

    let client_id = if let Some(explicit) = effective_client_id {
        explicit.to_owned()
    } else if let Some(ref reg_endpoint) = asm.registration_endpoint {
        register_public_client(
            &oauth_client,
            reg_endpoint,
            &redirect_uri,
            effective_client_name,
            allow_loopback,
            deadline,
        )
        .await?
    } else {
        return Err(Failure::new(
            2,
            "client_id_required",
            "authorization server has no registration_endpoint; pass --client-id <CLIENT_ID>",
        )
        .into());
    };

    let mut verifier_bytes = [0u8; 32];
    csprng_fill(&mut verifier_bytes)
        .map_err(|e| Failure::new(1, "runtime", format!("CSPRNG failure: {e}")))?;
    let code_verifier = base64url_nopad(&verifier_bytes);
    let code_challenge = pkce_s256_challenge(&code_verifier);

    let mut state_bytes = [0u8; 16];
    csprng_fill(&mut state_bytes)
        .map_err(|e| Failure::new(1, "runtime", format!("CSPRNG failure: {e}")))?;
    let state = base64url_nopad(&state_bytes);

    let requested_scopes = if !opts.configured_scopes.is_empty() || !opts.scopes.is_empty() {
        let mut out = Vec::new();
        for item in opts.configured_scopes.iter().chain(opts.scopes.iter()) {
            for token in item.split_whitespace() {
                if !token.is_empty() && !out.iter().any(|s| s == token) {
                    out.push(token.to_owned());
                }
            }
        }
        out
    } else {
        challenge
            .scope
            .as_deref()
            .map(split_scopes)
            .unwrap_or_default()
    };

    let mut auth_url = validate_oauth_endpoint_url(
        &asm.authorization_endpoint,
        "authorization_endpoint",
        allow_loopback,
    )
    .map_err(|e| Failure::new(4, "mcp_protocol", e.to_string()))?;
    ensure_resolved_host_allowed(&auth_url, "authorization_endpoint", allow_loopback)
        .await
        .map_err(|e| Failure::new(4, "mcp_protocol", e.to_string()))?;
    {
        let mut qp = auth_url.query_pairs_mut();
        qp.append_pair("response_type", "code");
        qp.append_pair("client_id", &client_id);
        qp.append_pair("redirect_uri", &redirect_uri);
        qp.append_pair("code_challenge", &code_challenge);
        qp.append_pair("code_challenge_method", "S256");
        qp.append_pair("state", &state);
        qp.append_pair("resource", &resource);
        if !requested_scopes.is_empty() {
            qp.append_pair("scope", &requested_scopes.join(" "));
        }
    }

    {
        let mut stderr = std::io::stderr().lock();
        let _ = writeln!(
            stderr,
            "Authorize {} by visiting:\n{}",
            safe_text(opts.server_name),
            safe_text(auth_url.as_str())
        );
        let _ = stderr.flush();
    }
    if !opts.no_browser {
        try_open_browser(auth_url.as_str());
    }

    let wait_rem = deadline.saturating_duration_since(Instant::now());
    if wait_rem.is_zero() {
        return Err(Failure::new(5, "timeout", "OAuth authorization timed out").into());
    }
    let code = timeout(wait_rem, async {
        if opts.no_browser {
            tokio::select! {
                res = accept_loopback_callback(&listener, callback_path, &state, &canonical_issuer) => res,
                res = read_stdin_callback(&state, &canonical_issuer) => res,
            }
        } else {
            accept_loopback_callback(&listener, callback_path, &state, &canonical_issuer).await
        }
    })
    .await
    .map_err(|_| Failure::new(5, "timeout", "timed out waiting for OAuth callback"))??;
    drop(listener);

    let exchange_rem = deadline.saturating_duration_since(Instant::now());
    if exchange_rem.is_zero() {
        return Err(Failure::new(5, "timeout", "OAuth token exchange timed out").into());
    }
    let token_url =
        validate_oauth_endpoint_url(&asm.token_endpoint, "token_endpoint", allow_loopback)
            .map_err(|e| Failure::new(4, "mcp_protocol", e.to_string()))?;
    let mut form_pairs = vec![
        ("grant_type", "authorization_code"),
        ("code", code.as_str()),
        ("redirect_uri", redirect_uri.as_str()),
        ("code_verifier", code_verifier.as_str()),
        ("resource", resource.as_str()),
    ];
    let token_req = apply_client_auth(
        oauth_client
            .post(token_url)
            .header(
                http::header::CONTENT_TYPE,
                "application/x-www-form-urlencoded",
            )
            .header(http::header::ACCEPT, "application/json"),
        &mut form_pairs,
        &client_id,
        effective_client_secret,
        Some(token_auth_method),
    )
    .map_err(|e| Failure::new(4, "mcp_protocol", e.to_string()))?;
    let form = encode_form(&form_pairs);
    let token_resp = timeout(exchange_rem, token_req.body(form).send())
        .await
        .map_err(|_| Failure::new(5, "timeout", "OAuth token exchange timed out"))?
        .map_err(|e| {
            Failure::new(
                4,
                "mcp_transport",
                format!("OAuth token exchange failed: {}", e.without_url()),
            )
        })?;
    if !token_resp.status().is_success() {
        return Err(Failure::new(
            4,
            "mcp_protocol",
            format!(
                "OAuth token exchange failed with HTTP {}",
                token_resp.status().as_u16()
            ),
        )
        .into());
    }
    let token_json = read_bounded_json(token_resp)
        .await
        .map_err(|e| Failure::new(4, "mcp_protocol", format!("{e:#}")))?;
    let parsed_tok: TokenEndpointResponse = serde_json::from_value(token_json).map_err(|_| {
        Failure::new(
            4,
            "mcp_protocol",
            "OAuth token endpoint returned invalid token fields",
        )
    })?;
    if let Some(ref tt) = parsed_tok.token_type
        && !tt.eq_ignore_ascii_case("bearer")
    {
        return Err(Failure::new(
            4,
            "mcp_protocol",
            format!("unsupported OAuth token_type {}", safe_text(tt)),
        )
        .into());
    }
    validate_bearer_token_string(&parsed_tok.access_token)
        .map_err(|e| Failure::new(4, "mcp_protocol", e.to_string()))?;

    let expires_at = parsed_tok
        .expires_in
        .map(|secs| now_unix_secs().saturating_add(secs));
    let granted_scopes = parsed_tok
        .scope
        .as_deref()
        .map(split_scopes)
        .unwrap_or(requested_scopes);
    let stored = StoredCredential {
        resource,
        issuer: canonical_issuer.clone(),
        token_endpoint: asm.token_endpoint,
        revocation_endpoint: asm.revocation_endpoint,
        client_id,
        token_endpoint_auth_method: Some(token_auth_method.to_owned()),
        access_token: parsed_tok.access_token,
        refresh_token: parsed_tok.refresh_token,
        expires_at,
        scopes: granted_scopes.clone(),
    };
    let storage_key = credential_storage_key(&canonical_server_resource, opts.account);
    let _guard = credential_lock(&storage_key)
        .await
        .map_err(|e| Failure::new(1, "runtime", format!("{e:#}")))?;
    mutate_store(|store| {
        store.resources.insert(storage_key, stored);
        Ok(())
    })
    .await
    .map_err(|e| Failure::new(1, "runtime", format!("{e:#}")))?;

    let mut summary_map = Map::new();
    summary_map.insert("server".into(), json!(opts.server_name));
    if let Some(acct) = opts.account {
        summary_map.insert("account".into(), json!(acct));
    }
    summary_map.insert("authenticated".into(), json!(true));
    summary_map.insert("issuer".into(), json!(canonical_issuer));
    summary_map.insert("expiresAt".into(), json!(expires_at));
    summary_map.insert("scopes".into(), json!(granted_scopes));
    let summary_json = Value::Object(summary_map);
    let summary_text = match opts.account {
        Some(acct) => format!(
            "authenticated {} (account: {}, issuer: {})",
            opts.server_name, acct, canonical_issuer
        ),
        None => format!(
            "authenticated {} (issuer: {})",
            opts.server_name, canonical_issuer
        ),
    };
    Ok((summary_json, summary_text))
}

fn server_status_entry(
    name: &str,
    url: &str,
    account: Option<&str>,
    store: &CredentialsStoreFile,
    now: u64,
) -> Result<(Value, String)> {
    let canonical = canonical_resource_url(url)?;
    let storage_key = credential_storage_key(&canonical, account);
    if let Some(cred) = store.resources.get(&storage_key) {
        let valid = cred.expires_at.is_none_or(|exp| exp > now) || cred.refresh_token.is_some();
        let mut entry = Map::new();
        entry.insert("server".into(), json!(name));
        if let Some(acct) = account {
            entry.insert("account".into(), json!(acct));
        }
        entry.insert("authenticated".into(), json!(valid));
        entry.insert("expiresAt".into(), json!(cred.expires_at));
        entry.insert("scopes".into(), json!(cred.scopes));
        entry.insert("issuer".into(), json!(cred.issuer));
        let exp_text = cred
            .expires_at
            .map_or_else(|| "none".to_owned(), |v| v.to_string());
        let scopes_text = if cred.scopes.is_empty() {
            "none".to_owned()
        } else {
            cred.scopes.join(",")
        };
        let state_label = if valid { "authenticated" } else { "expired" };
        let line = match account {
            Some(acct) => format!(
                "{name}\t{state_label}\taccount={acct}\tissuer={}\texpiresAt={exp_text}\tscopes={scopes_text}",
                cred.issuer
            ),
            None => format!(
                "{name}\t{state_label}\tissuer={}\texpiresAt={exp_text}\tscopes={scopes_text}",
                cred.issuer
            ),
        };
        Ok((Value::Object(entry), line))
    } else {
        let mut entry = Map::new();
        entry.insert("server".into(), json!(name));
        if let Some(acct) = account {
            entry.insert("account".into(), json!(acct));
        }
        entry.insert("authenticated".into(), json!(false));
        entry.insert("expiresAt".into(), Value::Null);
        entry.insert("scopes".into(), json!(Vec::<String>::new()));
        entry.insert("issuer".into(), Value::Null);
        let line = match account {
            Some(acct) => format!("{name}\tunauthenticated\taccount={acct}"),
            None => format!("{name}\tunauthenticated"),
        };
        Ok((Value::Object(entry), line))
    }
}

pub fn status(
    servers: &BTreeMap<String, ServerConfig>,
    target: Option<&str>,
) -> Result<(Value, String)> {
    let store = read_store_readonly().map_err(|e| Failure::new(1, "runtime", format!("{e:#}")))?;
    let now = now_unix_secs();
    if let Some(name) = target {
        let cfg = servers
            .get(name)
            .ok_or_else(|| Failure::new(2, "unknown_server", format!("unknown server {name:?}")))?;
        let ServerConfig::Http { url, oauth, .. } = cfg else {
            return Err(Failure::new(
                2,
                "invalid_transport",
                format!("server {name:?} uses stdio transport; OAuth applies only to http servers"),
            )
            .into());
        };
        return server_status_entry(
            name,
            url,
            oauth.as_ref().and_then(|o| o.account.as_deref()),
            &store,
            now,
        )
        .map_err(|e| Failure::new(2, "configuration", format!("{e:#}")).into());
    }
    let mut entries = Vec::new();
    let mut lines = Vec::new();
    for (name, cfg) in servers {
        if let ServerConfig::Http { url, oauth, .. } = cfg {
            let (entry, line) = server_status_entry(
                name,
                url,
                oauth.as_ref().and_then(|o| o.account.as_deref()),
                &store,
                now,
            )
            .map_err(|e| Failure::new(2, "configuration", format!("{e:#}")))?;
            entries.push(entry);
            lines.push(line);
        }
    }
    Ok((json!({ "servers": entries }), lines.join("\n")))
}

pub async fn logout(
    server_name: &str,
    server_url: &str,
    oauth: Option<&OAuthConfig>,
    server_enabled: bool,
    budget: Duration,
) -> Result<(Value, String)> {
    let deadline = Instant::now() + budget;
    let parsed_server = reqwest::Url::parse(server_url)
        .map_err(|_| Failure::new(2, "configuration", "invalid MCP server URL"))?;
    let server_loopback = is_loopback_literal_host(parsed_server.host_str());
    let canonical = canonical_resource_url(server_url)
        .map_err(|e| Failure::new(2, "configuration", e.to_string()))?;
    let storage_key = credential_storage_key(&canonical, oauth.and_then(|o| o.account.as_deref()));
    let (_guard, existing) = timeout_at(deadline, async {
        if resolve_store_location_readonly()?.is_none() {
            return Ok::<_, anyhow::Error>((None, None));
        }
        let guard = credential_lock(&storage_key).await?;
        let existing = read_store_readonly()?.resources.get(&storage_key).cloned();
        if existing.is_some() {
            mutate_store(|store| {
                store.resources.remove(&storage_key);
                Ok(())
            })
            .await?;
        }
        Ok((Some(guard), existing))
    })
    .await
    .map_err(|_| Failure::new(5, "timeout", "OAuth logout deadline exceeded"))?
    .map_err(|e| Failure::new(1, "runtime", format!("{e:#}")))?;

    // Local deletion is committed before best-effort remote revocation.
    let _ = timeout_at(deadline, async {
        if let Some(ref cred) = existing
            && let Some(ref rev_raw) = cred.revocation_endpoint
        {
            let auth_method = cred.token_endpoint_auth_method.as_deref().unwrap_or("none");
            // Disabled servers do not expand ${VAR} templates, so skip confidential-client
            // remote revocation to avoid transmitting unexpanded literals.
            if auth_method != "none" && !server_enabled {
                return;
            }
            let client_secret = oauth.and_then(|o| {
                (o.client_id.as_deref() == Some(cred.client_id.as_str()))
                    .then_some(o.client_secret.as_deref())
                    .flatten()
            });
            let resource_loopback = reqwest::Url::parse(&cred.resource)
                .ok()
                .is_some_and(|u| is_loopback_literal_host(u.host_str()));
            let allow_loopback = server_loopback && resource_loopback;
            if let Ok(rev_url) =
                validate_oauth_endpoint_url(rev_raw, "revocation_endpoint", allow_loopback)
                && let Ok(client) =
                    build_oauth_http_client(rev_raw, allow_loopback, Duration::from_secs(5))
            {
                for (token, hint) in [
                    (cred.refresh_token.as_deref(), "refresh_token"),
                    (Some(cred.access_token.as_str()), "access_token"),
                ] {
                    let Some(token) = token else { continue };
                    let mut form_pairs = vec![("token", token), ("token_type_hint", hint)];
                    let Ok(req) = apply_client_auth(
                        client.post(rev_url.clone()).header(
                            http::header::CONTENT_TYPE,
                            "application/x-www-form-urlencoded",
                        ),
                        &mut form_pairs,
                        &cred.client_id,
                        client_secret,
                        Some(auth_method),
                    ) else {
                        return;
                    };
                    let form = encode_form(&form_pairs);
                    let _ = timeout(Duration::from_secs(5), req.body(form).send()).await;
                }
            }
        }
    })
    .await;

    let out_json = json!({
        "server": server_name,
        "authenticated": false,
        "loggedOut": true,
    });
    let out_text = format!("logged out {server_name}");
    Ok((out_json, out_text))
}
