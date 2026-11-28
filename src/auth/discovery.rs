use super::challenge::{
    BearerChallenge, canonical_resource_url, parse_www_authenticate, validate_mcp_server_url,
};
use crate::{
    discovery::safe_text,
    ipc::Failure,
    oauth_url::{
        is_disallowed_remote_oauth_ip, parse_url_host_ip, validate_metadata_url_for_issuer,
        validate_oauth_endpoint_url,
    },
};
use anyhow::{Context, Result, bail};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{io, net::SocketAddr, sync::Arc, time::Duration};
use tokio::time::{Instant, timeout};

const MAX_OAUTH_HTTP_BYTES: usize = 256 * 1024;

pub(super) fn base64_standard(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
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
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(TABLE[(triple & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

pub(super) fn form_urlencode_component(value: &str) -> String {
    let mut url = reqwest::Url::parse("http://127.0.0.1/").expect("static loopback URL");
    url.query_pairs_mut().append_pair("k", value);
    url.query()
        .and_then(|q| q.strip_prefix("k="))
        .unwrap_or("")
        .to_owned()
}

pub(super) fn select_token_endpoint_auth_method(
    has_client_secret: bool,
    supported: Option<&[String]>,
) -> Result<&'static str> {
    if !has_client_secret {
        return Ok("none");
    }
    match supported {
        None => Ok("client_secret_basic"),
        Some(methods) if methods.iter().any(|m| m == "client_secret_basic") => {
            Ok("client_secret_basic")
        }
        Some(methods) if methods.iter().any(|m| m == "client_secret_post") => {
            Ok("client_secret_post")
        }
        Some(_) => Err(Failure::new(
            4,
            "mcp_protocol",
            "authorization server does not support client_secret_basic or client_secret_post",
        )
        .into()),
    }
}

pub(super) fn apply_client_auth<'a>(
    builder: reqwest::RequestBuilder,
    form_pairs: &mut Vec<(&'a str, &'a str)>,
    client_id: &'a str,
    client_secret: Option<&'a str>,
    auth_method: Option<&str>,
) -> Result<reqwest::RequestBuilder> {
    match auth_method.unwrap_or("none") {
        "client_secret_basic" => {
            let secret = client_secret
                .context("client_secret is required for client_secret_basic authentication")?;
            let encoded = base64_standard(
                format!(
                    "{}:{}",
                    form_urlencode_component(client_id),
                    form_urlencode_component(secret)
                )
                .as_bytes(),
            );
            let mut header = http::HeaderValue::from_str(&format!("Basic {encoded}"))
                .map_err(|_| anyhow::anyhow!("invalid client Basic authorization header"))?;
            header.set_sensitive(true);
            Ok(builder.header(http::header::AUTHORIZATION, header))
        }
        "client_secret_post" => {
            let secret = client_secret
                .context("client_secret is required for client_secret_post authentication")?;
            form_pairs.push(("client_id", client_id));
            form_pairs.push(("client_secret", secret));
            Ok(builder)
        }
        "none" => {
            form_pairs.push(("client_id", client_id));
            Ok(builder)
        }
        other => bail!(
            "unsupported OAuth token_endpoint_auth_method {}",
            safe_text(other)
        ),
    }
}

pub(super) fn encode_form(pairs: &[(&str, &str)]) -> String {
    let mut url = reqwest::Url::parse("http://127.0.0.1/").expect("static loopback URL");
    {
        let mut qp = url.query_pairs_mut();
        for &(k, v) in pairs {
            qp.append_pair(k, v);
        }
    }
    url.query().unwrap_or("").to_owned()
}

struct PublicOnlyDnsResolver;

impl reqwest::dns::Resolve for PublicOnlyDnsResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let host = name.as_str().to_owned();
        Box::pin(async move {
            let resolved: Vec<SocketAddr> = tokio::net::lookup_host((host.as_str(), 0))
                .await
                .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?
                .collect();
            if resolved.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "OAuth host resolved to no addresses",
                )
                .into());
            }
            if resolved
                .iter()
                .any(|addr| is_disallowed_remote_oauth_ip(addr.ip()))
            {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "OAuth endpoint resolved to a loopback, private, or metadata address",
                )
                .into());
            }
            let addrs: reqwest::dns::Addrs = Box::new(resolved.into_iter());
            Ok(addrs)
        })
    }
}

pub(super) fn build_oauth_http_client(
    target_url: &str,
    allow_loopback: bool,
    connect_timeout: Duration,
) -> Result<reqwest::Client> {
    let builder = || {
        let mut b = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .connect_timeout(connect_timeout);
        if !allow_loopback {
            b = b.dns_resolver(Arc::new(PublicOnlyDnsResolver));
        }
        b
    };
    builder()
        .build()
        .or_else(|error| {
            if allow_loopback && reqwest::Url::parse(target_url).is_ok_and(|u| u.scheme() == "http")
            {
                builder().tls_certs_only(std::iter::empty()).build()
            } else {
                Err(error)
            }
        })
        .map_err(|_| anyhow::anyhow!("cannot create HTTP client; check the system CA certificates"))
}

pub(super) async fn read_bounded_json(mut response: reqwest::Response) -> Result<Value> {
    if response
        .content_length()
        .is_some_and(|len| len > MAX_OAUTH_HTTP_BYTES as u64)
    {
        bail!("OAuth response exceeds size limit");
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| anyhow::anyhow!("OAuth HTTP read error: {}", e.without_url()))?
    {
        if body.len().saturating_add(chunk.len()) > MAX_OAUTH_HTTP_BYTES {
            bail!("OAuth response exceeds size limit");
        }
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body).context("OAuth endpoint returned invalid JSON")
}

#[derive(Deserialize)]
pub(super) struct TokenEndpointResponse {
    pub(super) access_token: String,
    #[serde(default)]
    pub(super) token_type: Option<String>,
    #[serde(default)]
    pub(super) refresh_token: Option<String>,
    #[serde(default)]
    pub(super) expires_in: Option<u64>,
    #[serde(default)]
    pub(super) scope: Option<String>,
}

pub(super) fn validate_bearer_token_string(token: &str) -> Result<()> {
    if token.is_empty() {
        bail!("OAuth token endpoint returned an empty access_token");
    }
    http::HeaderValue::from_str(&format!("Bearer {token}"))
        .map_err(|_| anyhow::anyhow!("OAuth token endpoint returned an invalid Bearer token"))?;
    Ok(())
}

pub(super) async fn ensure_resolved_host_allowed(
    url: &reqwest::Url,
    label: &str,
    allow_loopback: bool,
) -> Result<()> {
    if allow_loopback {
        return Ok(());
    }
    let Some(host) = url.host_str() else {
        bail!("invalid {label} URL");
    };
    if parse_url_host_ip(host).is_some() {
        return Ok(());
    }
    let port = url.port_or_known_default().unwrap_or(443);
    if let Ok(Ok(addrs)) = timeout(
        Duration::from_secs(3),
        tokio::net::lookup_host((host, port)),
    )
    .await
    {
        for addr in addrs {
            if is_disallowed_remote_oauth_ip(addr.ip()) {
                bail!(
                    "{label} resolved to a loopback, private, or metadata address for non-loopback MCP servers"
                );
            }
        }
    }
    Ok(())
}

#[derive(Deserialize)]
pub(super) struct ProtectedResourceMetadata {
    pub(super) resource: String,
    #[serde(default)]
    pub(super) authorization_servers: Vec<String>,
}

#[derive(Deserialize)]
pub(super) struct AuthorizationServerMetadata {
    #[serde(default)]
    pub(super) issuer: Option<String>,
    pub(super) authorization_endpoint: String,
    pub(super) token_endpoint: String,
    #[serde(default)]
    pub(super) registration_endpoint: Option<String>,
    #[serde(default)]
    pub(super) revocation_endpoint: Option<String>,
    #[serde(default)]
    pub(super) code_challenge_methods_supported: Vec<String>,
    #[serde(default)]
    pub(super) token_endpoint_auth_methods_supported: Option<Vec<String>>,
}

#[derive(Deserialize)]
struct DynamicClientRegistrationResponse {
    client_id: String,
}

fn origin_with_path(base: &reqwest::Url, path: &str) -> reqwest::Url {
    let mut u = base.clone();
    u.set_query(None);
    u.set_fragment(None);
    u.set_path(path);
    u
}

pub(crate) fn protected_resource_metadata_urls(
    server_url: &reqwest::Url,
    challenge_hint: Option<&str>,
) -> Result<Vec<reqwest::Url>> {
    let (_, allow_loopback) = validate_mcp_server_url(server_url.as_str())?;
    let mut urls = Vec::new();
    let mut push_unique = |candidate: reqwest::Url| {
        if !urls
            .iter()
            .any(|existing: &reqwest::Url| existing.as_str() == candidate.as_str())
        {
            urls.push(candidate);
        }
    };
    if let Some(hint) = challenge_hint {
        let validated = validate_oauth_endpoint_url(hint, "resource_metadata", allow_loopback)?;
        push_unique(validated);
    }
    let resource_path = if server_url.path() == "/" {
        ""
    } else {
        server_url.path()
    };
    let mut metadata_url = server_url.clone();
    metadata_url.set_path(&format!(
        "/.well-known/oauth-protected-resource{resource_path}"
    ));
    push_unique(metadata_url.clone());
    metadata_url.set_path("/.well-known/oauth-protected-resource");
    push_unique(metadata_url);
    Ok(urls)
}

pub(crate) fn authorization_server_metadata_urls(issuer_url: &reqwest::Url) -> Vec<reqwest::Url> {
    let trimmed_path = issuer_url.path().trim_end_matches('/');
    if trimmed_path.is_empty() {
        vec![
            origin_with_path(issuer_url, "/.well-known/oauth-authorization-server"),
            origin_with_path(issuer_url, "/.well-known/openid-configuration"),
        ]
    } else {
        vec![
            origin_with_path(
                issuer_url,
                &format!("/.well-known/oauth-authorization-server{trimmed_path}"),
            ),
            origin_with_path(
                issuer_url,
                &format!("/.well-known/openid-configuration{trimmed_path}"),
            ),
            origin_with_path(
                issuer_url,
                &format!("{trimmed_path}/.well-known/openid-configuration"),
            ),
        ]
    }
}

pub(super) async fn probe_mcp_server_challenge(
    client: &reqwest::Client,
    server_url: &reqwest::Url,
    step_budget: Duration,
) -> Result<BearerChallenge> {
    let probe_body = br#"{"jsonrpc":"2.0","id":1,"method":"server/discover","params":{}}"#;
    let resp = timeout(
        step_budget,
        client
            .post(server_url.clone())
            .header(http::header::ACCEPT, "application/json, text/event-stream")
            .header(http::header::CONTENT_TYPE, "application/json")
            .header("Mcp-Protocol-Version", "2026-07-28")
            .body(&probe_body[..])
            .send(),
    )
    .await
    .map_err(|_| Failure::new(5, "timeout", "timed out probing MCP server"))?
    .map_err(|e| {
        Failure::new(
            4,
            "mcp_transport",
            format!("cannot reach MCP server: {}", e.without_url()),
        )
    })?;
    Ok(parse_www_authenticate(resp.headers()))
}

pub(super) async fn discover_protected_resource(
    client: &reqwest::Client,
    candidates: &[reqwest::Url],
    canonical_server_resource: &str,
    deadline: Instant,
) -> Result<ProtectedResourceMetadata> {
    let mut last_status = None;
    for candidate in candidates {
        let rem = deadline.saturating_duration_since(Instant::now());
        if rem.is_zero() {
            return Err(Failure::new(5, "timeout", "OAuth discovery timed out").into());
        }
        let resp = timeout(
            rem,
            client
                .get(candidate.clone())
                .header(http::header::ACCEPT, "application/json")
                .send(),
        )
        .await
        .map_err(|_| Failure::new(5, "timeout", "OAuth discovery timed out"))?
        .map_err(|e| {
            Failure::new(
                4,
                "mcp_transport",
                format!(
                    "protected resource metadata request failed: {}",
                    e.without_url()
                ),
            )
        })?;
        let status = resp.status();
        if !status.is_success() {
            last_status = Some(status.as_u16());
            continue;
        }
        let json = read_bounded_json(resp)
            .await
            .map_err(|e| Failure::new(4, "mcp_protocol", format!("{e:#}")))?;
        let meta: ProtectedResourceMetadata = serde_json::from_value(json).map_err(|_| {
            Failure::new(
                4,
                "mcp_protocol",
                "invalid OAuth protected resource metadata document",
            )
        })?;
        let discovered_canonical = canonical_resource_url(&meta.resource).map_err(|e| {
            Failure::new(
                4,
                "mcp_protocol",
                format!("invalid protected resource identifier: {e}"),
            )
        })?;
        if discovered_canonical != canonical_server_resource {
            return Err(Failure::new(
                4,
                "mcp_protocol",
                format!(
                    "protected resource metadata mismatch: expected {}, got {}",
                    safe_text(canonical_server_resource),
                    safe_text(&discovered_canonical)
                ),
            )
            .into());
        }
        if meta.authorization_servers.is_empty() {
            return Err(Failure::new(
                4,
                "mcp_protocol",
                "protected resource metadata lists no authorization_servers",
            )
            .into());
        }
        return Ok(meta);
    }
    let detail = match last_status {
        Some(code) => format!(" (last HTTP status {code})"),
        None => String::new(),
    };
    Err(Failure::new(
        4,
        "mcp_protocol",
        format!("cannot discover OAuth protected resource metadata{detail}"),
    )
    .into())
}

pub(super) async fn discover_authorization_server(
    client: &reqwest::Client,
    issuer_url: &reqwest::Url,
    canonical_issuer: &str,
    allow_loopback: bool,
    deadline: Instant,
) -> Result<AuthorizationServerMetadata> {
    let candidates = authorization_server_metadata_urls(issuer_url);
    let mut last_status = None;
    for candidate in &candidates {
        let rem = deadline.saturating_duration_since(Instant::now());
        if rem.is_zero() {
            return Err(Failure::new(5, "timeout", "OAuth discovery timed out").into());
        }
        let resp = timeout(
            rem,
            client
                .get(candidate.clone())
                .header(http::header::ACCEPT, "application/json")
                .send(),
        )
        .await
        .map_err(|_| Failure::new(5, "timeout", "OAuth discovery timed out"))?
        .map_err(|e| {
            Failure::new(
                4,
                "mcp_transport",
                format!(
                    "authorization server metadata request failed: {}",
                    e.without_url()
                ),
            )
        })?;
        let status = resp.status();
        if !status.is_success() {
            last_status = Some(status.as_u16());
            continue;
        }
        let json = read_bounded_json(resp)
            .await
            .map_err(|e| Failure::new(4, "mcp_protocol", format!("{e:#}")))?;
        let meta: AuthorizationServerMetadata = serde_json::from_value(json).map_err(|_| {
            Failure::new(
                4,
                "mcp_protocol",
                "invalid OAuth authorization server metadata document",
            )
        })?;
        if let Some(ref meta_issuer) = meta.issuer {
            let discovered_issuer = canonical_resource_url(meta_issuer).map_err(|e| {
                Failure::new(
                    4,
                    "mcp_protocol",
                    format!("invalid authorization server issuer: {e}"),
                )
            })?;
            if discovered_issuer != canonical_issuer {
                return Err(Failure::new(
                    4,
                    "mcp_protocol",
                    format!(
                        "authorization server issuer mismatch: expected {}, got {}",
                        safe_text(canonical_issuer),
                        safe_text(&discovered_issuer)
                    ),
                )
                .into());
            }
        }
        validate_authorization_server_endpoints(&meta, allow_loopback)?;
        return Ok(meta);
    }
    let detail = match last_status {
        Some(code) => format!(" (last HTTP status {code})"),
        None => String::new(),
    };
    Err(Failure::new(
        4,
        "mcp_protocol",
        format!("cannot discover OAuth authorization server metadata{detail}"),
    )
    .into())
}

fn validate_authorization_server_endpoints(
    meta: &AuthorizationServerMetadata,
    allow_loopback: bool,
) -> Result<()> {
    if !meta
        .code_challenge_methods_supported
        .iter()
        .any(|m| m == "S256")
    {
        return Err(Failure::new(
            4,
            "mcp_protocol",
            "authorization server does not support PKCE S256",
        )
        .into());
    }
    validate_oauth_endpoint_url(
        &meta.authorization_endpoint,
        "authorization_endpoint",
        allow_loopback,
    )
    .map_err(|e| Failure::new(4, "mcp_protocol", e.to_string()))?;
    validate_oauth_endpoint_url(&meta.token_endpoint, "token_endpoint", allow_loopback)
        .map_err(|e| Failure::new(4, "mcp_protocol", e.to_string()))?;
    if let Some(ref reg) = meta.registration_endpoint {
        validate_oauth_endpoint_url(reg, "registration_endpoint", allow_loopback)
            .map_err(|e| Failure::new(4, "mcp_protocol", e.to_string()))?;
    }
    if let Some(ref rev) = meta.revocation_endpoint {
        validate_oauth_endpoint_url(rev, "revocation_endpoint", allow_loopback)
            .map_err(|e| Failure::new(4, "mcp_protocol", e.to_string()))?;
    }
    Ok(())
}

pub(super) async fn fetch_explicit_authorization_server_metadata(
    client: &reqwest::Client,
    metadata_url: &reqwest::Url,
    allow_loopback: bool,
    deadline: Instant,
) -> Result<(AuthorizationServerMetadata, String)> {
    ensure_resolved_host_allowed(metadata_url, "auth_server_metadata_url", allow_loopback)
        .await
        .map_err(|e| Failure::new(4, "mcp_protocol", e.to_string()))?;
    let rem = deadline.saturating_duration_since(Instant::now());
    if rem.is_zero() {
        return Err(Failure::new(5, "timeout", "OAuth discovery timed out").into());
    }
    let resp = timeout(
        rem,
        client
            .get(metadata_url.clone())
            .header(http::header::ACCEPT, "application/json")
            .send(),
    )
    .await
    .map_err(|_| Failure::new(5, "timeout", "OAuth discovery timed out"))?
    .map_err(|e| {
        Failure::new(
            4,
            "mcp_transport",
            format!(
                "authorization server metadata request failed: {}",
                e.without_url()
            ),
        )
    })?;
    if !resp.status().is_success() {
        return Err(Failure::new(
            4,
            "mcp_protocol",
            format!(
                "cannot fetch OAuth authorization server metadata (HTTP {})",
                resp.status().as_u16()
            ),
        )
        .into());
    }
    let json = read_bounded_json(resp)
        .await
        .map_err(|e| Failure::new(4, "mcp_protocol", format!("{e:#}")))?;
    let meta: AuthorizationServerMetadata = serde_json::from_value(json).map_err(|_| {
        Failure::new(
            4,
            "mcp_protocol",
            "invalid OAuth authorization server metadata document",
        )
    })?;
    let raw_issuer = meta
        .issuer
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            Failure::new(
                4,
                "mcp_protocol",
                "authorization server metadata document is missing issuer",
            )
        })?;
    let issuer_url =
        validate_oauth_endpoint_url(raw_issuer, "authorization server issuer", allow_loopback)
            .map_err(|e| Failure::new(4, "mcp_protocol", e.to_string()))?;
    validate_metadata_url_for_issuer(metadata_url, &issuer_url)
        .map_err(|e| Failure::new(4, "mcp_protocol", e.to_string()))?;
    let canonical_issuer = canonical_resource_url(raw_issuer).map_err(|e| {
        Failure::new(
            4,
            "mcp_protocol",
            format!("invalid authorization server issuer: {e}"),
        )
    })?;
    validate_authorization_server_endpoints(&meta, allow_loopback)?;
    Ok((meta, canonical_issuer))
}

pub(super) async fn register_public_client(
    client: &reqwest::Client,
    registration_endpoint: &str,
    redirect_uri: &str,
    client_name: &str,
    allow_loopback: bool,
    deadline: Instant,
) -> Result<String> {
    let url = validate_oauth_endpoint_url(
        registration_endpoint,
        "registration_endpoint",
        allow_loopback,
    )
    .map_err(|e| Failure::new(4, "mcp_protocol", e.to_string()))?;
    let rem = deadline.saturating_duration_since(Instant::now());
    if rem.is_zero() {
        return Err(Failure::new(5, "timeout", "dynamic client registration timed out").into());
    }
    let payload = json!({
        "client_name": client_name,
        "redirect_uris": [redirect_uri],
        "token_endpoint_auth_method": "none",
        "grant_types": ["authorization_code", "refresh_token"],
        "response_types": ["code"]
    });
    let resp = timeout(
        rem,
        client
            .post(url)
            .header(http::header::ACCEPT, "application/json")
            .json(&payload)
            .send(),
    )
    .await
    .map_err(|_| Failure::new(5, "timeout", "dynamic client registration timed out"))?
    .map_err(|e| {
        Failure::new(
            4,
            "mcp_transport",
            format!("dynamic client registration failed: {}", e.without_url()),
        )
    })?;
    if !resp.status().is_success() {
        return Err(Failure::new(
            4,
            "mcp_protocol",
            format!(
                "dynamic client registration failed with HTTP {}",
                resp.status().as_u16()
            ),
        )
        .into());
    }
    let body = read_bounded_json(resp)
        .await
        .map_err(|e| Failure::new(4, "mcp_protocol", format!("{e:#}")))?;
    let reg: DynamicClientRegistrationResponse = serde_json::from_value(body).map_err(|_| {
        Failure::new(
            4,
            "mcp_protocol",
            "invalid dynamic client registration response",
        )
    })?;
    if reg.client_id.trim().is_empty() {
        return Err(Failure::new(
            4,
            "mcp_protocol",
            "dynamic client registration returned an empty client_id",
        )
        .into());
    }
    Ok(reg.client_id)
}
