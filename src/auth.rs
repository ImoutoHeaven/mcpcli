//! OAuth 2.1 PKCE client flow, RFC 9728/8414 discovery, `WWW-Authenticate` diagnostics,
//! private credential persistence, and singleflight refresh rotation.
use crate::{config::ServerConfig, discovery::safe_text, ipc::Failure, platform, runtime::Lock};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    ffi::OsStr,
    io::{self, Write},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::{Instant, timeout, timeout_at},
};

const MAX_CREDENTIALS_BYTES: usize = 1 << 20;
const MAX_OAUTH_HTTP_BYTES: usize = 256 * 1024;
const MAX_WWW_AUTH_HEADER_BYTES: usize = 8 * 1024;
const MAX_AUTH_PARAM_BYTES: usize = 2 * 1024;
const PROACTIVE_REFRESH_WINDOW_SECS: u64 = 30;
const STORE_LOCK_WAIT: Duration = Duration::from_secs(10);

static WRITE_SEQ: AtomicU64 = AtomicU64::new(1);

fn store_version() -> u32 {
    1
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StoredCredential {
    pub resource: String,
    pub issuer: String,
    pub token_endpoint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revocation_endpoint: Option<String>,
    pub client_id: String,
    pub access_token: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<u64>,
    #[serde(default)]
    pub scopes: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CredentialsStoreFile {
    #[serde(default = "store_version")]
    version: u32,
    #[serde(default)]
    resources: BTreeMap<String, StoredCredential>,
}

impl Default for CredentialsStoreFile {
    fn default() -> Self {
        Self {
            version: 1,
            resources: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BearerChallenge {
    pub resource_metadata: Option<String>,
    pub scope: Option<String>,
    pub error: Option<String>,
    pub error_description: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthChallengeInfo {
    pub status: u16,
    pub resource_metadata: Option<String>,
    pub required_scopes: Vec<String>,
    pub error: Option<String>,
    pub error_description: Option<String>,
}

impl AuthChallengeInfo {
    pub fn from_response(status: http::StatusCode, headers: &http::HeaderMap) -> Self {
        let parsed = parse_www_authenticate(headers);
        let required_scopes = parsed
            .scope
            .as_deref()
            .map(split_scopes)
            .unwrap_or_default();
        Self {
            status: status.as_u16(),
            resource_metadata: parsed.resource_metadata,
            required_scopes,
            error: parsed.error,
            error_description: parsed.error_description,
        }
    }

    pub fn sanitized_summary(&self) -> String {
        let mut parts = vec![format!("status={}", self.status)];
        if let Some(rm) = &self.resource_metadata {
            parts.push(format!("resource_metadata={rm}"));
        }
        if !self.required_scopes.is_empty() {
            parts.push(format!("scope={}", self.required_scopes.join(" ")));
        }
        if let Some(err) = &self.error {
            parts.push(format!("error={err}"));
        }
        parts.join(" ")
    }

    pub fn format_message(&self, server: Option<&str>) -> String {
        let target = server.unwrap_or("<server>");
        let head = if self.status == 403 {
            "HTTP 403 insufficient scope"
        } else {
            "HTTP 401 authentication required"
        };
        let mut details = Vec::new();
        if let Some(rm) = &self.resource_metadata {
            details.push(format!("resourceMetadata={rm}"));
        }
        if !self.required_scopes.is_empty() {
            details.push(format!("requiredScopes={}", self.required_scopes.join(",")));
        }
        if let Some(err) = &self.error {
            details.push(format!("error={err}"));
        }
        if let Some(desc) = &self.error_description {
            details.push(format!("errorDescription={desc}"));
        }
        if details.is_empty() {
            format!("{head}; run: mcpcli auth login {target}")
        } else {
            format!(
                "{head} ({}); run: mcpcli auth login {target}",
                details.join(", ")
            )
        }
    }

    pub fn to_data_value(&self, server: Option<&str>) -> Value {
        let target = server.unwrap_or("<server>");
        let mut map = Map::new();
        map.insert("status".into(), Value::Number(self.status.into()));
        map.insert(
            "resourceMetadata".into(),
            self.resource_metadata
                .as_ref()
                .map_or(Value::Null, |v| Value::String(v.clone())),
        );
        map.insert(
            "requiredScopes".into(),
            Value::Array(
                self.required_scopes
                    .iter()
                    .cloned()
                    .map(Value::String)
                    .collect(),
            ),
        );
        if let Some(err) = &self.error {
            map.insert("error".into(), Value::String(err.clone()));
        }
        if let Some(desc) = &self.error_description {
            map.insert("errorDescription".into(), Value::String(desc.clone()));
        }
        map.insert(
            "hint".into(),
            Value::String(format!("run: mcpcli auth login {target}")),
        );
        Value::Object(map)
    }
}

fn split_scopes(raw: &str) -> Vec<String> {
    let mut out = Vec::new();
    for item in raw.split_whitespace() {
        if !item.is_empty() && !out.iter().any(|s| s == item) {
            out.push(item.to_owned());
        }
    }
    out
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Preserve non-root paths and queries when identifying an OAuth resource.
pub fn canonical_resource_url(raw: &str) -> Result<String> {
    let mut url = reqwest::Url::parse(raw).map_err(|_| anyhow::anyhow!("invalid resource URL"))?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        bail!("resource URL must be http(s) without user credentials");
    }
    url.set_fragment(None);
    let query = url.query().map(str::to_owned);
    url.set_query(None);
    let mut canonical = url.as_str().to_owned();
    if url.path() == "/" {
        canonical = canonical.trim_end_matches('/').to_owned();
    }
    if let Some(q) = query {
        canonical.push('?');
        canonical.push_str(&q);
    }
    Ok(canonical)
}

fn parse_url_host_ip(host: &str) -> Option<IpAddr> {
    if let Some(v6_str) = host.strip_prefix('[').and_then(|s| s.strip_suffix(']')) {
        v6_str.parse::<Ipv6Addr>().ok().map(IpAddr::V6)
    } else {
        host.parse::<Ipv4Addr>().ok().map(IpAddr::V4)
    }
}

fn is_loopback_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4.is_loopback(),
        IpAddr::V6(v6) => {
            v6.is_loopback() || v6.to_ipv4_mapped().is_some_and(|v4| v4.is_loopback())
        }
    }
}

fn is_non_public_ipv4(v4: Ipv4Addr) -> bool {
    let octets = v4.octets();
    v4.is_loopback()
        || v4.is_private()
        || v4.is_link_local()
        || v4.is_unspecified()
        || v4.is_broadcast()
        || v4.is_multicast()
        || octets[0] == 0
        || (octets[0] == 100 && (octets[1] & 0b1100_0000) == 64)
        || (octets[0] == 192 && octets[1] == 0 && octets[2] == 0)
        || (octets[0] == 198 && (octets[1] & 0xfe) == 18)
        || octets[0] >= 240
}

fn is_disallowed_remote_oauth_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_non_public_ipv4(v4),
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return is_non_public_ipv4(v4);
            }
            let seg0 = v6.segments()[0];
            v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                || (seg0 & 0xfe00) == 0xfc00
                || (seg0 & 0xffc0) == 0xfe80
        }
    }
}

pub(crate) fn is_loopback_literal_host(host: Option<&str>) -> bool {
    let Some(h) = host else {
        return false;
    };
    if let Some(ip) = parse_url_host_ip(h) {
        return is_loopback_ip(ip);
    }
    let trimmed = h.trim_end_matches('.');
    trimmed.eq_ignore_ascii_case("localhost")
        || trimmed.to_ascii_lowercase().ends_with(".localhost")
}

fn is_disallowed_remote_oauth_host(host: &str) -> bool {
    if let Some(ip) = parse_url_host_ip(host) {
        return is_disallowed_remote_oauth_ip(ip);
    }
    let lower = host.trim_end_matches('.').to_ascii_lowercase();
    lower == "localhost"
        || lower.ends_with(".localhost")
        || lower == "localhost.localdomain"
        || lower == "ip6-localhost"
        || lower == "ip6-loopback"
        || lower.ends_with(".local")
        || lower == "metadata"
        || lower == "metadata.google.internal"
        || lower == "instance-data"
}

fn validate_mcp_server_url(raw: &str) -> Result<(reqwest::Url, bool)> {
    let url = reqwest::Url::parse(raw).map_err(|_| anyhow::anyhow!("invalid MCP server URL"))?;
    if !url.username().is_empty() || url.password().is_some() || url.fragment().is_some() {
        bail!("MCP server URL must not include credentials or a fragment");
    }
    if url.host_str().is_none() {
        bail!("invalid MCP server URL");
    }
    let allow_loopback = is_loopback_literal_host(url.host_str());
    match url.scheme() {
        "https" => Ok((url, allow_loopback)),
        "http" if allow_loopback => Ok((url, true)),
        _ => bail!(
            "MCP server must use https:// or loopback http:// (127.0.0.0/8, [::1], or localhost)"
        ),
    }
}

/// Validate OAuth discovery, registration, authorization, token, or revocation URLs.
pub fn validate_oauth_endpoint_url(
    raw: &str,
    label: &str,
    allow_loopback: bool,
) -> Result<reqwest::Url> {
    let url = reqwest::Url::parse(raw).map_err(|_| anyhow::anyhow!("invalid {label} URL"))?;
    if !url.username().is_empty() || url.password().is_some() || url.fragment().is_some() {
        bail!("{label} URL must not include credentials or a fragment");
    }
    let Some(host) = url.host_str() else {
        bail!("invalid {label} URL");
    };
    if allow_loopback {
        match url.scheme() {
            "https" => Ok(url),
            "http" if is_loopback_literal_host(Some(host)) => Ok(url),
            _ => bail!(
                "{label} must use https:// or loopback http:// (127.0.0.0/8, [::1], or localhost)"
            ),
        }
    } else {
        if url.scheme() != "https" {
            bail!("{label} must use https:// for non-loopback MCP servers");
        }
        if is_disallowed_remote_oauth_host(host) {
            bail!(
                "{label} must not target loopback, private, or metadata addresses for non-loopback MCP servers"
            );
        }
        Ok(url)
    }
}

fn is_tchar_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric()
        || matches!(
            b,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}

fn skip_ows(bytes: &[u8], mut i: usize) -> usize {
    while i < bytes.len() && matches!(bytes[i], b' ' | b'\t') {
        i += 1;
    }
    i
}

fn skip_ows_and_commas(bytes: &[u8], mut i: usize) -> usize {
    while i < bytes.len() && matches!(bytes[i], b' ' | b'\t' | b',') {
        i += 1;
    }
    i
}

fn read_token(input: &str, start: usize) -> Option<(&str, usize)> {
    let bytes = input.as_bytes();
    let mut end = start;
    while end < bytes.len() && is_tchar_byte(bytes[end]) {
        end += 1;
    }
    (end > start).then(|| (&input[start..end], end))
}

fn read_quoted_string(input: &str, start: usize) -> Option<(String, usize)> {
    let bytes = input.as_bytes();
    if bytes.get(start) != Some(&b'"') {
        return None;
    }
    let mut out = String::new();
    let mut i = start + 1;
    while i < bytes.len() {
        if bytes[i] == b'\\' {
            i += 1;
            let ch = input[i..].chars().next()?;
            out.push(ch);
            i += ch.len_utf8();
        } else if bytes[i] == b'"' {
            return Some((out, i + 1));
        } else {
            let ch = input[i..].chars().next()?;
            out.push(ch);
            i += ch.len_utf8();
        }
    }
    None
}

fn sanitize_auth_param(value: &str) -> Option<String> {
    if value.is_empty() || value.len() > MAX_AUTH_PARAM_BYTES {
        return None;
    }
    Some(safe_text(value))
}

fn sanitize_resource_metadata(value: &str) -> Option<String> {
    let cleaned = sanitize_auth_param(value)?;
    let parsed = reqwest::Url::parse(&cleaned).ok()?;
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.fragment().is_some()
    {
        return None;
    }
    Some(parsed.into())
}

/// Extract RFC 6750 / RFC 9728 `Bearer` challenge parameters from `WWW-Authenticate` headers.
pub fn parse_www_authenticate(headers: &http::HeaderMap) -> BearerChallenge {
    let mut out = BearerChallenge::default();
    for header_val in headers.get_all(http::header::WWW_AUTHENTICATE) {
        let raw_bytes = header_val.as_bytes();
        if raw_bytes.len() > MAX_WWW_AUTH_HEADER_BYTES {
            continue;
        }
        let Ok(input) = header_val.to_str() else {
            continue;
        };
        parse_www_authenticate_str(input, &mut out);
    }
    out
}

fn parse_www_authenticate_str(input: &str, out: &mut BearerChallenge) {
    let bytes = input.as_bytes();
    let mut idx = skip_ows_and_commas(bytes, 0);
    let mut pre_read_scheme: Option<(&str, usize)> = None;

    while idx < bytes.len() || pre_read_scheme.is_some() {
        let (scheme, after_scheme) = match pre_read_scheme.take() {
            Some(pair) => pair,
            None => {
                let Some(pair) = read_token(input, idx) else {
                    break;
                };
                pair
            }
        };
        let is_bearer = scheme.eq_ignore_ascii_case("bearer");
        idx = skip_ows(bytes, after_scheme);
        if idx >= bytes.len() {
            break;
        }
        if bytes[idx] == b',' {
            idx = skip_ows_and_commas(bytes, idx + 1);
            continue;
        }

        // Parse auth-params (`token = (token | quoted-string)`) or skip token68.
        loop {
            idx = skip_ows(bytes, idx);
            if idx >= bytes.len() {
                break;
            }
            if bytes[idx] == b',' {
                idx = skip_ows_and_commas(bytes, idx + 1);
                if idx >= bytes.len() {
                    break;
                }
            }
            let Some((tok, after_tok)) = read_token(input, idx) else {
                // Skip token68 or malformed segment up to next comma.
                while idx < bytes.len() && bytes[idx] != b',' {
                    idx += 1;
                }
                idx = skip_ows_and_commas(bytes, idx);
                break;
            };
            let eq_pos = skip_ows(bytes, after_tok);
            if eq_pos >= bytes.len() || bytes[eq_pos] != b'=' {
                if eq_pos < bytes.len() {
                    if eq_pos == after_tok && bytes[eq_pos] != b',' {
                        while idx < bytes.len() && bytes[idx] != b',' {
                            idx += 1;
                        }
                        idx = skip_ows_and_commas(bytes, idx);
                        break;
                    }
                    if bytes[eq_pos] == b',' {
                        idx = skip_ows_and_commas(bytes, eq_pos + 1);
                        break;
                    }
                    pre_read_scheme = Some((tok, after_tok));
                    break;
                }
                idx = after_tok;
                break;
            }
            // Distinguish `param = value` from token68 trailing `=`.
            let val_pos = skip_ows(bytes, eq_pos + 1);
            if val_pos >= bytes.len() || bytes[val_pos] == b',' || bytes[val_pos] == b'=' {
                while idx < bytes.len() && bytes[idx] != b',' {
                    idx += 1;
                }
                idx = skip_ows_and_commas(bytes, idx);
                break;
            }
            let parsed_val = if bytes[val_pos] == b'"' {
                read_quoted_string(input, val_pos)
            } else {
                read_token(input, val_pos).map(|(s, end)| (s.to_owned(), end))
            };
            let Some((val, next_pos)) = parsed_val else {
                return;
            };
            if is_bearer {
                if tok.eq_ignore_ascii_case("resource_metadata") && out.resource_metadata.is_none()
                {
                    out.resource_metadata = sanitize_resource_metadata(&val);
                } else if tok.eq_ignore_ascii_case("scope") && out.scope.is_none() {
                    out.scope = sanitize_auth_param(&val);
                } else if tok.eq_ignore_ascii_case("error") && out.error.is_none() {
                    out.error = sanitize_auth_param(&val);
                } else if tok.eq_ignore_ascii_case("error_description")
                    && out.error_description.is_none()
                {
                    out.error_description = sanitize_auth_param(&val);
                }
            }
            idx = skip_ows(bytes, next_pos);
            if idx < bytes.len() && bytes[idx] == b',' {
                idx = skip_ows_and_commas(bytes, idx + 1);
                continue;
            }
            break;
        }
    }
}

struct StoreLocation {
    file_path: PathBuf,
    explicit_override: bool,
}

fn non_empty_env(key: &str) -> Option<std::ffi::OsString> {
    std::env::var_os(key).filter(|v| !v.is_empty())
}

fn resolve_store_location_readonly() -> Result<Option<PathBuf>> {
    if let Some(override_path) = non_empty_env("MCPCLI_CREDENTIALS") {
        let raw = PathBuf::from(override_path);
        let abs = if raw.is_absolute() {
            raw
        } else {
            std::env::current_dir()?.join(raw)
        };
        if !abs.try_exists()? {
            return Ok(None);
        }
        let parent = abs
            .parent()
            .context("MCPCLI_CREDENTIALS must include a parent directory")?
            .canonicalize()?;
        let name = abs
            .file_name()
            .context("MCPCLI_CREDENTIALS must name a file")?;
        return Ok(Some(parent.join(name)));
    }
    if let Some(rt) = non_empty_env("MCPCLI_RUNTIME_DIR") {
        let root = PathBuf::from(rt);
        let candidate = root.join("credentials.json");
        if !candidate.try_exists()? {
            return Ok(None);
        }
        return Ok(Some(root.canonicalize()?.join("credentials.json")));
    }
    #[cfg(unix)]
    {
        if let Some(xdg) = non_empty_env("XDG_STATE_HOME")
            && Path::new(&xdg).is_absolute()
        {
            let candidate = PathBuf::from(&xdg).join("mcpcli").join("credentials.json");
            if candidate.try_exists().unwrap_or(false) {
                let dir = PathBuf::from(xdg).join("mcpcli").canonicalize()?;
                return Ok(Some(dir.join("credentials.json")));
            }
            return Ok(None);
        }
        if let Some(home) = non_empty_env("HOME")
            && Path::new(&home).is_absolute()
        {
            let candidate = PathBuf::from(&home)
                .join(".local")
                .join("state")
                .join("mcpcli")
                .join("credentials.json");
            if candidate.try_exists().unwrap_or(false) {
                let dir = PathBuf::from(home)
                    .join(".local")
                    .join("state")
                    .join("mcpcli")
                    .canonicalize()?;
                return Ok(Some(dir.join("credentials.json")));
            }
            return Ok(None);
        }
        let rt = platform::default_runtime()?;
        let candidate = rt.join("credentials.json");
        if candidate.try_exists().unwrap_or(false) {
            return Ok(Some(rt.canonicalize()?.join("credentials.json")));
        }
        Ok(None)
    }
    #[cfg(windows)]
    {
        if let Some(local) = non_empty_env("LOCALAPPDATA") {
            let candidate = PathBuf::from(&local)
                .join("mcpcli")
                .join("state")
                .join("credentials.json");
            if candidate.try_exists().unwrap_or(false) {
                let dir = PathBuf::from(local)
                    .join("mcpcli")
                    .join("state")
                    .canonicalize()?;
                return Ok(Some(dir.join("credentials.json")));
            }
        }
        Ok(None)
    }
}

#[cfg(unix)]
fn ensure_state_base_unix(state_base: &Path) -> Result<PathBuf> {
    if !state_base.is_absolute() {
        bail!("state directory must be absolute");
    }
    let mut existing = state_base;
    let mut missing: Vec<&OsStr> = Vec::new();
    while !existing.exists() {
        let name = existing
            .file_name()
            .context("state directory ancestor is missing")?;
        missing.push(name);
        existing = existing
            .parent()
            .context("state directory ancestor is missing")?;
    }
    let mut cur = existing.canonicalize()?;
    for part in missing.into_iter().rev() {
        cur = cur.join(part);
        platform::private_dir(&cur)?;
        cur = cur.canonicalize()?;
    }
    let app = cur.join("mcpcli");
    platform::private_dir(&app)?;
    Ok(app.canonicalize()?)
}

fn prepare_store_location_writable() -> Result<StoreLocation> {
    if let Some(override_path) = non_empty_env("MCPCLI_CREDENTIALS") {
        let raw = PathBuf::from(override_path);
        let abs = if raw.is_absolute() {
            raw
        } else {
            std::env::current_dir()?.join(raw)
        };
        let parent = abs
            .parent()
            .context("MCPCLI_CREDENTIALS must include a parent directory")?;
        let file_name = abs
            .file_name()
            .context("MCPCLI_CREDENTIALS must name a file")?
            .to_owned();
        if !parent.exists() {
            let mut existing = parent;
            let mut missing: Vec<&OsStr> = Vec::new();
            while !existing.exists() {
                missing.push(
                    existing
                        .file_name()
                        .context("credentials parent path is invalid")?,
                );
                existing = existing
                    .parent()
                    .context("credentials parent path is invalid")?;
            }
            let mut cur = existing.canonicalize()?;
            for part in missing.into_iter().rev() {
                cur = cur.join(part);
                platform::private_dir(&cur)?;
                cur = cur.canonicalize()?;
            }
            return Ok(StoreLocation {
                file_path: cur.join(file_name),
                explicit_override: true,
            });
        }
        let meta = std::fs::symlink_metadata(parent)?;
        if !meta.is_dir() || meta.file_type().is_symlink() {
            bail!("MCPCLI_CREDENTIALS parent must be a real directory");
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if meta.uid() != platform::uid()
                || (meta.mode() & 0o022 != 0 && meta.mode() & 0o1000 == 0)
            {
                bail!("MCPCLI_CREDENTIALS parent directory has unsafe ownership or permissions");
            }
        }
        let canon_parent = parent.canonicalize()?;
        return Ok(StoreLocation {
            file_path: canon_parent.join(file_name),
            explicit_override: true,
        });
    }
    if let Some(rt) = non_empty_env("MCPCLI_RUNTIME_DIR") {
        let root = PathBuf::from(rt);
        if !root.is_absolute() {
            bail!("runtime directory must be absolute");
        }
        let parent = root
            .parent()
            .context("runtime directory must have a parent")?
            .canonicalize()?;
        let resolved = parent.join(
            root.file_name()
                .context("runtime directory must have a name")?,
        );
        platform::private_dir(&resolved)?;
        let canon = resolved.canonicalize()?;
        return Ok(StoreLocation {
            file_path: canon.join("credentials.json"),
            explicit_override: false,
        });
    }
    #[cfg(unix)]
    {
        if let Some(xdg) = non_empty_env("XDG_STATE_HOME")
            && Path::new(&xdg).is_absolute()
        {
            let dir = ensure_state_base_unix(Path::new(&xdg))?;
            return Ok(StoreLocation {
                file_path: dir.join("credentials.json"),
                explicit_override: false,
            });
        }
        if let Some(home) = non_empty_env("HOME")
            && Path::new(&home).is_absolute()
        {
            let dir = ensure_state_base_unix(&PathBuf::from(home).join(".local").join("state"))?;
            return Ok(StoreLocation {
                file_path: dir.join("credentials.json"),
                explicit_override: false,
            });
        }
        let rt = platform::default_runtime()?;
        platform::private_dir(&rt)?;
        Ok(StoreLocation {
            file_path: rt.canonicalize()?.join("credentials.json"),
            explicit_override: false,
        })
    }
    #[cfg(windows)]
    {
        let local = non_empty_env("LOCALAPPDATA").context("LOCALAPPDATA is not set")?;
        let parent = PathBuf::from(local).canonicalize()?;
        let app = parent.join("mcpcli");
        platform::private_dir(&app)?;
        let state_dir = app.canonicalize()?.join("state");
        platform::private_dir(&state_dir)?;
        Ok(StoreLocation {
            file_path: state_dir.canonicalize()?.join("credentials.json"),
            explicit_override: false,
        })
    }
}

fn read_store_at(path: &Path) -> Result<CredentialsStoreFile> {
    if !path.try_exists()? {
        return Ok(CredentialsStoreFile::default());
    }
    let file = platform::secure_file(path)?;
    let bytes = crate::json::read_limited(file, MAX_CREDENTIALS_BYTES)
        .context("cannot read OAuth credentials store")?;
    if bytes.is_empty() {
        return Ok(CredentialsStoreFile::default());
    }
    let parsed: CredentialsStoreFile =
        serde_json::from_slice(&bytes).context("invalid OAuth credentials store JSON")?;
    if parsed.version != 1 {
        bail!(
            "unsupported OAuth credentials store version {}",
            parsed.version
        );
    }
    Ok(parsed)
}

fn read_store_readonly() -> Result<CredentialsStoreFile> {
    match resolve_store_location_readonly()? {
        Some(path) => read_store_at(&path),
        None => Ok(CredentialsStoreFile::default()),
    }
}

async fn mutate_store<R>(mutate: impl FnOnce(&mut CredentialsStoreFile) -> Result<R>) -> Result<R> {
    let loc = prepare_store_location_writable()?;
    let parent = loc
        .file_path
        .parent()
        .context("credentials path has no parent")?;
    if !loc.explicit_override {
        platform::private_dir(parent)?;
    }
    let file_name = loc
        .file_path
        .file_name()
        .and_then(OsStr::to_str)
        .unwrap_or("credentials.json");
    let lock_path = parent.join(format!(".{file_name}.lock"));
    let _guard = Lock::acquire(&lock_path, STORE_LOCK_WAIT).await?;
    let mut store = read_store_at(&loc.file_path)?;
    let out = mutate(&mut store)?;
    let encoded = serde_json::to_vec(&store)?;
    if encoded.len() > MAX_CREDENTIALS_BYTES {
        bail!("OAuth credentials store exceeds the 1 MiB limit");
    }
    let seq = WRITE_SEQ.fetch_add(1, Ordering::Relaxed);
    let tmp_path = parent.join(format!(".{file_name}.tmp-{}-{seq}", std::process::id()));
    let _ = std::fs::remove_file(&tmp_path);
    let mut tmp = platform::new_private_file(&tmp_path)?;
    let write_res = (|| -> Result<()> {
        tmp.write_all(&encoded)?;
        tmp.sync_all()?;
        drop(tmp);
        platform::replace_file_with_retry(&tmp_path, &loc.file_path)?;
        platform::sync_directory(parent)?;
        Ok(())
    })();
    if write_res.is_err() {
        let _ = std::fs::remove_file(&tmp_path);
    }
    write_res?;
    Ok(out)
}

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

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (&x, &y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

fn encode_form(pairs: &[(&str, &str)]) -> String {
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

fn build_oauth_http_client(
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

async fn read_bounded_json(mut response: reqwest::Response) -> Result<Value> {
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
struct TokenEndpointResponse {
    access_token: String,
    #[serde(default)]
    token_type: Option<String>,
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default)]
    expires_in: Option<u64>,
    #[serde(default)]
    scope: Option<String>,
}

fn validate_bearer_token_string(token: &str) -> Result<()> {
    if token.is_empty() {
        bail!("OAuth token endpoint returned an empty access_token");
    }
    http::HeaderValue::from_str(&format!("Bearer {token}"))
        .map_err(|_| anyhow::anyhow!("OAuth token endpoint returned an invalid Bearer token"))?;
    Ok(())
}

async fn credential_lock(resource: &str) -> Result<Lock> {
    let location = prepare_store_location_writable()?;
    let parent = location
        .file_path
        .parent()
        .context("credentials path has no parent")?;
    // Keep one lock across filename aliases and the first store creation.
    let path = parent.join(format!(
        ".oauth-{}.lock",
        crate::config::digest(resource.as_bytes())
    ));
    // A refresh may take 15 seconds before committing its rotated token.
    Lock::acquire(&path, Duration::from_secs(30)).await
}

pub struct HttpAuthManager {
    canonical_resource: String,
    allow_loopback: bool,
    oauth_client: Option<reqwest::Client>,
}

impl HttpAuthManager {
    pub fn for_server_url(url: &str) -> Result<Arc<Self>> {
        let parsed =
            reqwest::Url::parse(url).map_err(|_| anyhow::anyhow!("invalid resource URL"))?;
        let allow_loopback = is_loopback_literal_host(parsed.host_str());
        let canonical = canonical_resource_url(url)?;
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
            canonical_resource: canonical,
            allow_loopback,
            oauth_client,
        }))
    }

    pub async fn prepare_token(&self, client: &reqwest::Client) -> (Option<String>, bool) {
        let Ok(store) = read_store_readonly() else {
            return (None, false);
        };
        let Some(cred) = store.resources.get(&self.canonical_resource) else {
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
        let Ok(_guard) = credential_lock(&self.canonical_resource).await else {
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
        let Some(reloaded) = reloaded_store.resources.get(&self.canonical_resource) else {
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
            .contains_key(&self.canonical_resource)
        {
            return None;
        }
        let _guard = credential_lock(&self.canonical_resource).await.ok()?;
        let store = read_store_readonly().ok()?;
        let cred = store.resources.get(&self.canonical_resource)?;
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

    async fn exchange_refresh_token(
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
        let form = encode_form(&[
            ("grant_type", "refresh_token"),
            ("refresh_token", refresh_token),
            ("client_id", &cred.client_id),
            ("resource", &cred.resource),
        ]);
        let response = timeout(
            Duration::from_secs(15),
            http_client
                .post(endpoint)
                .header(
                    http::header::CONTENT_TYPE,
                    "application/x-www-form-urlencoded",
                )
                .header(http::header::ACCEPT, "application/json")
                .body(form)
                .send(),
        )
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
                let key = self.canonical_resource.clone();
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
        let key = self.canonical_resource.clone();
        mutate_store(|store| {
            store.resources.insert(key, updated);
            Ok(())
        })
        .await?;
        Ok(parsed.access_token)
    }
}

#[derive(Deserialize)]
struct ProtectedResourceMetadata {
    resource: String,
    #[serde(default)]
    authorization_servers: Vec<String>,
}

#[derive(Deserialize)]
struct AuthorizationServerMetadata {
    #[serde(default)]
    issuer: Option<String>,
    authorization_endpoint: String,
    token_endpoint: String,
    #[serde(default)]
    registration_endpoint: Option<String>,
    #[serde(default)]
    revocation_endpoint: Option<String>,
    #[serde(default)]
    code_challenge_methods_supported: Vec<String>,
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

async fn probe_mcp_server_challenge(
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

async fn discover_protected_resource(
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

async fn discover_authorization_server(
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

async fn register_public_client(
    client: &reqwest::Client,
    registration_endpoint: &str,
    redirect_uri: &str,
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
        "client_name": "mcpcli",
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

fn validate_callback_params(
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

fn parse_pasted_callback(
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

async fn accept_loopback_callback(
    listener: &TcpListener,
    expected_state: &str,
    canonical_issuer: &str,
) -> Result<String> {
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
        if parsed.path() != "/callback" {
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
async fn read_stdin_callback(expected_state: &str, canonical_issuer: &str) -> Result<String> {
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

#[cfg(windows)]
async fn read_stdin_callback(expected_state: &str, canonical_issuer: &str) -> Result<String> {
    let mut reader = tokio::io::BufReader::new(tokio::io::stdin());
    let mut line = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        match reader.read(&mut byte).await {
            Ok(0) => {
                if line.is_empty() {
                    return std::future::pending().await;
                }
                break;
            }
            Ok(_) => {
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
            Err(_) => return std::future::pending().await,
        }
    }
    let text = String::from_utf8(line)
        .map_err(|_| Failure::new(2, "arguments", "OAuth callback input on stdin is not UTF-8"))?;
    if text.trim().is_empty() {
        return std::future::pending().await;
    }
    parse_pasted_callback(&text, expected_state, canonical_issuer)
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
    pub client_id: Option<&'a str>,
    pub no_browser: bool,
    pub budget: Duration,
}

async fn ensure_resolved_host_allowed(
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
    let connect_cap = opts.budget.min(Duration::from_secs(15));
    let probe_client = build_oauth_http_client(opts.server_url, true, connect_cap)
        .map_err(|e| Failure::new(4, "mcp_transport", e.to_string()))?;
    let oauth_client = build_oauth_http_client(opts.server_url, allow_loopback, connect_cap)
        .map_err(|e| Failure::new(4, "mcp_transport", e.to_string()))?;

    let probe_budget = deadline
        .saturating_duration_since(Instant::now())
        .min(Duration::from_secs(10));
    if probe_budget.is_zero() {
        return Err(Failure::new(5, "timeout", "OAuth login timed out").into());
    }
    let challenge = probe_mcp_server_challenge(&probe_client, &server_url, probe_budget).await?;

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

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| Failure::new(1, "runtime", format!("cannot bind loopback port: {e}")))?;
    let callback_port = listener
        .local_addr()
        .map_err(|e| Failure::new(1, "runtime", format!("cannot inspect loopback port: {e}")))?
        .port();
    let redirect_uri = format!("http://127.0.0.1:{callback_port}/callback");

    let client_id = if let Some(explicit) = opts.client_id.map(str::trim).filter(|s| !s.is_empty())
    {
        explicit.to_owned()
    } else if let Some(ref reg_endpoint) = asm.registration_endpoint {
        register_public_client(
            &oauth_client,
            reg_endpoint,
            &redirect_uri,
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

    let requested_scopes = if !opts.scopes.is_empty() {
        let mut out = Vec::new();
        for item in opts.scopes {
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
        qp.append_pair("resource", &prm.resource);
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
                res = accept_loopback_callback(&listener, &state, &canonical_issuer) => res,
                res = read_stdin_callback(&state, &canonical_issuer) => res,
            }
        } else {
            accept_loopback_callback(&listener, &state, &canonical_issuer).await
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
    let form = encode_form(&[
        ("grant_type", "authorization_code"),
        ("code", &code),
        ("redirect_uri", &redirect_uri),
        ("client_id", &client_id),
        ("code_verifier", &code_verifier),
        ("resource", &prm.resource),
    ]);
    let token_resp = timeout(
        exchange_rem,
        oauth_client
            .post(token_url)
            .header(
                http::header::CONTENT_TYPE,
                "application/x-www-form-urlencoded",
            )
            .header(http::header::ACCEPT, "application/json")
            .body(form)
            .send(),
    )
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
        resource: prm.resource,
        issuer: canonical_issuer.clone(),
        token_endpoint: asm.token_endpoint,
        revocation_endpoint: asm.revocation_endpoint,
        client_id,
        access_token: parsed_tok.access_token,
        refresh_token: parsed_tok.refresh_token,
        expires_at,
        scopes: granted_scopes.clone(),
    };
    let _guard = credential_lock(&canonical_server_resource)
        .await
        .map_err(|e| Failure::new(1, "runtime", format!("{e:#}")))?;
    let key = canonical_server_resource.clone();
    mutate_store(|store| {
        store.resources.insert(key, stored);
        Ok(())
    })
    .await
    .map_err(|e| Failure::new(1, "runtime", format!("{e:#}")))?;

    let summary_json = json!({
        "server": opts.server_name,
        "authenticated": true,
        "issuer": canonical_issuer,
        "expiresAt": expires_at,
        "scopes": granted_scopes,
    });
    let summary_text = format!(
        "authenticated {} (issuer: {})",
        opts.server_name, canonical_issuer
    );
    Ok((summary_json, summary_text))
}

fn server_status_entry(
    name: &str,
    url: &str,
    store: &CredentialsStoreFile,
    now: u64,
) -> Result<(Value, String)> {
    let canonical = canonical_resource_url(url)?;
    if let Some(cred) = store.resources.get(&canonical) {
        let valid = cred.expires_at.is_none_or(|exp| exp > now) || cred.refresh_token.is_some();
        let entry = json!({
            "server": name,
            "authenticated": valid,
            "expiresAt": cred.expires_at,
            "scopes": cred.scopes,
            "issuer": cred.issuer,
        });
        let exp_text = cred
            .expires_at
            .map_or_else(|| "none".to_owned(), |v| v.to_string());
        let scopes_text = if cred.scopes.is_empty() {
            "none".to_owned()
        } else {
            cred.scopes.join(",")
        };
        let state_label = if valid { "authenticated" } else { "expired" };
        let line = format!(
            "{name}\t{state_label}\tissuer={}\texpiresAt={exp_text}\tscopes={scopes_text}",
            cred.issuer
        );
        Ok((entry, line))
    } else {
        let entry = json!({
            "server": name,
            "authenticated": false,
            "expiresAt": Value::Null,
            "scopes": Vec::<String>::new(),
            "issuer": Value::Null,
        });
        let line = format!("{name}\tunauthenticated");
        Ok((entry, line))
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
        let ServerConfig::Http { url, .. } = cfg else {
            return Err(Failure::new(
                2,
                "invalid_transport",
                format!("server {name:?} uses stdio transport; OAuth applies only to http servers"),
            )
            .into());
        };
        return server_status_entry(name, url, &store, now)
            .map_err(|e| Failure::new(2, "configuration", format!("{e:#}")).into());
    }
    let mut entries = Vec::new();
    let mut lines = Vec::new();
    for (name, cfg) in servers {
        if let ServerConfig::Http { url, .. } = cfg {
            let (entry, line) = server_status_entry(name, url, &store, now)
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
    budget: Duration,
) -> Result<(Value, String)> {
    let deadline = Instant::now() + budget;
    let parsed_server = reqwest::Url::parse(server_url)
        .map_err(|_| Failure::new(2, "configuration", "invalid MCP server URL"))?;
    let server_loopback = is_loopback_literal_host(parsed_server.host_str());
    let canonical = canonical_resource_url(server_url)
        .map_err(|e| Failure::new(2, "configuration", e.to_string()))?;
    let (_guard, existing) = timeout_at(deadline, async {
        if resolve_store_location_readonly()?.is_none() {
            return Ok::<_, anyhow::Error>((None, None));
        }
        let guard = credential_lock(&canonical).await?;
        let existing = read_store_readonly()?.resources.get(&canonical).cloned();
        if existing.is_some() {
            mutate_store(|store| {
                store.resources.remove(&canonical);
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
                    let form = encode_form(&[
                        ("token", token),
                        ("token_type_hint", hint),
                        ("client_id", &cred.client_id),
                    ]);
                    let _ = timeout(
                        Duration::from_secs(5),
                        client
                            .post(rev_url.clone())
                            .header(
                                http::header::CONTENT_TYPE,
                                "application/x-www-form-urlencoded",
                            )
                            .body(form)
                            .send(),
                    )
                    .await;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::OnceLock;

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
            validate_oauth_endpoint_url("https://auth.example.com/oauth/token", "test", false)
                .is_ok()
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
                    .write_all(b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    .await
                    .unwrap();
            }
        });
        let login = tokio::spawn(async move {
            super::login(LoginOptions {
                server_name: "slow-metadata",
                server_url: &resource,
                scopes: &[],
                client_id: None,
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
            let manager = HttpAuthManager::for_server_url("http://127.0.0.1/mcp").unwrap();
            let credential = StoredCredential {
                resource: "http://127.0.0.1/mcp".into(),
                issuer: "http://127.0.0.1".into(),
                token_endpoint,
                revocation_endpoint: None,
                client_id: "client".into(),
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

        let mgr = HttpAuthManager::for_server_url("http://127.0.0.1:7777/mcp").unwrap();
        let client =
            build_oauth_http_client("http://127.0.0.1:7777/mcp", true, Duration::from_secs(5))
                .unwrap();
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
            logout("server", resource, Duration::from_secs(1)),
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
            logout("server", resource, Duration::from_millis(100)),
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
            logout("server", resource, Duration::from_secs(1)),
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
            let body = r#"{"access_token":"fresh-access","refresh_token":"fresh-refresh","expires_in":3600}"#;
            socket.write_all(format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            ).as_bytes()).await.unwrap();
        });
        let refresh = tokio::spawn(async move {
            HttpAuthManager::for_server_url(resource)
                .unwrap()
                .prepare_token(&reqwest::Client::new())
                .await
        });
        timeout(Duration::from_secs(5), request_received)
            .await
            .unwrap()
            .unwrap();
        let mut logout = tokio::spawn(super::logout("server", resource, Duration::from_secs(5)));
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

        let mgr = HttpAuthManager::for_server_url("https://remote.example/mcp").unwrap();
        let fallback = reqwest::Client::new();
        assert_eq!(mgr.prepare_token(&fallback).await, (None, true));
        assert_eq!(
            mgr.refresh_after_401(&fallback, Some("stale_at")).await,
            None
        );
        let (logout_json, _) = logout(
            "remote",
            "https://remote.example/mcp",
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
            },
        )]);
        assert_eq!(
            status(&servers, Some("srv")).unwrap().0["authenticated"],
            true
        );

        let mgr = HttpAuthManager::for_server_url(resource_url).unwrap();
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
        let _env_guard = test_env_lock().lock().await;
        let home_dir = tempfile::tempdir_in("/tmp").unwrap();
        let canon_home = home_dir.path().canonicalize().unwrap();

        let prev_creds = std::env::var_os("MCPCLI_CREDENTIALS");
        let prev_rt = std::env::var_os("MCPCLI_RUNTIME_DIR");
        let prev_xdg = std::env::var_os("XDG_STATE_HOME");
        let prev_home = std::env::var_os("HOME");

        // SAFETY: serialized across tests in this module via `test_env_lock`.
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
        // SAFETY: serialized across tests in this module via `test_env_lock`.
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

        // SAFETY: serialized across tests in this module via `test_env_lock`.
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
}
