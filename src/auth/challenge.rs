use crate::{discovery::safe_text, oauth_url::is_loopback_literal_host};
use anyhow::{Result, bail};
use serde_json::{Map, Value};
use std::time::{SystemTime, UNIX_EPOCH};

const MAX_WWW_AUTH_HEADER_BYTES: usize = 8 * 1024;
const MAX_AUTH_PARAM_BYTES: usize = 2 * 1024;

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

pub(super) fn split_scopes(raw: &str) -> Vec<String> {
    let mut out = Vec::new();
    for item in raw.split_whitespace() {
        if !item.is_empty() && !out.iter().any(|s| s == item) {
            out.push(item.to_owned());
        }
    }
    out
}

pub(super) fn now_unix_secs() -> u64 {
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

/// Map a canonical resource URL and optional account name to a `credentials.json` v1 key.
pub fn credential_storage_key(canonical_resource_url: &str, account: Option<&str>) -> String {
    match account {
        Some(acct) => format!("{canonical_resource_url}#account={acct}"),
        None => canonical_resource_url.to_owned(),
    }
}

pub(super) fn validate_mcp_server_url(raw: &str) -> Result<(reqwest::Url, bool)> {
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

pub(super) fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (&x, &y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}
