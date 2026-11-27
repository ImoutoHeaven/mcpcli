use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

pub const CONFIG_LIMIT: usize = 1 << 20;
pub const MAX_TIMEOUT_SECS: u64 = 3600;
const MAX_CLIENT_NAME_BYTES: usize = 128;

fn default_enabled() -> bool {
    true
}

#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OAuthConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_secret: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub callback_port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub callback_path: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scopes: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_server_metadata_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account: Option<String>,
}

impl std::fmt::Debug for OAuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuthConfig")
            .field("client_id", &self.client_id)
            .field(
                "client_secret",
                &self.client_secret.as_ref().map(|_| "[REDACTED]"),
            )
            .field("client_name", &self.client_name)
            .field("callback_port", &self.callback_port)
            .field("callback_path", &self.callback_path)
            .field("scopes", &self.scopes)
            .field("auth_server_metadata_url", &self.auth_server_metadata_url)
            .field("account", &self.account)
            .finish()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "transport", rename_all = "lowercase", deny_unknown_fields)]
pub enum ServerConfig {
    Stdio {
        command: String,
        #[serde(default)]
        args: Vec<String>,
        #[serde(default)]
        env: BTreeMap<String, String>,
        #[serde(default)]
        cwd: Option<PathBuf>,
        #[serde(default)]
        description: Option<String>,
        #[serde(default = "default_enabled")]
        enabled: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timeout: Option<u64>,
    },
    Http {
        url: String,
        #[serde(default)]
        headers: BTreeMap<String, String>,
        #[serde(default)]
        description: Option<String>,
        #[serde(default = "default_enabled")]
        enabled: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timeout: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        oauth: Option<OAuthConfig>,
    },
}

impl ServerConfig {
    pub fn description(&self) -> Option<&str> {
        match self {
            Self::Stdio { description, .. } | Self::Http { description, .. } => {
                description.as_deref()
            }
        }
    }
    pub fn transport(&self) -> &'static str {
        match self {
            Self::Stdio { .. } => "stdio",
            Self::Http { .. } => "http",
        }
    }
    pub fn enabled(&self) -> bool {
        match self {
            Self::Stdio { enabled, .. } | Self::Http { enabled, .. } => *enabled,
        }
    }
    pub fn timeout(&self) -> Option<u64> {
        match self {
            Self::Stdio { timeout, .. } | Self::Http { timeout, .. } => *timeout,
        }
    }
    pub fn oauth(&self) -> Option<&OAuthConfig> {
        match self {
            Self::Stdio { .. } => None,
            Self::Http { oauth, .. } => oauth.as_ref(),
        }
    }
}

fn version_one() -> u32 {
    1
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default = "version_one")]
    pub version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agents: Option<PathBuf>,
    #[serde(default, alias = "mcpServers")]
    pub servers: BTreeMap<String, ServerConfig>,
    #[serde(default, alias = "skillDirs")]
    pub skills: Vec<PathBuf>,
}

#[derive(Clone)]
pub struct LoadedConfig {
    pub path: PathBuf,
    pub config: Config,
    pub digest: String,
}

pub fn valid_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

pub fn is_reserved_header(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    matches!(
        lower.as_str(),
        "host"
            | "content-length"
            | "content-type"
            | "accept"
            | "connection"
            | "transfer-encoding"
            | "upgrade"
            | "te"
            | "trailer"
            | "last-event-id"
    ) || lower.starts_with("mcp-")
        || lower.starts_with("mcp_")
}

fn json_kind(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

fn safe_key(key: &str) -> String {
    if valid_name(key) {
        format!("{key:?}")
    } else {
        "<redacted-key>".into()
    }
}

/// Diagnose configuration shape errors using field paths and value kinds only,
/// never echoing scalar string or number contents.
fn diagnose_config_shape(value: &serde_json::Value) -> String {
    let Some(root) = value.as_object() else {
        return format!(
            "invalid configuration shape: root must be a JSON object, got {}",
            json_kind(value)
        );
    };
    if root.contains_key("servers") && root.contains_key("mcpServers") {
        return "invalid configuration shape: specify only one of \"servers\" or \"mcpServers\""
            .into();
    }
    if root.contains_key("skills") && root.contains_key("skillDirs") {
        return "invalid configuration shape: specify only one of \"skills\" or \"skillDirs\""
            .into();
    }
    for key in root.keys() {
        if !matches!(
            key.as_str(),
            "version" | "agents" | "servers" | "mcpServers" | "skills" | "skillDirs"
        ) {
            return format!(
                "invalid configuration shape: unknown top-level field {}",
                safe_key(key)
            );
        }
    }
    if let Some(version) = root.get("version")
        && version
            .as_u64()
            .and_then(|v| u32::try_from(v).ok())
            .is_none()
    {
        return format!(
            "invalid configuration shape at version: expected unsigned integer, got {}",
            json_kind(version)
        );
    }
    if let Some(agents) = root.get("agents")
        && !agents.is_string()
        && !agents.is_null()
    {
        return format!(
            "invalid configuration shape at agents: expected string, got {}",
            json_kind(agents)
        );
    }
    for skills_field in ["skills", "skillDirs"] {
        if let Some(skills) = root.get(skills_field) {
            let Some(items) = skills.as_array() else {
                return format!(
                    "invalid configuration shape at {skills_field}: expected array of strings, got {}",
                    json_kind(skills)
                );
            };
            for (index, item) in items.iter().enumerate() {
                if !item.is_string() {
                    return format!(
                        "invalid configuration shape at {skills_field}[{index}]: expected string, got {}",
                        json_kind(item)
                    );
                }
            }
        }
    }
    for servers_field in ["servers", "mcpServers"] {
        if let Some(servers) = root.get(servers_field) {
            let Some(entries) = servers.as_object() else {
                return format!(
                    "invalid configuration shape at {servers_field}: expected object, got {}",
                    json_kind(servers)
                );
            };
            for (name, server) in entries {
                let label = safe_key(name);
                let Some(fields) = server.as_object() else {
                    return format!(
                        "invalid configuration shape at {servers_field}.{label}: expected object, got {}",
                        json_kind(server)
                    );
                };
                let Some(transport) = fields.get("transport") else {
                    return format!(
                        "invalid configuration shape at {servers_field}.{label}: missing required field \"transport\""
                    );
                };
                match transport.as_str() {
                    Some("stdio") => {
                        if fields.contains_key("oauth") {
                            return format!(
                                "invalid configuration shape at {servers_field}.{label}.oauth: \"oauth\" is only supported on http servers"
                            );
                        }
                        for key in fields.keys() {
                            if !matches!(
                                key.as_str(),
                                "transport"
                                    | "command"
                                    | "args"
                                    | "env"
                                    | "cwd"
                                    | "description"
                                    | "enabled"
                                    | "timeout"
                            ) {
                                return format!(
                                    "invalid configuration shape at {servers_field}.{label}: unknown stdio field {}",
                                    safe_key(key)
                                );
                            }
                        }
                        match fields.get("command") {
                            None => {
                                return format!(
                                    "invalid configuration shape at {servers_field}.{label}: missing required field \"command\""
                                );
                            }
                            Some(cmd) if !cmd.is_string() => {
                                return format!(
                                    "invalid configuration shape at {servers_field}.{label}.command: expected string, got {}",
                                    json_kind(cmd)
                                );
                            }
                            _ => {}
                        }
                        if let Some(args) = fields.get("args") {
                            let Some(items) = args.as_array() else {
                                return format!(
                                    "invalid configuration shape at {servers_field}.{label}.args: expected array of strings, got {}",
                                    json_kind(args)
                                );
                            };
                            for (index, item) in items.iter().enumerate() {
                                if !item.is_string() {
                                    return format!(
                                        "invalid configuration shape at {servers_field}.{label}.args[{index}]: expected string, got {}",
                                        json_kind(item)
                                    );
                                }
                            }
                        }
                        if let Some(env) = fields.get("env") {
                            let Some(map) = env.as_object() else {
                                return format!(
                                    "invalid configuration shape at {servers_field}.{label}.env: expected object of string values, got {}",
                                    json_kind(env)
                                );
                            };
                            for (key, val) in map {
                                if !val.is_string() {
                                    return format!(
                                        "invalid configuration shape at {servers_field}.{label}.env.{}: expected string, got {}",
                                        safe_key(key),
                                        json_kind(val)
                                    );
                                }
                            }
                        }
                        for optional in ["cwd", "description"] {
                            if let Some(val) = fields.get(optional)
                                && !val.is_string()
                                && !val.is_null()
                            {
                                return format!(
                                    "invalid configuration shape at {servers_field}.{label}.{optional}: expected string, got {}",
                                    json_kind(val)
                                );
                            }
                        }
                        if let Some(enabled) = fields.get("enabled")
                            && !enabled.is_boolean()
                        {
                            return format!(
                                "invalid configuration shape at {servers_field}.{label}.enabled: expected boolean, got {}",
                                json_kind(enabled)
                            );
                        }
                        if let Some(timeout) = fields.get("timeout")
                            && !timeout.is_null()
                            && timeout.as_u64().is_none()
                        {
                            return format!(
                                "invalid configuration shape at {servers_field}.{label}.timeout: expected unsigned integer, got {}",
                                json_kind(timeout)
                            );
                        }
                    }
                    Some("http") => {
                        for key in fields.keys() {
                            if !matches!(
                                key.as_str(),
                                "transport"
                                    | "url"
                                    | "headers"
                                    | "description"
                                    | "enabled"
                                    | "timeout"
                                    | "oauth"
                            ) {
                                return format!(
                                    "invalid configuration shape at {servers_field}.{label}: unknown http field {}",
                                    safe_key(key)
                                );
                            }
                        }
                        match fields.get("url") {
                            None => {
                                return format!(
                                    "invalid configuration shape at {servers_field}.{label}: missing required field \"url\""
                                );
                            }
                            Some(url) if !url.is_string() => {
                                return format!(
                                    "invalid configuration shape at {servers_field}.{label}.url: expected string, got {}",
                                    json_kind(url)
                                );
                            }
                            _ => {}
                        }
                        if let Some(headers) = fields.get("headers") {
                            let Some(map) = headers.as_object() else {
                                return format!(
                                    "invalid configuration shape at {servers_field}.{label}.headers: expected object of string values, got {}",
                                    json_kind(headers)
                                );
                            };
                            for (key, val) in map {
                                if !val.is_string() {
                                    return format!(
                                        "invalid configuration shape at {servers_field}.{label}.headers.{}: expected string, got {}",
                                        safe_key(key),
                                        json_kind(val)
                                    );
                                }
                            }
                        }
                        if let Some(desc) = fields.get("description")
                            && !desc.is_string()
                            && !desc.is_null()
                        {
                            return format!(
                                "invalid configuration shape at {servers_field}.{label}.description: expected string, got {}",
                                json_kind(desc)
                            );
                        }
                        if let Some(enabled) = fields.get("enabled")
                            && !enabled.is_boolean()
                        {
                            return format!(
                                "invalid configuration shape at {servers_field}.{label}.enabled: expected boolean, got {}",
                                json_kind(enabled)
                            );
                        }
                        if let Some(timeout) = fields.get("timeout")
                            && !timeout.is_null()
                            && timeout.as_u64().is_none()
                        {
                            return format!(
                                "invalid configuration shape at {servers_field}.{label}.timeout: expected unsigned integer, got {}",
                                json_kind(timeout)
                            );
                        }
                        if let Some(oauth) = fields.get("oauth")
                            && !oauth.is_null()
                        {
                            let Some(oauth_map) = oauth.as_object() else {
                                return format!(
                                    "invalid configuration shape at {servers_field}.{label}.oauth: expected object, got {}",
                                    json_kind(oauth)
                                );
                            };
                            for key in oauth_map.keys() {
                                if !matches!(
                                    key.as_str(),
                                    "client_id"
                                        | "client_secret"
                                        | "client_name"
                                        | "callback_port"
                                        | "callback_path"
                                        | "scopes"
                                        | "auth_server_metadata_url"
                                        | "account"
                                ) {
                                    return format!(
                                        "invalid configuration shape at {servers_field}.{label}.oauth: unknown oauth field {}",
                                        safe_key(key)
                                    );
                                }
                            }
                            for field in [
                                "client_id",
                                "client_secret",
                                "client_name",
                                "callback_path",
                                "auth_server_metadata_url",
                                "account",
                            ] {
                                if let Some(val) = oauth_map.get(field)
                                    && !val.is_string()
                                    && !val.is_null()
                                {
                                    return format!(
                                        "invalid configuration shape at {servers_field}.{label}.oauth.{field}: expected string, got {}",
                                        json_kind(val)
                                    );
                                }
                            }
                            if let Some(port) = oauth_map.get("callback_port")
                                && !port.is_null()
                                && port.as_u64().and_then(|v| u16::try_from(v).ok()).is_none()
                            {
                                return format!(
                                    "invalid configuration shape at {servers_field}.{label}.oauth.callback_port: expected unsigned integer (1..=65535), got {}",
                                    json_kind(port)
                                );
                            }
                            if let Some(scopes) = oauth_map.get("scopes") {
                                let Some(items) = scopes.as_array() else {
                                    return format!(
                                        "invalid configuration shape at {servers_field}.{label}.oauth.scopes: expected array of strings, got {}",
                                        json_kind(scopes)
                                    );
                                };
                                for (index, item) in items.iter().enumerate() {
                                    if !item.is_string() {
                                        return format!(
                                            "invalid configuration shape at {servers_field}.{label}.oauth.scopes[{index}]: expected string, got {}",
                                            json_kind(item)
                                        );
                                    }
                                }
                            }
                        }
                    }
                    Some(_) => {
                        return format!(
                            "invalid configuration shape at {servers_field}.{label}.transport: expected \"stdio\" or \"http\""
                        );
                    }
                    None => {
                        return format!(
                            "invalid configuration shape at {servers_field}.{label}.transport: expected string (\"stdio\" or \"http\"), got {}",
                            json_kind(transport)
                        );
                    }
                }
            }
        }
    }
    "invalid configuration shape; check version, agents, servers, skills and transport fields"
        .into()
}

/// Management commands can still locate a daemon after its config is removed.
pub fn locate(path: &Path) -> Result<PathBuf> {
    if path.exists() {
        return path
            .canonicalize()
            .context("cannot resolve configuration path");
    }
    let absolute = if path.is_absolute() {
        path.to_owned()
    } else {
        std::env::current_dir()?.join(path)
    };
    let parent = absolute
        .parent()
        .context("configuration needs a file name")?;
    let file = absolute
        .file_name()
        .context("configuration needs a file name")?;
    Ok(parent
        .canonicalize()
        .context("configuration parent does not exist")?
        .join(file))
}

fn validate_env_name(name: &str) -> Result<()> {
    if name.is_empty() || !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
        bail!("invalid environment variable reference");
    }
    Ok(())
}

fn expand_env(value: &str) -> Result<String> {
    if let Some(name) = value
        .strip_prefix(concat!("$", "{"))
        .and_then(|v| v.strip_suffix('}'))
    {
        validate_env_name(name)?;
        std::env::var(name)
            .with_context(|| format!("environment variable {name} is missing or not UTF-8"))
    } else {
        Ok(value.to_owned())
    }
}

fn expand_header_template(value: &str) -> Result<String> {
    let open = concat!("$", "{");
    let Some(first) = value.find(open) else {
        return Ok(value.to_owned());
    };
    let mut out = String::with_capacity(value.len());
    let mut rest = value;
    let mut next_open = Some(first);
    while let Some(pos) = next_open {
        out.push_str(&rest[..pos]);
        let after_open = &rest[pos + open.len()..];
        let Some(close) = after_open.find('}') else {
            bail!("unclosed environment variable reference");
        };
        let name = &after_open[..close];
        validate_env_name(name)?;
        let resolved = std::env::var(name)
            .with_context(|| format!("environment variable {name} is missing or not UTF-8"))?;
        out.push_str(&resolved);
        rest = &after_open[close + 1..];
        next_open = rest.find(open);
    }
    out.push_str(rest);
    Ok(out)
}

fn resolve_home_dir() -> Result<PathBuf> {
    #[cfg(unix)]
    const VAR: &str = "HOME";
    #[cfg(windows)]
    const VAR: &str = "USERPROFILE";

    let raw = std::env::var_os(VAR)
        .with_context(|| format!("{VAR} must be set to expand '~' in configuration paths"))?;
    let text = raw
        .into_string()
        .map_err(|_| anyhow::anyhow!("{VAR} must be valid UTF-8 to expand '~'"))?;
    if text.is_empty() || text.contains('\0') {
        bail!("{VAR} must be nonempty without NUL to expand '~'");
    }
    let path = PathBuf::from(&text);
    if !path.is_absolute() {
        bail!("{VAR} must be an absolute path to expand '~'");
    }
    Ok(path)
}

fn expand_tilde_str(raw: &str) -> Result<Option<PathBuf>> {
    if raw == "~" {
        return Ok(Some(resolve_home_dir()?));
    }
    #[cfg(windows)]
    let rest = raw.strip_prefix("~/").or_else(|| raw.strip_prefix("~\\"));
    #[cfg(not(windows))]
    let rest = raw.strip_prefix("~/");
    let Some(rest) = rest else {
        return Ok(None);
    };
    #[cfg(windows)]
    let rest = rest.trim_start_matches(['/', '\\']);
    #[cfg(not(windows))]
    let rest = rest.trim_start_matches('/');
    let home = resolve_home_dir()?;
    if rest.is_empty() {
        return Ok(Some(home));
    }
    #[cfg(windows)]
    if Path::new(rest)
        .components()
        .any(|c| matches!(c, std::path::Component::Prefix(_)))
    {
        bail!("tilde-expanded path must not contain a Windows path prefix");
    }
    Ok(Some(home.join(rest)))
}

fn expand_tilde_path(path: &Path) -> Result<PathBuf> {
    let Some(raw) = path.to_str() else {
        return Ok(path.to_owned());
    };
    Ok(match expand_tilde_str(raw)? {
        Some(expanded) => expanded,
        None => path.to_owned(),
    })
}

pub fn validate_client_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > MAX_CLIENT_NAME_BYTES
        || name.trim().is_empty()
        || !name.bytes().all(|b| (0x20..=0x7e).contains(&b))
    {
        bail!("client_name must be 1-{MAX_CLIENT_NAME_BYTES} printable ASCII characters");
    }
    Ok(())
}

fn validate_oauth_config(
    name: &str,
    oauth: &mut OAuthConfig,
    allow_loopback: bool,
    expand_secrets: bool,
) -> Result<()> {
    if expand_secrets {
        if let Some(ref mut cid) = oauth.client_id {
            *cid = expand_header_template(cid)
                .with_context(|| format!("server {name}: invalid oauth.client_id"))?;
        }
        if let Some(ref mut secret) = oauth.client_secret {
            *secret = expand_header_template(secret)
                .with_context(|| format!("server {name}: invalid oauth.client_secret"))?;
        }
    }
    if let Some(ref cid) = oauth.client_id
        && (cid.trim().is_empty() || cid.contains('\0'))
    {
        bail!("server {name}: oauth.client_id must be nonempty without NUL");
    }
    if let Some(ref secret) = oauth.client_secret {
        if oauth.client_id.is_none() {
            bail!("server {name}: oauth.client_secret requires oauth.client_id");
        }
        if secret.is_empty() || secret.contains('\0') {
            bail!("server {name}: oauth.client_secret must be nonempty without NUL");
        }
    }
    if let Some(ref cname) = oauth.client_name {
        if oauth.client_id.is_some() {
            bail!("server {name}: oauth.client_name cannot be combined with oauth.client_id");
        }
        validate_client_name(cname).with_context(|| format!("server {name}: invalid oauth"))?;
    }
    if let Some(port) = oauth.callback_port
        && port == 0
    {
        bail!("server {name}: oauth.callback_port must be between 1 and 65535");
    }
    if let Some(ref path) = oauth.callback_path {
        crate::oauth_url::validate_callback_path(path)
            .with_context(|| format!("server {name}: invalid oauth.callback_path"))?;
    }
    for scope in &oauth.scopes {
        if scope.is_empty() || scope.chars().any(char::is_whitespace) || scope.contains('\0') {
            bail!("server {name}: oauth.scopes entries must be nonempty without whitespace");
        }
    }
    if let Some(ref meta_url) = oauth.auth_server_metadata_url {
        crate::oauth_url::validate_oauth_endpoint_url(
            meta_url,
            "auth_server_metadata_url",
            allow_loopback,
        )
        .with_context(|| format!("server {name}: invalid oauth.auth_server_metadata_url"))?;
    }
    if let Some(ref acct) = oauth.account
        && !valid_name(acct)
    {
        bail!("server {name}: oauth.account must be 1-64 ASCII letters, digits, '_' or '-'");
    }
    Ok(())
}

fn from_config_directory(parent: &Path, path: &Path) -> Result<PathBuf> {
    #[cfg(windows)]
    if !path.is_absolute()
        && (path.has_root()
            || matches!(
                path.components().next(),
                Some(std::path::Component::Prefix(_))
            ))
    {
        bail!("configuration paths must be absolute or relative, not drive-relative");
    }
    Ok(if path.is_absolute() {
        path.to_owned()
    } else {
        let mut resolved = parent.to_owned();
        for component in path.components() {
            match component {
                std::path::Component::CurDir => {}
                #[cfg(windows)]
                std::path::Component::ParentDir => {
                    let _ = resolved.pop();
                }
                other => resolved.push(other.as_os_str()),
            }
        }
        resolved
    })
}

#[cfg(windows)]
fn reject_duplicate_windows_env<'a>(keys: impl Iterator<Item = &'a String>) -> Result<()> {
    use windows_sys::Win32::Globalization::{CSTR_EQUAL, CompareStringOrdinal};
    let mut seen: Vec<Vec<u16>> = Vec::new();
    for key in keys {
        let key: Vec<u16> = key.encode_utf16().collect();
        for previous in &seen {
            // SAFETY: both UTF-16 buffers have explicit lengths and stay live for the call.
            let comparison = unsafe {
                CompareStringOrdinal(
                    previous.as_ptr(),
                    previous.len() as i32,
                    key.as_ptr(),
                    key.len() as i32,
                    1,
                )
            };
            if comparison == 0 {
                bail!("cannot compare Windows environment variable names");
            }
            if comparison == CSTR_EQUAL {
                bail!("duplicate case-insensitive Windows environment variable");
            }
        }
        seen.push(key);
    }
    Ok(())
}

impl LoadedConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let path = locate(path)?;
        let bytes = crate::json::read_file(&path, CONFIG_LIMIT)?;
        let value = crate::json::parse(&bytes)?;
        let diagnostic = diagnose_config_shape(&value);
        let mut config: Config =
            serde_json::from_value(value).map_err(|_| anyhow::anyhow!(diagnostic))?;
        if config.version != 1 {
            bail!("unsupported configuration version (expected 1)");
        }
        if config.servers.len() > 64 || config.skills.len() > 64 {
            bail!("configuration permits at most 64 servers and 64 skill roots");
        }
        let parent = path.parent().context("configuration has no parent")?;
        for (name, server) in &mut config.servers {
            if !valid_name(name) {
                bail!("server names must be 1-64 ASCII letters, digits, '_' or '-'");
            }
            if let Some(timeout) = server.timeout()
                && !(1..=MAX_TIMEOUT_SECS).contains(&timeout)
            {
                bail!("server {name}: timeout must be between 1 and {MAX_TIMEOUT_SECS} seconds");
            }
            match server {
                ServerConfig::Stdio {
                    command,
                    args,
                    env,
                    cwd,
                    enabled,
                    ..
                } => {
                    if command.is_empty() || command.contains('\0') {
                        bail!("server {name}: command must be nonempty without NUL");
                    }
                    if args.len() > 1024 || args.iter().any(|s| s.contains('\0')) {
                        bail!("server {name}: invalid or too many arguments");
                    }
                    if env.len() > 256 {
                        bail!("server {name}: too many environment overrides");
                    }
                    #[cfg(windows)]
                    reject_duplicate_windows_env(env.keys())?;
                    for (key, value) in env.iter() {
                        if key.is_empty() || key.contains(['=', '\0']) {
                            bail!("server {name}: invalid environment variable name");
                        }
                        if value.contains('\0') {
                            bail!("server {name}: environment value contains NUL");
                        }
                    }
                    if !*enabled {
                        continue;
                    }
                    if let Some(expanded) = expand_tilde_str(command)? {
                        *command = expanded
                            .to_str()
                            .context("command path is not UTF-8")?
                            .to_owned();
                    }
                    // Explicit command paths use the configuration directory on every platform.
                    if command.contains('/') || (cfg!(windows) && command.contains(['\\', ':'])) {
                        *command = from_config_directory(parent, Path::new(command))?
                            .to_str()
                            .context("command path is not UTF-8")?
                            .to_owned();
                    }
                    let expanded_cwd = match cwd.as_deref() {
                        Some(raw_cwd) => expand_tilde_path(raw_cwd)?,
                        None => PathBuf::from("."),
                    };
                    let resolved = from_config_directory(parent, &expanded_cwd)?;
                    let resolved = resolved
                        .canonicalize()
                        .with_context(|| format!("server {name}: cwd does not exist"))?;
                    if !resolved.is_dir() {
                        bail!("server {name}: cwd is not a directory");
                    }
                    *cwd = Some(resolved);
                    for value in env.values_mut() {
                        *value = expand_env(value)?;
                        if value.contains('\0') {
                            bail!("server {name}: environment value contains NUL");
                        }
                    }
                }
                ServerConfig::Http {
                    url,
                    headers,
                    enabled,
                    oauth,
                    ..
                } => {
                    let parsed = reqwest::Url::parse(url)
                        .map_err(|_| anyhow::anyhow!("server {name}: invalid HTTP URL"))?;
                    if !matches!(parsed.scheme(), "http" | "https")
                        || parsed.host_str().is_none()
                        || !parsed.username().is_empty()
                        || parsed.password().is_some()
                        || parsed.fragment().is_some()
                    {
                        bail!(
                            "server {name}: URL must be http(s), without credentials or fragment"
                        );
                    }
                    if headers.len() > 64 {
                        bail!("server {name}: too many HTTP headers");
                    }
                    let allow_loopback =
                        crate::oauth_url::is_loopback_literal_host(parsed.host_str());
                    if *enabled {
                        let mut normalized = BTreeMap::new();
                        for (key, value) in headers.iter() {
                            let key =
                                http::HeaderName::from_bytes(key.as_bytes()).map_err(|_| {
                                    anyhow::anyhow!("server {name}: invalid HTTP header name")
                                })?;
                            let lower = key.as_str();
                            if is_reserved_header(lower) {
                                bail!(
                                    "server {name}: a header reserved by the transport cannot be overridden"
                                );
                            }
                            let resolved = expand_header_template(value)?;
                            http::HeaderValue::from_str(&resolved).map_err(|_| {
                                anyhow::anyhow!("server {name}: invalid HTTP header value")
                            })?;
                            if normalized.insert(lower.to_owned(), resolved).is_some() {
                                bail!("server {name}: duplicate case-insensitive HTTP header");
                            }
                        }
                        *headers = normalized;
                    } else {
                        let mut seen = std::collections::BTreeSet::new();
                        for key in headers.keys() {
                            let key =
                                http::HeaderName::from_bytes(key.as_bytes()).map_err(|_| {
                                    anyhow::anyhow!("server {name}: invalid HTTP header name")
                                })?;
                            let lower = key.as_str();
                            if is_reserved_header(lower) {
                                bail!(
                                    "server {name}: a header reserved by the transport cannot be overridden"
                                );
                            }
                            if !seen.insert(lower.to_owned()) {
                                bail!("server {name}: duplicate case-insensitive HTTP header");
                            }
                        }
                    }
                    if let Some(oauth_cfg) = oauth.as_mut() {
                        validate_oauth_config(name, oauth_cfg, allow_loopback, *enabled)?;
                    }
                }
            }
        }
        if let Some(agents) = &mut config.agents {
            *agents = expand_tilde_path(agents)?;
            crate::agents::validate_path(agents)?;
            *agents = from_config_directory(parent, agents)?;
        }
        for root in &mut config.skills {
            if root.as_os_str().is_empty() {
                bail!("skill root path must not be empty");
            }
            *root = expand_tilde_path(root)?;
            *root = from_config_directory(parent, root)?;
        }
        let digest = crate::platform::digest(&serde_json::to_vec(&config)?);
        Ok(Self {
            path,
            config,
            digest,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn resolves_paths_and_rejects_ambiguity() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mcpcli.json");
        std::fs::write(
            &path,
            r#"{"servers":{"x":{"transport":"stdio","command":"bin/server"}},"skills":["skills"]}"#,
        )
        .unwrap();
        let loaded = LoadedConfig::load(&path).unwrap();
        let ServerConfig::Stdio { command, cwd, .. } = &loaded.config.servers["x"] else {
            panic!()
        };
        let canonical = dir.path().canonicalize().unwrap();
        assert_eq!(command, canonical.join("bin/server").to_str().unwrap());
        assert_eq!(cwd.as_deref(), Some(canonical.as_path()));
        assert!(loaded.config.skills[0].is_absolute());
        std::fs::write(&path, r#"{"servers":{"x":{"transport":"http","url":"https://example.test","headers":{"Host":"elsewhere"}}}}"#).unwrap();
        assert!(LoadedConfig::load(&path).is_err());
        std::fs::write(&path, r#"{"servers":{"x":{"transport":"http","url":"https://example.test","headers":{"Last-Event-Id":"42"}}}}"#).unwrap();
        assert!(LoadedConfig::load(&path).is_err());
        std::fs::write(&path, r#"{"skills":[""]}"#).unwrap();
        assert!(LoadedConfig::load(&path).is_err());
        std::fs::write(&path, r#"{"servers":{},"servers":{}}"#).unwrap();
        assert!(LoadedConfig::load(&path).is_err());

        let base = LoadedConfig::load({
            std::fs::write(&path, r#"{"agents":null}"#).unwrap();
            &path
        })
        .unwrap();
        assert!(base.config.agents.is_none());
        std::fs::write(&path, r#"{"agents":"./docs/AGENTS.md"}"#).unwrap();
        let with_agents = LoadedConfig::load(&path).unwrap();
        assert_eq!(
            with_agents.config.agents.as_deref(),
            Some(canonical.join("docs").join("AGENTS.md").as_path())
        );
        assert_ne!(base.digest, with_agents.digest);
        for invalid in ["", ".", "..", "AGENTS.md/", "docs/."] {
            let body = serde_json::json!({"agents": invalid});
            std::fs::write(&path, serde_json::to_vec(&body).unwrap()).unwrap();
            assert!(LoadedConfig::load(&path).is_err(), "{invalid:?}");
        }
    }
    #[test]
    fn invalid_config_shape_does_not_echo_credentials() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mcpcli.json");
        std::fs::write(&path, r#"{"servers":{"x":{"transport":"http","url":"https://example.test","headers":"Bearer SECRET_SENTINEL"}}}"#).unwrap();
        let error = LoadedConfig::load(&path).err().unwrap();
        let text = format!("{error:#}");
        assert!(!text.contains("SECRET_SENTINEL"));
        assert!(text.contains("servers.\"x\".headers"));

        std::fs::write(
            &path,
            r#"{"servers":{"x":{"transport":"stdio","command":"sh","arg":["-c"],"Bearer KEY_SECRET":"1"}}}"#,
        )
        .unwrap();
        let error = LoadedConfig::load(&path).err().unwrap();
        let text = format!("{error:#}");
        assert!(!text.contains("KEY_SECRET"));
        assert!(text.contains("servers.\"x\""));

        std::fs::write(&path, r#"{"servers":{},"mcpServers":{}}"#).unwrap();
        let error = LoadedConfig::load(&path).err().unwrap();
        assert!(format!("{error:#}").contains("mcpServers"));

        std::fs::write(&path, r#"{"agents":["AGENT_SECRET"]}"#).unwrap();
        let error = LoadedConfig::load(&path).err().unwrap();
        let text = format!("{error:#}");
        assert!(!text.contains("AGENT_SECRET"));
        assert!(text.contains("at agents:"));
    }
    #[test]
    fn http_headers_expand_inline_env_templates_and_reject_malformed_references() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mcpcli.json");
        unsafe {
            std::env::set_var("MCPCLI_UNIT_TOKEN_A", "tok_alpha");
            std::env::set_var("MCPCLI_UNIT_TOKEN_B", "tok_beta");
            std::env::remove_var("MCPCLI_UNIT_MISSING_VAR");
        }
        std::fs::write(
            &path,
            r#"{"servers":{"x":{"transport":"http","url":"https://example.test/mcp","headers":{"Authorization":"Bearer ${MCPCLI_UNIT_TOKEN_A}","X-Trace":"${MCPCLI_UNIT_TOKEN_A}:${MCPCLI_UNIT_TOKEN_B}","X-Static":"plain$literal"}}}}"#,
        )
        .unwrap();
        let loaded = LoadedConfig::load(&path).unwrap();
        let ServerConfig::Http { headers, .. } = &loaded.config.servers["x"] else {
            panic!("expected http server");
        };
        assert_eq!(headers["authorization"], "Bearer tok_alpha");
        assert_eq!(headers["x-trace"], "tok_alpha:tok_beta");
        assert_eq!(headers["x-static"], "plain$literal");

        for bad in [
            "Bearer ${}",
            "Bearer ${BAD-NAME}",
            "Bearer ${UNCLOSED",
            "${",
            "Bearer ${MCPCLI_UNIT_MISSING_VAR}",
            "Bearer ${A${B}}",
        ] {
            let doc = serde_json::json!({
                "servers": {
                    "x": {
                        "transport": "http",
                        "url": "https://example.test/mcp",
                        "headers": { "Authorization": bad }
                    }
                }
            });
            std::fs::write(&path, serde_json::to_vec(&doc).unwrap()).unwrap();
            assert!(
                LoadedConfig::load(&path).is_err(),
                "expected error for {bad:?}"
            );
        }
        unsafe {
            std::env::remove_var("MCPCLI_UNIT_TOKEN_A");
            std::env::remove_var("MCPCLI_UNIT_TOKEN_B");
        }
    }

    #[test]
    fn missing_config_can_be_located_for_stop() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(
            locate(&dir.path().join("deleted.json")).unwrap(),
            dir.path().canonicalize().unwrap().join("deleted.json")
        );
    }

    fn config_env_lock() -> &'static std::sync::Mutex<()> {
        static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(|| std::sync::Mutex::new(()))
    }

    #[test]
    fn oauth_config_debug_redacts_client_secret() {
        let cfg = OAuthConfig {
            client_id: Some("cid-123".into()),
            client_secret: Some("super_secret_value".into()),
            client_name: None,
            callback_port: Some(8765),
            callback_path: Some("/cb".into()),
            scopes: vec!["mcp:tools".into()],
            auth_server_metadata_url: Some(
                "https://auth.example.com/.well-known/openid-configuration".into(),
            ),
            account: Some("work".into()),
        };
        let rendered = format!("{cfg:?}");
        assert!(!rendered.contains("super_secret_value"));
        assert!(rendered.contains("[REDACTED]"));
        assert!(rendered.contains("cid-123"));
    }

    #[test]
    fn oauth_config_validation_and_redacted_shape_errors() {
        let _guard = config_env_lock().lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mcpcli.json");
        unsafe {
            std::env::set_var("MCPCLI_TEST_OAUTH_ID", "expanded_cid");
            std::env::set_var("MCPCLI_TEST_OAUTH_SECRET", "expanded_sec");
        }

        // Valid HTTP server with full OAuth config (including env expansion) and timeout.
        std::fs::write(
            &path,
            r#"{
                "servers": {
                    "api": {
                        "transport": "http",
                        "url": "https://mcp.example.com/mcp",
                        "timeout": 120,
                        "oauth": {
                            "client_id": "${MCPCLI_TEST_OAUTH_ID}",
                            "client_secret": "prefix-${MCPCLI_TEST_OAUTH_SECRET}",
                            "callback_port": 8765,
                            "callback_path": "/oauth/cb",
                            "scopes": ["mcp:tools", "mcp:events"],
                            "auth_server_metadata_url": "https://auth.example.com/.well-known/openid-configuration",
                            "account": "work_1"
                        }
                    }
                }
            }"#,
        )
        .unwrap();
        let loaded = LoadedConfig::load(&path).unwrap();
        let srv = &loaded.config.servers["api"];
        assert!(srv.enabled());
        assert_eq!(srv.timeout(), Some(120));
        let oauth = srv.oauth().unwrap();
        assert_eq!(oauth.client_id.as_deref(), Some("expanded_cid"));
        assert_eq!(oauth.client_secret.as_deref(), Some("prefix-expanded_sec"));
        assert_eq!(oauth.callback_port, Some(8765));
        assert_eq!(oauth.callback_path.as_deref(), Some("/oauth/cb"));
        assert_eq!(oauth.account.as_deref(), Some("work_1"));

        // Reject oauth on stdio without echoing secret.
        std::fs::write(
            &path,
            r#"{"servers":{"s":{"transport":"stdio","command":"sh","oauth":{"client_secret":"STDIO_SECRET"}}}}"#,
        )
        .unwrap();
        let err = format!("{:#}", LoadedConfig::load(&path).err().unwrap());
        assert!(!err.contains("STDIO_SECRET"));
        assert!(err.contains("oauth"));

        // Reject bad oauth shape without echoing secret.
        std::fs::write(
            &path,
            r#"{"servers":{"h":{"transport":"http","url":"https://mcp.example.com/mcp","oauth":{"client_secret":["SHAPE_SECRET"]}}}}"#,
        )
        .unwrap();
        let err = format!("{:#}", LoadedConfig::load(&path).err().unwrap());
        assert!(!err.contains("SHAPE_SECRET"));
        assert!(err.contains("oauth.client_secret"));

        // Semantic validation failures.
        let invalid_oauth_cases = [
            serde_json::json!({"client_secret": "only-secret"}),
            serde_json::json!({"client_id": "cid", "client_name": "custom"}),
            serde_json::json!({"client_name": "   "}),
            serde_json::json!({"client_name": "a".repeat(129)}),
            serde_json::json!({"callback_port": 0}),
            serde_json::json!({"callback_path": "no-leading-slash"}),
            serde_json::json!({"callback_path": "/cb?foo=1"}),
            serde_json::json!({"scopes": [""]}),
            serde_json::json!({"scopes": ["mcp:read mcp:write"]}),
            serde_json::json!({"auth_server_metadata_url": "http://127.0.0.1:9000/.well-known/openid-configuration"}),
            serde_json::json!({"auth_server_metadata_url": "https://169.254.169.254/.well-known/openid-configuration"}),
            serde_json::json!({"account": "bad account!"}),
        ];
        for oauth_val in invalid_oauth_cases {
            let doc = serde_json::json!({
                "servers": {
                    "h": {
                        "transport": "http",
                        "url": "https://mcp.example.com/mcp",
                        "oauth": oauth_val
                    }
                }
            });
            std::fs::write(&path, serde_json::to_vec(&doc).unwrap()).unwrap();
            assert!(
                LoadedConfig::load(&path).is_err(),
                "expected semantic error for oauth={oauth_val}"
            );
        }

        // Loopback HTTP server allows loopback auth_server_metadata_url.
        std::fs::write(
            &path,
            r#"{"servers":{"h":{"transport":"http","url":"http://127.0.0.1:9000/mcp","oauth":{"auth_server_metadata_url":"http://127.0.0.1:9001/.well-known/openid-configuration"}}}}"#,
        )
        .unwrap();
        assert!(LoadedConfig::load(&path).is_ok());

        // Timeout bounds 1..=3600.
        for bad_timeout in [0u64, 3601u64] {
            let doc = serde_json::json!({
                "servers": {
                    "h": {
                        "transport": "http",
                        "url": "https://mcp.example.com/mcp",
                        "timeout": bad_timeout
                    }
                }
            });
            std::fs::write(&path, serde_json::to_vec(&doc).unwrap()).unwrap();
            assert!(LoadedConfig::load(&path).is_err(), "timeout={bad_timeout}");
        }

        unsafe {
            std::env::remove_var("MCPCLI_TEST_OAUTH_ID");
            std::env::remove_var("MCPCLI_TEST_OAUTH_SECRET");
        }
    }

    #[test]
    fn disabled_servers_skip_env_tilde_and_cwd_checks_and_affect_digest() {
        let _guard = config_env_lock().lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mcpcli.json");
        unsafe {
            std::env::remove_var("MCPCLI_DISABLED_MISSING_ENV");
        }
        std::fs::write(
            &path,
            r#"{
                "servers": {
                    "off_stdio": {
                        "transport": "stdio",
                        "enabled": false,
                        "command": "~/nonexistent/bin/srv",
                        "cwd": "/nonexistent/path/for/disabled/server",
                        "env": { "TOKEN": "${MCPCLI_DISABLED_MISSING_ENV}" }
                    },
                    "off_http": {
                        "transport": "http",
                        "enabled": false,
                        "url": "https://mcp.example.com/mcp",
                        "headers": { "Authorization": "Bearer ${MCPCLI_DISABLED_MISSING_ENV}" },
                        "oauth": {
                            "client_id": "cid",
                            "client_secret": "${MCPCLI_DISABLED_MISSING_ENV}"
                        }
                    }
                }
            }"#,
        )
        .unwrap();
        let loaded = LoadedConfig::load(&path).unwrap();
        assert!(!loaded.config.servers["off_stdio"].enabled());
        assert!(!loaded.config.servers["off_http"].enabled());
        let ServerConfig::Stdio { command, env, .. } = &loaded.config.servers["off_stdio"] else {
            panic!()
        };
        assert_eq!(command, "~/nonexistent/bin/srv");
        assert_eq!(env["TOKEN"], "${MCPCLI_DISABLED_MISSING_ENV}");
        assert_eq!(
            loaded.config.servers["off_http"]
                .oauth()
                .unwrap()
                .client_secret
                .as_deref(),
            Some("${MCPCLI_DISABLED_MISSING_ENV}")
        );

        // Digest changes when a disabled server's fields change.
        let digest1 = loaded.digest.clone();
        std::fs::write(
            &path,
            r#"{
                "servers": {
                    "off_stdio": {
                        "transport": "stdio",
                        "enabled": false,
                        "command": "~/other/bin/srv",
                        "cwd": "/nonexistent/path/for/disabled/server",
                        "env": { "TOKEN": "${MCPCLI_DISABLED_MISSING_ENV}" }
                    },
                    "off_http": {
                        "transport": "http",
                        "enabled": false,
                        "url": "https://mcp.example.com/mcp",
                        "headers": { "Authorization": "Bearer ${MCPCLI_DISABLED_MISSING_ENV}" },
                        "oauth": {
                            "client_id": "cid",
                            "client_secret": "${MCPCLI_DISABLED_MISSING_ENV}"
                        }
                    }
                }
            }"#,
        )
        .unwrap();
        let loaded2 = LoadedConfig::load(&path).unwrap();
        assert_ne!(digest1, loaded2.digest);

        // Invalid shape on disabled server still fails fast.
        std::fs::write(
            &path,
            r#"{"servers":{"off":{"transport":"stdio","enabled":false,"command":123}}}"#,
        )
        .unwrap();
        assert!(LoadedConfig::load(&path).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn tilde_expansion_resolves_paths_and_rejects_bad_home() {
        const CHILD_MARKER: &str = "MCPCLI_TEST_TILDE_HOME_CHILD";
        if std::env::var_os(CHILD_MARKER).as_deref() != Some(std::ffi::OsStr::new("1")) {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "config::tests::tilde_expansion_resolves_paths_and_rejects_bad_home",
                    "--nocapture",
                ])
                .env(CHILD_MARKER, "1")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "isolated tilde expansion test failed: {}\nstdout:\n{}\nstderr:\n{}",
                output.status,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let home_dir = tempfile::tempdir().unwrap();
        let canon_home = home_dir.path().canonicalize().unwrap();
        std::fs::create_dir_all(canon_home.join("work")).unwrap();
        let path = dir.path().join("mcpcli.json");
        let prev_home = std::env::var_os("HOME");
        // SAFETY: this test runs alone in a child process with its own environment.
        unsafe {
            std::env::set_var("HOME", &canon_home);
        }

        std::fs::write(
            &path,
            r#"{
                "agents": "~/AGENTS.md",
                "skills": ["~", "~/skills"],
                "servers": {
                    "s": {
                        "transport": "stdio",
                        "command": "~/bin/server",
                        "args": ["~/untouched", "~other/untouched"],
                        "cwd": "~/work"
                    }
                }
            }"#,
        )
        .unwrap();
        let loaded = LoadedConfig::load(&path).unwrap();
        assert_eq!(
            loaded.config.agents.as_deref(),
            Some(canon_home.join("AGENTS.md").as_path())
        );
        assert_eq!(loaded.config.skills[0], canon_home);
        assert_eq!(loaded.config.skills[1], canon_home.join("skills"));
        let ServerConfig::Stdio {
            command, args, cwd, ..
        } = &loaded.config.servers["s"]
        else {
            panic!()
        };
        assert_eq!(
            command,
            canon_home.join("bin").join("server").to_str().unwrap()
        );
        assert_eq!(args, &vec!["~/untouched", "~other/untouched"]);
        assert_eq!(cwd.as_deref(), Some(canon_home.join("work").as_path()));

        // Double slash after ~ must stay under HOME rather than escaping to root /work.
        std::fs::write(
            &path,
            r#"{"servers":{"s":{"transport":"stdio","command":"~//bin/server","cwd":"~//work"}}}"#,
        )
        .unwrap();
        let loaded_double_slash = LoadedConfig::load(&path).unwrap();
        let ServerConfig::Stdio { command, cwd, .. } = &loaded_double_slash.config.servers["s"]
        else {
            panic!()
        };
        assert_eq!(
            command,
            canon_home.join("bin").join("server").to_str().unwrap()
        );
        assert_eq!(cwd.as_deref(), Some(canon_home.join("work").as_path()));

        // ~user/ is never expanded (remains relative to config dir).
        std::fs::write(
            &path,
            r#"{"servers":{"s":{"transport":"stdio","command":"~other/bin/srv"}}}"#,
        )
        .unwrap();
        let loaded_user = LoadedConfig::load(&path).unwrap();
        let ServerConfig::Stdio { command, .. } = &loaded_user.config.servers["s"] else {
            panic!()
        };
        assert_eq!(
            command,
            dir.path()
                .canonicalize()
                .unwrap()
                .join("~other/bin/srv")
                .to_str()
                .unwrap()
        );

        // Relative or empty HOME fails when tilde expansion is requested.
        for bad_home in ["", "relative/home"] {
            // SAFETY: this test runs alone in a child process with its own environment.
            unsafe {
                std::env::set_var("HOME", bad_home);
            }
            std::fs::write(&path, r#"{"agents":"~/AGENTS.md"}"#).unwrap();
            assert!(
                LoadedConfig::load(&path).is_err(),
                "expected error for HOME={bad_home:?}"
            );
        }

        // SAFETY: this test runs alone in a child process with its own environment.
        unsafe {
            match prev_home {
                Some(v) => std::env::set_var("HOME", v),
                None => std::env::remove_var("HOME"),
            }
        }
    }
    #[cfg(windows)]
    #[test]
    fn windows_command_paths_resolve_from_config_and_reject_ambiguous_drives() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mcpcli.json");
        std::fs::write(
            &path,
            r#"{"servers":{"x":{"transport":"stdio","command":"bin\\server.exe"}}}"#,
        )
        .unwrap();
        let loaded = LoadedConfig::load(&path).unwrap();
        let ServerConfig::Stdio { command, .. } = &loaded.config.servers["x"] else {
            panic!()
        };
        assert_eq!(
            Path::new(command),
            dir.path()
                .canonicalize()
                .unwrap()
                .join("bin")
                .join("server.exe")
        );
        for command in ["C:server.exe", r"\server.exe"] {
            let config =
                serde_json::json!({"servers":{"x":{"transport":"stdio","command":command}}});
            std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
            assert!(LoadedConfig::load(&path).is_err());
        }
        for agents in ["C:AGENTS.md", r"\AGENTS.md", "AGENTS.md:secret"] {
            let config = serde_json::json!({"agents": agents});
            std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
            assert!(LoadedConfig::load(&path).is_err(), "{agents:?}");
        }

        let sub = dir.path().join("sub");
        std::fs::create_dir_all(dir.path().join("skills").join("ops")).unwrap();
        std::fs::write(
            dir.path().join("skills").join("ops").join("SKILL.md"),
            "---\nname: ops\ndescription: Parent relative skill\n---\n",
        )
        .unwrap();
        std::fs::write(dir.path().join("AGENTS.md"), "# Parent Global\n").unwrap();
        std::fs::create_dir_all(&sub).unwrap();
        let sub_config = sub.join("mcpcli.json");
        std::fs::write(
            &sub_config,
            r#"{"agents":"../AGENTS.md","servers":{"x":{"transport":"stdio","command":"../bin/server.exe","cwd":".."}},"skills":["../skills"]}"#,
        )
        .unwrap();
        let parent_loaded = LoadedConfig::load(&sub_config).unwrap();
        let root_canonical = dir.path().canonicalize().unwrap();
        assert_eq!(
            parent_loaded.config.agents.as_deref(),
            Some(root_canonical.join("AGENTS.md").as_path())
        );
        let ServerConfig::Stdio { command, cwd, .. } = &parent_loaded.config.servers["x"] else {
            panic!()
        };
        assert_eq!(
            Path::new(command),
            root_canonical.join("bin").join("server.exe")
        );
        assert_eq!(cwd.as_deref(), Some(root_canonical.as_path()));
        assert_eq!(
            crate::agents::load(parent_loaded.config.agents.as_deref())
                .unwrap()
                .unwrap()
                .content,
            "# Parent Global\n"
        );
        assert_eq!(
            crate::skills::discover(&parent_loaded.config.skills)
                .unwrap()
                .skills
                .len(),
            1
        );
    }
    #[cfg(windows)]
    #[test]
    fn rejects_windows_environment_name_collisions() {
        assert!(
            reject_duplicate_windows_env(["Path".to_owned(), "PATH".to_owned()].iter()).is_err()
        );
        assert!(reject_duplicate_windows_env(["FOO".to_owned(), "BAR".to_owned()].iter()).is_ok());
    }
}
