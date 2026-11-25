use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

pub const CONFIG_LIMIT: usize = 1 << 20;

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
    },
    Http {
        url: String,
        #[serde(default)]
        headers: BTreeMap<String, String>,
        #[serde(default)]
        description: Option<String>,
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
                        for key in fields.keys() {
                            if !matches!(
                                key.as_str(),
                                "transport" | "command" | "args" | "env" | "cwd" | "description"
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
                    }
                    Some("http") => {
                        for key in fields.keys() {
                            if !matches!(
                                key.as_str(),
                                "transport" | "url" | "headers" | "description"
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

pub fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
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
            match server {
                ServerConfig::Stdio {
                    command,
                    args,
                    env,
                    cwd,
                    ..
                } => {
                    if command.is_empty() || command.contains('\0') {
                        bail!("server {name}: command must be nonempty without NUL");
                    }
                    if args.len() > 1024 || args.iter().any(|s| s.contains('\0')) {
                        bail!("server {name}: invalid or too many arguments");
                    }
                    // Explicit command paths use the configuration directory on every platform.
                    if command.contains('/') || (cfg!(windows) && command.contains(['\\', ':'])) {
                        *command = from_config_directory(parent, Path::new(command))?
                            .to_str()
                            .context("command path is not UTF-8")?
                            .to_owned();
                    }
                    let resolved =
                        from_config_directory(parent, cwd.as_deref().unwrap_or(Path::new(".")))?;
                    let resolved = resolved
                        .canonicalize()
                        .with_context(|| format!("server {name}: cwd does not exist"))?;
                    if !resolved.is_dir() {
                        bail!("server {name}: cwd is not a directory");
                    }
                    *cwd = Some(resolved);
                    if env.len() > 256 {
                        bail!("server {name}: too many environment overrides");
                    }
                    #[cfg(windows)]
                    reject_duplicate_windows_env(env.keys())?;
                    for (key, value) in env {
                        if key.is_empty() || key.contains(['=', '\0']) {
                            bail!("server {name}: invalid environment variable name");
                        }
                        *value = expand_env(value)?;
                        if value.contains('\0') {
                            bail!("server {name}: environment value contains NUL");
                        }
                    }
                }
                ServerConfig::Http { url, headers, .. } => {
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
                    let mut normalized = BTreeMap::new();
                    for (key, value) in headers.iter() {
                        let key = http::HeaderName::from_bytes(key.as_bytes()).map_err(|_| {
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
                }
            }
        }
        if let Some(agents) = &mut config.agents {
            crate::agents::validate_path(agents)?;
            *agents = from_config_directory(parent, agents)?;
        }
        for root in &mut config.skills {
            if root.as_os_str().is_empty() {
                bail!("skill root path must not be empty");
            }
            *root = from_config_directory(parent, root)?;
        }
        let digest = digest(&serde_json::to_vec(&config)?);
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
