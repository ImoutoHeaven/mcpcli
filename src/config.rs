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

fn expand_env(value: &str) -> Result<String> {
    if let Some(name) = value
        .strip_prefix(concat!("$", "{"))
        .and_then(|v| v.strip_suffix('}'))
    {
        if name.is_empty() || !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
            bail!("invalid environment variable reference");
        }
        std::env::var(name)
            .with_context(|| format!("environment variable {name} is missing or not UTF-8"))
    } else {
        Ok(value.to_owned())
    }
}

impl LoadedConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let path = locate(path)?;
        let bytes = crate::json::read_file(&path, CONFIG_LIMIT)?;
        let value = crate::json::parse(&bytes)?;
        let mut config: Config = serde_json::from_value(value).map_err(|_| {
            anyhow::anyhow!(
                "invalid configuration shape; check version, servers, skills and transport fields"
            )
        })?;
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
                    // Commands containing a slash resolve from the config, not the caller's cwd.
                    if command.contains('/') && !Path::new(command).is_absolute() {
                        *command = parent
                            .join(&*command)
                            .to_str()
                            .context("command path is not UTF-8")?
                            .to_owned();
                    }
                    let resolved = match cwd.as_ref() {
                        Some(p) if p.is_absolute() => p.clone(),
                        Some(p) => parent.join(p),
                        None => parent.to_path_buf(),
                    };
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
                        if matches!(
                            lower,
                            "host"
                                | "content-length"
                                | "content-type"
                                | "accept"
                                | "connection"
                                | "transfer-encoding"
                                | "upgrade"
                                | "te"
                                | "trailer"
                        ) || lower.starts_with("mcp-")
                            || lower.starts_with("mcp_")
                        {
                            bail!(
                                "server {name}: a header reserved by the transport cannot be overridden"
                            );
                        }
                        let resolved = expand_env(value)?;
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
        for root in &mut config.skills {
            if !root.is_absolute() {
                *root = parent.join(&*root);
            }
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
        assert_eq!(command, dir.path().join("bin/server").to_str().unwrap());
        assert_eq!(cwd.as_deref(), Some(dir.path()));
        assert!(loaded.config.skills[0].is_absolute());
        std::fs::write(&path, r#"{"servers":{"x":{"transport":"http","url":"https://example.test","headers":{"Host":"elsewhere"}}}}"#).unwrap();
        assert!(LoadedConfig::load(&path).is_err());
        std::fs::write(&path, r#"{"servers":{},"servers":{}}"#).unwrap();
        assert!(LoadedConfig::load(&path).is_err());
    }
    #[test]
    fn invalid_config_shape_does_not_echo_credentials() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mcpcli.json");
        std::fs::write(&path, r#"{"servers":{"x":{"transport":"http","url":"https://example.test","headers":"Bearer SECRET_SENTINEL"}}}"#).unwrap();
        let error = LoadedConfig::load(&path).err().unwrap();
        assert!(!format!("{error:#}").contains("SECRET_SENTINEL"));
    }
    #[test]
    fn missing_config_can_be_located_for_stop() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(
            locate(&dir.path().join("deleted.json")).unwrap(),
            dir.path().join("deleted.json")
        );
    }
}
