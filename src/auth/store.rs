use crate::{platform, runtime::Lock};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    ffi::OsStr,
    io::Write,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

const MAX_CREDENTIALS_BYTES: usize = 1 << 20;
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_endpoint_auth_method: Option<String>,
    pub access_token: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<u64>,
    #[serde(default)]
    pub scopes: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct CredentialsStoreFile {
    #[serde(default = "store_version")]
    pub(super) version: u32,
    #[serde(default)]
    pub(super) resources: BTreeMap<String, StoredCredential>,
}

impl Default for CredentialsStoreFile {
    fn default() -> Self {
        Self {
            version: 1,
            resources: BTreeMap::new(),
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

pub(super) fn resolve_store_location_readonly() -> Result<Option<PathBuf>> {
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

pub(super) fn read_store_at(path: &Path) -> Result<CredentialsStoreFile> {
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

pub(super) fn read_store_readonly() -> Result<CredentialsStoreFile> {
    match resolve_store_location_readonly()? {
        Some(path) => read_store_at(&path),
        None => Ok(CredentialsStoreFile::default()),
    }
}

pub(super) async fn mutate_store<R>(
    mutate: impl FnOnce(&mut CredentialsStoreFile) -> Result<R>,
) -> Result<R> {
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

pub(super) async fn credential_lock(storage_key: &str) -> Result<Lock> {
    let location = prepare_store_location_writable()?;
    let parent = location
        .file_path
        .parent()
        .context("credentials path has no parent")?;
    // Keep one lock across filename aliases and the first store creation.
    let path = parent.join(format!(
        ".oauth-{}.lock",
        crate::platform::digest(storage_key.as_bytes())
    ));
    // A refresh may take 15 seconds before committing its rotated token.
    Lock::acquire(&path, Duration::from_secs(30)).await
}
