//! Local media persistence, MIME extension whitelist, and cross-process cache admission.
use crate::{
    budget::{ByteBudget, ByteLease, format_bytes},
    ipc::Failure,
    platform,
    runtime::Lock,
};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashSet, hash_map::RandomState},
    ffi::OsStr,
    hash::{BuildHasher, Hasher},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub const MAX_MEDIA_CACHE_BYTES: u64 = 2 << 30;
pub const DEFAULT_PROTECTION_MS: u64 = 60_000;
pub const DEFAULT_LOCK_WAIT: Duration = Duration::from_secs(10);

pub type ClockFn = Arc<dyn Fn() -> io::Result<u64> + Send + Sync>;

static SEQUENCE: AtomicU64 = AtomicU64::new(1);

fn next_sequence() -> u64 {
    SEQUENCE.fetch_add(1, Ordering::Relaxed)
}

fn random_hex(chars: usize) -> String {
    let seq = next_sequence();
    let mut hasher_a = RandomState::new().build_hasher();
    hasher_a.write_u64(seq);
    let mut hasher_b = RandomState::new().build_hasher();
    hasher_b.write_u32(std::process::id());
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let mono = platform::monotonic_millis().unwrap_or(0);
    let mut sha = Sha256::new();
    sha.update(hasher_a.finish().to_le_bytes());
    sha.update(hasher_b.finish().to_le_bytes());
    sha.update(nanos.to_le_bytes());
    sha.update(mono.to_le_bytes());
    sha.update(seq.to_le_bytes());
    let digest = sha.finalize();
    let mut hex = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut hex, "{byte:02x}");
    }
    hex.truncate(chars);
    hex
}

pub fn extension_for_mime(mime: &str) -> &'static str {
    let essence = mime
        .split_once(';')
        .map_or(mime, |(essence, _)| essence)
        .trim();
    match essence.to_ascii_lowercase().as_str() {
        "video/mp4" => ".mp4",
        "video/webm" => ".webm",
        "video/quicktime" => ".mov",
        "video/3gpp" => ".3gp",
        "video/avi" => ".avi",
        "audio/mp4" => ".m4a",
        "audio/wav" => ".wav",
        "audio/mpeg" => ".mp3",
        "audio/ogg" => ".ogg",
        "audio/flac" => ".flac",
        "audio/aac" => ".aac",
        "audio/aiff" => ".aiff",
        "application/pdf" => ".pdf",
        "image/png" => ".png",
        "image/jpeg" => ".jpg",
        "image/gif" => ".gif",
        "image/webp" => ".webp",
        "image/bmp" => ".bmp",
        "image/tiff" => ".tiff",
        "image/heic" => ".heic",
        "image/heif" => ".heif",
        _ => ".bin",
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DecodeError {
    InvalidBase64,
    MemoryLimitExceeded,
}

const fn b64_table() -> [u8; 256] {
    let mut table = [0xffu8; 256];
    let mut i = 0usize;
    while i < 26 {
        table[b'A' as usize + i] = i as u8;
        table[b'a' as usize + i] = (26 + i) as u8;
        i += 1;
    }
    let mut d = 0usize;
    while d < 10 {
        table[b'0' as usize + d] = (52 + d) as u8;
        d += 1;
    }
    table[b'+' as usize] = 62;
    table[b'/' as usize] = 63;
    table
}

const B64: [u8; 256] = b64_table();

pub fn decode_base64_leased(
    encoded: &str,
    budget: &Arc<ByteBudget>,
) -> std::result::Result<(Vec<u8>, ByteLease), DecodeError> {
    let bytes = encoded.as_bytes();
    if bytes.is_empty() {
        return Ok((Vec::new(), budget.empty_lease()));
    }
    if !bytes.len().is_multiple_of(4) {
        return Err(DecodeError::InvalidBase64);
    }
    let pad = if bytes[bytes.len() - 2] == b'=' {
        if bytes[bytes.len() - 1] != b'=' {
            return Err(DecodeError::InvalidBase64);
        }
        2
    } else if bytes[bytes.len() - 1] == b'=' {
        1
    } else {
        0
    };
    let groups = bytes.len() / 4;
    let out_len = groups * 3 - pad;
    let lease = budget
        .try_acquire(out_len)
        .ok_or(DecodeError::MemoryLimitExceeded)?;
    let mut out = vec![0u8; out_len];
    let full_groups = if pad == 0 { groups } else { groups - 1 };
    let mut in_idx = 0usize;
    let mut out_idx = 0usize;
    for _ in 0..full_groups {
        let v0 = B64[bytes[in_idx] as usize];
        let v1 = B64[bytes[in_idx + 1] as usize];
        let v2 = B64[bytes[in_idx + 2] as usize];
        let v3 = B64[bytes[in_idx + 3] as usize];
        if (v0 | v1 | v2 | v3) & 0xc0 != 0 {
            return Err(DecodeError::InvalidBase64);
        }
        out[out_idx] = (v0 << 2) | (v1 >> 4);
        out[out_idx + 1] = (v1 << 4) | (v2 >> 2);
        out[out_idx + 2] = (v2 << 6) | v3;
        in_idx += 4;
        out_idx += 3;
    }
    if pad == 2 {
        let v0 = B64[bytes[in_idx] as usize];
        let v1 = B64[bytes[in_idx + 1] as usize];
        if ((v0 | v1) & 0xc0 != 0) || (v1 & 0x0f != 0) {
            return Err(DecodeError::InvalidBase64);
        }
        out[out_idx] = (v0 << 2) | (v1 >> 4);
    } else if pad == 1 {
        let v0 = B64[bytes[in_idx] as usize];
        let v1 = B64[bytes[in_idx + 1] as usize];
        let v2 = B64[bytes[in_idx + 2] as usize];
        if ((v0 | v1 | v2) & 0xc0 != 0) || (v2 & 0x03 != 0) {
            return Err(DecodeError::InvalidBase64);
        }
        out[out_idx] = (v0 << 2) | (v1 >> 4);
        out[out_idx + 1] = (v1 << 4) | (v2 >> 2);
    }
    Ok((out, lease))
}

fn ensure_runtime_media_dir(root: &Path) -> Result<PathBuf> {
    if !root.is_absolute() {
        bail!("runtime directory must be absolute");
    }
    let parent = root
        .parent()
        .context("runtime directory must have a parent")?
        .canonicalize()?;
    let root = parent.join(
        root.file_name()
            .context("runtime directory must have a name")?,
    );
    platform::private_dir(&root)?;
    let root = root.canonicalize()?;
    let media = root.join("media");
    platform::private_dir(&media)?;
    Ok(media.canonicalize()?)
}

fn ensure_cache_media_dir(cache_base: &Path) -> Result<PathBuf> {
    if !cache_base.is_absolute() {
        bail!("cache directory must be absolute");
    }
    let parent = cache_base
        .parent()
        .context("cache directory must have a parent")?
        .canonicalize()?;
    let base = parent.join(
        cache_base
            .file_name()
            .context("cache directory must have a name")?,
    );
    match std::fs::symlink_metadata(&base) {
        Ok(meta) => {
            if !meta.is_dir() || meta.file_type().is_symlink() {
                bail!("cache base is not a real directory");
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            platform::private_dir(&base)?;
        }
        Err(error) => return Err(error.into()),
    }
    let base = base.canonicalize()?;
    let app = base.join("mcpcli");
    platform::private_dir(&app)?;
    let media = app.canonicalize()?.join("media");
    platform::private_dir(&media)?;
    Ok(media.canonicalize()?)
}

pub fn resolve_media_dir() -> Result<PathBuf> {
    let non_empty = |key: &str| std::env::var_os(key).filter(|v| !v.is_empty());
    #[cfg(unix)]
    {
        resolve_media_dir_unix(
            non_empty("MCPCLI_RUNTIME_DIR").as_deref(),
            non_empty("XDG_CACHE_HOME").as_deref(),
            non_empty("HOME").as_deref(),
        )
    }
    #[cfg(windows)]
    {
        resolve_media_dir_windows(
            non_empty("MCPCLI_RUNTIME_DIR").as_deref(),
            non_empty("LOCALAPPDATA").as_deref(),
        )
    }
}

#[cfg(unix)]
pub fn resolve_media_dir_unix(
    runtime_dir: Option<&OsStr>,
    xdg_cache_home: Option<&OsStr>,
    home: Option<&OsStr>,
) -> Result<PathBuf> {
    if let Some(root) = runtime_dir {
        return ensure_runtime_media_dir(Path::new(root));
    }
    if let Some(xdg) = xdg_cache_home
        && let Ok(dir) = ensure_cache_media_dir(Path::new(xdg))
    {
        return Ok(dir);
    }
    if let Some(home) = home
        && let Ok(dir) = ensure_cache_media_dir(&PathBuf::from(home).join(".cache"))
    {
        return Ok(dir);
    }
    ensure_runtime_media_dir(&platform::default_runtime()?)
}

#[cfg(windows)]
pub fn resolve_media_dir_windows(
    runtime_dir: Option<&OsStr>,
    local_app_data: Option<&OsStr>,
) -> Result<PathBuf> {
    if let Some(root) = runtime_dir {
        return ensure_runtime_media_dir(Path::new(root));
    }
    let base = local_app_data.context("LOCALAPPDATA is not set")?;
    ensure_cache_media_dir(Path::new(base))
}

#[derive(Default, Serialize, Deserialize)]
struct MediaIndex {
    #[serde(default)]
    files: BTreeMap<String, FileMeta>,
    #[serde(default)]
    inflight: BTreeMap<String, Vec<String>>,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
struct FileMeta {
    published_ms: u64,
}

fn remove_ignoring_benign(path: &Path) -> bool {
    match std::fs::remove_file(path) {
        Ok(()) => true,
        Err(error) if error.kind() == io::ErrorKind::NotFound => true,
        Err(error) if cfg!(windows) && error.kind() == io::ErrorKind::PermissionDenied => false,
        Err(_) => !path.exists(),
    }
}

fn load_index(media_dir: &Path) -> MediaIndex {
    let path = media_dir.join(".index.json");
    let Ok(file) = platform::open_input(&path) else {
        return MediaIndex::default();
    };
    let Ok(bytes) = crate::json::read_limited(file, 4 << 20) else {
        return MediaIndex::default();
    };
    serde_json::from_slice(&bytes).unwrap_or_default()
}

fn save_index(media_dir: &Path, index: &MediaIndex) -> Result<()> {
    let tmp = media_dir.join(".index.tmp");
    let _ = remove_ignoring_benign(&tmp);
    let mut file = platform::new_private_file(&tmp)?;
    let encoded = serde_json::to_vec(index)?;
    let outcome = (|| -> Result<()> {
        file.write_all(&encoded)?;
        file.flush()?;
        drop(file);
        platform::replace_file(&tmp, &media_dir.join(".index.json"))?;
        Ok(())
    })();
    if outcome.is_err() {
        let _ = remove_ignoring_benign(&tmp);
    }
    outcome
}

fn gc_locked(
    media_dir: &Path,
    index: &mut MediaIndex,
    now_ms: u64,
    protection_ms: u64,
) -> Result<u64> {
    let mut active_inflight_files = HashSet::new();
    index.inflight.retain(|batch_id, files| {
        let lock_path = media_dir.join(format!(".inflight-{batch_id}.lock"));
        match Lock::try_acquire(&lock_path) {
            Ok(None) => {
                for file in files {
                    active_inflight_files.insert(file.clone());
                }
                true
            }
            Ok(Some(guard)) => {
                drop(guard);
                let _ = remove_ignoring_benign(&lock_path);
                false
            }
            Err(_) => {
                let _ = remove_ignoring_benign(&lock_path);
                false
            }
        }
    });

    let mut used_bytes = 0u64;
    let mut seen_files = HashSet::new();
    for entry in std::fs::read_dir(media_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let path = entry.path();
        let Some(name_str) = name.to_str() else {
            if !remove_ignoring_benign(&path)
                && let Ok(meta) = std::fs::symlink_metadata(&path)
            {
                used_bytes = used_bytes.saturating_add(meta.len());
            }
            continue;
        };
        if matches!(name_str, ".lock" | ".index.json") {
            continue;
        }
        if let Some(batch_id) = name_str
            .strip_prefix(".inflight-")
            .and_then(|s| s.strip_suffix(".lock"))
        {
            if !index.inflight.contains_key(batch_id)
                && let Ok(Some(guard)) = Lock::try_acquire(&path)
            {
                drop(guard);
                let _ = remove_ignoring_benign(&path);
            }
            continue;
        }
        if name_str.starts_with(".tmp-") || name_str == ".index.tmp" {
            if !remove_ignoring_benign(&path)
                && let Ok(meta) = std::fs::symlink_metadata(&path)
            {
                used_bytes = used_bytes.saturating_add(meta.len());
            }
            continue;
        }
        let Ok(meta) = std::fs::symlink_metadata(&path) else {
            continue;
        };
        if !meta.is_file() {
            continue;
        }
        let len = meta.len();
        let in_flight = active_inflight_files.contains(name_str);
        let within_window = index
            .files
            .get(name_str)
            .is_some_and(|record| now_ms.saturating_sub(record.published_ms) < protection_ms);
        if in_flight || within_window {
            used_bytes = used_bytes.saturating_add(len);
            seen_files.insert(name_str.to_owned());
        } else if remove_ignoring_benign(&path) {
            index.files.remove(name_str);
        } else {
            used_bytes = used_bytes.saturating_add(len);
            seen_files.insert(name_str.to_owned());
        }
    }
    index.files.retain(|name, _| seen_files.contains(name));
    let _ = save_index(media_dir, index);
    Ok(used_bytes)
}

#[derive(Clone)]
pub struct MediaStore {
    dir: Option<PathBuf>,
    max_bytes: u64,
    protection_ms: u64,
    lock_wait: Duration,
    clock: ClockFn,
}

impl Default for MediaStore {
    fn default() -> Self {
        Self {
            dir: None,
            max_bytes: MAX_MEDIA_CACHE_BYTES,
            protection_ms: DEFAULT_PROTECTION_MS,
            lock_wait: DEFAULT_LOCK_WAIT,
            clock: Arc::new(platform::monotonic_millis),
        }
    }
}

impl MediaStore {
    pub fn with_options(
        dir: PathBuf,
        max_bytes: u64,
        protection_ms: u64,
        lock_wait: Duration,
        clock: ClockFn,
    ) -> Self {
        Self {
            dir: Some(dir),
            max_bytes,
            protection_ms,
            lock_wait,
            clock,
        }
    }

    fn directory(&self) -> Result<PathBuf> {
        match &self.dir {
            Some(dir) => {
                let parent = dir
                    .parent()
                    .context("media directory must have a parent")?
                    .canonicalize()?;
                let target = parent.join(
                    dir.file_name()
                        .context("media directory must have a name")?,
                );
                platform::private_dir(&target)?;
                Ok(target.canonicalize()?)
            }
            None => resolve_media_dir(),
        }
    }

    pub async fn run_gc(&self) -> Result<u64> {
        let media_dir = self.directory()?;
        let _dir_lock = Lock::acquire(&media_dir.join(".lock"), self.lock_wait).await?;
        let now_ms = (self.clock)()?;
        let mut index = load_index(&media_dir);
        gc_locked(&media_dir, &mut index, now_ms, self.protection_ms)
    }

    pub async fn extract_and_save(
        &self,
        result: &mut Value,
        result_lease: &mut ByteLease,
        budget: &Arc<ByteBudget>,
    ) -> std::result::Result<Option<InflightBatch>, Failure> {
        let Some(root) = result.as_object_mut() else {
            return Ok(None);
        };
        // Never allow an upstream result field to impersonate local savedMedia records.
        root.remove("savedMedia");

        let is_media_resource = |res: &serde_json::Map<String, Value>| match res.get("blob") {
            None => false,
            Some(Value::Null) if res.get("text").is_some_and(Value::is_string) => false,
            Some(_) => true,
        };
        let has_media = root
            .get("content")
            .and_then(Value::as_array)
            .is_some_and(|items| {
                items.iter().any(|block| match block["type"].as_str() {
                    Some("image" | "audio") => block.get("data").is_some(),
                    Some("resource") => block
                        .get("resource")
                        .and_then(Value::as_object)
                        .is_some_and(is_media_resource),
                    _ => false,
                })
            });
        if !has_media {
            return Ok(None);
        }

        let media_dir = self.directory().map_err(|error| {
            Failure::new(
                1,
                "media_cache",
                format!("cannot prepare local media cache: {error:#}; side effects may have occurred, the call was not retried"),
            )
            .uncertain(true)
        })?;
        let _dir_lock = Lock::acquire(&media_dir.join(".lock"), self.lock_wait)
            .await
            .map_err(|error| {
                Failure::new(
                    1,
                    "media_cache",
                    format!("cannot acquire local media cache lock: {error:#}; side effects may have occurred, the call was not retried"),
                )
                .uncertain(true)
            })?;

        let now_ms = (self.clock)().map_err(|error| {
            Failure::new(
                1,
                "media_cache",
                format!("cannot read monotonic clock: {error}; side effects may have occurred, the call was not retried"),
            )
            .uncertain(true)
        })?;
        let mut index = load_index(&media_dir);
        let used_bytes = gc_locked(&media_dir, &mut index, now_ms, self.protection_ms).map_err(
            |error| {
                Failure::new(
                    1,
                    "media_cache",
                    format!("media cache cleanup failed: {error:#}; side effects may have occurred, the call was not retried"),
                )
                .uncertain(true)
            },
        )?;

        let mut created_paths: Vec<PathBuf> = Vec::new();
        let mut inflight_lock_path: Option<PathBuf> = None;
        let pid = std::process::id();

        let outcome = (|| -> std::result::Result<(Vec<Value>, InflightBatch), Failure> {
            let root = result.as_object_mut().expect("verified object");
            let content = root
                .get_mut("content")
                .and_then(Value::as_array_mut)
                .expect("verified content array");

            let mut batch_bytes = 0u64;
            let mut batch_filenames = Vec::new();
            let mut saved_entries = Vec::new();

            for (content_idx, block) in content.iter_mut().enumerate() {
                let Some(block_obj) = block.as_object_mut() else {
                    continue;
                };
                let (kind, mime_type, raw_value) =
                    match block_obj.get("type").and_then(Value::as_str) {
                        Some("image") => {
                            let Some(raw) = block_obj.remove("data") else {
                                continue;
                            };
                            let mime = block_obj
                                .get("mimeType")
                                .and_then(Value::as_str)
                                .unwrap_or("application/octet-stream")
                                .to_owned();
                            ("image", mime, raw)
                        }
                        Some("audio") => {
                            let Some(raw) = block_obj.remove("data") else {
                                continue;
                            };
                            let mime = block_obj
                                .get("mimeType")
                                .and_then(Value::as_str)
                                .unwrap_or("application/octet-stream")
                                .to_owned();
                            ("audio", mime, raw)
                        }
                        Some("resource") => {
                            let Some(res_obj) =
                                block_obj.get_mut("resource").and_then(Value::as_object_mut)
                            else {
                                continue;
                            };
                            if !is_media_resource(res_obj) {
                                continue;
                            }
                            let Some(raw) = res_obj.remove("blob") else {
                                continue;
                            };
                            let mime = res_obj
                                .get("mimeType")
                                .and_then(Value::as_str)
                                .unwrap_or("application/octet-stream")
                                .to_owned();
                            ("resource", mime, raw)
                        }
                        _ => continue,
                    };

                let Value::String(encoded) = raw_value else {
                    return Err(Failure::new(
                        4,
                        "mcp_protocol",
                        "MCP media payload is not a base64 string; side effects may have occurred, the call was not retried",
                    )
                    .uncertain(true));
                };
                let encoded_len = encoded.len();
                let decode_res = decode_base64_leased(&encoded, budget);
                drop(encoded);
                result_lease.shrink_by(encoded_len);

                let (decoded, decode_lease) = match decode_res {
                    Ok(pair) => pair,
                    Err(DecodeError::InvalidBase64) => {
                        return Err(Failure::new(
                            4,
                            "mcp_protocol",
                            "MCP media payload contains invalid base64; side effects may have occurred, the call was not retried",
                        )
                        .uncertain(true));
                    }
                    Err(DecodeError::MemoryLimitExceeded) => {
                        return Err(Failure::new(
                            1,
                            "memory_limit_exceeded",
                            format!(
                                "decoding MCP media exceeded the {} active memory limit; side effects may have occurred, the call was not retried",
                                format_bytes(budget.limit())
                            ),
                        )
                        .uncertain(true));
                    }
                };

                let decoded_len = decoded.len() as u64;
                let next_batch = batch_bytes.saturating_add(decoded_len);
                if used_bytes.saturating_add(next_batch) > self.max_bytes {
                    return Err(Failure::new(
                        1,
                        "media_cache_full",
                        format!(
                            "local media cache exceeded its {} limit; side effects may have occurred, the call was not retried",
                            format_bytes(self.max_bytes as usize)
                        ),
                    )
                    .uncertain(true));
                }

                let seq = next_sequence();
                let tmp_name = format!(".tmp-{pid}-{seq}-{}", random_hex(16));
                let tmp_path = media_dir.join(tmp_name);
                let mut file = platform::new_private_file(&tmp_path).map_err(|error| {
                    Failure::new(
                        1,
                        "media_cache",
                        format!("cannot create media staging file: {error}; side effects may have occurred, the call was not retried"),
                    )
                    .uncertain(true)
                })?;
                created_paths.push(tmp_path.clone());

                if let Err(error) = file.write_all(&decoded).and_then(|()| file.flush()) {
                    return Err(Failure::new(
                        1,
                        "media_cache",
                        format!("cannot write media file: {error}; side effects may have occurred, the call was not retried"),
                    )
                    .uncertain(true));
                }
                drop(file);
                drop(decoded);
                drop(decode_lease);

                let ext = extension_for_mime(&mime_type);
                let (final_name, final_path) = loop {
                    let candidate_name = format!("{}{ext}", random_hex(32));
                    let candidate_path = media_dir.join(&candidate_name);
                    match candidate_path.try_exists() {
                        Ok(false) => break (candidate_name, candidate_path),
                        Ok(true) => continue,
                        Err(error) => {
                            return Err(Failure::new(
                                1,
                                "media_cache",
                                format!("cannot inspect media cache entry: {error}; side effects may have occurred, the call was not retried"),
                            )
                            .uncertain(true));
                        }
                    }
                };
                platform::replace_file(&tmp_path, &final_path).map_err(|error| {
                    Failure::new(
                        1,
                        "media_cache",
                        format!("cannot publish media file: {error}; side effects may have occurred, the call was not retried"),
                    )
                    .uncertain(true)
                })?;
                if let Some(last) = created_paths.last_mut() {
                    *last = final_path.clone();
                }

                batch_bytes = next_batch;
                batch_filenames.push(final_name);
                saved_entries.push(json!({
                    "contentIndex": content_idx,
                    "kind": kind,
                    "savedPath": final_path,
                    "mimeType": mime_type,
                    "bytes": decoded_len,
                }));
            }

            let batch_id = format!("{pid}-{}-{}", next_sequence(), random_hex(16));
            let batch_lock_path = media_dir.join(format!(".inflight-{batch_id}.lock"));
            let batch_lock = Lock::try_acquire(&batch_lock_path)
                .map_err(|error| {
                    Failure::new(
                        1,
                        "media_cache",
                        format!("cannot create in-flight batch lock: {error:#}; side effects may have occurred, the call was not retried"),
                    )
                    .uncertain(true)
                })?
                .ok_or_else(|| {
                    Failure::new(
                        1,
                        "media_cache",
                        "in-flight batch lock collision; side effects may have occurred, the call was not retried",
                    )
                    .uncertain(true)
                })?;
            inflight_lock_path = Some(batch_lock_path.clone());

            for name in &batch_filenames {
                index.files.insert(
                    name.clone(),
                    FileMeta {
                        published_ms: now_ms,
                    },
                );
            }
            index.inflight.insert(batch_id.clone(), batch_filenames);
            save_index(&media_dir, &index).map_err(|error| {
                Failure::new(
                    1,
                    "media_cache",
                    format!("cannot update media cache index: {error:#}; side effects may have occurred, the call was not retried"),
                )
                .uncertain(true)
            })?;

            Ok((
                saved_entries,
                InflightBatch {
                    store: self.clone(),
                    media_dir: media_dir.clone(),
                    batch_id,
                    lock: Some(batch_lock),
                    lock_path: batch_lock_path,
                },
            ))
        })();

        match outcome {
            Ok((saved_entries, inflight)) => {
                if !saved_entries.is_empty()
                    && let Some(root) = result.as_object_mut()
                {
                    root.insert("savedMedia".into(), Value::Array(saved_entries));
                }
                Ok(Some(inflight))
            }
            Err(failure) => {
                for path in &created_paths {
                    let _ = remove_ignoring_benign(path);
                }
                if let Some(lock_path) = &inflight_lock_path {
                    let _ = remove_ignoring_benign(lock_path);
                }
                Err(failure)
            }
        }
    }
}

pub struct InflightBatch {
    store: MediaStore,
    media_dir: PathBuf,
    batch_id: String,
    lock: Option<Lock>,
    lock_path: PathBuf,
}

impl InflightBatch {
    pub async fn complete(mut self) {
        if let Ok(_dir_lock) =
            Lock::acquire(&self.media_dir.join(".lock"), self.store.lock_wait).await
        {
            let mut index = load_index(&self.media_dir);
            if let Some(files) = index.inflight.remove(&self.batch_id)
                && let Ok(now_ms) = (self.store.clock)()
            {
                for name in files {
                    if let Some(meta) = index.files.get_mut(&name) {
                        meta.published_ms = now_ms;
                    }
                }
            }
            let _ = save_index(&self.media_dir, &index);
            drop(self.lock.take());
            let _ = remove_ignoring_benign(&self.lock_path);
        } else {
            drop(self.lock.take());
        }
    }

    pub async fn abort(mut self) {
        if let Ok(_dir_lock) =
            Lock::acquire(&self.media_dir.join(".lock"), Duration::from_secs(2)).await
        {
            let mut index = load_index(&self.media_dir);
            index.inflight.remove(&self.batch_id);
            let _ = save_index(&self.media_dir, &index);
            drop(self.lock.take());
            let _ = remove_ignoring_benign(&self.lock_path);
        } else {
            drop(self.lock.take());
        }
    }
}

impl Drop for InflightBatch {
    fn drop(&mut self) {
        // Releasing the OS lock lets the next holder of .lock reclaim the stale batch.
        drop(self.lock.take());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mime_whitelist_covers_all_required_types_and_falls_back_to_bin() {
        let cases = [
            ("video/mp4", ".mp4"),
            ("VIDEO/MP4; codecs=avc1", ".mp4"),
            ("video/webm", ".webm"),
            ("video/quicktime", ".mov"),
            ("video/3gpp", ".3gp"),
            ("video/avi", ".avi"),
            ("audio/mp4", ".m4a"),
            ("audio/wav", ".wav"),
            ("audio/mpeg", ".mp3"),
            ("audio/ogg", ".ogg"),
            ("audio/flac", ".flac"),
            ("audio/aac", ".aac"),
            ("audio/aiff", ".aiff"),
            ("application/pdf", ".pdf"),
            ("image/png", ".png"),
            ("image/jpeg", ".jpg"),
            ("image/gif", ".gif"),
            ("image/webp", ".webp"),
            ("image/bmp", ".bmp"),
            ("image/tiff", ".tiff"),
            ("image/heic", ".heic"),
            ("image/heif", ".heif"),
            ("application/octet-stream", ".bin"),
            ("text/plain", ".bin"),
            ("", ".bin"),
        ];
        for (mime, expected) in cases {
            assert_eq!(extension_for_mime(mime), expected, "{mime}");
        }
    }

    #[test]
    fn base64_decoder_validates_strictly_and_enforces_memory_budget() {
        let budget = ByteBudget::new(64);
        for (encoded, expected) in [
            ("", b"".as_slice()),
            ("Zg==", b"f"),
            ("Zm8=", b"fo"),
            ("Zm9v", b"foo"),
            ("AQIDBA==", &[1, 2, 3, 4]),
        ] {
            let (out, lease) = decode_base64_leased(encoded, &budget).unwrap();
            assert_eq!(out, expected);
            assert_eq!(lease.bytes(), expected.len());
            drop(lease);
            assert_eq!(budget.used(), 0);
        }

        for invalid in ["abc", "Zg=a", "Z===", "Zm9v====", "ab!d", "Zg= ", "ZR=="] {
            assert_eq!(
                decode_base64_leased(invalid, &budget).unwrap_err(),
                DecodeError::InvalidBase64,
                "{invalid}"
            );
            assert_eq!(budget.used(), 0);
        }

        let tiny = ByteBudget::new(2);
        assert_eq!(
            decode_base64_leased("Zm9v", &tiny).unwrap_err(),
            DecodeError::MemoryLimitExceeded
        );
        assert_eq!(tiny.used(), 0);
    }

    #[cfg(unix)]
    #[test]
    fn unix_media_dir_follows_priority_without_socket_path_length_limit() {
        let base = tempfile::tempdir_in("/tmp").unwrap();
        let root = base.path().canonicalize().unwrap();
        // Long path (> 120 bytes) would fail sun_path socket creation, yet media dir succeeds.
        let deep = root.join("a".repeat(60)).join("b".repeat(60));
        std::fs::create_dir_all(&deep).unwrap();
        let rt = deep.join("rt");
        let resolved = resolve_media_dir_unix(Some(rt.as_os_str()), None, None).unwrap();
        assert_eq!(resolved, rt.canonicalize().unwrap().join("media"));

        let xdg = deep.join("xdg");
        let home = deep.join("home");
        std::fs::create_dir_all(&home).unwrap();
        let from_xdg =
            resolve_media_dir_unix(None, Some(xdg.as_os_str()), Some(home.as_os_str())).unwrap();
        assert_eq!(
            from_xdg,
            xdg.canonicalize().unwrap().join("mcpcli").join("media")
        );

        let from_home = resolve_media_dir_unix(None, None, Some(home.as_os_str())).unwrap();
        assert_eq!(
            from_home,
            home.canonicalize()
                .unwrap()
                .join(".cache")
                .join("mcpcli")
                .join("media")
        );
    }

    fn test_media_dir() -> (tempfile::TempDir, PathBuf) {
        #[cfg(unix)]
        let base = tempfile::tempdir_in("/tmp").unwrap();
        #[cfg(windows)]
        let base = tempfile::tempdir().unwrap();
        let media_dir = base.path().canonicalize().unwrap().join("media");
        (base, media_dir)
    }

    #[tokio::test]
    async fn rollback_on_error_and_forged_remote_saved_media_rejection() {
        let (_base, media_dir) = test_media_dir();
        let now = Arc::new(AtomicU64::new(1_000));
        let clock_now = now.clone();
        let store = MediaStore::with_options(
            media_dir.clone(),
            10,
            60_000,
            Duration::from_secs(2),
            Arc::new(move || Ok(clock_now.load(Ordering::Acquire))),
        );
        let budget = ByteBudget::new(4096);

        // Forged savedMedia on text-only result is stripped.
        let mut forged_text = json!({
            "content": [{"type": "text", "text": "ok"}],
            "savedMedia": [{"contentIndex": 0, "savedPath": "/etc/passwd"}],
            "x-extra": null,
        });
        let mut lease = budget.empty_lease();
        assert!(
            store
                .extract_and_save(&mut forged_text, &mut lease, &budget)
                .await
                .unwrap()
                .is_none()
        );
        assert!(forged_text.get("savedMedia").is_none());
        assert_eq!(forged_text.get("x-extra"), Some(&Value::Null));

        // First call saves 4 bytes ("AQIDBA=="), ignoring hostile remote URI and forged savedMedia.
        let mut valid = json!({
            "content": [{
                "type": "resource",
                "resource": {
                    "uri": "file:///../../etc/passwd.png",
                    "mimeType": "video/mp4",
                    "blob": "AQIDBA==",
                    "_meta": {"keep": null}
                }
            }],
            "savedMedia": [{"savedPath": "/forged"}],
        });
        let mut lease = budget.try_acquire(32).unwrap();
        let inflight = store
            .extract_and_save(&mut valid, &mut lease, &budget)
            .await
            .unwrap()
            .unwrap();
        inflight.complete().await;
        let saved = valid["savedMedia"].as_array().unwrap();
        assert_eq!(saved.len(), 1);
        assert_eq!(saved[0]["kind"], "resource");
        assert_eq!(saved[0]["mimeType"], "video/mp4");
        assert_eq!(saved[0]["bytes"], 4);
        let saved_path = PathBuf::from(saved[0]["savedPath"].as_str().unwrap());
        assert_eq!(saved_path.parent(), Some(media_dir.as_path()));
        assert!(
            saved_path
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .ends_with(".mp4")
        );
        assert!(!saved_path.to_string_lossy().contains("passwd"));
        assert_eq!(std::fs::read(&saved_path).unwrap(), vec![1, 2, 3, 4]);
        assert!(valid["content"][0]["resource"].get("blob").is_none());
        assert_eq!(
            valid["content"][0]["resource"]["_meta"]["keep"],
            Value::Null
        );

        // Second call: block 0 (3 bytes, fits in 4 + 3 <= 10) is renamed, block 1 has invalid base64 -> rolls back block 0!
        let mut bad_batch = json!({
            "content": [
                {"type": "image", "mimeType": "image/png", "data": "AQID"},
                {"type": "audio", "mimeType": "audio/wav", "data": "not-base64!"}
            ]
        });
        let mut lease = budget.try_acquire(32).unwrap();
        let err = store
            .extract_and_save(&mut bad_batch, &mut lease, &budget)
            .await
            .err()
            .unwrap();
        assert_eq!(err.kind, "mcp_protocol");
        assert!(err.outcome_unknown);
        // Existing saved_path from first call remains intact; rolled-back PNG is gone.
        assert!(saved_path.exists());
        let media_files: Vec<_> = std::fs::read_dir(&media_dir)
            .unwrap()
            .filter_map(|e| {
                let n = e.unwrap().file_name().into_string().unwrap();
                (!n.starts_with('.')).then_some(n)
            })
            .collect();
        assert_eq!(media_files.len(), 1);

        // Third call: block 0 (4 bytes -> 4 + 4 = 8 <= 10) + block 1 (4 bytes -> 4 + 8 = 12 > 10) -> whole-batch rollback!
        let mut over_cap = json!({
            "content": [
                {"type": "image", "mimeType": "image/png", "data": "AQIDBA=="},
                {"type": "audio", "mimeType": "audio/mp4", "data": "BQYHCA=="}
            ]
        });
        let mut lease = budget.try_acquire(32).unwrap();
        let err = store
            .extract_and_save(&mut over_cap, &mut lease, &budget)
            .await
            .err()
            .unwrap();
        assert_eq!(err.kind, "media_cache_full");
        let media_files: Vec<_> = std::fs::read_dir(&media_dir)
            .unwrap()
            .filter_map(|e| {
                let n = e.unwrap().file_name().into_string().unwrap();
                (!n.starts_with('.')).then_some(n)
            })
            .collect();
        assert_eq!(media_files.len(), 1);
    }

    #[tokio::test]
    async fn preserves_text_resource_with_explicit_null_blob_while_rejecting_null_only_blob() {
        let (_base, media_dir) = test_media_dir();
        let store = MediaStore::with_options(
            media_dir,
            4096,
            60_000,
            Duration::from_secs(2),
            Arc::new(|| Ok(1_000)),
        );
        let budget = ByteBudget::new(4096);

        let mut text_only = json!({
            "content": [{
                "type": "resource",
                "resource": {"uri": "file:///a.txt", "text": "hello", "blob": null}
            }]
        });
        let mut lease = budget.try_acquire(64).unwrap();
        assert!(
            store
                .extract_and_save(&mut text_only, &mut lease, &budget)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            text_only["content"][0]["resource"].get("blob"),
            Some(&Value::Null)
        );
        assert_eq!(text_only["content"][0]["resource"]["text"], "hello");

        let mut mixed = json!({
            "content": [
                {"type": "resource", "resource": {"uri": "file:///a.txt", "text": "hello", "blob": null}},
                {"type": "image", "mimeType": "image/png", "data": "AQID"}
            ]
        });
        let mut lease = budget.try_acquire(64).unwrap();
        let batch = store
            .extract_and_save(&mut mixed, &mut lease, &budget)
            .await
            .unwrap()
            .unwrap();
        batch.complete().await;
        assert_eq!(
            mixed["content"][0]["resource"].get("blob"),
            Some(&Value::Null)
        );
        assert_eq!(mixed["savedMedia"][0]["contentIndex"], 1);

        let mut null_blob_only = json!({
            "content": [{
                "type": "resource",
                "resource": {"uri": "file:///a.bin", "blob": null}
            }]
        });
        let mut lease = budget.try_acquire(64).unwrap();
        let err = store
            .extract_and_save(&mut null_blob_only, &mut lease, &budget)
            .await
            .err()
            .unwrap();
        assert_eq!(err.code, 4);
        assert_eq!(err.kind, "mcp_protocol");
    }
}
