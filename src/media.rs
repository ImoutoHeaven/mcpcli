//! Local media persistence, MIME extension whitelist, and cross-process cache admission.
use crate::{
    budget::{ByteBudget, ByteLease, format_bytes},
    ipc::Failure,
    platform,
    runtime::Lock,
};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashSet, hash_map::RandomState},
    ffi::OsStr,
    hash::{BuildHasher, Hasher},
    io::{self, Read, Seek, SeekFrom, Write},
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
const MAX_INDEX_BYTES: usize = 4 << 20;

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
    [
        ("video/mp4", ".mp4"),
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
    ]
    .into_iter()
    .find(|(known, _)| essence.eq_ignore_ascii_case(known))
    .map_or(".bin", |(_, extension)| extension)
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

fn load_index(media_dir: &Path) -> Result<MediaIndex> {
    let path = media_dir.join(".index.json");
    let file = match platform::open_input(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(MediaIndex::default()),
        Err(error) => return Err(error).context("cannot open media cache index"),
    };
    let bytes = crate::json::read_limited(file, MAX_INDEX_BYTES)
        .context("cannot read media cache index")?;
    serde_json::from_slice(&bytes).context("invalid media cache index")
}

fn save_index(media_dir: &Path, index: &MediaIndex) -> Result<()> {
    let encoded = serde_json::to_vec(index)?;
    if encoded.len() > MAX_INDEX_BYTES {
        bail!("media cache index exceeds its {MAX_INDEX_BYTES} byte limit");
    }
    let tmp = media_dir.join(".index.tmp");
    let _ = remove_ignoring_benign(&tmp);
    let mut file = platform::new_private_file(&tmp)?;
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

#[derive(Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
enum Writeback {
    Writing,
    Complete { completed_ms: u64 },
    Aborted,
}

fn read_writeback(lock: &Lock) -> Result<Option<Writeback>> {
    let mut file = lock.file();
    file.seek(SeekFrom::Start(0))?;
    let mut bytes = Vec::new();
    file.take(257).read_to_end(&mut bytes)?;
    if bytes.is_empty() {
        return Ok(None);
    }
    if bytes.len() > 256 {
        bail!("media batch writeback record is too large; files remain protected");
    }
    serde_json::from_slice(&bytes)
        .map(Some)
        .context("invalid media batch writeback record; files remain protected")
}

fn write_writeback(lock: &Lock, state: Writeback) -> Result<()> {
    let encoded = serde_json::to_vec(&state)?;
    let mut file = lock.file();
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&encoded)?;
    file.set_len(encoded.len() as u64)?;
    file.flush()?;
    file.sync_data()?;
    Ok(())
}

fn gc_locked(
    media_dir: &Path,
    index: &mut MediaIndex,
    now_ms: u64,
    protection_ms: u64,
) -> Result<u64> {
    // The cache survives a reboot, while the monotonic clock starts over. Give
    // future records one fresh protection window and persist that normalization.
    for meta in index.files.values_mut() {
        meta.published_ms = meta.published_ms.min(now_ms);
    }
    let mut active_inflight_files = HashSet::new();
    let mut retired = Vec::new();
    for (batch_id, files) in &index.inflight {
        let lock_path = media_dir.join(format!(".inflight-{batch_id}.lock"));
        match Lock::try_acquire(&lock_path)? {
            None => {
                active_inflight_files.extend(files.iter().cloned());
            }
            Some(guard) => {
                // Exclusive ownership means no writer can still complete this
                // batch. A crash may leave Writing or a partial record; protect
                // that uncertain completion once, without blocking later calls.
                let protection_start = match read_writeback(&guard) {
                    Ok(Some(Writeback::Complete { completed_ms })) => {
                        Some(completed_ms.min(now_ms))
                    }
                    Ok(Some(Writeback::Writing)) | Err(_) => Some(now_ms),
                    Ok(Some(Writeback::Aborted)) | Ok(None) => None,
                };
                if let Some(start) = protection_start {
                    for file in files {
                        if let Some(meta) = index.files.get_mut(file) {
                            meta.published_ms = start;
                        }
                    }
                }
                drop(guard);
                retired.push((batch_id.clone(), lock_path));
            }
        }
    }
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
    for (batch_id, _) in &retired {
        index.inflight.remove(batch_id);
    }
    save_index(media_dir, index)?;
    for (_, path) in retired {
        let _ = remove_ignoring_benign(&path);
    }
    Ok(used_bytes)
}

const WRAPPER_KEY_BYTES: usize = "result".len() + "savedMedia".len();

fn media_memory_failure(budget: &ByteBudget) -> Failure {
    Failure::new(
        1,
        "memory_limit_exceeded",
        format!("saving MCP media exceeded the {} active memory limit; side effects may have occurred, the call was not retried",
            format_bytes(budget.limit())),
    ).uncertain(true)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SavedMediaRecord<'a> {
    content_index: usize,
    kind: &'a str,
    saved_path: &'a str,
    mime_type: &'a str,
    bytes: u64,
}

impl SavedMediaRecord<'_> {
    fn into_value(self) -> Value {
        let mut record = Map::new();
        record.insert("contentIndex".into(), self.content_index.into());
        record.insert("kind".into(), Value::String(self.kind.to_owned()));
        record.insert(
            "savedPath".into(),
            Value::String(self.saved_path.to_owned()),
        );
        record.insert("mimeType".into(), Value::String(self.mime_type.to_owned()));
        record.insert("bytes".into(), self.bytes.into());
        Value::Object(record)
    }
}

fn wrap_saved_media(result: &mut Value, saved_media: Vec<Value>) {
    let mut output = serde_json::Map::new();
    output.insert("result".into(), result.take());
    output.insert("savedMedia".into(), Value::Array(saved_media));
    *result = Value::Object(output);
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
        let mut index = load_index(&media_dir)?;
        gc_locked(&media_dir, &mut index, now_ms, self.protection_ms)
    }

    pub async fn extract_and_save(
        &self,
        result: &mut Value,
        result_lease: &mut ByteLease,
        budget: &Arc<ByteBudget>,
    ) -> std::result::Result<Option<InflightBatch>, Failure> {
        let mut records_lease = budget
            .try_acquire(WRAPPER_KEY_BYTES)
            .ok_or_else(|| media_memory_failure(budget))?;
        let Some(root) = result.as_object_mut() else {
            wrap_saved_media(result, Vec::new());
            result_lease.merge(records_lease);
            return Ok(None);
        };

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
            wrap_saved_media(result, Vec::new());
            result_lease.merge(records_lease);
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
        let media_dir_text = media_dir.to_str().ok_or_else(|| Failure::new(1, "media_cache",
            "local media cache path cannot be represented as UTF-8; side effects may have occurred, the call was not retried").uncertain(true))?;
        // ASCII generated filenames fit within 96 bytes. PathBuf joining may
        // double its capacity; reserve before constructing or copying paths.
        let path_bytes = media_dir_text
            .len()
            .saturating_add(1 + 96)
            .saturating_mul(2);
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
        let mut index = load_index(&media_dir).map_err(|error| {
            Failure::new(1, "media_cache", format!("cannot read media cache index: {error:#}; side effects may have occurred, the call was not retried")).uncertain(true)
        })?;
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

        let mut created_paths_lease = budget.empty_lease();
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
                                .unwrap_or("application/octet-stream");
                            ("image", mime, raw)
                        }
                        Some("audio") => {
                            let Some(raw) = block_obj.remove("data") else {
                                continue;
                            };
                            let mime = block_obj
                                .get("mimeType")
                                .and_then(Value::as_str)
                                .unwrap_or("application/octet-stream");
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
                                .unwrap_or("application/octet-stream");
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

                let (decode_lease, decoded) = match decode_res {
                    Ok((decoded, lease)) => (lease, decoded),
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

                if !created_paths_lease.try_grow(path_bytes) {
                    return Err(media_memory_failure(budget));
                }
                // tmp path, final path and replacement rollback-path copy may
                // coexist; the retained rollback list has its own allowance.
                let _path_workspace = budget
                    .try_acquire(path_bytes.saturating_mul(3))
                    .ok_or_else(|| media_memory_failure(budget))?;
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

                let ext = extension_for_mime(mime_type);
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
                let record = SavedMediaRecord {
                    content_index: content_idx,
                    kind,
                    saved_path: final_path
                        .to_str()
                        .expect("UTF-8 directory and ASCII filename"),
                    mime_type,
                    bytes: decoded_len,
                };
                let record_bytes = crate::json::serialized_len(&record).map_err(|error| {
                    Failure::new(
                        1,
                        "media_cache",
                        format!("cannot encode local media record: {error}"),
                    )
                    .uncertain(true)
                })?;
                if !records_lease.try_grow(record_bytes) {
                    return Err(media_memory_failure(budget));
                }
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
                saved_entries.push(record.into_value());
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
                    clock: self.clock.clone(),
                    lock: Some(batch_lock),
                },
            ))
        })();

        match outcome {
            Ok((saved_entries, inflight)) => {
                wrap_saved_media(result, saved_entries);
                result_lease.merge(records_lease);
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
    clock: ClockFn,
    lock: Option<Lock>,
}

impl InflightBatch {
    pub fn begin_writeback(&self) -> Result<()> {
        write_writeback(
            self.lock.as_ref().expect("active batch"),
            Writeback::Writing,
        )
        .context("cannot record media writeback start")
    }

    pub async fn complete(mut self) -> Result<()> {
        let completed_ms = (self.clock)().context("cannot read media completion clock")?;
        write_writeback(
            self.lock.as_ref().expect("active batch"),
            Writeback::Complete { completed_ms },
        )
        .context("cannot record media writeback completion; files remain protected")?;
        drop(self.lock.take());
        Ok(())
    }

    pub async fn abort(mut self) -> Result<()> {
        write_writeback(
            self.lock.as_ref().expect("active batch"),
            Writeback::Aborted,
        )
        .context("cannot record interrupted media writeback; files remain protected")?;
        drop(self.lock.take());
        Ok(())
    }
}

impl Drop for InflightBatch {
    fn drop(&mut self) {
        // GC reads the durable writeback state after this OS lock is released.
        drop(self.lock.take());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn saved_media_wrapper_moves_existing_string_buffers() {
        let mut result = json!({"content": [{"type": "text", "text": "x".repeat(128 << 10)}]});
        let text_ptr = result["content"][0]["text"].as_str().unwrap().as_ptr();
        let saved = vec![json!({"savedPath": "a".repeat(4096)})];
        let path_ptr = saved[0]["savedPath"].as_str().unwrap().as_ptr();
        wrap_saved_media(&mut result, saved);
        assert_eq!(
            result["result"]["content"][0]["text"]
                .as_str()
                .unwrap()
                .as_ptr(),
            text_ptr
        );
        assert_eq!(
            result["savedMedia"][0]["savedPath"]
                .as_str()
                .unwrap()
                .as_ptr(),
            path_ptr
        );
    }

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
    async fn completion_record_survives_directory_contention_and_clock_failure() {
        let (_base, media_dir) = test_media_dir();
        let now = Arc::new(AtomicU64::new(1_000));
        let clock_now = now.clone();
        let store = MediaStore::with_options(
            media_dir.clone(),
            4096,
            60_000,
            Duration::from_millis(25),
            Arc::new(move || {
                let now = clock_now.load(Ordering::Acquire);
                if now == u64::MAX {
                    Err(io::Error::other("clock unavailable"))
                } else {
                    Ok(now)
                }
            }),
        );
        let budget = ByteBudget::new(4096);
        let mut result =
            json!({"content": [{"type":"image","mimeType":"image/png","data":"AQID"}]});
        let mut lease = budget.empty_lease();
        let batch = store
            .extract_and_save(&mut result, &mut lease, &budget)
            .await
            .unwrap()
            .unwrap();
        let path = PathBuf::from(result["savedMedia"][0]["savedPath"].as_str().unwrap());
        batch.begin_writeback().unwrap();
        now.store(80_000, Ordering::Release);
        let dir_lock = Lock::try_acquire(&media_dir.join(".lock"))
            .unwrap()
            .unwrap();
        // Completing a slow write never waits for the directory lock.
        batch.complete().await.unwrap();
        drop(dir_lock);
        now.store(139_999, Ordering::Release);
        assert_eq!(store.run_gc().await.unwrap(), 3);
        assert!(path.exists());
        now.store(140_000, Ordering::Release);
        assert_eq!(store.run_gc().await.unwrap(), 0);
        assert!(!path.exists());

        let mut result =
            json!({"content": [{"type":"image","mimeType":"image/png","data":"AQID"}]});
        let batch = store
            .extract_and_save(&mut result, &mut lease, &budget)
            .await
            .unwrap()
            .unwrap();
        let path = PathBuf::from(result["savedMedia"][0]["savedPath"].as_str().unwrap());
        batch.begin_writeback().unwrap();
        now.store(u64::MAX, Ordering::Release);
        assert!(
            batch
                .complete()
                .await
                .unwrap_err()
                .to_string()
                .contains("completion clock")
        );
        // With no writer left, recover Writing with one conservative window.
        now.store(300_000, Ordering::Release);
        assert_eq!(store.run_gc().await.unwrap(), 3);
        assert!(load_index(&media_dir).unwrap().inflight.is_empty());
        assert!(path.exists());
        now.store(359_999, Ordering::Release);
        assert_eq!(store.run_gc().await.unwrap(), 3);
        now.store(360_000, Ordering::Release);
        assert_eq!(store.run_gc().await.unwrap(), 0);
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn inactive_writeback_recovers_without_blocking_other_batches() {
        for state in ["writing", "incomplete", "aborted"] {
            let (_base, media_dir) = test_media_dir();
            let now = Arc::new(AtomicU64::new(1_000));
            let clock_now = now.clone();
            let store = MediaStore::with_options(
                media_dir.clone(),
                6,
                60_000,
                Duration::from_secs(1),
                Arc::new(move || Ok(clock_now.load(Ordering::Acquire))),
            );
            let budget = ByteBudget::new(16 * 1024);
            let mut result =
                json!({"content":[{"type":"image","mimeType":"image/png","data":"AQID"}]});
            let mut lease = budget.empty_lease();
            let batch = store
                .extract_and_save(&mut result, &mut lease, &budget)
                .await
                .unwrap()
                .unwrap();
            let path = PathBuf::from(result["savedMedia"][0]["savedPath"].as_str().unwrap());
            batch.begin_writeback().unwrap();
            if state == "incomplete" {
                // An interrupted journal update may leave only its first byte.
                let mut file = batch.lock.as_ref().unwrap().file();
                file.seek(SeekFrom::Start(0)).unwrap();
                file.write_all(b"{").unwrap();
                file.set_len(1).unwrap();
                file.flush().unwrap();
            }
            if state == "aborted" {
                batch.abort().await.unwrap();
                now.store(300_000, Ordering::Release);
                assert_eq!(store.run_gc().await.unwrap(), 0);
                assert!(!path.exists());
                continue;
            }
            drop(batch);
            now.store(300_000, Ordering::Release);
            assert_eq!(store.run_gc().await.unwrap(), 3);
            assert!(load_index(&media_dir).unwrap().inflight.is_empty());
            assert!(path.exists());
            let mut later =
                json!({"content":[{"type":"image","mimeType":"image/png","data":"BAUG"}]});
            let mut later_lease = budget.empty_lease();
            store
                .extract_and_save(&mut later, &mut later_lease, &budget)
                .await
                .unwrap()
                .unwrap()
                .complete()
                .await
                .unwrap();
            now.store(359_999, Ordering::Release);
            assert_eq!(store.run_gc().await.unwrap(), 6);
            assert!(path.exists());
            now.store(360_000, Ordering::Release);
            assert_eq!(store.run_gc().await.unwrap(), 0);
            assert!(!path.exists());
        }
    }

    #[tokio::test]
    async fn future_monotonic_timestamps_get_one_protection_window() {
        for pending_completion in [false, true] {
            let (_base, media_dir) = test_media_dir();
            let now = Arc::new(AtomicU64::new(500_000));
            let clock_now = now.clone();
            let store = MediaStore::with_options(
                media_dir.clone(),
                3,
                60_000,
                Duration::from_secs(1),
                Arc::new(move || Ok(clock_now.load(Ordering::Acquire))),
            );
            let budget = ByteBudget::new(16 * 1024);
            let mut result =
                json!({"content":[{"type":"image","mimeType":"image/png","data":"AQID"}]});
            let mut lease = budget.empty_lease();
            store
                .extract_and_save(&mut result, &mut lease, &budget)
                .await
                .unwrap()
                .unwrap()
                .complete()
                .await
                .unwrap();
            let path = PathBuf::from(result["savedMedia"][0]["savedPath"].as_str().unwrap());
            if !pending_completion {
                assert_eq!(store.run_gc().await.unwrap(), 3);
            }
            now.store(1_000, Ordering::Release);
            assert_eq!(store.run_gc().await.unwrap(), 3);
            assert!(
                load_index(&media_dir)
                    .unwrap()
                    .files
                    .values()
                    .all(|meta| meta.published_ms == 1_000)
            );
            now.store(60_999, Ordering::Release);
            assert_eq!(store.run_gc().await.unwrap(), 3);
            assert!(path.exists());
            now.store(61_000, Ordering::Release);
            assert_eq!(store.run_gc().await.unwrap(), 0);
            assert!(!path.exists());
        }
    }

    #[tokio::test]
    async fn corrupt_or_unreadable_index_never_runs_destructive_gc() {
        let (_base, media_dir) = test_media_dir();
        let store = MediaStore::with_options(
            media_dir.clone(),
            4096,
            60_000,
            Duration::from_secs(1),
            Arc::new(|| Ok(500_000)),
        );
        store.directory().unwrap();
        let saved = media_dir.join("existing.png");
        std::fs::write(&saved, b"keep").unwrap();
        let index_path = media_dir.join(".index.json");
        for contents in [b"{bad json".to_vec(), vec![b' '; MAX_INDEX_BYTES + 1]] {
            std::fs::write(&index_path, &contents).unwrap();
            assert!(store.run_gc().await.is_err());
            assert_eq!(std::fs::read(&saved).unwrap(), b"keep");
            assert_eq!(std::fs::read(&index_path).unwrap(), contents);
        }
        std::fs::remove_file(&index_path).unwrap();
        std::fs::create_dir(&index_path).unwrap();
        assert!(store.run_gc().await.is_err());
        assert!(saved.exists());
    }

    #[test]
    fn index_read_and_write_share_the_same_limit() {
        let (_base, media_dir) = test_media_dir();
        std::fs::create_dir(&media_dir).unwrap();
        let mut index = MediaIndex::default();
        index
            .files
            .insert(String::new(), FileMeta { published_ms: 1 });
        let overhead = serde_json::to_vec(&index).unwrap().len();
        index.files.clear();
        index.files.insert(
            "x".repeat(MAX_INDEX_BYTES - overhead),
            FileMeta { published_ms: 1 },
        );
        save_index(&media_dir, &index).unwrap();
        let valid = std::fs::read(media_dir.join(".index.json")).unwrap();
        assert_eq!(valid.len(), MAX_INDEX_BYTES);
        assert_eq!(load_index(&media_dir).unwrap().files.len(), 1);
        index
            .files
            .insert("extra".into(), FileMeta { published_ms: 2 });
        assert!(save_index(&media_dir, &index).is_err());
        assert_eq!(std::fs::read(media_dir.join(".index.json")).unwrap(), valid);
        assert!(!media_dir.join(".index.tmp").exists());
    }

    #[tokio::test]
    async fn saved_records_hold_their_budget_and_admission_rolls_back_only_this_batch() {
        let (base, _) = test_media_dir();
        let media_dir = base.path().canonicalize().unwrap().join("media-é");
        let store = MediaStore::with_options(
            media_dir.clone(),
            4096,
            60_000,
            Duration::from_secs(2),
            Arc::new(|| Ok(1_000)),
        );
        let source = json!({"content":[{"type":"image", "mimeType":"image/png", "data":"AQID"}]});
        let source_bytes = crate::json::serialized_len(&source).unwrap();
        let initial_budget = ByteBudget::new(16 << 10);
        let mut initial_lease = initial_budget.try_acquire(source_bytes).unwrap();
        let mut initial = source.clone();
        store
            .extract_and_save(&mut initial, &mut initial_lease, &initial_budget)
            .await
            .unwrap()
            .unwrap()
            .complete()
            .await
            .unwrap();
        let existing = PathBuf::from(initial["savedMedia"][0]["savedPath"].as_str().unwrap());
        drop(initial);
        drop(initial_lease);
        assert_eq!(initial_budget.used(), 0);

        let sample_path = media_dir.join(format!("{}.png", "0".repeat(32)));
        let sample_record = SavedMediaRecord {
            content_index: 0,
            kind: "image",
            saved_path: sample_path.to_str().unwrap(),
            mime_type: "image/png",
            bytes: 3,
        };
        let record_bytes = crate::json::serialized_len(&sample_record).unwrap();
        let path_bytes = 2 * (media_dir.to_str().unwrap().len() + 1 + 96);
        let retained = source_bytes - 4 + WRAPPER_KEY_BYTES + record_bytes;
        let peak = retained + 4 * path_bytes;
        for limit in [peak - 1, peak] {
            let budget = ByteBudget::new(limit);
            let mut lease = budget.try_acquire(source_bytes).unwrap();
            let mut result = source.clone();
            let outcome = store
                .extract_and_save(&mut result, &mut lease, &budget)
                .await;
            if limit < peak {
                let error = outcome
                    .err()
                    .expect("record admission must fail before publication");
                assert_eq!(error.kind, "memory_limit_exceeded");
                assert!(error.outcome_unknown);
                assert_eq!(budget.used(), source_bytes - 4);
            } else {
                outcome.unwrap().unwrap().complete().await.unwrap();
                assert_eq!(budget.used(), retained);
                assert_eq!(lease.bytes(), retained);
                assert_eq!(result["savedMedia"][0]["mimeType"], "image/png");
                let path = Path::new(result["savedMedia"][0]["savedPath"].as_str().unwrap());
                assert_eq!(path.parent(), Some(media_dir.as_path()));
                assert_eq!(std::fs::read(path).unwrap(), [1, 2, 3]);
            }
            assert!(
                existing.exists(),
                "a previous protected batch must survive admission failure"
            );
            let entries: Vec<_> = std::fs::read_dir(&media_dir)
                .unwrap()
                .map(|entry| entry.unwrap().file_name())
                .collect();
            assert!(
                !entries
                    .iter()
                    .any(|name| name.to_str().unwrap().starts_with(".tmp-"))
            );
            assert_eq!(
                entries
                    .iter()
                    .filter(|name| !name.to_str().unwrap().starts_with('.'))
                    .count(),
                if limit < peak { 1 } else { 2 }
            );
            drop(result);
            drop(lease);
            assert_eq!(budget.used(), 0);
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn non_utf8_media_directory_returns_a_cache_error_before_publication() {
        use std::os::unix::ffi::OsStringExt;
        let (base, _) = test_media_dir();
        let media_dir = base.path().join(std::ffi::OsString::from_vec(vec![
            b'm', b'e', b'd', b'i', b'a', b'-', 0xff,
        ]));
        let store = MediaStore::with_options(
            media_dir.clone(),
            4096,
            60_000,
            Duration::from_secs(2),
            Arc::new(|| Ok(1_000)),
        );
        let budget = ByteBudget::new(4096);
        let mut result =
            json!({"content":[{"type":"image", "mimeType":"image/png", "data":"AQID"}]});
        let source_bytes = crate::json::serialized_len(&result).unwrap();
        let mut lease = budget.try_acquire(source_bytes).unwrap();
        let error = store
            .extract_and_save(&mut result, &mut lease, &budget)
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind, "media_cache");
        assert!(error.outcome_unknown);
        assert_eq!(result["content"][0]["data"], "AQID");
        assert_eq!(budget.used(), source_bytes);
        if let Ok(entries) = std::fs::read_dir(&media_dir) {
            assert_eq!(entries.count(), 0);
        }
        drop(result);
        drop(lease);
        assert_eq!(budget.used(), 0);
    }

    #[tokio::test]
    async fn rollback_on_error_and_remote_saved_media_preservation() {
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

        // Remote fields stay inside result even when no local media is saved.
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
        assert_eq!(forged_text["savedMedia"], json!([]));
        assert_eq!(
            forged_text["result"]["savedMedia"][0]["savedPath"],
            "/etc/passwd"
        );
        assert_eq!(forged_text["result"].get("x-extra"), Some(&Value::Null));

        // Local filenames and saved records remain separate from remote metadata.
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
        inflight.complete().await.unwrap();
        assert_eq!(
            valid["result"]["savedMedia"],
            json!([{"savedPath": "/forged"}])
        );
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
        assert!(
            valid["result"]["content"][0]["resource"]
                .get("blob")
                .is_none()
        );
        assert_eq!(
            valid["result"]["content"][0]["resource"]["_meta"]["keep"],
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
            text_only["result"]["content"][0]["resource"].get("blob"),
            Some(&Value::Null)
        );
        assert_eq!(
            text_only["result"]["content"][0]["resource"]["text"],
            "hello"
        );

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
        batch.complete().await.unwrap();
        assert_eq!(
            mixed["result"]["content"][0]["resource"].get("blob"),
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
