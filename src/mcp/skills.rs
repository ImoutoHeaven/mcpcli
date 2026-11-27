//! Remote MCP Skills client (SEP-2640): capability gating, selector resolution,
//! clean-URI manifest validation, integrity verification, and single-retry self-healing.
use std::{collections::HashSet, time::Duration};

use rmcp::model::{
    ErrorCode, GetSkillRequestParams, PaginatedRequestParams, ProtocolVersion,
    ReadResourceDirectoryRequestParams, ReadResourceRequestParams, ServerPeerInfo, Skill,
    SkillResource, SkillResources, skill_name_from_uri,
};
use serde_json::{Map, Value};
use tokio::time::{Instant, timeout};

use super::{
    McpConnection, McpOpError as Failure, busy_capture, observed_result,
    routing_header_payload_bound, service_failure,
};
use crate::{
    budget::{ByteLease, format_bytes},
    media::{DecodeError, InflightBatch, MediaStore, decode_base64_leased},
};

pub const MAX_SKILL_PAGES: usize = 64;
pub const MAX_REMOTE_SKILLS: usize = 1024;
pub const MAX_SKILL_CONTENT_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_SELECTOR_BYTES: usize = 2048;

pub(super) fn check_skills_capability(
    info: Option<&ServerPeerInfo>,
    require_dir: bool,
) -> std::result::Result<(), Failure> {
    let Some(info) = info else {
        return Err(Failure::new(
            4,
            "mcp_protocol",
            "MCP server info is missing",
        ));
    };
    if info.protocol_version < ProtocolVersion::V_2026_07_28 {
        return Err(Failure::new(
            4,
            "mcp_protocol",
            format!(
                "MCP server protocol version {} does not support skills (requires >= 2026-07-28)",
                info.protocol_version
            ),
        ));
    }
    if info.capabilities.resources.is_none() {
        return Err(Failure::new(
            4,
            "mcp_protocol",
            "MCP server does not advertise the resources capability required for skills",
        ));
    }
    if !info.capabilities.supports_skills() {
        return Err(Failure::new(
            4,
            "mcp_protocol",
            "MCP server does not advertise the io.modelcontextprotocol/skills extension",
        ));
    }
    if require_dir && !info.capabilities.supports_directory_read() {
        return Err(Failure::new(
            4,
            "mcp_protocol",
            "MCP server does not advertise skills directoryRead support",
        ));
    }
    Ok(())
}

/// Returns `Ok(true)` for a full skill URI (`scheme://.../SKILL.md`) and
/// `Ok(false)` for a short name or `<prefix>/<name>` selector.
pub fn validate_selector(selector: &str) -> std::result::Result<bool, Failure> {
    if selector.is_empty()
        || selector.len() > MAX_SELECTOR_BYTES
        || selector.bytes().any(|b| b < 0x20 || b == 0x7f)
    {
        return Err(Failure::new(
            2,
            "invalid_skill_uri",
            "Remote skill selector must be 1..=2048 bytes without ASCII control characters",
        ));
    }
    if selector.contains("://") {
        if skill_name_from_uri(selector).is_none() {
            return Err(Failure::new(
                2,
                "invalid_skill_uri",
                format!(
                    "Invalid remote skill URI {selector:?}: must end with '/<name>/SKILL.md' without '?' or '#'"
                ),
            ));
        }
        return Ok(true);
    }
    if selector.bytes().any(|b| matches!(b, b'?' | b'#' | b'\\')) {
        return Err(Failure::new(
            2,
            "invalid_skill_uri",
            format!("Invalid remote skill selector {selector:?}"),
        ));
    }
    for segment in selector.split('/') {
        if segment.is_empty() || segment == "." || segment == ".." {
            return Err(Failure::new(
                2,
                "invalid_skill_uri",
                format!("Invalid remote skill selector {selector:?}"),
            ));
        }
    }
    Ok(false)
}

fn skill_root(skill_uri: &str) -> std::result::Result<&str, Failure> {
    skill_uri.strip_suffix("/SKILL.md").ok_or_else(|| {
        Failure::new(
            4,
            "skill_integrity",
            format!("Skill URI {skill_uri:?} does not end with '/SKILL.md'"),
        )
    })
}

fn validate_static_manifest(
    skill: &Skill,
) -> std::result::Result<Vec<(String, SkillResource)>, Failure> {
    let SkillResources::Files(files) = &skill.resources else {
        return Ok(Vec::new());
    };
    let mut entries = Vec::with_capacity(files.len());
    for entry in files {
        let rel_path =
            crate::skills::validate_manifest_entry_uri(&skill.uri, &entry.uri).map_err(|err| {
                Failure::new(
                    4,
                    "skill_integrity",
                    format!(
                        "Skill {:?} manifest entry {:?} failed URI validation: {err}",
                        skill.uri, entry.uri
                    ),
                )
            })?;
        entries.push((rel_path, entry.clone()));
    }
    Ok(entries)
}

fn frontmatter_map(skill: &Skill) -> std::result::Result<Map<String, Value>, Failure> {
    match serde_json::to_value(&skill.frontmatter) {
        Ok(Value::Object(map)) => Ok(map),
        _ => Err(Failure::new(
            4,
            "skill_integrity",
            "Skill frontmatter could not be represented as a JSON object",
        )),
    }
}

fn is_invalid_params(failure: &Failure) -> bool {
    failure
        .rpc_error
        .as_deref()
        .and_then(|err| err.get("code"))
        .and_then(Value::as_i64)
        == Some(i64::from(ErrorCode::INVALID_PARAMS.0))
}

fn remaining_budget(deadline: Instant, context: &str) -> std::result::Result<Duration, Failure> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(Failure::new(5, "timeout", format!("{context} timed out")));
    }
    Ok(remaining)
}

async fn walk_skills(
    conn: &McpConnection,
    _server: &str,
    deadline: Instant,
) -> std::result::Result<(Vec<Skill>, Vec<Value>, ByteLease), Failure> {
    let budget = remaining_budget(deadline, "Remote skills listing")?;
    if conn.termination.is_terminated() {
        conn.abort_service();
        let err = conn
            .attach_stderr_tail(conn.termination.failure(false))
            .await;
        return Err(Failure::from_error(err, false));
    }
    conn.termination.clear_transient();
    let _capture = conn
        .ingress
        .capture()
        .ok_or_else(|| Failure::from_error(busy_capture(), false))?;

    let listing = async {
        let mut lease = conn.limits.budget.empty_lease();
        let mut cursors_lease = conn.limits.budget.empty_lease();
        let mut seen_uris_lease = conn.limits.budget.empty_lease();
        let mut raw_skills = Vec::new();
        let mut typed_skills = Vec::new();
        let mut cursors = HashSet::new();
        let mut seen_uris = HashSet::new();
        let mut cursor: Option<String> = None;
        let mut bytes = 0usize;

        for _ in 0..MAX_SKILL_PAGES {
            let mut params = PaginatedRequestParams::default();
            params.cursor = cursor;
            let list_fut = conn.peer.list_skills(Some(params));
            let mut page = tokio::select! {
                biased;
                () = conn.termination.cancelled() => {
                    conn.abort_service();
                    return Err(Failure::from_error(conn.termination.failure(false), false));
                }
                res = list_fut => res.map_err(|e| {
                    Failure::from_error(
                        service_failure(e, false, &conn.ingress, &conn.termination),
                        false,
                    )
                })?,
            };
            let (_, mut raw_page, page_lease) =
                observed_result(conn.ingress.take(), Some("skills/list"), false)
                    .map_err(|e| Failure::from_error(e, false))?;
            lease.merge(page_lease);

            let page_bytes = crate::json::serialized_len(&raw_page).map_err(|e| {
                Failure::new(4, "mcp_protocol", format!("Invalid skills/list page: {e}"))
            })?;
            bytes = bytes.saturating_add(page_bytes);
            if bytes > conn.limits.max_tool_catalog_bytes {
                return Err(Failure::new(
                    4,
                    "mcp_protocol",
                    format!(
                        "Remote skills catalog exceeds the {} size limit",
                        format_bytes(conn.limits.max_tool_catalog_bytes)
                    ),
                ));
            }

            let items = raw_page
                .get_mut("skills")
                .and_then(Value::as_array_mut)
                .map(std::mem::take)
                .ok_or_else(|| {
                    Failure::new(
                        4,
                        "mcp_protocol",
                        "skills/list response missing 'skills' array",
                    )
                })?;

            for item in items {
                let skill: Skill = serde_json::from_value(item.clone()).map_err(|e| {
                    Failure::new(
                        4,
                        "mcp_protocol",
                        format!("Invalid skill entry in skills/list: {e}"),
                    )
                })?;
                if !seen_uris_lease.try_grow(skill.uri.len()) {
                    return Err(Failure::new(
                        1,
                        "memory_limit_exceeded",
                        "Remote skill URIs exceed the active memory limit",
                    ));
                }
                if !seen_uris.insert(skill.uri.clone()) {
                    return Err(Failure::new(
                        4,
                        "mcp_protocol",
                        format!("Remote skills catalog listed duplicate URI {:?}", skill.uri),
                    ));
                }
                if typed_skills.len() >= MAX_REMOTE_SKILLS {
                    return Err(Failure::new(
                        4,
                        "mcp_protocol",
                        format!(
                            "Remote skills catalog exceeds the {MAX_REMOTE_SKILLS} skill limit"
                        ),
                    ));
                }
                validate_static_manifest(&skill)?;
                typed_skills.push(skill);
                raw_skills.push(item);
            }

            cursor = page.next_cursor.take();
            drop(page);
            if let Some(c) = &cursor {
                if !cursors_lease.try_grow(c.len()) {
                    return Err(Failure::new(
                        1,
                        "memory_limit_exceeded",
                        "Remote skills cursors exceed the active memory limit",
                    ));
                }
                if !cursors.insert(c.clone()) {
                    return Err(Failure::new(
                        4,
                        "mcp_protocol",
                        "Remote skills catalog repeated a pagination cursor",
                    ));
                }
            } else {
                return Ok((typed_skills, raw_skills, lease));
            }
        }
        Err(Failure::new(
            4,
            "mcp_protocol",
            format!("Remote skills catalog exceeds the {MAX_SKILL_PAGES}-page pagination limit"),
        ))
    };

    match timeout(budget, listing).await {
        Ok(Ok(res)) => Ok(res),
        Ok(Err(err)) => Err(err),
        Err(_) => Err(Failure::new(
            5,
            "timeout",
            "Remote skills listing timed out",
        )),
    }
}

async fn get_skill_once(
    conn: &McpConnection,
    _server: &str,
    uri: &str,
    deadline: Instant,
) -> std::result::Result<Skill, Failure> {
    let budget = remaining_budget(deadline, "Remote skill lookup")?;
    if conn.termination.is_terminated() {
        conn.abort_service();
        let err = conn
            .attach_stderr_tail(conn.termination.failure(false))
            .await;
        return Err(Failure::from_error(err, false));
    }
    conn.termination.clear_transient();
    let _capture = conn
        .ingress
        .capture()
        .ok_or_else(|| Failure::from_error(busy_capture(), false))?;

    let req = async {
        let fut = conn
            .peer
            .get_skill(GetSkillRequestParams::new(uri.to_owned()));
        let res = tokio::select! {
            biased;
            () = conn.termination.cancelled() => {
                conn.abort_service();
                return Err(Failure::from_error(conn.termination.failure(false), false));
            }
            res = fut => res.map_err(|e| {
                Failure::from_error(
                    service_failure(e, false, &conn.ingress, &conn.termination),
                    false,
                )
            })?,
        };
        let _ = observed_result(conn.ingress.take(), Some("skills/get"), false)
            .map_err(|e| Failure::from_error(e, false))?;
        validate_static_manifest(&res.skill)?;
        Ok(res.skill)
    };

    match timeout(budget, req).await {
        Ok(res) => res,
        Err(_) => Err(Failure::new(5, "timeout", "Remote skill lookup timed out")),
    }
}

async fn resolve_remote_skill(
    conn: &McpConnection,
    server: &str,
    selector: &str,
    deadline: Instant,
) -> std::result::Result<(Skill, bool), Failure> {
    let is_full_uri = validate_selector(selector)?;
    if is_full_uri {
        let skill = get_skill_once(conn, server, selector, deadline).await?;
        return Ok((skill, false));
    }
    let candidate_uri = format!("skill://{selector}/SKILL.md");
    match get_skill_once(conn, server, &candidate_uri, deadline).await {
        Ok(skill) => Ok((skill, false)),
        Err(err) if is_invalid_params(&err) => {
            let (skills, _, _lease) = walk_skills(conn, server, deadline).await?;
            let suffix = format!("/{selector}/SKILL.md");
            let mut matches: Vec<Skill> = skills
                .into_iter()
                .filter(|skill| {
                    if selector.contains('/') {
                        skill.uri.ends_with(&suffix)
                    } else {
                        skill.frontmatter.name == selector
                    }
                })
                .collect();
            match matches.len() {
                0 => Err(Failure::new(
                    4,
                    "mcp_protocol",
                    format!("Remote skill {selector:?} was not found on server {server:?}"),
                )),
                1 => Ok((matches.pop().expect("checked len 1"), true)),
                _ => {
                    let candidates: Vec<Value> =
                        matches.into_iter().map(|s| Value::String(s.uri)).collect();
                    let mut fail = Failure::new(
                        2,
                        "ambiguous_skill",
                        format!(
                            "Remote skill selector {selector:?} matched multiple skills on server {server:?}"
                        ),
                    );
                    let mut data = Map::new();
                    data.insert("candidates".into(), Value::Array(candidates));
                    fail.data = Some(Box::new(Value::Object(data)));
                    Err(fail)
                }
            }
        }
        Err(err) => Err(err),
    }
}

async fn read_resource_single(
    conn: &McpConnection,
    _server: &str,
    uri: &str,
    deadline: Instant,
) -> std::result::Result<(Value, ByteLease), Failure> {
    let budget = remaining_budget(deadline, "Remote skill resource read")?;
    let copies = if conn.http { 2 } else { 1 };
    let header_bytes = if conn.http {
        routing_header_payload_bound(uri)
    } else {
        0
    };
    let _param_lease = conn
        .limits
        .budget
        .try_acquire(
            uri.len()
                .saturating_mul(copies)
                .saturating_add(header_bytes.saturating_mul(5)),
        )
        .ok_or_else(|| {
            Failure::new(
                1,
                "memory_limit_exceeded",
                "Resource URI and routing headers exceed the active memory limit",
            )
        })?;
    if conn.termination.is_terminated() {
        conn.abort_service();
        let err = conn
            .attach_stderr_tail(conn.termination.failure(false))
            .await;
        return Err(Failure::from_error(err, false));
    }
    conn.termination.clear_transient();
    let _capture = conn
        .ingress
        .capture()
        .ok_or_else(|| Failure::from_error(busy_capture(), false))?;

    let read = async {
        let fut = conn
            .peer
            .read_resource_once(ReadResourceRequestParams::new(uri.to_owned()));
        let res = tokio::select! {
            biased;
            () = conn.termination.cancelled() => {
                conn.abort_service();
                return Err(Failure::from_error(conn.termination.failure(false), false));
            }
            res = fut => res.map_err(|e| {
                Failure::from_error(
                    service_failure(e, false, &conn.ingress, &conn.termination),
                    false,
                )
            })?,
        };
        if !matches!(res, rmcp::model::ReadResourceResponse::Complete(_)) {
            return Err(Failure::new(
                4,
                "mcp_protocol",
                "Remote skill resource requires unsupported interactive input",
            ));
        }
        let (_, mut raw, lease) =
            observed_result(conn.ingress.take(), Some("resources/read"), false)
                .map_err(|e| Failure::from_error(e, false))?;
        let Some(contents) = raw.get_mut("contents").and_then(Value::as_array_mut) else {
            return Err(Failure::new(
                4,
                "mcp_protocol",
                "resources/read response is missing 'contents' array",
            ));
        };
        if contents.len() != 1 {
            return Err(Failure::new(
                4,
                "mcp_protocol",
                format!(
                    "resources/read returned {} content items; expected exactly 1",
                    contents.len()
                ),
            ));
        }
        let item = contents.remove(0);
        if item.get("uri").and_then(Value::as_str) != Some(uri) {
            return Err(Failure::new(
                4,
                "skill_integrity",
                format!("resources/read returned mismatched URI for {uri:?}"),
            ));
        }
        Ok((item, lease))
    };

    match timeout(budget, read).await {
        Ok(res) => res,
        Err(_) => Err(Failure::new(
            5,
            "timeout",
            "Remote skill resource read timed out",
        )),
    }
}

async fn walk_directory(
    conn: &McpConnection,
    _server: &str,
    dir_uri: &str,
    deadline: Instant,
) -> std::result::Result<(Vec<Value>, ByteLease), Failure> {
    let budget = remaining_budget(deadline, "Remote skill directory listing")?;
    if conn.termination.is_terminated() {
        conn.abort_service();
        let err = conn
            .attach_stderr_tail(conn.termination.failure(false))
            .await;
        return Err(Failure::from_error(err, false));
    }
    conn.termination.clear_transient();
    let _capture = conn
        .ingress
        .capture()
        .ok_or_else(|| Failure::from_error(busy_capture(), false))?;

    let listing = async {
        let mut lease = conn.limits.budget.empty_lease();
        let mut cursors_lease = conn.limits.budget.empty_lease();
        let mut resources = Vec::new();
        let mut cursors = HashSet::new();
        let mut cursor: Option<String> = None;
        let mut bytes = 0usize;

        for _ in 0..MAX_SKILL_PAGES {
            let mut params = ReadResourceDirectoryRequestParams::new(dir_uri.to_owned());
            params.cursor = cursor;
            let fut = conn.peer.read_resource_directory(params);
            let mut page = tokio::select! {
                biased;
                () = conn.termination.cancelled() => {
                    conn.abort_service();
                    return Err(Failure::from_error(conn.termination.failure(false), false));
                }
                res = fut => res.map_err(|e| {
                    Failure::from_error(
                        service_failure(e, false, &conn.ingress, &conn.termination),
                        false,
                    )
                })?,
            };
            let (_, mut raw_page, page_lease) =
                observed_result(conn.ingress.take(), Some("resources/directory/read"), false)
                    .map_err(|e| Failure::from_error(e, false))?;
            lease.merge(page_lease);

            let page_bytes = crate::json::serialized_len(&raw_page).map_err(|e| {
                Failure::new(
                    4,
                    "mcp_protocol",
                    format!("Invalid resources/directory/read page: {e}"),
                )
            })?;
            bytes = bytes.saturating_add(page_bytes);
            if bytes > conn.limits.max_tool_catalog_bytes {
                return Err(Failure::new(
                    4,
                    "mcp_protocol",
                    format!(
                        "Remote skill directory listing exceeds the {} size limit",
                        format_bytes(conn.limits.max_tool_catalog_bytes)
                    ),
                ));
            }

            let items = raw_page
                .get_mut("resources")
                .and_then(Value::as_array_mut)
                .map(std::mem::take)
                .unwrap_or_default();
            resources.extend(items);

            cursor = page.next_cursor.take();
            drop(page);
            if let Some(c) = &cursor {
                if !cursors_lease.try_grow(c.len()) {
                    return Err(Failure::new(
                        1,
                        "memory_limit_exceeded",
                        "Remote skill directory cursors exceed the active memory limit",
                    ));
                }
                if !cursors.insert(c.clone()) {
                    return Err(Failure::new(
                        4,
                        "mcp_protocol",
                        "Remote skill directory listing repeated a pagination cursor",
                    ));
                }
            } else {
                return Ok((resources, lease));
            }
        }
        Err(Failure::new(
            4,
            "mcp_protocol",
            format!(
                "Remote skill directory listing exceeds the {MAX_SKILL_PAGES}-page pagination limit"
            ),
        ))
    };

    match timeout(budget, listing).await {
        Ok(res) => res,
        Err(_) => Err(Failure::new(
            5,
            "timeout",
            "Remote skill directory listing timed out",
        )),
    }
}

pub async fn skills_list(
    conn: &McpConnection,
    server: &str,
    budget: Duration,
) -> std::result::Result<(Value, ByteLease), Failure> {
    check_skills_capability(conn.peer.peer_info().as_deref(), false)?;
    let deadline = Instant::now() + budget;
    let (_typed, raw_skills, lease) = walk_skills(conn, server, deadline).await?;
    let mut out = Map::new();
    out.insert("server".into(), Value::String(server.to_owned()));
    out.insert("skills".into(), Value::Array(raw_skills));
    Ok((Value::Object(out), lease))
}

async fn fetch_and_verify_skill_md(
    conn: &McpConnection,
    server: &str,
    skill: &Skill,
    deadline: Instant,
) -> std::result::Result<(String, ByteLease), Failure> {
    let (mut item, lease) = read_resource_single(conn, server, &skill.uri, deadline).await?;
    let Some(Value::String(text)) = item.as_object_mut().and_then(|o| o.remove("text")) else {
        return Err(Failure::new(
            4,
            "skill_integrity",
            format!(
                "Remote SKILL.md {:?} did not return UTF-8 text content",
                skill.uri
            ),
        ));
    };
    if text.len() > MAX_SKILL_CONTENT_BYTES {
        return Err(Failure::new(
            4,
            "skill_integrity",
            format!(
                "Remote SKILL.md {:?} exceeds the {} content limit",
                skill.uri,
                format_bytes(MAX_SKILL_CONTENT_BYTES)
            ),
        ));
    }
    if let SkillResources::Files(files) = &skill.resources {
        let entry = files.iter().find(|f| f.uri == skill.uri).ok_or_else(|| {
            Failure::new(
                4,
                "skill_integrity",
                format!(
                    "Skill manifest {:?} does not contain its own SKILL.md",
                    skill.uri
                ),
            )
        })?;
        entry.verify(text.as_bytes()).map_err(|err| {
            Failure::new(
                4,
                "skill_integrity",
                format!("Remote SKILL.md integrity check failed: {err}"),
            )
        })?;
    }
    let expected_fm = frontmatter_map(skill)?;
    crate::skills::verify_remote_skill_md_frontmatter(&text, &expected_fm).map_err(|err| {
        Failure::new(
            4,
            "skill_integrity",
            format!("Remote SKILL.md frontmatter verification failed: {err}"),
        )
    })?;
    Ok((text, lease))
}

pub async fn skills_show(
    conn: &McpConnection,
    server: &str,
    selector: &str,
    budget: Duration,
) -> std::result::Result<(Value, ByteLease), Failure> {
    check_skills_capability(conn.peer.peer_info().as_deref(), false)?;
    let deadline = Instant::now() + budget;
    let (mut skill, from_list) = resolve_remote_skill(conn, server, selector, deadline).await?;
    let (text, lease) = match fetch_and_verify_skill_md(conn, server, &skill, deadline).await {
        Ok(pair) => pair,
        Err(err) if from_list && err.kind == "skill_integrity" => {
            skill = get_skill_once(conn, server, &skill.uri, deadline).await?;
            fetch_and_verify_skill_md(conn, server, &skill, deadline).await?
        }
        Err(err) => return Err(err),
    };
    let is_dynamic = matches!(skill.resources, SkillResources::Dynamic);
    let fm_val = serde_json::to_value(&skill.frontmatter).map_err(|e| {
        Failure::new(
            4,
            "mcp_protocol",
            format!("Failed to serialize skill frontmatter: {e}"),
        )
    })?;
    let mut out = Map::new();
    out.insert("server".into(), Value::String(server.to_owned()));
    out.insert("uri".into(), Value::String(skill.uri.clone()));
    out.insert("name".into(), Value::String(skill.name().to_owned()));
    out.insert("frontmatter".into(), fm_val);
    out.insert("verified".into(), Value::Bool(!is_dynamic));
    out.insert("dynamic".into(), Value::Bool(is_dynamic));
    out.insert("content".into(), Value::String(text));
    Ok((Value::Object(out), lease))
}

pub async fn skills_files(
    conn: &McpConnection,
    server: &str,
    selector: &str,
    dir: Option<&str>,
    budget: Duration,
) -> std::result::Result<(Value, ByteLease), Failure> {
    check_skills_capability(conn.peer.peer_info().as_deref(), dir.is_some())?;
    let deadline = Instant::now() + budget;
    let (skill, _from_list) = resolve_remote_skill(conn, server, selector, deadline).await?;
    let root = skill_root(&skill.uri)?;

    if let Some(rel_dir) = dir {
        let dir_uri = if rel_dir.is_empty() || rel_dir == "." {
            root.to_owned()
        } else {
            crate::skills::validate_clean_skill_rel_path(rel_dir).map_err(|err| {
                Failure::new(
                    2,
                    "invalid_skill_path",
                    format!("Invalid remote skill directory {rel_dir:?}: {err}"),
                )
            })?;
            format!("{root}/{rel_dir}")
        };
        let (resources, lease) = walk_directory(conn, server, &dir_uri, deadline).await?;
        let mut out = Map::new();
        out.insert("server".into(), Value::String(server.to_owned()));
        out.insert("uri".into(), Value::String(dir_uri));
        out.insert("resources".into(), Value::Array(resources));
        return Ok((Value::Object(out), lease));
    }

    match &skill.resources {
        SkillResources::Files(_) => {
            let entries = validate_static_manifest(&skill)?;
            let projected: Vec<Value> = entries
                .into_iter()
                .filter(|(_, entry)| entry.uri != skill.uri)
                .map(|(rel_path, entry)| {
                    let mut obj = Map::new();
                    obj.insert("path".into(), Value::String(rel_path));
                    obj.insert("uri".into(), Value::String(entry.uri));
                    obj.insert("size".into(), Value::Number(entry.size.into()));
                    obj.insert("digest".into(), Value::String(entry.digest));
                    Value::Object(obj)
                })
                .collect();
            let mut out = Map::new();
            out.insert("server".into(), Value::String(server.to_owned()));
            out.insert("uri".into(), Value::String(root.to_owned()));
            out.insert("resources".into(), Value::Array(projected));
            let bytes = crate::json::serialized_len(&out).unwrap_or(0);
            let lease = conn.limits.budget.try_acquire(bytes).ok_or_else(|| {
                Failure::new(
                    1,
                    "memory_limit_exceeded",
                    "Remote skill file list exceeds the active memory limit",
                )
            })?;
            Ok((Value::Object(out), lease))
        }
        SkillResources::Dynamic => {
            check_skills_capability(conn.peer.peer_info().as_deref(), true)?;
            let (resources, lease) = walk_directory(conn, server, root, deadline).await?;
            let mut out = Map::new();
            out.insert("server".into(), Value::String(server.to_owned()));
            out.insert("uri".into(), Value::String(root.to_owned()));
            out.insert("resources".into(), Value::Array(resources));
            Ok((Value::Object(out), lease))
        }
        _ => Err(Failure::new(
            4,
            "mcp_protocol",
            "Unsupported skill resources format",
        )),
    }
}

enum VerifiedResource {
    Text {
        text: String,
        mime_type: String,
        lease: ByteLease,
    },
    Blob {
        blob: String,
        decoded: Vec<u8>,
        decoded_lease: ByteLease,
        mime_type: String,
        wire_lease: ByteLease,
    },
}

fn find_manifest_entry<'a>(
    skill: &'a Skill,
    file_uri: &str,
) -> std::result::Result<Option<&'a SkillResource>, Failure> {
    let SkillResources::Files(files) = &skill.resources else {
        return Ok(None);
    };
    files
        .iter()
        .find(|entry| entry.uri == file_uri)
        .map(Some)
        .ok_or_else(|| {
            Failure::new(
                4,
                "skill_integrity",
                format!("Resource {file_uri:?} is not listed in skill manifest"),
            )
        })
}

async fn fetch_and_verify_resource(
    conn: &McpConnection,
    server: &str,
    skill: &Skill,
    file_uri: &str,
    deadline: Instant,
) -> std::result::Result<VerifiedResource, Failure> {
    let entry = find_manifest_entry(skill, file_uri)?.cloned();
    let (item, lease) = read_resource_single(conn, server, file_uri, deadline).await?;
    verify_resource_item(
        skill,
        entry.as_ref(),
        file_uri,
        item,
        lease,
        &conn.limits.budget,
    )
}

fn verify_resource_item(
    skill: &Skill,
    entry: Option<&SkillResource>,
    file_uri: &str,
    mut item: Value,
    lease: ByteLease,
    budget: &std::sync::Arc<crate::budget::ByteBudget>,
) -> std::result::Result<VerifiedResource, Failure> {
    let mime_type = item
        .get("mimeType")
        .and_then(Value::as_str)
        .unwrap_or("application/octet-stream")
        .to_owned();

    if let Some(Value::String(text)) = item.as_object_mut().and_then(|o| o.remove("text")) {
        if text.len() > MAX_SKILL_CONTENT_BYTES {
            return Err(Failure::new(
                4,
                "skill_integrity",
                format!(
                    "Remote skill resource {file_uri:?} exceeds the {} content limit",
                    format_bytes(MAX_SKILL_CONTENT_BYTES)
                ),
            ));
        }
        if let Some(entry) = entry {
            entry.verify(text.as_bytes()).map_err(|err| {
                Failure::new(
                    4,
                    "skill_integrity",
                    format!("Remote skill resource integrity check failed: {err}"),
                )
            })?;
        }
        if file_uri == skill.uri {
            let expected_fm = frontmatter_map(skill)?;
            crate::skills::verify_remote_skill_md_frontmatter(&text, &expected_fm).map_err(
                |err| {
                    Failure::new(
                        4,
                        "skill_integrity",
                        format!("Remote SKILL.md frontmatter verification failed: {err}"),
                    )
                },
            )?;
        }
        return Ok(VerifiedResource::Text {
            text,
            mime_type,
            lease,
        });
    }

    if file_uri == skill.uri {
        return Err(Failure::new(
            4,
            "skill_integrity",
            format!("Remote SKILL.md {file_uri:?} did not return UTF-8 text content"),
        ));
    }

    if let Some(Value::String(blob)) = item.as_object_mut().and_then(|o| o.remove("blob")) {
        let (decoded, decoded_lease) =
            decode_base64_leased(&blob, budget).map_err(|err| match err {
                DecodeError::InvalidBase64 => Failure::new(
                    4,
                    "mcp_protocol",
                    format!("Remote skill resource {file_uri:?} contains invalid base64"),
                ),
                DecodeError::MemoryLimitExceeded => Failure::new(
                    1,
                    "memory_limit_exceeded",
                    "Decoding remote skill resource exceeded the active memory limit",
                ),
            })?;
        if decoded.len() > MAX_SKILL_CONTENT_BYTES {
            return Err(Failure::new(
                4,
                "skill_integrity",
                format!(
                    "Remote skill resource {file_uri:?} exceeds the {} content limit",
                    format_bytes(MAX_SKILL_CONTENT_BYTES)
                ),
            ));
        }
        if let Some(entry) = entry {
            entry.verify(&decoded).map_err(|err| {
                Failure::new(
                    4,
                    "skill_integrity",
                    format!("Remote skill resource integrity check failed: {err}"),
                )
            })?;
        }
        return Ok(VerifiedResource::Blob {
            blob,
            decoded,
            decoded_lease,
            mime_type,
            wire_lease: lease,
        });
    }

    Err(Failure::new(
        4,
        "mcp_protocol",
        format!("Remote skill resource {file_uri:?} returned neither text nor blob"),
    ))
}

pub async fn skills_read(
    conn: &McpConnection,
    server: &str,
    selector: &str,
    rel_path: &str,
    inline_media: bool,
    media: &MediaStore,
    budget: Duration,
) -> std::result::Result<(Value, ByteLease, Option<InflightBatch>), Failure> {
    check_skills_capability(conn.peer.peer_info().as_deref(), false)?;
    let deadline = Instant::now() + budget;
    let (mut skill, from_list) = resolve_remote_skill(conn, server, selector, deadline).await?;
    let file_uri =
        crate::skills::resolve_skill_relative_uri(&skill.uri, rel_path).map_err(|err| {
            Failure::new(
                2,
                "invalid_skill_path",
                format!("Invalid remote skill path {rel_path:?}: {err}"),
            )
        })?;

    let verified = match fetch_and_verify_resource(conn, server, &skill, &file_uri, deadline).await
    {
        Ok(res) => res,
        Err(err) if from_list && err.kind == "skill_integrity" => {
            skill = get_skill_once(conn, server, &skill.uri, deadline).await?;
            fetch_and_verify_resource(conn, server, &skill, &file_uri, deadline).await?
        }
        Err(err) => return Err(err),
    };

    let is_dynamic = matches!(skill.resources, SkillResources::Dynamic);
    let mut out = Map::new();
    out.insert("server".into(), Value::String(server.to_owned()));
    out.insert("uri".into(), Value::String(file_uri));
    out.insert("path".into(), Value::String(rel_path.to_owned()));
    out.insert("verified".into(), Value::Bool(!is_dynamic));
    out.insert("dynamic".into(), Value::Bool(is_dynamic));

    match verified {
        VerifiedResource::Text {
            text,
            mime_type,
            lease,
        } => {
            out.insert("mimeType".into(), Value::String(mime_type));
            out.insert("content".into(), Value::String(text));
            Ok((Value::Object(out), lease, None))
        }
        VerifiedResource::Blob {
            blob,
            decoded,
            decoded_lease,
            mime_type,
            wire_lease,
        } => {
            out.insert("mimeType".into(), Value::String(mime_type.clone()));
            if inline_media {
                drop(decoded);
                drop(decoded_lease);
                out.insert("blob".into(), Value::String(blob));
                Ok((Value::Object(out), wire_lease, None))
            } else {
                drop(blob);
                drop(wire_lease);
                let (saved_record, record_lease, inflight) = media
                    .extract_resource_blob(&decoded, &mime_type, &conn.limits.budget)
                    .await
                    .map_err(|err| Failure {
                        code: err.code,
                        kind: err.kind,
                        message: err.message,
                        outcome_unknown: err.outcome_unknown,
                        rpc_error: err.rpc_error,
                        auth: None,
                        stderr_tail: None,
                        data: err.data.map(Box::new),
                        lease: err.lease,
                    })?;
                drop(decoded);
                drop(decoded_lease);
                out.insert("savedMedia".into(), Value::Array(vec![saved_record]));
                Ok((Value::Object(out), record_lease, Some(inflight)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rmcp::model::{Implementation, InitializeResult, ServerCapabilities, SkillFrontmatter};

    fn peer_info(
        version: ProtocolVersion,
        resources: bool,
        skills: bool,
        directory_read: bool,
    ) -> ServerPeerInfo {
        let mut caps = ServerCapabilities::default();
        if resources {
            caps.resources = Some(Default::default());
        }
        if skills {
            let mut exts = rmcp::model::ExtensionCapabilities::new();
            let mut settings = Map::new();
            if directory_read {
                settings.insert("directoryRead".into(), Value::Bool(true));
            }
            exts.insert(rmcp::model::SKILLS_EXTENSION_ID.to_owned(), settings);
            caps.extensions = Some(exts);
        }
        let mut init = InitializeResult::new(caps);
        init.protocol_version = version;
        init.server_info = Implementation::new("srv", "1.0");
        init.into()
    }

    #[test]
    fn capability_gate_enforces_version_resources_skills_and_directory_read() {
        assert!(check_skills_capability(None, false).is_err());

        let legacy = peer_info(ProtocolVersion::V_2025_11_25, true, true, true);
        let err = check_skills_capability(Some(&legacy), false).unwrap_err();
        assert_eq!(err.code, 4);
        assert_eq!(err.kind, "mcp_protocol");
        assert!(!err.outcome_unknown);

        let no_res = peer_info(ProtocolVersion::V_2026_07_28, false, true, true);
        assert_eq!(
            check_skills_capability(Some(&no_res), false)
                .unwrap_err()
                .code,
            4
        );

        let no_skills = peer_info(ProtocolVersion::V_2026_07_28, true, false, false);
        assert_eq!(
            check_skills_capability(Some(&no_skills), false)
                .unwrap_err()
                .code,
            4
        );

        let basic = peer_info(ProtocolVersion::V_2026_07_28, true, true, false);
        assert!(check_skills_capability(Some(&basic), false).is_ok());
        assert_eq!(
            check_skills_capability(Some(&basic), true)
                .unwrap_err()
                .code,
            4
        );

        let full = peer_info(ProtocolVersion::V_2026_07_28, true, true, true);
        assert!(check_skills_capability(Some(&full), true).is_ok());
    }

    #[test]
    fn selector_validation_distinguishes_uris_names_and_rejects_invalid_inputs() {
        assert!(validate_selector("skill://demo/SKILL.md").unwrap());
        assert!(validate_selector("https://example.com/a/demo/SKILL.md").unwrap());
        assert!(!validate_selector("demo").unwrap());
        assert!(!validate_selector("team/demo").unwrap());

        for bad in [
            "",
            "skill://demo/other.md",
            "skill://demo/SKILL.md?x=1",
            "skill://demo/SKILL.md#frag",
            "../demo",
            "a//b",
            r"a\b",
            "demo\nname",
            "demo?q=1",
        ] {
            let err = validate_selector(bad).unwrap_err();
            assert_eq!(err.code, 2, "{bad:?}");
            assert_eq!(err.kind, "invalid_skill_uri", "{bad:?}");
        }
    }

    #[test]
    fn static_manifest_validation_rejects_encoded_traversal_and_queries() {
        let ok_skill = Skill::new(
            "skill://demo/SKILL.md",
            SkillFrontmatter::new("demo", "desc"),
            SkillResources::Files(vec![
                SkillResource::from_bytes("skill://demo/SKILL.md", b"---\nname: demo\n---\n"),
                SkillResource::from_bytes("skill://demo/refs/guide.md", b"hi"),
            ]),
        );
        let entries = validate_static_manifest(&ok_skill).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[1].0, "refs/guide.md");

        for bad_uri in [
            "skill://demo/%2e%2E/secret.md",
            "skill://demo/refs/guide.md?raw=1",
            "skill://demo/../other/guide.md",
            "skill://other/guide.md",
        ] {
            let bad_skill = Skill::new(
                "skill://demo/SKILL.md",
                SkillFrontmatter::new("demo", "desc"),
                SkillResources::Files(vec![SkillResource::from_bytes(bad_uri, b"x")]),
            );
            let err = validate_static_manifest(&bad_skill).unwrap_err();
            assert_eq!(err.code, 4, "{bad_uri:?}");
            assert_eq!(err.kind, "skill_integrity", "{bad_uri:?}");
        }
    }

    #[test]
    fn manifest_membership_precheck_blocks_unlisted_uri() {
        let skill = Skill::new(
            "skill://demo/SKILL.md",
            SkillFrontmatter::new("demo", "desc"),
            SkillResources::Files(vec![SkillResource::from_bytes(
                "skill://demo/SKILL.md",
                b"ok",
            )]),
        );
        assert!(
            find_manifest_entry(&skill, "skill://demo/SKILL.md")
                .unwrap()
                .is_some()
        );
        let err = find_manifest_entry(&skill, "skill://demo/secret.txt").unwrap_err();
        assert_eq!(err.code, 4);
        assert_eq!(err.kind, "skill_integrity");
    }

    #[test]
    fn skills_read_rejects_skill_md_returned_as_blob_with_skill_integrity() {
        let raw_md = b"---\nname: demo\ndescription: desc\n---\nBody\n";
        let entry = SkillResource::from_bytes("skill://demo/SKILL.md", raw_md);
        let skill = Skill::new(
            "skill://demo/SKILL.md",
            SkillFrontmatter::new("demo", "desc"),
            SkillResources::Files(vec![entry.clone()]),
        );
        let budget = crate::budget::ByteBudget::new(64 * 1024);
        // Valid base64 of the exact SKILL.md bytes: must still be rejected because SKILL.md
        // cannot bypass frontmatter verification via the binary blob branch.
        let blob_item = serde_json::json!({
            "uri": "skill://demo/SKILL.md",
            "mimeType": "text/markdown",
            "blob": "LS0tCm5hbWU6IGRlbW8KZGVzY3JpcHRpb246IGRlc2MKLS0tCkJvZHkK"
        });
        let err = verify_resource_item(
            &skill,
            Some(&entry),
            "skill://demo/SKILL.md",
            blob_item,
            budget.empty_lease(),
            &budget,
        )
        .err()
        .expect("SKILL.md blob must be rejected");
        assert_eq!(err.code, 4);
        assert_eq!(err.kind, "skill_integrity");
        assert_eq!(budget.used(), 0);
    }
}
