use super::args::{SkillCommand, usage};
use super::output::{Output, brief};
use anyhow::Result;
use mcpcli::{
    agents,
    config::LoadedConfig,
    discovery::{self, safe_text},
    skills,
};
use serde_json::json;
use std::path::PathBuf;

pub(super) fn builtin_skill() -> Output {
    Output::with_text(
        json!({
            "name": discovery::BUILTIN_SKILL_NAME,
            "version": discovery::BUILTIN_SKILL_VERSION,
            "content": discovery::BUILTIN_SKILL_MD,
        }),
        discovery::BUILTIN_SKILL_MD.to_owned(),
    )
}

pub(super) fn catalog(loaded: &LoadedConfig, query: Option<&str>) -> Result<Output> {
    let agents_doc = agents::load(loaded.config.agents.as_deref())
        .map_err(|error| usage("agents", format!("{error:#}")))?;
    let skill_catalog = skills::discover(&loaded.config.skills)
        .map_err(|error| usage("skills", format!("{error:#}")))?;
    for warning in &skill_catalog.warnings {
        eprintln!("mcpcli: {}", safe_text(warning));
    }
    let query = query.map(str::to_lowercase);
    let matches = |name: &str, description: &str| {
        query
            .as_ref()
            .is_none_or(|q| format!("{name} {description}").to_lowercase().contains(q))
    };
    let selected_agents = agents_doc.filter(|doc| {
        query.as_ref().is_none_or(|q| {
            let file_name = doc
                .path
                .file_name()
                .map(|name| name.to_string_lossy())
                .unwrap_or_default();
            format!("{file_name} {}", doc.content)
                .to_lowercase()
                .contains(q)
        })
    });
    let servers: Vec<_> = loaded
        .config
        .servers
        .iter()
        .filter(|(name, server)| matches(name, server.description().unwrap_or("")))
        .map(|(name, server)| {
            json!({
                "name": name,
                "transport": server.transport(),
                "enabled": server.enabled(),
                "description": server.description().unwrap_or("")
            })
        })
        .collect();
    let selected_skills: Vec<_> = skill_catalog
        .skills
        .into_iter()
        .filter(|s| matches(&s.name, &s.description))
        .collect();
    let mut lines = Vec::new();
    for server in &servers {
        let name = server["name"].as_str().unwrap();
        let desc = brief(server["description"].as_str().unwrap());
        if server["enabled"].as_bool() == Some(false) {
            lines.push(format!("server {name} [disabled]\t{desc}"));
        } else {
            lines.push(format!("server {name}\t{desc}"));
        }
    }
    for skill in &selected_skills {
        lines.push(format!(
            "skill {}\t{}",
            skill.name,
            brief(&skill.description)
        ));
    }
    let mut sections = Vec::new();
    if let Some(doc) = &selected_agents {
        let path = safe_text(&doc.path.display().to_string());
        let rendered = safe_text(&doc.content);
        let body = rendered.trim_end_matches('\n');
        if body.is_empty() {
            sections.push(format!("agents {path}"));
        } else {
            sections.push(format!("agents {path}\n\n{body}"));
        }
    }
    if !lines.is_empty() {
        sections.push(lines.join("\n"));
    }
    Ok(Output::with_text(
        json!({
            "agents": selected_agents,
            "servers": servers,
            "skills": selected_skills,
            "warnings": skill_catalog.warnings,
        }),
        sections.join("\n\n"),
    ))
}

pub(super) fn run_agents(loaded: &LoadedConfig) -> Result<Output> {
    let doc = agents::load(loaded.config.agents.as_deref())
        .map_err(|error| usage("agents", format!("{error:#}")))?;
    let text = doc
        .as_ref()
        .map(|doc| doc.content.clone())
        .unwrap_or_default();
    Ok(Output::with_text(json!({ "agents": doc }), text))
}

pub(super) fn run_search(loaded: &LoadedConfig, query: &str) -> Result<Output> {
    if query.is_empty() || query.len() > 1024 {
        return Err(usage("query", "query must contain 1-1024 bytes"));
    }
    catalog(loaded, Some(query))
}

pub(super) fn run_config_validate(loaded: &LoadedConfig) -> Result<Output> {
    if loaded.config.agents.is_some() {
        agents::load(loaded.config.agents.as_deref())
            .map_err(|error| usage("agents", format!("{error:#}")))?;
    }
    let skill_catalog = if loaded.config.skills.is_empty() {
        skills::SkillCatalog::default()
    } else {
        skills::discover(&loaded.config.skills)
            .map_err(|error| usage("skills", format!("{error:#}")))?
    };
    let servers = loaded.config.servers.len();
    let enabled_servers = loaded
        .config
        .servers
        .values()
        .filter(|server| server.enabled())
        .count();
    let skills_count = skill_catalog.skills.len();
    let mut lines = vec![
        "valid: true".to_owned(),
        format!("servers: {servers} ({enabled_servers} enabled)"),
        format!("skills: {skills_count}"),
    ];
    for warning in &skill_catalog.warnings {
        lines.push(format!("warning: {}", safe_text(warning)));
    }
    Ok(Output::with_text(
        json!({
            "valid": true,
            "servers": servers,
            "enabledServers": enabled_servers,
            "skills": skills_count,
            "warnings": skill_catalog.warnings,
        }),
        lines.join("\n"),
    ))
}

pub(super) fn run_local_skills(loaded: &LoadedConfig, command: &SkillCommand) -> Result<Output> {
    match command {
        SkillCommand::Files { dir: Some(_), .. } => {
            return Err(usage("usage", "--dir requires --server"));
        }
        SkillCommand::Read {
            inline_media: true, ..
        } => {
            return Err(usage("usage", "--inline-media requires --server"));
        }
        _ => {}
    }
    let catalog =
        skills::discover(&loaded.config.skills).map_err(|e| usage("skills", format!("{e:#}")))?;
    for warning in &catalog.warnings {
        eprintln!("mcpcli: {}", safe_text(warning));
    }
    if matches!(command, SkillCommand::List { .. }) {
        let text = catalog
            .skills
            .iter()
            .map(|s| format!("{}\t{}", s.name, brief(&s.description)))
            .collect::<Vec<_>>()
            .join("\n");
        return Ok(Output::with_text(json!(catalog), text));
    }
    let name = match command {
        SkillCommand::Show { name, .. }
        | SkillCommand::Files { name, .. }
        | SkillCommand::Read { name, .. } => name,
        _ => unreachable!(),
    };
    let skill = catalog
        .skills
        .iter()
        .find(|s| s.name == *name)
        .ok_or_else(|| usage("unknown_skill", format!("unknown skill {name:?}")))?;
    match command {
        SkillCommand::Show { .. } => {
            let content = skills::show(skill).map_err(|e| usage("skill_read", format!("{e:#}")))?;
            let root = safe_text(&skill.root.display().to_string());
            let text = format!("Base directory: {root}\n\n{content}");
            Ok(Output::with_text(
                json!({"name":name,"root":skill.root,"path":skill.path,"content":content}),
                text,
            ))
        }
        SkillCommand::Files { .. } => {
            let files = skills::files(skill).map_err(|e| usage("skill_files", format!("{e:#}")))?;
            let text = files
                .iter()
                .map(|path| safe_text(path))
                .collect::<Vec<_>>()
                .join("\n");
            Ok(Output::with_text(
                json!({"name":name,"root":skill.root,"files":files}),
                text,
            ))
        }
        SkillCommand::Read { path, .. } => {
            let local_path = PathBuf::from(path);
            let content = skills::read(skill, &local_path)
                .map_err(|e| usage("skill_read", format!("{e:#}")))?;
            Ok(Output::with_text(
                json!({"name":name,"root":skill.root,"path":local_path,"content":content}),
                content,
            ))
        }
        _ => unreachable!(),
    }
}
