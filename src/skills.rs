use anyhow::{Context, Result, bail, ensure};
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read};
use std::path::{Component, Path, PathBuf};

const MAX_DOCUMENT_BYTES: u64 = 1024 * 1024;
const MAX_HEADER_BYTES: usize = 64 * 1024;
const MAX_ENTRIES: usize = 4096;
const MAX_SKILLS: usize = 1024;
const MAX_RESOURCE_DEPTH: usize = 16;

#[derive(Clone, Debug, Serialize)]
pub struct Skill {
    pub name: String,
    pub description: String,
    pub path: PathBuf,
    pub root: PathBuf,
}

#[derive(Debug, Default, Serialize)]
pub struct SkillCatalog {
    pub skills: Vec<Skill>,
    pub warnings: Vec<String>,
}

/// Only explicit roots and their immediate SKILL.md-bearing child directories are mounted.
pub fn discover(roots: &[PathBuf]) -> Result<SkillCatalog> {
    ensure!(
        roots.len() <= MAX_SKILLS,
        "too many skill roots (maximum {MAX_SKILLS})"
    );
    let mut catalog = SkillCatalog::default();
    let mut skills = BTreeMap::<String, Skill>::new();
    let mut seen_roots = BTreeSet::new();
    let mut scanned_entries = 0;
    for configured in roots {
        let root = crate::platform::canonical_skill_root(configured)
            .with_context(|| format!("open configured skill directory {configured:?}"))?;
        if !seen_roots.insert(root.clone()) {
            catalog
                .warnings
                .push(format!("duplicate skill root ignored: {root:?}"));
            continue;
        }
        let mut candidates = Vec::new();
        if has_manifest(&root)? {
            candidates.push(root);
        } else {
            for entry in entries(&root, Path::new(""))? {
                scanned_entries += 1;
                ensure!(
                    scanned_entries <= MAX_ENTRIES,
                    "skill discovery exceeds {MAX_ENTRIES} entries"
                );
                if entry.kind.is_symlink() {
                    catalog.warnings.push(format!(
                        "symlink is not followed during skill discovery: {:?}",
                        root.join(&entry.name)
                    ));
                } else if entry.kind.is_dir() {
                    let child = root.join(&entry.name);
                    if has_manifest(&child)? {
                        candidates.push(child);
                    }
                }
            }
        }
        for root in candidates {
            let result = load_skill(&root);
            match result {
                Ok(skill) => {
                    if let Some(existing) = skills.get(&skill.name) {
                        if existing.path == skill.path {
                            catalog
                                .warnings
                                .push(format!("duplicate skill source ignored: {:?}", skill.path));
                            continue;
                        }
                        bail!(
                            "duplicate skill name {:?}: {:?} and {:?}",
                            skill.name,
                            existing.path,
                            skill.path
                        );
                    }
                    ensure!(
                        skills.len() < MAX_SKILLS,
                        "too many skills (maximum {MAX_SKILLS})"
                    );
                    skills.insert(skill.name.clone(), skill);
                }
                Err(error) => catalog.warnings.push(format!(
                    "invalid skill {:?}: {error:#}",
                    root.join("SKILL.md")
                )),
            }
        }
    }
    catalog.skills = skills.into_values().collect();
    Ok(catalog)
}

fn has_manifest(root: &Path) -> Result<bool> {
    match fs::symlink_metadata(root.join("SKILL.md")) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error).with_context(|| format!("inspect skill manifest in {root:?}")),
    }
}

fn load_skill(root: &Path) -> Result<Skill> {
    let file = open_beneath(root, Path::new("SKILL.md"), false)?;
    check_document_size(&file)?;
    let mut reader = BufReader::new(file.take(MAX_HEADER_BYTES as u64 + 1));
    let mut line = String::new();
    reader.read_line(&mut line)?;
    ensure!(
        line.trim_end_matches(['\r', '\n'])
            .trim_start_matches('\u{feff}')
            == "---",
        "SKILL.md must start with YAML frontmatter"
    );
    let mut header = String::new();
    loop {
        line.clear();
        ensure!(
            reader.read_line(&mut line)? > 0,
            "missing closing YAML frontmatter delimiter"
        );
        if line.trim_end_matches(['\r', '\n']) == "---" {
            break;
        }
        ensure!(
            header.len() + line.len() <= MAX_HEADER_BYTES,
            "YAML frontmatter exceeds {MAX_HEADER_BYTES} bytes"
        );
        header.push_str(&line);
    }
    let (name, description) = parse_metadata(&header, root)?;
    Ok(Skill {
        name,
        description,
        path: root.join("SKILL.md"),
        root: root.to_path_buf(),
    })
}

fn parse_metadata(header: &str, root: &Path) -> Result<(String, String)> {
    ensure!(
        header.len() <= MAX_HEADER_BYTES,
        "YAML frontmatter is too large"
    );
    let options = serde_saphyr::options! {
        strict_booleans: true,
        reject_unsupported_tags: true,
        with_snippet: false,
        budget: serde_saphyr::budget! {
            max_documents: 1,
            max_depth: 16,
            flow_nesting_limit: 16,
            max_nodes: 1024,
            max_events: 4096,
            max_aliases: 16,
            max_anchors: 16,
            max_recorded_anchor_events: 1024,
            max_recorded_anchor_bytes: MAX_HEADER_BYTES,
            max_total_scalar_bytes: MAX_HEADER_BYTES,
        },
    };
    let value: Value =
        serde_saphyr::from_str_with_options(header, options).context("invalid YAML frontmatter")?;
    let object = value
        .as_object()
        .context("YAML frontmatter must be a mapping")?;
    let name = object
        .get("name")
        .and_then(Value::as_str)
        .context("name must be a string")?;
    ensure!(
        !name.is_empty()
            && name.len() <= 64
            && name
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
            && !name.starts_with('-')
            && !name.ends_with('-')
            && !name.contains("--"),
        "name must contain 1-64 lowercase ASCII letters, digits or single hyphens"
    );
    ensure!(
        root.file_name().and_then(|name| name.to_str()) == Some(name),
        "name {name:?} must match the skill directory name"
    );
    let description = object
        .get("description")
        .and_then(Value::as_str)
        .context("description must be a string")?;
    ensure!(
        !description.trim().is_empty() && description.chars().count() <= 1024,
        "description must contain 1-1024 characters"
    );
    for field in ["license", "allowed-tools", "compatibility"] {
        if let Some(value) = object.get(field) {
            let text = value
                .as_str()
                .with_context(|| format!("{field} must be a string"))?;
            ensure!(!text.trim().is_empty(), "{field} must not be empty");
            if field == "compatibility" {
                ensure!(
                    text.chars().count() <= 500,
                    "compatibility exceeds 500 characters"
                );
            }
        }
    }
    if let Some(metadata) = object.get("metadata") {
        let metadata = metadata
            .as_object()
            .context("metadata must be a string-to-string mapping")?;
        ensure!(
            metadata.values().all(Value::is_string),
            "metadata values must be strings"
        );
    }
    Ok((name.to_owned(), description.to_owned()))
}

/// Return the complete manifest, including optional metadata, without executing anything.
pub fn show(skill: &Skill) -> Result<String> {
    read(skill, Path::new("SKILL.md"))
}

/// Return all regular resource paths without reading their contents.
pub fn files(skill: &Skill) -> Result<Vec<String>> {
    let mut result = Vec::new();
    let mut visited = 0;
    collect_files(&skill.root, Path::new(""), 0, &mut visited, &mut result)?;
    result.sort();
    Ok(result)
}

fn collect_files(
    root: &Path,
    relative: &Path,
    depth: usize,
    visited: &mut usize,
    result: &mut Vec<String>,
) -> Result<()> {
    ensure!(
        depth <= MAX_RESOURCE_DEPTH,
        "skill resources exceed depth {MAX_RESOURCE_DEPTH}"
    );
    for entry in entries(root, relative)? {
        *visited += 1;
        ensure!(
            *visited <= MAX_ENTRIES,
            "skill resources exceed {MAX_ENTRIES} entries"
        );
        let path = relative.join(entry.name);
        let file_type = entry.kind;
        ensure!(
            !file_type.is_symlink(),
            "symlink resources are not supported: {path:?}"
        );
        if file_type.is_dir() {
            collect_files(root, &path, depth + 1, visited, result)?;
        } else {
            ensure!(
                file_type.is_file(),
                "resource is not a regular file: {path:?}"
            );
            if path != Path::new("SKILL.md") {
                result.push(
                    path.iter()
                        .map(|part| part.to_str().context("resource path is not UTF-8"))
                        .collect::<Result<Vec<_>>>()?
                        .join("/"),
                );
            }
        }
    }
    Ok(())
}

/// Resource reads are UTF-8, bounded and confined to the mounted skill directory.
pub fn read(skill: &Skill, relative: &Path) -> Result<String> {
    ensure!(
        !relative.as_os_str().is_empty(),
        "resource path must not be empty"
    );
    let file = open_beneath(&skill.root, relative, false)?;
    check_document_size(&file)?;
    let mut bytes = Vec::new();
    file.take(MAX_DOCUMENT_BYTES + 1).read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 <= MAX_DOCUMENT_BYTES,
        "resource exceeds {MAX_DOCUMENT_BYTES} bytes"
    );
    String::from_utf8(bytes).context("resource is not UTF-8 text")
}

fn check_document_size(file: &File) -> Result<()> {
    ensure!(
        file.metadata()?.len() <= MAX_DOCUMENT_BYTES,
        "resource exceeds {MAX_DOCUMENT_BYTES} bytes"
    );
    Ok(())
}

fn entries(root: &Path, relative: &Path) -> Result<Vec<crate::platform::DirectoryEntry>> {
    let directory = open_beneath(root, relative, true)?;
    let mut result = crate::platform::directory_entries(&directory, MAX_ENTRIES)?;
    result.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(result)
}

fn open_beneath(root: &Path, relative: &Path, directory: bool) -> Result<File> {
    ensure!(!relative.is_absolute(), "resource path must be relative");
    let components: Vec<_> = relative.components().collect();
    ensure!(
        components
            .iter()
            .all(|component| matches!(component, Component::Normal(_))),
        "resource path must not contain '.' or '..'"
    );
    ensure!(
        components.len() <= MAX_RESOURCE_DEPTH + 1,
        "resource path is too deep"
    );
    ensure!(root.is_absolute(), "skill root must be absolute");
    ensure!(
        root.components().all(|part| matches!(
            part,
            Component::RootDir | Component::Prefix(_) | Component::Normal(_)
        )),
        "skill root must be normalized"
    );
    crate::platform::open_beneath(root, relative, directory)
        .with_context(|| format!("open resource {relative:?} beneath {root:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::ffi::CString;
    #[cfg(unix)]
    use std::os::unix::ffi::OsStrExt;
    #[cfg(unix)]
    use std::os::unix::fs::symlink;
    #[cfg(windows)]
    fn symlink(source: impl AsRef<Path>, target: impl AsRef<Path>) -> std::io::Result<()> {
        if source.as_ref().is_dir() {
            std::os::windows::fs::symlink_dir(source, target)
        } else {
            std::os::windows::fs::symlink_file(source, target)
        }
    }
    use tempfile::TempDir;

    fn create_skill(parent: &Path, name: &str) -> PathBuf {
        let root = parent.join(name);
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("SKILL.md"), format!(
            "---\nname: {name}\ndescription: >-\n  Use this skill to inspect\n  stateful services.\nmetadata:\n  revision: \"1\"\n---\n# Instructions\nRead references/guide.md.\n"
        )).unwrap();
        root
    }

    fn get_skill(root: &Path) -> Skill {
        let catalog = discover(&[root.to_owned()]).unwrap();
        assert!(catalog.warnings.is_empty(), "{:?}", catalog.warnings);
        catalog.skills.into_iter().next().unwrap()
    }

    #[test]
    fn parses_real_yaml_and_discloses_content_on_demand() {
        let temp = TempDir::new().unwrap();
        let root = create_skill(temp.path(), "stateful-service");
        fs::create_dir(root.join("references")).unwrap();
        fs::write(root.join("references/guide.md"), "Keep the session open.").unwrap();
        let skill = get_skill(temp.path());
        assert_eq!(
            skill.description,
            "Use this skill to inspect stateful services."
        );
        assert!(
            !serde_json::to_string(&skill)
                .unwrap()
                .contains("# Instructions")
        );
        assert!(show(&skill).unwrap().contains("# Instructions"));
        assert_eq!(files(&skill).unwrap(), ["references/guide.md"]);
        assert_eq!(
            read(&skill, Path::new("references/guide.md")).unwrap(),
            "Keep the session open."
        );
    }

    #[test]
    fn accepts_bom_crlf_and_preserves_full_manifest() {
        let temp = TempDir::new().unwrap();
        let root = create_skill(temp.path(), "portable");
        let text =
            "\u{feff}---\r\nname: portable\r\ndescription: \"Colon: content\"\r\n---\r\nBody\r\n";
        fs::write(root.join("SKILL.md"), text).unwrap();
        assert_eq!(show(&get_skill(&root)).unwrap(), text);
    }

    #[test]
    fn discovery_ignores_flat_markdown_and_nested_references() {
        let temp = TempDir::new().unwrap();
        fs::write(
            temp.path().join("legacy.md"),
            "---\nname: legacy\ndescription: flat\n---\n",
        )
        .unwrap();
        let root = create_skill(temp.path(), "root-skill");
        let nested = create_skill(&root, "nested");
        let catalog = discover(&[temp.path().to_owned()]).unwrap();
        assert_eq!(catalog.skills.len(), 1);
        assert_eq!(catalog.skills[0].name, "root-skill");
        assert_eq!(get_skill(&nested).name, "nested");
    }

    #[test]
    fn rejects_ambiguous_names_but_deduplicates_same_source() {
        let a = TempDir::new().unwrap();
        let b = TempDir::new().unwrap();
        let root = create_skill(a.path(), "duplicate");
        create_skill(b.path(), "duplicate");
        let error = discover(&[a.path().to_owned(), b.path().to_owned()]).unwrap_err();
        assert!(error.to_string().contains("duplicate skill name"));
        let catalog = discover(&[a.path().to_owned(), root]).unwrap();
        assert_eq!(catalog.skills.len(), 1);
        assert_eq!(catalog.warnings.len(), 1);
    }

    #[test]
    fn reports_missing_roots_and_invalid_manifests() {
        let temp = TempDir::new().unwrap();
        assert!(discover(&[temp.path().join("missing")]).is_err());
        let root = create_skill(temp.path(), "broken");
        fs::write(
            root.join("SKILL.md"),
            "---\nname: wrong\ndescription: ok\n---\n",
        )
        .unwrap();
        let catalog = discover(&[temp.path().to_owned()]).unwrap();
        assert!(catalog.skills.is_empty());
        assert!(catalog.warnings[0].contains("must match"));
    }

    #[test]
    fn rejects_invalid_standard_fields_and_duplicate_yaml_keys() {
        for header in [
            "name: valid\nname: valid\ndescription: ok\n",
            "name: valid\ndescription: []\n",
            "name: valid\ndescription: ok\nmetadata:\n  version: 1\n",
            "name: valid\ndescription: ok\nallowed-tools: [bash]\n",
            "name: valid\ndescription: ok\ncompatibility: null\n",
            "name: valid\ndescription: !include /etc/passwd\n",
        ] {
            assert!(
                parse_metadata(header, Path::new("/skills/valid")).is_err(),
                "{header}"
            );
        }
        let deep = format!(
            "name: valid\ndescription: ok\nextra: {}0{}\n",
            "[".repeat(1000),
            "]".repeat(1000)
        );
        assert!(parse_metadata(&deep, Path::new("/skills/valid")).is_err());
    }

    #[test]
    fn rejects_path_traversal_symlinks_and_special_files() {
        let temp = TempDir::new().unwrap();
        let root = create_skill(temp.path(), "safe");
        let skill = get_skill(&root);
        for path in [
            "../outside",
            "/etc/passwd",
            "",
            "./SKILL.md",
            "references/../../outside",
        ] {
            assert!(read(&skill, Path::new(path)).is_err(), "{path}");
        }
        fs::write(temp.path().join("secret"), "private").unwrap();
        symlink(temp.path().join("secret"), root.join("escape")).unwrap();
        assert!(read(&skill, Path::new("escape")).is_err());
        symlink(temp.path(), root.join("directory-link")).unwrap();
        assert!(read(&skill, Path::new("directory-link/secret")).is_err());
        assert!(files(&skill).is_err());
        #[cfg(unix)]
        {
            let fifo = CString::new(root.join("fifo").as_os_str().as_bytes()).unwrap();
            assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
            assert!(read(&skill, Path::new("fifo")).is_err());
        }
    }

    #[test]
    fn rejects_replaced_ancestor_directory() {
        let temp = TempDir::new().unwrap();
        let collection = temp.path().join("collection");
        let root = create_skill(&collection, "original");
        let skill = get_skill(&root);
        let moved = temp.path().join("moved");
        fs::rename(&collection, &moved).unwrap();
        symlink(&moved, &collection).unwrap();
        assert!(show(&skill).is_err());
        assert!(files(&skill).is_err());
    }

    #[test]
    fn refuses_linked_manifests_and_roots() {
        let temp = TempDir::new().unwrap();
        let root = create_skill(temp.path(), "source");
        let linked = temp.path().join("linked");
        symlink(&root, &linked).unwrap();
        for suffix in ["", "/", "/."] {
            let mut alias = linked.as_os_str().to_os_string();
            alias.push(suffix);
            assert!(
                discover(&[PathBuf::from(alias)]).is_err(),
                "symlink suffix {suffix:?}"
            );
        }
        #[cfg(windows)]
        for suffix in [r"\", r"\."] {
            let mut alias = linked.as_os_str().to_os_string();
            alias.push(suffix);
            assert!(
                discover(&[PathBuf::from(alias)]).is_err(),
                "symlink suffix {suffix:?}"
            );
        }
        fs::remove_file(root.join("SKILL.md")).unwrap();
        fs::write(temp.path().join("outside"), "secret").unwrap();
        symlink(temp.path().join("outside"), root.join("SKILL.md")).unwrap();
        let catalog = discover(&[root]).unwrap();
        assert!(catalog.skills.is_empty());
        assert_eq!(catalog.warnings.len(), 1);
    }

    #[test]
    fn bounds_file_sizes_text_encoding_and_resource_depth() {
        let temp = TempDir::new().unwrap();
        let root = create_skill(temp.path(), "bounded");
        let skill = get_skill(&root);
        File::create(root.join("large"))
            .unwrap()
            .set_len(MAX_DOCUMENT_BYTES + 1)
            .unwrap();
        assert!(read(&skill, Path::new("large")).is_err());
        fs::write(root.join("binary"), [0xff]).unwrap();
        assert!(read(&skill, Path::new("binary")).is_err());
        let relative: PathBuf = (0..MAX_RESOURCE_DEPTH + 1).map(|_| "nested").collect();
        fs::create_dir_all(root.join(relative)).unwrap();
        assert!(files(&skill).is_err());
        fs::write(
            root.join("SKILL.md"),
            format!("---\n# {}\n---\n", "x".repeat(MAX_HEADER_BYTES)),
        )
        .unwrap();
        let catalog = discover(&[root]).unwrap();
        assert!(catalog.skills.is_empty());
        assert_eq!(catalog.warnings.len(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn traverses_execute_only_ancestor_as_unprivileged_user() {
        use std::os::unix::fs::PermissionsExt;
        if unsafe { libc::geteuid() } != 0 {
            return;
        }
        let temp = TempDir::new_in("/tmp").unwrap();
        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o711)).unwrap();
        let guard = temp.path().join("restricted");
        fs::create_dir(&guard).unwrap();
        fs::set_permissions(&guard, fs::Permissions::from_mode(0o711)).unwrap();
        let workspace = guard.join("skills");
        fs::create_dir(&workspace).unwrap();
        let root = create_skill(&workspace, "restricted-skill");
        fs::create_dir(root.join("references")).unwrap();
        fs::write(root.join("references/note.md"), "Accessible\n").unwrap();
        for entry in [
            &workspace,
            &root,
            &root.join("SKILL.md"),
            &root.join("references"),
            &root.join("references/note.md"),
        ] {
            let mode = if entry.is_dir() { 0o755 } else { 0o644 };
            fs::set_permissions(entry, fs::Permissions::from_mode(mode)).unwrap();
        }
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0);
        if pid == 0 {
            let ok = unsafe { libc::setgid(65534) == 0 && libc::setuid(65534) == 0 }
                && discover(&[workspace])
                    .and_then(|cat| {
                        let skill = cat.skills.into_iter().next().context("missing skill")?;
                        let _ = show(&skill)?;
                        let list = files(&skill)?;
                        ensure!(list == ["references/note.md"]);
                        let body = read(&skill, Path::new("references/note.md"))?;
                        ensure!(body == "Accessible\n");
                        Ok(())
                    })
                    .is_ok();
            unsafe { libc::_exit(if ok { 0 } else { 1 }) };
        }
        let mut status = 0;
        assert_eq!(unsafe { libc::waitpid(pid, &mut status, 0) }, pid);
        assert!(libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0);
    }
}
