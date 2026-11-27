//! Static DAG gate enforcing the full-repo L0-L8 strict lower-triangular module hierarchy.
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

/// Full-repo L0-L8 module layering table (16 library modules + `main` binary entry point).
const MODULE_LAYERS: &[(&str, u8)] = &[
    // L0: zero internal crate:: dependencies
    ("budget", 0),
    ("discovery", 0),
    ("platform", 0),
    ("oauth_url", 0),
    // L1
    ("json", 1),
    ("agents", 1),
    ("skills", 1),
    // L2
    ("config", 2),
    ("ipc", 2),
    ("events_exec", 2),
    // L3
    ("runtime", 3),
    // L4
    ("media", 4),
    ("auth", 4),
    // L5
    ("mcp", 5),
    // L6
    ("daemon", 6),
    // L7
    ("events_stream", 7),
    // L8
    ("main", 8),
];

const L0_MODULES: &[&str] = &["budget", "discovery", "platform", "oauth_url"];

#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    Ident(String),
    ColonColon,
    Hash,
    LBracket,
    RBracket,
    LBrace,
    RBrace,
    LParen,
    RParen,
    Semi,
    Comma,
    Other,
}

fn tokenize(src: &str) -> Vec<Token> {
    let bytes = src.as_bytes();
    let len = bytes.len();
    let mut i = 0;
    let mut tokens = Vec::new();

    while i < len {
        let b = bytes[i];
        // Whitespace
        if b.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        // Line comment
        if b == b'/' && i + 1 < len && bytes[i + 1] == b'/' {
            i += 2;
            while i < len && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        // Nested block comment
        if b == b'/' && i + 1 < len && bytes[i + 1] == b'*' {
            i += 2;
            let mut depth = 1usize;
            while i + 1 < len && depth > 0 {
                if bytes[i] == b'/' && bytes[i + 1] == b'*' {
                    depth += 1;
                    i += 2;
                } else if bytes[i] == b'*' && bytes[i + 1] == b'/' {
                    depth -= 1;
                    i += 2;
                } else {
                    i += 1;
                }
            }
            if depth > 0 {
                i = len;
            }
            continue;
        }
        // Raw string literal: r"...", r#"..."#, br"...", br#"..."#, cr"...", cr#"..."#
        if let Some(after_raw) = try_skip_raw_string(bytes, i) {
            i = after_raw;
            continue;
        }
        // Byte/C string literal prefix: b"...", c"..."
        if matches!(b, b'b' | b'c') && i + 1 < len && bytes[i + 1] == b'"' {
            i = skip_quoted_string(bytes, i + 1);
            continue;
        }
        // Byte char literal: b'...'
        if b == b'b'
            && i + 1 < len
            && bytes[i + 1] == b'\''
            && let Some(after_char) = try_skip_char_literal(bytes, i + 1)
        {
            i = after_char;
            continue;
        }
        // Normal string literal
        if b == b'"' {
            i = skip_quoted_string(bytes, i);
            continue;
        }
        // Char literal vs lifetime
        if b == b'\'' {
            if let Some(after_char) = try_skip_char_literal(bytes, i) {
                i = after_char;
            } else {
                // Lifetime token: skip '\'' and following identifier chars
                i += 1;
                while i < len && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
            }
            continue;
        }
        // Identifier
        if b.is_ascii_alphabetic() || b == b'_' {
            let start = i;
            i += 1;
            while i < len && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
            tokens.push(Token::Ident(src[start..i].to_owned()));
            continue;
        }
        // Two-char punctuation
        if b == b':' && i + 1 < len && bytes[i + 1] == b':' {
            tokens.push(Token::ColonColon);
            i += 2;
            continue;
        }
        let tok = match b {
            b'#' => Token::Hash,
            b'[' => Token::LBracket,
            b']' => Token::RBracket,
            b'{' => Token::LBrace,
            b'}' => Token::RBrace,
            b'(' => Token::LParen,
            b')' => Token::RParen,
            b';' => Token::Semi,
            b',' => Token::Comma,
            _ => Token::Other,
        };
        tokens.push(tok);
        i += 1;
    }

    tokens
}

fn try_skip_raw_string(bytes: &[u8], start: usize) -> Option<usize> {
    let len = bytes.len();
    let mut i = start;
    if matches!(bytes[i], b'b' | b'c') {
        i += 1;
        if i >= len || bytes[i] != b'r' {
            return None;
        }
    } else if bytes[i] != b'r' {
        return None;
    }
    i += 1;
    let mut hashes = 0usize;
    while i < len && bytes[i] == b'#' {
        hashes += 1;
        i += 1;
    }
    if i >= len || bytes[i] != b'"' {
        return None;
    }
    i += 1;
    while i < len {
        if bytes[i] == b'"' {
            let mut matched = 0usize;
            while matched < hashes && i + 1 + matched < len && bytes[i + 1 + matched] == b'#' {
                matched += 1;
            }
            if matched == hashes {
                return Some(i + 1 + hashes);
            }
        }
        i += 1;
    }
    Some(len)
}

fn skip_quoted_string(bytes: &[u8], quote_pos: usize) -> usize {
    let len = bytes.len();
    let mut i = quote_pos + 1;
    while i < len {
        match bytes[i] {
            b'\\' => i += 2,
            b'"' => return i + 1,
            _ => i += 1,
        }
    }
    len
}

fn try_skip_char_literal(bytes: &[u8], quote_pos: usize) -> Option<usize> {
    let len = bytes.len();
    let i = quote_pos + 1;
    if i >= len {
        return None;
    }
    if bytes[i] == b'\\' {
        let mut j = (i + 2).min(len);
        while j < len {
            if bytes[j] == b'\'' {
                return Some(j + 1);
            }
            j += 1;
        }
        return Some(len);
    }
    // Decode one UTF-8 scalar and check if immediately followed by '\''
    let rest = std::str::from_utf8(&bytes[i..]).ok()?;
    let ch = rest.chars().next()?;
    let next = i + ch.len_utf8();
    if next < len && bytes[next] == b'\'' {
        Some(next + 1)
    } else {
        None
    }
}

fn is_cfg_test_predicate(inner: &[Token]) -> bool {
    // Matches bare `test`
    if matches!(inner, [Token::Ident(s)] if s == "test") {
        return true;
    }
    // Matches `all(..., test, ...)` where `test` is a top-level predicate inside `all(...)`.
    if matches!(inner.first(), Some(Token::Ident(s)) if s == "all")
        && inner.get(1) == Some(&Token::LParen)
        && skip_balanced(inner, 1, &Token::LParen, &Token::RParen) == inner.len()
    {
        let args = &inner[2..inner.len() - 1];
        let mut depth = 0usize;
        let mut start = 0usize;
        for idx in 0..=args.len() {
            let at_sep = idx == args.len() || (depth == 0 && args[idx] == Token::Comma);
            if at_sep {
                if matches!(&args[start..idx], [Token::Ident(s)] if s == "test") {
                    return true;
                }
                start = idx + 1;
            } else {
                match args[idx] {
                    Token::LParen => depth += 1,
                    Token::RParen => depth = depth.saturating_sub(1),
                    _ => {}
                }
            }
        }
    }
    false
}

fn is_cfg_test_attr(tokens: &[Token], i: usize) -> Option<usize> {
    // Matches: # [ cfg ( test ) ] and # [ cfg ( all ( ..., test, ... ) ) ]
    if tokens.get(i) != Some(&Token::Hash) || tokens.get(i + 1) != Some(&Token::LBracket) {
        return None;
    }
    let attr_end = skip_balanced(tokens, i + 1, &Token::LBracket, &Token::RBracket);
    if attr_end < i + 6 {
        return None;
    }
    let inside_bracket = &tokens[i + 2..attr_end - 1];
    if !matches!(inside_bracket.first(), Some(Token::Ident(s)) if s == "cfg")
        || inside_bracket.get(1) != Some(&Token::LParen)
        || skip_balanced(inside_bracket, 1, &Token::LParen, &Token::RParen) != inside_bracket.len()
    {
        return None;
    }
    let inner = &inside_bracket[2..inside_bracket.len() - 1];
    if is_cfg_test_predicate(inner) {
        Some(attr_end)
    } else {
        None
    }
}

fn skip_balanced(tokens: &[Token], start: usize, open: &Token, close: &Token) -> usize {
    let mut i = start;
    let mut depth = 0usize;
    while i < tokens.len() {
        if &tokens[i] == open {
            depth += 1;
        } else if &tokens[i] == close {
            depth = depth.saturating_sub(1);
            if depth == 0 {
                return i + 1;
            }
        }
        i += 1;
    }
    tokens.len()
}

fn skip_cfg_test_item(tokens: &[Token], mut i: usize) -> usize {
    // Skip any additional outer attributes attached to the same item.
    while tokens.get(i) == Some(&Token::Hash) && tokens.get(i + 1) == Some(&Token::LBracket) {
        i = skip_balanced(tokens, i + 1, &Token::LBracket, &Token::RBracket);
    }
    // Check if the item is a semicolon-terminated statement (use / const / static / type).
    let mut probe = i;
    if matches!(tokens.get(probe), Some(Token::Ident(s)) if s == "pub") {
        probe += 1;
        if tokens.get(probe) == Some(&Token::LParen) {
            probe = skip_balanced(tokens, probe, &Token::LParen, &Token::RParen);
        }
    }
    let semicolon_terminated = match tokens.get(probe) {
        Some(Token::Ident(s)) if matches!(s.as_str(), "use" | "static" | "type") => true,
        Some(Token::Ident(s)) if s == "const" => !matches!(
            tokens.get(probe + 1),
            Some(Token::Ident(next)) if matches!(next.as_str(), "fn" | "unsafe" | "extern")
        ),
        _ => false,
    };

    if semicolon_terminated {
        let mut depth = 0usize;
        while i < tokens.len() {
            match tokens[i] {
                Token::LBrace => depth += 1,
                Token::RBrace => depth = depth.saturating_sub(1),
                Token::Semi if depth == 0 => return i + 1,
                _ => {}
            }
            i += 1;
        }
        return tokens.len();
    }

    // Block item (mod/fn/struct/enum/impl/trait) or `mod tests;`
    while i < tokens.len() {
        match tokens[i] {
            Token::Semi => return i + 1,
            Token::LBrace => return skip_balanced(tokens, i, &Token::LBrace, &Token::RBrace),
            _ => i += 1,
        }
    }
    tokens.len()
}

fn strip_cfg_test(tokens: &[Token]) -> Vec<Token> {
    let mut out = Vec::with_capacity(tokens.len());
    let mut i = 0;
    while i < tokens.len() {
        if let Some(after_attr) = is_cfg_test_attr(tokens, i) {
            i = skip_cfg_test_item(tokens, after_attr);
            continue;
        }
        out.push(tokens[i].clone());
        i += 1;
    }
    out
}

fn extract_dependencies(src: &str, root_ident: &str, self_module: &str) -> BTreeSet<String> {
    let tokens = strip_cfg_test(&tokenize(src));
    let mut deps = BTreeSet::new();
    let mut i = 0;

    while i + 2 < tokens.len() {
        let is_root = matches!(&tokens[i], Token::Ident(id) if id == root_ident);
        if is_root && tokens[i + 1] == Token::ColonColon {
            match &tokens[i + 2] {
                Token::Ident(target) => {
                    if target != self_module && target != "self" {
                        deps.insert(target.clone());
                    }
                    i += 3;
                    continue;
                }
                Token::LBrace => {
                    // Parse top-level comma-separated module names inside `root::{ ... }`.
                    let mut j = i + 3;
                    let mut depth = 0usize;
                    let mut expect_module = true;
                    while j < tokens.len() {
                        match &tokens[j] {
                            Token::LBrace => {
                                depth += 1;
                                expect_module = false;
                            }
                            Token::RBrace => {
                                if depth == 0 {
                                    j += 1;
                                    break;
                                }
                                depth -= 1;
                            }
                            Token::Comma if depth == 0 => {
                                expect_module = true;
                            }
                            Token::Ident(target) if depth == 0 && expect_module => {
                                if target != self_module && target != "self" {
                                    deps.insert(target.clone());
                                }
                                expect_module = false;
                            }
                            _ => {
                                if depth == 0 {
                                    expect_module = false;
                                }
                            }
                        }
                        j += 1;
                    }
                    i = j;
                    continue;
                }
                _ => {}
            }
        }
        i += 1;
    }

    deps
}

fn collect_rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let mut entries: Vec<_> = fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", dir.display()))
        .map(|entry| entry.expect("dir entry").path())
        .collect();
    entries.sort();
    for path in entries {
        if path.is_dir() {
            collect_rs_files(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

fn module_for_src_file(src_root: &Path, file: &Path) -> Option<String> {
    let rel = file.strip_prefix(src_root).expect("file under src/");
    let rel_str = rel.to_string_lossy().replace('\\', "/");
    if rel_str == "lib.rs" || rel_str == "platform/tests.rs" {
        return None;
    }
    let first = rel.components().next().expect("non-empty relative path");
    let first_str = first.as_os_str().to_string_lossy();
    let module = first_str.strip_suffix(".rs").unwrap_or(&first_str);
    Some(module.to_owned())
}

fn is_top_level_module_file(src_root: &Path, file: &Path) -> bool {
    let rel = file.strip_prefix(src_root).expect("file under src/");
    let components: Vec<_> = rel.components().collect();
    match components.as_slice() {
        [single] => {
            let name = single.as_os_str().to_string_lossy();
            name != "lib.rs" && name != "main.rs"
        }
        [_, second] => second.as_os_str() == "mod.rs",
        _ => false,
    }
}

fn extract_top_level_super_dependencies(src: &str, self_module: &str) -> BTreeSet<String> {
    let layers = layer_map();
    extract_dependencies(src, "super", self_module)
        .into_iter()
        .filter(|dep| layers.contains_key(dep.as_str()))
        .collect()
}

fn scan_production_graph() -> (BTreeSet<String>, BTreeMap<String, BTreeSet<String>>) {
    let src_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    collect_rs_files(&src_root, &mut files);

    let mut modules = BTreeSet::new();
    let mut graph: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();

    for file in files {
        let Some(module) = module_for_src_file(&src_root, &file) else {
            continue;
        };
        modules.insert(module.clone());
        let content = fs::read_to_string(&file)
            .unwrap_or_else(|e| panic!("failed to read {}: {e}", file.display()));

        let entry = graph.entry(module.clone()).or_default();
        entry.extend(extract_dependencies(&content, "crate", &module));
        if is_top_level_module_file(&src_root, &file) {
            entry.extend(extract_top_level_super_dependencies(&content, &module));
        }
        if module == "main" {
            entry.extend(extract_dependencies(&content, "mcpcli", &module));
        }
    }

    (modules, graph)
}

fn layer_map() -> BTreeMap<&'static str, u8> {
    MODULE_LAYERS.iter().copied().collect()
}

#[test]
fn all_src_modules_are_registered_in_layer_table() {
    let (discovered, _) = scan_production_graph();
    let layers = layer_map();

    let unregistered: Vec<_> = discovered
        .iter()
        .filter(|m| !layers.contains_key(m.as_str()))
        .cloned()
        .collect();
    assert!(
        unregistered.is_empty(),
        "unregistered src/ module(s) not in L0-L8 table: {unregistered:?}"
    );

    let stale: Vec<_> = layers
        .keys()
        .filter(|m| !discovered.contains(**m))
        .copied()
        .collect();
    assert!(
        stale.is_empty(),
        "stale module(s) in L0-L8 table not found in src/: {stale:?}"
    );

    // 16 library modules + main binary entry point
    assert_eq!(layers.len(), 17);
}

#[test]
fn production_dependencies_form_strict_lower_triangular_dag() {
    let (_, graph) = scan_production_graph();
    let layers = layer_map();
    let mut violations = Vec::new();

    for (src, deps) in &graph {
        let Some(&src_layer) = layers.get(src.as_str()) else {
            violations.push(format!("unregistered source module: {src}"));
            continue;
        };
        for dst in deps {
            let Some(&dst_layer) = layers.get(dst.as_str()) else {
                violations.push(format!(
                    "{src} (L{src_layer}) references unregistered crate module `{dst}`"
                ));
                continue;
            };
            if src_layer <= dst_layer {
                violations.push(format!(
                    "{src} (L{src_layer}) -> {dst} (L{dst_layer}) violates strict lower-triangular DAG (layer(src) must be > layer(dst))"
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "DAG violations found:\n{}",
        violations.join("\n")
    );
}

#[test]
fn l0_modules_have_zero_internal_crate_dependencies() {
    let (_, graph) = scan_production_graph();
    for &module in L0_MODULES {
        let deps = graph.get(module).cloned().unwrap_or_default();
        assert!(
            deps.is_empty(),
            "L0 module `{module}` must have zero internal crate:: dependencies, found: {deps:?}"
        );
    }
}

#[test]
fn cross_layer_leaks_remain_eliminated() {
    let (_, graph) = scan_production_graph();

    let mcp_deps = graph.get("mcp").cloned().unwrap_or_default();
    assert!(
        !mcp_deps.contains("ipc"),
        "L5 `mcp` must not depend on `ipc`, found deps: {mcp_deps:?}"
    );

    let events_exec_deps = graph.get("events_exec").cloned().unwrap_or_default();
    assert!(
        !events_exec_deps.contains("mcp"),
        "L2 `events_exec` must not depend on `mcp`, found deps: {events_exec_deps:?}"
    );
}

#[test]
fn scanner_handles_multiline_uses_comments_strings_and_cfg_test() {
    let sample = r###"
        // crate::fake_comment::foo
        /* nested /* crate::fake_nested::bar */ crate::fake_block::baz */
        use crate::{
            alpha::Thing,
            beta::{One, Two},
            gamma,
        };
        use crate::delta::item;
        const S: &str = "crate::fake_str::x";
        const R: &str = r#"crate::fake_raw::y"#;
        fn call(c: char) {
            let _ = crate::epsilon::run('a', '\'');
            let _ = matches!(c, '\''|'"');
            crate::zeta::call();
        }
        #[cfg(test)]
        const TEST_ONLY: &str = "ok";
        #[cfg(test)]
        const fn test_only_const_fn() -> usize {
            let _ = crate::ignored_const_fn::value();
            1
        }
        #[cfg(all(unix, test))]
        fn unix_test_helper() {
            let _ = crate::ignored_unix_test::call();
        }
        #[cfg(all(test, not(windows)))]
        mod nested_cfg_tests {
            use crate::ignored_all_test::secret;
        }
        #[cfg(not(test))]
        fn prod_only() {
            crate::eta::run();
        }
        #[cfg(test)]
        mod tests {
            use crate::forbidden_in_prod::secret;
            fn helper() {
                let _ = crate::another_test_only::call();
            }
        }
    "###;

    let deps = extract_dependencies(sample, "crate", "self_mod");
    let expected: BTreeSet<String> = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta"]
        .into_iter()
        .map(str::to_owned)
        .collect();
    assert_eq!(deps, expected);
}

#[test]
fn scanner_checks_top_level_super_module_references() {
    let sample = r#"
        use super::config::LoadedConfig;
        use super::{platform, ipc::Failure, not_a_top_level_module::Item};
        fn f() {
            let _ = super::budget::ByteBudget::global();
            let _ = super::unknown_helper();
        }
    "#;

    let deps = extract_top_level_super_dependencies(sample, "runtime");
    let expected: BTreeSet<String> = ["budget", "config", "ipc", "platform"]
        .into_iter()
        .map(str::to_owned)
        .collect();
    assert_eq!(deps, expected);
}
