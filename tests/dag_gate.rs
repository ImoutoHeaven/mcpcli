//! Static DAG gate enforcing the full-repo L0-L8 strict lower-triangular module hierarchy.
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

/// Full-repo L0-L8 module layering table (17 library modules + `main` binary entry point).
const MODULE_LAYERS: &[(&str, u8)] = &[
    // L0: zero internal crate:: dependencies
    ("budget", 0),
    ("discovery", 0),
    ("platform", 0),
    ("oauth_url", 0),
    // L1
    ("schema", 1),
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
const RMCP_ALLOWED: &[&str] = &["mcp"];
const SERDE_SAPHYR_ALLOWED: &[&str] = &["skills"];
const SKILLS_ALLOWED: &[&str] = &["platform"];
const MAX_PROD_LINES: usize = 1500;

/// Lateral dependency allowlist among `src/mcp/*.rs` sibling submodules.
const MCP_SUBMODULE_DAG: &[(&str, &[&str])] = &[
    ("types", &[]),
    ("ingress", &["types"]),
    ("sse", &["types", "ingress"]),
    ("events", &["types", "ingress", "sse"]),
    ("stdio", &["types", "ingress"]),
    ("http", &["types", "ingress", "sse", "events"]),
    ("events_client", &["types", "ingress", "events", "stdio"]),
    (
        "connection",
        &[
            "types",
            "ingress",
            "sse",
            "events",
            "stdio",
            "http",
            "events_client",
        ],
    ),
    ("skills", &["types", "ingress", "connection"]),
    ("tasks", &["types", "ingress", "connection"]),
];

#[derive(Debug, Clone, PartialEq, Eq)]
enum TokenKind {
    Ident(String),
    Literal,
    ColonColon,
    Hash,
    Bang,
    LBracket,
    RBracket,
    LBrace,
    RBrace,
    LParen,
    RParen,
    Lt,
    Gt,
    Semi,
    Comma,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Token {
    kind: TokenKind,
    line: usize,
}

fn tokenize(src: &str) -> Vec<Token> {
    let bytes = src.as_bytes();
    let len = bytes.len();
    let mut i = 0;
    let mut line = 1usize;
    let mut tokens = Vec::new();

    while i < len {
        let b = bytes[i];
        if b == b'\n' {
            line += 1;
            i += 1;
            continue;
        }
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
            while i < len && depth > 0 {
                if bytes[i] == b'/' && i + 1 < len && bytes[i + 1] == b'*' {
                    depth += 1;
                    i += 2;
                } else if bytes[i] == b'*' && i + 1 < len && bytes[i + 1] == b'/' {
                    depth -= 1;
                    i += 2;
                } else {
                    if bytes[i] == b'\n' {
                        line += 1;
                    }
                    i += 1;
                }
            }
            continue;
        }
        // Raw string literal: r"...", r#"..."#, br/rb/cr/rc variants
        if let Some(after_raw) = try_skip_raw_string(bytes, i, &mut line) {
            tokens.push(Token {
                kind: TokenKind::Literal,
                line,
            });
            i = after_raw;
            continue;
        }
        // Byte/C string literal prefix: b"...", c"..."
        if matches!(b, b'b' | b'c') && i + 1 < len && bytes[i + 1] == b'"' {
            i = skip_quoted_string(bytes, i + 1, &mut line);
            tokens.push(Token {
                kind: TokenKind::Literal,
                line,
            });
            continue;
        }
        // Byte char literal: b'...'
        if b == b'b'
            && i + 1 < len
            && bytes[i + 1] == b'\''
            && let Some(after_char) = try_skip_char_literal(bytes, i + 1, &mut line)
        {
            tokens.push(Token {
                kind: TokenKind::Literal,
                line,
            });
            i = after_char;
            continue;
        }
        // Normal string literal
        if b == b'"' {
            i = skip_quoted_string(bytes, i, &mut line);
            tokens.push(Token {
                kind: TokenKind::Literal,
                line,
            });
            continue;
        }
        // Char literal vs lifetime
        if b == b'\'' {
            if let Some(after_char) = try_skip_char_literal(bytes, i, &mut line) {
                tokens.push(Token {
                    kind: TokenKind::Literal,
                    line,
                });
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
            tokens.push(Token {
                kind: TokenKind::Ident(src[start..i].to_owned()),
                line,
            });
            continue;
        }
        // Two-char punctuation
        if b == b':' && i + 1 < len && bytes[i + 1] == b':' {
            tokens.push(Token {
                kind: TokenKind::ColonColon,
                line,
            });
            i += 2;
            continue;
        }
        if matches!(b, b'-' | b'=') && i + 1 < len && bytes[i + 1] == b'>' {
            tokens.push(Token {
                kind: TokenKind::Other,
                line,
            });
            i += 2;
            continue;
        }
        let kind = match b {
            b'#' => TokenKind::Hash,
            b'!' => TokenKind::Bang,
            b'[' => TokenKind::LBracket,
            b']' => TokenKind::RBracket,
            b'{' => TokenKind::LBrace,
            b'}' => TokenKind::RBrace,
            b'(' => TokenKind::LParen,
            b')' => TokenKind::RParen,
            b'<' => TokenKind::Lt,
            b'>' => TokenKind::Gt,
            b';' => TokenKind::Semi,
            b',' => TokenKind::Comma,
            _ => TokenKind::Other,
        };
        tokens.push(Token { kind, line });
        i += 1;
    }

    tokens
}

fn try_skip_raw_string(bytes: &[u8], start: usize, line: &mut usize) -> Option<usize> {
    let len = bytes.len();
    let r_offset = match (bytes.get(start).copied(), bytes.get(start + 1).copied()) {
        (Some(b'b' | b'c'), Some(b'r')) | (Some(b'r'), Some(b'b' | b'c')) => 2,
        (Some(b'r'), _) => 1,
        _ => return None,
    };
    let mut i = start + r_offset;
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
        if bytes[i] == b'\n' {
            *line += 1;
            i += 1;
            continue;
        }
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

fn skip_quoted_string(bytes: &[u8], quote_pos: usize, line: &mut usize) -> usize {
    let len = bytes.len();
    let mut i = quote_pos + 1;
    while i < len {
        match bytes[i] {
            b'\\' => {
                if i + 1 < len && bytes[i + 1] == b'\n' {
                    *line += 1;
                }
                i += 2;
            }
            b'"' => return i + 1,
            b'\n' => {
                *line += 1;
                i += 1;
            }
            _ => i += 1,
        }
    }
    len
}

fn try_skip_char_literal(bytes: &[u8], quote_pos: usize, line: &mut usize) -> Option<usize> {
    let len = bytes.len();
    let i = quote_pos + 1;
    if i >= len {
        return None;
    }
    if bytes[i] == b'\\' {
        let mut j = (i + 2).min(len);
        if i + 1 < len && bytes[i + 1] == b'\n' {
            *line += 1;
        }
        while j < len {
            if bytes[j] == b'\n' {
                *line += 1;
            }
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
        if ch == '\n' {
            *line += 1;
        }
        Some(next + 1)
    } else {
        None
    }
}

fn is_cfg_test_predicate(inner: &[Token]) -> bool {
    // Matches bare `test`
    if matches!(inner.first().map(|t| &t.kind), Some(TokenKind::Ident(s)) if s == "test")
        && inner.len() == 1
    {
        return true;
    }
    // Matches `all(..., test, ...)` where `test` is a top-level predicate inside `all(...)`.
    if matches!(inner.first().map(|t| &t.kind), Some(TokenKind::Ident(s)) if s == "all")
        && inner.get(1).map(|t| &t.kind) == Some(&TokenKind::LParen)
        && skip_balanced(inner, 1, &TokenKind::LParen, &TokenKind::RParen) == inner.len()
    {
        let args = &inner[2..inner.len() - 1];
        let mut depth = 0usize;
        let mut start = 0usize;
        for idx in 0..=args.len() {
            let at_sep = idx == args.len() || (depth == 0 && args[idx].kind == TokenKind::Comma);
            if at_sep {
                if args[start..idx].len() == 1
                    && matches!(&args[start].kind, TokenKind::Ident(s) if s == "test")
                {
                    return true;
                }
                start = idx + 1;
            } else {
                match args[idx].kind {
                    TokenKind::LParen => depth += 1,
                    TokenKind::RParen => depth = depth.saturating_sub(1),
                    _ => {}
                }
            }
        }
    }
    false
}

/// Parses `#![...]` (`is_inner = true`) or `#[...]` (`is_inner = false`), returning
/// `(after_bracket, is_inner, is_cfg_test)`.
fn parse_attr(tokens: &[Token], i: usize) -> Option<(usize, bool, bool)> {
    if tokens.get(i)?.kind != TokenKind::Hash {
        return None;
    }
    let (is_inner, bracket_idx) = match (
        tokens.get(i + 1).map(|t| &t.kind),
        tokens.get(i + 2).map(|t| &t.kind),
    ) {
        (Some(TokenKind::Bang), Some(TokenKind::LBracket)) => (true, i + 2),
        (Some(TokenKind::LBracket), _) => (false, i + 1),
        _ => return None,
    };
    let attr_end = skip_balanced(
        tokens,
        bracket_idx,
        &TokenKind::LBracket,
        &TokenKind::RBracket,
    );
    if attr_end <= bracket_idx + 1 {
        return Some((attr_end, is_inner, false));
    }
    let inside_bracket = &tokens[bracket_idx + 1..attr_end - 1];
    let is_cfg_test = matches!(inside_bracket.first().map(|t| &t.kind), Some(TokenKind::Ident(s)) if s == "cfg")
        && inside_bracket.get(1).map(|t| &t.kind) == Some(&TokenKind::LParen)
        && skip_balanced(inside_bracket, 1, &TokenKind::LParen, &TokenKind::RParen)
            == inside_bracket.len()
        && is_cfg_test_predicate(&inside_bracket[2..inside_bracket.len() - 1]);
    Some((attr_end, is_inner, is_cfg_test))
}

fn skip_balanced(tokens: &[Token], start: usize, open: &TokenKind, close: &TokenKind) -> usize {
    let mut i = start;
    let mut depth = 0usize;
    while i < tokens.len() {
        if &tokens[i].kind == open {
            depth += 1;
        } else if &tokens[i].kind == close {
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
    while let Some((after_attr, false, _)) = parse_attr(tokens, i) {
        i = after_attr;
    }

    // Advance past visibility and item modifiers to classify the gated target.
    let mut probe = i;
    loop {
        if matches!(tokens.get(probe).map(|t| &t.kind), Some(TokenKind::Ident(s)) if s == "pub") {
            probe += 1;
            if tokens.get(probe).map(|t| &t.kind) == Some(&TokenKind::LParen) {
                probe = skip_balanced(tokens, probe, &TokenKind::LParen, &TokenKind::RParen);
            }
            continue;
        }
        if matches!(
            tokens.get(probe).map(|t| &t.kind),
            Some(TokenKind::Ident(s)) if matches!(s.as_str(), "default" | "async" | "unsafe")
        ) {
            probe += 1;
            continue;
        }
        if matches!(tokens.get(probe).map(|t| &t.kind), Some(TokenKind::Ident(s)) if s == "const")
            && matches!(
                tokens.get(probe + 1).map(|t| &t.kind),
                Some(TokenKind::Ident(next)) if matches!(next.as_str(), "fn" | "unsafe" | "extern")
            )
        {
            probe += 1;
            continue;
        }
        if matches!(tokens.get(probe).map(|t| &t.kind), Some(TokenKind::Ident(s)) if s == "extern")
            && !matches!(tokens.get(probe + 1).map(|t| &t.kind), Some(TokenKind::Ident(next)) if next == "crate")
        {
            probe += 1;
            if tokens.get(probe).map(|t| &t.kind) == Some(&TokenKind::Literal) {
                probe += 1;
            }
            continue;
        }
        break;
    }

    let Some(lead) = tokens.get(probe) else {
        return tokens.len();
    };

    // Semicolon-terminated declarations (`use`, `static`, `type`, `const`, `let`, `extern crate`).
    let is_extern_crate = matches!(&lead.kind, TokenKind::Ident(s) if s == "extern")
        && matches!(tokens.get(probe + 1).map(|t| &t.kind), Some(TokenKind::Ident(next)) if next == "crate");
    let semicolon_terminated = is_extern_crate
        || matches!(
            &lead.kind,
            TokenKind::Ident(s) if matches!(s.as_str(), "use" | "static" | "type" | "const" | "let")
        );

    if semicolon_terminated {
        let (mut paren, mut bracket, mut brace) = (0usize, 0usize, 0usize);
        while i < tokens.len() {
            match tokens[i].kind {
                TokenKind::LParen => paren += 1,
                TokenKind::RParen => paren = paren.saturating_sub(1),
                TokenKind::LBracket => bracket += 1,
                TokenKind::RBracket => bracket = bracket.saturating_sub(1),
                TokenKind::LBrace => brace += 1,
                TokenKind::RBrace => brace = brace.saturating_sub(1),
                TokenKind::Semi if paren == 0 && bracket == 0 && brace == 0 => return i + 1,
                _ => {}
            }
            i += 1;
        }
        return tokens.len();
    }

    // Block items (`mod`, `fn`, `struct`, `enum`, `union`, `trait`, `impl`, `macro_rules`) or `mod tests;`
    let is_brace_macro = matches!(&lead.kind, TokenKind::Ident(_))
        && tokens.get(probe + 1).map(|t| &t.kind) == Some(&TokenKind::Bang)
        && tokens.get(probe + 2).map(|t| &t.kind) == Some(&TokenKind::LBrace);
    let is_block_item = is_brace_macro
        || matches!(
            &lead.kind,
            TokenKind::Ident(s)
                if matches!(
                    s.as_str(),
                    "fn" | "struct" | "enum" | "union" | "trait" | "impl" | "mod" | "macro_rules"
                )
        );

    if is_block_item {
        let (mut paren, mut bracket) = (0usize, 0usize);
        while i < tokens.len() {
            match tokens[i].kind {
                TokenKind::LParen => paren += 1,
                TokenKind::RParen => paren = paren.saturating_sub(1),
                TokenKind::LBracket => bracket += 1,
                TokenKind::RBracket => bracket = bracket.saturating_sub(1),
                TokenKind::Semi if paren == 0 && bracket == 0 => return i + 1,
                TokenKind::LBrace if paren == 0 && bracket == 0 => {
                    return skip_balanced(tokens, i, &TokenKind::LBrace, &TokenKind::RBrace);
                }
                _ => {}
            }
            i += 1;
        }
        return tokens.len();
    }

    // Direct block `{ ... }`.
    if lead.kind == TokenKind::LBrace {
        i = skip_balanced(tokens, probe, &TokenKind::LBrace, &TokenKind::RBrace);
        if matches!(
            tokens.get(i).map(|t| &t.kind),
            Some(TokenKind::Semi | TokenKind::Comma)
        ) {
            i += 1;
        }
        return i;
    }

    // Struct/enum fields, parameters, match arms, or call arguments: stop at `,`/`;` or before a closing delimiter.
    let (mut paren, mut bracket, mut brace, mut angle) = (0usize, 0usize, 0usize, 0usize);
    while i < tokens.len() {
        if paren == 0 && bracket == 0 && brace == 0 {
            if matches!(
                tokens[i].kind,
                TokenKind::RBrace | TokenKind::RParen | TokenKind::RBracket
            ) {
                return i;
            }
            if angle == 0 && matches!(tokens[i].kind, TokenKind::Comma | TokenKind::Semi) {
                return i + 1;
            }
            match tokens[i].kind {
                TokenKind::Lt => angle += 1,
                TokenKind::Gt => angle = angle.saturating_sub(1),
                _ => {}
            }
        }
        match tokens[i].kind {
            TokenKind::LParen => paren += 1,
            TokenKind::RParen => paren = paren.saturating_sub(1),
            TokenKind::LBracket => bracket += 1,
            TokenKind::RBracket => bracket = bracket.saturating_sub(1),
            TokenKind::LBrace => brace += 1,
            TokenKind::RBrace => brace = brace.saturating_sub(1),
            _ => {}
        }
        i += 1;
    }
    tokens.len()
}

fn strip_cfg_test_with_spans(tokens: &[Token]) -> (Vec<Token>, bool, Vec<(usize, usize)>) {
    let mut out = Vec::with_capacity(tokens.len());
    let mut spans = Vec::new();
    let mut scope_starts: Vec<(usize, usize)> = Vec::new();
    let mut brace_depth = 0usize;
    let mut i = 0;

    while i < tokens.len() {
        if let Some((after_attr, true, true)) = parse_attr(tokens, i) {
            if brace_depth == 0 {
                return (Vec::new(), true, Vec::new());
            }
            let start_line = if let Some(&(out_start, open_line)) = scope_starts.last() {
                out.truncate(out_start);
                open_line
            } else {
                tokens[i].line
            };
            let target_depth = brace_depth - 1;
            i = after_attr;
            while i < tokens.len() {
                if tokens[i].kind == TokenKind::LBrace {
                    brace_depth += 1;
                } else if tokens[i].kind == TokenKind::RBrace {
                    if brace_depth - 1 == target_depth {
                        if i > 0 {
                            spans.push((start_line, tokens[i - 1].line));
                        }
                        break;
                    }
                    brace_depth -= 1;
                }
                i += 1;
            }
            continue;
        }

        if matches!(parse_attr(tokens, i), Some((_, false, _))) {
            let mut cursor = i;
            let mut has_cfg_test = false;
            while let Some((after_attr, false, is_cfg_test)) = parse_attr(tokens, cursor) {
                if is_cfg_test {
                    has_cfg_test = true;
                }
                cursor = after_attr;
            }
            if has_cfg_test {
                let start_line = tokens[i].line;
                let end_idx = skip_cfg_test_item(tokens, cursor);
                if end_idx > i {
                    let end_line = tokens[end_idx - 1].line;
                    spans.push((start_line, end_line));
                }
                i = end_idx;
                continue;
            }
            out.extend_from_slice(&tokens[i..cursor]);
            i = cursor;
            continue;
        }

        if tokens[i].kind == TokenKind::LBrace {
            brace_depth += 1;
            let open_line = tokens[i].line;
            out.push(tokens[i].clone());
            scope_starts.push((out.len(), open_line));
            i += 1;
            continue;
        }
        if tokens[i].kind == TokenKind::RBrace {
            brace_depth = brace_depth.saturating_sub(1);
            scope_starts.pop();
        }
        out.push(tokens[i].clone());
        i += 1;
    }

    (out, false, spans)
}

fn strip_cfg_test(tokens: &[Token]) -> Vec<Token> {
    strip_cfg_test_with_spans(tokens).0
}

fn count_prod_lines(source: &str) -> usize {
    let total_lines = source.lines().count();
    if total_lines == 0 {
        return 0;
    }
    let tokens = tokenize(source);
    let (_, whole_file_test, mut spans) = strip_cfg_test_with_spans(&tokens);
    if whole_file_test {
        return 0;
    }
    if spans.is_empty() {
        return total_lines;
    }
    spans.sort_unstable();
    let mut excluded = 0usize;
    let mut cur_start = spans[0].0.max(1);
    let mut cur_end = spans[0].1.min(total_lines);
    for &(s, e) in &spans[1..] {
        let s = s.max(1);
        let e = e.min(total_lines);
        if s <= cur_end.saturating_add(1) {
            cur_end = cur_end.max(e);
        } else {
            if cur_end >= cur_start {
                excluded += cur_end - cur_start + 1;
            }
            cur_start = s;
            cur_end = e;
        }
    }
    if cur_end >= cur_start {
        excluded += cur_end - cur_start + 1;
    }
    total_lines.saturating_sub(excluded)
}

fn is_test_source_file(src_root: &Path, file: &Path) -> bool {
    if file.file_name().and_then(|n| n.to_str()) == Some("tests.rs") {
        return true;
    }
    let rel = file.strip_prefix(src_root).unwrap_or(file);
    rel.components().any(|comp| comp.as_os_str() == "tests")
}

fn prod_line_budget_ok(src_root: &Path, file: &Path, source: &str) -> bool {
    is_test_source_file(src_root, file) || count_prod_lines(source) <= MAX_PROD_LINES
}

fn extract_dependencies(src: &str, root_ident: &str, self_module: &str) -> BTreeSet<String> {
    let tokens = strip_cfg_test(&tokenize(src));
    let mut deps = BTreeSet::new();
    let mut i = 0;

    while i + 2 < tokens.len() {
        let is_root = matches!(&tokens[i].kind, TokenKind::Ident(id) if id == root_ident);
        if is_root && tokens[i + 1].kind == TokenKind::ColonColon {
            match &tokens[i + 2].kind {
                TokenKind::Ident(target) => {
                    if target != self_module && target != "self" {
                        deps.insert(target.clone());
                    }
                    i += 3;
                    continue;
                }
                TokenKind::LBrace => {
                    // Parse top-level comma-separated module names inside `root::{ ... }`.
                    let mut j = i + 3;
                    let mut depth = 0usize;
                    let mut expect_module = true;
                    while j < tokens.len() {
                        match &tokens[j].kind {
                            TokenKind::LBrace => {
                                depth += 1;
                                expect_module = false;
                            }
                            TokenKind::RBrace => {
                                if depth == 0 {
                                    j += 1;
                                    break;
                                }
                                depth -= 1;
                            }
                            TokenKind::Comma if depth == 0 => {
                                expect_module = true;
                            }
                            TokenKind::Ident(target) if depth == 0 && expect_module => {
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
    if rel_str == "lib.rs" {
        return None;
    }
    let first = rel.components().next().expect("non-empty relative path");
    let first_str = first.as_os_str().to_string_lossy();
    let module = first_str.strip_suffix(".rs").unwrap_or(&first_str);
    if module == "cli" {
        return Some("main".to_owned());
    }
    Some(module.to_owned())
}

fn has_module_path_override(src: &str) -> bool {
    let tokens = strip_cfg_test(&tokenize(src));
    let mut index = 0;
    while index + 1 < tokens.len() {
        if tokens[index].kind == TokenKind::Hash && tokens[index + 1].kind == TokenKind::LBracket {
            let end = skip_balanced(
                &tokens,
                index + 1,
                &TokenKind::LBracket,
                &TokenKind::RBracket,
            );
            if tokens[index + 2..end - 1].windows(2).any(|pair| {
                matches!(&pair[0].kind, TokenKind::Ident(name) if name == "path")
                    && pair[1].kind == TokenKind::Other
            }) {
                return true;
            }
            index = end;
        } else {
            index += 1;
        }
    }
    false
}

fn module_depth(src_root: &Path, file: &Path) -> usize {
    let relative = file.strip_prefix(src_root).expect("file under src/");
    if relative == Path::new("main.rs") {
        0
    } else {
        relative.components().count() - usize::from(file.file_name().unwrap() == "mod.rs")
    }
}

fn collect_relative_path(
    tokens: &[Token],
    mut index: usize,
    mut depth: usize,
    self_module: &str,
    deps: &mut BTreeSet<String>,
) -> usize {
    while let Some(TokenKind::Ident(name)) = tokens.get(index).map(|t| &t.kind) {
        if tokens.get(index + 1).map(|t| &t.kind) != Some(&TokenKind::ColonColon) {
            break;
        }
        if name == "super" {
            let Some(parent) = depth.checked_sub(1) else {
                return index + 2;
            };
            depth = parent;
        } else if name != "self" {
            break;
        }
        index += 2;
    }
    match tokens.get(index).map(|t| &t.kind) {
        Some(TokenKind::LBrace) => {
            let end = skip_balanced(tokens, index, &TokenKind::LBrace, &TokenKind::RBrace);
            index += 1;
            while index + 1 < end {
                index = collect_relative_path(tokens, index, depth, self_module, deps);
                while index + 1 < end && tokens[index].kind != TokenKind::Comma {
                    index = if tokens[index].kind == TokenKind::LBrace {
                        skip_balanced(tokens, index, &TokenKind::LBrace, &TokenKind::RBrace)
                    } else {
                        index + 1
                    };
                }
                index += 1;
            }
            end
        }
        Some(TokenKind::Ident(target)) => {
            if depth == 0 && target != self_module && layer_map().contains_key(target.as_str()) {
                deps.insert(target.clone());
            }
            index + 1
        }
        _ => index + 1,
    }
}

fn extract_super_dependencies(src: &str, self_module: &str, file_depth: usize) -> BTreeSet<String> {
    let tokens = strip_cfg_test(&tokenize(src));
    let mut deps = BTreeSet::new();
    let mut depth = file_depth;
    let mut scopes = Vec::new();
    let mut index = 0;
    while index < tokens.len() {
        if matches!(&tokens[index].kind, TokenKind::Ident(name) if name == "super")
            && tokens.get(index + 1).map(|t| &t.kind) == Some(&TokenKind::ColonColon)
        {
            index = collect_relative_path(&tokens, index, depth, self_module, &mut deps);
            continue;
        }
        match &tokens[index].kind {
            TokenKind::LBrace => {
                let module = index >= 2
                    && matches!(&tokens[index - 2].kind, TokenKind::Ident(name) if name == "mod")
                    && matches!(&tokens[index - 1].kind, TokenKind::Ident(_));
                scopes.push(module);
                depth += usize::from(module);
            }
            TokenKind::RBrace => {
                depth -= usize::from(scopes.pop().unwrap_or(false));
            }
            _ => {}
        }
        index += 1;
    }
    deps
}

fn extract_module_crate_dependencies(src: &str, module: &str) -> BTreeSet<String> {
    let own_module = if L0_MODULES.contains(&module) || module == "schema" {
        ""
    } else {
        module
    };
    let mut deps = extract_dependencies(src, "crate", own_module);
    if module == "main" {
        deps.remove("cli");
        deps.extend(extract_dependencies(src, "mcpcli", module));
    }
    deps
}

fn scan_production_graph() -> (BTreeSet<String>, BTreeMap<String, BTreeSet<String>>) {
    let src_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    collect_rs_files(&src_root, &mut files);

    let mut modules = BTreeSet::new();
    let mut graph: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();

    for file in files {
        if is_test_source_file(&src_root, &file) {
            continue;
        }
        let content = fs::read_to_string(&file)
            .unwrap_or_else(|e| panic!("failed to read {}: {e}", file.display()));
        assert!(
            !has_module_path_override(&content),
            "{}: DAG gate requires standard module file paths; remove the path attribute",
            file.display()
        );
        let Some(module) = module_for_src_file(&src_root, &file) else {
            continue;
        };
        modules.insert(module.clone());
        let entry = graph.entry(module.clone()).or_default();
        entry.extend(extract_module_crate_dependencies(&content, &module));
        entry.extend(extract_super_dependencies(
            &content,
            &module,
            module_depth(&src_root, &file),
        ));
    }

    (modules, graph)
}

fn layer_map() -> BTreeMap<&'static str, u8> {
    MODULE_LAYERS.iter().copied().collect()
}

fn skills_dependencies_allowed(deps: &BTreeSet<String>) -> bool {
    deps.iter()
        .all(|dependency| SKILLS_ALLOWED.contains(&dependency.as_str()))
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

    // 17 library modules + main binary entry point
    assert_eq!(layers.len(), 18);
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
fn foundational_modules_keep_their_dependency_contracts() {
    let (_, graph) = scan_production_graph();
    for &module in L0_MODULES {
        let deps = graph.get(module).cloned().unwrap_or_default();
        assert!(
            deps.is_empty(),
            "L0 module `{module}` must have zero internal crate:: dependencies, found: {deps:?}"
        );
    }
    let schema_deps = graph.get("schema").cloned().unwrap_or_default();
    assert!(
        schema_deps.is_empty(),
        "L1 module `schema` must have zero internal crate:: dependencies, found: {schema_deps:?}"
    );
    let skills_deps = graph.get("skills").cloned().unwrap_or_default();
    assert!(
        skills_dependencies_allowed(&skills_deps),
        "L1 module `skills` may depend only on {SKILLS_ALLOWED:?}, found: {skills_deps:?}"
    );
}

fn references_external_crate(src: &str, crate_ident: &str) -> bool {
    strip_cfg_test(&tokenize(src))
        .iter()
        .any(|tok| matches!(&tok.kind, TokenKind::Ident(id) if id == crate_ident))
}

fn check_external_crate_allowed(
    module: &str,
    src: &str,
    crate_ident: &str,
    allowed: &[&str],
) -> bool {
    allowed.contains(&module) || !references_external_crate(src, crate_ident)
}

#[test]
fn external_crate_boundaries_are_enforced() {
    let src_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    collect_rs_files(&src_root, &mut files);

    let mut violations = Vec::new();
    for file in files {
        if is_test_source_file(&src_root, &file) {
            continue;
        }
        let Some(module) = module_for_src_file(&src_root, &file) else {
            continue;
        };
        let content = fs::read_to_string(&file)
            .unwrap_or_else(|e| panic!("failed to read {}: {e}", file.display()));
        if !check_external_crate_allowed(&module, &content, "rmcp", RMCP_ALLOWED) {
            violations.push(format!(
                "{} (module `{module}`) references `rmcp` outside RMCP_ALLOWED ({RMCP_ALLOWED:?})",
                file.display()
            ));
        }
        if !check_external_crate_allowed(&module, &content, "serde_saphyr", SERDE_SAPHYR_ALLOWED) {
            violations.push(format!(
                "{} (module `{module}`) references `serde_saphyr` outside SERDE_SAPHYR_ALLOWED ({SERDE_SAPHYR_ALLOWED:?})",
                file.display()
            ));
        }
    }
    assert!(
        violations.is_empty(),
        "external crate boundary violations:\n{}",
        violations.join("\n")
    );
}

fn mcp_submodule_names() -> BTreeSet<&'static str> {
    MCP_SUBMODULE_DAG.iter().map(|(name, _)| *name).collect()
}

fn collect_submodule_relative_targets(
    tokens: &[Token],
    mut index: usize,
    mut depth: usize,
    self_submod: &str,
    known: &BTreeSet<String>,
    root_node: Option<&str>,
    deps: &mut BTreeSet<String>,
) -> usize {
    while let Some(TokenKind::Ident(name)) = tokens.get(index).map(|t| &t.kind) {
        if tokens.get(index + 1).map(|t| &t.kind) != Some(&TokenKind::ColonColon) {
            break;
        }
        if name == "super" {
            let Some(parent) = depth.checked_sub(1) else {
                return index + 2;
            };
            depth = parent;
        } else if name != "self" {
            break;
        }
        index += 2;
    }
    match tokens.get(index).map(|t| &t.kind) {
        Some(TokenKind::LBrace) => {
            let end = skip_balanced(tokens, index, &TokenKind::LBrace, &TokenKind::RBrace);
            index += 1;
            while index + 1 < end {
                index = collect_submodule_relative_targets(
                    tokens,
                    index,
                    depth,
                    self_submod,
                    known,
                    root_node,
                    deps,
                );
                while index + 1 < end && tokens[index].kind != TokenKind::Comma {
                    index = if tokens[index].kind == TokenKind::LBrace {
                        skip_balanced(tokens, index, &TokenKind::LBrace, &TokenKind::RBrace)
                    } else {
                        index + 1
                    };
                }
                index += 1;
            }
            end
        }
        Some(TokenKind::Ident(target)) => {
            if depth == 0 && target != self_submod && target != "self" {
                if known.contains(target.as_str()) {
                    deps.insert(target.clone());
                } else if let Some(root) = root_node {
                    deps.insert(root.to_owned());
                }
            }
            index + 1
        }
        _ => index + 1,
    }
}

fn extract_tree_submodule_deps(
    src: &str,
    top_module: &str,
    self_submod: &str,
    initial_depth: usize,
    known: &BTreeSet<String>,
    root_node: Option<&str>,
) -> BTreeSet<String> {
    let tokens = strip_cfg_test(&tokenize(src));
    let mut deps = BTreeSet::new();
    let mut depth = initial_depth;
    let mut scopes = Vec::new();
    let mut i = 0;

    while i < tokens.len() {
        // Match `crate::<top_module>::<submod>` or `crate::<top_module>::{...}`.
        if matches!(&tokens[i].kind, TokenKind::Ident(id) if id == "crate")
            && tokens.get(i + 1).map(|t| &t.kind) == Some(&TokenKind::ColonColon)
            && matches!(tokens.get(i + 2).map(|t| &t.kind), Some(TokenKind::Ident(id)) if id == top_module)
            && tokens.get(i + 3).map(|t| &t.kind) == Some(&TokenKind::ColonColon)
        {
            i = collect_submodule_relative_targets(
                &tokens,
                i + 4,
                0,
                self_submod,
                known,
                root_node,
                &mut deps,
            );
            continue;
        }
        // Match `super::<submod>` or `super::{...}` resolving to `crate::<top_module>`.
        if matches!(&tokens[i].kind, TokenKind::Ident(id) if id == "super")
            && tokens.get(i + 1).map(|t| &t.kind) == Some(&TokenKind::ColonColon)
        {
            i = collect_submodule_relative_targets(
                &tokens,
                i,
                depth,
                self_submod,
                known,
                root_node,
                &mut deps,
            );
            continue;
        }
        match &tokens[i].kind {
            TokenKind::LBrace => {
                let is_mod = i >= 2
                    && matches!(&tokens[i - 2].kind, TokenKind::Ident(id) if id == "mod")
                    && matches!(&tokens[i - 1].kind, TokenKind::Ident(_));
                scopes.push(is_mod);
                depth += usize::from(is_mod);
            }
            TokenKind::RBrace => {
                depth -= usize::from(scopes.pop().unwrap_or(false));
            }
            _ => {}
        }
        i += 1;
    }

    deps
}

fn extract_root_child_deps(src: &str, known: &BTreeSet<String>) -> BTreeSet<String> {
    let tokens = strip_cfg_test(&tokenize(src));
    let mut deps = BTreeSet::new();
    for i in 0..tokens.len() {
        if matches!(&tokens[i].kind, TokenKind::Ident(id) if id == "mod")
            && let Some(TokenKind::Ident(child)) = tokens.get(i + 1).map(|t| &t.kind)
            && known.contains(child.as_str())
        {
            deps.insert(child.clone());
        }
        if let TokenKind::Ident(child) = &tokens[i].kind
            && tokens.get(i + 1).map(|t| &t.kind) == Some(&TokenKind::ColonColon)
            && known.contains(child.as_str())
        {
            deps.insert(child.clone());
        }
    }
    deps
}

fn extract_mcp_submodule_deps(src: &str, self_submod: &str) -> BTreeSet<String> {
    let known: BTreeSet<String> = mcp_submodule_names()
        .into_iter()
        .map(str::to_owned)
        .collect();
    extract_tree_submodule_deps(src, "mcp", self_submod, 1, &known, None)
}

/// Tarjan's strongly connected components algorithm; returns all cyclic SCCs (size > 1 or self-loop).
fn find_scc_cycles(graph: &BTreeMap<String, BTreeSet<String>>) -> Vec<Vec<String>> {
    struct State<'a> {
        graph: &'a BTreeMap<String, BTreeSet<String>>,
        next_index: usize,
        indices: BTreeMap<String, usize>,
        lowlink: BTreeMap<String, usize>,
        on_stack: BTreeSet<String>,
        stack: Vec<String>,
        cycles: Vec<Vec<String>>,
    }

    fn strongconnect(v: &str, st: &mut State<'_>) {
        let idx = st.next_index;
        st.next_index += 1;
        st.indices.insert(v.to_owned(), idx);
        st.lowlink.insert(v.to_owned(), idx);
        st.stack.push(v.to_owned());
        st.on_stack.insert(v.to_owned());

        if let Some(neighbors) = st.graph.get(v) {
            for w in neighbors {
                if !st.indices.contains_key(w) {
                    strongconnect(w, st);
                    let low_w = st.lowlink[w];
                    let low_v = st.lowlink.get_mut(v).expect("v index");
                    *low_v = (*low_v).min(low_w);
                } else if st.on_stack.contains(w) {
                    let idx_w = st.indices[w];
                    let low_v = st.lowlink.get_mut(v).expect("v index");
                    *low_v = (*low_v).min(idx_w);
                }
            }
        }

        if st.lowlink[v] == st.indices[v] {
            let mut scc = Vec::new();
            while let Some(w) = st.stack.pop() {
                st.on_stack.remove(&w);
                scc.push(w.clone());
                if w == v {
                    break;
                }
            }
            let has_self_loop =
                scc.len() == 1 && st.graph.get(&scc[0]).is_some_and(|n| n.contains(&scc[0]));
            if scc.len() > 1 || has_self_loop {
                scc.sort();
                st.cycles.push(scc);
            }
        }
    }

    let mut st = State {
        graph,
        next_index: 0,
        indices: BTreeMap::new(),
        lowlink: BTreeMap::new(),
        on_stack: BTreeSet::new(),
        stack: Vec::new(),
        cycles: Vec::new(),
    };
    for node in graph.keys() {
        if !st.indices.contains_key(node) {
            strongconnect(node, &mut st);
        }
    }
    st.cycles
}

fn module_tree_scc_cycles(
    src_root: &Path,
    top_module: &str,
    include_root: bool,
) -> Vec<Vec<String>> {
    let dir = src_root.join(top_module);
    let mut files = Vec::new();
    collect_rs_files(&dir, &mut files);

    let root_file = if dir.join("mod.rs").exists() {
        dir.join("mod.rs")
    } else {
        src_root.join(format!("{top_module}.rs"))
    };

    let mut child_files: Vec<(String, PathBuf, usize)> = Vec::new();
    let mut known_children = BTreeSet::new();
    for file in files {
        if is_test_source_file(src_root, &file) || file == root_file {
            continue;
        }
        let rel = file.strip_prefix(&dir).expect("file under module dir");
        let first = rel.components().next().expect("non-empty relative path");
        let first_str = first.as_os_str().to_string_lossy();
        let child = first_str
            .strip_suffix(".rs")
            .unwrap_or(&first_str)
            .to_owned();
        let depth = rel.components().count() - usize::from(file.file_name().unwrap() == "mod.rs");
        known_children.insert(child.clone());
        child_files.push((child, file, depth));
    }

    let root_key = format!("{top_module} (root)");
    let mut graph: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    if include_root && root_file.exists() {
        let root_src = fs::read_to_string(&root_file)
            .unwrap_or_else(|e| panic!("failed to read {}: {e}", root_file.display()));
        graph.insert(
            root_key.clone(),
            extract_root_child_deps(&root_src, &known_children),
        );
    }
    for (child, file, depth) in child_files {
        let src = fs::read_to_string(&file)
            .unwrap_or_else(|e| panic!("failed to read {}: {e}", file.display()));
        let deps = extract_tree_submodule_deps(
            &src,
            top_module,
            &child,
            depth,
            &known_children,
            include_root.then_some(root_key.as_str()),
        );
        graph.entry(child).or_default().extend(deps);
    }

    find_scc_cycles(&graph)
}

#[test]
fn platform_module_tree_has_no_cycles() {
    let src_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let cycles = module_tree_scc_cycles(&src_root, "platform", true);
    assert!(
        cycles.is_empty(),
        "Tarjan SCC detected root-inclusive cycle(s) in src/platform/: {cycles:?}"
    );
}

#[test]
fn all_multi_file_module_trees_have_no_cycles() {
    let src_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut trees = Vec::new();
    for entry in fs::read_dir(&src_root)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", src_root.display()))
    {
        let entry = entry.expect("dir entry");
        if !entry.file_type().expect("file type").is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if name == "tests" {
            continue;
        }
        trees.push(name);
    }
    trees.sort();

    for expected in ["auth", "cli", "daemon", "mcp", "platform"] {
        assert!(
            trees.iter().any(|t| t == expected),
            "expected multi-file module tree `src/{expected}/` to be checked, found: {trees:?}"
        );
    }

    let mut violations = Vec::new();
    for tree in &trees {
        let cycles = module_tree_scc_cycles(&src_root, tree, true);
        if !cycles.is_empty() {
            violations.push(format!("src/{tree}/: {cycles:?}"));
        }
    }
    assert!(
        violations.is_empty(),
        "Tarjan SCC detected root-inclusive submodule cycle(s):\n{}",
        violations.join("\n")
    );
}

#[test]
fn production_files_stay_within_line_budget() {
    let src_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    collect_rs_files(&src_root, &mut files);
    assert!(
        !files.is_empty(),
        "expected production .rs files under {}",
        src_root.display()
    );

    let mut violations = Vec::new();
    for file in files {
        if is_test_source_file(&src_root, &file) {
            continue;
        }
        let content = fs::read_to_string(&file)
            .unwrap_or_else(|e| panic!("failed to read {}: {e}", file.display()));
        let prod_lines = count_prod_lines(&content);
        if prod_lines > MAX_PROD_LINES {
            let rel = file.strip_prefix(&src_root).unwrap_or(&file);
            violations.push(format!(
                "src/{}: {} > {}",
                rel.display(),
                prod_lines,
                MAX_PROD_LINES
            ));
        }
    }
    violations.sort();
    assert!(
        violations.is_empty(),
        "production files exceed MAX_PROD_LINES ({MAX_PROD_LINES}):\n{}",
        violations.join("\n")
    );
}

#[test]
fn mcp_submodules_follow_dag_and_have_no_cycles() {
    let src_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mcp_dir = src_root.join("mcp");
    let allowlist: BTreeMap<&str, BTreeSet<&str>> = MCP_SUBMODULE_DAG
        .iter()
        .map(|(k, v)| (*k, v.iter().copied().collect()))
        .collect();

    let mut files = Vec::new();
    collect_rs_files(&mcp_dir, &mut files);
    assert!(
        !files.is_empty(),
        "expected at least one submodule under {}",
        mcp_dir.display()
    );

    let mut graph: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut violations = Vec::new();

    for file in files {
        if is_test_source_file(&src_root, &file) {
            continue;
        }
        let stem = file
            .file_stem()
            .and_then(|s| s.to_str())
            .expect("valid UTF-8 submodule filename")
            .to_owned();
        if !allowlist.contains_key(stem.as_str()) {
            violations.push(format!(
                "unregistered src/mcp/ submodule `{stem}` not in MCP_SUBMODULE_DAG"
            ));
            continue;
        }
        let content = fs::read_to_string(&file)
            .unwrap_or_else(|e| panic!("failed to read {}: {e}", file.display()));
        let deps = extract_mcp_submodule_deps(&content, &stem);
        let allowed = &allowlist[stem.as_str()];
        for dst in &deps {
            if !allowed.contains(dst.as_str()) {
                violations.push(format!(
                    "src/mcp/{stem}.rs -> {dst} is not permitted by MCP_SUBMODULE_DAG (allowed: {allowed:?})"
                ));
            }
        }
        graph.insert(stem, deps);
    }

    let cycles = find_scc_cycles(&graph);
    assert!(
        cycles.is_empty(),
        "Tarjan SCC detected cycle(s) in src/mcp/ submodules: {cycles:?}"
    );
    let root_cycles = module_tree_scc_cycles(&src_root, "mcp", true);
    assert!(
        root_cycles.is_empty(),
        "Tarjan SCC detected root-inclusive cycle(s) in src/mcp/: {root_cycles:?}"
    );
    assert!(
        violations.is_empty(),
        "src/mcp/ submodule DAG violations:\n{}",
        violations.join("\n")
    );
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
        const RB: &[u8] = rb#"crate::fake_rb::z"#;
        const RC: &std::ffi::CStr = rc#"crate::fake_rc::w"#;
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

    let deps = extract_super_dependencies(sample, "runtime", 1);
    let expected: BTreeSet<String> = ["budget", "config", "ipc", "platform"]
        .into_iter()
        .map(str::to_owned)
        .collect();
    assert_eq!(deps, expected);
}

#[test]
fn scanner_resolves_super_chains_in_files_and_inline_modules() {
    let sample = r#"
        use super::super::daemon::App;
        use super::{super::{config, json::parse}, ingress::Ingress};
        mod nested {
            use super::super::super::auth::HttpAuthManager;
            fn f() { let _ = super::super::super::ipc::PROTOCOL; }
            use super::super::events::EventRouter;
        }
        fn f() { let _ = super::super::runtime::Paths::new; }
        #[cfg(test)]
        mod tests { use super::super::super::events_stream::run; }
    "#;
    let deps = extract_super_dependencies(sample, "mcp", 2);
    let expected = ["auth", "config", "daemon", "ipc", "json", "runtime"]
        .into_iter()
        .map(str::to_owned)
        .collect();
    assert_eq!(deps, expected);
    assert_eq!(
        extract_super_dependencies("mod nested { use super::super::daemon::App; }", "mcp", 1),
        ["daemon".to_owned()].into_iter().collect()
    );
    assert!(extract_super_dependencies("use super::daemon::Helper;", "mcp", 2).is_empty());
    assert!(
        extract_super_dependencies("use super::super::mcp::McpConnection;", "mcp", 2).is_empty()
    );
    let root = Path::new("src");
    assert_eq!(module_depth(root, Path::new("src/mcp.rs")), 1);
    assert_eq!(module_depth(root, Path::new("src/platform/mod.rs")), 1);
    assert_eq!(module_depth(root, Path::new("src/mcp/ingress.rs")), 2);
    assert_eq!(
        module_depth(root, Path::new("src/platform/nested/mod.rs")),
        2
    );
    assert_eq!(module_depth(root, Path::new("src/cli.rs")), 1);
    assert_eq!(module_depth(root, Path::new("src/cli/args.rs")), 2);
}

#[test]
fn scanner_maps_cli_submodules_to_main_and_skips_test_files() {
    let root = Path::new("src");
    assert_eq!(
        module_for_src_file(root, Path::new("src/cli.rs")).as_deref(),
        Some("main")
    );
    assert_eq!(
        module_for_src_file(root, Path::new("src/cli/remote.rs")).as_deref(),
        Some("main")
    );
    assert!(is_test_source_file(root, Path::new("src/auth/tests.rs")));
    assert!(is_test_source_file(root, Path::new("src/cli/tests.rs")));
    assert!(is_test_source_file(
        root,
        Path::new("src/daemon/tests/smoke.rs")
    ));
    assert!(!is_test_source_file(root, Path::new("src/cli/remote.rs")));

    let cli_src = r#"
        use crate::cli::args::Cli;
        use mcpcli::daemon::DaemonOptions;
        use mcpcli::config::LoadedConfig;
    "#;
    let deps = extract_module_crate_dependencies(cli_src, "main");
    let expected: BTreeSet<String> = ["config", "daemon"]
        .into_iter()
        .map(str::to_owned)
        .collect();
    assert_eq!(deps, expected);
}

#[test]
fn scanner_requires_standard_production_module_paths() {
    assert!(has_module_path_override(
        r#"#[path = "helper.rs"] mod helper;"#
    ));
    assert!(has_module_path_override(
        r#"#[cfg_attr(unix, path = "helper.rs")] mod helper;"#
    ));
    assert!(!has_module_path_override(
        r#"#[cfg(test)] #[path = "helper.rs"] mod helper;"#
    ));
    assert!(!has_module_path_override(
        r##"const DOC: &str = r#"#[path = "helper.rs"]"#;"##
    ));
    assert!(!has_module_path_override("#[cfg(unix)] mod helper;"));
    let root = Path::new("src");
    assert_eq!(
        module_depth(root, Path::new("src/platform/unix/freebsd_acl.rs")),
        3
    );
    assert_eq!(
        extract_super_dependencies("use super::super::super::daemon::App;", "platform", 3),
        ["daemon".to_owned()].into_iter().collect()
    );
}

#[test]
fn scanner_keeps_self_qualified_crate_paths_for_l0() {
    assert_eq!(
        extract_dependencies("use crate::platform::digest;", "crate", ""),
        ["platform".to_owned()].into_iter().collect()
    );
}

#[test]
fn dependency_gates_reject_disallowed_modules_and_ignore_cfg_test() {
    for dependency in ["platform", "budget", "discovery", "oauth_url"] {
        let source = format!("use crate::{dependency}::Item;");
        let deps = extract_dependencies(&source, "crate", "skills");
        assert_eq!(
            skills_dependencies_allowed(&deps),
            dependency == "platform",
            "skills -> {dependency}"
        );
    }
    let test_only = "#[cfg(test)] mod tests { use crate::budget::ByteBudget; }";
    assert!(skills_dependencies_allowed(&extract_dependencies(
        test_only, "crate", "skills"
    )));

    let uses_rmcp = "use rmcp::model::Tool;";
    assert!(!check_external_crate_allowed(
        "schema",
        uses_rmcp,
        "rmcp",
        RMCP_ALLOWED
    ));
    assert!(!check_external_crate_allowed(
        "main",
        uses_rmcp,
        "rmcp",
        RMCP_ALLOWED
    ));
    assert!(check_external_crate_allowed(
        "mcp",
        uses_rmcp,
        "rmcp",
        RMCP_ALLOWED
    ));

    let uses_saphyr = "fn f() { let _ = serde_saphyr::from_str::<()>; }";
    assert!(!check_external_crate_allowed(
        "schema",
        uses_saphyr,
        "serde_saphyr",
        SERDE_SAPHYR_ALLOWED
    ));
    assert!(!check_external_crate_allowed(
        "mcp",
        uses_saphyr,
        "serde_saphyr",
        SERDE_SAPHYR_ALLOWED
    ));
    assert!(check_external_crate_allowed(
        "skills",
        uses_saphyr,
        "serde_saphyr",
        SERDE_SAPHYR_ALLOWED
    ));

    let test_only = r#"
        #[cfg(test)]
        mod tests {
            use rmcp::model::Tool;
            use serde_saphyr::Options;
        }
    "#;
    assert!(check_external_crate_allowed(
        "json",
        test_only,
        "rmcp",
        RMCP_ALLOWED
    ));
    assert!(check_external_crate_allowed(
        "json",
        test_only,
        "serde_saphyr",
        SERDE_SAPHYR_ALLOWED
    ));
}

#[test]
fn mcp_submodule_scanner_and_tarjan_scc_detect_illegal_edges_and_cycles() {
    let sample = r#"
        use super::ingress::Ingress;
        use crate::mcp::sse::observe_sse;
        mod nested {
            use super::super::tasks::poll;
        }
        #[cfg(test)]
        mod tests {
            use super::skills::walk_skills;
        }
    "#;
    let deps = extract_mcp_submodule_deps(sample, "events");
    let expected: BTreeSet<String> = ["ingress", "sse", "tasks"]
        .into_iter()
        .map(str::to_owned)
        .collect();
    assert_eq!(deps, expected);

    // Acyclic graph has no cyclic SCCs.
    let mut dag: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    dag.insert("ingress".into(), BTreeSet::new());
    dag.insert("sse".into(), ["ingress".into()].into_iter().collect());
    dag.insert(
        "events".into(),
        ["ingress".into(), "sse".into()].into_iter().collect(),
    );
    assert!(find_scc_cycles(&dag).is_empty());

    // 2-node cycle is detected.
    let mut cyclic = dag.clone();
    cyclic.get_mut("ingress").unwrap().insert("sse".into());
    assert_eq!(
        find_scc_cycles(&cyclic),
        vec![vec!["ingress".to_owned(), "sse".to_owned()]]
    );
}

#[test]
fn prod_line_counter_enforces_budget_and_handles_cfg_test_spans() {
    let root = Path::new("src");

    // (a) 1,501-line production file fails.
    let over_budget = "fn prod() {}\n".repeat(MAX_PROD_LINES + 1);
    assert_eq!(count_prod_lines(&over_budget), MAX_PROD_LINES + 1);
    assert!(!prod_line_budget_ok(
        root,
        Path::new("src/monolith.rs"),
        &over_budget
    ));

    // (b) 1,000-line production file + 2,000-line `#[cfg(test)] mod tests` passes.
    let mut mixed = "fn prod() {}\n".repeat(1000);
    mixed.push_str("#[cfg(test)]\nmod tests {\n");
    mixed.push_str(&"    fn test_case() {}\n".repeat(1997));
    mixed.push_str("}\n");
    assert_eq!(count_prod_lines(&mixed), 1000);
    assert!(prod_line_budget_ok(root, Path::new("src/mixed.rs"), &mixed));

    // (c) Multi-line raw string literals (r#, rb#, rc#) in production code count all lines.
    let raw_strings = concat!(
        "const A: &str = r#\"\n",
        "line 2\n",
        "line 3\n",
        "\"#;\n",
        "const B: &[u8] = rb#\"\n",
        "line 6\n",
        "\"#;\n",
        "const C: &std::ffi::CStr = rc#\"\n",
        "line 9\n",
        "\"#;\n",
    );
    assert_eq!(count_prod_lines(raw_strings), 10);

    // (d) `#[cfg(test)]` struct field ending in `,` and comma-less final field before `}`
    // exclude only the gated field lines, preserving the struct and closing brace.
    let gated_fields = concat!(
        "struct Sample {\n",
        "    kept_before: usize,\n",
        "    #[allow(dead_code)]\n",
        "    #[cfg(test)]\n",
        "    test_mid: std::collections::BTreeMap<String, usize>,\n",
        "    kept_after: bool,\n",
        "    #[cfg(test)]\n",
        "    test_last: u8\n",
        "}\n",
        "fn after_struct() {}\n",
    );
    assert_eq!(count_prod_lines(gated_fields), 5);
    assert!(
        extract_dependencies(
            &format!("{gated_fields}\nfn f() {{ crate::alpha::run(); }}"),
            "crate",
            "self_mod"
        )
        .contains("alpha")
    );

    // (e) `tests.rs` (and `tests/` directory files) > 1,500 lines pass.
    assert!(prod_line_budget_ok(
        root,
        Path::new("src/mcp/tests.rs"),
        &over_budget
    ));
    assert!(prod_line_budget_ok(
        root,
        Path::new("src/mcp/tests/extra.rs"),
        &over_budget
    ));

    // Top-level `#![cfg(test)]` marks the entire file as 0 production lines.
    let inner_cfg_test = "#![cfg(test)]\nfn a() {}\nfn b() {}\n";
    assert_eq!(count_prod_lines(inner_cfg_test), 0);
}
