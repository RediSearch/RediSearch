/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Scanning of the C and C++ sources: modules, the headers they include, the
//! names each file defines for others to use, and the names it uses.

use anyhow::{Context, Result};
use std::collections::BTreeSet;
use std::path::{Component, Path, PathBuf};
use walkdir::WalkDir;

/// Extensions of the C and C++ files making up modules.
const EXTENSIONS: &[&str] = &["c", "cc", "cpp", "cxx", "h", "hpp"];

/// Extensions of the headers, whose definitions are meant to be shared.
const HEADER_EXTENSIONS: &[&str] = &["h", "hpp"];

/// The include directories of the C build, in search order, relative to the
/// repository root. Includes resolving elsewhere, such as system headers, are
/// not part of the graph.
pub const INCLUDE_DIRS: &[&str] = &[
    "src",
    "src/buffer",
    "src/coord",
    "src/redisearch_rs/headers",
    "deps/libuv/include",
    "deps",
    "deps/VectorSimilarity/src",
    "deps/rmalloc",
    "",
];

/// Specifiers taking a parenthesized argument that are neither functions nor
/// part of a function's type, so they are skipped along with their argument.
const ATTRIBUTES: &[&str] = &["__attribute__", "__declspec", "alignas", "_Alignas"];

/// Identifiers that can precede a `(` at file scope without naming a function.
const NOT_FUNCTION_NAMES: &[&str] = &[
    "_Static_assert",
    "decltype",
    "defined",
    "sizeof",
    "static_assert",
];

/// A C or C++ file, part of a module.
pub struct CFile {
    /// Path relative to the repository root.
    pub path: PathBuf,
    /// The module the file belongs to, see [`module_name`].
    pub module: String,
    /// The `#include` operands, as written.
    pub includes: Vec<Include>,
    pub symbols: Symbols,
    /// Lines of code, see [`code_lines`].
    pub lines: usize,
}

impl CFile {
    pub fn is_header(&self) -> bool {
        FileKind::of(&self.path) == FileKind::Header
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum FileKind {
    Header,
    Source,
}

impl FileKind {
    fn of(path: &Path) -> Self {
        let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
        if HEADER_EXTENSIONS.contains(&ext) {
            Self::Header
        } else {
            Self::Source
        }
    }
}

/// The names a file shares with others, and those it uses.
#[derive(Default, Debug)]
pub struct Symbols {
    /// The names other files can use: in a source file, its non-`static`
    /// functions and global variables; in a header, its inline functions,
    /// macros, types and enum constants. Function prototypes and `extern`
    /// declarations are left out, as the code they announce is elsewhere.
    pub defined: BTreeSet<String>,
    /// The identifiers the file refers to, in its code and macro bodies,
    /// member names left out.
    pub used: BTreeSet<String>,
    /// Whether the file defines any function, `static` ones included.
    pub has_function_bodies: bool,
}

pub struct Include {
    pub target: String,
    /// `#include "..."`, which also searches the including file's directory.
    pub quoted: bool,
}

/// Scans every C and C++ file under `dir`, except those under `skip`; both
/// are relative to `repo_root`.
pub fn scan(repo_root: &Path, dir: &str, skip: Option<&str>) -> Result<Vec<CFile>> {
    let skip = skip.map(|s| repo_root.join(s));
    let mut files = Vec::new();
    for entry in WalkDir::new(repo_root.join(dir))
        .sort_by_file_name()
        .into_iter()
        .filter_entry(|e| Some(e.path()) != skip.as_deref())
    {
        let entry = entry?;
        let path = entry.path();
        let Some(ext) = path.extension().and_then(|e| e.to_str()) else {
            continue;
        };
        if !entry.file_type().is_file() || !EXTENSIONS.contains(&ext) {
            continue;
        }
        let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
        let source = String::from_utf8_lossy(&bytes);
        let (code, directives) = strip(&source);
        let relative = path.strip_prefix(repo_root)?.to_path_buf();
        let kind = FileKind::of(&relative);
        let tokens = tokenize(&code);
        let mut symbols = file_symbols(&tokens, kind);
        let macros: Vec<(String, &str)> =
            directives.iter().filter_map(|d| parse_define(d)).collect();
        if kind == FileKind::Header {
            symbols
                .defined
                .extend(macros.iter().map(|(name, _)| name.clone()));
        }
        symbols.used = used_identifiers(&tokens);
        for (_, body) in &macros {
            symbols.used.extend(used_identifiers(&tokenize(body)));
        }
        files.push(CFile {
            lines: count_lines(&code, &directives),
            module: module_name(&relative),
            includes: directives.iter().filter_map(|d| parse_include(d)).collect(),
            symbols,
            path: relative,
        });
    }
    Ok(files)
}

/// The lines of code of a C or C++ source: blank lines, comments and
/// `#include` directives left out.
pub fn code_lines(source: &str) -> usize {
    let (code, directives) = strip(source);
    count_lines(&code, &directives)
}

fn count_lines(code: &str, directives: &[String]) -> usize {
    let code = code.lines().filter(|l| !l.trim().is_empty()).count();
    code + directives
        .iter()
        .filter(|d| !d.starts_with("include"))
        .count()
}

/// The lines of code of every C and C++ file under `dir`, see [`code_lines`].
pub fn dir_code_lines(dir: &Path) -> usize {
    WalkDir::new(dir)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| {
            e.file_type().is_file()
                && e.path()
                    .extension()
                    .and_then(|x| x.to_str())
                    .is_some_and(|x| EXTENSIONS.contains(&x))
        })
        .filter_map(|e| std::fs::read(e.path()).ok())
        .map(|bytes| code_lines(&String::from_utf8_lossy(&bytes)))
        .sum()
}

/// A file's module: its path under `src/` without extension, so that a
/// source file and the header declaring its interface form one module.
pub fn module_name(relative: &Path) -> String {
    let under_src = relative.strip_prefix("src").unwrap_or(relative);
    under_src
        .with_extension("")
        .to_string_lossy()
        .replace('\\', "/")
}

/// Resolves an include the way the compiler does, returning the path relative
/// to the repository root if it names a file in `exists`.
pub fn resolve(include: &Include, from: &Path, exists: impl Fn(&Path) -> bool) -> Option<PathBuf> {
    let own_dir = include
        .quoted
        .then(|| from.parent().unwrap_or(Path::new("")).to_path_buf());
    own_dir
        .into_iter()
        .chain(INCLUDE_DIRS.iter().map(PathBuf::from))
        .map(|dir| normalize(&dir.join(&include.target)))
        .find(|candidate| exists(candidate))
}

/// Removes `.` and `..` components without touching the file system.
fn normalize(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other),
        }
    }
    out
}

/// Splits a source into its code, with comments blanked out, and its
/// preprocessor directives, with line continuations joined and the leading
/// `#` dropped.
fn strip(source: &str) -> (String, Vec<String>) {
    #[derive(PartialEq)]
    enum State {
        Code,
        LineComment,
        BlockComment,
        Literal(char),
    }
    let mut out = String::with_capacity(source.len());
    let mut state = State::Code;
    let mut chars = source.chars().peekable();
    while let Some(c) = chars.next() {
        match state {
            State::Code => match (c, chars.peek()) {
                ('/', Some('/')) => {
                    chars.next();
                    state = State::LineComment;
                }
                ('/', Some('*')) => {
                    chars.next();
                    state = State::BlockComment;
                    out.push(' ');
                }
                ('"' | '\'', _) => {
                    state = State::Literal(c);
                    out.push(c);
                }
                _ => out.push(c),
            },
            State::LineComment => {
                if c == '\n' {
                    state = State::Code;
                    out.push('\n');
                }
            }
            State::BlockComment => {
                if c == '*' && chars.peek() == Some(&'/') {
                    chars.next();
                    state = State::Code;
                } else if c == '\n' {
                    // Keeps directives following the comment on their own line.
                    out.push('\n');
                }
            }
            State::Literal(quote) => {
                out.push(c);
                if c == '\\' {
                    if let Some(escaped) = chars.next() {
                        out.push(escaped);
                    }
                } else if c == quote || c == '\n' {
                    state = State::Code;
                }
            }
        }
    }

    let mut code = String::with_capacity(out.len());
    let mut directives = Vec::new();
    let mut lines = out.lines();
    while let Some(line) = lines.next() {
        let mut line = line.to_owned();
        while line.ends_with('\\') {
            line.pop();
            match lines.next() {
                Some(next) => line.push_str(next),
                None => break,
            }
        }
        match line.trim_start().strip_prefix('#') {
            Some(directive) => directives.push(directive.trim_start().to_owned()),
            None => {
                code.push_str(&line);
                code.push('\n');
            }
        }
    }
    (code, directives)
}

fn parse_include(directive: &str) -> Option<Include> {
    let operand = directive.strip_prefix("include")?.trim();
    let (quoted, close) = match operand.chars().next()? {
        '"' => (true, '"'),
        '<' => (false, '>'),
        _ => return None,
    };
    let rest = &operand[1..];
    let target = &rest[..rest.find(close)?];
    Some(Include {
        target: target.to_owned(),
        quoted,
    })
}

/// A macro's name and body.
fn parse_define(directive: &str) -> Option<(String, &str)> {
    let rest = directive.strip_prefix("define")?.trim_start();
    let name: String = rest
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect();
    (!name.is_empty()).then(|| {
        let body = &rest[name.len()..];
        (name, body)
    })
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum Token<'a> {
    Ident(&'a str),
    Str,
    Punct(char),
}

fn tokenize(code: &str) -> Vec<Token<'_>> {
    let bytes = code.as_bytes();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        if c.is_ascii_whitespace() {
            i += 1;
        } else if c.is_ascii_alphabetic() || c == b'_' {
            let start = i;
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
            tokens.push(Token::Ident(&code[start..i]));
        } else if c.is_ascii_digit() {
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'.') {
                i += 1;
            }
        } else if c == b'"' || c == b'\'' {
            i += 1;
            while i < bytes.len() && bytes[i] != c && bytes[i] != b'\n' {
                i += if bytes[i] == b'\\' { 2 } else { 1 };
            }
            i += 1;
            if c == b'"' {
                tokens.push(Token::Str);
            }
        } else {
            let ch = code[i..].chars().next().unwrap_or(' ');
            i += ch.len_utf8();
            tokens.push(Token::Punct(ch));
        }
    }
    tokens
}

/// What an open brace delimits.
#[derive(Clone, Copy, PartialEq)]
enum Scope {
    /// `extern "C"` or a namespace: its content is still at file scope.
    Transparent,
    /// An enum body, whose constants are symbols.
    Enum,
    /// A function body, not followed by a `;`.
    FunctionBody,
    Other,
}

/// The names a file defines for other files, see [`Symbols::defined`]. A
/// heuristic, not a parser.
fn file_symbols(tokens: &[Token], kind: FileKind) -> Symbols {
    let mut symbols = Symbols::default();
    let mut scopes: Vec<Scope> = Vec::new();
    let mut statement: Vec<Token> = Vec::new();
    let at_file_scope = |scopes: &[Scope]| scopes.iter().all(|s| *s == Scope::Transparent);
    let in_header = kind == FileKind::Header;

    for (i, token) in tokens.iter().enumerate() {
        match token {
            Token::Punct('{') => {
                let scope = if !at_file_scope(&scopes) {
                    Scope::Other
                } else if statement == [Token::Ident("extern"), Token::Str]
                    || statement.first() == Some(&Token::Ident("namespace"))
                {
                    Scope::Transparent
                } else if is_function_definition(&statement) {
                    symbols.has_function_bodies = true;
                    // A header's static inline functions are shared with
                    // every file including it.
                    if let Some(name) = function_name(&statement)
                        && (in_header || !statement.contains(&Token::Ident("static")))
                    {
                        symbols.defined.insert(name);
                    }
                    Scope::FunctionBody
                } else if statement.contains(&Token::Ident("enum")) {
                    Scope::Enum
                } else {
                    Scope::Other
                };
                if scope == Scope::Transparent {
                    statement.clear();
                }
                scopes.push(scope);
            }
            Token::Punct('}') => {
                if matches!(scopes.pop(), Some(Scope::Transparent | Scope::FunctionBody))
                    && at_file_scope(&scopes)
                {
                    statement.clear();
                }
            }
            Token::Punct(';') if at_file_scope(&scopes) => {
                let name = match kind {
                    FileKind::Header => typedef_name(&statement),
                    FileKind::Source => global_variable(&statement),
                };
                symbols.defined.extend(name);
                statement.clear();
            }
            Token::Ident(name)
                if in_header
                    && scopes.last() == Some(&Scope::Enum)
                    && matches!(tokens[i - 1], Token::Punct('{' | ',')) =>
            {
                symbols.defined.insert((*name).to_owned());
            }
            _ if at_file_scope(&scopes) => statement.push(*token),
            _ => {}
        }
        // Tagged types may be defined inside any statement, including nested
        // ones. Only definitions count: headers forward-declare the types of
        // other modules. Those defined in a source file are private to it.
        if let (
            true,
            Some(Token::Ident("struct" | "union" | "enum" | "class")),
            Token::Ident(name),
            Some(Token::Punct('{')),
        ) = (
            in_header,
            i.checked_sub(1).map(|p| &tokens[p]),
            token,
            tokens.get(i + 1),
        ) {
            symbols.defined.insert((*name).to_owned());
        }
    }
    symbols
}

/// The identifiers in `tokens`, except member names, which follow a `.` or a
/// `->` and would match unrelated globals.
fn used_identifiers(tokens: &[Token]) -> BTreeSet<String> {
    tokens
        .iter()
        .enumerate()
        .filter_map(|(i, token)| {
            let Token::Ident(name) = token else {
                return None;
            };
            let previous = |back: usize| i.checked_sub(back).map(|p| tokens[p]);
            let is_member = previous(1) == Some(Token::Punct('.'))
                || (previous(1) == Some(Token::Punct('>'))
                    && previous(2) == Some(Token::Punct('-')));
            (!is_member).then(|| (*name).to_owned())
        })
        .collect()
}

/// Whether a file-scope statement followed by `{` opens a function body: its
/// parameter list closes right before the brace, whatever its return type or
/// parameters (`struct`, `enum`) mention.
fn is_function_definition(statement: &[Token]) -> bool {
    let mut end = statement.len();
    // C++ qualifiers after the parameter list.
    while let Some(Token::Ident("const" | "noexcept" | "override" | "final" | "volatile")) =
        end.checked_sub(1).map(|i| statement[i])
    {
        end -= 1;
    }
    statement.first() != Some(&Token::Ident("typedef"))
        && !statement.contains(&Token::Punct('='))
        && end > 0
        && statement[end - 1] == Token::Punct(')')
}

/// The first identifier called like a function after a type, if any.
fn function_name(statement: &[Token]) -> Option<String> {
    without_attributes(statement)
        .windows(3)
        .find_map(|w| match w {
            [
                Token::Ident(_) | Token::Punct('*' | '&'),
                Token::Ident(name),
                Token::Punct('('),
            ] if !NOT_FUNCTION_NAMES.contains(name) => Some((*name).to_owned()),
            _ => None,
        })
}

fn without_attributes<'a>(statement: &[Token<'a>]) -> Vec<Token<'a>> {
    let mut out = Vec::with_capacity(statement.len());
    let mut tokens = statement.iter().peekable();
    while let Some(token) = tokens.next() {
        let is_attribute = matches!(token, Token::Ident(name) if ATTRIBUTES.contains(name));
        if !is_attribute || tokens.peek() != Some(&&Token::Punct('(')) {
            out.push(*token);
            continue;
        }
        let mut depth = 0usize;
        for t in tokens.by_ref() {
            match t {
                Token::Punct('(') => depth += 1,
                Token::Punct(')') => depth -= 1,
                _ => {}
            }
            if depth == 0 {
                break;
            }
        }
    }
    out
}

/// The name a `typedef` statement gives, if `statement` is one.
fn typedef_name(statement: &[Token]) -> Option<String> {
    if statement.first() != Some(&Token::Ident("typedef")) {
        return None;
    }
    // A function type, `typedef int (Name)(...)`, or a pointer to one,
    // `typedef void (*Name)(...)`: the name is the first in parentheses.
    let function = statement.windows(4).find_map(|w| match w {
        [
            Token::Punct('('),
            Token::Punct('*'),
            Token::Ident(name),
            Token::Punct(')'),
        ]
        | [
            Token::Punct('('),
            Token::Ident(name),
            Token::Punct(')'),
            Token::Punct('('),
        ] => Some((*name).to_owned()),
        _ => None,
    });
    function.or_else(|| last_ident(statement))
}

/// The global variable a file-scope statement of a source file defines for
/// other files, if any.
fn global_variable(statement: &[Token]) -> Option<String> {
    let head = statement
        .split(|t| *t == Token::Punct('='))
        .next()
        .unwrap_or_default();
    let idents = head.iter().filter(|t| matches!(t, Token::Ident(_))).count();
    let private_or_elsewhere = head.iter().any(|t| {
        matches!(
            t,
            Token::Ident("static" | "extern" | "typedef") | Token::Punct('(')
        )
    });
    // `struct Foo;` declares a type, not a variable.
    let declares_type = idents == 2
        && matches!(
            head.first(),
            Some(Token::Ident("struct" | "union" | "enum" | "class"))
        );
    (idents >= 2 && !private_or_elsewhere && !declares_type)
        .then(|| last_ident(head))
        .flatten()
}

fn last_ident(tokens: &[Token]) -> Option<String> {
    tokens.iter().rev().find_map(|t| match t {
        Token::Ident(name) => Some((*name).to_owned()),
        _ => None,
    })
}

/// The modules holding a source file, as opposed to headers alone.
pub fn modules_with_source(files: &[CFile]) -> BTreeSet<&str> {
    files
        .iter()
        .filter(|f| !f.is_header())
        .map(|f| f.module.as_str())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn defined(source: &str, kind: FileKind) -> Vec<String> {
        let (code, directives) = strip(source);
        let mut symbols = file_symbols(&tokenize(&code), kind).defined;
        if kind == FileKind::Header {
            symbols.extend(
                directives
                    .iter()
                    .filter_map(|d| parse_define(d))
                    .map(|(n, _)| n),
            );
        }
        symbols.into_iter().collect()
    }

    #[test]
    fn header_defines_types_macros_and_inline_functions() {
        let header = r#"
#ifndef FOO_H__
#define FOO_H__
#ifdef __cplusplus
extern "C" {
#endif
/* A comment with Commented(x); */
struct Forward;
typedef struct Spec { int (*cb)(int); } Spec;
typedef enum { Red, Green = 2 } Color;
typedef void (*Callback)(void *);
typedef int(RangeCallback)(const char *, size_t);
extern int GlobalCount;
int Spec_Load(const char *name, Spec *out);
static inline int Spec_Inline(int x) { return helper(x); }
static inline struct Spec *Spec_Of(enum Kind k) { if (k) { return 0; } return 0; }
const char* __attribute__((pure)) Spec_Name(Spec *s);
#ifdef __cplusplus
}
#endif
#endif
"#;
        // Prototypes and `extern` declarations announce code found elsewhere.
        assert_eq!(
            defined(header, FileKind::Header),
            [
                "Callback",
                "Color",
                "FOO_H__",
                "Green",
                "RangeCallback",
                "Red",
                "Spec",
                "Spec_Inline",
                "Spec_Of",
            ]
        );
    }

    #[test]
    fn source_defines_non_static_functions_and_globals() {
        let source = r#"
typedef struct Spec Spec;
struct Private { int x; };
int Spec_Load(const char *name);
static int helper(int x) { return x; }
int Spec_Free(Spec *s) { return helper(0); }
RSConfig RSGlobalConfig = { .a = 1 };
static int counter = 0;
extern int elsewhere;
int table[16];
"#;
        assert_eq!(
            defined(source, FileKind::Source),
            ["RSGlobalConfig", "Spec_Free", "table"]
        );
    }

    #[test]
    fn code_lines_skip_blanks_comments_and_includes() {
        let source = "#include <stdio.h>\n\n/* a\n   comment */\n#define N 1\nint f(void) {\n  return N; // end\n}\n";
        assert_eq!(code_lines(source), 4);
    }

    #[test]
    fn used_identifiers_skip_member_names() {
        let (code, _) = strip("void f(Spec *s) { s->size = obj.len + Spec_Size(s); }");
        let used = used_identifiers(&tokenize(&code));
        assert!(used.contains("Spec_Size") && used.contains("Spec"));
        assert!(!used.contains("size") && !used.contains("len"));
    }

    #[test]
    fn parses_includes() {
        let (_, directives) = strip("#include \"a/b.h\"\n#  include <c.h> // x\n#define X 1\n");
        let includes: Vec<_> = directives
            .iter()
            .filter_map(|d| parse_include(d))
            .map(|i| (i.target, i.quoted))
            .collect();
        assert_eq!(
            includes,
            [("a/b.h".to_owned(), true), ("c.h".to_owned(), false)]
        );
    }

    #[test]
    fn resolves_includes_like_the_compiler() {
        let known = [
            PathBuf::from("src/util/arr.h"),
            PathBuf::from("src/coord/rmr/rmr.h"),
        ];
        let exists = |p: &Path| known.iter().any(|k| k == p);
        let quoted = |target: &str| Include {
            target: target.to_owned(),
            quoted: true,
        };
        assert_eq!(
            resolve(
                &quoted("../util/arr.h"),
                Path::new("src/aggregate/x.c"),
                exists
            ),
            Some(PathBuf::from("src/util/arr.h"))
        );
        assert_eq!(
            resolve(&quoted("rmr/rmr.h"), Path::new("src/module.c"), exists),
            Some(PathBuf::from("src/coord/rmr/rmr.h"))
        );
        assert_eq!(
            resolve(&quoted("redismodule.h"), Path::new("src/module.c"), exists),
            None
        );
    }

    #[test]
    fn modules_with_source_skip_header_only_ones() {
        let file = |path: &str| CFile {
            path: PathBuf::from(path),
            module: module_name(Path::new(path)),
            includes: Vec::new(),
            symbols: Symbols::default(),
            lines: 0,
        };
        let files = [
            file("src/err.h"),
            file("src/spec.h"),
            file("src/spec.c"),
            file("src/geometry/rtree.hpp"),
            file("src/geometry/rtree.cpp"),
        ];
        assert_eq!(
            modules_with_source(&files),
            ["geometry/rtree", "spec"].into()
        );
    }

    #[test]
    fn module_pairs_source_and_header() {
        assert_eq!(module_name(Path::new("src/util/arr.h")), "util/arr");
        assert_eq!(module_name(Path::new("src/util/arr.c")), "util/arr");
    }
}
