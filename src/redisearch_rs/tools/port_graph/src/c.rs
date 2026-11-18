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

use crate::syntax::{Syntax, Walk, walk_from};
use anyhow::{Context, Result};
use std::collections::BTreeSet;
use std::path::{Component, Path, PathBuf};
use tree_sitter::Node;
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
        let relative = path.strip_prefix(repo_root)?.to_path_buf();
        let parsed = Parsed::new(&bytes, Grammar::of(&relative))?;
        files.push(CFile {
            module: module_name(&relative),
            includes: parsed.includes(),
            symbols: parsed.symbols(FileKind::of(&relative)),
            lines: parsed.code_lines(),
            path: relative,
        });
    }
    Ok(files)
}

/// The lines of code of a C or C++ source: blank lines, comments and
/// `#include` directives left out.
fn code_lines(source: &[u8], grammar: Grammar) -> Result<usize> {
    Ok(Parsed::new(source, grammar)?.code_lines())
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
        .filter_map(|e| {
            let source = std::fs::read(e.path()).ok()?;
            code_lines(&source, Grammar::of(e.path())).ok()
        })
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

/// The grammar a file is parsed with.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Grammar {
    C,
    Cpp,
    /// A `.h` header, which may hold either language: parsed as C, and as C++
    /// if that has fewer syntax errors.
    Either,
}

impl Grammar {
    fn of(path: &Path) -> Self {
        match path.extension().and_then(|e| e.to_str()) {
            Some("c") => Self::C,
            Some("h") => Self::Either,
            _ => Self::Cpp,
        }
    }
}

/// A C or C++ file's syntax tree.
struct Parsed<'a>(Syntax<'a>);

impl<'a> Parsed<'a> {
    fn new(source: &'a [u8], grammar: Grammar) -> Result<Self> {
        let c = || {
            Syntax::parse_as(
                source,
                &without_cplusplus(source),
                &tree_sitter_c::LANGUAGE.into(),
            )
        };
        let cpp = || Syntax::parse(source, &tree_sitter_cpp::LANGUAGE.into());
        let syntax = match grammar {
            Grammar::C => c()?,
            Grammar::Cpp => cpp()?,
            Grammar::Either => {
                let c = c()?;
                if c.root().has_error() {
                    let cpp = cpp()?;
                    if cpp.error_count() < c.error_count() {
                        cpp
                    } else {
                        c
                    }
                } else {
                    c
                }
            }
        };
        Ok(Self(syntax))
    }

    fn text(&self, node: Node) -> &'a str {
        self.0.text(node)
    }

    /// The `#include` operands, `#if` branches included.
    fn includes(&self) -> Vec<Include> {
        let mut includes = Vec::new();
        self.0.walk(|node| {
            if node.kind() != "preproc_include" {
                return Walk::Into;
            }
            if let Some(path) = node.child_by_field_name("path") {
                let text = self.text(path);
                let quoted = match path.kind() {
                    "string_literal" => Some(true),
                    "system_lib_string" => Some(false),
                    // An include computed by a macro.
                    _ => None,
                };
                if let Some(quoted) = quoted
                    && text.len() >= 2
                {
                    includes.push(Include {
                        target: text[1..text.len() - 1].to_owned(),
                        quoted,
                    });
                }
            }
            Walk::Over
        });
        includes
    }

    /// Lines holding something other than comments and `#include` directives.
    fn code_lines(&self) -> usize {
        self.0.code_lines(|node| node.kind() == "preproc_include")
    }

    fn symbols(&self, kind: FileKind) -> Symbols {
        let mut symbols = Symbols::default();
        self.definitions(self.0.root(), kind, &mut symbols);
        self.0.walk(|node| match node.kind() {
            "identifier" | "type_identifier" | "namespace_identifier" => {
                symbols.used.insert(self.text(node).to_owned());
                Walk::Over
            }
            "preproc_arg" => {
                symbols.used.extend(macro_identifiers(self.text(node)));
                Walk::Over
            }
            // A macro's parameters, and the names of members, which would
            // match unrelated globals.
            "preproc_params" | "field_identifier" => Walk::Over,
            _ => Walk::Into,
        });
        symbols
    }

    /// Collects the file-scope definitions of `node`'s children, see
    /// [`Symbols::defined`].
    fn definitions(&self, node: Node, kind: FileKind, symbols: &mut Symbols) {
        let in_header = kind == FileKind::Header;
        let mut cursor = node.walk();
        for child in node.named_children(&mut cursor) {
            match child.kind() {
                // Their content is still at file scope.
                "linkage_specification"
                | "declaration_list"
                | "namespace_definition"
                | "template_declaration"
                | "preproc_if"
                | "preproc_ifdef"
                | "preproc_else"
                | "preproc_elif"
                | "preproc_elifdef" => {
                    self.definitions(child, kind, symbols);
                }
                "function_definition" => {
                    symbols.has_function_bodies = true;
                    // A header's static inline functions are shared with every
                    // file including it.
                    if (in_header || !has_storage_class(child, self, "static"))
                        && let Some(name) = child
                            .child_by_field_name("declarator")
                            .and_then(|d| declarator_name(d, self))
                    {
                        symbols.defined.insert(name.to_owned());
                    }
                }
                "type_definition" if in_header => {
                    let mut cursor = child.walk();
                    for declarator in child.children_by_field_name("declarator", &mut cursor) {
                        symbols
                            .defined
                            .extend(declarator_name(declarator, self).map(str::to_owned));
                    }
                }
                "declaration"
                    if !in_header
                        && !has_storage_class(child, self, "static")
                        && !has_storage_class(child, self, "extern") =>
                {
                    let mut cursor = child.walk();
                    for declarator in child.children_by_field_name("declarator", &mut cursor) {
                        if !declares_function(declarator) {
                            symbols
                                .defined
                                .extend(declarator_name(declarator, self).map(str::to_owned));
                        }
                    }
                }
                "preproc_def" | "preproc_function_def" if in_header => {
                    symbols.defined.extend(
                        child
                            .child_by_field_name("name")
                            .map(|n| self.text(n).to_owned()),
                    );
                }
                _ => {}
            }
            if in_header {
                self.tagged_types(child, symbols);
            }
        }
    }

    /// Collects the tagged types `node` defines, at any depth, and the
    /// constants of its enums. Only definitions count: headers forward-declare
    /// the types of other modules.
    fn tagged_types(&self, node: Node, symbols: &mut Symbols) {
        walk_from(node, |node| match node.kind() {
            // Their definitions are collected by [`Self::definitions`].
            "preproc_if"
            | "preproc_ifdef"
            | "preproc_else"
            | "preproc_elif"
            | "preproc_elifdef"
            | "linkage_specification"
            | "namespace_definition"
            | "declaration_list"
            | "template_declaration"
            | "function_definition" => Walk::Over,
            "struct_specifier" | "union_specifier" | "enum_specifier" | "class_specifier" => {
                if let (Some(name), Some(_)) = (
                    node.child_by_field_name("name"),
                    node.child_by_field_name("body"),
                ) {
                    symbols.defined.insert(self.text(name).to_owned());
                }
                Walk::Into
            }
            "enumerator" => {
                symbols.defined.extend(
                    node.child_by_field_name("name")
                        .map(|n| self.text(n).to_owned()),
                );
                Walk::Over
            }
            _ => Walk::Into,
        });
    }
}

/// `source` with the code only C++ compilers see blanked out, as C compilers
/// never define `__cplusplus`. Generated headers put such code in the middle
/// of declarations, as in `enum Code #ifdef __cplusplus : uint8_t #endif {`,
/// which tree-sitter cannot parse. Offsets and lines are kept, so that the
/// tree still maps onto `source`.
fn without_cplusplus(source: &[u8]) -> Vec<u8> {
    /// Whether the lines of an open conditional are blanked out.
    #[derive(Clone, Copy)]
    enum Branch {
        /// A conditional not about `__cplusplus`.
        Other,
        /// The branch of a `__cplusplus` conditional, blanked if C++ only.
        Cplusplus { blanked: bool },
    }
    let mut out = source.to_vec();
    let mut open: Vec<Branch> = Vec::new();
    let mut start = 0;
    while start < source.len() {
        let end = source[start..]
            .iter()
            .position(|&b| b == b'\n')
            .map_or(source.len(), |p| start + p + 1);
        let line = String::from_utf8_lossy(&source[start..end]);
        let directive = line.trim_start().strip_prefix('#').map(|d| {
            let d = d.trim();
            let (name, rest) = d.split_once(char::is_whitespace).unwrap_or((d, ""));
            (name, rest.trim())
        });
        let blanked_before = open
            .iter()
            .any(|b| matches!(b, Branch::Cplusplus { blanked: true }));
        let mut blank_line = blanked_before;
        match directive {
            Some(("ifdef", "__cplusplus"))
            | Some(("if", "defined(__cplusplus)" | "__cplusplus")) => {
                open.push(Branch::Cplusplus { blanked: true });
                blank_line = true;
            }
            Some(("ifndef", "__cplusplus")) | Some(("if", "!defined(__cplusplus)")) => {
                open.push(Branch::Cplusplus { blanked: false });
                blank_line = true;
            }
            Some(("if" | "ifdef" | "ifndef", _)) => open.push(Branch::Other),
            Some(("else", _)) => {
                if let Some(Branch::Cplusplus { blanked }) = open.last_mut() {
                    *blanked = !*blanked;
                    blank_line = true;
                }
            }
            Some(("endif", _)) => {
                if let Some(Branch::Cplusplus { .. }) = open.pop() {
                    blank_line = true;
                }
            }
            _ => {}
        }
        if blank_line {
            for b in &mut out[start..end] {
                if *b != b'\n' {
                    *b = b' ';
                }
            }
        }
        start = end;
    }
    out
}

fn has_storage_class(node: Node, parsed: &Parsed, class: &str) -> bool {
    let mut cursor = node.walk();
    node.children(&mut cursor)
        .any(|c| c.kind() == "storage_class_specifier" && parsed.text(c) == class)
}

/// The name a declarator declares, if it is a plain identifier: qualified C++
/// names, operators and destructors are left out.
fn declarator_name<'a>(mut node: Node, parsed: &Parsed<'a>) -> Option<&'a str> {
    loop {
        match node.kind() {
            "identifier" | "type_identifier" | "primitive_type" => return Some(parsed.text(node)),
            "parenthesized_declarator" | "reference_declarator" | "attributed_declarator" => {
                node = node.named_child(0)?;
            }
            _ => node = node.child_by_field_name("declarator")?,
        }
    }
}

/// Whether a declaration's declarator involves a function: a prototype, or a
/// pointer to a function.
fn declares_function(mut node: Node) -> bool {
    loop {
        if node.kind() == "function_declarator" {
            return true;
        }
        match node.child_by_field_name("declarator").or_else(|| {
            node.named_child(0)
                .filter(|_| node.kind() == "parenthesized_declarator")
        }) {
            Some(inner) => node = inner,
            None => return false,
        }
    }
}

/// The identifiers in a macro body, which tree-sitter leaves unparsed, except
/// those in literals and member names.
fn macro_identifiers(body: &str) -> impl Iterator<Item = String> + '_ {
    let bytes = body.as_bytes();
    let mut i = 0;
    let mut names = Vec::new();
    while i < bytes.len() {
        let c = bytes[i];
        if c.is_ascii_alphabetic() || c == b'_' {
            let start = i;
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
            let before = body[..start].trim_end();
            if !before.ends_with('.') && !before.ends_with("->") {
                names.push(&body[start..i]);
            }
        } else if c.is_ascii_digit() {
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'.') {
                i += 1;
            }
        } else if c == b'"' || c == b'\'' {
            i += 1;
            while i < bytes.len() && bytes[i] != c {
                i += if bytes[i] == b'\\' { 2 } else { 1 };
            }
            i += 1;
        } else {
            i += 1;
        }
    }
    names.into_iter().map(str::to_owned)
}

/// The modules holding a source file, as opposed to headers alone.
pub fn modules_with_source(files: &[CFile]) -> BTreeSet<&str> {
    files
        .iter()
        .filter(|f| !f.is_header())
        .map(|f| f.module.as_str())
        .collect()
}

// Miri can't run the tree-sitter grammars, which are C code.
#[cfg(all(test, not(miri)))]
mod tests {
    use super::*;

    fn parse(source: &str, grammar: Grammar) -> Parsed<'_> {
        Parsed::new(source.as_bytes(), grammar).unwrap()
    }

    fn defined(source: &str, kind: FileKind) -> Vec<String> {
        let grammar = match kind {
            FileKind::Header => Grammar::Either,
            FileKind::Source => Grammar::C,
        };
        let symbols = parse(source, grammar).symbols(kind).defined;
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
    fn header_ignores_code_for_cplusplus_only() {
        let header = r#"
enum Code
#ifdef __cplusplus
  : uint8_t
#endif // __cplusplus
 {
  CODE_OK = 0,
  CODE_ERR,
};
#ifndef __cplusplus
typedef uint8_t Code;
#else
typedef int CppOnly;
#endif
"#;
        assert_eq!(
            defined(header, FileKind::Header),
            ["CODE_ERR", "CODE_OK", "Code"]
        );
    }

    #[test]
    fn cplusplus_header_defines_classes_and_templates() {
        let header = r#"
#pragma once
namespace fs = std::filesystem;
namespace vecsim {
template <typename T> class Index : public Base<T> {
  void *operator new(size_t size);
  int size() const { return 0; }
};
template <bool partial> float L2_SVE(const void *a, size_t dim) { return 0; }
}
"#;
        assert_eq!(defined(header, FileKind::Header), ["Index", "L2_SVE"]);
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
        let source = "#include <stdio.h>\n\n/* a\n   comment */\n#define N \\\n  1\nint f(void) {\n  return N; // end\n}\n";
        assert_eq!(parse(source, Grammar::C).code_lines(), 5);
    }

    #[test]
    fn used_identifiers_skip_member_names() {
        let source = "#define LEN(o) ((o).len + o->cap)\nvoid f(Spec *s) { s->size = obj.len + Spec_Size(s); }";
        let used = parse(source, Grammar::C).symbols(FileKind::Source).used;
        assert!(used.contains("Spec_Size") && used.contains("Spec") && used.contains("obj"));
        assert!(!used.contains("size") && !used.contains("len") && !used.contains("cap"));
    }

    #[test]
    fn parses_includes() {
        let source = "#include \"a/b.h\"\n#ifdef X\n#  include <c.h> // x\n#endif\n#define X 1\n";
        let includes: Vec<_> = parse(source, Grammar::C)
            .includes()
            .into_iter()
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
