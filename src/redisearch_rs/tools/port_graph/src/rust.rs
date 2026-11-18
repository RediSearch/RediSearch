/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Scanning of the Rust workspace: production crates, their dependencies on
//! each other, the C headers generated from them, and the C names they use.

use crate::syntax::{Syntax, Walk};
use anyhow::{Context, Result};
use cargo_metadata::{DependencyKind, Metadata, Package};
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use tree_sitter::Node;
use walkdir::WalkDir;

/// Crates that only support testing, benchmarking or the build.
const NON_PRODUCTION_CRATES: &[&str] = &[
    "build_utils",
    "redis_mock",
    "tracing_assert",
    "workspace_hack",
];

/// Crates left out because they would connect to nearly everything:
/// - `ffi` holds the bindings to all of C; the C names each crate uses
///   through it are resolved to their modules instead.
/// - `redisearch_rs` only bundles every FFI crate into the static library.
const HUB_CRATES: &[&str] = &["ffi", "redisearch_rs"];

/// Directories of the crates bridging Rust and C, which shrink back as the C
/// side goes away.
const FFI_DIRS: &[&str] = &["c_entrypoint", "c_wrappers"];

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CrateKind {
    /// Rust code that does not exist to bridge with C.
    Core,
    /// A crate exposing Rust to C, or wrapping C for Rust: one of [`FFI_DIRS`].
    Ffi,
}

fn crate_kind(manifest_path: &Path) -> CrateKind {
    let in_ffi_dir = manifest_path.components().any(|c| {
        c.as_os_str()
            .to_str()
            .is_some_and(|c| FFI_DIRS.contains(&c))
    });
    if in_ffi_dir {
        CrateKind::Ffi
    } else {
        CrateKind::Core
    }
}

/// The crate holding the bindings to C.
const BINDINGS_CRATE: &str = "ffi";

pub struct Crate {
    pub name: String,
    pub kind: CrateKind,
    /// Workspace crates this one depends on, outside dev and build dependencies.
    pub dependencies: BTreeSet<String>,
    /// Names this crate refers to through the bindings or declares in its own
    /// `extern` blocks.
    pub c_names: BTreeSet<String>,
    /// Functions this crate exports to C, by their symbol name.
    pub exported: BTreeSet<String>,
    /// Lines of production code: blank lines, comments and test-only items
    /// left out.
    pub lines: usize,
}

/// The production crates of the workspace.
pub fn scan(metadata: &Metadata) -> Result<Vec<Crate>> {
    let packages: Vec<&Package> = metadata
        .workspace_packages()
        .into_iter()
        .filter(|p| is_production(p))
        .collect();
    let names: BTreeSet<&str> = packages.iter().map(|p| p.name.as_str()).collect();
    packages
        .iter()
        .map(|p| {
            let src = p
                .manifest_path
                .parent()
                .context("manifest without parent")?;
            let Sources {
                c_names,
                exported,
                lines,
            } = scan_sources(src.join("src").as_std_path());
            Ok(Crate {
                name: p.name.to_string(),
                kind: crate_kind(p.manifest_path.as_std_path()),
                dependencies: p
                    .dependencies
                    .iter()
                    .filter(|d| d.kind == DependencyKind::Normal && names.contains(d.name.as_str()))
                    .map(|d| d.name.clone())
                    .collect(),
                c_names,
                exported,
                lines,
            })
        })
        .collect()
}

fn is_production(package: &Package) -> bool {
    let name = package.name.as_str();
    !NON_PRODUCTION_CRATES.contains(&name)
        && !HUB_CRATES.contains(&name)
        && !name.ends_with("_bencher")
        && !name.ends_with("_test_utils")
        && !package
            .manifest_path
            .components()
            .any(|c| c.as_str() == "tools")
}

/// Maps each header generated into `headers_dir` to the crate it is generated
/// from: the header is named after the crate, unless `cheadergen.toml` renames
/// it.
pub fn generated_headers(
    workspace_root: &Path,
    headers_dir: &Path,
    crates: &[Crate],
) -> Result<HashMap<PathBuf, String>> {
    let config_path = workspace_root.join("cheadergen.toml");
    let config: toml::Table = toml::from_str(
        &std::fs::read_to_string(&config_path)
            .with_context(|| format!("reading {}", config_path.display()))?,
    )?;
    let renamed = |name: &str| {
        config
            .get("package")?
            .get(name)?
            .get("header_name")?
            .as_str()
            .map(str::to_owned)
    };
    Ok(crates
        .iter()
        .map(|c| {
            let header = renamed(&c.name).unwrap_or_else(|| c.name.clone());
            (headers_dir.join(format!("{header}.h")), c.name.clone())
        })
        .collect())
}

/// The names a crate's production code uses through the bindings, the
/// functions it exports to C, and its lines of code.
fn scan_sources(src: &Path) -> Sources {
    let mut sources = Sources::default();
    for entry in WalkDir::new(src)
        .into_iter()
        .filter_entry(|e| e.file_name() != "tests")
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "rs"))
    {
        let Ok(content) = std::fs::read(entry.path()) else {
            continue;
        };
        if let Ok(file) = RustFile::parse(&content) {
            file.scan(&mut sources);
        }
    }
    sources
}

/// What [`scan_sources`] finds in a crate, see the fields of [`Crate`].
#[derive(Default)]
struct Sources {
    c_names: BTreeSet<String>,
    exported: BTreeSet<String>,
    lines: usize,
}

/// A Rust file's syntax tree. tree-sitter recovers from what it cannot parse,
/// such as the syntax the `#[gat]` attribute macro takes, so that the rest of
/// the file is still seen.
struct RustFile<'a>(Syntax<'a>);

impl<'a> RustFile<'a> {
    fn parse(source: &'a [u8]) -> Result<Self> {
        Ok(Self(Syntax::parse(
            source,
            &tree_sitter_rust::LANGUAGE.into(),
        )?))
    }

    fn scan(&self, sources: &mut Sources) {
        sources.lines += self.0.code_lines(|node| self.is_test_only(node));
        let uses = self.binding_uses();
        sources.c_names.extend(uses.qualified);
        sources.c_names.extend(uses.imported);
        sources.c_names.extend(uses.declared);
        if uses.glob_imported {
            sources.c_names.extend(uses.bare);
        }
        sources.exported.extend(uses.exported);
    }

    /// Collects every name that may come from the bindings. Names that are
    /// not C symbols are filtered out later, against the headers.
    fn binding_uses(&self) -> BindingUses {
        let mut uses = BindingUses::default();
        self.0.walk(|node| {
            if self.is_test_only(node) {
                return Walk::Over;
            }
            match node.kind() {
                "use_declaration" => {
                    if let Some(argument) = node.child_by_field_name("argument") {
                        self.collect_imports(false, argument, &mut uses);
                    }
                    Walk::Over
                }
                "function_item" => {
                    if self.has_attribute(node, &["no_mangle", "unsafe(no_mangle)"])
                        && let Some(name) = node.child_by_field_name("name")
                    {
                        uses.exported.insert(self.text(name));
                    }
                    Walk::Into
                }
                "foreign_mod_item" => {
                    let body = node.child_by_field_name("body");
                    for item in body.iter().flat_map(|b| named_children(*b)) {
                        if matches!(item.kind(), "function_signature_item" | "static_item")
                            && let Some(name) = item.child_by_field_name("name")
                        {
                            uses.declared.insert(self.text(name));
                        }
                    }
                    Walk::Into
                }
                "scoped_identifier" | "scoped_type_identifier" => {
                    // The inner paths of `a::b::c` are the same path.
                    let is_inner =
                        node.parent().and_then(|p| p.child_by_field_name("path")) == Some(node);
                    if !is_inner
                        && self.path_has_bindings(node)
                        && let Some(name) = node.child_by_field_name("name")
                    {
                        uses.qualified.insert(self.text(name));
                    }
                    Walk::Into
                }
                "identifier" | "type_identifier" => {
                    if !node
                        .parent()
                        .is_some_and(|p| p.kind().starts_with("scoped_"))
                    {
                        uses.bare.insert(self.text(node));
                    }
                    Walk::Over
                }
                "token_tree" => {
                    self.token_paths(node, &mut uses.qualified);
                    Walk::Into
                }
                _ => Walk::Into,
            }
        });
        uses
    }

    fn collect_imports(&self, from_bindings: bool, node: Node, uses: &mut BindingUses) {
        let through = |path: Option<Node>| {
            from_bindings || path.is_some_and(|p| self.has_bindings_segment(p))
        };
        match node.kind() {
            "identifier" if from_bindings => {
                uses.imported.insert(self.text(node));
            }
            "scoped_identifier" => {
                if through(node.child_by_field_name("path"))
                    && let Some(name) = node.child_by_field_name("name")
                {
                    uses.imported.insert(self.text(name));
                }
            }
            "use_as_clause" => {
                if let Some(path) = node.child_by_field_name("path") {
                    self.collect_imports(from_bindings, path, uses);
                }
            }
            "scoped_use_list" => {
                let from_bindings = through(node.child_by_field_name("path"));
                if let Some(list) = node.child_by_field_name("list") {
                    self.collect_imports(from_bindings, list, uses);
                }
            }
            "use_list" => {
                for item in named_children(node) {
                    self.collect_imports(from_bindings, item, uses);
                }
            }
            "use_wildcard" => {
                if through(node.named_child(0)) {
                    uses.glob_imported = true;
                }
            }
            _ => {}
        }
    }

    /// Whether a scoped path goes through the bindings crate.
    fn path_has_bindings(&self, node: Node) -> bool {
        node.child_by_field_name("path")
            .is_some_and(|p| self.has_bindings_segment(p))
    }

    /// Whether a path, a segment or a scoped path, names the bindings crate.
    fn has_bindings_segment(&self, mut node: Node) -> bool {
        loop {
            match node.kind() {
                "identifier" => return self.0.text(node) == BINDINGS_CRATE,
                "scoped_identifier" => {
                    if node
                        .child_by_field_name("name")
                        .is_some_and(|n| self.0.text(n) == BINDINGS_CRATE)
                    {
                        return true;
                    }
                    match node.child_by_field_name("path") {
                        Some(path) => node = path,
                        None => return false,
                    }
                }
                _ => return false,
            }
        }
    }

    /// The names used through the bindings crate in a macro's tokens, which
    /// tree-sitter leaves unparsed: the last segment of each `ffi::a::B` path,
    /// and every name of an `ffi::{A, B}` group.
    fn token_paths(&self, tree: Node, names: &mut BTreeSet<String>) {
        let tokens: Vec<Node> = children(tree);
        let is_separator = |i: usize| tokens.get(i).is_some_and(|t| t.kind() == "::");
        for (i, token) in tokens.iter().enumerate() {
            if token.kind() != "identifier"
                || self.0.text(*token) != BINDINGS_CRATE
                || !is_separator(i + 1)
            {
                continue;
            }
            let mut j = i + 2;
            let mut last = None;
            while let Some(next) = tokens.get(j) {
                match next.kind() {
                    "identifier" => last = Some(self.text(*next)),
                    "token_tree" => {
                        names.extend(
                            children(*next)
                                .into_iter()
                                .filter(|t| t.kind() == "identifier")
                                .map(|t| self.text(t)),
                        );
                        break;
                    }
                    _ => break,
                }
                if !is_separator(j + 1) {
                    break;
                }
                j += 2;
            }
            names.extend(last);
        }
    }

    /// Whether `node` is an item existing only in tests, or an attribute of
    /// one.
    fn is_test_only(&self, node: Node) -> bool {
        let mut item = node;
        while item.kind() == "attribute_item" || COMMENTS.contains(&item.kind()) {
            match item.next_named_sibling() {
                Some(next) => item = next,
                None => return false,
            }
        }
        // An attribute applies to the next item only.
        if item != node && node.kind() != "attribute_item" {
            return false;
        }
        self.has_attribute(item, &["test", "cfg(test)"])
    }

    /// Whether one of the outer attributes of `item`, written without spaces,
    /// is in `attributes`.
    fn has_attribute(&self, item: Node, attributes: &[&str]) -> bool {
        let mut sibling = item.prev_named_sibling();
        while let Some(node) = sibling {
            match node.kind() {
                "attribute_item" => {
                    let attribute: String = node
                        .named_child(0)
                        .map(|a| self.0.text(a))
                        .unwrap_or("")
                        .chars()
                        .filter(|c| !c.is_whitespace())
                        .collect();
                    if attributes.contains(&attribute.as_str()) {
                        return true;
                    }
                }
                kind if COMMENTS.contains(&kind) => {}
                _ => return false,
            }
            sibling = node.prev_named_sibling();
        }
        false
    }

    fn text(&self, node: Node) -> String {
        self.0.text(node).to_owned()
    }
}

/// Comment node kinds of the Rust grammar, which may sit between an item and
/// its attributes.
const COMMENTS: &[&str] = &["line_comment", "block_comment"];

fn children(node: Node) -> Vec<Node> {
    let mut cursor = node.walk();
    node.children(&mut cursor).collect()
}

fn named_children(node: Node) -> Vec<Node> {
    let mut cursor = node.walk();
    node.named_children(&mut cursor).collect()
}

/// The names one file may use from the bindings, see
/// [`RustFile::binding_uses`].
#[derive(Default)]
struct BindingUses {
    /// Last segments of paths going through the bindings crate.
    qualified: BTreeSet<String>,
    /// Names imported from the bindings crate, by their original name.
    imported: BTreeSet<String>,
    /// Whether the bindings crate is glob-imported, making every bare name a
    /// candidate.
    glob_imported: bool,
    /// Single-segment paths.
    bare: BTreeSet<String>,
    /// Functions and static variables declared in the crate's own `extern`
    /// blocks.
    declared: BTreeSet<String>,
    /// Functions exported to C under their own name.
    exported: BTreeSet<String>,
}

// Miri can't run the tree-sitter grammars, which are C code.
#[cfg(all(test, not(miri)))]
mod tests {
    use super::*;

    fn scan(source: &str) -> Sources {
        let mut sources = Sources::default();
        RustFile::parse(source.as_bytes())
            .unwrap()
            .scan(&mut sources);
        sources
    }

    fn uses(source: &str) -> BTreeSet<String> {
        scan(source).c_names
    }

    #[test]
    fn collects_names_from_the_bindings() {
        let names = uses(
            r#"
            use ffi::{Spec_Load, t_docId as DocId};
            unsafe extern "C" { fn Local_Decl(); }
            fn f(x: ffi::RSIndexResult) { unsafe { ffi::Spec_Free(x) } }
            #[cfg(test)]
            mod tests { fn g() { ffi::TestOnly() } }
            "#,
        );
        let expected = [
            "Local_Decl",
            "RSIndexResult",
            "Spec_Free",
            "Spec_Load",
            "t_docId",
        ];
        assert_eq!(names, expected.map(String::from).into());
    }

    #[test]
    fn recovers_from_unparsable_code() {
        let source = r#"
            use ffi::{Spec_Load, Spec_Free};
            #[gat]
            impl<'a> LendingIterator for Iter<'a> {
                type Item<'next> where Self: 'next = ffi::RedisJSON;
                fn next(&mut self) { unsafe { ffi::nested::JSON_Next(self.0) } }
            }
            "#;
        let names = uses(source);
        for name in ["JSON_Next", "RedisJSON", "Spec_Free", "Spec_Load"] {
            assert!(names.contains(name), "{name} missing from {names:?}");
        }
    }

    #[test]
    fn ffi_crates_are_those_under_the_bridge_dirs() {
        let kind = |path: &str| crate_kind(Path::new(path));
        assert_eq!(
            kind("src/redisearch_rs/c_entrypoint/geo_ffi/Cargo.toml"),
            CrateKind::Ffi
        );
        assert_eq!(
            kind("src/redisearch_rs/c_wrappers/buffer/Cargo.toml"),
            CrateKind::Ffi
        );
        assert_eq!(kind("src/redisearch_rs/geo/Cargo.toml"), CrateKind::Core);
    }

    #[test]
    fn collects_exported_functions() {
        let source = r#"
            #[unsafe(no_mangle)] pub extern "C" fn RLookup_Init() {}
            #[no_mangle] pub extern "C" fn Old_Style() {}
            pub fn not_exported() {}
        "#;
        assert_eq!(
            scan(source).exported,
            ["Old_Style", "RLookup_Init"].map(String::from).into()
        );
    }

    #[test]
    fn code_lines_skip_comments_and_tests() {
        let source = r#"
/// Doc.
pub fn f() -> u32 {
    // Comment.
    1
}

#[cfg(test)]
mod tests {
    #[test]
    fn t() {}
}
"#;
        assert_eq!(scan(source).lines, 3);
    }

    #[test]
    fn finds_bindings_in_macro_arguments() {
        let names = uses(r#"fn f() { println!("{}", ffi::MAX_LEN); m!(ffi::{A, B}); }"#);
        assert_eq!(names, ["A", "B", "MAX_LEN"].map(String::from).into());
    }

    #[test]
    fn test_only_items_export_nothing() {
        let source = r#"
            #[cfg(test)]
            // A stub for the linker.
            #[unsafe(no_mangle)]
            extern "C" fn Stub() {}
        "#;
        assert!(scan(source).exported.is_empty());
    }

    #[test]
    fn glob_import_makes_bare_names_candidates() {
        assert!(uses("use ffi::*; fn f() { Spec_Load() }").contains("Spec_Load"));
        assert!(!uses("fn f() { Spec_Load() }").contains("Spec_Load"));
    }
}
