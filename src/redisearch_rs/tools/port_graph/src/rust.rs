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

use anyhow::{Context, Result};
use cargo_metadata::{DependencyKind, Metadata, Package};
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use syn::spanned::Spanned;
use syn::visit::Visit;
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

/// The names a crate's production code refers to through the bindings.
/// The names a crate's production code uses through the bindings, and the
/// functions it exports to C.
fn scan_sources(src: &Path) -> Sources {
    let mut names = BTreeSet::new();
    let mut exported = BTreeSet::new();
    let mut lines = 0;
    for entry in WalkDir::new(src)
        .into_iter()
        .filter_entry(|e| e.file_name() != "tests")
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "rs"))
    {
        let path = entry.path();
        let Ok(content) = std::fs::read_to_string(path) else {
            continue;
        };
        let file = match syn::parse_file(&content) {
            Ok(file) => file,
            // Syntax accepted only once rewritten by an attribute macro, such
            // as the pre-stabilization GAT syntax `#[gat]` takes.
            Err(_) => {
                if let Ok(tokens) = content.parse() {
                    names.extend(qualified_names_in_tokens(tokens));
                }
                lines += code_lines(&content, &[]);
                continue;
            }
        };
        let mut tests = TestItems::default();
        tests.visit_file(&file);
        lines += code_lines(&content, &tests.lines);
        let mut visitor = BindingUses::default();
        visitor.visit_file(&file);
        names.extend(visitor.qualified);
        names.extend(visitor.imported);
        names.extend(visitor.declared);
        if visitor.glob_imported {
            names.extend(visitor.bare);
        }
        exported.extend(visitor.exported);
    }
    Sources {
        c_names: names,
        exported,
        lines,
    }
}

/// What [`scan_sources`] finds in a crate, see the fields of [`Crate`].
struct Sources {
    c_names: BTreeSet<String>,
    exported: BTreeSet<String>,
    lines: usize,
}

/// The lines holding code in `content`, leaving out blank and comment lines
/// and those within `excluded`, inclusive 1-based line ranges.
fn code_lines(content: &str, excluded: &[(usize, usize)]) -> usize {
    content
        .lines()
        .enumerate()
        .filter(|(i, line)| {
            let line_number = i + 1;
            let trimmed = line.trim();
            !trimmed.is_empty()
                && !trimmed.starts_with("//")
                && !excluded
                    .iter()
                    .any(|(start, end)| (*start..=*end).contains(&line_number))
        })
        .count()
}

/// The line ranges of a file's test-only items.
#[derive(Default)]
struct TestItems {
    lines: Vec<(usize, usize)>,
}

impl<'ast> Visit<'ast> for TestItems {
    fn visit_item(&mut self, item: &'ast syn::Item) {
        let attrs: &[syn::Attribute] = match item {
            syn::Item::Mod(i) => &i.attrs,
            syn::Item::Fn(i) => &i.attrs,
            syn::Item::Impl(i) => &i.attrs,
            syn::Item::Use(i) => &i.attrs,
            syn::Item::Const(i) => &i.attrs,
            syn::Item::Static(i) => &i.attrs,
            syn::Item::Struct(i) => &i.attrs,
            syn::Item::Enum(i) => &i.attrs,
            _ => &[],
        };
        if is_test_only(attrs) {
            let span = item.span();
            self.lines.push((span.start().line, span.end().line));
        } else {
            syn::visit::visit_item(self, item);
        }
    }
}

/// Collects, in one file, every name that may come from the bindings. Names
/// that are not C symbols are filtered out later, against the headers.
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

impl BindingUses {
    fn collect_imports(&mut self, from_bindings: bool, tree: &syn::UseTree) {
        match tree {
            syn::UseTree::Path(p) => {
                self.collect_imports(from_bindings || p.ident == BINDINGS_CRATE, &p.tree)
            }
            syn::UseTree::Name(n) if from_bindings => {
                self.imported.insert(n.ident.to_string());
            }
            syn::UseTree::Rename(r) if from_bindings => {
                self.imported.insert(r.ident.to_string());
            }
            syn::UseTree::Glob(_) if from_bindings => self.glob_imported = true,
            syn::UseTree::Group(g) => {
                for item in &g.items {
                    self.collect_imports(from_bindings, item);
                }
            }
            _ => {}
        }
    }
}

impl<'ast> Visit<'ast> for BindingUses {
    fn visit_item_mod(&mut self, node: &'ast syn::ItemMod) {
        if !is_test_only(&node.attrs) {
            syn::visit::visit_item_mod(self, node);
        }
    }

    fn visit_item_fn(&mut self, node: &'ast syn::ItemFn) {
        if is_no_mangle(&node.attrs) {
            self.exported.insert(node.sig.ident.to_string());
        }
        if !is_test_only(&node.attrs) {
            syn::visit::visit_item_fn(self, node);
        }
    }

    fn visit_item_use(&mut self, node: &'ast syn::ItemUse) {
        self.collect_imports(false, &node.tree);
    }

    fn visit_foreign_item_fn(&mut self, node: &'ast syn::ForeignItemFn) {
        self.declared.insert(node.sig.ident.to_string());
    }

    fn visit_foreign_item_static(&mut self, node: &'ast syn::ForeignItemStatic) {
        self.declared.insert(node.ident.to_string());
    }

    fn visit_path(&mut self, node: &'ast syn::Path) {
        let segments = &node.segments;
        if segments.len() == 1 {
            self.bare.insert(segments[0].ident.to_string());
        } else if segments.iter().any(|s| s.ident == BINDINGS_CRATE)
            && let Some(last) = segments.last()
        {
            self.qualified.insert(last.ident.to_string());
        }
        syn::visit::visit_path(self, node);
    }
}

/// The names used through the bindings crate in a file that does not parse:
/// the last segment of each path through it, and every name imported from it
/// by a group. Less precise than [`BindingUses`], which also sees glob
/// imports and leaves test-only code out.
fn qualified_names_in_tokens(tokens: proc_macro2::TokenStream) -> BTreeSet<String> {
    use proc_macro2::TokenTree;

    let mut names = BTreeSet::new();
    let tokens: Vec<TokenTree> = tokens.into_iter().collect();
    let is_path_sep = |i: usize| {
        matches!(
            (tokens.get(i), tokens.get(i + 1)),
            (Some(TokenTree::Punct(a)), Some(TokenTree::Punct(b)))
                if a.as_char() == ':' && b.as_char() == ':'
        )
    };
    for (i, token) in tokens.iter().enumerate() {
        match token {
            TokenTree::Ident(ident) if ident == BINDINGS_CRATE && is_path_sep(i + 1) => {
                let mut j = i + 3;
                let mut last = None;
                while let Some(next) = tokens.get(j) {
                    match next {
                        TokenTree::Ident(segment) => last = Some(segment.to_string()),
                        TokenTree::Group(group) => {
                            names.extend(group.stream().into_iter().filter_map(|t| match t {
                                TokenTree::Ident(name) => Some(name.to_string()),
                                _ => None,
                            }));
                            break;
                        }
                        TokenTree::Punct(_) | TokenTree::Literal(_) => break,
                    }
                    if !is_path_sep(j + 1) {
                        break;
                    }
                    j += 3;
                }
                names.extend(last);
            }
            TokenTree::Group(group) => names.extend(qualified_names_in_tokens(group.stream())),
            _ => {}
        }
    }
    names
}

/// Whether the attributes export an item under its own name: `#[no_mangle]`,
/// or `#[unsafe(no_mangle)]` since edition 2024.
fn is_no_mangle(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|attr| {
        attr.path().is_ident("no_mangle")
            || (attr.path().is_ident("unsafe")
                && attr
                    .parse_args::<syn::Ident>()
                    .is_ok_and(|i| i == "no_mangle"))
    })
}

/// Whether the attributes make an item exist only in tests.
fn is_test_only(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|attr| {
        if attr.path().is_ident("test") {
            return true;
        }
        if !attr.path().is_ident("cfg") {
            return false;
        }
        attr.parse_args::<syn::Ident>().is_ok_and(|i| i == "test")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn uses(source: &str) -> BTreeSet<String> {
        let mut visitor = BindingUses::default();
        visitor.visit_file(&syn::parse_file(source).unwrap());
        let mut names = visitor.qualified;
        names.extend(visitor.imported);
        names.extend(visitor.declared);
        if visitor.glob_imported {
            names.extend(visitor.bare);
        }
        names
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
    fn falls_back_to_tokens_on_unparsable_files() {
        let source = r#"
            use ffi::{Spec_Load, Spec_Free};
            #[gat]
            impl<'a> LendingIterator for Iter<'a> {
                type Item<'next> where Self: 'next = ffi::RedisJSON;
                fn next(&mut self) { unsafe { ffi::nested::JSON_Next(self.0) } }
            }
            "#;
        assert!(syn::parse_file(source).is_err());
        let names = qualified_names_in_tokens(source.parse().unwrap());
        let expected = ["JSON_Next", "RedisJSON", "Spec_Free", "Spec_Load"];
        assert_eq!(names, expected.map(String::from).into());
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
        let mut visitor = BindingUses::default();
        let source = r#"
            #[unsafe(no_mangle)] pub extern "C" fn RLookup_Init() {}
            #[no_mangle] pub extern "C" fn Old_Style() {}
            pub fn not_exported() {}
        "#;
        visitor.visit_file(&syn::parse_file(source).unwrap());
        assert_eq!(
            visitor.exported,
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
        let mut tests = TestItems::default();
        tests.visit_file(&syn::parse_file(source).unwrap());
        assert_eq!(code_lines(source, &tests.lines), 3);
    }

    #[test]
    fn glob_import_makes_bare_names_candidates() {
        assert!(uses("use ffi::*; fn f() { Spec_Load() }").contains("Spec_Load"));
        assert!(!uses("fn f() { Spec_Load() }").contains("Spec_Load"));
    }
}
