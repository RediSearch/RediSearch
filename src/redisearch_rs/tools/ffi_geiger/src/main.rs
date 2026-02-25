/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::cmp::Reverse;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use cargo_metadata::MetadataCommand;
use syn::spanned::Spanned;
use syn::visit::Visit;
use walkdir::WalkDir;

/// Build a dictionary of all bindgen-generated FFI function names by parsing the
/// `bindings.rs` file from the `ffi` crate's build artifacts.
fn build_ffi_dictionary(workspace_target_dir: &Path) -> Result<HashSet<String>> {
    // Search for bindings.rs under target/*/build/ffi-*/out/bindings.rs
    let mut candidates: Vec<PathBuf> = Vec::new();

    for entry in WalkDir::new(workspace_target_dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name() == "bindings.rs")
    {
        let path = entry.path();
        // Match pattern: .../build/ffi-<hash>/out/bindings.rs
        let components: Vec<_> = path.components().collect();
        let len = components.len();
        if len >= 4 {
            let out = components[len - 2].as_os_str().to_str().unwrap_or("");
            let ffi_hash = components[len - 3].as_os_str().to_str().unwrap_or("");
            let build = components[len - 4].as_os_str().to_str().unwrap_or("");

            if build == "build" && ffi_hash.starts_with("ffi-") && out == "out" {
                candidates.push(path.to_path_buf());
            }
        }
    }

    if candidates.is_empty() {
        bail!(
            "Could not find ffi bindings.rs in target directory: {}\n\
             Please build the project first: ./build.sh",
            workspace_target_dir.display()
        );
    }

    // If multiple candidates exist (debug + release), pick the most recently modified.
    candidates.sort_by(|a, b| {
        let a_mod = std::fs::metadata(a)
            .and_then(|m| m.modified())
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        let b_mod = std::fs::metadata(b)
            .and_then(|m| m.modified())
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        b_mod.cmp(&a_mod)
    });

    let bindings_path = &candidates[0];
    eprintln!("Using bindings from: {}", bindings_path.display());

    let content = std::fs::read_to_string(bindings_path)
        .with_context(|| format!("Failed to read {}", bindings_path.display()))?;
    let file = syn::parse_file(&content)
        .with_context(|| format!("Failed to parse {}", bindings_path.display()))?;

    let mut ffi_fns = HashSet::new();

    for item in &file.items {
        // bindgen generates `unsafe extern "C" { ... }` (bindgen >=0.70)
        // or `extern "C" { ... }` (older bindgen) blocks
        if let syn::Item::ForeignMod(foreign_mod) = item {
            for foreign_item in &foreign_mod.items {
                if let syn::ForeignItem::Fn(f) = foreign_item {
                    ffi_fns.insert(f.sig.ident.to_string());
                }
            }
        }
    }

    eprintln!(
        "Found {} FFI function signatures in bindings",
        ffi_fns.len()
    );
    Ok(ffi_fns)
}

/// Collect FFI symbol names from local `extern "C"` blocks in a crate's source.
///
/// Some crates declare their own `unsafe extern "C" { ... }` blocks to import
/// C functions directly, instead of going through the central `ffi` crate (e.g.
/// to avoid circular dependencies). These symbols need to be part of the FFI
/// dictionary so that calls to them are counted.
fn collect_local_ffi_declarations(src_dir: &Path) -> HashSet<String> {
    let mut symbols = HashSet::new();
    for entry in WalkDir::new(src_dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| !e.path().components().any(|c| c.as_os_str() == "tests"))
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "rs"))
    {
        let path = entry.path();
        let Ok(content) = std::fs::read_to_string(path) else {
            continue;
        };
        let Ok(file) = syn::parse_file(&content) else {
            continue;
        };
        for item in &file.items {
            if let syn::Item::ForeignMod(foreign_mod) = item {
                for foreign_item in &foreign_mod.items {
                    if let syn::ForeignItem::Fn(f) = foreign_item {
                        symbols.insert(f.sig.ident.to_string());
                    }
                }
            }
        }
    }
    symbols
}

/// Check if attributes contain `#[cfg(test)]`.
fn is_cfg_test(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|attr| {
        if !attr.path().is_ident("cfg") {
            return false;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("test") {
                Err(syn::Error::new_spanned(&meta.path, "found"))
            } else {
                Ok(())
            }
        })
        .is_err()
    })
}

/// Visitor that detects invocations of bindgen-generated FFI functions from the `ffi` crate.
struct FfiCallVisitor<'a> {
    /// All bindgen fn names.
    ffi_dict: &'a HashSet<String>,
    /// FFI fns imported in the current scope (via `use ffi::{...}`).
    /// Maps local name (possibly an alias) to the original FFI symbol name.
    imported_ffi: HashMap<String, String>,
    /// Whether a glob import `use ffi::*` was seen.
    glob_imported: bool,
    /// FFI symbols declared locally via `unsafe extern "C" { ... }` blocks
    /// (pre-computed by [`collect_local_ffi_declarations`]).
    ///
    /// NOTE: this set is crate-wide, not per-module. A bare call `Foo()` will
    /// be counted as FFI if *any* module in the crate declares
    /// `extern "C" { fn Foo(); }`, even if the call is in a different module.
    /// In practice this never produces false positives because C symbols use
    /// `PascalCase`/`camelCase` while Rust functions use `snake_case`.
    local_decls: &'a HashSet<String>,
    /// Detected calls: (fn_name, line).
    calls: Vec<(String, usize)>,
}

impl<'a> FfiCallVisitor<'a> {
    fn new(ffi_dict: &'a HashSet<String>, local_decls: &'a HashSet<String>) -> Self {
        Self {
            ffi_dict,
            imported_ffi: HashMap::new(),
            glob_imported: false,
            local_decls,
            calls: Vec::new(),
        }
    }

    /// Check if a `use` tree imports from `ffi` and collect the imported names.
    fn collect_ffi_imports(&mut self, prefix_is_ffi: bool, tree: &syn::UseTree) {
        match tree {
            syn::UseTree::Path(use_path) => {
                let is_ffi = use_path.ident == "ffi";
                if is_ffi || prefix_is_ffi {
                    self.collect_ffi_imports(true, &use_path.tree);
                }
            }
            syn::UseTree::Name(use_name) => {
                if prefix_is_ffi {
                    let name = use_name.ident.to_string();
                    if self.ffi_dict.contains(&name) {
                        self.imported_ffi.insert(name.clone(), name);
                    }
                }
            }
            syn::UseTree::Rename(use_rename) => {
                if prefix_is_ffi {
                    let original = use_rename.ident.to_string();
                    if self.ffi_dict.contains(&original) {
                        // Map the alias to the original FFI symbol name so calls
                        // through the alias are attributed to the correct symbol.
                        let alias = use_rename.rename.to_string();
                        self.imported_ffi.insert(alias, original);
                    }
                }
            }
            syn::UseTree::Glob(_) => {
                if prefix_is_ffi {
                    self.glob_imported = true;
                }
            }
            syn::UseTree::Group(use_group) => {
                for item in &use_group.items {
                    self.collect_ffi_imports(prefix_is_ffi, item);
                }
            }
        }
    }

    /// Extract the function name from a call expression if it's an FFI call.
    fn check_call_expr(&self, func: &syn::Expr) -> Option<String> {
        match func {
            // Qualified path: `ffi::SomeFunction(...)`
            syn::Expr::Path(expr_path) => {
                let segments = &expr_path.path.segments;
                if segments.len() >= 2 {
                    let first = segments[0].ident.to_string();
                    if first == "ffi" {
                        let last = segments.last().unwrap().ident.to_string();
                        if self.ffi_dict.contains(&last) {
                            return Some(last);
                        }
                    }
                } else if segments.len() == 1 {
                    // Bare identifier: check if it was imported from ffi.
                    let name = segments[0].ident.to_string();
                    if self.glob_imported && self.ffi_dict.contains(&name) {
                        return Some(name);
                    }
                    // Resolve alias to the original FFI symbol name.
                    if let Some(original) = self.imported_ffi.get(&name) {
                        return Some(original.clone());
                    }
                    // Check if it was declared locally via `unsafe extern "C" { ... }`.
                    if self.local_decls.contains(&name) {
                        return Some(name);
                    }
                }
                None
            }
            _ => None,
        }
    }
}

impl<'ast, 'a> Visit<'ast> for FfiCallVisitor<'a> {
    fn visit_item_mod(&mut self, node: &'ast syn::ItemMod) {
        // Skip `#[cfg(test)]` modules.
        if is_cfg_test(&node.attrs) {
            return;
        }
        // Each inline module has its own scope — imports and local declarations
        // from one module must not leak into sibling modules. Save and restore.
        let saved_imported = std::mem::take(&mut self.imported_ffi);
        let saved_glob = std::mem::replace(&mut self.glob_imported, false);
        syn::visit::visit_item_mod(self, node);
        self.imported_ffi = saved_imported;
        self.glob_imported = saved_glob;
    }

    fn visit_item_fn(&mut self, node: &'ast syn::ItemFn) {
        // Skip `#[test]` functions.
        if node.attrs.iter().any(|a| a.path().is_ident("test")) {
            return;
        }
        // Skip `#[cfg(test)]` functions.
        if is_cfg_test(&node.attrs) {
            return;
        }
        syn::visit::visit_item_fn(self, node);
    }

    fn visit_item_use(&mut self, node: &'ast syn::ItemUse) {
        self.collect_ffi_imports(false, &node.tree);
        syn::visit::visit_item_use(self, node);
    }

    fn visit_expr_call(&mut self, node: &'ast syn::ExprCall) {
        if let Some(name) = self.check_call_expr(&node.func) {
            let line = node.func.span().start().line;
            self.calls.push((name, line));
        }
        syn::visit::visit_expr_call(self, node);
    }
}

/// Stats for a single crate.
struct CrateStats {
    name: String,
    version: String,
    /// Maps FFI function name -> call count within this crate.
    symbol_counts: BTreeMap<String, u64>,
}

impl CrateStats {
    fn ffi_calls(&self) -> u64 {
        self.symbol_counts.values().sum()
    }

    fn unique_symbols(&self) -> usize {
        self.symbol_counts.len()
    }
}

/// Analyze all `.rs` files in a crate's `src/` directory for FFI call sites.
fn analyze_crate(
    src_dir: &Path,
    ffi_dict: &HashSet<String>,
    local_decls: &HashSet<String>,
) -> BTreeMap<String, u64> {
    let mut symbol_counts: BTreeMap<String, u64> = BTreeMap::new();

    for entry in WalkDir::new(src_dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| {
            // Skip tests/ directories.
            !e.path().components().any(|c| c.as_os_str() == "tests")
        })
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "rs"))
    {
        let path = entry.path();
        let Ok(content) = std::fs::read_to_string(path) else {
            continue;
        };

        match syn::parse_file(&content) {
            Ok(file) => {
                let mut visitor = FfiCallVisitor::new(ffi_dict, local_decls);
                visitor.visit_file(&file);
                for (name, _line) in visitor.calls {
                    *symbol_counts.entry(name).or_insert(0) += 1;
                }
            }
            Err(e) => {
                eprintln!("Warning: failed to parse {}: {}", path.display(), e);
            }
        }
    }

    symbol_counts
}

fn main() -> Result<()> {
    let manifest_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "Cargo.toml".to_string());

    let manifest_path = Path::new(&manifest_path);
    if !manifest_path.exists() {
        bail!("Cargo.toml not found at: {}", manifest_path.display());
    }

    // Get workspace metadata.
    let metadata = MetadataCommand::new()
        .manifest_path(manifest_path)
        .exec()
        .context("Failed to get cargo metadata")?;

    // Step 1: Build FFI function dictionary from bindings.rs.
    let target_dir = metadata.target_directory.as_std_path();
    let ffi_dict = build_ffi_dictionary(target_dir)?;

    // Step 2: Enumerate crates to scan.
    let workspace_packages: Vec<_> = metadata
        .workspace_packages()
        .into_iter()
        .filter(|p| {
            // Exclude crates under c_entrypoint/.
            if p.manifest_path
                .components()
                .any(|c| c.as_str() == "c_entrypoint")
            {
                return false;
            }
            // Exclude crates under tools/.
            if p.manifest_path.components().any(|c| c.as_str() == "tools") {
                return false;
            }
            // Exclude bencher crates.
            if p.name.ends_with("_bencher") {
                return false;
            }
            // Exclude test_utils crates.
            if p.name.ends_with("_test_utils") {
                return false;
            }
            true
        })
        .collect();

    // Step 2b: Collect locally-declared extern "C" symbols per crate and augment the
    // global FFI dictionary so that qualified-path calls (`ffi::Foo`) in *other*
    // crates can also match these symbols.
    let mut local_decls_per_crate: HashMap<String, HashSet<String>> = HashMap::new();
    for pkg in &workspace_packages {
        let src_dir = pkg.manifest_path.parent().unwrap().join("src");
        let local_symbols = collect_local_ffi_declarations(src_dir.as_std_path());
        local_decls_per_crate.insert(pkg.name.clone(), local_symbols);
    }

    eprintln!(
        "Scanning {} workspace crates (after filtering)",
        workspace_packages.len()
    );

    // Step 3: Parse and scan source files.
    let mut all_stats: Vec<CrateStats> = Vec::new();

    let empty = HashSet::new();
    for pkg in workspace_packages {
        let src_dir = pkg.manifest_path.parent().unwrap().join("src");
        let local_decls = local_decls_per_crate.get(&pkg.name).unwrap_or(&empty);
        let symbol_counts = analyze_crate(src_dir.as_std_path(), &ffi_dict, local_decls);

        if !symbol_counts.is_empty() {
            all_stats.push(CrateStats {
                name: pkg.name.clone(),
                version: pkg.version.to_string(),
                symbol_counts,
            });
        }
    }

    // Step 4: Generate report.
    // Sort by FFI call count descending.
    all_stats.sort_by_key(|s| Reverse(s.ffi_calls()));

    let crate_count = all_stats.len();
    println!();
    println!("## FFI Function Invocations ({crate_count} crates)");

    let mut total_calls: u64 = 0;
    let mut total_unique: BTreeMap<String, u64> = BTreeMap::new();

    for s in &all_stats {
        total_calls += s.ffi_calls();
        for (name, count) in &s.symbol_counts {
            *total_unique.entry(name.clone()).or_insert(0) += count;
        }

        // Sort symbols by count descending, then name ascending.
        let mut symbols: Vec<_> = s.symbol_counts.iter().collect();
        symbols.sort_by(|a, b| b.1.cmp(a.1).then(a.0.cmp(b.0)));

        // Find the longest symbol name for column sizing.
        let max_name_len = symbols
            .iter()
            .map(|(name, _)| name.len())
            .max()
            .unwrap_or(6)
            .max(6);

        println!();
        println!(
            "### {} v{} \u{2014} {} calls, {} unique symbols",
            s.name,
            s.version,
            s.ffi_calls(),
            s.unique_symbols()
        );
        println!();
        println!("| {:<max_name_len$} | Calls |", "Symbol");
        println!("|{:-<width$}|-------|", "", width = max_name_len + 2);
        for (name, count) in &symbols {
            println!("| {:<max_name_len$} | {:>5} |", name, count);
        }
    }

    println!();
    println!(
        "**TOTAL: {} calls, {} unique symbols across {} crates**",
        total_calls,
        total_unique.len(),
        crate_count
    );

    Ok(())
}
