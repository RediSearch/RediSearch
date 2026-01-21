/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use anyhow::{Context, Result, bail};
use cargo_metadata::MetadataCommand;
use std::path::Path;
use syn::visit::Visit;
use walkdir::WalkDir;

/// Counts unsafe usage and lines of code in Rust code
#[derive(Default, Debug)]
struct UnsafeVisitor {
    /// number of unsafe fn declarations
    unsafe_fns: u64,
    /// lines inside unsafe blocks
    unsafe_block_lines: u64,
    /// unsafe impl declarations (count)
    unsafe_impls: u64,
    /// unsafe trait declarations (count)
    unsafe_traits: u64,
    /// total lines of code (excluding test code)
    lines: u64,
}

/// Check if attributes contain #[cfg(test)]
fn is_cfg_test(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|attr| {
        if !attr.path().is_ident("cfg") {
            return false;
        }
        // Parse the cfg attribute to check for "test"
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("test") {
                // Found #[cfg(test)]
                Err(syn::Error::new_spanned(&meta.path, "found"))
            } else {
                Ok(())
            }
        })
        .is_err()
    })
}

/// Count the lines spanned by a syntax node
fn span_lines(span: proc_macro2::Span) -> u64 {
    let start = span.start().line;
    let end = span.end().line;
    (end - start + 1) as u64
}

impl<'ast> Visit<'ast> for UnsafeVisitor {
    fn visit_item_fn(&mut self, node: &'ast syn::ItemFn) {
        // Skip #[test] functions
        if node.attrs.iter().any(|a| a.path().is_ident("test")) {
            return;
        }
        // Skip #[cfg(test)] functions
        if is_cfg_test(&node.attrs) {
            return;
        }

        let fn_lines = span_lines(node.block.brace_token.span.join());
        self.lines += fn_lines;

        if node.sig.unsafety.is_some() {
            self.unsafe_fns += 1;
        }
        syn::visit::visit_item_fn(self, node);
    }

    fn visit_item_mod(&mut self, node: &'ast syn::ItemMod) {
        // Skip #[cfg(test)] modules entirely
        if is_cfg_test(&node.attrs) {
            return;
        }
        syn::visit::visit_item_mod(self, node);
    }

    fn visit_impl_item_fn(&mut self, node: &'ast syn::ImplItemFn) {
        let fn_lines = span_lines(node.block.brace_token.span.join());
        self.lines += fn_lines;

        if node.sig.unsafety.is_some() {
            self.unsafe_fns += 1;
        }
        syn::visit::visit_impl_item_fn(self, node);
    }

    fn visit_trait_item_fn(&mut self, node: &'ast syn::TraitItemFn) {
        if let Some(block) = &node.default {
            let fn_lines = span_lines(block.brace_token.span.join());
            self.lines += fn_lines;
        }
        if node.sig.unsafety.is_some() {
            self.unsafe_fns += 1;
        }
        syn::visit::visit_trait_item_fn(self, node);
    }

    fn visit_expr_unsafe(&mut self, node: &'ast syn::ExprUnsafe) {
        self.unsafe_block_lines += span_lines(node.block.brace_token.span.join());
        syn::visit::visit_expr_unsafe(self, node);
    }

    fn visit_item_impl(&mut self, node: &'ast syn::ItemImpl) {
        // Skip #[cfg(test)] impl blocks
        if is_cfg_test(&node.attrs) {
            return;
        }
        if node.unsafety.is_some() {
            self.unsafe_impls += 1;
        }
        syn::visit::visit_item_impl(self, node);
    }

    fn visit_item_trait(&mut self, node: &'ast syn::ItemTrait) {
        if node.unsafety.is_some() {
            self.unsafe_traits += 1;
        }
        syn::visit::visit_item_trait(self, node);
    }

    fn visit_item_struct(&mut self, node: &'ast syn::ItemStruct) {
        if !is_cfg_test(&node.attrs) {
            self.lines += span_lines(node.ident.span());
        }
        syn::visit::visit_item_struct(self, node);
    }

    fn visit_item_enum(&mut self, node: &'ast syn::ItemEnum) {
        if !is_cfg_test(&node.attrs) {
            self.lines += span_lines(node.brace_token.span.join());
        }
        syn::visit::visit_item_enum(self, node);
    }

    fn visit_item_const(&mut self, node: &'ast syn::ItemConst) {
        if !is_cfg_test(&node.attrs) {
            self.lines += 1;
        }
        syn::visit::visit_item_const(self, node);
    }

    fn visit_item_static(&mut self, node: &'ast syn::ItemStatic) {
        if !is_cfg_test(&node.attrs) {
            self.lines += 1;
        }
        syn::visit::visit_item_static(self, node);
    }

    fn visit_item_type(&mut self, node: &'ast syn::ItemType) {
        if !is_cfg_test(&node.attrs) {
            self.lines += 1;
        }
        syn::visit::visit_item_type(self, node);
    }
}

/// Stats for a single crate
#[derive(Debug)]
struct CrateStats {
    name: String,
    version: String,
    /// Total lines of code
    lines: u64,
    /// Number of unsafe fn declarations
    unsafe_fns: u64,
    /// Lines inside unsafe blocks
    unsafe_block_lines: u64,
    /// Number of unsafe impl declarations
    unsafe_impls: u64,
    /// Number of unsafe trait declarations
    unsafe_traits: u64,
}

impl CrateStats {
    fn unsafe_ratio(&self) -> f64 {
        if self.lines == 0 {
            0.0
        } else {
            (self.unsafe_block_lines as f64) / (self.lines as f64) * 100.0
        }
    }
}

fn main() -> Result<()> {
    let manifest_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "Cargo.toml".to_string());

    let manifest_path = Path::new(&manifest_path);
    if !manifest_path.exists() {
        bail!("Cargo.toml not found at: {}", manifest_path.display());
    }

    // Get workspace metadata
    let metadata = MetadataCommand::new()
        .manifest_path(manifest_path)
        .exec()
        .context("Failed to get cargo metadata")?;

    let workspace_packages: Vec<_> = metadata
        .workspace_packages()
        .into_iter()
        .filter(|p| {
            // Exclude crates ending in _bencher
            if p.name.ends_with("_bencher") {
                return false;
            }
            // Exclude redis_mock
            if p.name == "redis_mock" {
                return false;
            }
            // Exclude crates under tools/
            if p.manifest_path.components().any(|c| c.as_str() == "tools") {
                return false;
            }
            true
        })
        .collect();
    eprintln!(
        "Found {} workspace crates (after filtering)",
        workspace_packages.len()
    );

    let mut ffi_stats: Vec<CrateStats> = Vec::new();
    let mut other_stats: Vec<CrateStats> = Vec::new();

    for pkg in workspace_packages {
        // Only analyze src/ directory, not tests/
        let src_dir = pkg.manifest_path.parent().unwrap().join("src");
        let visitor = analyze_directory(src_dir.as_std_path());

        let stats = CrateStats {
            name: pkg.name.clone(),
            version: pkg.version.to_string(),
            lines: visitor.lines,
            unsafe_fns: visitor.unsafe_fns,
            unsafe_block_lines: visitor.unsafe_block_lines,
            unsafe_impls: visitor.unsafe_impls,
            unsafe_traits: visitor.unsafe_traits,
        };

        // Classify as FFI or other
        let is_ffi =
            pkg.name.contains("ffi") || pkg.name == "buffer" || pkg.name == "rqe_iterators_interop";

        if is_ffi {
            ffi_stats.push(stats);
        } else {
            other_stats.push(stats);
        }
    }

    // Sort both by unsafe ratio descending
    ffi_stats.sort_by(|a, b| {
        b.unsafe_ratio()
            .partial_cmp(&a.unsafe_ratio())
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    other_stats.sort_by(|a, b| {
        b.unsafe_ratio()
            .partial_cmp(&a.unsafe_ratio())
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    // Print FFI report
    println!();
    println!("## FFI Crates ({})", ffi_stats.len());
    print_report(&ffi_stats);

    // Print other crates report
    println!();
    println!("## Core Crates ({})", other_stats.len());
    print_report(&other_stats);

    Ok(())
}

fn print_report(stats: &[CrateStats]) {
    println!();
    println!(
        "| {:<38} | {:>8} | {:>11} | {:>18} | {:>12} | {:>13} | {:>10} |",
        "Crate",
        "Lines",
        "Unsafe Fn",
        "Unsafe Block Lines",
        "Unsafe Impls",
        "Unsafe Traits",
        "Ratio (%)"
    );
    println!(
        "|{:-<40}|{:->10}|{:->13}|{:->20}|{:->14}|{:->15}|{:->12}|",
        "", "", "", "", "", "", ""
    );

    let mut total_lines = 0u64;

    for s in stats {
        total_lines += s.lines;

        println!(
            "| {:<38} | {:>8} | {:>11} | {:>18} | {:>12} | {:>13} | {:>10.4} |",
            format!("{} v{}", s.name, s.version),
            s.lines,
            s.unsafe_fns,
            s.unsafe_block_lines,
            s.unsafe_impls,
            s.unsafe_traits,
            s.unsafe_ratio()
        );
    }

    let overall_ratio = if total_lines > 0 {
        (stats.iter().map(|s| s.unsafe_block_lines).sum::<u64>() as f64) / (total_lines as f64)
            * 100.0
    } else {
        0.0
    };
    println!(
        "| {:<38} | {:>8} | {:>11} | {:>18} | {:>12} | {:>13} | {:>10.4} |",
        "**TOTAL**",
        total_lines,
        stats.iter().map(|s| s.unsafe_fns).sum::<u64>(),
        stats.iter().map(|s| s.unsafe_block_lines).sum::<u64>(),
        stats.iter().map(|s| s.unsafe_impls).sum::<u64>(),
        stats.iter().map(|s| s.unsafe_traits).sum::<u64>(),
        overall_ratio
    );
}

/// Analyze all .rs files in a directory, returning unsafe counts (including lines)
/// Excludes tests/ directory and files under it
fn analyze_directory(dir: &Path) -> UnsafeVisitor {
    let mut visitor = UnsafeVisitor::default();

    for entry in WalkDir::new(dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| {
            // Skip tests directories
            !e.path().components().any(|c| c.as_os_str() == "tests")
        })
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "rs"))
    {
        let path = entry.path();
        if let Ok(content) = std::fs::read_to_string(path) {
            // Parse and visit
            match syn::parse_file(&content) {
                Ok(file) => visitor.visit_file(&file),
                Err(e) => {
                    eprintln!("Warning: failed to parse {}: {}", path.display(), e);
                }
            }
        }
    }

    visitor
}
