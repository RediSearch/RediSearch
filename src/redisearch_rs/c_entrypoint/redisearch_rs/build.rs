/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::collections::HashSet;
use std::fs;
use std::path::Path;
use syn::{Abi, ItemFn, Visibility, visit::Visit};
use walkdir::WalkDir;

/// This build script ensures that all `extern "C"` functions defined in our `*_ffi` crates
/// are included in the final test and benchmark binaries, even when they're not directly
/// called from Rust code.
///
/// # The Problem
///
/// Our `*_ffi` crates define `#[unsafe(no_mangle)] pub extern "C" fn` functions that are meant to be
/// called by C code. From Rust's perspective, these functions are never used—no Rust code
/// calls them. This creates two issues:
///
/// 1. **LLVM dead code elimination**: The compiler may remove "unused" functions entirely.
/// 2. **Linker garbage collection**: Even if the functions survive compilation, the linker
///    (with `-dead_strip` on macOS or `--gc-sections` on Linux) will remove symbols that
///    nothing references.
///
/// In production, this isn't an issue. We compile `redisearch_rs` as a `staticlib`, which
/// preserves all symbols unconditionally.
///
/// We have issues when it comes to tests and benchmarks.
/// Some of our tests and benchmarks need to invoke C-defined symbols, which are provided by
/// the `redisearch_all` static library. Those C-defined symbols may in turn call back into Rust-defined FFI
/// symbols. `cargo` isn't able to see this relationship: `redisearch_rs` is consumed
/// as a regular Rust dependency (an `rlib`) by our tests and benchmarks, and the FFI symbols it
/// defines are stripped out as unused. This in turn causes the `redisearch_all` static library to fail linking,
/// since the Rust-provided symbols it needs are missing.
///
/// # Obvious Solutions That Don't Work
///
/// - **`extern crate`**: Adding `extern crate fnv_ffi;` ensures the crate is linked, but
///   doesn't prevent the linker from stripping unused symbols within that crate.
///
/// - **`#[used]` on functions**: This would be the ideal solution, but `#[used]` is only
///   stable for `static` items. Using it on functions requires the unstable
///   `#![feature(used_linker)]` ([Tracking issue](https://github.com/rust-lang/rust/issues/93798)).
///
/// # The Solution
///
/// We use a multi-pronged approach:
///
/// 1. **Parse the `*_ffi` crates** to discover all `#[no_mangle] pub extern "C" fn` symbols.
///    We only scan crates listed as dependencies in this crate's `Cargo.toml` to avoid
///    pulling in symbols from crates that are not yet integrated in the C project.
///
/// 2. **Generate a `#[used]` static array** containing pointers to all FFI functions.
///    The `#[used]` attribute prevents the linker from eliminating the static, and since it
///    references all our FFI functions, they're kept alive through compilation.
///
/// This ensures that when the C static library calls these functions, the symbols are
/// present in the final test/benchmark binary.
fn main() {
    println!("cargo:rerun-if-changed=Cargo.toml");
    let manifest_path = Path::new("Cargo.toml");
    let manifest_content = fs::read_to_string(manifest_path).expect("Failed to read Cargo.toml");
    let manifest: toml::Table = manifest_content
        .parse()
        .expect("Failed to parse Cargo.toml");

    // Extract *_ffi dependencies from [dependencies]
    let ffi_crates: Vec<_> = manifest
        .get("dependencies")
        .and_then(|d| d.as_table())
        .map(|deps| {
            deps.keys()
                .filter(|name| name.ends_with("_ffi"))
                .filter_map(|name| {
                    deps.get(name)
                        .and_then(|v| v.as_table())
                        .and_then(|t| t.get("path"))
                        .and_then(|p| p.as_str())
                        .map(|path| (name.clone(), Path::new(path).join("src")))
                })
                .collect()
        })
        .unwrap_or_default();
    if ffi_crates.is_empty() {
        panic!("No *_ffi crates found in Cargo.toml dependencies");
    }

    let mut symbols: Vec<(String, String)> = Vec::new();

    for (_, crate_path) in &ffi_crates {
        println!("cargo:rerun-if-changed={}", crate_path.display());

        for entry in WalkDir::new(crate_path)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "rs"))
        {
            let path = entry.path();

            let content = fs::read_to_string(path).unwrap();
            if let Ok(file) = syn::parse_file(&content) {
                let mut visitor = FfiVisitor {
                    source_path: path.to_string_lossy().into_owned(),
                    symbols: &mut symbols,
                };
                visitor.visit_file(&file);
            } else {
                eprintln!("Failed to parse file: {}", path.display());
            }
        }
    }

    // Check for duplicates
    let mut seen: HashSet<&str> = HashSet::new();
    for (symbol, location) in &symbols {
        if !seen.insert(symbol.as_str()) {
            let first_location = symbols
                .iter()
                .find(|(s, _)| s == symbol)
                .map(|(_, l)| l.as_str())
                .unwrap();
            panic!(
                "Duplicate FFI symbol `{symbol}`:\n  - first defined in: {first_location}\n  - also defined in: {location}"
            );
        }
    }

    let count = symbols.len();
    let extern_decls = symbols
        .iter()
        .map(|(s, _)| format!("fn {s}();"))
        .collect::<Vec<_>>()
        .join("\n        ");
    let symbol_refs = symbols
        .iter()
        .map(|(s, _)| format!("FfiSymbol({s} as *const ())"))
        .collect::<Vec<_>>()
        .join(",\n        ");

    let generated = format!(
        r#"// Auto-generated by build.rs. Do not edit.

#[allow(unused)]
struct FfiSymbol(*const ());

/// Safety: It's only needed to force the linker to keep symbols around.
unsafe impl Sync for FfiSymbol {{}}

unsafe extern "C" {{
    {extern_decls}
}}

#[used]
#[doc(hidden)]
static FFI_LINK_GUARD: [FfiSymbol; {count}] = [
    {symbol_refs}
];
"#
    );

    let out_dir = std::env::var("OUT_DIR").unwrap();
    let out_path = Path::new(&out_dir).join("link_guard.rs");
    fs::write(&out_path, generated).unwrap();
}

struct FfiVisitor<'a> {
    source_path: String,
    symbols: &'a mut Vec<(String, String)>,
}

impl<'ast> Visit<'ast> for FfiVisitor<'ast> {
    fn visit_item_fn(&mut self, node: &'ast ItemFn) {
        let is_pub = matches!(node.vis, Visibility::Public(_));
        let is_no_mangle = node.attrs.iter().any(|attr| {
            // #[no_mangle]
            attr.path().is_ident("no_mangle") ||
            // #[unsafe(no_mangle)]
            (attr.path().is_ident("unsafe") &&
                attr.parse_args::<syn::Ident>().map(|id| id == "no_mangle").unwrap_or(false))
        });
        let is_extern_c = matches!(
            &node.sig.abi,
            Some(Abi { name: Some(lit), .. }) if lit.value() == "C" || lit.value() == "C-unwind"
        );

        if is_no_mangle && is_extern_c {
            if is_pub {
                self.symbols
                    .push((node.sig.ident.to_string(), self.source_path.clone()));
            } else {
                panic!(
                    "{} is an extern 'C' function that's not public",
                    node.sig.ident
                );
            }
        }

        syn::visit::visit_item_fn(self, node);
    }
}
