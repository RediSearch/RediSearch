/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Link the C search core into `redisearch.so`.
//!
//! `rustc` is the final linker for this cdylib, so we attach the C-only combined
//! archive (`libredisearch_all.a`), MKL, and OpenSSL here. The Rust `*_ffi`
//! symbols this archive calls are provided by the `redisearch_rs` c_entrypoint
//! dependency; the shared subgraph + libstd come dynamically from
//! `libsearch_shared.so`. NO speedb / vecsim — those live in `redisearch_disk.so`.

fn main() {
    // This crate lives inside the `deps/RediSearch` submodule, so its git root is
    // NOT the superrepo. The build harness passes BINDIR pointing at the CMake
    // output dir (`<superrepo>/build/deps/RediSearch`); fall back to deriving it
    // from the superrepo root only as a last resort. MKL lives at
    // `<superrepo>/build/_deps/svs-src/lib`, so we also need the superrepo build dir.
    let bin_dir = std::env::var("BINDIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| {
            let root = build_utils::repository_root()
                .expect("Could not find git root for static library linking");
            // From deps/RediSearch up to the superrepo, then into build/deps/RediSearch.
            root.join("..")
                .join("..")
                .join("build")
                .join("deps")
                .join("RediSearch")
        });
    // Superrepo build dir (parent-of-parent of bin_dir's `deps/RediSearch`).
    let superrepo_build = bin_dir
        .parent()
        .and_then(|p| p.parent())
        .map(std::path::Path::to_path_buf)
        .unwrap_or_else(|| std::path::PathBuf::from("build"));

    let linting_only = std::env::var("LINTING_ONLY").as_deref() == Ok("1");
    let plugin_exports_file = std::env::var("REDISEARCH_PLUGIN_EXPORTS_FILE").ok();

    // C-only combined RediSearch archive (linked `static:-bundle`; the linker pulls
    // only referenced objects, plus any forced via `--undefined` below).
    let redisearch_all = match build_utils::link_redisearch_all(&bin_dir) {
        Ok(path) => Some(path),
        Err(e) if linting_only || plugin_exports_file.is_none() => {
            println!("cargo::warning={e}");
            None
        }
        Err(e) => panic!("{e}"),
    };

    // MKL is excluded from libredisearch_all.a; link it separately (SVS dep).
    build_utils::link_mkl(&superrepo_build.join("_deps/svs-src/lib"));

    // OpenSSL, used by the coordinator (rmr) inside the C core.
    println!("cargo::rustc-link-lib=dylib=ssl");
    println!("cargo::rustc-link-lib=dylib=crypto");

    // The C core embeds C++ (VectorSimilarity, geometry), so it needs the C++
    // runtime. A Rust cdylib does not pull libstdc++ automatically.
    println!("cargo::rustc-link-lib=dylib=stdc++");

    // The module entry `RedisModule_OnLoad` is provided by the Rust shim in
    // `module_entry.rs` (a `#[no_mangle]` symbol that the cdylib version script
    // DOES export); it forwards to the C core's `RediSearch_OnLoad_Impl`. Force
    // that C implementation to be pulled from the archive (nothing else calls it).
    println!("cargo::rustc-link-arg=-Wl,--undefined=RediSearch_OnLoad_Impl");

    // On RoF the dlopened `redisearch_disk.so` resolves C-core symbols from this
    // module via `RTLD_GLOBAL`, so the C-core globals must live in `redisearch.so`'s
    // `.dynsym` (matching what the legacy C-linked `redisearch.so` exposed).
    //
    // This is harder than it looks. `rustc` emits an anonymous cdylib version script
    // `{ global: <#[no_mangle] Rust syms>; local: *; }`. The `local: *` localizes
    // every C-core / VecSim symbol, and empirically NEITHER `--export-dynamic`,
    // `--export-dynamic-symbol`, `--dynamic-list` NOR `-u` can promote a symbol that
    // a version script already marked local (verified with GNU ld 2.38 and LLD 21);
    // and a second `--version-script` cannot be combined with `rustc`'s anonymous
    // one ("anonymous version tag cannot be combined with other version tags").
    //
    // The robust mechanism is to inject the needed names into the `global:` section
    // of `rustc`'s own version script at link time. A thin linker wrapper
    // (`redisearch_core_linker.sh`, wired in by the superrepo `build.sh`) does this;
    // here we (a) emit the list of symbols it must inject, and (b) ensure each is
    // pulled from the C archive and kept, so the name actually exists in the link.
    //
    // The export set is NOT hand-maintained. It is derived at build time from the
    // C-only combined archive (`libredisearch_all.a`) itself: every globally-defined
    // C-core symbol is exported. This way, any future disk/plugin or shared-dylib
    // code that calls a new C-core function resolves at dlopen with ZERO linker-config
    // edits — the symbol is already in the archive, so it is already exported. The
    // alternative (a hand-enumerated list) silently broke `dlopen` whenever a new
    // cross-`.so` call was added without updating the list.
    //
    // These symbols are genuinely shared: the plugin and core MUST operate on the
    // SAME `IndexSpec` / `DocTable` / config instances, so they are exported from
    // the single core copy rather than duplicated into the plugin.
    println!("cargo::rustc-link-arg=-Wl,--export-dynamic");

    // Derive the full set of C-core globals to export from the archive via `nm`.
    let plugin_exported_core_symbols: Vec<String> = match &redisearch_all {
        Some(archive) => {
            println!("cargo::rerun-if-changed={}", archive.display());
            collect_c_core_globals(archive)
        }
        None => Vec::new(),
    };

    // Force every exported global to be pulled from the archive and kept through GC,
    // so the name is present in the link for the version-script `global:` injection
    // to promote. The archive is linked with `static:-bundle`, so the linker only
    // pulls objects it sees a reference to; `--undefined` provides that reference for
    // symbols the C core does not itself call (e.g. utility helpers only the plugin
    // uses), making the blanket export complete and future-proof.
    for sym in &plugin_exported_core_symbols {
        println!("cargo::rustc-link-arg=-Wl,--undefined={sym}");
    }

    // Hand the symbol list to the linker wrapper via a file. The wrapper injects
    // these names into `rustc`'s version-script `global:` section when it links
    // `libredisearch_core.so`. `REDISEARCH_PLUGIN_EXPORTS_FILE` is set by the
    // superrepo `build.sh`; when unset (e.g. a normal in-submodule `cargo build`)
    // we skip — the C-only static link does not need the wrapper.
    if let Some(exports_file) = plugin_exports_file {
        let body = plugin_exported_core_symbols.join("\n");
        if let Err(e) = std::fs::write(&exports_file, format!("{body}\n")) {
            panic!("failed to write plugin exports file {exports_file}: {e}");
        }
        println!("cargo::rerun-if-env-changed=REDISEARCH_PLUGIN_EXPORTS_FILE");
    }

    if redisearch_all.is_some() {
        // Link the C archive last so its symbols are preferred.
        println!("cargo::rustc-link-arg=-lredisearch_all");
    }
}

/// Extract every globally-defined C-core symbol from `libredisearch_all.a`.
///
/// Runs `nm -g --defined-only` on the combined C archive and keeps the **strong**
/// defined globals — text (`T`), initialized/uninitialized data (`D`/`B`), and
/// read-only data (`R`). These are the C-core API surface (functions and globals)
/// that the dlopened plugin (`redisearch_disk.so`) and the shared dylib
/// (`libsearch_shared.so`) resolve across the `.so` boundary.
///
/// Weak symbols (`W`/`V`) are deliberately excluded: in this archive they are
/// overwhelmingly C++ template instantiations, inline functions, and vtables from
/// the embedded C++ (VectorSimilarity, geometry). They are already merged at link
/// time, are not part of the stable C-core ABI the plugin links against, and
/// blanket-exporting ~75k of them would bloat the version script and risk ODR /
/// interposition surprises across the boundary for no benefit. Every symbol the
/// consumers actually need is a strong global.
fn collect_c_core_globals(archive: &std::path::Path) -> Vec<String> {
    let nm = std::env::var("NM").unwrap_or_else(|_| "nm".to_string());
    let output = std::process::Command::new(&nm)
        .args(["-g", "--defined-only"])
        .arg(archive)
        .output()
        .unwrap_or_else(|e| panic!("failed to run `{nm}` on {}: {e}", archive.display()));
    if !output.status.success() {
        panic!(
            "`{nm} -g --defined-only {}` failed: {}",
            archive.display(),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let mut symbols: Vec<String> = String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| {
            // `nm` output for a defined global: "<addr> <type> <name>". Keep only
            // strong defined types; skip undefined (`U`) and weak (`W`/`V`/`w`/`v`).
            let mut fields = line.split_whitespace();
            let _addr = fields.next()?;
            let kind = fields.next()?;
            let name = fields.next()?;
            matches!(kind, "T" | "D" | "B" | "R").then(|| name.to_string())
        })
        .collect();
    symbols.sort_unstable();
    symbols.dedup();

    assert!(
        !symbols.is_empty(),
        "no strong C-core globals found in {}; refusing to produce an empty export set",
        archive.display()
    );
    symbols
}
