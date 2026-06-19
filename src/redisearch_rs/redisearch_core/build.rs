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
    let bin_dir = std::env::var("BINDIR").map(std::path::PathBuf::from).unwrap_or_else(|_| {
        let root = build_utils::repository_root()
            .expect("Could not find git root for static library linking");
        // From deps/RediSearch up to the superrepo, then into build/deps/RediSearch.
        root.join("..").join("..").join("build").join("deps").join("RediSearch")
    });
    // Superrepo build dir (parent-of-parent of bin_dir's `deps/RediSearch`).
    let superrepo_build = bin_dir
        .parent()
        .and_then(|p| p.parent())
        .map(std::path::Path::to_path_buf)
        .unwrap_or_else(|| std::path::PathBuf::from("build"));

    let linting_only = std::env::var("LINTING_ONLY").as_deref() == Ok("1");

    // C-only combined RediSearch archive (whole-archive pull at final link).
    let redisearch_all = match build_utils::link_redisearch_all(&bin_dir) {
        Ok(path) => Some(path),
        Err(e) if linting_only => {
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
    // here we (a) emit the list of symbols it must inject, and (b) force each to be
    // pulled from the C archive and kept, so the name actually exists in the link.
    //
    // These symbols are genuinely shared: the plugin and core MUST operate on the
    // SAME `IndexSpec` / `DocTable` / config instances, so they are exported from
    // the single core copy rather than duplicated into the plugin.
    println!("cargo::rustc-link-arg=-Wl,--export-dynamic");

    const PLUGIN_EXPORTED_CORE_SYMBOLS: &[&str] = &[
        // RediSearch C-core functions/globals the disk layer (redisearch_disk +
        // vecsim_disk) references across the .so boundary, plus the C-core
        // functions the shared dylib (`libsearch_shared.so`) references back into
        // the core. Both consumers resolve these from `redisearch.so` at load time:
        // the plugin via `RTLD_GLOBAL` at `dlopen`, the shared dylib via its
        // `NEEDED redisearch.so`-equivalent load order (it is a `NEEDED` of
        // `redisearch.so`, so the core's `.dynsym` must satisfy its undefined
        // references). Every name here is otherwise localized by `rustc`'s cdylib
        // `local: *` version script; the linker wrapper promotes exactly this set.
        "AREQ_CheckTimedOut",
        "Buffer_Grow",
        "DMD_Free",
        "DocTable_Exists",
        "HiddenString_CompareC",
        "HiddenString_GetUnsafe",
        "HybridIterator_GetChild",
        "HybridIterator_GetMaxBatchIteration",
        "HybridIterator_GetMaxBatchSize",
        "HybridIterator_GetNumIterations",
        "HybridIterator_GetSearchModeString",
        "HybridIterator_IsBatchMode",
        "IndexSpecCache_Decref",
        "Obfuscate_Number",
        "Obfuscate_Text",
        "OptimizerIterator_GetChild",
        "OptimizerIterator_GetOptimizationType",
        "RPProfile_IncrementCount",
        "RSDummyContext",
        "RSGlobalConfig",
        "Redis_OpenInvertedIndex",
        "SEDestroy",
        "isWithinRadius",
        "loadIndividualKeys",
        "sdscatlen",
        // C-core symbols referenced by `libsearch_shared.so` (the shared Rust
        // subgraph: rqe_iterators / index_spec / ffi / ...). These are undefined
        // (`U`) in the shared dylib and defined in the C core, so they must be
        // exported from `redisearch.so` for the shared dylib's references to bind
        // at module load. Without these, RoR module load fails to resolve them.
        "IndexSpecRef_Promote",
        "IndexSpecRef_Release",
        "IndexSpec_AcquireWriteLock",
        "IndexSpec_DecrementNumTerms",
        "IndexSpec_DecrementTrieTermCount",
        "IndexSpec_ReleaseWriteLock",
        "NewNumericFilter",
        "RS_dictFetchValue",
        "StrongRef_Get",
        "TimeToLiveTable_VerifyDocAndField",
        "TimeToLiveTable_VerifyDocAndFieldMask",
        "TimeToLiveTable_VerifyDocAndWideFieldMask",
        "array_len_func",
    ];

    // Pull each symbol from the archive and keep it through GC, so the name is
    // present in the link for the version-script `global:` injection to promote.
    if redisearch_all.is_some() {
        for sym in PLUGIN_EXPORTED_CORE_SYMBOLS {
            println!("cargo::rustc-link-arg=-Wl,--undefined={sym}");
        }
    }

    // Hand the symbol list to the linker wrapper via a file. The wrapper injects
    // these names into `rustc`'s version-script `global:` section when it links
    // `libredisearch_core.so`. `REDISEARCH_PLUGIN_EXPORTS_FILE` is set by the
    // superrepo `build.sh`; when unset (e.g. a normal in-submodule `cargo build`)
    // we skip — the C-only static link does not need the wrapper.
    if let Ok(exports_file) = std::env::var("REDISEARCH_PLUGIN_EXPORTS_FILE") {
        let body = PLUGIN_EXPORTED_CORE_SYMBOLS.join("\n");
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
