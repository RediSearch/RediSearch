//! Link configuration for the disk plugin cdylib `redisearch_disk.so`.
//!
//! `rustc` is the final linker for this cdylib. We must:
//! - Keep the `SearchDiskPlugin_{Has,Get,Set}API` `#[no_mangle]` exports (defined
//!   in the `redisearch_disk` rlib dependency) in the cdylib's dynamic symbol
//!   table. Because nothing in *this* crate references them, the linker would
//!   otherwise garbage-collect them; `-Wl,--undefined=NAME` pins each one.
//! - Export the dynamic symbols so the dlopening core (`RTLD_GLOBAL`) and the
//!   shared dylib can see them.
//! - Link the VecSim archives (vecsim_disk + VectorSimilarity + transitive deps)
//!   INTO this cdylib. The disk vector indexes the plugin owns are implemented in
//!   `vecsim_disk` (C++) on top of `VectorSimilarity`; in `redisearch.so` those
//!   symbols are localized by the cdylib version script (`local: *`) and so cannot
//!   resolve from the core via `RTLD_GLOBAL`. They MUST be linked here, in the
//!   *cdylib* crate: `cargo::rustc-link-arg` directives emitted by the
//!   `redisearch_disk` rlib dependency do NOT propagate to this final link, so the
//!   linking has to happen in this build script.
//!
//! The C-core archive / MKL are intentionally NOT linked (they resolve at `dlopen`
//! time from `redisearch.so` via `RTLD_GLOBAL`); that part is handled by
//! `redisearch_disk`'s own `build.rs`, gated on `REDISEARCH_DISK_PLUGIN=1`.

use std::path::{Path, PathBuf};

fn main() {
    // Pin the plugin entry points so the linker keeps them in `.dynsym`.
    for sym in [
        "SearchDiskPlugin_HasAPI",
        "SearchDiskPlugin_GetAPI",
        "SearchDiskPlugin_SetAPI",
        // Bootstrap entry for this `.so`'s own `redis_module` API table; the core
        // forwarder resolves it via `dlsym` and calls it with the live
        // `RedisModule_GetApi` pointer (see `redisearch_core::disk_forwarder`).
        "SearchDiskPlugin_InitRedisModuleAPI",
    ] {
        println!("cargo::rustc-link-arg=-Wl,--undefined={sym}");
    }

    // Export the dynamic symbol table so the dlopening core and the shared dylib
    // can resolve the plugin's exports at runtime.
    println!("cargo::rustc-link-arg=-Wl,--export-dynamic");

    // AArch64 outline-atomics helper(s). A C++ object in the disk layer (VecSim /
    // SpeedB) emits a reference to `__aarch64_ldadd4_acq_rel`, which lives in the
    // static `libgcc.a` but NOT the shared `libgcc_s.so.1`. By the time that
    // reference is seen, the linker has already finished with libgcc, so the symbol
    // is left undefined and the plugin `dlopen` aborts (`RTLD_NOW`) with
    // `undefined symbol: __aarch64_ldadd4_acq_rel`. Pin the reference and append
    // `-lgcc` so the helper is pulled from `libgcc.a` into this `.so`.
    //
    // These outline-atomics helpers exist ONLY on AArch64; on x86_64 (and other
    // arches) the symbol does not exist, so pinning `--undefined=__aarch64_*` would
    // leave an unsatisfiable reference and fail the link. Gate the pin on the build
    // *target* arch via `CARGO_CFG_TARGET_ARCH` (set by cargo to the value of
    // `cfg!(target_arch)`), so the same source builds on every arch.
    let target_arch = std::env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_default();
    if target_arch == "aarch64" {
        println!("cargo::rustc-link-arg=-Wl,--undefined=__aarch64_ldadd4_acq_rel");
        println!("cargo::rustc-link-arg=-lgcc");
    }

    link_speedb_into_plugin();
    link_vecsim_into_plugin();
}

/// Links SpeedB into the plugin cdylib as a `NEEDED` shared library.
///
/// The disk layer's storage engine is SpeedB (a RocksDB fork) shipped as
/// `libspeedb.so.<major>.<minor>`. Its `rocksdb::*` C++ symbols are referenced
/// from the `redisearch_disk` -> `speedb` (rust-speedb) rlib dependency, but the
/// `cargo:rustc-link-lib=dylib=speedb` that `libspeedb-sys`'s build script emits
/// does NOT carry the link-search path through to *this* final cdylib link (the
/// same rlib-build-script-propagation gap that forces the VecSim archives to be
/// linked here). Left unaddressed, `redisearch_disk.so` would carry ~127 undefined
/// `rocksdb::*` symbols and NO `DT_NEEDED libspeedb`, relying on libspeedb having
/// been preloaded into the global scope. Emitting the link here gives the plugin a
/// real `DT_NEEDED libspeedb.so.*` (the library's SONAME), resolved at `dlopen`
/// (on RoF) via the `$ORIGIN`/speedb rpath that `build.sh` stamps onto the staged
/// artifact.
///
/// `redisearch.so` (the core module) does NOT link this — it has no disk code —
/// so it remains free of any `libspeedb` dependency (the RoR requirement).
///
/// `build.sh` exports `SPEEDB_LIB_DIR` to the directory containing `libspeedb.so`.
/// When it is unset (e.g. a plain in-submodule `cargo build`/`clippy` with no
/// superrepo) we skip — the plugin is only ever produced by the superrepo build.
fn link_speedb_into_plugin() {
    println!("cargo::rerun-if-env-changed=SPEEDB_LIB_DIR");
    let Ok(lib_dir) = std::env::var("SPEEDB_LIB_DIR") else {
        return;
    };
    let lib_dir = PathBuf::from(lib_dir);
    let lib_path = lib_dir.join("libspeedb.so");
    if !std::fs::exists(&lib_path).unwrap_or(false) {
        // No staged library to link against; leave the plugin as-is rather than
        // failing the build (mirrors libspeedb-sys's tolerant behaviour).
        println!("cargo::warning=SPEEDB_LIB_DIR set but {} not found", lib_path.display());
        return;
    }
    println!("cargo::rerun-if-changed={}", lib_path.display());

    // Record a `DT_NEEDED` for SpeedB. `-lspeedb` resolves `libspeedb.so` in
    // `lib_dir`; the recorded NEEDED is the library's SONAME
    // (`libspeedb.so.<major>.<minor>`). The plugin's ~127 undefined `rocksdb::*`
    // references keep it from being dropped under the default `--as-needed`.
    println!("cargo::rustc-link-search=native={}", lib_dir.display());
    println!("cargo::rustc-link-arg=-L{}", lib_dir.display());
    println!("cargo::rustc-link-arg=-lspeedb");
    // Embed an rpath to the speedb directory so a `cargo`-only link (no `build.sh`
    // rpath stamping) can still resolve it at runtime; `build.sh` additionally
    // stamps `$ORIGIN:<speedb_dir>` over this for the staged artifact.
    println!("cargo::rustc-link-arg=-Wl,-rpath,{}", lib_dir.display());
}

/// Links the disk vector index implementation and its VecSim dependencies into the
/// plugin cdylib. The build coordinator (`build.sh`) sets `REDISEARCH_VECSIM_BUILD_DIR`
/// to the CMake build directory that contains the archives; when it is unset (e.g. a
/// plain in-submodule `cargo build`/`clippy` with no superrepo) we skip — the plugin
/// is only ever produced by the superrepo build.
fn link_vecsim_into_plugin() {
    let Ok(build_dir) = std::env::var("REDISEARCH_VECSIM_BUILD_DIR") else {
        println!("cargo::rerun-if-env-changed=REDISEARCH_VECSIM_BUILD_DIR");
        return;
    };
    println!("cargo::rerun-if-env-changed=REDISEARCH_VECSIM_BUILD_DIR");
    let build_dir = PathBuf::from(build_dir);
    let vecsim_dir = build_dir.join("deps/RediSearch/src/VectorSimilarity/src/VecSim");
    let spaces_dir = vecsim_dir.join("spaces");

    // Resolve `lib<name>.a` from the first matching candidate (spdlog/fmt carry a
    // `d` suffix in debug builds, none in release).
    let resolve = |dir: &Path, candidates: &[&str]| -> PathBuf {
        for name in candidates {
            let p = dir.join(format!("lib{name}.a"));
            if std::fs::exists(&p).unwrap_or(false) {
                return p;
            }
        }
        panic!(
            "VecSim archive {} not found under {}",
            candidates.join("/"),
            dir.display()
        );
    };

    // `vecsim_disk` is the plugin's payload (it owns disk vector indexes). Its
    // `VecSimDisk_*` C API and C++ objects are referenced from the Rust crate only
    // through FFI function pointers, which the linker cannot see as live when it
    // scans the archive — so on-demand archive semantics leave them unpulled. Force
    // the whole archive in.
    let vecsim_disk_a = resolve(&build_dir.join("vecsim_disk"), &["vecsim_disk"]);
    println!("cargo::rerun-if-changed={}", vecsim_disk_a.display());
    println!("cargo::rustc-link-arg=-Wl,--whole-archive");
    println!("cargo::rustc-link-arg={}", vecsim_disk_a.display());
    println!("cargo::rustc-link-arg=-Wl,--no-whole-archive");

    // The transitive VecSim libraries (the same set the CMake `vecsim_disk` target
    // links via `PUBLIC VectorSimilarity`:
    //   VectorSimilarity -> VectorSimilaritySpaces
    //     -> VectorSimilaritySpaces_no_optimization + cpu_features
    //   (VecSim logging) -> spdlog -> fmt)
    // reference each other, so wrap them in a linker group for back-references. All
    // archives are passed by ABSOLUTE PATH to avoid `-l`/search-path and
    // `.so`-vs-`.a` ambiguities. The plugin's VecSim copy is pointed at Redis's
    // allocator at init via `VecSim_SetMemoryFunctions` (see the
    // `vecsim_set_memory_functions` helper in `redisearch_disk`).
    let group_libs: [(PathBuf, &[&str]); 6] = [
        (vecsim_dir.clone(), &["VectorSimilarity"]),
        (spaces_dir.clone(), &["VectorSimilaritySpaces"]),
        (spaces_dir.clone(), &["VectorSimilaritySpaces_no_optimization"]),
        (build_dir.join("_deps/cpu_features-build"), &["cpu_features"]),
        (build_dir.join("_deps/spdlog-build"), &["spdlog", "spdlogd"]),
        (build_dir.join("_deps/fmt-build"), &["fmt", "fmtd"]),
    ];
    println!("cargo::rustc-link-arg=-Wl,--start-group");
    for (dir, candidates) in &group_libs {
        let path = resolve(dir, candidates);
        println!("cargo::rustc-link-arg={}", path.display());
        println!("cargo::rerun-if-changed={}", path.display());
    }
    println!("cargo::rustc-link-arg=-Wl,--end-group");

    // The VecSim / vecsim_disk / spdlog / fmt C++ code needs the C++ runtime
    // (`std::exception` typeinfo & vtables, `__dynamic_cast`, `__gxx_personality_v0`,
    // `__once_proxy`, ...). The legacy static link gets this via
    // `build_utils`'s C++ link step, which plugin mode skips, so add the C++ runtime
    // as a `NEEDED` shared library here. Emitted after the archives so it resolves
    // their references.
    println!("cargo::rustc-link-arg=-lstdc++");
}
