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
        println!(
            "cargo::warning=SPEEDB_LIB_DIR set but {} not found",
            lib_path.display()
        );
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
///
/// The plugin must link the SAME VecSim copy + transitive deps that the C core's
/// `libredisearch_all.a` links, otherwise its `VectorSimilarity` objects leave their
/// `spdlog` / `fmt` / SVS symbols undefined. That transitive set is layout- and
/// arch-dependent:
///   - **aarch64** (no SVS): `VectorSimilarity`, `VectorSimilaritySpaces`
///     (+ `_no_optimization`), `cpu_features`, and `spdlog` / `fmt` under
///     `_deps/{spdlog,fmt}-build/` (the archives carry a `d` suffix in debug builds).
///   - **x86_64** (SVS): the same VecSim/Spaces/cpu_features archives, but `spdlog`
///     and `fmt` come precompiled under `_deps/svs-src/lib/` instead, PLUS the SVS
///     archives `libsvs_static_library.a` and `libsvs_x86_objects.a` (which the C
///     core also merges into `libredisearch_all.a`).
///
/// To stay layout-agnostic we do not hardcode a single directory per archive. The
/// CMake build exports the already-resolved absolute paths to a file pointed at by
/// `REDISEARCH_VECSIM_ARCHIVES` (see `deps/RediSearch/src/CMakeLists.txt`); when that
/// file is present and non-empty we link exactly that set. Otherwise we fall back to
/// searching the known candidate directories and globbing for the version-suffixed
/// archives — including the SVS archives only when `_deps/svs-src/lib/` exists.
fn link_vecsim_into_plugin() {
    let Ok(build_dir) = std::env::var("REDISEARCH_VECSIM_BUILD_DIR") else {
        println!("cargo::rerun-if-env-changed=REDISEARCH_VECSIM_BUILD_DIR");
        return;
    };
    println!("cargo::rerun-if-env-changed=REDISEARCH_VECSIM_BUILD_DIR");
    let build_dir = PathBuf::from(build_dir);

    // `vecsim_disk` is the plugin's payload (it owns disk vector indexes). Its
    // `VecSimDisk_*` C API and C++ objects are referenced from the Rust crate only
    // through FFI function pointers, which the linker cannot see as live when it
    // scans the archive — so on-demand archive semantics leave them unpulled. Force
    // the whole archive in.
    let vecsim_disk_a = build_dir.join("vecsim_disk/libvecsim_disk.a");
    assert!(
        std::fs::exists(&vecsim_disk_a).unwrap_or(false),
        "plugin payload archive not found: {}",
        vecsim_disk_a.display()
    );
    println!("cargo::rerun-if-changed={}", vecsim_disk_a.display());
    println!("cargo::rustc-link-arg=-Wl,--whole-archive");
    println!("cargo::rustc-link-arg={}", vecsim_disk_a.display());
    println!("cargo::rustc-link-arg=-Wl,--no-whole-archive");

    // The transitive VecSim libraries (the same set `libredisearch_all.a` merges via
    // `vecsim_disk`'s `PUBLIC VectorSimilarity`:
    //   VectorSimilarity -> VectorSimilaritySpaces
    //     -> VectorSimilaritySpaces_no_optimization + cpu_features
    //   (VecSim logging) -> spdlog -> fmt   [+ SVS on x86_64])
    // reference each other, so wrap them in a linker group for back-references. All
    // archives are passed by ABSOLUTE PATH to avoid `-l`/search-path and
    // `.so`-vs-`.a` ambiguities. The plugin's VecSim copy is pointed at Redis's
    // allocator at init via `VecSim_SetMemoryFunctions` (see the
    // `vecsim_set_memory_functions` helper in `redisearch_disk`).
    let group_libs = vecsim_transitive_archives(&build_dir);

    println!("cargo::rustc-link-arg=-Wl,--start-group");
    for path in &group_libs {
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

/// Resolves the transitive VecSim archives the plugin must whole-link-group.
///
/// Prefers the explicit list CMake exports via `REDISEARCH_VECSIM_ARCHIVES` (a file
/// of newline-separated absolute paths); that path is fully layout/arch-agnostic and
/// already includes the SVS archives on x86_64. Falls back to searching the build
/// tree when the env var is unset, the file is empty/missing, or the list is
/// incomplete — the latter guards against the CMake export lagging (its spdlog/fmt
/// paths are globbed at configure time and could be empty before those archives are
/// built), which would otherwise silently drop them and break the link.
fn vecsim_transitive_archives(build_dir: &Path) -> Vec<PathBuf> {
    println!("cargo::rerun-if-env-changed=REDISEARCH_VECSIM_ARCHIVES");
    if let Ok(list_file) = std::env::var("REDISEARCH_VECSIM_ARCHIVES") {
        println!("cargo::rerun-if-changed={list_file}");
        if let Ok(contents) = std::fs::read_to_string(&list_file) {
            let archives: Vec<PathBuf> = contents
                .lines()
                .map(str::trim)
                .filter(|l| !l.is_empty())
                .map(PathBuf::from)
                .filter(|p| std::fs::exists(p).unwrap_or(false))
                .collect();

            // Sanity-check completeness: VectorSimilarity links spdlog (which links
            // fmt), so both MUST be in any valid set. If the exported list is missing
            // either, treat it as stale and discover from the build tree instead.
            let has = |needle: &str| {
                archives.iter().any(|p| {
                    p.file_name()
                        .and_then(|n| n.to_str())
                        .is_some_and(|n| n.starts_with(&format!("lib{needle}")))
                })
            };
            if has("spdlog") && has("fmt") {
                return archives;
            }
            if !archives.is_empty() {
                println!(
                    "cargo::warning=REDISEARCH_VECSIM_ARCHIVES ({list_file}) is missing \
                     spdlog/fmt; falling back to build-tree search"
                );
            }
        }
    }
    discover_vecsim_transitive_archives(build_dir)
}

/// Fallback discovery of the transitive VecSim archives by searching the CMake build
/// tree. Layout-agnostic: each archive is looked up across all known candidate
/// directories and matched by glob so version/debug suffixes (`libspdlogd.a`,
/// `libfmtd.a`) and the SVS layout (`_deps/svs-src/lib/`) are both handled.
///
/// `VectorSimilarity`, `VectorSimilaritySpaces`, `VectorSimilaritySpaces_no_optimization`,
/// `cpu_features`, `spdlog` and `fmt` are MANDATORY and panic if not found. The SVS
/// archives (`svs_static_library`, `svs_x86_objects`) are CONDITIONAL: present only on
/// x86_64 (under `_deps/svs-src/lib/`) and silently skipped when absent (aarch64).
fn discover_vecsim_transitive_archives(build_dir: &Path) -> Vec<PathBuf> {
    let vecsim_dir = build_dir.join("deps/RediSearch/src/VectorSimilarity/src/VecSim");
    let spaces_dir = vecsim_dir.join("spaces");
    let cpu_features_dir = build_dir.join("_deps/cpu_features-build");
    // On x86_64, VectorSimilarity is built WITH SVS: spdlog/fmt are the precompiled
    // copies shipped under the SVS lib dir, alongside the SVS archives themselves.
    let svs_lib_dir = build_dir.join("_deps/svs-src/lib");
    let spdlog_dir = build_dir.join("_deps/spdlog-build");
    let fmt_dir = build_dir.join("_deps/fmt-build");

    let mut archives = Vec::new();

    // Mandatory VecSim core archives (stable CMake target output locations).
    for (dirs, stem) in [
        (&[vecsim_dir.as_path()][..], "VectorSimilarity"),
        (&[spaces_dir.as_path()][..], "VectorSimilaritySpaces"),
        (
            &[spaces_dir.as_path()][..],
            "VectorSimilaritySpaces_no_optimization",
        ),
        (&[cpu_features_dir.as_path()][..], "cpu_features"),
    ] {
        archives.push(require_archive(dirs, stem));
    }

    // spdlog/fmt: `_deps/{spdlog,fmt}-build/` on aarch64, `_deps/svs-src/lib/` on
    // x86_64. Search both; `find_archive` matches the optional debug `d` suffix.
    archives.push(require_archive(
        &[spdlog_dir.as_path(), svs_lib_dir.as_path()],
        "spdlog",
    ));
    archives.push(require_archive(
        &[fmt_dir.as_path(), svs_lib_dir.as_path()],
        "fmt",
    ));

    // SVS archives: x86_64-only. Include when present, skip silently otherwise.
    for stem in ["svs_static_library", "svs_x86_objects"] {
        if let Some(p) = find_archive(&[svs_lib_dir.as_path()], stem) {
            archives.push(p);
        }
    }

    archives
}

/// Returns the archive for `stem` found across `dirs`, or `None` if absent.
///
/// Matches the release name `lib<stem>.a` and the debug name `lib<stem>d.a` (CMake's
/// `CMAKE_DEBUG_POSTFIX` for spdlog/fmt) — and only those exact two forms. Matching a
/// bare `lib<stem>*.a` glob would be ambiguous: `libVectorSimilaritySpaces.a` and
/// `libVectorSimilaritySpaces_no_optimization.a` both start with the
/// `VectorSimilaritySpaces` prefix, so a loose prefix match could pull the wrong
/// archive. The release variant is preferred when both happen to exist.
fn find_archive(dirs: &[&Path], stem: &str) -> Option<PathBuf> {
    let candidates = [format!("lib{stem}.a"), format!("lib{stem}d.a")];
    for dir in dirs {
        for name in &candidates {
            let p = dir.join(name);
            if std::fs::exists(&p).unwrap_or(false) {
                return Some(p);
            }
        }
    }
    None
}

/// Like [`find_archive`] but panics with the searched locations when not found.
fn require_archive(dirs: &[&Path], stem: &str) -> PathBuf {
    find_archive(dirs, stem).unwrap_or_else(|| {
        let searched: Vec<String> = dirs.iter().map(|d| d.display().to_string()).collect();
        panic!(
            "VecSim archive lib{stem}*.a not found under any of: {}",
            searched.join(", ")
        );
    })
}
