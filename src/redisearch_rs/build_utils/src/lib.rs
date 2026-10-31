/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! build.rs utilities.

use std::{
    env,
    fs::read_dir,
    path::{Path, PathBuf},
};

/// Return the root folder of the repository.
///
/// `build_utils` lives at `src/redisearch_rs/build_utils`, so its Cargo manifest
/// directory is a stable anchor even when source archives omit VCS metadata.
pub fn repository_root() -> Result<PathBuf, Box<dyn std::error::Error>> {
    // Keep this lexical rather than canonicalizing it. Callers only join paths
    // below this root, and `canonicalize` requires `realpath`, which Miri does
    // not support with filesystem isolation enabled.
    Ok(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.."))
}

fn rerun_if_changes(dir: &Path, extensions: &[&str]) -> std::io::Result<()> {
    // Don't descend into Cargo's target directory. Cargo marks it with a
    // `CACHEDIR.TAG` file, so detect it that way regardless of where
    // `CARGO_TARGET_DIR` points (e.g. the CMake build sets it to
    // `src/redisearch_rs/target`, which sits under the `src` include root
    // scanned by `ffi/build.rs`). Otherwise the sweep would reach a build
    // script's own `OUT_DIR` and emit `rerun-if-changed` for the headers that
    // script just staged there, forcing a rebuild on every invocation.
    if dir.join("CACHEDIR.TAG").exists() {
        return Ok(());
    }
    for entry in read_dir(dir)? {
        let Ok(entry) = entry else {
            continue;
        };
        let path = entry.path();
        if path.is_dir() {
            rerun_if_changes(&path, extensions)?;
        } else if let Some(extension) = path.extension().and_then(|ext| ext.to_str())
            && extensions.contains(&extension)
        {
            println!("cargo::rerun-if-changed={}", path.display());
        }
    }
    Ok(())
}

/// Walk the specified directory and emit granular `rerun-if-changed` statements,
/// scoped to `*.c` and `*.h` files.
/// It'd be nice if `cargo` supported globbing syntax natively, but that's not the
/// case today.
pub fn rerun_if_c_changes(dir: &Path) -> std::io::Result<()> {
    rerun_if_changes(dir, &["c", "h"])
}

/// Include roots every Rust-side consumer of the RediSearch C headers needs.
///
/// Ordering is load-bearing: `src` must precede `src/redisearch_rs/headers`,
/// which holds generated counterparts of `rlookup.h` and `ttl_table.h`. Swapping
/// the two silently binds against the generated header instead of the C original.
/// They are the only colliding header basenames across these roots.
fn c_include_roots(root: &Path) -> Vec<PathBuf> {
    vec![
        root.join("src"),
        root.join("deps"),
        root.join("src").join("redisearch_rs").join("headers"),
        root.join("deps").join("VectorSimilarity").join("src"),
        root.join("src").join("buffer"),
        root.join("src").join("ttl_table"),
    ]
}

/// Compile a benchmark shim written in C against the RediSearch headers.
///
/// Bench crates use a small C shim to drive a production C iterator as the
/// baseline for its Rust counterpart. Every such shim needs the same include
/// roots, the same preprocessor defines, and the same warning suppressions, so
/// they are centralised here: a shim that configures its own [`cc::Build`] from
/// scratch silently drifts from how CMake compiles the very headers it includes.
///
/// `shim` is the path to the C file, relative to the calling crate's manifest
/// directory. The compiled archive is named after the shim's file stem, and a
/// `rerun-if-changed` is emitted for it.
///
/// # Defines
///
/// `_GNU_SOURCE` is required, for the same reason the bindgen path in
/// `ffi/build.rs` passes it: shims are compiled without `REDIS_MODULE_TARGET`, so
/// `deps/rmalloc/rmalloc.h` takes its non-module fallback and expands
/// `rm_asprintf` to bare `asprintf` — which glibc's `<stdio.h>` only declares
/// under `_GNU_SOURCE`, and the top-level `CMakeLists.txt` promotes implicit
/// declarations to errors.
///
/// `REDIS_MODULE_TARGET` is deliberately *not* set. It would route `rm_malloc`
/// through `RedisModule_Alloc`, but CMake defines it only when
/// `USE_REDIS_ALLOCATOR` is on, so forcing it here could disagree with how
/// `libredisearch_c_bundle.a` was actually built. No shim calls `rm_*` today —
/// they use `RedisModule_Alloc` directly where the module owns the buffer.
///
/// # Panics
///
/// Panics if the repository root cannot be located, or if `shim` has no file stem.
pub fn compile_c_bench_shim(shim: &str) {
    let root = repository_root().expect("Could not find repository root");

    // Beyond the shared roots, shims reach the iterator API, `rmalloc.h` and the
    // trie headers directly. Appended, so they cannot shadow a shared root.
    let mut includes = c_include_roots(&root);
    includes.extend([
        root.join("src").join("iterators"),
        root.join("deps").join("rmalloc"),
        root.join("src").join("trie"),
    ]);

    let lib_name = Path::new(shim)
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or_else(|| panic!("bench shim path has no file stem: {shim}"));

    cc::Build::new()
        .file(shim)
        .define("_GNU_SOURCE", None)
        // Silence warnings originating from transitively-included RediSearch
        // headers (static helpers, sign-compare in inline funcs, etc.) — they
        // are not actionable from a shim and the main CMake build already
        // suppresses them.
        .flag_if_supported("-Wno-unused-function")
        .flag_if_supported("-Wno-unused-variable")
        .flag_if_supported("-Wno-unused-parameter")
        .flag_if_supported("-Wno-sign-compare")
        .includes(includes)
        .compile(lib_name);

    println!("cargo:rerun-if-changed={shim}");
}

/// Link all the relevant C dependencies to allow Rust (testing and benchmarking) code to invoke
/// RediSearch C symbols.
///
/// This links a single combined static library (`libredisearch_c_bundle.a`) that bundles
/// all C code and dependencies together. The combined library is created by CMake
/// during the build process.
pub fn bind_foreign_c_symbols() {
    let bin_root = bin_root();
    force_link_time_symbol_resolution();
    link_redisearch_c_bundle(&bin_root).unwrap_or_else(|e| panic!("{e}"));
    let mkl_linked = link_mkl(&bin_root.join("_deps/svs-src/lib"));
    // Cargo's `-l` flags precede the rlibs; re-scan the bundle after them. GNU ld reads
    // each archive once, so group it there with the system libs its members need.
    let gnu_ld = env::var("CARGO_CFG_TARGET_OS").unwrap_or_else(|_| "linux".to_string()) != "macos";
    if gnu_ld {
        println!("cargo::rustc-link-arg=-Wl,--start-group");
    }
    println!("cargo::rustc-link-arg=-lredisearch_c_bundle");
    if mkl_linked {
        // The bundle holds the SVS members that call into MKL but not MKL itself, so this
        // pass can reference it after its own `-l` flag was already scanned. Their
        // references run both ways, which the surrounding group resolves.
        println!("cargo::rustc-link-arg=-lmkl_static_library");
    }
    if gnu_ld {
        println!("cargo::rustc-link-arg=-lstdc++");
        println!("cargo::rustc-link-arg=-lpthread");
        println!("cargo::rustc-link-arg=-lc");
        println!("cargo::rustc-link-arg=-Wl,--end-group");
    }
    link_c_plusplus();
}

/// Require all symbols to be resolved at link time.
pub fn force_link_time_symbol_resolution() {
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_else(|_| "linux".to_string());
    if target_os == "macos" {
        println!("cargo::rustc-link-arg=-Wl,-undefined,error");
    } else {
        println!("cargo::rustc-link-arg=-Wl,--unresolved-symbols=report-all");
    }
}

/// Return the CMake build output directory.
///
/// When the top-level build coordinator sets `BINDIR`, that value is used
/// directly. Otherwise we fall back to the conventional release layout
/// derived from the git root.
///
/// The chosen directory is baked into the `-L` link-search flags this crate
/// emits, so the `rerun-if-env-changed` below re-evaluates it whenever
/// `BINDIR` changes — otherwise Cargo replays a stale cached `-L` (e.g. a
/// release path captured during a `BINDIR`-less run) into a later build.
fn bin_root() -> PathBuf {
    println!("cargo::rerun-if-env-changed=BINDIR");
    if let Ok(bin_root) = std::env::var("BINDIR") {
        // The directory changes depending on a variety of factors: target architecture, target OS,
        // optimization level, coverage, etc.
        // We rely on the top-level build coordinator to give us the correct path, rather
        // than duplicating the whole layout logic here.
        PathBuf::from(bin_root)
    } else {
        // If one is not provided (e.g. `cargo` has been invoked directly), we look
        // for a release build of the static library in the conventional location
        // for the bin directory.
        let root =
            repository_root().expect("Could not find repository root for static library linking");
        let target_arch = match env::var("CARGO_CFG_TARGET_ARCH").ok().as_deref() {
            Some("x86_64") | None => "x64".to_owned(),
            Some(a) => a.to_owned(),
        };
        let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_else(|_| "linux".to_string());
        root.join(format!(
            "bin/{target_os}-{target_arch}-release/search-community/"
        ))
    }
}

/// Link `libredisearch_c_bundle.a` using the `-bundle` modifier, returning an error if the
/// library is not found.
///
/// The `-bundle` modifier prevents the (very large) C archive from being
/// embedded into every Rust rlib in the dependency tree. Instead, the linker
/// flag `-lredisearch_c_bundle` propagates to final binaries (tests, benchmarks)
/// where the linker selectively pulls only the objects that are actually
/// needed. This avoids two problems:
///
/// 1. Cross-crate rlib contamination during `cargo test --workspace`, where
///    C objects bundled into one crate's rlib can trigger undefined-symbol
///    errors in unrelated workspace members.
/// 2. Archive member counts exceeding `u16::MAX` in rustc's
///    `ar_archive_writer` when MKL or other large archives are involved.
///
/// Callers that need soft-fail behaviour (e.g. lint-only runs where the
/// library has not been built) can inspect the returned `Err` and emit a
/// `cargo::warning` instead of panicking.
pub fn link_redisearch_c_bundle(bin_root: &Path) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let lib_dir = bin_root.join("src");
    let lib = lib_dir.join("libredisearch_c_bundle.a");
    if std::fs::exists(&lib).unwrap_or(false) {
        println!("cargo::rustc-link-lib=static:-bundle=redisearch_c_bundle");
        println!("cargo::rerun-if-changed={}", lib.display());
        println!("cargo::rustc-link-search=native={}", lib_dir.display());
        Ok(lib)
    } else {
        Err(format!("Static library not found: {}", lib.display()).into())
    }
}

/// Link Intel MKL separately if present.
///
/// MKL is excluded from `libredisearch_c_bundle.a` because its ~42K object files
/// overflow the `u16` archive member index in rustc's `ar_archive_writer`.
/// Like `redisearch_c_bundle`, we link with `-bundle` to avoid rlib bloat.
///
/// `svs_lib_dir` is the directory that contains `libmkl_static_library.a`.
/// Its location varies across build configurations, so callers are responsible
/// for supplying the correct path.
///
/// Returns whether the archive was found and linked. It is absent whenever SVS was
/// built without the Intel optimisation, so callers that reference MKL again — e.g.
/// through a raw link argument — must gate on this rather than assume it is there.
pub fn link_mkl(svs_lib_dir: &Path) -> bool {
    let mkl = svs_lib_dir.join("libmkl_static_library.a");
    if std::fs::exists(&mkl).unwrap_or(false) {
        println!("cargo::rerun-if-changed={}", mkl.display());
        println!("cargo::rustc-link-search=native={}", svs_lib_dir.display());
        println!("cargo::rustc-link-lib=static:-bundle=mkl_static_library");
        true
    } else {
        false
    }
}

/// Link the C++ standard library using the platform's default.
///
/// This is needed for VectorSimilarity and other C++ code that RediSearch depends on.
/// We compile a dummy C++ file which causes cc to emit the appropriate link flags,
/// using the same approach as the `link-c-plusplus` crate.
fn link_c_plusplus() {
    let out_dir = env::var("OUT_DIR").expect("OUT_DIR not set");
    let dummy_path = std::path::Path::new(&out_dir).join("dummy.cc");
    // Define a symbol to avoid "empty archive" warnings from ranlib
    std::fs::write(&dummy_path, "void __link_cplusplus_dummy() {}\n")
        .expect("Failed to write dummy C++ file");
    cc::Build::new()
        .cpp(true)
        .file(&dummy_path)
        .compile("link-cplusplus");
}

pub fn link_static_lib(
    bin_root: &Path,
    lib_subdir: &str,
    lib_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let lib_dir = bin_root.join(lib_subdir);
    let lib = lib_dir.join(format!("lib{lib_name}.a"));
    if std::fs::exists(&lib).unwrap_or(false) {
        println!("cargo::rustc-link-lib=static={lib_name}");
        println!("cargo::rerun-if-changed={}", lib.display());
        println!("cargo::rustc-link-search=native={}", lib_dir.display());
        Ok(())
    } else {
        Err(format!("Static library not found: {}", lib.display()).into())
    }
}

/// Generates Rust FFI bindings from C header files using bindgen.
///
/// # Arguments
/// * `headers` - A vector of paths to C header files to generate bindings for.
/// * `allowlist_file` - A file path pattern used to filter which files bindgen should generate bindings for.
///
/// # Generated Output
/// The function writes the generated bindings to `bindings.rs` in the cargo build output directory.
pub fn generate_c_bindings(
    headers: Vec<PathBuf>,
    allowlist_file: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let root =
        repository_root().expect("Could not find repository root for static library linking");

    let includes = c_include_roots(&root);

    let headers = headers
        .into_iter()
        .map(|h| h.into_os_string().into_string().unwrap())
        .collect::<Vec<_>>();
    let mut bindings = bindgen::Builder::default().headers(headers);

    for include in includes {
        bindings = bindings.clang_arg(format!("-I{}", include.display()));
        // Re-run the build script if any of the C files in the included
        // directory changes
        let _ = rerun_if_c_changes(&include);
    }

    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    bindings
        .allowlist_file(allowlist_file)
        // Don't generate the Rust exported types else we'll have a compiler issue about the wrong
        // type being used
        .blocklist_file(".*/types_rs.h")
        .blocklist_file(".*/inverted_index.h")
        .blocklist_type("InvertedIndex")
        .generate()?
        .write_to_file(out_dir.join("bindings.rs"))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::repository_root;
    use std::path::PathBuf;

    #[test]
    fn repository_root_resolves_to_redisearch_source_root() -> Result<(), Box<dyn std::error::Error>>
    {
        let root = repository_root()?;

        assert_eq!(
            root,
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../..")
        );

        // Miri runs with filesystem isolation enabled, so metadata checks such
        // as `is_file` are unavailable there. The path construction above is
        // still covered by Miri; the source-root marker checks run in normal
        // test execution.
        #[cfg(not(miri))]
        {
            assert!(root.join("CMakeLists.txt").is_file());
            assert!(
                root.join("src")
                    .join("redisearch_rs")
                    .join("Cargo.toml")
                    .is_file()
            );
            assert!(root.join("src").join("version.h").is_file());
        }

        Ok(())
    }
}
