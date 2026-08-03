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

/// Link all the relevant C dependencies to allow Rust (testing and benchmarking) code to invoke
/// RediSearch C symbols.
///
/// This links a single combined static library (`libredisearch_c_bundle.a`) that bundles
/// all C code and dependencies together. The combined library is created by CMake
/// during the build process. It also compiles and force-links the `RedisModule_*`
/// API function-pointer table — see [`link_redis_module_api_table`].
pub fn bind_foreign_c_symbols() {
    let bin_root = bin_root();
    force_link_time_symbol_resolution();
    // Must be emitted before `libredisearch_c_bundle.a`, see the function's docs.
    link_redis_module_api_table();
    link_redisearch_c_bundle(&bin_root).unwrap_or_else(|e| panic!("{e}"));
    link_mkl(&bin_root.join("_deps/svs-src/lib"));
    link_c_plusplus();
}

/// Define the `RedisModule_*` API function-pointer table for test and benchmark
/// binaries.
///
/// `redismodule.h` declares the module API as `extern` function pointers in
/// every translation unit but the one that defines `REDISMODULE_MAIN` — for
/// RediSearch that is `src/module.c`, which is where the pointers actually live.
/// Any Rust code that reads one of them (the `redis-module` crate's generated
/// bindings do) therefore leaves the linker an undefined reference to resolve.
/// If it resolves it out of `libredisearch_c_bundle.a`, it pulls in `module.c.o` and
/// with it the module's whole command-dispatch layer — including every Rust FFI
/// symbol that layer calls back into, which a single-crate test binary does not
/// link. The link then fails on a wall of undefined `QueryError_*`, `RLookup_*`
/// and friends.
///
/// `redis-module` already carries a definition of the table — its build script
/// compiles a TU against its own vendored copy of the header, which has no
/// `REDISMODULE_MAIN` gate — so this is purely a question of which definition the
/// linker reaches first, and `libredisearch_c_bundle.a` gets there before that rlib.
/// It bites even when the reference itself originates *inside* an rlib listed
/// after the C archive, because lld resolves archives as a group rather than in a
/// single left-to-right pass, so a later reference still reaches back and extracts
/// `module.c.o`. Expect this wherever the link goes through lld; in this repo that
/// is the nightly toolchain `SAN=address` selects, which is where it was found.
///
/// Compile a translation unit that defines nothing but the table and link it with
/// `+whole-archive` before `libredisearch_c_bundle.a`, so the pointers are already
/// defined by the time the C archive is considered.
///
/// This does not turn into a pile of duplicate definitions because
/// `redismodule.h` tags every pointer with `REDISMODULE_ATTR_COMMON`
/// (`__attribute__((__common__))`), so they are *common* symbols even though
/// modern compilers default to `-fno-common`. Commons merge with each other, so
/// the table coexists with `module.c.o` when something else in a test binary
/// legitimately needs that object.
///
/// Two consequences worth knowing about:
///
/// - A crate that reads a `RedisModule_*` pointer without providing a definition
///   now links and calls a NULL pointer at runtime, where it used to fail at link
///   time. Reach for `redis_mock::mock_or_stub_missing_redis_c_symbols!` when a
///   test needs a working entry point rather than a zeroed one.
/// - A strong definition overrides a common only when it is in an object the
///   linker already has. lld will *not* fetch an archive member just to upgrade a
///   common, so a `mock_or_stub_missing_redis_c_symbols!` invoked at the top level
///   of a *library* crate (the `*_bencher` crates do this) can lose to the zeroed
///   table if nothing else pulls its codegen unit in. GNU ld does fetch it. If a
///   mocked entry point ever reads back as NULL, check `nm` for `B` instead of `D`.
///
/// The reordering is verified against GNU ld and LLVM lld. Mach-O linkers resolve
/// overrides of tentative definitions differently, so this is not known to fix
/// macOS; the extra commons are harmless there either way.
fn link_redis_module_api_table() {
    let root = repository_root().expect("Could not find repository root for the RedisModule API");
    let src = root.join("src");
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR not set"));

    let stub = out_dir.join("redismodule_api_table.c");
    std::fs::write(
        &stub,
        "#define REDISMODULE_MAIN\n#include \"redismodule.h\"\n",
    )
    .expect("Failed to write the RedisModule API table translation unit");
    println!(
        "cargo::rerun-if-changed={}",
        src.join("redismodule.h").display()
    );

    // `cc` emits the `-l`/`-L` directives, and with them the
    // `rerun-if-env-changed` lines for the toolchain that builds this archive —
    // otherwise cargo would happily force-link a stale archive built by a
    // different compiler than the one the final binary is linked with.
    cc::Build::new()
        .file(&stub)
        .include(&src)
        .link_lib_modifier("-bundle,+whole-archive")
        .compile("redismodule_api_table");
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
pub fn link_mkl(svs_lib_dir: &Path) {
    let mkl = svs_lib_dir.join("libmkl_static_library.a");
    if std::fs::exists(&mkl).unwrap_or(false) {
        println!("cargo::rerun-if-changed={}", mkl.display());
        println!("cargo::rustc-link-search=native={}", svs_lib_dir.display());
        println!("cargo::rustc-link-lib=static:-bundle=mkl_static_library");
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

    let includes = vec![
        root.join("deps").join("RedisModulesSDK"),
        root.join("src"),
        root.join("deps"),
        root.join("src").join("redisearch_rs").join("headers"),
        root.join("deps").join("VectorSimilarity").join("src"),
        root.join("src").join("buffer"),
        root.join("src").join("ttl_table"),
    ];

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
