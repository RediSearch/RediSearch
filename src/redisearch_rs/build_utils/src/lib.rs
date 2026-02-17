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
pub fn repository_root() -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
    let mut path = std::env::current_dir()?;
    // Jujutsu (`jj`) doesn't colocate with `git` when using `jj workspace add`,
    // so looking for `.git` won't be enough.
    while !(path.join(".git").exists() || path.join(".jj").exists()) {
        path = path
            .parent()
            .ok_or("Could not find git root")?
            .to_path_buf();
    }
    Ok(path)
}

fn rerun_if_changes(dir: &Path, extensions: &[&str]) -> std::io::Result<()> {
    for entry in read_dir(dir)? {
        let Ok(entry) = entry else {
            continue;
        };
        let path = entry.path();
        if path.is_dir() {
            return rerun_if_changes(&path, extensions);
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

/// Walk the specified directory and emit granular `rerun-if-changed` statements,
/// scoped to `*.rs` files.
/// It'd be nice if `cargo` supported globbing syntax natively, but that's not the
/// case today.
fn rerun_if_rust_changes(dir: &Path) -> std::io::Result<()> {
    rerun_if_changes(dir, &["rs"])
}

/// Generate a C header file via `cbindgen` for the calling crate.
/// It'll read `cbindgen` configuration from the `cbindgen.toml` file at the crate root
/// and output the header file to `header_path`.
pub fn run_cbindgen(header_path: impl AsRef<Path>) -> Result<(), Box<dyn std::error::Error>> {
    let config =
        cbindgen::Config::from_file("cbindgen.toml").expect("Failed to find cbindgen config");
    println!("cargo::rerun-if-changed=cbindgen.toml");

    // emit `rerun-if-changed` for all the headers files referenced by the config as well
    if let Some(include) = &config.parse.include {
        for included_crate in include.iter() {
            let path = repository_root()?
                .join("src")
                .join("redisearch_rs")
                .join(included_crate);
            if path.exists() {
                let _ = rerun_if_rust_changes(&path);
            }
        }
    }
    // We should also regenerate the header files if the source of the current
    // crate changes. The current crate isn't usually included in `cbindgen`'s
    // config file under `parse.include`.
    let _ = rerun_if_rust_changes(&PathBuf::from("src"));

    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();

    cbindgen::Builder::new()
        .with_crate(crate_dir)
        .with_config(config)
        .generate()?
        .write_to_file(header_path);

    Ok(())
}

/// Link all the relevant C dependencies to allow Rust (testing and benchmarking) code to invoke
/// RediSearch C symbols.
///
/// This links a single combined static library (`libredisearch_all.a`) that bundles
/// all C code and dependencies together. The combined library is created by CMake
/// during the build process.
pub fn bind_foreign_c_symbols() {
    force_link_time_symbol_resolution();
    link_redisearch_all();
    link_mkl();
    link_c_plusplus();
}

/// Require all symbols to be resolved at link time.
fn force_link_time_symbol_resolution() {
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
fn bin_root() -> PathBuf {
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

/// Link `libredisearch_all.a` using the `-bundle` modifier.
///
/// The `-bundle` modifier prevents the (very large) C archive from being
/// embedded into every Rust rlib in the dependency tree. Instead, the linker
/// flag `-lredisearch_all` propagates to final binaries (tests, benchmarks)
/// where the linker selectively pulls only the objects that are actually
/// needed. This avoids two problems:
///
/// 1. Cross-crate rlib contamination during `cargo test --workspace`, where
///    C objects bundled into one crate's rlib can trigger undefined-symbol
///    errors in unrelated workspace members.
/// 2. Archive member counts exceeding `u16::MAX` in rustc's
///    `ar_archive_writer` when MKL or other large archives are involved.
fn link_redisearch_all() {
    let bin_root = bin_root();
    let lib_dir = bin_root.join("src");
    let lib = lib_dir.join("libredisearch_all.a");
    if std::fs::exists(&lib).unwrap_or(false) {
        println!("cargo::rustc-link-lib=static:-bundle=redisearch_all");
        println!("cargo::rerun-if-changed={}", lib.display());
        println!("cargo::rustc-link-search=native={}", lib_dir.display());
    } else {
        panic!("Static library not found: {}", lib.display());
    }
}

/// Link Intel MKL separately if present.
///
/// MKL is excluded from `libredisearch_all.a` because its ~42K object files
/// overflow the `u16` archive member index in rustc's `ar_archive_writer`.
/// Like `redisearch_all`, we link with `-bundle` to avoid rlib bloat.
fn link_mkl() {
    let svs_lib_dir = bin_root().join("_deps/svs-src/lib");
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
