/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::env;
use std::fs::read_dir;
use std::path::{Path, PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Always attempt to link static libraries if they exist, independent of bindgen
    try_link_static_libraries();

    // Generate C bindings - fail build if this doesn't work
    generate_c_bindings()?;

    Ok(())
}

fn try_link_static_libraries() {
    let Ok(root) = git_root() else {
        println!("cargo:warning=Could not find git root for static library linking");
        return;
    };

    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_else(|_| "linux".to_string());
    let target_arch = env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_else(|_| "x86_64".to_string());

    let target_arch = match target_arch.as_str() {
        "x86_64" => "x64",
        _ => &target_arch,
    };

    // There are several symbols exposed by `libtrie.a` that we don't
    // actually invoke (either directly or indirectly) in our benchmarks.
    // We provide a definition for the ones we need (e.g. Redis' allocation functions),
    // but we don't want to be forced to add dummy definitions for the ones we don't rely on.
    // We prefer to fail at runtime if we try to use a symbol that's undefined.
    // This is the default linker behaviour on macOS. On other platforms, the default
    // configuration is stricter: it exits with an error if any symbol is undefined.
    // We intentionally relax it here.
    if target_os != "macos" {
        println!("cargo:rustc-link-arg=-Wl,--unresolved-symbols=ignore-in-object-files");
    }

    let bin_root = root.join(format!(
        "bin/{target_os}-{target_arch}-release/search-community/"
    ));

    // Try to link static libraries, but don't fail build if they don't exist
    if let Err(e) = link_static_lib(&bin_root, "src/inverted_index", "inverted_index") {
        println!(
            "cargo:warning=Could not link inverted_index static library: {}",
            e
        );
    }
}

fn generate_c_bindings() -> Result<(), Box<dyn std::error::Error>> {
    let root = git_root()?;

    let includes = {
        let redis_modules = root.join("deps").join("RedisModulesSDK");
        let src = root.join("src");
        let deps = root.join("deps");
        let redisearch_rs = root.join("src").join("redisearch_rs").join("headers");
        let vec_sim = root.join("deps").join("VectorSimilarity").join("src");
        [redis_modules, src, deps, redisearch_rs, vec_sim]
    };

    let mut bindings = bindgen::Builder::default().header(
        root.join("src")
            .join("inverted_index")
            .join("inverted_index.h")
            .to_str()
            .ok_or("Invalid path")?,
    );

    for include in includes {
        bindings = bindings.clang_arg(format!("-I{}", include.display()));
        // Re-run the build script if any of the C files in the included
        // directory changes
        let _ = rerun_if_c_changes(&include)?;
    }

    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    bindings
        .allowlist_file(".*/inverted_index.h")
        .blocklist_file(".*/types_rs.h")
        .generate()?
        .write_to_file(out_dir.join("bindings.rs"))?;

    Ok(())
}

fn link_static_lib(
    bin_root: &Path,
    lib_subdir: &str,
    lib_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let lib_dir = bin_root.join(lib_subdir);
    let lib = lib_dir.join(format!("lib{lib_name}.a"));
    if std::fs::exists(&lib).unwrap_or(false) {
        println!("cargo:rustc-link-lib=static={lib_name}");
        println!("cargo:rerun-if-changed={}", lib.display());
        println!("cargo:rustc-link-search=native={}", lib_dir.display());
        Ok(())
    } else {
        Err(format!("Static library not found: {}", lib.display()).into())
    }
}

fn git_root() -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
    let mut path = std::env::current_dir()?;
    while !path.join(".git").exists() {
        path = path
            .parent()
            .ok_or("Could not find git root")?
            .to_path_buf();
    }
    Ok(path)
}

/// Walk the specified directory and emit granular `rerun-if-changed` statements,
/// scoped to `*.c` and `*.h` files.
/// It'd be nice if `cargo` supported globbing syntax natively, but that's not the
/// case today.
fn rerun_if_c_changes(dir: &Path) -> std::io::Result<()> {
    for entry in read_dir(dir)? {
        let Ok(entry) = entry else {
            continue;
        };
        let path = entry.path();
        if path.is_dir() {
            return rerun_if_c_changes(&path);
        } else if let Some(extension) = path.extension() {
            if extension == "c" || extension == "h" {
                println!("cargo:rerun-if-changed={}", path.display());
            }
        }
    }

    Ok(())
}
