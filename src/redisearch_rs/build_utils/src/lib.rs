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

/// Return the root folder of the project containing the `.git` directory.
pub fn git_root() -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
    let mut path = std::env::current_dir()?;
    while !path.join(".git").exists() {
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
            println!("cargo:rerun-if-changed={}", path.display());
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

/// Emit granular `rerun-if-changed` statements for each files which are
/// part of the crates listed in `config.parse.include`.
/// This ensures cbindgen output is regenerated when the underlying Rust code
/// is updated.
pub fn rerun_cbinden(config: &cbindgen::Config) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(include) = &config.parse.include {
        for included_crate in include.iter() {
            let path = git_root()?
                .join("src")
                .join("redisearch_rs")
                .join(included_crate);
            if path.exists() {
                let _ = rerun_if_rust_changes(&path);
            }
        }
    }

    Ok(())
}

/// Links static libraries
///
/// This function configures the linker to include static libraries built by the main
/// RediSearch build system.
/// It's meant to be called from the `build.rs` script using `bindgen` to generate Rust bindings.
///
/// # Arguments
/// * `libs` - A slice of tuples where each tuple contains:
///   - Library subdirectory path relative to the build output directory
///   - Library name (without lib prefix and .a suffix)
///
/// # Panics
/// Panics if any required static library is not found in the expected location.
pub fn link_static_libraries(libs: &[(&str, &str)]) {
    let root = git_root().expect("Could not find git root for static library linking");
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_else(|_| "linux".to_string());
    let target_arch = match env::var("CARGO_CFG_TARGET_ARCH").ok().as_deref() {
        Some("x86_64") | None => "x64".to_owned(),
        Some(a) => a.to_owned(),
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

    for &(lib_subdir, lib_name) in libs {
        link_static_lib(&bin_root, lib_subdir, lib_name).unwrap();
    }
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
    let root = git_root().expect("Could not find git root for static library linking");

    let includes = [
        root.join("deps").join("RedisModulesSDK"),
        root.join("src"),
        root.join("deps"),
        root.join("src").join("redisearch_rs").join("headers"),
        root.join("deps").join("VectorSimilarity").join("src"),
        root.join("src").join("buffer"),
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
        .generate()?
        .write_to_file(out_dir.join("bindings.rs"))?;

    Ok(())
}
