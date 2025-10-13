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
pub fn run_cbinden(header_path: impl AsRef<Path>) -> Result<(), Box<dyn std::error::Error>> {
    let config =
        cbindgen::Config::from_file("cbindgen.toml").expect("Failed to find cbindgen config");
    println!("cargo::rerun-if-changed=cbindgen.toml");

    // emit `rerun-if-changed` for all the headers files referenced by the config as well
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
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_else(|_| "linux".to_string());

    // There may be several symbols exposed by the static library that we are trying to link
    // that we don't actually invoke (either directly or indirectly) in our benchmarks.
    // We will provide a definition for the ones we need (e.g. Redis' allocation functions),
    // but we don't want to be forced to add dummy definitions for the ones we don't rely on.
    // We prefer to fail at runtime if we try to use a symbol that's undefined.
    if target_os == "macos" {
        println!("cargo::rustc-link-arg=-Wl,-undefined,dynamic_lookup");
    } else {
        println!("cargo::rustc-link-arg=-Wl,--unresolved-symbols=ignore-in-object-files");
    }

    let bin_root = if let Ok(bin_root) = std::env::var("BINDIR") {
        // The directory changes depending on a variety of factors: target architecture, target OS,
        // optimization level, coverage, etc.
        // We rely on the top-level build coordinator to give us the correct path, rather
        // than duplicating the whole layout logic here.
        PathBuf::from(bin_root)
    } else {
        // If one is not provided (e.g. `cargo` has been invoked directly), we look
        // for a release build of the static library in the conventional location
        // for the bin directory.
        let root = git_root().expect("Could not find git root for static library linking");
        let target_arch = match env::var("CARGO_CFG_TARGET_ARCH").ok().as_deref() {
            Some("x86_64") | None => "x64".to_owned(),
            Some(a) => a.to_owned(),
        };
        root.join(format!(
            "bin/{target_os}-{target_arch}-release/search-community/"
        ))
    };

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
/// * `include_inverted_index` - Whether to include the inverted_index directory in the include paths.
///
/// # Generated Output
/// The function writes the generated bindings to `bindings.rs` in the cargo build output directory.
pub fn generate_c_bindings(
    headers: Vec<PathBuf>,
    allowlist_file: &str,
    include_inverted_index: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let root = git_root().expect("Could not find git root for static library linking");

    let mut includes = vec![
        // root.join("deps").join("RedisModulesSDK"),
        root.join("src"),
        root.join("deps"),
        root.join("src").join("redisearch_rs").join("headers"),
        root.join("deps").join("VectorSimilarity").join("src"),
        root.join("src").join("buffer"),
    ];

    if include_inverted_index {
        includes.push(root.join("src").join("inverted_index"));
    }

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
        .blocklist_function("InvertedIndex_Summary")
        .blocklist_function("InvertedIndex_BlocksSummary")
        .blocklist_function("InvertedIndex_BlocksSummaryFree")
        .generate()?
        .write_to_file(out_dir.join("bindings.rs"))?;

    Ok(())
}
