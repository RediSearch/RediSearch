/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::env;
use std::path::{Path, PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    link_static_libraries();

    // Generate C bindings - fail build if this doesn't work
    generate_c_bindings()?;

    Ok(())
}

fn link_static_libraries() {
    let root = git_root().expect("Could not find git root for static library linking");
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_else(|_| "linux".to_string());
    let target_arch = match env::var("CARGO_CFG_TARGET_ARCH").ok().as_deref() {
        Some("x86_64") | None => "x64".to_owned(),
        Some(a) => a.to_owned(),
    };

    let bin_root = root.join(format!(
        "bin/{target_os}-{target_arch}-release/search-community/"
    ));

    link_static_lib(&bin_root, ".", "varint").unwrap();
    link_static_lib(&bin_root, "src/util/arr", "arr").unwrap();
}

fn generate_c_bindings() -> Result<(), Box<dyn std::error::Error>> {
    let root = git_root()?;

    let redis_modules = root.join("deps").join("RedisModulesSDK");
    let src = root.join("src");
    let deps = root.join("deps");
    let headers_dir = root.join("src").join("redisearch_rs").join("headers");

    let bindings = bindgen::Builder::default()
        .header(
            root.join("src")
                .join("varint.h")
                .to_str()
                .ok_or("Invalid path")?,
        )
        .clang_arg(format!("-I{}", src.display()))
        .clang_arg(format!("-I{}", deps.display()))
        .clang_arg(format!("-I{}", redis_modules.display()))
        .clang_arg(format!("-I{}", headers_dir.display()))
        .generate()?;

    // Re-run the build script if any of the files in those directories change
    println!("cargo:rerun-if-changed={}", src.display());
    println!("cargo:rerun-if-changed={}", deps.display());
    println!("cargo:rerun-if-changed={}", redis_modules.display());
    println!("cargo:rerun-if-changed={}", headers_dir.display());

    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    bindings.write_to_file(out_dir.join("bindings.rs"))?;

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
