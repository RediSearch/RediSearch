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

fn main() {
    let root = git_root();

    // Construct the correct folder path based on OS and architecture
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap();

    // There are several symbols exposed by the C code that we don't
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

    let includes = {
        let redis_modules = root.join("deps").join("RedisModulesSDK");
        let src = root.join("src");
        let deps = root.join("deps");

        let redisearch_rs = src.join("redisearch_rs").join("headers");
        let inverted_index = src.join("inverted_index");
        let vecsim = deps.join("VectorSimilarity").join("src");

        [
            redis_modules,
            src,
            deps,
            redisearch_rs,
            inverted_index,
            vecsim,
        ]
    };

    let headers = {
        let buffer_h = root.join("src").join("buffer.h");
        let redisearch_h = root.join("src").join("redisearch.h");
        let result_processor_h = root.join("src").join("result_processor.h");
        [buffer_h, redisearch_h, result_processor_h]
    };

    let mut bindings = bindgen::Builder::default();

    for header in headers {
        bindings = bindings
            .header(header.display().to_string())
            .allowlist_file(header.display().to_string());

        println!("cargo:rerun-if-changed={}", header.display());
    }
    for include in includes {
        bindings = bindings.clang_arg(format!("-I{}", include.display()));
        // Re-run the build script if any of the C files in the included
        // directory changes
        rerun_if_c_changes(&include);
    }

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .allowlist_file(".*/types_rs.h")
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file(out_dir.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}

fn git_root() -> std::path::PathBuf {
    let mut path = std::env::current_dir().unwrap();
    while !path.join(".git").exists() {
        path = path.parent().unwrap().to_path_buf();
    }
    path
}

/// Walk the specified directory and emit granular `rerun-if-changed` statements,
/// scoped to `*.c` and `*.h` files.
/// It'd be nice if `cargo` supported globbing syntax natively, but that's not the
/// case today.
fn rerun_if_c_changes(dir: &Path) {
    for entry in read_dir(dir).expect("Failed to read directory") {
        let Ok(entry) = entry else {
            continue;
        };
        let path = entry.path();
        if path.is_dir() {
            rerun_if_c_changes(&path);
        } else if let Some(extension) = path.extension() {
            if extension == "c" || extension == "h" {
                println!("cargo:rerun-if-changed={}", path.display());
            }
        }
    }
}
