/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::env;
use std::path::PathBuf;

use build_utils::{git_root, rerun_if_c_changes};

fn main() {
    let root = git_root().expect("Could not find git root for static library linking");

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
        let buffer = src.join("buffer");

        [
            redis_modules,
            src,
            deps,
            redisearch_rs,
            inverted_index,
            vecsim,
            buffer,
        ]
    };

    let headers = [
        root.join("src").join("redisearch.h"),
        root.join("deps")
            .join("RedisModulesSDK")
            .join("redismodule.h"),
        root.join("src").join("buffer/buffer.h"),
        root.join("src").join("config.h"),
        root.join("src").join("result_processor.h"),
        root.join("src").join("sortable.h"),
        root.join("src").join("value.h"),
        root.join("src").join("obfuscation").join("hidden.h"),
        root.join("src").join("spec.h"),
        root.join("src").join("doc_table.h"),
        root.join("src").join("score_explain.h"),
        root.join("src").join("rlookup.h"),
        root.join("src").join("query.h"),
        root.join("src").join("util").join("arr").join("arr.h"),
    ];

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
        let _ = rerun_if_c_changes(&include);
    }

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .allowlist_file(".*/types_rs.h")
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file(out_dir.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
