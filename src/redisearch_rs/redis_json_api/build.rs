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
    let root = git_root().expect("Could not find git root");

    let includes = [
        root.join("deps").join("RedisModulesSDK"),
        root.join("deps")
            .join("RedisJSON")
            .join("redis_json")
            .join("src")
            .join("include"),
    ];

    let header = root
        .join("deps")
        .join("RedisJSON")
        .join("redis_json")
        .join("src")
        .join("include")
        .join("rejson_api.h");

    let mut bindings = bindgen::Builder::default()
        .header(header.display().to_string())
        .allowlist_file(header.display().to_string())
        // Generate Rust enums for C enums
        .rustified_enum("JSONType")
        // Block the opaque pointer types - we'll define our own newtype wrappers
        .blocklist_type("RedisJSON")
        .blocklist_type("RedisJSONPtr")
        .blocklist_type("JSONResultsIterator")
        .blocklist_type("JSONPath")
        .blocklist_type("JSONKeyValuesIterator")
        // Don't generate layout tests to reduce noise
        .layout_tests(false);

    println!("cargo:rerun-if-changed={}", header.display());

    for include in includes {
        bindings = bindings.clang_arg(format!("-I{}", include.display()));
        let _ = rerun_if_c_changes(&include);
    }

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file(out_dir.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
