/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::path::PathBuf;

fn main() {
    let data = vec![
        ("value", "value"),
        ("mempool", "util/mempool"),
        ("module_init", "module-init"),
    ];

    let os = std::env::var("CARGO_CFG_TARGET_OS").unwrap();
    let arch = std::env::var("CARGO_CFG_TARGET_ARCH").unwrap();
    let platform = format!("{}-{}-{}", os, arch, get_build_profile_name());

    let root = git_root();
    let base_lib_path = root
        .join("bin")
        .join(platform)
        .join("search-community")
        .join("src");
    for (lib_name, sub_path) in data {
        let path: PathBuf = base_lib_path.join(sub_path);

        println!("cargo:rustc-link-lib=static={}", lib_name);
        println!("cargo:rustc-link-search=native={}", path.display());
        println!(
            "cargo:rerun-if-changed={}",
            path.join("CMakeLists.txt").display()
        );
    }
}

fn get_build_profile_name() -> String {
    // The profile name is always the 3rd last part of the path (with 1 based indexing).
    // e.g. /code/core/target/cli/build/my-build-info-9f91ba6f99d7a061/out
    std::env::var("OUT_DIR")
        .unwrap()
        .split(std::path::MAIN_SEPARATOR)
        .nth_back(3)
        .unwrap_or("unknown")
        .to_string()
}

fn git_root() -> std::path::PathBuf {
    let mut path = std::env::current_dir().unwrap();
    while !path.join(".git").exists() {
        path = path.parent().unwrap().to_path_buf();
    }
    path
}
