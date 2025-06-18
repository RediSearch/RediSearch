/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

fn main() {
    let os = std::env::var("CARGO_CFG_TARGET_OS").unwrap();
    let arch = std::env::var("CARGO_CFG_TARGET_ARCH").unwrap();
    let platform = format!("{}-{}-{}", os, arch, get_build_profile_name());

    let root = git_root();
    let lib_path = root
        .join("bin")
        .join(platform)
        .join("search-community")
        .join("src")
        .join("rust-binded");

    println!("cargo:rustc-link-lib=static=rust_binded");
    println!("cargo:rustc-link-search=native={}", lib_path.display());
    println!(
        "cargo:rerun-if-changed={}",
        root.join("src")
            .join("rust-binded")
            .join("CMakeLists.txt")
            .display()
    );
}

fn get_build_profile_name() -> String {
    // The profile name is always the 3rd last part of the path (with 1 based indexing).
    let candidate = std::env::var("OUT_DIR")
        .unwrap()
        .split(std::path::MAIN_SEPARATOR)
        .nth_back(3)
        .unwrap_or("unknown")
        .to_string();

    if candidate == "optimised_test" || candidate == "profiling" {
        "release".to_string()
    } else {
        candidate
    }
}

fn git_root() -> std::path::PathBuf {
    let mut path = std::env::current_dir().unwrap();
    while !path.join(".git").exists() {
        path = path.parent().unwrap().to_path_buf();
    }
    path
}
