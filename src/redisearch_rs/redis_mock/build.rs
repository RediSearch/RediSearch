/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{path::PathBuf, str::FromStr};

fn main() {
    // link `libc2rust.a` to this crate `redis_mock`
    let root = git_root();

    let binroot = match std::env::var("BINDIR") {
        Ok(val) => PathBuf::from_str(val.as_str()).expect("ENV Var: BINDIR must be a valid path"),
        Err(_) => {
            // We're running cargo directly on a dev machine:
            // Get variables to determine paths
            let os = std::env::var("CARGO_CFG_TARGET_OS").unwrap();
            let arch = std::env::var("CARGO_CFG_TARGET_ARCH")
                .unwrap()
                .replace("x86_64", "x64");
            let platform = format!("{}-{}-{}", os, arch, get_build_profile_name());
            root.join("bin").join(platform).join("search-community")
        }
    };

    let lib_path = root.join(binroot).join("src").join("c2rust");
    let lib_name = cfg!(target_os = "windows")
        .then(|| "c2rust.lib")
        .unwrap_or("libc2rust.a");

    // check if the static library exists and quit with an error if it does not
    let full_path = lib_path.join(lib_name);
    if !full_path.exists() {
        println!(
            "cargo::error=Could not find the static library at {}",
            full_path.display()
        );
    }

    /*
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap();
    if target_os != "macos" {
        println!("cargo:rustc-link-arg=-Wl,--unresolved-symbols=ignore-in-object-files");
    }
    */

    // configure linker with libname and path
    println!("cargo:rustc-link-lib=static=c2rust");
    println!("cargo:rustc-link-search=native={}", lib_path.display());

    // rerun if cmake file changes
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

    // the profiles are named `debug`, `release`, `optimised_test` and `profiling`
    // the latter are mapped to `release` from the outer build script
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
