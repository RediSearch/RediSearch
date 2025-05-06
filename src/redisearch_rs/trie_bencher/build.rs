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

fn main() {
    let root = git_root();

    // Construct the correct folder path based on OS and architecture
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap();
    let lib_dir = {
        let target_arch = env::var("CARGO_CFG_TARGET_ARCH").unwrap();
        let target_arch = match target_arch.as_str() {
            "aarch64" => "arm64v8",
            "x86_64" => "x64",
            _ => &target_arch,
        };

        root.join(format!(
            "bin/{target_os}-{target_arch}-release/search-community/deps/triemap"
        ))
    };

    assert!(std::fs::exists(lib_dir.join("libtrie.a")).unwrap());
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
    println!("cargo:rustc-link-lib=static=trie");
    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    println!(
        "cargo:rerun-if-changed={}",
        lib_dir.join("libtrie.a").display()
    );

    let redis_modules = root.join("deps").join("RedisModulesSDK");
    let src = root.join("src");
    let deps = root.join("deps");
    let bindings = bindgen::Builder::default()
        .header(
            root.join("deps")
                .join("triemap")
                .join("triemap.h")
                .to_str()
                .unwrap(),
        )
        .clang_arg(format!("-I{}", src.display()))
        .clang_arg(format!("-I{}", deps.display()))
        .clang_arg(format!("-I{}", redis_modules.display()))
        .generate()
        .expect("Unable to generate bindings");
    // Re-run the build script if any of the files in those directories change
    println!("cargo:rerun-if-changed={}", src.display());
    println!("cargo:rerun-if-changed={}", deps.display());
    println!("cargo:rerun-if-changed={}", redis_modules.display());

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
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
