/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Compiles the minimum libnu translation unit required to expose `nu_tofold`.
//!
//! Only `deps/libnu/tofold.c` is compiled. All helpers it transitively uses
//! (`_nu_to_something`, `nu_udb_lookup`, `nu_mph_*`) are `static inline` and
//! are inlined directly into the `nu_tofold` symbol, so no other libnu
//! translation units need to be linked.

use std::path::PathBuf;

fn main() {
    let root = repository_root().expect("could not locate repository root");
    let libnu_dir = root.join("deps").join("libnu");
    let tofold = libnu_dir.join("tofold.c");

    println!("cargo::rerun-if-changed={}", tofold.display());
    println!(
        "cargo::rerun-if-changed={}",
        libnu_dir.join("casemap.h").display()
    );
    println!(
        "cargo::rerun-if-changed={}",
        libnu_dir.join("casemap_internal.h").display()
    );
    println!(
        "cargo::rerun-if-changed={}",
        libnu_dir.join("udb.h").display()
    );
    println!(
        "cargo::rerun-if-changed={}",
        libnu_dir.join("mph.h").display()
    );
    println!(
        "cargo::rerun-if-changed={}",
        libnu_dir.join("config.h").display()
    );
    println!(
        "cargo::rerun-if-changed={}",
        libnu_dir.join("gen").join("_tofold.c").display()
    );

    cc::Build::new()
        .file(&tofold)
        .include(&libnu_dir)
        .warnings(false)
        .compile("nu_tofold");
}

fn repository_root() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let mut path = std::env::current_dir()?;
    while !(path.join(".git").exists() || path.join(".jj").exists()) {
        path = path
            .parent()
            .ok_or("could not find repository root")?
            .to_path_buf();
    }
    Ok(path)
}
