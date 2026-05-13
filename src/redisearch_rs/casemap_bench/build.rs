/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Compiles the two libnu translation units needed by the bench:
//! `tofold.c` (`nu_tofold` — case-fold table lookup, the same tables that
//! drive `runeFold` in `src/trie/rune_util.c`) and `tolower.c` (`nu_tolower`
//! — lowercase table lookup, the production text-normalisation path used by
//! `unicode_tolower` in `src/util/strconv.h`).
//!
//! No UTF-8 reader/writer is needed: the bench iterates input codepoints with
//! Rust's native UTF-8 decoder and reads libnu's null-terminated mapping
//! buffers via [`std::ffi::CStr`].

use std::path::PathBuf;

fn main() {
    let root = repository_root().expect("could not locate repository root");
    let libnu_dir = root.join("deps").join("libnu");
    let tofold = libnu_dir.join("tofold.c");
    let tolower = libnu_dir.join("tolower.c");

    for src in [&tofold, &tolower] {
        println!("cargo::rerun-if-changed={}", src.display());
    }
    for hdr in [
        "casemap.h",
        "casemap_internal.h",
        "udb.h",
        "mph.h",
        "config.h",
        "defines.h",
    ] {
        println!("cargo::rerun-if-changed={}", libnu_dir.join(hdr).display());
    }
    for table in ["_tofold.c", "_tolower.c"] {
        println!(
            "cargo::rerun-if-changed={}",
            libnu_dir.join("gen").join(table).display()
        );
    }

    cc::Build::new()
        .file(&tofold)
        .file(&tolower)
        .include(&libnu_dir)
        .warnings(false)
        .compile("nu_casemap_min");
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
