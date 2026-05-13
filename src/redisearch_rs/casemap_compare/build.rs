/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Compiles the minimum libnu translation units required to expose
//! `nu_tofold` (case-fold table lookup), `nu_tolower` (lowercase table
//! lookup, the production text-normalisation path used by `unicode_tolower`
//! in `src/util/strconv.h`), and `nu_utf8_write` (runtime UTF-8 encoder,
//! which routes 4-byte codepoints through the suspect `b4_utf8` helper in
//! `utf8_internal.h`).
//!
//! The fold helpers are `static inline` and inline directly into the
//! `nu_tofold`/`nu_tolower` symbols. `nu_utf8_write` requires
//! `NU_WITH_UTF8_WRITER` to be defined for its body to compile;
//! `deps/libnu/config.h` defines `NU_WITH_EVERYTHING` unconditionally so
//! `NU_WITH_TOFOLD`/`NU_WITH_TOLOWER` are already on.

use std::path::PathBuf;

fn main() {
    let root = repository_root().expect("could not locate repository root");
    let libnu_dir = root.join("deps").join("libnu");
    let tofold = libnu_dir.join("tofold.c");
    let tolower = libnu_dir.join("tolower.c");
    let utf8 = libnu_dir.join("utf8.c");

    for src in [&tofold, &tolower, &utf8] {
        println!("cargo::rerun-if-changed={}", src.display());
    }
    for hdr in [
        "casemap.h",
        "casemap_internal.h",
        "udb.h",
        "mph.h",
        "config.h",
        "utf8.h",
        "utf8_internal.h",
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
        .file(&utf8)
        .define("NU_WITH_UTF8_WRITER", None)
        .include(&libnu_dir)
        .warnings(false)
        .compile("nu_libnu_min");
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
