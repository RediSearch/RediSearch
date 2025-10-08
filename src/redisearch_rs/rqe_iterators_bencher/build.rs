/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use build_utils::{generate_c_bindings, git_root, link_static_libraries};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Always link the static libraries, independent of bindgen
    link_static_libraries(&[
        ("src/inverted_index", "inverted_index"),
        ("src/iterators", "iterators"),
    ]);

    // Compile the wildcard iterator benchmark C file
    let root = git_root().expect("Could not find git root");
    cc::Build::new()
        .file("src/benchers/c/wildcard.c")
        .include(root.join("src").join("wildcard"))
        .opt_level(3)
        .compile("wildcard_iterator_benchmark");

    // Generate C bindings - fail build if this doesn't work
    let headers = [
        "iterator_api.h",
        "empty_iterator.h",
        "idlist_iterator.h",
        "wildcard_iterator.h",
    ]
    .iter()
    .map(|h| root.join("src").join("iterators").join(h))
    .collect::<Vec<_>>();
    generate_c_bindings(headers, ".*/iterators/.*.h", true)?;

    Ok(())
}
