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
        ("src/buffer", "buffer"),
        ("src/util/arr", "arr"),
    ]);

    // Generate C bindings - fail build if this doesn't work
    let root = git_root().expect("Could not find git root");
    let ii_header = root
        .join("src")
        .join("inverted_index")
        .join("inverted_index.h");
    generate_c_bindings(vec![ii_header], ".*/inverted_index.h", false)?;

    Ok(())
}
