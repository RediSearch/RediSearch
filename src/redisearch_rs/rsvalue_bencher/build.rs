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
        ("deps/fast_float", "fast_float_strtod_static"),
        ("src/util/mempool", "mempool"),
        ("src/value", "value"),
    ]);

    // Generate C bindings - fail build if this doesn't work
    let root = git_root().expect("Could not find git root");
    let ii_header = root.join("src").join("value").join("value.h");
    generate_c_bindings(vec![ii_header], ".*/value.h", false)?;

    Ok(())
}
