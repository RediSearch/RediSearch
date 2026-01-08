/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use build_utils::{generate_c_bindings, git_root, link_dynamic_library};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Link the dynamic library that bundles all C dependencies.
    // This is simpler than linking multiple static libraries and allows
    // undefined symbols (like RedisModule_*) to be resolved at runtime.
    link_dynamic_library("src", "redisearch_c");
    println!("cargo:rustc-link-lib=stdc++");

    // Compile the wildcard iterator benchmark C file
    let root = git_root().expect("Could not find git root");

    // Generate C bindings - fail build if this doesn't work
    let mut headers = [
        "iterator_api.h",
        "inverted_index_iterator.h",
        "not_iterator.h",
        "optional_iterator.h",
        "intersection_iterator.h",
    ]
    .iter()
    .map(|h| root.join("src").join("iterators").join(h))
    .collect::<Vec<_>>();

    // Add the Rust-generated iterators header
    headers.push(root.join("src/redisearch_rs/headers/iterators_rs.h"));

    generate_c_bindings(headers, ".*/iterators/.*.h|.*/headers/iterators_rs.h")?;

    Ok(())
}
