/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use build_utils::link_static_libraries;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Link the static libraries that contain our array functions
    link_static_libraries(&[
        ("src/util/arr", "arr"),
    ]);

    Ok(())
}
