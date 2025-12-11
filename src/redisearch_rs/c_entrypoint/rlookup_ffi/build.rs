/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use build_utils::{link_static_libraries, run_cbinden};

fn main() {
    link_static_libraries(&[
        ("src/util/arr", "arr"),
        ("src/util/mempool", "mempool"),
        ("src/iterators", "iterators"),
        ("src/buffer", "buffer"),
        ("src/index_result", "index_result"),
        ("src/value", "value"),
    ]);

    run_cbinden("../../headers/rlookup_rs.h").unwrap();
}
