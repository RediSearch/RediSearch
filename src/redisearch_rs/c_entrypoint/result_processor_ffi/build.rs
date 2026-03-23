/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use build_utils::run_cbindgen;

fn main() {
    run_cbindgen("../../headers/result_processor_rs.h").unwrap();

    #[cfg(feature = "unittest")]
    build_utils::bind_foreign_c_symbols();
}
