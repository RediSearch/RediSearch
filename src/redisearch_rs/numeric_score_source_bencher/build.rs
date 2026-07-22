/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

fn main() {
    // Emit optimizer_shim before redisearch_c_bundle: static archives resolve
    // left-to-right, and the shim's symbols are defined in libredisearch_c_bundle.a.
    //
    // Drives the real C OptimizerIterator from libredisearch_c_bundle.a, as a
    // faithful counterpart to the Rust NumericTopKIterator.
    build_utils::compile_c_bench_shim("benches/optimizer_shim.c");
    build_utils::bind_foreign_c_symbols();
}
