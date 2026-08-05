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
    compile_optimizer_shim();
    build_utils::bind_foreign_c_symbols();
}

fn compile_optimizer_shim() {
    use build_utils::repository_root;

    let root = repository_root().expect("Could not find repository root");
    let src = root.join("src");
    let deps = root.join("deps");

    cc::Build::new()
        // Drives the real C OptimizerIterator from libredisearch_c_bundle.a, as a
        // faithful counterpart to the Rust NumericTopKIterator.
        .file("benches/optimizer_shim.c")
        // Silence warnings originating from transitively-included RediSearch
        // headers (static helpers, sign-compare in inline funcs, etc.) — they
        // are not actionable from this shim and the main CMake build already
        // suppresses them.
        .flag_if_supported("-Wno-unused-function")
        .flag_if_supported("-Wno-unused-variable")
        .flag_if_supported("-Wno-unused-parameter")
        .flag_if_supported("-Wno-sign-compare")
        .include(&src)
        // `deps` (rmutil/*) and RedisModulesSDK (redismodule.h) are pulled in
        // transitively by spec.h / search_ctx.h for the optimizer shim.
        .include(&deps)
        .include(deps.join("RedisModulesSDK"))
        .include(src.join("iterators"))
        .include(deps.join("VectorSimilarity").join("src"))
        .include(deps.join("rmalloc"))
        .include(src.join("redisearch_rs").join("headers"))
        .include(src.join("buffer"))
        .include(src.join("ttl_table"))
        .include(src.join("trie"))
        .compile("optimizer_shim");

    println!("cargo:rerun-if-changed=benches/optimizer_shim.c");
}
