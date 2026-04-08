/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

fn main() {
    build_utils::bind_foreign_c_symbols();
    compile_bench_shim();
}

fn compile_bench_shim() {
    use build_utils::repository_root;

    let root = repository_root().expect("Could not find repository root");
    let src = root.join("src");
    let deps = root.join("deps");

    cc::Build::new()
        .file("benches/bench_shim.c")
        .include(&src)
        .include(src.join("iterators"))
        .include(deps.join("VectorSimilarity").join("src"))
        .include(deps.join("rmalloc"))
        .include(src.join("redisearch_rs").join("headers"))
        .include(src.join("buffer"))
        .include(src.join("ttl_table"))
        .include(src.join("trie"))
        .compile("bench_shim");

    println!("cargo:rerun-if-changed=benches/bench_shim.c");
}
