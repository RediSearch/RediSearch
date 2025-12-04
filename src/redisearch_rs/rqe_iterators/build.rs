/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Link the static libraries that contain our array functions
    #[cfg(feature = "unittest")]
    {
        build_utils::link_static_libraries(&[
            ("deps/libnu", "libnu"),
            ("deps/rmutil", "rmutil"),
            ("deps/thpool", "thpool"),
            ("hiredis", "hiredisd"),
            ("src/aggregate/expr", "aggregate_expr"),
            ("src/aggregate/functions", "aggregate_functions"),
            ("src/doc_table", "doc_table"),
            ("src/hll", "hll"),
            ("src/index_result", "index_result"),
            ("src/info/global_stats", "global_stats"),
            ("src/info/index_error", "index_error"),
            ("src/iterators", "iterators"),
            ("src/language", "language"),
            ("src/alias", "alias"),
            ("src/field_spec", "field_spec"),
            ("src/numeric_index", "numeric_index"),
            ("src/obfuscation", "obfuscation"),
            ("src/redis_index", "redis_index"),
            ("src/rlookup", "rlookup"),
            ("src/rules", "rules"),
            ("src/spec", "spec"),
            ("src/trie", "trie"),
            ("src/ttl_table", "ttl_table"),
            ("src/util/arr", "arr"),
            ("src/util/block_alloc", "block_alloc"),
            ("src/util/dict", "dict"),
            ("src/util/hash", "redisearch-hash"),
            ("src/util/heap_doubles", "heap_doubles"),
            ("src/util/mempool", "mempool"),
            ("src/util/references", "references"),
            ("src/util/workers", "workers"),
            ("src/value", "value"),
            ("src/vector_index", "vector_index"),
            ("src/cursor", "cursor"),
            ("deps/VectorSimilarity/src/VecSim", "VectorSimilarity"),
        ]);
    }

    Ok(())
}
