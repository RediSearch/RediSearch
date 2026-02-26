load("@buck2tf//rust:bindgen.bzl", "rust_bindgen")

# C headers to generate Rust FFI bindings for
_FFI_HEADERS = [
    "src/buffer/buffer.h",
    "src/config.h",
    "src/doc_table.h",
    "src/forward_index.h",
    "src/index_result/index_result.h",
    "src/iterators/intersection_iterator.h",
    "src/iterators/inverted_index_iterator.h",
    "src/iterators/not_iterator.h",
    "src/iterators/optional_iterator.h",
    "src/json.h",
    "src/numeric_index.h",
    "src/obfuscation/hidden.h",
    "src/query.h",
    "src/redis_index.h",
    "src/redisearch.h",
    "src/result_processor.h",
    "src/rlookup.h",
    "src/rules.h",
    "src/score_explain.h",
    "src/search_ctx.h",
    "src/search_disk_api.h",
    "src/search_result.h",
    "src/sortable.h",
    "src/spec.h",
    "src/stopwords.h",
    "src/trie/trie.h",
    "src/trie/trie_type.h",
    "src/ttl_table/ttl_table.h",
    "src/util/arr/arr.h",
    "src/util/dict/dict.h",
    "src/util/references.h",
    "src/value/value.h"
]

rust_bindgen(
    name = "c_bindings",
    headers = _FFI_HEADERS + ["//deps:redismodule.h", "//deps:rmutil/vector.h"],
    blocklist_files = [
        "src/redisearch_rs/headers/document_rs.h",
        "src/redisearch_rs/headers/numeric_range_tree.h",
    ],
    include_directories = ["src", "src/redisearch_rs/headers", "src/inverted_index", "src/buffer", "src/ttl_table", "src/trie"],
    deps = [
        "//deps:RedisModulesSDK",
        "//deps:hiredis",
        "//deps:rmutil",
        "//deps:rmalloc",
        "//deps:libnu",
        "//deps:fast_float",
        "//deps:VectorSimilarity",
        "//deps:thpool",
        "//deps:geohash"
    ],
    visibility = ["PUBLIC"],
)
