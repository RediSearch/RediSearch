load("@buck2tf//rust:bindgen.bzl", "rust_bindgen")

cxx_library(
    name = "RediSearch",
    headers =
    { h: h for h in glob(["src/**/*.h", "src/**/*.hpp"], exclude = ["src/redisearch_rs/headers/**"]) }
    | {
        "document_rs.h": "//src/redisearch_rs:document_ffi_header[header]",
        "idf.h": "//src/redisearch_rs:idf_ffi_header[header]",
        "inverted_index.h": "//src/redisearch_rs:inverted_index_ffi_header[header]",
        "iterators_rs.h": "//src/redisearch_rs:iterators_ffi_header[header]",
        "module_init.h": "//src/redisearch_rs:module_init_ffi_header[header]",
        "numeric_range_tree.h": "//src/redisearch_rs:numeric_range_tree_ffi_header[header]",
        "query_error.h": "//src/redisearch_rs:query_error_ffi_header[header]",
        "query_term.h": "//src/redisearch_rs:query_term_ffi_header[header]",
        "result_processor_rs.h": "//src/redisearch_rs:result_processor_ffi_header[header]",
        # "rlookup_rs.h": "//src/redisearch_rs:rlookup_ffi_header[header]",
        "search_result_rs.h": "//src/redisearch_rs:search_result_ffi_header[header]",
        "slots_tracker.h": "//src/redisearch_rs:slots_tracker_ffi_header[header]",
        "sorting_vector.h": "//src/redisearch_rs:sorting_vector_ffi_header[header]",
        "thin_vec.h": "//src/redisearch_rs:thin_vec_ffi_header[header]",
        "triemap.h": "//src/redisearch_rs:triemap_ffi_header[header]",
        "types_rs.h": "//src/redisearch_rs:types_ffi_header[header]",
        # "value.h": "//src/redisearch_rs:value_ffi_header[header]",
        "varint.h": "//src/redisearch_rs:varint_ffi_header[header]",
    },
    srcs = glob(["src/**/*.c", "src/**/*.cpp"]),
    include_directories = [
        "src",
        "src/coord",
        "src/buffer",
        "src/value",
    ],
    deps = [
        "//deps:rmalloc",
        "//deps:thpool",
        "//deps:RedisModulesSDK",
        "//deps:rmutil",
        "//deps:hiredis",
        "//deps:VectorSimilarity",
        "//deps:libuv",
        "//deps:libnu",
        "//deps:fast_float",
        "//deps:snowball",
        "//deps:friso",
        "//deps:geohash",
        "//deps:miniz",
        "//deps:phonetics",
        "//deps:openssl",
        "//deps:cndict",
        "buck2tf//boost:boost",
        "//src/redisearch_rs:redisearch_rs",
        # "//src/redisearch_rs:redis-module[staticlib]",
    ],
    lang_compiler_flags = {
        "cxx": ["-std=c++20"],
    },
    preprocessor_flags = [
        "-DREDISEARCH_MODULE_NAME=\"${MODULE_NAME}\"",
        "-DGIT_VERSPEC=\"${GIT_VERSPEC}\"",
        "-DGIT_SHA=\"${GIT_SHA}\"",
        "-DREDISMODULE_SDK_RLEC",
        "-D_GNU_SOURCE"
    ],
    visibility = ["PUBLIC"],
)

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
    blocklist_types = [
        "RSQueryTerm",
        "RSTokenFlags"
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
