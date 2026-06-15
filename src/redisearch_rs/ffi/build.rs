/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use build_utils::{repository_root, rerun_if_c_changes};

/// One C header bindgen consumes, plus the FFI symbols Rust code reaches
/// for from that file. The build script verifies at startup that every
/// listed symbol appears in the file at `path` — typos and upstream renames
/// fail the build with a clear error instead of leaking into bindings.
struct HeaderAllowlist {
    /// Repository-relative path, e.g. `src/buffer/buffer.h`.
    path: &'static str,
    fns: &'static [&'static str],
    types: &'static [&'static str],
    vars: &'static [&'static str],
}

/// Bindgen inputs. Each entry both feeds clang and constrains which symbols
/// bindgen emits. Sorted alphabetically by `path` for easy scanning.
const HEADERS: &[HeaderAllowlist] = &[
    // RSE: `ThrottleCB` and `VecSimParamsDisk` are consumed by the disk
    // vector-index API exposed through `src/search_disk_api.h`.
    HeaderAllowlist {
        path: "deps/VectorSimilarity/src/VecSim/vec_sim_common.h",
        fns: &[],
        types: &["ThrottleCB", "VecSimParamsDisk"],
        vars: &[],
    },
    HeaderAllowlist {
        path: "deps/hiredis/sds.h",
        fns: &["sdscatlen", "sdsnewlen", "sdsfree"],
        types: &[],
        vars: &[],
    },
    HeaderAllowlist {
        path: "src/aggregate/aggregate.h",
        fns: &["AREQ_CheckTimedOut"],
        types: &[],
        vars: &[],
    },
    HeaderAllowlist {
        path: "src/aggregate/reducer.h",
        fns: &[],
        types: &["Reducer", "ReducerOptions"],
        vars: &[],
    },
    HeaderAllowlist {
        path: "src/buffer/buffer.h",
        fns: &["Buffer_Free", "Buffer_Grow"],
        types: &["BufferReader", "BufferWriter"],
        vars: &[],
    },
    HeaderAllowlist {
        path: "src/config.h",
        fns: &[],
        types: &[],
        vars: &["RM_SCAN_KEY_API_FIX", "RSGlobalConfig"],
    },
    HeaderAllowlist {
        path: "src/doc_table.h",
        fns: &[
            "DMD_Free",
            "DocTable_Exists",
            "DocTable_GetId",
            "DocTable_Put",
        ],
        types: &[],
        vars: &[],
    },
    HeaderAllowlist {
        path: "src/fork_gc.h",
        fns: &[],
        types: &["ForkGC"],
        vars: &[],
    },
    HeaderAllowlist {
        path: "src/forward_index.h",
        fns: &["InvertedIndex_WriteForwardIndexEntry"],
        types: &[],
        vars: &[],
    },
    HeaderAllowlist {
        path: "src/indexes.h",
        fns: &[
            "Indexes_Init",
            "Indexes_RemoveFromGlobals",
            "Spec_AddToDict",
        ],
        types: &[],
        vars: &["specDict_g", "specIdDict_g"],
    },
    HeaderAllowlist {
        path: "src/iterators/hybrid_reader.h",
        fns: &[
            "HybridIterator_GetChild",
            "HybridIterator_GetMaxBatchIteration",
            "HybridIterator_GetMaxBatchSize",
            "HybridIterator_GetNumIterations",
            "HybridIterator_GetSearchModeString",
            "HybridIterator_IsBatchMode",
        ],
        types: &[],
        vars: &[],
    },
    HeaderAllowlist {
        path: "src/iterators/iterator_api.h",
        fns: &[],
        types: &["QueryIterator"],
        vars: &[],
    },
    HeaderAllowlist {
        path: "src/iterators/optimizer_reader.h",
        fns: &[
            "OptimizerIterator_GetChild",
            "OptimizerIterator_GetOptimizationType",
        ],
        types: &[],
        vars: &[],
    },
    HeaderAllowlist {
        path: "src/json.h",
        fns: &[],
        types: &[],
        vars: &["RedisJSONAPI_MIN_API_VER", "japi", "japi_ver"],
    },
    HeaderAllowlist {
        path: "src/numeric_filter.h",
        fns: &["NewNumericFilter"],
        types: &[],
        vars: &[],
    },
    HeaderAllowlist {
        path: "src/obfuscation/hidden.h",
        fns: &[
            "HiddenString_CompareC",
            "HiddenString_Free",
            "HiddenString_GetUnsafe",
            "NewHiddenString",
        ],
        types: &[],
        vars: &[],
    },
    HeaderAllowlist {
        path: "src/obfuscation/obfuscation_api.h",
        fns: &["Obfuscate_Number", "Obfuscate_Text"],
        types: &[],
        vars: &[],
    },
    HeaderAllowlist {
        path: "src/query.h",
        fns: &["tag_strtolower"],
        types: &["QueryEvalCtx"],
        vars: &[],
    },
    HeaderAllowlist {
        path: "src/query_node.h",
        fns: &[],
        types: &[
            "QueryFuzzyNode",
            "QueryGeofilterNode",
            "QueryGeometryNode",
            "QueryIdFilterNode",
            "QueryLexRangeNode",
            "QueryMissingNode",
            "QueryNullNode",
            "QueryNumericNode",
            "QueryPhraseNode",
            "QueryPrefixNode",
            "QueryTagNode",
            "QueryTokenNode",
            "QueryVectorNode",
            "QueryVerbatimNode",
            "RSQueryNode",
        ],
        vars: &[],
    },
    HeaderAllowlist {
        path: "src/redis_index.h",
        fns: &["Redis_OpenInvertedIndex"],
        types: &[],
        vars: &[],
    },
    HeaderAllowlist {
        path: "src/redisearch.h",
        fns: &[],
        types: &["RSToken"],
        vars: &[],
    },
    HeaderAllowlist {
        path: "src/redismodule.h",
        fns: &[],
        // RSE: `RedisModuleIO` is referenced by the RDB save/load entry points
        // in `src/search_disk_api.h`.
        types: &["RedisModuleIO", "RedisModuleString"],
        vars: &[
            "REDISMODULE_ERR",
            "REDISMODULE_OK",
            "REDISMODULE_POSTPONED_ARRAY_LEN",
            "REDISMODULE_POSTPONED_LEN",
            "RedisModule_Alloc",
            "RedisModule_Free",
            "RedisModule_FreeString",
            "RedisModule_FreeThreadSafeContext",
            "RedisModule_GetDetachedThreadSafeContext",
            "RedisModule_GetThreadSafeContext",
            "RedisModule_InfoAddFieldCString",
            // RSE: u64 field writer used by `redisearch_disk`'s
            // `RedisModuleInfoCtx`-backed INFO sink.
            "RedisModule_InfoAddFieldULongLong",
            "RedisModule_InfoAddSection",
            // RSE: open/close pair for nested dict fields, used by the same
            // INFO sink in `redisearch_disk`.
            "RedisModule_InfoBeginDictField",
            "RedisModule_InfoEndDictField",
            "RedisModule_Log",
            "RedisModule_ReplySetArrayLength",
            "RedisModule_ReplySetMapLength",
            "RedisModule_ReplyWithArray",
            "RedisModule_ReplyWithDouble",
            "RedisModule_ReplyWithEmptyArray",
            "RedisModule_ReplyWithLongLong",
            "RedisModule_ReplyWithMap",
            "RedisModule_ReplyWithSimpleString",
            "RedisModule_ReplyWithStringBuffer",
            "RedisModule_StringPtrLen",
        ],
    },
    HeaderAllowlist {
        path: "src/result_processor.h",
        fns: &["RPProfile_IncrementCount"],
        types: &["RPStatus"],
        vars: &[],
    },
    HeaderAllowlist {
        path: "src/rlookup_load_document.h",
        fns: &["loadIndividualKeys", "sdslen_rust"],
        types: &[],
        vars: &[],
    },
    HeaderAllowlist {
        path: "src/rules.h",
        fns: &["SchemaPrefixes_Free"],
        types: &[],
        vars: &["SchemaPrefixes_g"],
    },
    HeaderAllowlist {
        path: "src/score_explain.h",
        fns: &["SEDestroy"],
        types: &[],
        vars: &[],
    },
    HeaderAllowlist {
        path: "src/search_ctx.h",
        fns: &["NewSearchCtxC", "SearchCtx_Free"],
        types: &[],
        vars: &[],
    },
    // RSE: the entire disk API struct family lives in this header and is
    // consumed by `redisearch_disk` to bridge from C into the Rust storage
    // layer. None of these symbols are referenced by RediSearch itself.
    HeaderAllowlist {
        path: "src/search_disk_api.h",
        fns: &[],
        types: &[
            "AllocateDMDCallback",
            "AllocateKeyCallback",
            "AsyncPollResult",
            "AsyncReadResult",
            "BasicDiskAPI",
            "DocTableDiskAPI",
            "IndexDiskAPI",
            "MetricsDiskAPI",
            "RedisSearchDisk",
            "RedisSearchDiskAPI",
            "RedisSearchDiskAsyncReadPool",
            "RedisSearchDiskRdbState",
            "RedisSearchDiskSnapshot",
            "SearchDiskCompactionCallbacks",
            "SearchDiskWriteBatchHandle",
            "VectorDiskAPI",
        ],
        vars: &[],
    },
    HeaderAllowlist {
        path: "src/spec.h",
        fns: &[
            "IndexSpec_AcquireWriteLock",
            "IndexSpec_DecrementNumTerms",
            "IndexSpec_DecrementTrieTermCount",
            "IndexSpec_GetFieldWithLength",
            "IndexSpec_ParseC",
            "IndexSpec_ReleaseWriteLock",
            "IndexSpecCache_Decref",
            "IndexSpecRef_Promote",
            "IndexSpecRef_Release",
        ],
        types: &[],
        vars: &["isCrdt"],
    },
    HeaderAllowlist {
        path: "src/stopwords.h",
        fns: &["StopWordList_FreeGlobals"],
        types: &[],
        vars: &[],
    },
    HeaderAllowlist {
        path: "src/suffix.h",
        fns: &[],
        types: &[],
        vars: &["MIN_SUFFIX"],
    },
    HeaderAllowlist {
        path: "src/tag_index.h",
        fns: &["TagIndex_Ensure", "TagIndex_OpenIndex"],
        types: &[],
        vars: &[],
    },
    HeaderAllowlist {
        path: "src/trie/rune_util.h",
        fns: &["strToLowerRunes"],
        types: &[],
        vars: &["MAX_RUNE_STR_LEN"],
    },
    HeaderAllowlist {
        path: "src/trie/trie.h",
        fns: &["Trie_DecrementNumDocs"],
        types: &[],
        vars: &[],
    },
    HeaderAllowlist {
        path: "src/ttl_table/ttl_table.h",
        fns: &[
            "TimeToLiveTable_Add",
            "TimeToLiveTable_VerifyDocAndField",
            "TimeToLiveTable_VerifyDocAndFieldMask",
            "TimeToLiveTable_VerifyDocAndWideFieldMask",
            "TimeToLiveTable_VerifyInit",
            // Used by bench.
            "TimeToLiveTable_Destroy",
            "TimeToLiveTable_IsEmpty",
        ],
        types: &[],
        vars: &[],
    },
    HeaderAllowlist {
        path: "src/util/arr/arr.h",
        fns: &["array_free", "array_len_func", "array_new_sz"],
        types: &[],
        vars: &[],
    },
    HeaderAllowlist {
        path: "src/util/dict/dict.h",
        fns: &[
            "RS_dictAdd",
            "RS_dictDelete",
            "RS_dictFetchValue",
            "RS_dictRelease",
        ],
        types: &[],
        vars: &[],
    },
    HeaderAllowlist {
        path: "src/util/references.h",
        fns: &["StrongRef_Get"],
        types: &[],
        vars: &[],
    },
    HeaderAllowlist {
        path: "src/util/strconv.h",
        fns: &["unicode_tolower_fn"],
        types: &[],
        vars: &[],
    },
    HeaderAllowlist {
        path: "src/wildcard/wildcard.h",
        fns: &["Wildcard_RemoveEscape"],
        types: &[],
        vars: &[],
    },
];

/// Generated headers (from `src/redisearch_rs/headers/`) that bindgen is allowed
/// to consume transitively. Every entry is an explicit decision: each new entry
/// re-introduces an edge from a bindgen-input C header to cheadergen output,
/// which means a malformed regeneration of that file will wedge the build.
/// Prefer adding a forward declaration in the C header instead — see e.g.
/// `src/redis_index.h` for `InvertedIndex` or `src/spec.h` for `QueryError`.
const PERMITTED_GENERATED_HEADERS: &[&str] = &[
    // Fundamental type aliases (t_docId, t_fieldIndex, t_fieldMask) used
    // across both C code and Rust-generated headers. Bindgen emits these
    // as `pub type t_docId = u64` etc.; user code should import the
    // canonical names (`DocId`, `FieldIndex`, `FieldMask`) from `rqe_core`.
    "rqe_core.h",
    // `DocumentType` is used as a bitfield in `RSDocumentMetadata`
    // (src/redisearch.h) — the full enum definition is required.
    "document_rs.h",
    // `FieldExpirationPredicate` is taken by value in TTL table function
    // signatures (src/ttl_table/ttl_table.h).
    "field.h",
    // `RSOffsetVector` is embedded by value in `RSByteOffsets`
    // (src/byte_offsets.h) — the struct body is required.
    "inverted_index.h",
    // Required by `inverted_index.h`.
    "index_result_rs.h",
    // Carries `QueryError`'s actual struct body (kept separate from
    // `query_error_ffi.h` only because cheadergen emits the type and the
    // function surface in two files).
    "query_error.h",
    // `QEFlags` is included by `src/aggregate/aggregate.h`.
    "query_flags.h",
    // `QueryNodeType` is taken by value in `src/query_node.h`.
    "query_node_type.h",
    // `geo_index.h` includes `geo_ffi.h` for the Rust geo function declarations.
    "geo_ffi.h",
    // `src/field_spec.h`, `src/info/index_error.h`, and `src/util/timeout.h`
    // contain `static inline` functions calling `QueryError_GetDisplayableError`
    // / `QueryError_SetCode` etc., so they need the function declarations.
    "query_error_ffi.h",
    // `QEFlags` and the `QEFlag_*` named constants are required by
    // `src/aggregate/aggregate.h` (pulled in for `AREQ_CheckTimedOut`).
    "query_flags.h",
    // `QueryProcessingCtx` is embedded by value in `src/pipeline/pipeline.h`
    // and `src/aggregate/aggregate.h`. Brings `rs_wall_clock.h` into bindgen's
    // closure too, which is needed by `ffi::QueryProcessingCtx` (defined in
    // `ffi/src/lib.rs`).
    "result_processor_ffi.h",
    // `RSValueType` and friends are required by `src/aggregate/aggregate.h`
    // (pulled in transitively for `AREQ_CheckTimedOut`).
    "value_ffi.h",
    // `enum IteratorType` is used by value in `src/iterators/iterator_api.h`.
    "rqe_iterator_type.h",
    // `IteratorsConfig` is embedded by value in `RSGlobalConfig` (src/config.h).
    "rqe_iterators.h",
    // `src/rlookup.h` has `static inline` accessors that dereference
    // `RLookupKey` / `RLookupIterator` fields, so the full struct bodies in
    // these three are required. (`rlookup_ffi.h` includes `search_result_rs.h`
    // which in turn includes `rlookup.h`.)
    "rlookup.h",
    "rlookup_ffi.h",
    "search_result_rs.h",
    // `RSSortingVector` (a typedef of `ThinVec_SharedValue__u64`) is embedded
    // by value in `RSDocumentMetadata` (src/redisearch.h).
    "sorting_vector.h",
    // `aggregate.h` includes `value_ffi.h`; reachable via
    // `optimizer_reader.h` -> `query_optimizer.h` -> `aggregate.h`.
    "value_ffi.h",
    // `src/byte_offsets.h` defines `static inline` functions that call
    // `NewVarintVectorWriter` / `VVW_Free` / `VVW_Write`. The whole file is
    // small (one opaque type + a handful of functions).
    "varint_ffi.h",
];

/// Types defined in Rust (re-exported from their owning crate in
/// `ffi/src/lib.rs`). Blocklisted so bindgen doesn't emit a conflicting
/// opaque stub when it sees a forward reference.
const BLOCKLIST_TYPES: &[&str] = &[
    "IteratorType",
    "QueryNodeType",
    "QASTValidationFlagsSet",
    "QueryNodeOptions",
    "QueryProcessingCtx", // defined directly in `ffi/src/lib.rs`
    "RSQueryTerm",
    "RSTokenFlags",
];

/// cheadergen-produced headers whose types are provided from the owning
/// Rust crate via `pub use` in `ffi/src/lib.rs`, not from bindgen's output.
const BLOCKLIST_FILES: &[&str] = &[
    ".*/document_rs.h",
    ".*/numeric_range_tree.h",
    ".*/query_node_type.h",
    ".*/query_term.h",
    ".*/query_term_ffi.h",
    ".*/rqe_iterator_type.h",
];

fn main() {
    let root =
        repository_root().expect("Could not find repository root for static library linking");
    let src = root.join("src");
    let deps = root.join("deps");
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    // On non-macOS platforms the default linker rejects undefined symbols.
    // Several symbols exposed by the C code aren't actually invoked from our
    // benchmarks; relax the check so we don't have to ship dummy definitions.
    if env::var("CARGO_CFG_TARGET_OS").unwrap() != "macos" {
        println!("cargo:rustc-link-arg=-Wl,--unresolved-symbols=ignore-in-object-files");
    }

    // Stage permitted generated headers into an OUT_DIR subdirectory and put
    // only that subdirectory on bindgen's clang include path. Generated
    // headers not on the allowlist are unreachable from bindgen, so a
    // malformed regeneration of an unlisted header cannot wedge the build.
    let permitted_dir = out_dir.join("permitted_generated_headers");
    fs::create_dir_all(&permitted_dir).expect("create permitted_generated_headers");
    let cheadergen_dir = src.join("redisearch_rs").join("headers");
    for hdr in PERMITTED_GENERATED_HEADERS {
        let src_path = cheadergen_dir.join(hdr);
        let dst_path = permitted_dir.join(hdr);
        fs::copy(&src_path, &dst_path).unwrap_or_else(|e| {
            panic!("copy {} -> {}: {e}", src_path.display(), dst_path.display())
        });
        println!("cargo:rerun-if-changed={}", src_path.display());
    }

    let includes = [
        src.clone(),
        deps.clone(),
        permitted_dir.clone(),
        src.join("inverted_index"),
        deps.join("VectorSimilarity").join("src"),
        src.join("buffer"),
        src.join("ttl_table"),
        src.join("trie"),
        deps.join("rmalloc"),
    ];

    let mut bindings = bindgen::Builder::default();
    for entry in HEADERS {
        let abs = root.join(entry.path);
        verify_symbols(&abs, entry);
        bindings = bindings.header(abs.display().to_string());
        println!("cargo:rerun-if-changed={}", abs.display());

        for func in entry.fns {
            bindings = bindings.allowlist_function(func);
        }
        for ty in entry.types {
            bindings = bindings.allowlist_type(ty);
        }
        for var in entry.vars {
            bindings = bindings.allowlist_var(var);
        }
    }
    for include in &includes {
        bindings = bindings.clang_arg(format!("-I{}", include.display()));
        // Don't watch the staged copies under OUT_DIR: the build script writes
        // them itself, so their mtimes will always be post-date cargo's fingerprint
        // reference and would force a rebuild on every invocation. The source
        // generated headers are already watched above.
        if include != &permitted_dir {
            let _ = rerun_if_c_changes(include);
        }
    }
    // `_GNU_SOURCE` makes `<stdio.h>` declare `asprintf`/`vasprintf`, which
    // `deps/rmalloc/rmalloc.h` uses.
    bindings = bindings.clang_arg("-D_GNU_SOURCE");

    for ty in BLOCKLIST_TYPES {
        bindings = bindings.blocklist_type(ty);
    }
    for file in BLOCKLIST_FILES {
        bindings = bindings.blocklist_file(file);
    }

    bindings
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file(out_dir.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}

/// Verify that every symbol claimed in `entry` actually appears (as a
/// whole identifier) in the file at `abs`. Catches typos and upstream
/// renames at `cargo build` time.
fn verify_symbols(abs: &Path, entry: &HeaderAllowlist) {
    let content = fs::read_to_string(abs)
        .unwrap_or_else(|e| panic!("ffi build.rs: read {}: {e}", abs.display()));
    let check = |sym: &str, kind: &str| {
        if !contains_word(&content, sym) {
            panic!(
                "ffi build.rs: {kind} `{sym}` claimed to be in `{}` but not found",
                entry.path,
            );
        }
    };
    for f in entry.fns {
        check(f, "function");
    }
    for t in entry.types {
        check(t, "type");
    }
    for v in entry.vars {
        check(v, "variable");
    }
}

/// True when `sym` appears in `haystack` surrounded by non-identifier
/// characters (so `Buffer` doesn't match inside `BufferReader`).
fn contains_word(haystack: &str, sym: &str) -> bool {
    let bytes = haystack.as_bytes();
    let mut start = 0;
    while let Some(idx) = haystack[start..].find(sym) {
        let pos = start + idx;
        let end = pos + sym.len();
        let left_ok = pos == 0 || !is_ident_byte(bytes[pos - 1]);
        let right_ok = end == bytes.len() || !is_ident_byte(bytes[end]);
        if left_ok && right_ok {
            return true;
        }
        start = pos + 1;
    }
    false
}

const fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}
