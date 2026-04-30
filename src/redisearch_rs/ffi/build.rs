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
use std::path::PathBuf;

use build_utils::{repository_root, rerun_if_c_changes};

/// Generated headers (from `src/redisearch_rs/headers/`) that bindgen is allowed
/// to consume transitively. Every entry is an explicit decision: each new entry
/// re-introduces an edge from a bindgen-input C header to cheadergen output,
/// which means a malformed regeneration of that file will wedge the build.
/// Prefer adding a forward declaration in the C header instead — see e.g.
/// `src/redis_index.h` for `InvertedIndex` or `src/spec.h` for `QueryError`.
const PERMITTED_GENERATED_HEADERS: &[&str] = &[
    // `DocumentType` is used as a bitfield in `RSDocumentMetadata`
    // (src/redisearch.h) — the full enum definition is required.
    "document_rs.h",
    // `FieldExpirationPredicate` is taken by value in TTL table function
    // signatures (src/ttl_table/ttl_table.h).
    "field.h",
    // `RSOffsetVector` is embedded by value in `RSByteOffsets`
    // (src/byte_offsets.h) — the struct body is required.
    "inverted_index.h",
    // Carries `QueryError`'s actual struct body (kept separate from
    // `query_error_ffi.h` only because cheadergen emits the type and the
    // function surface in two files).
    "query_error.h",
    // `QueryNodeType` is taken by value in `src/query_node.h`.
    "query_node_type.h",
    // `geo_index.h` includes `geo_ffi.h` for the Rust geo function declarations.
    "geo_ffi.h",
    // `src/field_spec.h`, `src/info/index_error.h`, and `src/util/timeout.h`
    // contain `static inline` functions calling `QueryError_GetDisplayableError`
    // / `QueryError_SetCode` etc., so they need the function declarations.
    "query_error_ffi.h",
    // `QueryProcessingCtx` is embedded by value in `src/pipeline/pipeline.h`
    // and `src/aggregate/aggregate.h`. Brings `rs_wall_clock.h` into bindgen's
    // closure too, which is needed by `ffi::QueryProcessingCtx` (defined in
    // `ffi/src/lib.rs`).
    "result_processor_ffi.h",
    // `enum IteratorType` is used by value in `src/iterators/iterator_api.h`.
    "rqe_iterator_type.h",
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
    // `src/byte_offsets.h` defines `static inline` functions that call
    // `NewVarintVectorWriter` / `VVW_Free` / `VVW_Write`. The whole file is
    // small (one opaque type + a handful of functions).
    "varint_ffi.h",
];

fn main() {
    let root =
        repository_root().expect("Could not find repository root for static library linking");

    // Construct the correct folder path based on OS and architecture
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap();

    // There are several symbols exposed by the C code that we don't
    // actually invoke (either directly or indirectly) in our benchmarks.
    // We provide a definition for the ones we need (e.g. Redis' allocation functions),
    // but we don't want to be forced to add dummy definitions for the ones we don't rely on.
    // We prefer to fail at runtime if we try to use a symbol that's undefined.
    // This is the default linker behaviour on macOS. On other platforms, the default
    // configuration is stricter: it exits with an error if any symbol is undefined.
    // We intentionally relax it here.
    if target_os != "macos" {
        println!("cargo:rustc-link-arg=-Wl,--unresolved-symbols=ignore-in-object-files");
    }

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    // Stage the explicitly permitted generated headers into an OUT_DIR
    // subdirectory and put only that subdirectory on bindgen's include path.
    // Generated headers not on the allowlist are unreachable from bindgen's
    // clang invocation, so a malformed regeneration of an unlisted header
    // cannot wedge the build.
    let permitted_dir = out_dir.join("permitted_generated_headers");
    fs::create_dir_all(&permitted_dir).expect("create permitted_generated_headers");
    for hdr in PERMITTED_GENERATED_HEADERS {
        let src_path = root
            .join("src")
            .join("redisearch_rs")
            .join("headers")
            .join(hdr);
        let dst_path = permitted_dir.join(hdr);
        fs::copy(&src_path, &dst_path).unwrap_or_else(|e| {
            panic!("copy {} -> {}: {e}", src_path.display(), dst_path.display())
        });
        println!("cargo:rerun-if-changed={}", src_path.display());
    }

    let includes = {
        let src = root.join("src");
        let deps = root.join("deps");

        let inverted_index = src.join("inverted_index");
        let vecsim = deps.join("VectorSimilarity").join("src");
        let buffer = src.join("buffer");
        let ttl_table = src.join("ttl_table");
        let trie = src.join("trie");
        let rmalloc = deps.join("rmalloc");

        [
            src,
            deps,
            permitted_dir,
            inverted_index,
            vecsim,
            buffer,
            ttl_table,
            trie,
            rmalloc,
        ]
    };

    let src = root.join("src");
    let deps = root.join("deps");
    let headers = [
        src.join("redismodule.h"),
        deps.join("hiredis").join("sds.h"),
        deps.join("rmutil").join("vector.h"),
        src.join("aggregate").join("reducer.h"),
        src.join("buffer/buffer.h"),
        src.join("config.h"),
        src.join("doc_table.h"),
        src.join("fork_gc.h"),
        src.join("forward_index.h"),
        src.join("geo_index.h"),
        deps.join("geohash").join("geohash.h"),
        src.join("index_result").join("index_result.h"),
        src.join("json.h"),
        src.join("obfuscation").join("hidden.h"),
        src.join("obfuscation").join("obfuscation_api.h"),
        src.join("query.h"),
        src.join("redis_index.h"),
        src.join("redisearch.h"),
        src.join("result_processor.h"),
        src.join("rlookup.h"),
        src.join("rlookup_load_document.h"),
        src.join("rules.h"),
        src.join("score_explain.h"),
        src.join("search_ctx.h"),
        src.join("search_disk.h"),
        src.join("search_disk_api.h"),
        src.join("search_result.h"),
        src.join("sortable.h"),
        src.join("spec.h"),
        src.join("stopwords.h"),
        src.join("numeric_filter.h"),
        src.join("tag_index.h"),
        src.join("trie").join("trie_node.h"),
        src.join("trie").join("trie.h"),
        src.join("ttl_table").join("ttl_table.h"),
        src.join("util").join("arr").join("arr.h"),
        src.join("util").join("dict").join("dict.h"),
        src.join("util").join("references.h"),
    ];

    let mut bindings = bindgen::Builder::default();

    for header in headers {
        bindings = bindings
            .header(header.display().to_string())
            .allowlist_file(header.display().to_string());

        println!("cargo:rerun-if-changed={}", header.display());
    }
    for include in includes {
        bindings = bindings.clang_arg(format!("-I{}", include.display()));
        // Re-run the build script if any of the C files in the included
        // directory changes
        let _ = rerun_if_c_changes(&include);
    }

    // Required so `<stdio.h>` declares `asprintf`/`vasprintf` (used by
    // `deps/rmalloc/rmalloc.h`) when bindgen parses the headers with clang.
    bindings = bindings.clang_arg("-D_GNU_SOURCE");

    bindings
        .blocklist_file(".*/document_rs.h")
        // numeric_range_tree.h is generated by Rust (cheadergen) and those types
        // are provided by the numeric_range_tree_ffi crate, not parsed from C
        .blocklist_file(".*/numeric_range_tree.h")
        // Provided by the query_term crate, not parsed from C
        .blocklist_file(".*/query_term_ffi.h")
        .blocklist_file(".*/query_term.h")
        // IteratorType is defined in Rust (rqe_iterator_type crate);
        // cheadergen generates rqe_iterator_type.h which is included by
        // iterator_api.h. We blocklist the generated header so bindgen
        // doesn't re-parse it, and re-export the Rust type from lib.rs.
        .blocklist_file(".*/rqe_iterator_type.h")
        // QueryNodeType is defined in Rust (query_node_type crate);
        // cheadergen generates query_node_type.h which is included by
        // query_node.h. We blocklist the generated header so bindgen
        // doesn't re-parse it, and re-export the Rust type from lib.rs.
        .blocklist_file(".*/query_node_type.h")
        .blocklist_file(".*/rqe_iterators.h")
        // Suppress bindgen's opaque-struct fallbacks for types it sees
        // referenced (in allowlisted headers) but whose definitions live in
        // the blocklisted Rust-generated headers above. The Rust side
        // re-exports these from their owning crates.
        .blocklist_type("RSQueryTerm")
        .blocklist_type("RSTokenFlags")
        .blocklist_type("IteratorType")
        .blocklist_type("QueryNodeType")
        // QueryProcessingCtx is defined directly in the `ffi` crate
        // (`ffi/src/lib.rs`). Without this blocklist, bindgen would emit a
        // conflicting opaque struct from its forward reference in
        // `result_processor.h`.
        .blocklist_type("QueryProcessingCtx")
        .allowlist_file(".*/types_ffi.h")
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file(out_dir.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
