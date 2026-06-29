/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// Provide stubs for Redis allocator and OpenSSL symbols referenced by
// libredisearch_all.a, and force-link FFI crates so their #[no_mangle]
// symbols satisfy archive references. All FFI crates are needed because
// wildcard.c.o's RS_ABORT macro (in debug builds) pulls in RSDummyContext,
// which cascades through most of the archive.
redis_mock::mock_or_stub_missing_redis_c_symbols!();
extern crate fnv_ffi as _;
extern crate fork_gc_ffi as _;
extern crate geo_ffi as _;
extern crate idf_ffi as _;
extern crate inverted_index_ffi as _;
extern crate iterators_ffi as _;
extern crate metrics_ffi as _;
extern crate module_init_ffi as _;
extern crate numeric_range_tree_ffi as _;
extern crate query_error_ffi as _;
extern crate query_eval_ffi as _;
extern crate query_term_ffi as _;
extern crate reducers_ffi as _;
extern crate result_processor_ffi as _;
extern crate rlookup_ffi as _;
extern crate search_result_ffi as _;
extern crate slots_tracker_ffi as _;
extern crate sorting_vector_ffi as _;
extern crate triemap_ffi as _;
extern crate ttl_table_ffi as _;
extern crate types_ffi as _;
extern crate value_ffi as _;
extern crate varint_ffi as _;

mod fmt;
mod matches;
mod parse;
// Disable the proptests when testing with Miri,
// as proptest accesses the file system, which is not supported by Miri
#[cfg(not(miri))]
mod properties;
mod remove_escape;
mod utils;
