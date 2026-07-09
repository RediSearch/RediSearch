/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// Provide stubs for C symbols
// that rqe_iterators and inverted_index reference at link time.
redis_mock::mock_or_stub_missing_redis_c_symbols!();
extern crate redisearch_rs;

/// Implement [`top_k::TopKSourceProfile`] for a test score source as a plain leaf: these sources
/// are never printed by a test, but every source a `TopKIterator` is built over must be printable.
macro_rules! impl_leaf_source_profile {
    ($source:ty) => {
        impl top_k::TopKSourceProfile for $source {
            fn print_profile(
                &self,
                _mode: top_k::TopKMode,
                _metrics: &top_k::TopKMetrics,
                map: &mut redis_reply::MapBuilder<'_>,
                ctx: &mut rqe_iterators::profile_print::ProfilePrintCtx<'_>,
                _child: Option<&dyn rqe_iterators::profile_print::ProfilePrint>,
            ) {
                ctx.print_leaf(c"TEST SOURCE", map);
            }
        }
    };
}

mod adhoc_lifecycle;
mod iterator;
mod reduce_child;
mod revalidation;
