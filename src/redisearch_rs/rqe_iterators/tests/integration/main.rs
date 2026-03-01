/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#![cfg_attr(miri, allow(dead_code, unused_imports))]
pub(crate) mod utils;

// Mock implementations of C symbol that aren't provided
// by the static C libraries we are linking against in build.rs.
redis_mock::mock_or_stub_missing_redis_c_symbols!();
extern crate redisearch_rs;

use rstest_reuse::template;

// cases used by id_list and metric tests
#[template]
#[rstest::rstest]
#[case::sorted_odd(&[1u64, 3, 5, 7, 9])]
#[case::sorted_even(&[2u64, 4, 6, 8, 10])]
#[case::sequential_10(&[1u64, 2, 3, 4, 5, 6, 7, 8, 9, 10])]
#[case::sparse(&[1u64, 2, 3, 5, 6, 20, 98, 500, 1000])]
#[case::single(&[42u64])]
#[case::large_ids(&[1000000u64, 2000000, 3000000])]
#[case::tens(&[10u64, 20, 30, 40, 50])]
#[case::sequential_40(&[
    1u64, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
    25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
])]
fn id_cases(#[case] case: &[u64]) {}

mod empty;
mod id_list;
mod intersection;
mod inverted_index;
mod maybe_empty;
mod metric;
mod not;
mod optional;
mod profile;
mod wildcard;
