/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#![cfg(feature = "wip_features")]

//! End-to-end tests for the COLLECT reducer that drive
//! [`RemoteCollectReducer`] and [`LocalCollectReducer`] through
//! `add` → `finalize`. Pure comparator unit tests live in
//! `reducers/tests/storage.rs`. The
//! `RSGlobalConfig.maxAggregateResults` array-path cap is covered by the
//! Python E2E tests because mutating the process-global would require
//! serialising Rust tests.

extern crate redisearch_rs;

redis_mock::mock_or_stub_missing_redis_c_symbols!();

mod common;
#[path = "collect/helpers.rs"]
mod helpers;
#[path = "collect/limit.rs"]
mod limit;
#[path = "collect/local.rs"]
mod local;
#[path = "collect/remote.rs"]
mod remote;
#[path = "collect/sortby.rs"]
mod sortby;
