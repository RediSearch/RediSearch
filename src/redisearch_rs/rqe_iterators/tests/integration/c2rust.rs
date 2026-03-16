/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests that exercise C symbols linked into the test binary via `extern crate redisearch_rs`.
//!
//! These tests verify behaviour that requires calling C-side functions at runtime,
//! such as `NewIntersectionIterator` / `NewSortedIdListIterator`.
