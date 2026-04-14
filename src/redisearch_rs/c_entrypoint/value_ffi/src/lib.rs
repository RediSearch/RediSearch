/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

pub mod array;
pub mod comparisons;
pub mod constructors;
pub mod conversions;
pub mod debug;
pub mod getters;
pub mod hash;
pub mod map;
pub mod setters;
pub mod shared;
pub mod util;
pub mod value_type;

/// The total heap allocation size of an `RsValue` wrapped in a `triomphe::Arc`.
#[expect(non_upper_case_globals)]
pub const RSValueSize: usize = 32;

// Ensure the size doesn't increase unexpectedly.
// The 8 bytes overhead is from the `triomphe::Arc` atomic refcount.
const _: () = assert!(RSValueSize == size_of::<value::RsValue>() + size_of::<usize>());
