/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::marker::PhantomData;

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

/// The C version of a [`SharedValue`](value::SharedValue)
pub struct RSValue {
    // Ideally we want this marker to be around `thriomphe::Arc`s inner struct
    // containing the refcount, but that is hidden from use.
    _marker: PhantomData<value::Value>,
}
