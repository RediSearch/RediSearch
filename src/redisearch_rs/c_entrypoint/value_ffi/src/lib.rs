/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#![allow(unused_variables)]

pub mod array;
pub mod comparisons;
pub mod constructors;
pub mod getters;
pub mod hash;
pub mod map;
pub mod placeholders;
pub mod sds;
pub mod setters;
pub mod shared;
pub mod string;
pub mod util;
pub mod value_type;

// TODO: This should export the size of const expr below instead
// pub const RSValueSize: usize = std::mem::size_of::<RsValue>();
#[allow(non_upper_case_globals)]
pub const RSValueSize: usize = 16;
