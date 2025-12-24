/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#![allow(unused_variables)]

mod array;
mod comparisons;
mod constructors;
mod hash;
mod map;
mod placeholders;
mod sds;
mod string;
mod value_type;
// pub mod collection;
// pub mod shared;

pub use array::*;
pub use comparisons::*;
pub use constructors::*;
pub use hash::*;
pub use map::*;
pub use placeholders::*;
pub use sds::*;
pub use string::*;
pub use value_type::*;

// TODO: This should export the size of const expr below instead
// pub const RSValueSize: usize = std::mem::size_of::<RsValue>();
#[allow(non_upper_case_globals)]
pub const RSValueSize: usize = 16;
