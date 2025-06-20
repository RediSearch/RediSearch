/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! FFI layer to access, from C, the varint encoding machinery implemented in Rust.

mod field_mask;
mod value;
mod vector_writer;

pub use field_mask::*;
pub use value::*;
pub use vector_writer::*;
