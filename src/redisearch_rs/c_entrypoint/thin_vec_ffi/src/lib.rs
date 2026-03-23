/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Re-export the `Header` type from the `thin_vec` crate for use by other FFI crates
//! that expose a `ThinVec` type.
pub type Header = thin_vec::Header<u16>;
