/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Opaque FFI wrapper around [`TrieMap`](crate::TrieMap) for use with C code.

use std::ffi::c_void;

/// Opaque type wrapping a [`TrieMap<*mut c_void>`](crate::TrieMap) for FFI use.
///
/// This type is intended to be passed across the FFI boundary as an opaque
/// pointer. It can be instantiated with `TrieMap(crate::TrieMap::new())` and
/// the inner [`crate::TrieMap`] can be accessed via the public field.
pub struct TrieMap(pub crate::TrieMap<*mut c_void>);
