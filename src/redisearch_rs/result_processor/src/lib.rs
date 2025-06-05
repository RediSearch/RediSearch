/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

pub type DocId = u64;

use ffi::RSSortingVector;
use ffi::RSValue;

/// Row data for a lookup key. This abstracts the question of "where" the
/// data comes from.
/// cbindgen:field-names=[sv, dyn, ndyn]
#[repr(C)]
pub struct RLookupRow {
    pub sv: *const RSSortingVector,

    // todo bindgen rename
    pub dyn_: *mut *mut RSValue,

    ndyn: usize,
}
