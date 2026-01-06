/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

mod metadata;
mod ty;

use std::ffi::CStr;

pub use metadata::DocumentMetadata;
pub use ty::DocumentType;

pub const UNDERSCORE_KEY: &CStr = c"__key";
pub const UNDERSCORE_SCORE: &CStr = c"__score";
pub const UNDERSCORE_PAYLOAD: &CStr = c"__payload";
pub const UNDERSCORE_LANGUAGE: &CStr = c"__language";
