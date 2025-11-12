/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

mod fmt;
mod matches;
mod parse;
// Disable the proptests when testing with Miri,
// as proptest accesses the file system, which is not supported by Miri
#[cfg(not(miri))]
mod properties;
mod utils;
