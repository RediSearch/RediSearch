/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Geographic utilities for RediSearch.
//!
//! This crate provides geo coordinate parsing, validation, and geohash
//! encoding/decoding. The lower-level geohash primitives live in the
//! [`hash`] module.

pub mod hash;
mod parse;

pub use parse::{Coordinates, ParseGeoError};
