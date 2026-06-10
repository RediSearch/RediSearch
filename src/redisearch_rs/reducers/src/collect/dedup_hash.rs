/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The COLLECT DISTINCT dedup-key digest.
//!
//! [`DistinctHasher`] folds a row's projected fields, one `(name, value)` pair
//! at a time, into a single 64-bit FNV-1a digest that is the row's whole dedup
//! identity.

use fnv::Fnv64;
use std::hash::Hasher;
use value::Value;
use value::hash::hash_value;

/// Streaming FNV-1a hasher for the COLLECT DISTINCT dedup key: a row's
/// projected fields are fed in one `(name, value)` pair at a time, in a stable
/// field order, yielding a single 64-bit digest that is the row's whole dedup
/// identity.
#[derive(Default)]
pub struct DistinctHasher(Fnv64);

/// Presence discriminant for a field carrying a (non-`Null`) value.
const PRESENT: u8 = 0x01;
/// Presence discriminant for an absent or `Null` field.
const ABSENT: u8 = 0x00;

impl DistinctHasher {
    pub fn push_field(&mut self, name: &[u8], value: Option<&Value>) {
        // Length-prefix the name so a field boundary is unambiguous: name "ab"
        // + next field cannot be confused with name "a" + "b…".
        self.0.write_u64(name.len() as u64);
        self.0.write(name);
        match value {
            Some(v) if !matches!(v, Value::Null) => {
                self.0.write_u8(PRESENT);
                hash_value(v, &mut self.0);
            }
            // Absent or `Null`: presence marker only, no value.
            _ => self.0.write_u8(ABSENT),
        }
    }

    /// Finish, yielding the 64-bit digest.
    pub fn finish(&self) -> u64 {
        self.0.finish()
    }
}
