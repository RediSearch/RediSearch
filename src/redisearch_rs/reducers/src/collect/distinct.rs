/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Deduplication key for the `COLLECT … SORTBY DISTINCT` path.
//!
//! [`DistinctRow`] wraps a projected [`RLookupRow`] and gives it a `Hash + Eq`
//! identity so it can key the `DoublePriorityQueue` used by
//! [`Storage::DistinctHeap`][super::storage::Storage]. Both `Hash` and `Eq`
//! derive from a single **canonical byte encoding** of the projected values,
//! so the `Hash`/`Eq` contract holds by construction.
//!
//!
//! This module owns its encoding. It is the string-comparison decision made
//! concrete: scalars are encoded by their textual form, JSON paths by their
//! serialized string (the [`Trio`][value::trio::Trio] `middle` slot), and
//! compound values recursively. Each value is tagged by variant; variable-length
//! values (strings, numbers) and compound values (arrays, maps) are additionally
//! length-prefixed, so field and element boundaries are unambiguous.
//!
//! ## Accepted divergence from `RSValue_Cmp`
//!
//! The encoding does **not** coerce number↔string: `Number(5)` and
//! `String("5")` get different tags and so never collapse. This is sound for
//! DISTINCT because a typed index field has a uniform representation across
//! documents (a `NUMERIC` field is always a number, a `TAG`/`TEXT` field always
//! a string), so two documents that genuinely share a field value share a
//! variant. The mixed-type cases that make `compare` non-transitive do not
//! arise for one field across documents.

use std::hash::{Hash, Hasher};

use rlookup::RLookupRow;
use value::util::num_to_str;
use value::{SharedValue, Value};

/// Variant tags written ahead of each encoded value. Distinct tags keep
/// otherwise-similar encodings apart (e.g. `Number` vs `String`, `Null` vs an
/// absent slot vs the empty string).
mod tag {
    /// A `None` slot in the row (field absent for this row).
    pub const ABSENT: u8 = 0x00;
    /// [`value::Value::Null`].
    pub const NULL: u8 = 0x01;
    /// [`value::Value::Undefined`].
    pub const UNDEFINED: u8 = 0x02;
    /// [`value::Value::String`] / [`value::Value::RedisString`] / a
    /// [`value::Value::Trio`] `middle` resolved to a string. Length-prefixed
    /// bytes follow.
    pub const STRING: u8 = 0x03;
    /// [`value::Value::Number`], encoded via `num_to_str`. Length-prefixed
    /// bytes follow.
    pub const NUMBER: u8 = 0x04;
    /// [`value::Value::Array`]. A length-prefixed count of recursively-encoded
    /// elements follows.
    pub const ARRAY: u8 = 0x05;
    /// [`value::Value::Map`]. A length-prefixed count of recursively-encoded
    /// `(key, value)` pairs follows.
    pub const MAP: u8 = 0x06;
}

/// A projected row paired with its canonical dedup key.
///
/// The wrapped [`RLookupRow`] is the payload yielded at finalize; `canon` is
/// the byte encoding (built once by the caller via [`encode_values`] /
/// [`encode_value_refs`]) that drives [`Hash`] and [`Eq`], so hashing and
/// comparison in the priority queue never re-walk the row.
///
/// The caller — not `DistinctRow` — chooses *which* slots the encoding covers,
/// because the dedup identity is the **projected fields only** and the stored
/// row may also carry sort-key columns in some configs (see the design doc
/// §5.1.1).
pub struct DistinctRow {
    row: RLookupRow<'static>,
    canon: Box<[u8]>,
}

impl DistinctRow {
    /// Pair a projected `row` with its precomputed canonical encoding.
    pub const fn from_parts(row: RLookupRow<'static>, canon: Box<[u8]>) -> Self {
        Self { row, canon }
    }

    /// The projected row, consumed at finalize.
    pub fn into_row(self) -> RLookupRow<'static> {
        self.row
    }
}

impl PartialEq for DistinctRow {
    fn eq(&self, other: &Self) -> bool {
        self.canon == other.canon
    }
}

impl Eq for DistinctRow {}

impl Hash for DistinctRow {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.canon.hash(state);
    }
}

/// Canonically encode a slice of projected slots, in order, into a single byte
/// buffer. Two inputs encode to equal bytes iff they are duplicates under the
/// DISTINCT string-comparison policy.
///
/// Used for the whole-row configs (remote `FIELDS *`, both local modes), where
/// the stored row already holds exactly the projected fields. Remote explicit
/// `FIELDS` instead encodes a subset by key via [`encode_value_refs`].
///
/// Exposed (like the sibling `Storage`/`EntryKey` types) so the crate's
/// integration tests in `reducers/tests/` can exercise the encoding directly.
pub fn encode_values(values: &[Option<SharedValue>]) -> Box<[u8]> {
    encode_value_refs(values.iter().map(|slot| slot.as_deref()))
}

/// Like [`encode_values`] but over an iterator of value references, so a caller
/// can encode a *subset* of a row's slots (the projected fields) in a chosen
/// order without materialising an intermediate slice — used by remote explicit
/// `FIELDS`, whose stored row also carries sort-key columns.
pub fn encode_value_refs<'a>(values: impl IntoIterator<Item = Option<&'a Value>>) -> Box<[u8]> {
    let mut buf = Vec::new();
    for slot in values {
        match slot {
            None => buf.push(tag::ABSENT),
            Some(v) => encode_value(v, &mut buf),
        }
    }
    buf.into_boxed_slice()
}

/// Append the tagged, length-prefixed encoding of `v` to `buf`.
///
/// [`Value::Ref`] is transparent (recurses into the referent) and
/// [`Value::Trio`] encodes its `middle` (serialized-JSON) slot, so neither
/// wrapper changes the encoding of the underlying value.
fn encode_value(v: &Value, buf: &mut Vec<u8>) {
    match v {
        Value::Null => buf.push(tag::NULL),
        Value::Undefined => buf.push(tag::UNDEFINED),
        Value::Number(n) => {
            buf.push(tag::NUMBER);
            let mut tmp = [0u8; 32];
            let len = num_to_str(*n, &mut tmp);
            write_bytes(buf, &tmp[..len]);
        }
        Value::String(s) => {
            buf.push(tag::STRING);
            write_bytes(buf, s.as_bytes());
        }
        Value::RedisString(s) => {
            buf.push(tag::STRING);
            write_bytes(buf, s.as_bytes());
        }
        // Transparent wrappers: encode the underlying value so a `Ref`/`Trio`
        // never differs from the value it stands for.
        Value::Ref(r) => encode_value(r, buf),
        Value::Trio(t) => encode_value(t.middle(), buf),
        Value::Array(a) => {
            buf.push(tag::ARRAY);
            write_len(buf, a.len());
            for elem in a.iter() {
                encode_value(elem, buf);
            }
        }
        Value::Map(m) => {
            buf.push(tag::MAP);
            write_len(buf, m.len());
            for (k, val) in m.iter() {
                encode_value(k, buf);
                encode_value(val, buf);
            }
        }
    }
}

/// Write a `u64` little-endian length prefix.
fn write_len(buf: &mut Vec<u8>, n: usize) {
    buf.extend_from_slice(&(n as u64).to_le_bytes());
}

/// Write a length-prefixed byte string.
fn write_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
    write_len(buf, bytes.len());
    buf.extend_from_slice(bytes);
}
