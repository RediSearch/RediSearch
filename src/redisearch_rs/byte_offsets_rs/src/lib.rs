/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Byte-offset map for tokenized text fields.
//!
//! This is a port of `src/byte_offsets.c`.
//!
//! # Task
//!
//! Implement `ByteOffsets::iterate(field_id)`, which returns an iterator that
//! yields the absolute byte offset of each token in the given field.
//!
//! The `varint` crate is available:
//! - `varint::VectorWriter` — writes delta-encoded u32 values (already used in `build()` in the tests)
//! - `varint::read::<u32, _>(&mut reader)` — reads one delta back out
//!
//! # How the data is laid out
//!
//! All fields share a single flat byte stream (`ByteOffsets::offsets`).
//! The stream stores **deltas** between consecutive absolute byte offsets,
//! varint-encoded.  For example, absolute offsets `[5, 10, 20, 30, 45, 60]`
//! are stored as deltas `[5, 5, 10, 10, 15, 15]`.
//!
//! Each `OffsetField` records which slice of the stream belongs to it via
//! `first_tok_pos` and `last_tok_pos` (1-based token positions).
//!
//! # Worked example
//!
//! Two fields over six tokens:
//!
//! ```text
//! token position:  1    2    3    4    5    6
//! byte offset:     5   10   20   30   45   60
//! delta in stream: 5    5   10   10   15   15
//!
//! field 0: first_tok_pos=1, last_tok_pos=3  → yields 5, 10, 20
//! field 1: first_tok_pos=4, last_tok_pos=6  → yields 30, 45, 60
//! ```
//!
//! To iterate **field 1**, the iterator must first skip positions 1–3 by
//! reading and accumulating their deltas (pre-advance), then yield positions
//! 4–6.  The C implementation does this in two steps:
//!
//! **Setup (`RSByteOffset_Iterate`)** — skip to `first_tok_pos`:
//! ```text
//! cur_pos=1, last_value=0
//!
//! cur_pos < 4 → read delta=5,  last_value= 5, cur_pos=2
//! cur_pos < 4 → read delta=5,  last_value=10, cur_pos=3
//! cur_pos < 4 → read delta=10, last_value=20, cur_pos=4
//! cur_pos < 4 → false, exit loop
//!
//! cur_pos-- → cur_pos=3   ← so that next() can increment before reading
//! ```
//!
//! **Iteration (`RSByteOffsetIterator_Next`)** — one call per token:
//! ```text
//! ++cur_pos=4, 4 > 6? no  → read delta=10, last_value=30  → return 30
//! ++cur_pos=5, 5 > 6? no  → read delta=15, last_value=45  → return 45
//! ++cur_pos=6, 6 > 6? no  → read delta=15, last_value=60  → return 60
//! ++cur_pos=7, 7 > 6? yes → return None
//! ```

/// Token position range for a single field within the shared byte-offset stream.
pub struct OffsetField {
    field_id: u16,
    first_tok_pos: u32,
    last_tok_pos: u32,
}

/// Byte-offset map for the tokens of one document.
///
/// `offsets` is a flat stream of delta-encoded varints shared by all fields.
/// Position *n* in the stream holds the varint-encoded delta from position
/// *n-1* to position *n* (or from 0 for position 1).
pub struct ByteOffsets {
    fields: Vec<OffsetField>,
    /// Delta-encoded byte offsets, stored as raw varint bytes.
    offsets: Vec<u8>,
}

impl ByteOffsets {
    pub fn new() -> Self {
        Self {
            fields: Vec::new(),
            offsets: Vec::new(),
        }
    }

    pub fn add_field(&mut self, field_id: u16, first_tok_pos: u32, last_tok_pos: u32) {
        self.fields.push(OffsetField { field_id, first_tok_pos, last_tok_pos });
    }

    pub fn set_offset_bytes(&mut self, bytes: Vec<u8>) {
        self.offsets = bytes;
    }
}
