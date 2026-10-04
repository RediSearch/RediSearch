/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Malformed input.
//!
//! Every one of these must produce a `DecodeError` — never a panic, never a read past the
//! buffer, and never an allocation sized from a number the block cannot back up.

use crate::harness::{Malformed, Phase, TAGGED_COLUMN, begin, block, malformed_blocks, try_decode};
use pretty_assertions::assert_eq;
use rlookup::{RLookup, RLookupRow};
use row_block::{Block, RowBlockDecoder};

#[test]
fn every_malformed_block_is_rejected_by_the_reader_and_the_decoder_alike() {
    for Malformed {
        what,
        block: bad,
        error,
        phase,
    } in malformed_blocks()
    {
        assert_eq!(try_decode(&bad).err().as_ref(), Some(&error), "{what}");
        assert_eq!(
            Block::parse(&bad).is_err(),
            phase == Phase::Schema,
            "{what} failed in the wrong phase"
        );

        // The coordinator reports a failed `begin` and a failed `next_row` differently, and
        // a failure must leave no block behind either way. Starting from an active block
        // makes the failure tear one down.
        let mut coordinator = RLookup::new();
        let mut decoder = RowBlockDecoder::new();
        let valid = block(1, &[&TAGGED_COLUMN, &[0]]);
        begin(&mut decoder, &mut coordinator, &valid).expect("a valid block");
        let got = match begin(&mut decoder, &mut coordinator, &bad) {
            Err(error) => Some((error, Phase::Schema)),
            Ok(()) => {
                let mut failure = None;
                while decoder.has_rows() {
                    let mut row = RLookupRow::new();
                    // SAFETY: `coordinator` outlives the decoder.
                    if let Err(error) = unsafe { decoder.next_row(&mut row) } {
                        failure = Some((error, Phase::Row));
                    }
                }
                failure
            }
        };
        assert_eq!(got, Some((error, phase)), "{what}");
        assert!(!decoder.is_active() && !decoder.has_rows(), "{what}");
    }
}

#[test]
fn an_empty_schema_without_rows_is_a_valid_block() {
    assert_eq!(try_decode(&block(0, &[])), Ok(vec![]));
}

#[test]
fn the_row_iterator_stops_at_the_first_error() {
    // Row boundaries are implied by the values themselves, so there is nothing to resynchronise
    // to: continuing would emit garbage rows for as long as the buffer lasts.
    let corrupt = block(1, &[&TAGGED_COLUMN, &[0b1, 200], &[0b1, 200]]);
    let parsed = Block::parse(&corrupt).expect("the header and schema are intact");
    let outcomes: Vec<_> = parsed.rows().map(|row| row.is_ok()).collect();
    assert_eq!(outcomes, vec![false]);
}
