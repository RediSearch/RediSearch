/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! [`string_utils::libnu::nu_utf8_decode`] and
//! [`string_utils::libnu::nu_utf8_encode`], checked against the C codec they
//! model.

// These tests call the C codec, so they cannot run under miri.
#![cfg(not(miri))]

use proptest::prelude::*;
use string_utils::libnu::{nu_seq_len, nu_utf8_decode, nu_utf8_encode};

/// Decode the sequence at the start of `seq` with C `nu_utf8_read`, returning
/// the codepoint and how many bytes it consumed.
fn c_decode(seq: &[u8]) -> (u32, usize) {
    assert!(seq.len() >= nu_seq_len(seq[0]), "C would read past `seq`");
    let mut codepoint = 0;
    // SAFETY: `seq` holds every byte the decoder reads for its lead byte, as
    // asserted above.
    let end = unsafe { ffi::nu_utf8_read_fn(seq.as_ptr().cast(), &mut codepoint) };
    // SAFETY: the decoder advances within `seq`, which the assertion bounds.
    let consumed = unsafe { end.offset_from(seq.as_ptr().cast()) };
    (
        codepoint,
        consumed.try_into().expect("the decoder moves forward"),
    )
}

/// Encode `codepoint` with C `nu_utf8_write`.
fn c_encode(codepoint: u32) -> Vec<u8> {
    let mut buf = [0u8; 4];
    // SAFETY: the encoder writes at most four bytes, which `buf` holds.
    let end = unsafe { ffi::nu_utf8_write_fn(codepoint, buf.as_mut_ptr().cast()) };
    // SAFETY: the encoder advances within `buf`, as above.
    let written = unsafe { end.offset_from(buf.as_ptr().cast()) };
    buf[..usize::try_from(written).expect("the encoder moves forward")].to_vec()
}

/// One sequence as the decoder sees it: any lead byte followed by as many
/// arbitrary bytes as that lead announces, well-formed or not.
fn sequence() -> impl Strategy<Value = Vec<u8>> {
    any::<u8>().prop_flat_map(|lead| {
        proptest::collection::vec(any::<u8>(), nu_seq_len(lead) - 1).prop_map(move |rest| {
            let mut seq = vec![lead];
            seq.extend(rest);
            seq
        })
    })
}

proptest! {
    #[test]
    fn decode_matches_c(seq in sequence()) {
        prop_assert_eq!(c_decode(&seq), (nu_utf8_decode(&seq), seq.len()));
    }

    #[test]
    fn encode_matches_c(codepoint: u32) {
        let mut out = Vec::new();
        nu_utf8_encode(codepoint, &mut out);
        prop_assert_eq!(out, c_encode(codepoint));
    }

    #[test]
    fn encode_matches_c_for_well_formed_values(codepoint in 0..=0x10_FFFFu32) {
        // `u32` spans far more than the codepoint range, so this makes sure
        // every width is well represented.
        let mut out = Vec::new();
        nu_utf8_encode(codepoint, &mut out);
        prop_assert_eq!(out, c_encode(codepoint));
    }
}
