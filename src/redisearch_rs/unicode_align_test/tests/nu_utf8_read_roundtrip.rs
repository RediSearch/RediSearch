/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Sweep every Unicode scalar through libnu's UTF-8 decoder and assert it
//! is the inverse of a canonical UTF-8 encoder.
//!
//! # Why
//!
//! `nu_utf8_read` (decoder) and `nu_utf8_write` (encoder) are built from
//! the same handwritten bit-mask idiom in `deps/libnu/utf8_internal.h`. The
//! encoder had wrong masks in `b4_utf8` until our local patch (every
//! supplementary-plane codepoint with bit 12 set encoded to invalid UTF-8;
//! see the `nu_utf8_write` sweep for the regression guard). Since the
//! decoder helpers `utf8_2b` / `utf8_3b` / `utf8_4b` use the same pattern,
//! they are a similar regression-prone surface — but the bug class is
//! invisible by inspection. A round-trip sweep is the cheapest way to
//! assert "the decoder agrees with a known-correct encoder for every
//! scalar."
//!
//! RediSearch invokes `nu_utf8_read` from production text-handling code at
//! `src/util/strconv.h:unicode_tolower` (per-codepoint decode loop during
//! text normalisation), `src/trie/rune_util.c:strToLowerRunes` and
//! `runesToStr` (decode while transforming UTF-8 to rune arrays), and
//! `src/trie/rune_util.c:strToSingleCodepointFoldedRunes`. If the decoder
//! silently mis-decoded a codepoint, every token in those paths would be
//! mis-indexed.
//!
//! # Test shape
//!
//! For every Unicode scalar value:
//!
//! - **Strict round-trip**: encode with Rust (`char::encode_utf8`, known
//!   correct), then decode with libnu. Assert decoded codepoint == input
//!   and consumed-bytes == encoded-length. This is the strict version —
//!   any decoder-only bug shows up here.
//! - **Symmetric round-trip**: encode with libnu (`nu_utf8_write`), then
//!   decode with libnu (`nu_utf8_read`). This is weaker (a pair of
//!   compensating bugs in encoder and decoder could mask each other) but
//!   it tests the actual production data path where both halves are
//!   libnu.

use unicode_align_test::{decode_codepoint_with_libnu, encode_codepoint_with_libnu};
use rayon::prelude::*;

#[derive(Debug)]
struct DecodeFailure {
    input: u32,
    encoded: Vec<u8>,
    decoded: u32,
    consumed: usize,
}

fn iter_scalars() -> impl ParallelIterator<Item = u32> {
    (0u32..=0x10FFFF)
        .into_par_iter()
        .filter(|cp| !(0xD800..=0xDFFF).contains(cp))
        .filter(|cp| char::from_u32(*cp).is_some())
}

#[test]
fn strict_roundtrip_rust_encode_libnu_decode() {
    let offenders: Vec<DecodeFailure> = iter_scalars()
        .filter_map(|cp| {
            // Encode with Rust (canonical, known correct).
            let ch = char::from_u32(cp).expect("scalar was pre-filtered");
            let mut buf = [0u8; 4];
            let encoded = ch.encode_utf8(&mut buf).as_bytes();

            // Decode with libnu.
            let (decoded, consumed) = decode_codepoint_with_libnu(encoded);

            if decoded == cp && consumed == encoded.len() {
                None
            } else {
                Some(DecodeFailure {
                    input: cp,
                    encoded: encoded.to_vec(),
                    decoded,
                    consumed,
                })
            }
        })
        .collect();

    report_and_assert("rust_encode -> libnu_decode", &offenders);
}

#[test]
fn symmetric_roundtrip_libnu_encode_libnu_decode() {
    let offenders: Vec<DecodeFailure> = iter_scalars()
        .filter_map(|cp| {
            // Encode and decode both with libnu. Less strict than the
            // canonical-encode variant because compensating bugs could
            // mask each other, but it mirrors the production data path
            // where both halves live in libnu (e.g. `unicode_tolower`
            // reads then writes through `nu_utf8_read`/`nu_utf8_write`).
            let encoded = encode_codepoint_with_libnu(cp);
            let (decoded, consumed) = decode_codepoint_with_libnu(&encoded);

            if decoded == cp && consumed == encoded.len() {
                None
            } else {
                Some(DecodeFailure {
                    input: cp,
                    encoded,
                    decoded,
                    consumed,
                })
            }
        })
        .collect();

    report_and_assert("libnu_encode -> libnu_decode", &offenders);
}

#[test]
fn decode_u11004_consumes_four_bytes() {
    // Anchor spot-check on the historical pain point: U+11004 sits in the
    // supplementary plane with bit 12 set, the exact class of codepoint
    // that the pre-fix `b4_utf8` encoder corrupted. The decoder side has
    // the matching bit-shift logic in `utf8_4b` — assert it produces the
    // right codepoint from the canonical bytes.
    let canonical = [0xF0u8, 0x91, 0x80, 0x84];
    let (decoded, consumed) = decode_codepoint_with_libnu(&canonical);
    assert_eq!(
        decoded, 0x11004,
        "libnu decoded F0 91 80 84 as U+{decoded:06X}; expected U+11004"
    );
    assert_eq!(
        consumed, 4,
        "libnu advanced {consumed} bytes for a 4-byte sequence; expected 4"
    );
}

fn report_and_assert(label: &str, offenders: &[DecodeFailure]) {
    const PREVIEW: usize = 32;

    println!(
        "{}: scanned {} scalars, {} round-trip failures",
        label,
        1_112_064,
        offenders.len()
    );

    for f in offenders.iter().take(PREVIEW) {
        println!(
            "  U+{:06X} encoded={:02X?} decoded=U+{:06X} consumed={}",
            f.input, f.encoded, f.decoded, f.consumed,
        );
    }
    if offenders.len() > PREVIEW {
        println!("  ... ({} more)", offenders.len() - PREVIEW);
    }

    assert!(
        offenders.is_empty(),
        "{label}: {} codepoint(s) failed UTF-8 round-trip through libnu — \
         a mask bug in `utf8_2b`/`utf8_3b`/`utf8_4b` (or `b4_utf8`) may have \
         regressed; see deps/libnu/utf8_internal.h",
        offenders.len(),
    );
}
