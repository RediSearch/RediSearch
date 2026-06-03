/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Sweep every Unicode scalar through libnu's runtime UTF-8 encoder
//! (`nu_utf8_write` → `b4_utf8` for 4-byte codepoints) and report any input
//! whose output is not valid UTF-8.
//!
//! This complements the earlier `libnu_invalid_utf8` test, which only
//! exercised `nu_tofold`'s pre-baked static table bytes. RediSearch's
//! live production paths invoke `nu_utf8_write` from two places:
//!
//! - `unicode_tolower()` in `src/util/strconv.h` — re-encodes each
//!   lowercased codepoint during text normalisation.
//! - `runesToStr()` in `src/trie/rune_util.c` — converts trie rune
//!   arrays back to UTF-8 for returned terms.
//!
//! So this is the encoder that actually ships in user output. (The old
//! `normalizeStr` wrapper in `src/sortable.c` that combined fold +
//! re-encode was removed as dead code in #9538; the encoder bug remains
//! reachable through the two call sites above.)
//!
//! # Regression guard
//!
//! Upstream libnu's `b4_utf8` (in `deps/libnu/utf8_internal.h`) historically
//! shipped with wrong bit masks for byte 2 and byte 3 of a 4-byte UTF-8
//! sequence: byte 2 used `0x03E000` (missing bit 12) and byte 3 used
//! `0x001F00` (overflowing bit 12 into bit 6 of the continuation byte,
//! corrupting the `10xxxxxx` framing into `11xxxxxx`). Every supplementary-
//! plane codepoint with bit 12 set — 524,288 codepoints, half of planes
//! 1..=16 — encoded to invalid UTF-8.
//!
//! Our local patch in `deps/libnu/utf8_internal.h` corrects the masks. This
//! test sweeps every Unicode scalar through `nu_utf8_write` and asserts the
//! output is valid UTF-8 for every input, plus spot-checks the correct
//! encoding of U+11004 so a *partial* regression doesn't slip through.

use unicode_align_test::encode_codepoint_with_libnu;

#[test]
fn sweep_nu_utf8_write_validity() {
    let mut offenders: Vec<(u32, Vec<u8>, std::str::Utf8Error)> = Vec::new();

    for cp in 0u32..=0x10FFFF {
        if (0xD800..=0xDFFF).contains(&cp) {
            continue;
        }
        if char::from_u32(cp).is_none() {
            continue;
        }
        let bytes = encode_codepoint_with_libnu(cp);
        if let Err(err) = std::str::from_utf8(&bytes) {
            offenders.push((cp, bytes, err));
        }
    }

    println!(
        "scanned {} codepoints; {} produced invalid UTF-8 via nu_utf8_write",
        1_112_064,
        offenders.len()
    );

    // Show a representative slice — enumerating all offenders may be long.
    const PREVIEW: usize = 32;
    for (cp, bytes, err) in offenders.iter().take(PREVIEW) {
        println!("  U+{cp:06X}  raw_bytes={bytes:02X?}  utf8_error={err}");
    }
    if offenders.len() > PREVIEW {
        println!("  ... ({} more)", offenders.len() - PREVIEW);
    }

    assert!(
        offenders.is_empty(),
        "nu_utf8_write produced invalid UTF-8 for {} codepoint(s); \
         the b4_utf8 mask fix in deps/libnu/utf8_internal.h may have regressed",
        offenders.len()
    );

    // Spot-check that U+11004 round-trips to the canonical 4-byte encoding.
    // The pre-fix b4_utf8 produced `F0 90 C0 84` (bit 12 dropped from byte 2,
    // leaked into byte 3's framing); the correct encoding is `F0 91 80 84`.
    let sample = encode_codepoint_with_libnu(0x11004);
    assert_eq!(
        sample,
        vec![0xF0, 0x91, 0x80, 0x84],
        "U+11004 mis-encoded — expected canonical UTF-8 bytes for the codepoint"
    );
}
