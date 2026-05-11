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
//! production path (`normalizeStr` in `src/sortable.c`) re-encodes each
//! folded codepoint via `nu_utf8_write`, so this is the encoder that
//! actually ships in user output.
//!
//! # The bug this test documents
//!
//! `b4_utf8` in `deps/libnu/utf8_internal.h:116` uses the wrong bit masks
//! for byte 2 and byte 3 of a 4-byte UTF-8 sequence. Byte 2 uses mask
//! `0x03E000` (missing bit 12); byte 3 uses mask `0x001F00` (which includes
//! bit 12 and overflows it into bit 6 of the continuation byte, corrupting
//! the `10xxxxxx` framing into `11xxxxxx`).
//!
//! The bug fires for every codepoint with bit 12 set in any supplementary
//! plane — exactly half of `U+10000..=U+10FFFF`, i.e. 524,288 codepoints.
//! The test asserts this exact count so a fix (or a *different* bug) is
//! noticed immediately.

use casemap_compare::encode_codepoint_with_libnu;

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

    // Exactly half of the supplementary-plane codepoints have bit 12 set:
    // planes 1..=16 contain 0x100000 codepoints, half is 0x80000.
    const EXPECTED_OFFENDERS: usize = 0x80000;

    assert_eq!(
        offenders.len(),
        EXPECTED_OFFENDERS,
        "b4_utf8 bug behaviour changed: expected {EXPECTED_OFFENDERS} invalid \
         outputs (every supplementary-plane codepoint with bit 12 set), got {}. \
         If the upstream encoder was patched this test should be deleted or \
         the assertion flipped to `offenders.is_empty()`.",
        offenders.len()
    );

    // Spot-check: the bug pattern says byte 2 drops bit 12 and byte 3's bit 6
    // gets set. U+11004 should produce `F0 90 C0 84` (bug) instead of the
    // correct `F0 91 80 84`.
    let sample = encode_codepoint_with_libnu(0x11004);
    assert_eq!(
        sample,
        vec![0xF0, 0x90, 0xC0, 0x84],
        "expected the known b4_utf8 bug signature for U+11004"
    );
}
