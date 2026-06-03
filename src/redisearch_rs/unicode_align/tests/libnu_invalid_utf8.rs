/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Find valid Unicode inputs that libnu folds into invalid UTF-8 output.
//!
//! Single-codepoint sweep is exhaustive: libnu folds per codepoint and
//! concatenates the results, so if every individual codepoint folds to
//! valid UTF-8, every multi-codepoint input does too (concatenating
//! well-formed UTF-8 strings yields well-formed UTF-8). Conversely, any
//! bad-bytes input must contain a single codepoint that already triggers
//! the problem.

use unicode_align::fold_libnu_raw;

#[test]
fn sweep_for_invalid_libnu_output() {
    let mut offenders: Vec<(u32, Vec<u8>, std::str::Utf8Error)> = Vec::new();

    for cp in 0u32..=0x10FFFF {
        if (0xD800..=0xDFFF).contains(&cp) {
            continue;
        }
        let Some(ch) = char::from_u32(cp) else {
            continue;
        };
        let input = ch.to_string();
        let raw = fold_libnu_raw(&input);
        if let Err(err) = std::str::from_utf8(&raw) {
            offenders.push((cp, raw, err));
        }
    }

    println!(
        "scanned {} codepoints; {} produced invalid UTF-8 from libnu",
        0x10FFFFu32 - (0xDFFFu32 - 0xD800u32 + 1) + 1,
        offenders.len()
    );
    for (cp, bytes, err) in &offenders {
        println!("  U+{cp:06X}  raw_bytes={bytes:02X?}  utf8_error={err}");
    }

    assert!(
        offenders.is_empty(),
        "libnu produced invalid UTF-8 for {} input codepoint(s)",
        offenders.len()
    );
}
