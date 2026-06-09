/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Idempotence sweep for libnu's `nu_tofold`.
//!
//! Unicode simple case-folding is, by spec, idempotent: every codepoint in
//! the *output* of a fold mapping is itself a fold fixed-point. Concretely,
//! for every scalar `cp`, `fold(fold(cp)) == fold(cp)` must hold. A failure
//! here is not a Unicode edge case — it points at a stale or misgenerated
//! entry in libnu's fold table (`deps/libnu/tofold.c`).
//!
//! Since [`unicode_align_test::checks::fold_libnu`] is a per-codepoint loop, string-level
//! idempotence follows trivially from per-codepoint idempotence. Sweeping all
//! 1,112,064 scalars is therefore necessary and sufficient.

use unicode_align_test::checks::fold_libnu;
use rayon::prelude::*;

#[derive(Debug, Clone)]
struct Violation {
    input: u32,
    once: String,
    twice: String,
}

#[test]
fn fold_is_idempotent_for_every_scalar() {
    let violations: Vec<Violation> = (0u32..=0x10FFFFu32)
        .into_par_iter()
        .filter(|cp| !(0xD800..=0xDFFF).contains(cp))
        .filter_map(|cp| {
            let ch = char::from_u32(cp)?;
            let once = fold_libnu(&ch.to_string());
            let twice = fold_libnu(&once);
            (once != twice).then_some(Violation {
                input: cp,
                once,
                twice,
            })
        })
        .collect();

    if !violations.is_empty() {
        eprintln!(
            "nu_tofold is not idempotent for {} of 1,112,064 scalars:",
            violations.len()
        );
        for v in &violations {
            eprintln!(
                "  U+{:04X}: fold once = {:?}, fold twice = {:?}",
                v.input, v.once, v.twice
            );
        }
    }

    assert!(
        violations.is_empty(),
        "nu_tofold is not idempotent for {} scalars (see stderr)",
        violations.len()
    );
}
