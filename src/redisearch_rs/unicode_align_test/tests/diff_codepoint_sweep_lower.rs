/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Sweep every Unicode scalar value through both lowercase implementations.
//!
//! Sister test to `codepoint_sweep` (fold). This is the production-relevant
//! comparison: `unicode_tolower` at `src/util/strconv.h:121` drives the
//! `nu_tolower` path for every indexed/queried token, and the Rust sorting
//! vector uses ICU's case folding (not lowercase) — so the two libraries
//! sit on either side of a semantic gap that this sweep makes concrete.
//!
//! Feeding codepoints individually means context-sensitive ICU mappings
//! (Greek final sigma in particular) won't trigger here; that interaction
//! is left to the random / corpus tests.

use unicode_align_test::diff::run_corpus_lower;

#[test]
#[ignore = "reporting-only sweep; run with --run-ignored or --test 'diff_*'"]
fn sweep_all_codepoints_lower() {
    let inputs: Vec<String> = (0u32..=0x10FFFFu32)
        // Skip UTF-16 surrogate range; these are not valid Rust `char`s.
        .filter(|cp| !(0xD800..=0xDFFF).contains(cp))
        .filter_map(char::from_u32)
        .map(|c| c.to_string())
        .collect();

    let report = run_corpus_lower(inputs);

    print!("{}", report.render());
    println!(
        "\nLowercase sweep complete: total={} matched={} diverged={}",
        report.total, report.matched, report.diverged
    );
}
