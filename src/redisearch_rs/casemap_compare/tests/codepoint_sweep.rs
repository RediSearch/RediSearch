/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Sweep every Unicode scalar value through both folders.
//!
//! This is the most decisive way to enumerate per-codepoint divergence:
//! we feed each codepoint individually (so multi-codepoint context effects
//! like final sigma don't interfere) and let the corpus aggregator emit the
//! full codepoint divergence table.

use casemap_compare::run_corpus;

#[test]
fn sweep_all_codepoints() {
    let inputs = (0u32..=0x10FFFFu32)
        // Skip UTF-16 surrogate range; these are not valid Rust `char`s.
        .filter(|cp| !(0xD800..=0xDFFF).contains(cp))
        .filter_map(char::from_u32)
        .map(|c| c.to_string());

    let report = run_corpus(inputs);

    print!("{}", report.render());
    println!(
        "\nSweep complete: total={} matched={} diverged={}",
        report.total, report.matched, report.diverged
    );
}
