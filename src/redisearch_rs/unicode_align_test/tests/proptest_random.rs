/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Random Unicode strings via proptest.
//!
//! Single-codepoint sweep misses multi-codepoint context effects (e.g.
//! final sigma triggers based on adjacency). Random strings put codepoints
//! into context with each other and surface those interactions.

use unicode_align_test::run_corpus;
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::TestRunner;

#[test]
fn random_unicode_corpus() {
    let mut runner = TestRunner::deterministic();
    let strategy = prop::collection::vec(any::<char>(), 0..50);

    const SAMPLES: usize = 1_000_000;
    let mut inputs: Vec<String> = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let value = strategy
            .new_tree(&mut runner)
            .expect("proptest tree generation should not fail")
            .current();
        inputs.push(value.into_iter().collect());
    }

    let report = run_corpus(inputs);
    print!("{}", report.render());
    println!(
        "\nRandom corpus: total={} matched={} diverged={}",
        report.total, report.matched, report.diverged
    );
}
