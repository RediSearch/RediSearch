/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Compare the owned [`unicode_tolower`] against the borrowing
//! [`unicode_tolower_cow`].
//!
//! The `Cow` variant only pays off when the input is already lowercase: it
//! borrows and skips the allocation entirely. When a fold is required it does
//! strictly more work (the `is_lowercase` scan *plus* the same fold+allocate),
//! so the inputs are split along two axes — already-lowercase vs needs-fold,
//! ASCII vs non-ASCII — to show both the win and the overhead.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use string_utils::{unicode_tolower, unicode_tolower_cow};

/// Representative inputs, one per (case, script) quadrant.
const INPUTS: &[(&str, &str)] = &[
    ("ascii_lower", "hello world this is a lowercase query term"),
    ("ascii_mixed", "Hello World This Is A Mixed Case Query Term"),
    // Cow worst case: the scan must walk the whole string before the trailing
    // uppercase char forces a fold+allocate, so it pays scan cost on top of the
    // owned path's work.
    (
        "ascii_late_upper",
        "hello world this is a lowercase query terM",
    ),
    ("unicode_lower", "straße café naïve façade œuvre αβγδε"),
    ("unicode_mixed", "Straße Café Naïve Façade Œuvre Αβγδε"),
];

fn bench_tolower(c: &mut Criterion) {
    let mut group = c.benchmark_group("unicode_tolower");
    for &(name, input) in INPUTS {
        group.bench_with_input(BenchmarkId::new("owned", name), input, |b, s| {
            b.iter(|| unicode_tolower(black_box(s)));
        });
        group.bench_with_input(BenchmarkId::new("cow", name), input, |b, s| {
            b.iter(|| unicode_tolower_cow(black_box(s)));
        });
    }
    group.finish();
}

criterion_group!(benches, bench_tolower);
criterion_main!(benches);
