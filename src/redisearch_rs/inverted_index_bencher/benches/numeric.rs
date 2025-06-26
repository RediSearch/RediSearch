/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Benchmarks the numeric encoding and decoding

use criterion::{Criterion, criterion_group, criterion_main};
use inverted_index_bencher::benchers::{freqs_only::FreqsOnlyBencher, numeric::Bencher};

fn benchmark_numeric(c: &mut Criterion) {
    let bencher = Bencher::new();

    bencher.encoding(c);
    bencher.decoding(c);
}

fn benchmark_freqs_only(c: &mut Criterion) {
    let bencher = FreqsOnlyBencher::new();

    bencher.encoding(c);
    bencher.decoding(c);
}

// FIXME: should we move this to lib.rs? We can only have one criterion_main
// and I'm not sure it's worth having a sub module for each type of benches.
criterion_group!(benches, benchmark_numeric, benchmark_freqs_only);
criterion_main!(benches);
