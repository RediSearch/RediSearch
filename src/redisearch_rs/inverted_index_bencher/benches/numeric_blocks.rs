/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Encodes and decodes whole blocks of realistic numeric values.
//!
//! The per-representation benchmarks in `encoding_decoding` group values by their
//! encoded size, so the same input lands in different groups whenever the codec picks a
//! different representation for it. These blocks keep the inputs fixed, so a run on one
//! revision is directly comparable with a run on another.

use std::{hint::black_box, io::Cursor, time::Duration};

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use index_result::RSIndexResult;
use inverted_index::{
    Decoder, Encoder, IdDelta,
    numeric::{Numeric, NumericDelta},
};

/// Matches the numeric encoder's recommended block size.
const BLOCK_ENTRIES: usize = 100;

/// Deterministic PCG-style generator, so every revision benchmarks identical inputs.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 11
    }
}

fn datasets() -> Vec<(&'static str, Vec<f64>)> {
    let mut rng = Lcg(42);
    let mut block = |f: &mut dyn FnMut(u64) -> f64| -> Vec<f64> {
        (0..BLOCK_ENTRIES).map(|_| f(rng.next())).collect()
    };

    vec![
        // Prices in cents, some of them whole amounts.
        ("money", block(&mut |r| (r % 10_000) as f64 / 100.0)),
        // Six-decimal longitudes around New York.
        (
            "coordinates",
            block(&mut |r| (-74_000_000 + (r % 300_000) as i64) as f64 / 1e6),
        ),
        // Values exactly representable in an f32.
        ("eighths", block(&mut |r| (r % 800) as f64 / 8.0)),
        // Full-precision doubles with no short decimal form.
        (
            "random_doubles",
            block(&mut |r| r as f64 / (1u64 << 53) as f64 * 100.0),
        ),
        // Epoch-second timestamps.
        (
            "timestamps",
            block(&mut |r| (1_420_070_400 + r % 30_000_000) as f64),
        ),
    ]
}

fn encode_block(values: &[f64]) -> Vec<u8> {
    let mut buffer = Cursor::new(Vec::with_capacity(values.len() * 10));
    for &value in values {
        let record = RSIndexResult::build_numeric(value).build();
        Numeric::encode(&mut buffer, NumericDelta::from_u64(1).unwrap(), &record).unwrap();
    }
    buffer.into_inner()
}

fn benchmark_blocks(c: &mut Criterion) {
    for (name, values) in datasets() {
        let encoded = encode_block(&values);
        println!(
            "numeric block {name}: {} bytes for {} entries",
            encoded.len(),
            values.len()
        );

        let mut group = c.benchmark_group(format!("Numeric Block/{name}"));
        group.throughput(Throughput::Elements(values.len() as u64));

        group.bench_function("encode", |b| {
            b.iter_batched_ref(
                || Cursor::new(Vec::with_capacity(values.len() * 10)),
                |buffer| {
                    for &value in &values {
                        let record = RSIndexResult::build_numeric(value).build();
                        let written = Numeric::encode(
                            &mut *buffer,
                            NumericDelta::from_u64(1).unwrap(),
                            &record,
                        )
                        .unwrap();
                        black_box(written);
                    }
                },
                BatchSize::SmallInput,
            );
        });

        group.bench_function("decode", |b| {
            b.iter(|| {
                let mut cursor = Cursor::new(encoded.as_slice());
                let mut result = RSIndexResult::build_numeric(0.0).build();
                let mut base = 0;
                let mut sum = 0.0;
                for _ in 0..values.len() {
                    Numeric::decode(&mut cursor, base, &mut result).unwrap();
                    base = result.doc_id;
                    sum += result.as_numeric().unwrap();
                }
                black_box(sum)
            });
        });

        group.finish();
    }
}

criterion_group!(
    name = benches;
    config = Criterion::default().measurement_time(Duration::from_secs(3)).warm_up_time(Duration::from_secs(1));
    targets = benchmark_blocks,
);

criterion_main!(benches);
