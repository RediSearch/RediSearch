use std::io::{Cursor, Seek, SeekFrom};

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use qint::{ValidQIntSize, qint_encode};

/// inserts benchmarks for encoding and decoding using a minimal set of inputs
/// the three inputs cover the minimum and maximum sizes for 2, 3 and 4 integers
fn qint_encode_decode(c: &mut Criterion) {
    let s2: [([u32; 2], usize); 4] = [
        ([0xFF, 0xFF], 2),
        ([0xFF, 0xFFFFFF], 4),
        ([0xFFFFFF, 0xFFFFFFFF], 7),
        ([0xFFFFFFFF, 0xFFFFFFFF], 8),
    ];

    let s3: [([u32; 3], usize); 4] = [
        ([0xFF, 0xFF, 0xFF], 3),
        ([0xFFFFFF, 0xFFFF, 0xFF], 6),
        ([0xFFFFFF, 0xFFFFFF, 0xFFFFFF], 9),
        ([0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF], 12),
    ];

    let s4: [([u32; 4], usize); 4] = [
        ([0xFF, 0xFF, 0xFF, 0xFF], 4),
        ([0xFF, 0xFFFF, 0xFFFFFFFF, 0xFFFFFF], 10),
        ([0xFFFFFF, 0xFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF], 14),
        ([0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF], 16),
    ];

    // insert encode benchmarks
    encode(c, &s2);
    encode(c, &s3);
    encode(c, &s4);

    // insert decode benchmarks
    decode(c, &s2);
    decode(c, &s3);
    decode(c, &s4);
}

criterion_group!(random_bench, qint_encode_decode);
criterion_main!(random_bench);

// helper method to insert encode benchmarks for N integers
fn encode<const N: usize>(criterion: &mut Criterion, slice: &[([u32; N], usize)])
where
    [u32; N]: ValidQIntSize,
{
    let mut group = criterion.benchmark_group(format!("qint-encode, {N} integers"));
    for (input, n_bytes) in slice {
        let buf = vec![0u8; input.len() * 24];

        group.bench_function(format!("{n_bytes} bytes"), |b| {
            b.iter_batched_ref(
                || Cursor::new(buf.clone()),
                |mut cursor| {
                    qint_encode(black_box(&mut cursor), black_box(*input)).unwrap();
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

// helper method to insert decode benchmarks for N integers
fn decode<const N: usize>(criterion: &mut Criterion, slice: &[([u32; N], usize)])
where
    [u32; N]: ValidQIntSize,
{
    let mut group = criterion.benchmark_group(format!("qint-decode, {N} integers"));
    for (input, n_bytes) in slice {
        let buf = vec![0u8; input.len() * 24];

        // prepare a buffer for the decode benchmark
        let mut cursor = Cursor::new(buf);
        qint_encode::<N, _>(&mut cursor, *input).unwrap();
        cursor.seek(SeekFrom::Start(0)).unwrap();

        group.bench_function(format!("{n_bytes} bytes"), |b| {
            b.iter_batched_ref(
                || cursor.clone(),
                |cursor| {
                    qint::qint_decode::<N, _>(black_box(cursor)).unwrap();
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}
