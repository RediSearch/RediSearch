use std::io::{Cursor, Seek, SeekFrom};

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use qint::{ValidQIntSize, qint_encode};

/// inserts benchmarks for encoding and decoding using a minimal set of inputs
/// the three inputs cover the minimum and maximum sizes for 2, 3 and 4 integers
fn qint_encode_decode(c: &mut Criterion) {
    // all variatnts of sizes for 2 integers
    let s2: [[u32; 2]; 4] = [
        [0xFF, 0xFF],             // 2 bytes
        [0xFF, 0xFFFFFF],         // 4 bytes
        [0xFFFFFF, 0xFFFFFFFF],   // 7 bytes
        [0xFFFFFFFF, 0xFFFFFFFF], // 8 bytes
    ];

    // 16 variants of sizes for 3 integers
    let s3: [[u32; 3]; 4] = [
        [0xFF, 0xFF, 0xFF],                   // 3 bytes
        [0xFFFFFF, 0xFFFF, 0xFF],             // 6 bytes
        [0xFFFFFF, 0xFFFFFF, 0xFFFFFF],       // 9 bytes
        [0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF], // 12 bytes
    ];

    // 16 variants of sizes for 4 integers
    let s4: [[u32; 4]; 4] = [
        [0xFF, 0xFF, 0xFF, 0xFF],                         // 4 bytes
        [0xFF, 0xFFFF, 0xFFFFFFFF, 0xFFFFFF],             // 10 bytes
        [0xFFFFFF, 0xFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF],     // 14 bytes
        [0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF], // 16 bytes
    ];

    // insert encode benchmarks
    insert_encode_n(c, &s2);
    insert_encode_n(c, &s3);
    insert_encode_n(c, &s4);

    // insert decode benchmarks
    insert_decode_n(c, &s2);
    insert_decode_n(c, &s3);
    insert_decode_n(c, &s4);
}

criterion_group!(random_bench, qint_encode_decode);
criterion_main!(random_bench);

// helper method to insert encode benchmarks for N integers
fn insert_encode_n<const N: usize>(criterion: &mut Criterion, slice: &[[u32; N]])
where
    [u32; N]: ValidQIntSize,
{
    let buf = vec![0u8; slice.len() * 24];

    let mut group = criterion.benchmark_group("qint-encode");
    group.bench_function(format!("{} bytes", N), |b| {
        b.iter_batched_ref(
            || Cursor::new(buf.clone()),
            |mut cursor| {
                for values in slice.iter() {
                    qint_encode::<N, _>(black_box(&mut cursor), black_box(*values)).unwrap();
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

// helper method to insert decode benchmarks for N integers
fn insert_decode_n<const N: usize>(criterion: &mut Criterion, slice: &[[u32; N]])
where
    [u32; N]: ValidQIntSize,
{
    let buf = vec![0u8; slice.len() * 24];

    // prepare a buffer for the decode benchmark
    let mut cursor = Cursor::new(buf);
    for values in slice.iter() {
        qint_encode::<N, _>(&mut cursor, *values).unwrap();
    }
    cursor.seek(SeekFrom::Start(0)).unwrap();

    let mut group = criterion.benchmark_group("qint-decode");
    group.bench_function(format!("{} bytes", N), |b| {
        b.iter_batched_ref(
            || cursor.clone(),
            |cursor| {
                for _ in slice.iter() {
                    qint::qint_decode::<N, _>(black_box(cursor)).unwrap();
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}
