use std::io::{Cursor, Seek, SeekFrom, Write};

use criterion::{Criterion, criterion_group, criterion_main};
use qint::qint_encode;

fn criterion_qint_proptest_corpus(c: &mut Criterion) {
    // all variatnts of sizes for 2 integers
    let s2: [[u32; 2]; 16] = [
        [0xFF, 0xFF],
        [0xFF, 0xFF00],
        [0xFF, 0xFFFFFF],
        [0xFF, 0xFFFFFFFF],
        [0xFFFF, 0xFF],
        [0xFFFF, 0xFF00],
        [0xFFFF, 0xFFFFFF],
        [0xFFFF, 0xFFFFFFFF],
        [0xFFFFFF, 0xFF],
        [0xFFFFFF, 0xFF00],
        [0xFFFFFF, 0xFFFFFF],
        [0xFFFFFF, 0xFFFFFFFF],
        [0x0FFFFFFF, 0xFF],
        [0x0FFFFFFF, 0xFF00],
        [0x0FFFFFFF, 0xFFFFFF],
        [0x0FFFFFFF, 0xFFFFFFFF],
    ];

    // 16 variants of sizes for 3 integers
    let s3: [[u32; 3]; 16] = [
        [0xFF, 0xFF, 0xFF],
        [0xFF, 0xFF00, 0xFF],
        [0xFF, 0xFFFFFF, 0xFF],
        [0xFF, 0xFFFFFFFF, 0xFF],
        [0xFFFF, 0xFF, 0xFF],
        [0xFFFF, 0xFF00, 0xFF],
        [0xFFFF, 0xFFFFFF, 0xFF],
        [0xFFFF, 0xFFFFFFFF, 0xFF],
        [0xFFFFFF, 0xFF, 0xFF],
        [0xFFFFFF, 0xFF00, 0xFF],
        [0xFFFFFF, 0xFFFFFF, 0xFF],
        [0xFFFFFF, 0xFFFFFFFF, 0xFF],
        [0xFFFFFFFF, 0xFF, 0xFF],
        [0xFFFFFFFF, 0xFF00, 0xFF],
        [0xFFFFFFFF, 0xFFFFFF, 0xFF],
        [0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF],
    ];

    // 16 variants of sizes for 4 integers
    let s4: [[u32; 4]; 16] = [
        [0xFF, 0xFF, 0xFF, 0xFF],
        [0xFF, 0xFF00, 0xFF, 0xFF],
        [0xFF, 0xFFFFFF, 0xFF, 0xFF],
        [0xFF, 0xFFFFFFFF, 0xFF, 0xFF],
        [0xFFFF, 0xFF, 0xFF, 0xFF],
        [0xFFFF, 0xFF00, 0xFF, 0xFF],
        [0xFFFF, 0xFFFFFF, 0xFF, 0xFF],
        [0xFFFF, 0xFFFFFFFF, 0xFF, 0xFF],
        [0xFFFFFF, 0xFF, 0xFF, 0xFF],
        [0xFFFFFF, 0xFF00, 0xFF, 0xFF],
        [0xFFFFFF, 0xFFFFFF, 0xFF, 0xFF],
        [0xFFFFFF, 0xFFFFFFFF, 0xFF, 0xFF],
        [0xFFFFFFFF, 0xFF, 0xFF, 0xFF],
        [0xFFFFFFFF, 0xFF00, 0xFF, 0xFF],
        [0xFFFFFFFF, 0xFFFFFF, 0xFF, 0xFF],
        [0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF],
    ];

    let buf = [0u8; 48 * 24];
    let mut group = c.benchmark_group("qint-proptest");
    group.bench_function("encode", |b| {
        b.iter_batched_ref(
            || Cursor::new(buf.clone()),
            |cursor| {
                encode_slices(cursor, &s2, &s3, &s4);
            },
            criterion::BatchSize::SmallInput,
        );
    });

    // make an encode buffer for the decode benchmark:
    let mut cursor = Cursor::new(buf);
    encode_slices(&mut cursor, &s2, &s3, &s4);
    cursor.seek(SeekFrom::Start(0)).unwrap();

    group.bench_function("decode", |b| {
        b.iter_batched_ref(
            || cursor.clone(),
            |cursor| {
                for _ in s2.iter() {
                    qint::qint_decode::<2, _>(cursor).unwrap();
                }
                for _ in s3.iter() {
                    qint::qint_decode::<3, _>(cursor).unwrap();
                }
                for _ in s4.iter() {
                    qint::qint_decode::<4, _>(cursor).unwrap();
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn encode_slices<C: Write + Seek>(
    cursor: &mut C,
    slice2: &[[u32; 2]],
    slice3: &[[u32; 3]],
    slice4: &[[u32; 4]],
) {
    for values in slice2.iter() {
        qint_encode(cursor, *values).unwrap();
    }
    for values in slice3.iter() {
        qint_encode(cursor, *values).unwrap();
    }
    for values in slice4.iter() {
        qint_encode(cursor, *values).unwrap();
    }
}

criterion_group!(random_bench, criterion_qint_proptest_corpus);
criterion_main!(random_bench);
