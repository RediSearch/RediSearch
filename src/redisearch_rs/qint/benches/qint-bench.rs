#![cfg(feature = "test-extensions")]
use std::io::{Cursor, Seek, SeekFrom, Write};

use criterion::{Criterion, criterion_group, criterion_main};
use qint::qint_encode;

mod corpus;

fn criterion_qint_proptest_corpus(c: &mut Criterion) {
    let (slice2, slice3, slice4) = corpus::Corpus::get_sliced();

    let buf = [0u8; 500 * 24];
    let mut group = c.benchmark_group("qint-proptest");
    group.bench_function("encode", |b| {
        b.iter_batched_ref(
            || Cursor::new(buf.clone()),
            |cursor| {
                encode_slices(cursor, &slice2, &slice3, &slice4);
            },
            criterion::BatchSize::SmallInput,
        );
    });

    // make an encode buffer for the decode benchmark:
    let mut cursor = Cursor::new(buf);
    encode_slices(&mut cursor, &slice2, &slice3, &slice4);
    cursor.seek(SeekFrom::Start(0)).unwrap();

    group.bench_function("decode", |b| {
        b.iter_batched_ref(
            || cursor.clone(),
            |cursor| {
                for _ in slice2.iter() {
                    qint::qint_decode::<2, _>(cursor).unwrap();
                }
                for _ in slice3.iter() {
                    qint::qint_decode::<3, _>(cursor).unwrap();
                }
                for _ in slice4.iter() {
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
