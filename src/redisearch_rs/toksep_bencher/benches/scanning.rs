/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};

fn scanning(c: &mut Criterion) {
    for (name, mut input) in [
        ("empty", Vec::new()),
        ("short", b"a few short words".to_vec()),
        ("long", vec![b'a'; 4096]),
        ("dense", b", ,,,..\t ;".repeat(32)),
        ("escaped", br"one\,two three\\,four five\\\ six".repeat(32)),
        ("high_bytes", b"\xc3\xa9\xff\x80 word ".repeat(32)),
        ("early_nul", b"first\0ignore the rest".to_vec()),
    ] {
        input.push(0);
        let mut group = c.benchmark_group(format!("scanning/{name}"));
        group.bench_function("c", |b| {
            b.iter(|| {
                let mut cursor = black_box(input.as_ptr())
                    .cast_mut()
                    .cast::<std::ffi::c_char>();
                let mut total = 0;
                while !cursor.is_null() {
                    let mut len = 0;
                    // SAFETY: cursor starts in a NUL-terminated input and toksep
                    // advances it within that input or sets it to null. The output
                    // slots are initialized, disjoint, and writable; input is read-only.
                    unsafe { ffi::toksep(&mut cursor, &mut len) };
                    total += len;
                }
                black_box(total)
            })
        });
        group.bench_function("rust", |b| {
            b.iter(|| {
                let mut rest = black_box(input.as_slice());
                let mut total = 0;
                loop {
                    let len = toksep::token_len(rest.iter().copied());
                    total += len;
                    if rest[len] == 0 || rest[len + 1] == 0 {
                        break;
                    }
                    rest = &rest[len + 1..];
                }
                black_box(total)
            })
        });
        group.finish();
    }
    let bytes: Vec<_> = (0..=255u8).collect();
    let mut group = c.benchmark_group("classification");
    group.bench_function("c", |b| {
        b.iter(|| {
            black_box(
                bytes
                    .iter()
                    .map(|&byte| {
                        // SAFETY: istoksep accepts every int value and accesses no caller memory.
                        unsafe { ffi::istoksep(i32::from(black_box(byte))) }
                    })
                    .sum::<i32>(),
            )
        })
    });
    group.bench_function("rust", |b| {
        b.iter(|| {
            black_box(
                bytes
                    .iter()
                    .map(|&byte| i32::from(toksep::is_separator(black_box(byte))))
                    .sum::<i32>(),
            )
        })
    });
    group.finish();
}

criterion_group!(benches, scanning);
criterion_main!(benches);
