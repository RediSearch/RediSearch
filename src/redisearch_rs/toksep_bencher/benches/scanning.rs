/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;

fn scanning(c: &mut Criterion) {
    for (name, input) in [
        ("empty", b"\0".to_vec()),
        ("word", b"token remaining text\0".to_vec()),
        ("escapes", b"hello\\ world\\\\ more\0".to_vec()),
        ("separators", b", ;!\0".to_vec()),
        ("utf8", "שלום_עולם more\0".as_bytes().to_vec()),
        ("long", [vec![b'a'; 4096], vec![0]].concat()),
    ] {
        let mut group = c.benchmark_group(format!("scan/{name}"));
        group.bench_function("c", |b| {
            b.iter(|| {
                let mut cursor = black_box(input.as_ptr()).cast_mut().cast();
                let mut len = 0;
                // SAFETY: input is NUL-terminated, toksep only reads its bytes, and
                // both output pointers refer to distinct initialized local variables.
                unsafe { ffi::toksep(&raw mut cursor, &raw mut len) };
                black_box((cursor, len))
            })
        });
        group.bench_function("rust", |b| {
            b.iter(|| {
                let mut cursor = black_box(input.as_ptr());
                // Match the NUL-terminated input used by C and the production FFI,
                // rather than adding a slice-length check to every byte read.
                toksep::token_len(std::iter::from_fn(|| {
                    // SAFETY: input ends in NUL, and token_len stops at that byte.
                    let byte = unsafe { cursor.read() };
                    cursor = cursor.wrapping_add(1);
                    Some(byte)
                }))
            })
        });
        group.finish();
    }
    let mut group = c.benchmark_group("classification");
    group.bench_function("c", |b| {
        b.iter(|| {
            for byte in 0..=255 {
                // SAFETY: istoksep accepts every integer without accessing caller memory.
                black_box(unsafe { ffi::istoksep(black_box(byte)) });
            }
        })
    });
    group.bench_function("rust", |b| {
        b.iter(|| {
            for byte in 0..=255 {
                black_box(toksep::is_separator(black_box(byte)));
            }
        })
    });
    group.finish();
}

criterion_group!(benches, scanning);
criterion_main!(benches);
