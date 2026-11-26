/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use criterion::{Criterion, criterion_group, criterion_main};
use std::{ffi::CString, hint::black_box};
use stopwords::{DEFAULT_STOPWORDS, MAX_STOPWORDLIST_SIZE, StopWordList};
use stopwords_bencher as _;

fn stopwords(c: &mut Criterion) {
    for (name, words) in [
        ("empty", Vec::new()),
        (
            "default",
            DEFAULT_STOPWORDS.iter().map(|s| s.to_vec()).collect(),
        ),
        (
            "unicode",
            ["НЕ", "FUßBALL", "İ", "שלום"]
                .map(|s| s.as_bytes().to_vec())
                .to_vec(),
        ),
        (
            "maximum",
            (0..MAX_STOPWORDLIST_SIZE)
                .map(|i| format!("word{i}").into_bytes())
                .collect(),
        ),
    ] {
        let strings: Vec<_> = words
            .iter()
            .map(|word| CString::new(word.as_slice()).expect("benchmark words contain no NUL"))
            .collect();
        let pointers: Vec<_> = strings.iter().map(|s| s.as_ptr()).collect();
        let rust = StopWordList::new(words.iter().map(Vec::as_slice));
        // SAFETY: pointers borrow live NUL-terminated strings for the duration of the call.
        let c_list =
            unsafe { ffi::NewStopWordListCStr(pointers.as_ptr().cast_mut(), pointers.len()) };
        let mut group = c.benchmark_group(format!("contains/{name}"));
        let queries = [
            b"the".as_slice(),
            b"THE",
            b"absent",
            b"word1023",
            "НЕ".as_bytes(),
            "İ".as_bytes(),
        ];
        for query in queries {
            // SAFETY: c_list is live and query is readable for query.len() bytes.
            assert_eq!(
                unsafe { ffi::StopWordList_Contains(c_list, query.as_ptr().cast(), query.len()) }
                    != 0,
                rust.contains(query)
            );
        }
        group.bench_function("c", |b| {
            b.iter(|| {
                for query in queries {
                    let query = black_box(query);
                    // SAFETY: c_list is live and query is readable for query.len() bytes.
                    black_box(unsafe {
                        ffi::StopWordList_Contains(
                            black_box(c_list),
                            query.as_ptr().cast(),
                            query.len(),
                        )
                    });
                }
            })
        });
        group.bench_function("rust", |b| {
            b.iter(|| {
                for query in queries {
                    black_box(black_box(&rust).contains(black_box(query)));
                }
            })
        });
        group.finish();
        // SAFETY: release the sole reference returned by NewStopWordListCStr.
        unsafe { ffi::StopWordList_Unref(c_list) };

        let mut group = c.benchmark_group(format!("construct/{name}"));
        group.bench_function("c", |b| {
            b.iter(|| {
                let input = black_box(pointers.as_slice());
                // SAFETY: pointers borrow live NUL-terminated strings throughout the call.
                let list =
                    unsafe { ffi::NewStopWordListCStr(input.as_ptr().cast_mut(), input.len()) };
                black_box(list);
                // SAFETY: release the sole reference just returned by NewStopWordListCStr.
                unsafe { ffi::StopWordList_Unref(list) };
            })
        });
        group.bench_function("rust", |b| {
            b.iter(|| {
                let list = StopWordList::new(black_box(words.as_slice()).iter().map(Vec::as_slice));
                black_box(&list);
            })
        });
        group.finish();
    }
    // SAFETY: all benchmark-owned lists are released and no users of the globals remain.
    unsafe { ffi::StopWordList_FreeGlobals() };
}

criterion_group!(benches, stopwords);
criterion_main!(benches);
