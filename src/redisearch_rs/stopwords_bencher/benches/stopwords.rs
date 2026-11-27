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
use stopwords::{DEFAULT_STOPWORDS, MAX_STOPWORDLIST_SIZE, StopWordList};

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
        let rust = StopWordList::new(words.iter().map(Vec::as_slice));
        let mut group = c.benchmark_group(format!("contains/{name}"));
        let queries = [
            b"the".as_slice(),
            b"THE",
            b"absent",
            b"word1023",
            "НЕ".as_bytes(),
            "İ".as_bytes(),
        ];
        group.bench_function("rust", |b| {
            b.iter(|| {
                for query in queries {
                    black_box(black_box(&rust).contains(black_box(query)));
                }
            })
        });
        group.finish();

        let mut group = c.benchmark_group(format!("construct/{name}"));
        group.bench_function("rust", |b| {
            b.iter(|| {
                let list = StopWordList::new(black_box(words.as_slice()).iter().map(Vec::as_slice));
                black_box(&list);
            })
        });
        group.finish();
    }
}

criterion_group!(benches, stopwords);
criterion_main!(benches);
