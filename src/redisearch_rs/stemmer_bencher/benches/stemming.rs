/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use criterion::{Criterion, criterion_group, criterion_main};
use language::Language;
use std::{hint::black_box, ptr::NonNull};
use stemmer::StemBuffer;

extern crate redisearch_rs;
redis_mock::mock_or_stub_missing_redis_c_symbols!();

fn stemming(c: &mut Criterion) {
    for (name, language, c_language, words) in [
        (
            "changed",
            Language::English,
            ffi::RSLanguage_RS_LANG_ENGLISH,
            vec!["running".into(), "arbitrary".into(), "worlds".into()],
        ),
        (
            "unchanged",
            Language::English,
            ffi::RSLanguage_RS_LANG_ENGLISH,
            vec!["world".into(), "cat".into(), "a".into()],
        ),
        (
            "case",
            Language::English,
            ffi::RSLanguage_RS_LANG_ENGLISH,
            vec!["WORLD".into(), "World".into()],
        ),
        (
            "empty",
            Language::English,
            ffi::RSLanguage_RS_LANG_ENGLISH,
            vec![String::new()],
        ),
        (
            "multibyte",
            Language::Russian,
            ffi::RSLanguage_RS_LANG_RUSSIAN,
            vec!["машины".into(), "работающий".into()],
        ),
        (
            "long_multibyte",
            Language::Russian,
            ffi::RSLanguage_RS_LANG_RUSSIAN,
            vec![format!("{}машины", "я".repeat(1024)), "дом".into()],
        ),
        (
            "tagalog",
            Language::Tagalog,
            ffi::RSLanguage_RS_LANG_TAGALOG,
            vec!["kumakain".into(), "mabilis".into()],
        ),
        (
            "malay",
            Language::Malay,
            ffi::RSLanguage_RS_LANG_MALAY,
            vec!["makanan".into(), "berjalan".into()],
        ),
    ] {
        // SAFETY: the algorithm name is NUL-terminated; NULL selects UTF-8.
        let sb = NonNull::new(unsafe {
            ffi::sb_stemmer_new(language.snowball_stemmer().as_ptr(), std::ptr::null())
        })
        .expect("benchmark algorithm is available");
        // SAFETY: the type and language are supported enum values.
        let mut original =
            NonNull::new(unsafe { ffi::NewStemmer(ffi::StemmerType_SnowballStemmer, c_language) })
                .expect("benchmark algorithm is available");
        // SAFETY: the constructor returned an initialized stemmer, exclusively owned here.
        let original_ref = unsafe { original.as_mut() };
        let stem = original_ref.Stem.expect("Snowball sets the stem callback");
        let free = original_ref.Free.expect("Snowball sets the free callback");
        let mut buffer = StemBuffer::default();
        let mut group = c.benchmark_group(name);
        group.throughput(criterion::Throughput::Elements(words.len() as u64));
        group.bench_function("c", |b| {
            b.iter(|| {
                for word in &words {
                    let word = black_box(word.as_bytes());
                    let mut len = 0;
                    // SAFETY: context is live, input spans word.len() bytes and len is writable.
                    black_box(unsafe {
                        stem(original_ref.ctx, word.as_ptr().cast(), word.len(), &mut len)
                    });
                    black_box(len);
                }
            })
        });
        group.bench_function("rust", |b| {
            b.iter(|| {
                for word in &words {
                    let word = black_box(word.as_bytes());
                    let len = word.len().try_into().expect("benchmark input fits in int");
                    // SAFETY: sb is live and exclusively used here; word spans len bytes.
                    let result = unsafe { ffi::sb_stemmer_stem(sb.as_ptr(), word.as_ptr(), len) };
                    if !result.is_null() {
                        // SAFETY: a successful stem operation initialized the backend's length.
                        let len = unsafe { ffi::sb_stemmer_length(sb.as_ptr()) };
                        // SAFETY: Snowball owns len initialized bytes until its next update or deletion.
                        let result = unsafe { std::slice::from_raw_parts(result, len as usize) };
                        black_box(buffer.update(word, result));
                    }
                }
            })
        });
        // SAFETY: both handles are exclusively owned here and no result is still borrowed.
        unsafe { free(original.as_ptr()) };
        // SAFETY: sb was created above and has not been deleted.
        unsafe { ffi::sb_stemmer_delete(sb.as_ptr()) };
    }
}

criterion_group!(benches, stemming);
criterion_main!(benches);
