/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use language_ffi::RSLanguage;
use std::ptr;
use stemmer_ffi::{ResetStemmer, Stemmer, StemmerType::SnowballStemmer};

extern crate redisearch_rs;
redis_mock::mock_or_stub_missing_redis_c_symbols!();

#[test]
#[cfg(not(miri))] // Calls the native Snowball backend.
fn stemming_reuses_owned_output() {
    use std::{mem::MaybeUninit, slice};
    use stemmer_ffi::NewStemmer;
    let owner = NewStemmer(SnowballStemmer, RSLanguage::Russian);
    // SAFETY: NewStemmer returned an owned, initialized stemmer, exclusively used here.
    let stemmer = unsafe { owner.as_ref() }.unwrap();
    for (name, word, expected) in [
        (
            "long multibyte",
            format!("{}машины", "я".repeat(1024)),
            format!("+{}машин\0", "я".repeat(1024)),
        ),
        ("short after long", "машины".into(), "+машин\0".into()),
    ] {
        let mut len = MaybeUninit::uninit();
        // SAFETY: 1. The context is exclusively owned. 2. word is readable and fits in int.
        // 3. len is aligned, writable, and disjoint from the input and context.
        let result = unsafe {
            stemmer.Stem.unwrap()(
                stemmer.ctx,
                word.as_ptr().cast(),
                word.len(),
                len.as_mut_ptr(),
            )
        };
        // SAFETY: every successful backend call writes len, including unchanged stems.
        let len = unsafe { len.assume_init() };
        assert!(!result.is_null(), "{name}");
        assert_eq!(len, expected.len() - 1, "{name}");
        assert_eq!(
            // SAFETY: the expansion includes its terminator and remains readable before the next call.
            unsafe { slice::from_raw_parts(result.cast::<u8>(), len + 1) },
            expected.as_bytes(),
            "{name}"
        );
    }
    // SAFETY: 1. The owner is transferred exactly once; no result remains borrowed.
    unsafe { stemmer.Free.unwrap()(owner) };
}

#[test]
#[cfg(not(miri))] // Calls the native Snowball backend.
fn unchanged_stems_report_backend_length() {
    use std::mem::MaybeUninit;
    let owner = stemmer_ffi::NewStemmer(SnowballStemmer, RSLanguage::English);
    // SAFETY: the constructor returned a live owner, used exclusively here.
    let stemmer = unsafe { owner.as_ref() }.unwrap();
    for word in ["WORLD", ""] {
        let mut len = MaybeUninit::uninit();
        // SAFETY: 1. The context is exclusive. 2. word is readable and fits in int.
        // 3. len is writable and disjoint from word and the context.
        let result = unsafe {
            stemmer.Stem.unwrap()(
                stemmer.ctx,
                word.as_ptr().cast(),
                word.len(),
                len.as_mut_ptr(),
            )
        };
        assert!(result.is_null());
        // SAFETY: the successful backend call initialized len even without an expansion.
        assert_eq!(unsafe { len.assume_init() }, word.len());
    }
    // SAFETY: 1. This transfers the unique owner exactly once.
    unsafe { stemmer.Free.unwrap()(owner) };
}

#[test]
#[cfg(not(miri))] // Calls the native Snowball backend.
fn languages_without_algorithm_have_no_stemmer() {
    assert!(stemmer_ffi::NewStemmer(SnowballStemmer, RSLanguage::Chinese).is_null());
}

#[test]
fn missing_reset_callback_rejects_reuse() {
    let mut owner = Stemmer {
        ctx: ptr::null_mut(),
        Stem: None,
        Free: None,
        Reset: None,
        language: RSLanguage::English,
        r#type: SnowballStemmer,
    };
    assert_eq!(
        // SAFETY: 1. owner is exclusively accessible. 2. No callback is installed.
        unsafe { ResetStemmer(&raw mut owner, SnowballStemmer, RSLanguage::English) },
        0
    );
}

#[test]
#[cfg(not(miri))] // The constructor calls native Snowball.
fn stemmer_reuse_requires_matching_language() {
    let languages = || (0..=RSLanguage::Unset as usize).map(|i| RSLanguage::from_repr(i).unwrap());
    for language in languages() {
        let owner = stemmer_ffi::NewStemmer(SnowballStemmer, language);
        if owner.is_null() {
            continue;
        }
        // SAFETY: the constructor returned an initialized owner used exclusively here.
        let stemmer = unsafe { owner.as_ref() }.unwrap();
        let ctx = stemmer.ctx;
        for requested in languages() {
            assert_eq!(
                // SAFETY: 1. The owner is live and exclusive. 2. Its constructor installed the matching callback.
                unsafe { ResetStemmer(owner, SnowballStemmer, requested) } != 0,
                language == requested,
                "{language:?} -> {requested:?}"
            );
            assert_eq!(stemmer.ctx, ctx);
        }
        // SAFETY: 1. The owner is transferred exactly once.
        unsafe { stemmer.Free.unwrap()(owner) };
    }
}
