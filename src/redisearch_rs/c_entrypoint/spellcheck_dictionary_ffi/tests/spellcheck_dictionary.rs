/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Exercises the `extern "C"` surface the way a C caller would: raw
//! pointers in, cursor pull-loop out.

use spellcheck_dictionary_ffi::*;
use std::ffi::c_char;
use std::ptr;

fn with_dict(terms: &[&str], f: impl FnOnce(*mut SpellCheckDictionary)) {
    let dict = SpellCheckDictionary_New();
    for term in terms {
        // Safety: `dict` is valid, `term` points to valid UTF-8 bytes,
        // and no other call on `dict` runs concurrently.
        unsafe { SpellCheckDictionary_Add(dict, term.as_ptr().cast(), term.len()) };
    }
    f(dict);
    // Safety: `dict` is valid, all iterators created by `f` have been
    // freed, and `dict` is not used afterwards.
    unsafe { SpellCheckDictionary_Free(dict) };
}

/// Pull every term out of `it` through the cursor protocol, then free it.
fn drain(it: *mut SpellCheckDictionaryIterator) -> Vec<String> {
    let mut terms = Vec::new();
    let mut str: *const c_char = ptr::null();
    let mut len = 0_usize;
    // Safety: `it` is a live iterator and `str`/`len` point at live locals.
    while unsafe { SpellCheckDictionaryIterator_Next(it, &raw mut str, &raw mut len) } == 1 {
        // Safety: `str` points at `len` valid UTF-8 bytes owned by the
        // iterator, alive until the next `Next` call below.
        let bytes = unsafe { std::slice::from_raw_parts(str.cast::<u8>(), len) };
        terms.push(std::str::from_utf8(bytes).unwrap().to_owned());
    }
    // Safety: `it` is live and not used after this call.
    unsafe { SpellCheckDictionaryIterator_Free(it) };
    terms
}

#[test]
fn add_reports_newly_added_and_len_tracks() {
    with_dict(&[], |dict| {
        let term = "hello";

        // Safety: `dict` is valid and `term` points to valid UTF-8 bytes.
        let first = unsafe { SpellCheckDictionary_Add(dict, term.as_ptr().cast(), term.len()) };
        // Safety: same as above.
        let second = unsafe { SpellCheckDictionary_Add(dict, term.as_ptr().cast(), term.len()) };

        assert!(first, "first insertion must report newly added");
        assert!(!second, "duplicate insertion must not report newly added");
        // Safety: `dict` is valid.
        assert_eq!(unsafe { SpellCheckDictionary_Len(dict) }, 1);
    });
}

#[test]
fn add_rejects_empty_term() {
    with_dict(&[], |dict| {
        // Safety: `dict` is valid; an empty slice's pointer is non-null.
        let added = unsafe { SpellCheckDictionary_Add(dict, "".as_ptr().cast(), 0) };

        assert!(!added);
        // Safety: `dict` is valid.
        assert_eq!(unsafe { SpellCheckDictionary_Len(dict) }, 0);
    });
}

#[test]
fn remove_is_case_sensitive_and_reports_presence() {
    with_dict(&["Foo"], |dict| {
        let wrong_case = "foo";
        let exact = "Foo";

        // Safety: `dict` is valid and both terms point to valid UTF-8 bytes.
        let removed_wrong = unsafe {
            SpellCheckDictionary_Remove(dict, wrong_case.as_ptr().cast(), wrong_case.len())
        };
        // Safety: same as above.
        let removed_exact =
            unsafe { SpellCheckDictionary_Remove(dict, exact.as_ptr().cast(), exact.len()) };

        assert!(!removed_wrong, "wrong-case removal must be a no-op");
        assert!(removed_exact, "exact-case removal must report presence");
        // Safety: `dict` is valid.
        assert_eq!(unsafe { SpellCheckDictionary_Len(dict) }, 0);
    });
}

#[test]
fn contains_is_case_insensitive() {
    with_dict(&["Hello"], |dict| {
        let hits = ["Hello", "hello", "HELLO"];
        let miss = "world";

        for query in hits {
            // Safety: `dict` is valid and `query` points to valid UTF-8 bytes.
            assert!(
                unsafe { SpellCheckDictionary_Contains(dict, query.as_ptr().cast(), query.len()) },
                "expected {query:?} to match"
            );
        }
        // Safety: `dict` is valid and `miss` points to valid UTF-8 bytes.
        assert!(!unsafe { SpellCheckDictionary_Contains(dict, miss.as_ptr().cast(), miss.len()) });
    });
}

#[test]
fn iterate_all_yields_stored_case_in_lexicographical_order() {
    with_dict(&["banana", "Apple", "cherry"], |dict| {
        // Safety: `dict` is valid and outlives the iterator.
        let it = unsafe { SpellCheckDictionary_IterateAll(dict) };
        let actual = drain(it);

        assert_eq!(actual, ["Apple", "banana", "cherry"]);
    });
}

#[test]
fn iterate_fuzzy_matches_within_distance_ignoring_case() {
    with_dict(&["Hello", "help", "world"], |dict| {
        let query = "HELO";

        // Safety: `dict` is valid, `query` points to valid UTF-8 bytes,
        // and `dict` outlives the iterator.
        let it = unsafe {
            SpellCheckDictionary_IterateFuzzy(dict, query.as_ptr().cast(), query.len(), 1)
        };
        let mut actual = drain(it);
        actual.sort();

        // "helo" is one edit from both "hello" (insertion) and "help"
        // (substitution); terms come back in their stored case.
        assert_eq!(actual, ["Hello", "help"]);
    });
}

#[test]
fn iterate_fuzzy_is_computed_eagerly() {
    with_dict(&["hello"], |dict| {
        let query = "hello";

        // Safety: `dict` is valid, `query` points to valid UTF-8 bytes,
        // and `dict` outlives the iterator.
        let it = unsafe {
            SpellCheckDictionary_IterateFuzzy(dict, query.as_ptr().cast(), query.len(), 0)
        };
        // Removing the term after cursor creation must not affect what the
        // cursor yields.
        // Safety: `dict` is valid; the fuzzy cursor holds no borrow of it.
        unsafe { SpellCheckDictionary_Remove(dict, query.as_ptr().cast(), query.len()) };
        let actual = drain(it);

        assert_eq!(actual, ["hello"]);
    });
}

#[test]
fn exhausted_cursor_keeps_returning_zero() {
    with_dict(&["only"], |dict| {
        // Safety: `dict` is valid and outlives the iterator.
        let it = unsafe { SpellCheckDictionary_IterateAll(dict) };
        let mut str: *const c_char = ptr::null();
        let mut len = 0_usize;

        // Safety: `it` is live and `str`/`len` point at live locals.
        assert_eq!(
            unsafe { SpellCheckDictionaryIterator_Next(it, &raw mut str, &raw mut len) },
            1
        );
        // Safety: same as above.
        assert_eq!(
            unsafe { SpellCheckDictionaryIterator_Next(it, &raw mut str, &raw mut len) },
            0
        );
        // Safety: same as above; a cursor must stay exhausted, not restart.
        assert_eq!(
            unsafe { SpellCheckDictionaryIterator_Next(it, &raw mut str, &raw mut len) },
            0
        );

        // Safety: `it` is live and not used after this call.
        unsafe { SpellCheckDictionaryIterator_Free(it) };
    });
}

#[test]
fn multibyte_terms_roundtrip() {
    with_dict(&["żółć", "köln"], |dict| {
        let query = "ŻÓŁĆ";

        // Safety: `dict` is valid and `query` points to valid UTF-8 bytes.
        let contained =
            unsafe { SpellCheckDictionary_Contains(dict, query.as_ptr().cast(), query.len()) };
        // Safety: `dict` is valid, `query` points to valid UTF-8 bytes,
        // and `dict` outlives the iterator.
        let it = unsafe {
            SpellCheckDictionary_IterateFuzzy(dict, query.as_ptr().cast(), query.len(), 0)
        };
        let actual = drain(it);

        assert!(contained);
        assert_eq!(actual, ["żółć"]);
    });
}
