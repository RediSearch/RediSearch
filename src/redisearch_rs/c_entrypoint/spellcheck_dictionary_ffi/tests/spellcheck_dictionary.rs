/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
use std::collections::BTreeSet;
use std::ffi::{c_char, c_int};

use spellcheck_dictionary_ffi::*;

/// Create a [`SpellCheckDictionary`], add `terms` to it, run the callback,
/// then free it.
fn with_dict<F>(terms: &[&str], f: F)
where
    F: FnOnce(*mut SpellCheckDictionary),
{
    let t = NewSpellCheckDictionary();
    for term in terms {
        // Safety: `t` is freshly created and `term` points to valid UTF-8.
        unsafe { SpellCheckDictionary_Add(t, term.as_ptr().cast::<c_char>(), term.len()) };
    }

    f(t);

    // Safety: `t` was obtained from `NewSpellCheckDictionary` and has no live
    // iterators at this point.
    unsafe { SpellCheckDictionary_Free(t) };
}

fn add(t: *mut SpellCheckDictionary, term: &str) -> c_int {
    // Safety: `t` is valid and `term` points to valid UTF-8.
    unsafe { SpellCheckDictionary_Add(t, term.as_ptr().cast::<c_char>(), term.len()) }
}

fn remove(t: *mut SpellCheckDictionary, term: &str) -> c_int {
    // Safety: `t` is valid and `term` points to valid UTF-8.
    unsafe { SpellCheckDictionary_Remove(t, term.as_ptr().cast::<c_char>(), term.len()) }
}

fn contains(t: *const SpellCheckDictionary, term: &str) -> bool {
    // Safety: `t` is valid and `term` points to valid UTF-8.
    unsafe { SpellCheckDictionary_Contains(t, term.as_ptr().cast::<c_char>(), term.len()) == 1 }
}

/// Drain an iterator into a sorted set of strings, freeing it afterwards.
fn drain(it: *mut SpellCheckDictionaryIterator) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let mut str: *const c_char = std::ptr::null();
    let mut len: usize = 0;
    // Safety: `it` is a live iterator; `str`/`len` are writable.
    while unsafe { SpellCheckDictionaryIterator_Next(it, &mut str, &mut len) } == 1 {
        // Safety: `Next` returned 1, so `str`/`len` describe a valid byte run
        // owned by the iterator and live until the next advance.
        let bytes = unsafe { std::slice::from_raw_parts(str.cast::<u8>(), len) };
        out.insert(String::from_utf8(bytes.to_vec()).unwrap());
    }
    // Safety: `it` is a valid iterator no longer in use.
    unsafe { SpellCheckDictionaryIterator_Free(it) };
    out
}

fn to_set(terms: &[&str]) -> BTreeSet<String> {
    terms.iter().map(|s| s.to_string()).collect()
}

#[test]
fn add_reports_new_versus_existing() {
    with_dict(&[], |t| {
        assert_eq!(add(t, "hello"), 1, "first insert is new");
        assert_eq!(add(t, "hello"), 0, "second insert already exists");
    });
}

#[test]
fn add_is_case_sensitive() {
    with_dict(&[], |t| {
        assert_eq!(add(t, "Foo"), 1);
        assert_eq!(add(t, "foo"), 1, "different case is a distinct member");

        // Safety: `t` is valid.
        assert_eq!(unsafe { SpellCheckDictionary_Len(t) }, 2);
    });
}

#[test]
fn remove_reports_whether_member_existed() {
    with_dict(&["hello"], |t| {
        assert_eq!(remove(t, "world"), 0, "non-member");
        assert_eq!(remove(t, "hello"), 1, "member");
        assert_eq!(remove(t, "hello"), 0, "already removed");
    });
}

#[test]
fn remove_is_case_sensitive_but_contains_is_not() {
    with_dict(&["Foo"], |t| {
        assert_eq!(remove(t, "foo"), 0, "case-mismatched remove is a no-op");
        assert!(contains(t, "foo"), "contains still folds case");

        assert_eq!(remove(t, "Foo"), 1);
        assert!(!contains(t, "foo"));
    });
}

#[test]
fn len_tracks_membership() {
    with_dict(&["a", "b", "c"], |t| {
        // Safety: `t` is valid.
        assert_eq!(unsafe { SpellCheckDictionary_Len(t) }, 3);
    });
}

#[test]
fn contains_folds_case_and_multibyte() {
    with_dict(&["Hello", "Fußball", "И"], |t| {
        assert!(contains(t, "hello"));
        assert!(contains(t, "HELLO"));
        assert!(contains(t, "fußball"));
        assert!(contains(t, "и"));
        assert!(!contains(t, "world"));
    });
}

#[test]
fn iterate_all_yields_every_term() {
    with_dict(&["apple", "apply", "orange"], |t| {
        // Safety: `t` is valid and not mutated while the iterator lives.
        let it = unsafe { SpellCheckDictionary_IterateAll(t) };
        assert_eq!(drain(it), to_set(&["apple", "apply", "orange"]));
    });
}

#[test]
fn iterate_all_on_empty_dict_yields_nothing() {
    with_dict(&[], |t| {
        // Safety: `t` is valid.
        let it = unsafe { SpellCheckDictionary_IterateAll(t) };
        assert!(drain(it).is_empty());
    });
}

#[test]
fn iterate_fuzzy_yields_terms_within_distance() {
    with_dict(&["apple", "apply", "ample", "orange"], |t| {
        // dist 0 -> exact match only.
        // Safety: `t` is valid, needle is valid UTF-8.
        let it = unsafe {
            SpellCheckDictionary_IterateFuzzy(t, "apple".as_ptr().cast::<c_char>(), 5, 0)
        };
        assert_eq!(drain(it), to_set(&["apple"]));

        // dist 1 -> "aple" reaches "apple" and "ample".
        // Safety: as above.
        let it =
            unsafe { SpellCheckDictionary_IterateFuzzy(t, "aple".as_ptr().cast::<c_char>(), 4, 1) };
        assert_eq!(drain(it), to_set(&["apple", "ample"]));
    });
}

#[test]
fn iterate_fuzzy_is_case_insensitive_but_preserves_stored_case() {
    with_dict(&["Apple"], |t| {
        // Safety: `t` is valid, needle is valid UTF-8.
        let it = unsafe {
            SpellCheckDictionary_IterateFuzzy(t, "APPLE".as_ptr().cast::<c_char>(), 5, 0)
        };
        assert_eq!(drain(it), to_set(&["Apple"]));
    });
}
