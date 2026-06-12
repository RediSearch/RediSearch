/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
use redis_mock::mock_or_stub_missing_redis_c_symbols;
use std::collections::HashSet;
use std::ffi::c_char;
use triemap_ffi::*;

mock_or_stub_missing_redis_c_symbols!();

#[test]
fn iterate_contains_yields_terms_containing_needle() {
    with_index(&["bike", "biker", "trike", "cool"], |t| {
        let needle = "ike";

        // Safety: `t` is valid, `needle` points to valid UTF-8 bytes,
        // and `t` outlives the iterator.
        let it =
            unsafe { TermSuffixIndex_IterateContains(t, needle.as_ptr().cast(), needle.len()) };
        let actual = drain(it);

        assert_eq!(actual, to_set(&["bike", "biker", "trike"]));
    });
}

#[test]
fn iterate_suffix_yields_terms_ending_with_needle() {
    with_index(&["bike", "biker", "trike", "cool"], |t| {
        let needle = "ike";

        // Safety: `t` is valid, `needle` points to valid UTF-8 bytes,
        // and `t` outlives the iterator.
        let it = unsafe { TermSuffixIndex_IterateSuffix(t, needle.as_ptr().cast(), needle.len()) };
        let actual = drain(it);

        assert_eq!(actual, to_set(&["bike", "trike"]));
    });
}

#[test]
fn empty_needle_yields_no_matches() {
    with_index(&["bike"], |t| {
        let needle = "";

        // Safety: `t` is valid, the dangling needle pointer is never
        // dereferenced for a zero-length slice, and `t` outlives the
        // iterator.
        let contains =
            unsafe { TermSuffixIndex_IterateContains(t, needle.as_ptr().cast(), needle.len()) };
        let suffix =
            unsafe { TermSuffixIndex_IterateSuffix(t, needle.as_ptr().cast(), needle.len()) };

        assert!(drain(contains).is_empty());
        assert!(drain(suffix).is_empty());
    });
}

#[test]
fn remove_drops_term_from_results() {
    with_index(&["bike", "biker"], |t| {
        let term = "biker";
        // Safety: `term` points to valid UTF-8 bytes and no iterator
        // on `t` is alive.
        unsafe { TermSuffixIndex_Remove(t, term.as_ptr().cast(), term.len()) };

        let needle = "ike";
        // Safety: `t` is valid, `needle` points to valid UTF-8 bytes,
        // and `t` outlives the iterator.
        let it =
            unsafe { TermSuffixIndex_IterateContains(t, needle.as_ptr().cast(), needle.len()) };
        let actual = drain(it);

        assert_eq!(actual, to_set(&["bike"]));
    });
}

#[test]
fn iterate_all_yields_terms_and_proper_suffixes() {
    with_index(&["bike"], |t| {
        // Safety: `t` is valid and outlives the iterator.
        let it = unsafe { TermSuffixIndex_IterateAll(t) };
        let actual = drain(it);

        // Full term plus proper suffixes of length >= MIN_SUFFIX (2).
        assert_eq!(actual, to_set(&["bike", "ike", "ke"]));
    });
}

#[test]
fn multibyte_terms_roundtrip() {
    with_index(&["żółć", "köln"], |t| {
        let needle = "ółć";

        // Safety: `t` is valid, `needle` points to valid UTF-8 bytes,
        // and `t` outlives the iterator.
        let it = unsafe { TermSuffixIndex_IterateSuffix(t, needle.as_ptr().cast(), needle.len()) };
        let actual = drain(it);

        assert_eq!(actual, to_set(&["żółć"]));
    });
}

#[test]
fn mem_usage_grows_with_content() {
    with_index(&[], |t| {
        // Safety: `t` is a valid index pointer.
        let empty_usage = unsafe { TermSuffixIndex_MemUsage(t) };

        let term = "bicycle";
        // Safety: `term` points to valid UTF-8 bytes and no iterator
        // on `t` is alive.
        unsafe { TermSuffixIndex_Add(t, term.as_ptr().cast(), term.len()) };

        // Safety: `t` is a valid index pointer.
        let populated_usage = unsafe { TermSuffixIndex_MemUsage(t) };
        assert!(populated_usage > empty_usage);
    });
}

#[test]
fn iterate_wildcard_yields_matching_terms() {
    with_index(&["bike", "biker", "trike", "cool"], |t| {
        let pattern = "*ike*";

        // Safety: `t` is valid, `pattern` points to valid UTF-8 bytes
        // that outlive the iterator, and `t` outlives the iterator.
        let it =
            unsafe { TermSuffixIndex_IterateWildcard(t, pattern.as_ptr().cast(), pattern.len()) };
        assert!(!it.is_null(), "'ike' anchors the search");
        let actual = drain(it);

        assert_eq!(actual, to_set(&["bike", "biker", "trike"]));
    });
}

#[test]
fn iterate_wildcard_without_anchor_returns_null() {
    with_index(&["bike"], |t| {
        // Both tokens are single characters — too short to anchor.
        let pattern = "b*e";

        // Safety: `t` is valid and `pattern` points to valid UTF-8
        // bytes.
        let it =
            unsafe { TermSuffixIndex_IterateWildcard(t, pattern.as_ptr().cast(), pattern.len()) };

        assert!(it.is_null(), "caller must fall back to a full scan");
    });
}

/// Create a [`TermSuffixIndex`], add `terms` to it, call the callback
/// with the index pointer, and free the index.
fn with_index<F>(terms: &[&str], f: F)
where
    F: FnOnce(*mut TermSuffixIndex),
{
    let t = NewTermSuffixIndex();
    for term in terms {
        // Safety: `term` points to `term.len()` valid UTF-8 bytes and
        // no iterator on `t` is alive.
        unsafe { TermSuffixIndex_Add(t, term.as_ptr().cast::<c_char>(), term.len()) };
    }

    f(t);

    // Safety: `t` was obtained from `NewTermSuffixIndex` and all
    // iterators have been freed by the callback.
    unsafe { TermSuffixIndex_Free(t) };
}

/// Drain an iterator into the set of strings it yields, then free it.
fn drain(it: *mut TermSuffixIndexIterator) -> HashSet<String> {
    let mut yielded = HashSet::new();
    let mut str = std::ptr::null();
    let mut len = 0;
    // Safety: `it` is a live iterator and `str`/`len` point to
    // writable locals; the yielded bytes are copied before the next
    // advance invalidates them.
    while unsafe { TermSuffixIndexIterator_Next(it, &mut str, &mut len) } == 1 {
        let bytes = unsafe { std::slice::from_raw_parts(str.cast::<u8>(), len) };
        yielded.insert(String::from_utf8(bytes.to_vec()).unwrap());
    }
    // Safety: `it` is a live iterator, not used after this call.
    unsafe { TermSuffixIndexIterator_Free(it) };
    yielded
}

fn to_set(terms: &[&str]) -> HashSet<String> {
    terms.iter().map(|s| s.to_string()).collect()
}
