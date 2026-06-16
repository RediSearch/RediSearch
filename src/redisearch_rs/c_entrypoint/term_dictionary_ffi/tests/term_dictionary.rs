/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
use redis_mock::mock_or_stub_missing_redis_c_symbols;
use std::collections::HashMap;
use std::ffi::c_char;
use term_dictionary_ffi::*;

mock_or_stub_missing_redis_c_symbols!();

#[test]
fn add_term_then_get_reports_score_and_num_docs() {
    let t = NewTermDictionary();

    let outcome = add(t, "bike", 1.0, 3);
    assert_eq!(outcome, TermDictionaryInsertOutcome::New);

    let (score, num_docs) = get(t, "bike").expect("term was added");
    assert_eq!(score, 1.0);
    assert_eq!(num_docs, 3);

    free(t);
}

#[test]
fn add_term_accumulates_score_and_num_docs() {
    let t = NewTermDictionary();

    add(t, "bike", 1.0, 3);
    let outcome = add(t, "bike", 2.0, 4);
    assert_eq!(outcome, TermDictionaryInsertOutcome::Updated);

    let (score, num_docs) = get(t, "bike").unwrap();
    assert_eq!(score, 3.0, "ADD_INCR accumulates score");
    assert_eq!(num_docs, 7, "ADD_INCR accumulates num_docs");

    free(t);
}

#[test]
fn replace_term_overwrites_score_but_accumulates_num_docs() {
    let t = NewTermDictionary();

    add(t, "bike", 5.0, 3);
    let outcome =
        unsafe { TermDictionary_ReplaceTerm(t, "bike".as_ptr().cast(), "bike".len(), 1.0, 4) };
    assert_eq!(outcome, TermDictionaryInsertOutcome::Updated);

    let (score, num_docs) = get(t, "bike").unwrap();
    assert_eq!(score, 1.0, "ADD_REPLACE overwrites score");
    assert_eq!(num_docs, 7, "ADD_REPLACE still accumulates num_docs");

    free(t);
}

#[test]
fn insert_overwrites_without_accumulating() {
    let t = NewTermDictionary();

    let first = unsafe { TermDictionary_Insert(t, "bike".as_ptr().cast(), "bike".len(), 5.0, 3) };
    assert_eq!(first, TermDictionaryInsertOutcome::New);

    let second = unsafe { TermDictionary_Insert(t, "bike".as_ptr().cast(), "bike".len(), 1.0, 4) };
    assert_eq!(second, TermDictionaryInsertOutcome::Updated);

    let (score, num_docs) = get(t, "bike").unwrap();
    assert_eq!(score, 1.0, "primitive insert overwrites score");
    assert_eq!(num_docs, 4, "primitive insert does NOT accumulate num_docs");

    free(t);
}

#[test]
fn get_reports_absent_term() {
    let t = NewTermDictionary();
    add(t, "bike", 1.0, 1);

    assert!(get(t, "trike").is_none());

    free(t);
}

#[test]
fn get_tolerates_null_out_pointers() {
    let t = NewTermDictionary();
    add(t, "bike", 1.0, 1);

    let found = unsafe {
        TermDictionary_Get(
            t,
            "bike".as_ptr().cast(),
            "bike".len(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    };
    assert_eq!(found, 1, "presence is reported even with NULL out-pointers");

    free(t);
}

#[test]
fn remove_drops_the_term() {
    let t = NewTermDictionary();
    add(t, "bike", 1.0, 1);

    let removed = unsafe { TermDictionary_Remove(t, "bike".as_ptr().cast(), "bike".len()) };
    assert_eq!(removed, 1);
    assert!(get(t, "bike").is_none());

    let removed_again = unsafe { TermDictionary_Remove(t, "bike".as_ptr().cast(), "bike".len()) };
    assert_eq!(removed_again, 0, "removing an absent term reports 0");

    free(t);
}

#[test]
fn decrement_num_docs_reports_each_outcome() {
    let t = NewTermDictionary();
    add(t, "bike", 1.0, 3);

    let updated = decr(t, "bike", 1);
    assert_eq!(updated, TermDictionaryDecrResult::Updated);
    assert_eq!(get(t, "bike").unwrap().1, 2);

    let deleted = decr(t, "bike", 5);
    assert_eq!(
        deleted,
        TermDictionaryDecrResult::Deleted,
        "count reaching zero removes the entry"
    );
    assert!(get(t, "bike").is_none());

    let not_found = decr(t, "bike", 1);
    assert_eq!(not_found, TermDictionaryDecrResult::NotFound);

    free(t);
}

#[test]
fn len_and_mem_usage_grow_with_content() {
    let t = NewTermDictionary();
    let empty_mem = unsafe { TermDictionary_MemUsage(t) };
    assert_eq!(unsafe { TermDictionary_Len(t) }, 0);

    add(t, "bicycle", 1.0, 1);

    assert_eq!(unsafe { TermDictionary_Len(t) }, 1);
    assert!(unsafe { TermDictionary_MemUsage(t) } > empty_mem);

    free(t);
}

#[test]
fn iterate_yields_all_terms_with_payloads() {
    let t = NewTermDictionary();
    add(t, "bike", 1.0, 2);
    add(t, "trike", 3.0, 4);

    let it = unsafe { TermDictionary_Iterate(t) };
    let actual = drain(it);

    assert_eq!(actual.get("bike"), Some(&(1.0, 2)));
    assert_eq!(actual.get("trike"), Some(&(3.0, 4)));
    assert_eq!(actual.len(), 2);

    free(t);
}

#[test]
fn iterate_prefix_filters_by_prefix() {
    let t = NewTermDictionary();
    for term in ["bike", "biker", "trike"] {
        add(t, term, 1.0, 1);
    }

    let it = unsafe { TermDictionary_IteratePrefix(t, "bik".as_ptr().cast(), "bik".len()) };
    let actual = keys(drain(it));

    assert_eq!(actual, to_set(&["bike", "biker"]));

    free(t);
}

#[test]
fn iterate_suffix_filters_by_suffix() {
    let t = NewTermDictionary();
    for term in ["bike", "trike", "cool"] {
        add(t, term, 1.0, 1);
    }

    let it = unsafe { TermDictionary_IterateSuffix(t, "ike".as_ptr().cast(), "ike".len()) };
    let actual = keys(drain(it));

    assert_eq!(actual, to_set(&["bike", "trike"]));

    free(t);
}

#[test]
fn iterate_contains_filters_by_substring() {
    let t = NewTermDictionary();
    for term in ["bike", "biker", "trike", "cool"] {
        add(t, term, 1.0, 1);
    }

    let needle = "ike";
    let it = unsafe { TermDictionary_IterateContains(t, needle.as_ptr().cast(), needle.len()) };
    let actual = keys(drain(it));

    assert_eq!(actual, to_set(&["bike", "biker", "trike"]));

    free(t);
}

#[test]
fn iterate_wildcard_filters_by_pattern() {
    let t = NewTermDictionary();
    for term in ["bike", "biker", "trike", "cool"] {
        add(t, term, 1.0, 1);
    }

    let pattern = "b*e";
    let it = unsafe { TermDictionary_IterateWildcard(t, pattern.as_ptr().cast(), pattern.len()) };
    let actual = keys(drain(it));

    assert_eq!(actual, to_set(&["bike"]));

    free(t);
}

#[test]
fn iterate_range_closed_bounds_are_inclusive() {
    let t = NewTermDictionary();
    for term in ["apple", "banana", "cherry", "date", "fig"] {
        add(t, term, 1.0, 1);
    }

    // [banana, date] with both ends included.
    let actual = keys(drain(range(
        t,
        Some(("banana", true)),
        Some(("date", true)),
    )));

    assert_eq!(actual, to_set(&["banana", "cherry", "date"]));

    free(t);
}

#[test]
fn iterate_range_open_bounds_exclude_endpoints() {
    let t = NewTermDictionary();
    for term in ["apple", "banana", "cherry", "date", "fig"] {
        add(t, term, 1.0, 1);
    }

    // (banana, date) with both ends excluded.
    let actual = keys(drain(range(
        t,
        Some(("banana", false)),
        Some(("date", false)),
    )));

    assert_eq!(actual, to_set(&["cherry"]));

    free(t);
}

#[test]
fn iterate_range_null_bounds_are_unbounded() {
    let t = NewTermDictionary();
    for term in ["apple", "banana", "cherry"] {
        add(t, term, 1.0, 1);
    }

    // NULL lower bound -> everything up to and including "banana".
    let upto = keys(drain(range(t, None, Some(("banana", true)))));
    assert_eq!(upto, to_set(&["apple", "banana"]));

    // NULL upper bound -> everything from "banana" onwards.
    let from = keys(drain(range(t, Some(("banana", true)), None)));
    assert_eq!(from, to_set(&["banana", "cherry"]));

    // Both NULL -> the whole dictionary.
    let all = keys(drain(range(t, None, None)));
    assert_eq!(all, to_set(&["apple", "banana", "cherry"]));

    free(t);
}

#[test]
fn iterate_range_empty_string_lower_bound_differs_from_null() {
    let t = NewTermDictionary();
    for term in ["", "apple", "banana"] {
        add(t, term, 1.0, 1);
    }

    // An empty-string lower bound is a real bound: excluding it drops the
    // empty term while keeping the rest (distinct from a NULL pointer).
    // Safety: the lower bound points to valid (empty) UTF-8; the upper bound
    // is NULL; `t` is a live dictionary not modified while the iterator lives.
    let it = unsafe {
        TermDictionary_IterateRange(t, "".as_ptr().cast(), 0, false, std::ptr::null(), 0, false)
    };
    let actual = keys(drain(it));
    assert_eq!(actual, to_set(&["apple", "banana"]));

    // Including the empty-string bound keeps it.
    let with_empty = keys(drain(range(t, Some(("", true)), None)));
    assert_eq!(with_empty, to_set(&["", "apple", "banana"]));

    free(t);
}

#[test]
fn iterate_range_case_folds_bounds() {
    let t = NewTermDictionary();
    for term in ["Apple", "Banana", "Cherry"] {
        add(t, term, 1.0, 1);
    }

    // Bounds given in a different case still match the folded keys.
    let actual = keys(drain(range(
        t,
        Some(("APPLE", true)),
        Some(("BANANA", true)),
    )));

    assert_eq!(actual, to_set(&["apple", "banana"]));

    free(t);
}

#[test]
fn iterate_dfa_reports_terms_and_distance_within_budget() {
    let t = NewTermDictionary();
    for term in ["bike", "bake", "trike"] {
        add(t, term, 1.0, 1);
    }

    // Whole-term match (prefix_mode = false), edit distance <= 1 of "bike".
    let query = "bike";
    let it = unsafe { TermDictionary_IterateDfa(t, query.as_ptr().cast(), query.len(), 1, false) };
    let actual = drain_with_dist(it);

    assert_eq!(
        actual.get("bike").copied(),
        Some(0),
        "exact match has distance 0"
    );
    assert_eq!(actual.get("bake").copied(), Some(1), "one substitution");
    assert!(
        !actual.contains_key("trike"),
        "distance 2 is outside the budget"
    );

    free(t);
}

#[test]
fn case_folding_unifies_query_and_stored_case() {
    let t = NewTermDictionary();
    add(t, "Bike", 1.0, 1);

    // Lookup with a different case still hits the folded entry.
    let (_score, num_docs) = get(t, "BIKE").expect("lookup is case-insensitive");
    assert_eq!(num_docs, 1);

    free(t);
}

#[test]
fn multibyte_terms_roundtrip() {
    let t = NewTermDictionary();
    add(t, "żółć", 1.0, 1);
    add(t, "köln", 1.0, 1);

    let it = unsafe { TermDictionary_IterateSuffix(t, "ółć".as_ptr().cast(), "ółć".len()) };
    let actual = keys(drain(it));

    assert_eq!(actual, to_set(&["żółć"]));

    free(t);
}

// --- helpers ---------------------------------------------------------------

fn add(
    t: *mut TermDictionary,
    term: &str,
    score: f32,
    num_docs: usize,
) -> TermDictionaryInsertOutcome {
    // Safety: `term` points to valid UTF-8 bytes and no iterator on `t` is alive.
    unsafe {
        TermDictionary_AddTerm(
            t,
            term.as_ptr().cast::<c_char>(),
            term.len(),
            score,
            num_docs,
        )
    }
}

fn get(t: *mut TermDictionary, term: &str) -> Option<(f32, usize)> {
    let mut score = 0.0_f32;
    let mut num_docs = 0_usize;
    // Safety: `term` points to valid UTF-8 bytes; the out-pointers are writable.
    let found = unsafe {
        TermDictionary_Get(
            t,
            term.as_ptr().cast(),
            term.len(),
            &mut score,
            &mut num_docs,
        )
    };
    (found == 1).then_some((score, num_docs))
}

fn decr(t: *mut TermDictionary, term: &str, delta: usize) -> TermDictionaryDecrResult {
    // Safety: `term` points to valid UTF-8 bytes and no iterator on `t` is alive.
    unsafe { TermDictionary_DecrementNumDocs(t, term.as_ptr().cast(), term.len(), delta) }
}

/// Build a range iterator. Each bound is `Some((value, inclusive))` or
/// `None` for an unbounded side (passed to C as a NULL pointer).
fn range<'a>(
    t: *mut TermDictionary,
    min: Option<(&'a str, bool)>,
    max: Option<(&'a str, bool)>,
) -> *mut TermDictionaryIterator<'a> {
    let (min_ptr, min_len, include_min) = match min {
        Some((s, inc)) => (s.as_ptr().cast::<c_char>(), s.len(), inc),
        None => (std::ptr::null(), 0, false),
    };
    let (max_ptr, max_len, include_max) = match max {
        Some((s, inc)) => (s.as_ptr().cast::<c_char>(), s.len(), inc),
        None => (std::ptr::null(), 0, false),
    };
    // Safety: bound pointers are NULL or point to valid UTF-8 that outlives
    // the iterator; `t` is a live dictionary not modified while it lives.
    unsafe {
        TermDictionary_IterateRange(
            t,
            min_ptr,
            min_len,
            include_min,
            max_ptr,
            max_len,
            include_max,
        )
    }
}

fn free(t: *mut TermDictionary) {
    // Safety: `t` was obtained from `NewTermDictionary` and all iterators are freed.
    unsafe { TermDictionary_Free(t) };
}

/// Drain an iterator into a map of term -> (score, num_docs), then free it.
fn drain(it: *mut TermDictionaryIterator) -> HashMap<String, (f32, usize)> {
    let mut out = HashMap::new();
    let mut term = std::ptr::null();
    let mut len = 0;
    let mut score = 0.0_f32;
    let mut num_docs = 0_usize;
    // Safety: `it` is a live iterator; all out-pointers are writable; the yielded
    // bytes are copied before the next advance invalidates them.
    while unsafe {
        TermDictionaryIterator_Next(
            it,
            &mut term,
            &mut len,
            &mut score,
            &mut num_docs,
            std::ptr::null_mut(),
        )
    } == 1
    {
        let bytes = unsafe { std::slice::from_raw_parts(term.cast::<u8>(), len) };
        out.insert(
            String::from_utf8(bytes.to_vec()).unwrap(),
            (score, num_docs),
        );
    }
    // Safety: `it` is a live iterator, not used after this call.
    unsafe { TermDictionaryIterator_Free(it) };
    out
}

/// Drain a DFA iterator into a map of term -> distance, then free it.
fn drain_with_dist(it: *mut TermDictionaryIterator) -> HashMap<String, u32> {
    let mut out = HashMap::new();
    let mut term = std::ptr::null();
    let mut len = 0;
    let mut dist = 0_u32;
    // Safety: `it` is a live iterator; `term`/`len`/`dist` are writable; the
    // yielded bytes are copied before the next advance invalidates them.
    while unsafe {
        TermDictionaryIterator_Next(
            it,
            &mut term,
            &mut len,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            &mut dist,
        )
    } == 1
    {
        let bytes = unsafe { std::slice::from_raw_parts(term.cast::<u8>(), len) };
        out.insert(String::from_utf8(bytes.to_vec()).unwrap(), dist);
    }
    // Safety: `it` is a live iterator, not used after this call.
    unsafe { TermDictionaryIterator_Free(it) };
    out
}

fn keys(map: HashMap<String, (f32, usize)>) -> std::collections::HashSet<String> {
    map.into_keys().collect()
}

fn to_set(terms: &[&str]) -> std::collections::HashSet<String> {
    terms.iter().map(|s| s.to_string()).collect()
}
