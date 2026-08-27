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

// Link both Rust-provided and C-provided symbols
extern crate redisearch_rs;
// Mock or stub the ones that aren't provided by the line above
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
        // Safety: `t` is a live dictionary; the term pointer/len come from a valid `&str`.
        unsafe { TermDictionary_ReplaceTerm(t, "bike".as_ptr().cast(), "bike".len(), 1.0, 4) };
    assert_eq!(outcome, TermDictionaryInsertOutcome::Updated);

    let (score, num_docs) = get(t, "bike").unwrap();
    assert_eq!(score, 1.0, "ADD_REPLACE overwrites score");
    assert_eq!(num_docs, 7, "ADD_REPLACE still accumulates num_docs");

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

    // Safety: `t` is a live dictionary; NULL out-pointers are explicitly allowed.
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

    // Safety: `t` is a live dictionary; the term pointer/len come from a valid `&str`.
    let removed = unsafe { TermDictionary_Remove(t, "bike".as_ptr().cast(), "bike".len()) };
    assert_eq!(removed, 1);
    assert!(get(t, "bike").is_none());

    // Safety: `t` is a live dictionary; the term pointer/len come from a valid `&str`.
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

/// The eligibility rules the C terms trie applies to a term, as a table
/// of `(term, is_storable)` cases around each bound.
fn eligibility_cases() -> Vec<(String, bool)> {
    let ascii = |n: usize| "a".repeat(n);
    // Three bytes per codepoint, so the byte bound bites before the rune one.
    let cjk = |n: usize| "\u{4e2d}".repeat(n);

    vec![
        (String::new(), false),
        (ascii(1), true),
        (ascii(MAX_TERM_RUNES - 1), true),
        (ascii(MAX_TERM_RUNES), false),
        (cjk(MAX_TERM_BYTES / 3), true),
        (cjk(MAX_TERM_BYTES / 3 + 1), false),
    ]
}

#[test]
fn add_term_applies_the_c_trie_eligibility_rules() {
    for (term, storable) in eligibility_cases() {
        let t = NewTermDictionary();

        let outcome = add(t, &term, 1.0, 1);

        if storable {
            assert_eq!(
                outcome,
                TermDictionaryInsertOutcome::New,
                "{} bytes / {} runes is storable",
                term.len(),
                term.chars().count()
            );
            assert!(get(t, &term).is_some());
        } else {
            assert_eq!(
                outcome,
                TermDictionaryInsertOutcome::Unsupported,
                "{} bytes / {} runes is not storable",
                term.len(),
                term.chars().count()
            );
            assert!(get(t, &term).is_none(), "nothing was stored");
            // Safety: `t` is a live dictionary.
            assert_eq!(unsafe { TermDictionary_Len(t) }, 0);
        }

        free(t);
    }
}

#[test]
fn replace_rejects_the_same_terms_as_add() {
    for (term, storable) in eligibility_cases() {
        if storable {
            continue;
        }

        let t = NewTermDictionary();

        // Safety: `t` is a live dictionary; the term pointer/len come from a valid `&str`.
        let replaced =
            unsafe { TermDictionary_ReplaceTerm(t, term.as_ptr().cast(), term.len(), 1.0, 1) };

        assert_eq!(replaced, TermDictionaryInsertOutcome::Unsupported);
        // Safety: `t` is a live dictionary.
        assert_eq!(unsafe { TermDictionary_Len(t) }, 0);

        free(t);
    }
}

#[test]
fn decrement_num_docs_separates_unsupported_from_not_found() {
    let t = NewTermDictionary();

    let too_long = "a".repeat(MAX_TERM_RUNES);
    assert_eq!(decr(t, &too_long, 1), TermDictionaryDecrResult::Unsupported);
    assert_eq!(decr(t, "", 1), TermDictionaryDecrResult::Unsupported);
    assert_eq!(decr(t, "bike", 1), TermDictionaryDecrResult::NotFound);

    free(t);
}

#[test]
fn a_term_of_max_bytes_is_storable_when_its_rune_count_fits() {
    let t = NewTermDictionary();
    // Exactly at the byte bound, but 254 runes: under both.
    let term = "\u{4e2d}".repeat(129) + &"a".repeat(MAX_TERM_BYTES - 129 * 3);
    assert_eq!(term.len(), MAX_TERM_BYTES);
    assert!(term.chars().count() < MAX_TERM_RUNES);

    let outcome = add(t, &term, 1.0, 1);

    assert_eq!(outcome, TermDictionaryInsertOutcome::New);

    free(t);
}

#[test]
fn len_and_mem_usage_grow_with_content() {
    let t = NewTermDictionary();
    // Safety: `t` is a live dictionary.
    let empty_mem = unsafe { TermDictionary_MemUsage(t) };
    // Safety: `t` is a live dictionary.
    assert_eq!(unsafe { TermDictionary_Len(t) }, 0);

    add(t, "bicycle", 1.0, 1);

    // Safety: `t` is a live dictionary.
    assert_eq!(unsafe { TermDictionary_Len(t) }, 1);
    // Safety: `t` is a live dictionary.
    assert!(unsafe { TermDictionary_MemUsage(t) } > empty_mem);

    free(t);
}

#[test]
fn iterate_yields_all_terms_with_payloads() {
    let t = NewTermDictionary();
    add(t, "bike", 1.0, 2);
    add(t, "trike", 3.0, 4);

    // Safety: `t` is a live dictionary that outlives the iterator.
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

    // Safety: `t` is a live dictionary that outlives the iterator; the pattern bytes come from a valid `&str`.
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

    // Safety: `t` is a live dictionary that outlives the iterator; the pattern bytes come from a valid `&str`.
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
    // Safety: `t` is a live dictionary that outlives the iterator; the pattern bytes come from a valid `&str`.
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
    // Safety: `t` is a live dictionary that outlives the iterator; the pattern bytes come from a valid `&str`.
    let it = unsafe { TermDictionary_IterateWildcard(t, pattern.as_ptr().cast(), pattern.len()) };
    let actual = keys(drain(it));

    assert_eq!(actual, to_set(&["bike"]));

    free(t);
}

#[test]
fn empty_suffix_and_substring_yield_no_terms() {
    let t = NewTermDictionary();
    for term in ["bike", "trike"] {
        add(t, term, 1.0, 1);
    }

    // Safety: `t` is a live dictionary that outlives both iterators; the empty pattern bytes come
    // from a valid `&str`.
    let suffixed = unsafe { TermDictionary_IterateSuffix(t, "".as_ptr().cast(), 0) };
    // Safety: as above.
    let contained = unsafe { TermDictionary_IterateContains(t, "".as_ptr().cast(), 0) };

    assert!(keys(drain(suffixed)).is_empty());
    assert!(keys(drain(contained)).is_empty());

    free(t);
}

#[test]
fn iterate_fuzzy_reports_terms_within_budget() {
    let t = NewTermDictionary();
    for term in ["bike", "bake", "trike"] {
        add(t, term, 1.0, 1);
    }

    let query = "bike";
    // Safety: `t` is a live dictionary that outlives the iterator; the pattern bytes come from a valid `&str`.
    let it = unsafe { TermDictionary_IterateFuzzy(t, query.as_ptr().cast(), query.len(), 1) };
    let actual = drain(it);

    assert!(actual.contains_key("bike"), "exact match");
    assert!(actual.contains_key("bake"), "one substitution");
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

    // Safety: `t` is a live dictionary that outlives the iterator; the pattern bytes come from a valid `&str`.
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
    while unsafe { TermDictionaryIterator_Next(it, &mut term, &mut len, &mut score, &mut num_docs) }
        == 1
    {
        // Safety: `term`/`len` were written by the successful `Next` above and remain valid until the next advance.
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

fn keys(map: HashMap<String, (f32, usize)>) -> std::collections::HashSet<String> {
    map.into_keys().collect()
}

fn to_set(terms: &[&str]) -> std::collections::HashSet<String> {
    terms.iter().map(|s| s.to_string()).collect()
}
