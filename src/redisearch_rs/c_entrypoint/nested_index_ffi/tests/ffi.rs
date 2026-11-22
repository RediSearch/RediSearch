/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The C entry points, driven as C drives them.

use std::ffi::{CStr, c_char};

use nested_index_ffi::{
    NestedIndex_CheckDocument, NestedIndex_Free, NestedIndex_IndexDocument, NestedIndex_New,
    NestedIndex_NumTerminals, NestedIndexOutcome, NestedLevelDef, NestedLevelKind,
    NestedRows_DiscardElement, NestedRows_FinishNumeric, NestedRows_FinishTag, NestedRows_Free,
    NestedRows_New, NestedRows_PushNumericKey, NestedRows_PushTagKey, NestedTerminalKind,
};

extern crate redisearch_rs;

redis_mock::mock_or_stub_missing_redis_c_symbols!();

const TAG_LEVEL: NestedLevelDef = NestedLevelDef {
    kind: NestedLevelKind::Tag,
    max_cardinality: 0,
    boundaries: std::ptr::null(),
    num_boundaries: 0,
};

/// A `color` tag level over a `terminal`.
fn new_index(terminal: NestedTerminalKind) -> *mut nested_index::NestedIndex {
    new_index_with(&[TAG_LEVEL], terminal)
}

fn new_index_with(
    levels: &[NestedLevelDef],
    terminal: NestedTerminalKind,
) -> *mut nested_index::NestedIndex {
    let mut err = [0 as c_char; 64];
    // SAFETY: the level array and error buffer are valid for their lengths.
    let index = unsafe {
        NestedIndex_New(
            levels.as_ptr(),
            levels.len(),
            terminal,
            false,
            err.as_mut_ptr(),
            err.len(),
        )
    };
    assert!(!index.is_null());
    index
}

/// The NUL-terminated text in `err`.
fn error_text(err: &[c_char]) -> String {
    // SAFETY: the error buffer was written NUL-terminated, or left zeroed.
    unsafe { CStr::from_ptr(err.as_ptr()) }
        .to_string_lossy()
        .into_owned()
}

fn push_tag(rows: *mut nested_index_ffi::NestedRows, value: &str) {
    // SAFETY: `rows` is live, and `value` is valid for its length.
    unsafe { NestedRows_PushTagKey(rows, value.as_ptr().cast(), value.len()) };
}

/// Index `rows` as document `doc_id`, returning the outcome or the error text.
fn index_document(
    index: *mut nested_index::NestedIndex,
    doc_id: u64,
    rows: *const nested_index_ffi::NestedRows,
    err_len: usize,
) -> Result<NestedIndexOutcome, String> {
    let mut outcome = NestedIndexOutcome::default();
    let mut err = vec![0 as c_char; err_len.max(1)];
    // SAFETY: every pointer is live and valid for its length.
    let ok = unsafe {
        NestedIndex_IndexDocument(
            index,
            doc_id,
            rows,
            0,
            &mut outcome,
            err.as_mut_ptr(),
            err_len,
        )
    };
    if ok {
        Ok(outcome)
    } else {
        Err(error_text(&err))
    }
}

/// Check `rows` against `index`, returning the error text on rejection.
fn check_document(
    index: *const nested_index::NestedIndex,
    rows: *const nested_index_ffi::NestedRows,
) -> Result<(), String> {
    let mut err = [0 as c_char; 128];
    // SAFETY: every pointer is live and valid for its length.
    let ok = unsafe { NestedIndex_CheckDocument(index, rows, err.as_mut_ptr(), err.len()) };
    if ok { Ok(()) } else { Err(error_text(&err)) }
}

#[test]
fn rows_index_each_finished_element_and_skip_discarded_ones() {
    let index = new_index(NestedTerminalKind::Numeric);
    let rows = NestedRows_New(2);
    push_tag(rows, "red");
    // SAFETY: `rows` is live.
    unsafe { NestedRows_FinishNumeric(rows, 20.0) };
    push_tag(rows, "blue");
    // A skipped element: its key is dropped, and it never reaches the index.
    // SAFETY: `rows` is live.
    unsafe { NestedRows_DiscardElement(rows) };
    push_tag(rows, "green");
    // SAFETY: `rows` is live.
    unsafe { NestedRows_FinishNumeric(rows, 5.0) };

    let outcome = index_document(index, 1, rows, 64).unwrap();
    assert_eq!(outcome.new_terminals, 2);
    assert_eq!(outcome.records, 2);
    // SAFETY: `index` is live.
    assert_eq!(unsafe { NestedIndex_NumTerminals(index) }, 2);
    // SAFETY: both were created above and are freed once.
    unsafe {
        NestedRows_Free(rows);
        NestedIndex_Free(index);
    }
}

#[test]
fn a_rejected_document_reports_why_and_changes_nothing() {
    let index = new_index(NestedTerminalKind::Numeric);
    let rows = NestedRows_New(2);
    push_tag(rows, "red");
    // SAFETY: `rows` is live.
    unsafe { NestedRows_FinishNumeric(rows, f64::NAN) };

    let err = index_document(index, 1, rows, 64).unwrap_err();
    assert!(err.contains("NaN"), "{err}");
    // SAFETY: `index` is live.
    assert_eq!(unsafe { NestedIndex_NumTerminals(index) }, 0);
    // SAFETY: both were created above and are freed once.
    unsafe {
        NestedRows_Free(rows);
        NestedIndex_Free(index);
    }
}

#[test]
fn errors_are_truncated_to_the_buffer() {
    let index = new_index(NestedTerminalKind::Numeric);
    let rows = NestedRows_New(2);
    // SAFETY: `rows` is live.
    unsafe { NestedRows_PushNumericKey(rows, 1.0) };
    // SAFETY: `rows` is live.
    unsafe { NestedRows_FinishNumeric(rows, 1.0) };

    // A numeric key on a tag level is rejected; four bytes leave room for three and the NUL.
    let err = index_document(index, 1, rows, 4).unwrap_err();
    assert_eq!(err.len(), 3);
    // SAFETY: both were created above and are freed once.
    unsafe {
        NestedRows_Free(rows);
        NestedIndex_Free(index);
    }
}

#[test]
fn a_tag_value_with_a_nul_byte_is_rejected() {
    let index = new_index(NestedTerminalKind::Tag);
    let rows = NestedRows_New(2);
    push_tag(rows, "red");
    let value = "st\0eel";
    // SAFETY: `rows` is live, and `value` is valid for its length.
    unsafe { NestedRows_FinishTag(rows, value.as_ptr().cast(), value.len()) };

    let err = index_document(index, 1, rows, 128).unwrap_err();
    assert!(err.contains("NUL"), "{err}");
    // SAFETY: both were created above and are freed once.
    unsafe {
        NestedRows_Free(rows);
        NestedIndex_Free(index);
    }
}

#[test]
fn invalid_levels_are_rejected_with_a_reason() {
    let decreasing = [5.0, 1.0];
    let cases: &[(&str, &[NestedLevelDef], &str)] = &[
        ("no routing level", &[], "at least one routing level"),
        (
            "decreasing bucket boundaries",
            &[NestedLevelDef {
                kind: NestedLevelKind::Buckets,
                max_cardinality: 0,
                boundaries: decreasing.as_ptr(),
                num_boundaries: decreasing.len(),
            }],
            "not greater than the previous",
        ),
    ];
    for (name, levels, expected) in cases {
        let mut err = [0 as c_char; 128];
        // SAFETY: the level array, its boundaries and the error buffer are valid.
        let index = unsafe {
            NestedIndex_New(
                levels.as_ptr(),
                levels.len(),
                NestedTerminalKind::Numeric,
                false,
                err.as_mut_ptr(),
                err.len(),
            )
        };
        assert!(index.is_null(), "{name}");
        let message = error_text(&err);
        assert!(message.contains(expected), "{name}: {message}");
    }
}

#[test]
fn a_zero_length_error_buffer_is_left_untouched() {
    let index = new_index(NestedTerminalKind::Numeric);
    let rows = NestedRows_New(2);
    push_tag(rows, "red");
    // SAFETY: `rows` is live.
    unsafe { NestedRows_FinishNumeric(rows, f64::NAN) };

    // `index_document` hands a one-byte zeroed buffer but says its length is zero.
    let err = index_document(index, 1, rows, 0).unwrap_err();
    assert_eq!(err, "");
    // SAFETY: both were created above and are freed once.
    unsafe {
        NestedRows_Free(rows);
        NestedIndex_Free(index);
    }
}

#[test]
fn a_buckets_level_routes_numeric_keys() {
    let boundaries = [0.0, 10.0, 20.0];
    let index = new_index_with(
        &[NestedLevelDef {
            kind: NestedLevelKind::Buckets,
            max_cardinality: 0,
            boundaries: boundaries.as_ptr(),
            num_boundaries: boundaries.len(),
        }],
        NestedTerminalKind::Numeric,
    );
    let rows = NestedRows_New(2);
    for (key, value) in [(1.0, 1.0), (5.0, 2.0), (15.0, 3.0)] {
        // SAFETY: `rows` is live.
        unsafe {
            NestedRows_PushNumericKey(rows, key);
            NestedRows_FinishNumeric(rows, value);
        }
    }

    let outcome = index_document(index, 1, rows, 64).unwrap();
    // 1 and 5 share the first bucket.
    assert_eq!(outcome.new_terminals, 2);
    assert_eq!(outcome.records, 3);
    // SAFETY: both were created above and are freed once.
    unsafe {
        NestedRows_Free(rows);
        NestedIndex_Free(index);
    }
}

#[test]
fn a_tag_terminal_counts_its_records() {
    let index = new_index(NestedTerminalKind::Tag);
    let rows = NestedRows_New(2);
    for (key, value) in [("red", "steel"), ("red", "wood"), ("blue", "steel")] {
        push_tag(rows, key);
        // SAFETY: `rows` is live, and `value` is valid for its length.
        unsafe { NestedRows_FinishTag(rows, value.as_ptr().cast(), value.len()) };
    }

    let outcome = index_document(index, 1, rows, 64).unwrap();
    assert_eq!(outcome.new_terminals, 2);
    assert_eq!(outcome.records, 3);
    assert!(outcome.size_delta > 0);
    // SAFETY: both were created above and are freed once.
    unsafe {
        NestedRows_Free(rows);
        NestedIndex_Free(index);
    }
}

#[test]
fn doc_ids_must_increase() {
    let index = new_index(NestedTerminalKind::Numeric);
    let rows = NestedRows_New(2);
    push_tag(rows, "red");
    // SAFETY: `rows` is live.
    unsafe { NestedRows_FinishNumeric(rows, 1.0) };

    index_document(index, 2, rows, 64).unwrap();
    let err = index_document(index, 2, rows, 128).unwrap_err();
    assert!(err.contains("doc id"), "{err}");
    // SAFETY: both were created above and are freed once.
    unsafe {
        NestedRows_Free(rows);
        NestedIndex_Free(index);
    }
}

#[test]
fn check_document_agrees_with_index_document_and_writes_nothing() {
    let index = new_index(NestedTerminalKind::Numeric);
    let good = NestedRows_New(2);
    push_tag(good, "red");
    // SAFETY: `good` is live.
    unsafe { NestedRows_FinishNumeric(good, 1.0) };
    let bad = NestedRows_New(2);
    // SAFETY: `bad` is live.
    unsafe {
        NestedRows_PushNumericKey(bad, 1.0);
        NestedRows_FinishNumeric(bad, 1.0);
    }

    check_document(index, good).unwrap();
    let err = check_document(index, bad).unwrap_err();
    assert_eq!(err, index_document(index, 1, bad, 128).unwrap_err());
    // SAFETY: `index` is live.
    assert_eq!(unsafe { NestedIndex_NumTerminals(index) }, 0);
    // SAFETY: all were created above and are freed once.
    unsafe {
        NestedRows_Free(good);
        NestedRows_Free(bad);
        NestedIndex_Free(index);
    }
}
