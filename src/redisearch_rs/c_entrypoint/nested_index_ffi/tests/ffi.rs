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
    NestedIndex_Free, NestedIndex_IndexDocument, NestedIndex_New, NestedIndex_NumTerminals,
    NestedIndexOutcome, NestedLevelDef, NestedLevelKind, NestedRows_DiscardElement,
    NestedRows_FinishNumeric, NestedRows_FinishTag, NestedRows_Free, NestedRows_New,
    NestedRows_PushNumericKey, NestedRows_PushTagKey, NestedTerminalKind,
};

redis_mock::mock_or_stub_missing_redis_c_symbols!();

const TAG_LEVEL: NestedLevelDef = NestedLevelDef {
    kind: NestedLevelKind::Tag,
    max_cardinality: 0,
    boundaries: std::ptr::null(),
    num_boundaries: 0,
};

/// A `color` tag level over a numeric terminal.
fn new_index() -> *mut nested_index::NestedIndex {
    let mut err = [0 as c_char; 64];
    // SAFETY: the level array and error buffer are valid for their lengths.
    let index = unsafe {
        NestedIndex_New(
            &TAG_LEVEL,
            1,
            NestedTerminalKind::Numeric,
            false,
            err.as_mut_ptr(),
            err.len(),
        )
    };
    assert!(!index.is_null());
    index
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
        // SAFETY: the error buffer was written NUL-terminated, or left zeroed.
        Err(unsafe { CStr::from_ptr(err.as_ptr()) }
            .to_string_lossy()
            .into_owned())
    }
}

#[test]
fn rows_index_each_finished_element_and_skip_discarded_ones() {
    let index = new_index();
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
    let index = new_index();
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
    let index = new_index();
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
    let index = NestedIndex_New_tag();
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

/// A `color` tag level over a tag terminal.
#[expect(non_snake_case, reason = "named after the entry point it wraps")]
fn NestedIndex_New_tag() -> *mut nested_index::NestedIndex {
    let mut err = [0 as c_char; 64];
    // SAFETY: the level array and error buffer are valid for their lengths.
    let index = unsafe {
        NestedIndex_New(
            &TAG_LEVEL,
            1,
            NestedTerminalKind::Tag,
            false,
            err.as_mut_ptr(),
            err.len(),
        )
    };
    assert!(!index.is_null());
    index
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
        // SAFETY: the error buffer was written NUL-terminated.
        let message = unsafe { CStr::from_ptr(err.as_ptr()) }.to_string_lossy();
        assert!(message.contains(expected), "{name}: {message}");
    }
}
