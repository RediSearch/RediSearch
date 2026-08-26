/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The suffix-expansion entry points, driven as `query.c` drives them: the
//! expansion cap bounds the array before it is built, and the wildcard parser
//! takes the pattern in the already-unescaped domain C hands over.

use std::ffi::{CStr, c_char, c_longlong};

use tag_index_ffi::{
    ErasedTagIndex, Rust_TagIndex_GetSuffixMatches, Rust_TagIndex_GetSuffixWildcardMatches,
};

use crate::handle::{free, index_and_commit, new_in_memory};

/// Every test here bounds the walk by the cap alone, so the deadline is skipped
/// and its value never read.
const NO_DEADLINE: ffi::timespec = ffi::timespec {
    tv_sec: 0,
    tv_nsec: 0,
};

/// Read back an `arr.h` array of borrowed C strings and free it, as the caller
/// of both entry points does.
///
/// # Safety
///
/// `arr` must be a non-NULL array from one of the suffix entry points, over an
/// index that has not been mutated since.
unsafe fn take_matches(arr: *mut *mut c_char) -> Vec<String> {
    assert!(!arr.is_null(), "expected matches");

    // SAFETY: the caller guarantees `arr` is one of our arrays.
    let len = unsafe { ffi::array_len_func(arr.cast()) } as usize;
    let terms = (0..len)
        .map(|i| {
            // SAFETY: `i` is in bounds, and each element is a NUL-terminated
            // term borrowed from the still-live suffix index.
            let term = unsafe { *arr.add(i) };
            unsafe { CStr::from_ptr(term) }
                .to_str()
                .expect("test terms are UTF-8")
                .to_owned()
        })
        .collect();

    // SAFETY: only the array is ours to free; the terms are borrowed.
    unsafe { ffi::array_free(arr.cast()) };

    terms
}

/// Expand `pattern` as a *contains* query, i.e. what `*pattern*` reaches.
///
/// # Safety
///
/// `idx` must be a live index created `WITHSUFFIXTRIE`.
unsafe fn contains(idx: *mut ErasedTagIndex, pattern: &str, cap: c_longlong) -> Vec<String> {
    // SAFETY: `idx` is live with a suffix index, and `pattern` is readable.
    let arr = unsafe {
        Rust_TagIndex_GetSuffixMatches(
            idx,
            pattern.as_ptr().cast(),
            pattern.len(),
            /* prefix (contains) = */ true,
            NO_DEADLINE,
            cap,
            /* skip_timeout_checks = */ true,
        )
    };
    // SAFETY: the array came from the call above and nothing mutated `idx`.
    unsafe { take_matches(arr) }
}

/// The cap has to bound the expansion itself, not just what the caller keeps:
/// many tags sharing one ending would otherwise all be materialised — into a
/// `Vec` and then a C array — before `query.c` got to stop at its own
/// `maxPrefixExpansions`.
#[test]
fn the_expansion_cap_bounds_the_array_that_is_built() {
    let idx = new_in_memory(true);
    let tags: Vec<String> = (0..50).map(|i| format!("tag{i:02}shared")).collect();
    for (doc_id, tag) in tags.iter().enumerate() {
        index_and_commit(idx, &[tag.as_str()], doc_id as u64 + 1);
    }

    // Uncapped, the ending reaches every tag.
    // SAFETY: `idx` is live and has a suffix index.
    assert_eq!(unsafe { contains(idx, "shared", -1) }.len(), 50);

    // Capped, it stops one past the cap — the overshoot is what tells the
    // caller there were more, so it can warn.
    // SAFETY: as above.
    assert_eq!(unsafe { contains(idx, "shared", 10) }.len(), 11);

    free(idx);
}

/// A cap of zero still yields the one overshoot element, and never NULL for a
/// pattern that does match.
#[test]
fn a_zero_cap_still_reports_that_there_were_matches() {
    let idx = new_in_memory(true);
    index_and_commit(idx, &["alpha", "beta"], 1);

    // SAFETY: `idx` is live and has a suffix index.
    assert_eq!(unsafe { contains(idx, "a", 0) }.len(), 1);

    free(idx);
}

/// `Wildcard_RemoveEscape` runs before this boundary, so a query for a literal
/// backslash arrives with a trailing `\` that escapes nothing. That must expand
/// rather than panic, and must anchor on the literal token — an anchor of `\`
/// would select only terms the full-pattern recheck then rejects.
#[test]
fn a_pattern_ending_in_a_dangling_escape_still_expands() {
    let idx = new_in_memory(true);
    index_and_commit(idx, &["abc", "abcd", "zzz"], 1);

    // What `Wildcard_RemoveEscape` leaves of the query `abc*\\`.
    let pattern = "abc*\\";
    // SAFETY: `idx` is live with a suffix index, and `pattern` is readable.
    let arr = unsafe {
        Rust_TagIndex_GetSuffixWildcardMatches(
            idx,
            pattern.as_ptr().cast(),
            pattern.len(),
            NO_DEADLINE,
            -1,
            /* skip_timeout_checks = */ true,
        )
    };
    // SAFETY: the array came from the call above and nothing mutated `idx`.
    let mut terms = unsafe { take_matches(arr) };
    terms.sort();

    // The trailing backslash is dropped by both the anchor and the recheck, so
    // the pattern reads as `abc*`.
    assert_eq!(terms, ["abc", "abcd"]);

    free(idx);
}
