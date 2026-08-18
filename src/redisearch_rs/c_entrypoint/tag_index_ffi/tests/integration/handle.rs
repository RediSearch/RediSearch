/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The mode-erased handle: which union field a constructor selects, that the
//! accessors read the one that is live, and that the lifecycle is sound.

use std::ffi::{CString, c_char};

use tag_index_ffi::{
    Rust_TagIndex_Commit, Rust_TagIndex_Free, Rust_TagIndex_GetId, Rust_TagIndex_GetOverhead,
    Rust_TagIndex_HasDiskSpec, Rust_TagIndex_HasSuffix, Rust_TagIndex_Index,
    Rust_TagIndex_NUniqueValues, RustTagIndex,
};

/// Build a memory-mode index the way `NewTagIndex` does for a non-disk spec.
pub fn new_in_memory(with_suffix: bool) -> *mut RustTagIndex {
    // SAFETY: a NULL `disk_spec` selects memory mode, which reads neither the
    // spec nor the field index.
    unsafe { tag_index_ffi::Rust_TagIndex_New(std::ptr::null_mut(), 0, with_suffix) }
}

/// Hold `tags` as C strings alongside the `const char **` array C would pass.
pub struct CValues {
    _owned: Vec<CString>,
    ptrs: Vec<*const c_char>,
}

impl CValues {
    pub fn new(tags: &[&str]) -> Self {
        let owned: Vec<CString> = tags
            .iter()
            .map(|t| CString::new(*t).expect("test literal is NUL-free"))
            .collect();
        let ptrs = owned.iter().map(|t| t.as_ptr()).collect();
        Self {
            _owned: owned,
            ptrs,
        }
    }

    pub fn as_ptr(&self) -> *const *const c_char {
        self.ptrs.as_ptr()
    }

    pub fn len(&self) -> usize {
        self.ptrs.len()
    }
}

/// Index `tags` under `doc_id` and run the commit phase, as `document.c` does.
pub fn index_and_commit(idx: *mut RustTagIndex, tags: &[&str], doc_id: u64) {
    let values = CValues::new(tags);
    // SAFETY: `idx` is live and `values` holds `len()` valid C strings.
    let result = unsafe {
        Rust_TagIndex_Index(
            idx,
            std::ptr::null(),
            std::ptr::null(),
            values.as_ptr(),
            values.len(),
            doc_id,
            false,
        )
    };
    assert!(result.ok, "memory-mode indexing is infallible");
    // SAFETY: as above.
    unsafe { Rust_TagIndex_Commit(idx, values.as_ptr(), values.len()) };
}

#[test]
fn null_disk_spec_selects_memory_mode() {
    let idx = new_in_memory(false);

    // SAFETY: `idx` is live.
    assert!(!unsafe { Rust_TagIndex_HasDiskSpec(idx) });
    // SAFETY: `idx` is live.
    assert!(!unsafe { Rust_TagIndex_HasSuffix(idx) });

    free(idx);
}

#[test]
fn with_suffix_is_reported_back() {
    let idx = new_in_memory(true);

    // SAFETY: `idx` is live.
    assert!(unsafe { Rust_TagIndex_HasSuffix(idx) });

    free(idx);
}

#[test]
fn each_index_gets_a_distinct_id() {
    let first = new_in_memory(false);
    let second = new_in_memory(false);

    // SAFETY: both handles are live.
    let (a, b) = unsafe { (Rust_TagIndex_GetId(first), Rust_TagIndex_GetId(second)) };
    assert_ne!(
        a, b,
        "ids distinguish a field's index from its replacement across a GC cycle"
    );

    free(first);
    free(second);
}

#[test]
fn indexing_grows_the_distinct_tag_count() {
    let idx = new_in_memory(false);

    // SAFETY: `idx` is live.
    assert_eq!(unsafe { Rust_TagIndex_NUniqueValues(idx) }, 0);

    index_and_commit(idx, &["red", "green"], 1);
    // SAFETY: `idx` is live.
    assert_eq!(unsafe { Rust_TagIndex_NUniqueValues(idx) }, 2);

    // "red" is already there, so only "blue" is new.
    index_and_commit(idx, &["red", "blue"], 2);
    // SAFETY: `idx` is live.
    assert_eq!(unsafe { Rust_TagIndex_NUniqueValues(idx) }, 3);

    free(idx);
}

#[test]
fn a_repeated_tag_in_one_document_counts_once() {
    let idx = new_in_memory(false);

    let values = CValues::new(&["red", "red", "green"]);
    // SAFETY: `idx` is live and `values` holds three valid C strings.
    let result = unsafe {
        Rust_TagIndex_Index(
            idx,
            std::ptr::null(),
            std::ptr::null(),
            values.as_ptr(),
            values.len(),
            1,
            false,
        )
    };

    assert_eq!(
        result.num_records, 2,
        "a value repeated within one document adds one posting, not two"
    );

    free(idx);
}

#[test]
fn overhead_grows_with_the_tries() {
    let idx = new_in_memory(false);

    // SAFETY: `idx` is live.
    let empty = unsafe { Rust_TagIndex_GetOverhead(idx) };
    index_and_commit(idx, &["red", "green", "blue"], 1);
    // SAFETY: `idx` is live.
    let populated = unsafe { Rust_TagIndex_GetOverhead(idx) };

    assert!(
        populated > empty,
        "FT.INFO's tag overhead tracks the values trie: {empty} -> {populated}"
    );

    free(idx);
}

#[test]
fn free_nulls_the_callers_slot() {
    let mut slot = new_in_memory(true);
    index_and_commit(slot, &["red"], 1);

    // SAFETY: `slot` holds a live handle.
    unsafe { Rust_TagIndex_Free(&raw mut slot) };

    assert!(slot.is_null(), "freeing must null the field spec's pointer");

    // Freeing the same slot again is a no-op, which is what makes
    // `FieldSpec_Cleanup` safe to run twice.
    //
    // SAFETY: `slot` is a valid slot holding NULL.
    unsafe { Rust_TagIndex_Free(&raw mut slot) };
}

/// Release `idx`, checking the slot is nulled.
fn free(idx: *mut RustTagIndex) {
    let mut slot = idx;
    // SAFETY: `slot` holds a live handle from `new_in_memory`.
    unsafe { Rust_TagIndex_Free(&raw mut slot) };
    assert!(slot.is_null());
}

#[test]
fn a_null_tag_entry_is_skipped() {
    // `TagIndex_Preprocess` appends a NULL entry for an `INDEXEMPTY` field whose
    // value is neither empty nor separator-terminated. Both C write paths skipped
    // it, and a NULL means "no tag here" — not the empty tag, which arrives as "".
    let idx = new_in_memory(false);

    let red = CString::new("red").expect("literal is NUL-free");
    let values: [*const c_char; 3] = [red.as_ptr(), std::ptr::null(), red.as_ptr()];

    // SAFETY: `idx` is live; the array holds one valid C string, a NULL, and a
    // repeat of the same string.
    let result = unsafe {
        Rust_TagIndex_Index(
            idx,
            std::ptr::null(),
            std::ptr::null(),
            values.as_ptr(),
            values.len(),
            1,
            false,
        )
    };
    assert!(result.ok);
    // SAFETY: as above.
    unsafe { Rust_TagIndex_Commit(idx, values.as_ptr(), values.len()) };

    // SAFETY: `idx` is live.
    assert_eq!(
        unsafe { Rust_TagIndex_NUniqueValues(idx) },
        1,
        "the NULL contributes no tag, and the repeat is the same tag"
    );

    free(idx);
}
