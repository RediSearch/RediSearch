/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! [`TagSuffixIndex`] is created when `WITHSUFFIXTRIE` option is given.
//!
//! For each tags, [`TagSuffixIndex`] stores all the suffixes as key.
//!
//! # Memory model
//!
//! The values of this index are [`SuffixData`] which stores:
//! - owned term (one occurrence)
//! - borrowed term (N occurrence, one for each suffix)
//!
//! [`OwnedTerm`] holds the owned memory, while [`TermPtr`] points to the owned memory.
//! [`OwnedTerm`] frees the memory on drop, [`TermPtr`] doesn't.
//! Adding and removing items from this trie require order-aware operations.
//!
//! For instance, during the insertion:
//! - insert owned term under tag key
//! - for each suffix:
//!   - insert borrowed term under the suffix key
//!

use std::{
    alloc::{Layout, alloc, dealloc, handle_alloc_error},
    ptr::NonNull,
};

use thin_vec::{AlignedU32, ThinVec};
use trie_rs::TrieMap;

/// Layout of a tag term allocation.
fn tag_term_layout(size: usize) -> Layout {
    let align = align_of::<u16>();

    // `align` is not 0 and power of two
    // `size` is less than isize::MAX
    Layout::from_size_align(size, align)
        .expect("tag term length is bounded by u16::MAX, far below Layout's limits")
}

/// Owning handle to a tag term allocation.
///
/// Dropping it frees the allocation.
#[derive(Debug)]
struct OwnedTerm(NonNull<u8>);

impl OwnedTerm {
    /// Copy `term` into a fresh allocation.
    ///
    /// # Safety
    /// 1. term is NULL terminated and cannot contain NULL inside
    /// 2. term is not empty (it contains at least the null)
    ///
    /// # Panics
    /// - the method panics if the `term` is longer than [`u16::MAX`] bytes.
    unsafe fn new(term: &[u8]) -> Self {
        #[cfg(debug_assertions)]
        {
            debug_assert!(!term.is_empty(), "term shouldn't be empty");
            debug_assert_eq!(
                *term.last().expect("term shouldn't be empty"),
                0,
                "term should be NULL terminated"
            );
        }

        let len = u16::try_from(term.len()).expect("caller rejects terms longer than u16::MAX");

        let layout = tag_term_layout(len as usize);

        // SAFETY: `layout` has non-zero size (guarantee 2)
        let ptr = unsafe { alloc(layout) };
        let Some(ptr) = NonNull::new(ptr) else {
            handle_alloc_error(layout);
        };

        // SAFETY: source and destination are valid for `term.len()` bytes
        // and cannot overlap because `dst` is a freshly allocated block.
        unsafe { std::ptr::copy_nonoverlapping(term.as_ptr(), ptr.as_ptr(), len as usize) };

        Self(ptr)
    }

    /// Full allocation size in bytes (term bytes + the trailing NUL).
    ///
    /// # Safety
    /// `self` is built by [`OwnedTerm::new`]
    const unsafe fn alloc_size(&self) -> usize {
        // This cast doesn't change size, we care about only the NULL
        let ptr = self.0.as_ptr().cast::<std::ffi::c_char>().cast_const();
        // SAFETY: guarantee 1 of [`OwnedTerm::new`]
        unsafe { std::ffi::CStr::from_ptr(ptr) }
            .to_bytes_with_nul()
            .len()
    }
}

impl Drop for OwnedTerm {
    fn drop(&mut self) {
        // SAFETY: `self` is allocated using new method.
        let len = unsafe { self.alloc_size() };

        // SAFETY: `self.0` came from `OwnedTerm::new` with exactly this
        // layout, and this is the only deallocation.
        unsafe { dealloc(self.0.as_ptr(), tag_term_layout(len)) };
    }
}

/// Weak, thin (8-byte) pointer to a term allocation.
///
/// The pointee is owned by the [`OwnedTerm`] of the member's own trie entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TermPtr(NonNull<u8>);

/// Payload of one trie entry.
#[derive(Debug, Default)]
struct SuffixData {
    /// `Some` iff this entry's key is itself a member: the owning handle of
    /// that member's tag term allocation.
    full_term: Option<OwnedTerm>,
    /// Every member this entry's key is a suffix of.
    refs: ThinVec<TermPtr, AlignedU32>,
}

#[derive(Debug, Default)]
pub struct TagSuffixIndex {
    /// The suffix entries
    entries: TrieMap<SuffixData>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Read back the bytes stored in an [`OwnedTerm`].
    ///
    /// # Safety
    /// `t` must be a live [`OwnedTerm`] produced by [`OwnedTerm::new`].
    unsafe fn read_back(t: &OwnedTerm) -> Vec<u8> {
        // SAFETY: guaranteed by Safety rule
        let len = unsafe { t.alloc_size() };

        // SAFETY: `src` is valid for `len` initialized bytes.
        unsafe { std::slice::from_raw_parts(t.0.as_ptr(), len) }.to_vec()
    }

    #[test]
    fn roundtrip() {
        let term = unsafe { OwnedTerm::new(b"hello\0") };
        // SAFETY: `term` is live and built by `OwnedTerm::new`.
        assert_eq!(unsafe { read_back(&term) }, b"hello\0");
        // `term` drops here; miri checks the alloc/dealloc layout match.
    }

    #[test]
    fn empty_term() {
        let term = unsafe { OwnedTerm::new(b"\0") };
        // SAFETY: `term` is live and built by `OwnedTerm::new`.
        assert_eq!(unsafe { read_back(&term) }, b"\0");
    }

    #[test]
    fn larger_term() {
        let mut expected = vec![0xABu8; 300];
        expected.push(0);
        let term = unsafe { OwnedTerm::new(&expected) };
        // SAFETY: `term` is live and built by `OwnedTerm::new`.
        assert_eq!(unsafe { read_back(&term) }, expected);
    }

    #[test]
    #[should_panic(expected = "caller rejects terms longer than u16::MAX")]
    fn too_long_panics() {
        // The panic fires at `u16::try_from` before any allocation happens.
        let mut term = vec![1u8; u16::MAX as usize + 1];
        term.push(0);
        let _ = unsafe { OwnedTerm::new(&term) };
    }
}
