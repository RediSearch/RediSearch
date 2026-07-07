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
//! [`OwnedTerm`] uses:
//! - the first two bytes (the size of u16) as header to store the string length
//! - the remaining bytes as the string
//!

use std::{
    alloc::{Layout, alloc, dealloc, handle_alloc_error},
    ptr::NonNull,
};

use thin_vec::{AlignedU32, ThinVec};
use trie_rs::TrieMap;

/// [`OwnedTerm`] header length
const TERM_LEN_PREFIX: usize = size_of::<u16>();

/// Layout of a tag term allocation: length prefix followed by the term bytes,
/// aligned for the `u16` prefix.
fn tag_term_layout(len: usize) -> Layout {
    let align = align_of::<u16>();
    let size = TERM_LEN_PREFIX + len;

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
    /// Copy `term` into a fresh length-prefixed allocation.
    ///
    /// # Panics
    /// - the method panics if the `term` is longer than [`u16::MAX`] bytes.
    fn new(term: &[u8]) -> Self {
        let len = u16::try_from(term.len()).expect("caller rejects terms longer than u16::MAX");

        let layout = tag_term_layout(len as usize);

        // SAFETY: `layout` has non-zero size (the prefix alone is 2 bytes).
        let ptr = unsafe { alloc(layout) };
        let Some(ptr) = NonNull::new(ptr) else {
            handle_alloc_error(layout);
        };

        // SAFETY: the allocation is at least `TERM_LEN_PREFIX` bytes and
        // aligned for u16, so the prefix write is in bounds.
        unsafe { ptr.cast::<u16>().write(len) };

        // SAFETY: ptr + TERM_LEN_PREFIX is still in bounds.
        let dst = unsafe { ptr.as_ptr().add(TERM_LEN_PREFIX) };

        // SAFETY: source and destination are valid for `term.len()` bytes
        // and cannot overlap because `dst` is a freshly allocated block.
        unsafe { std::ptr::copy_nonoverlapping(term.as_ptr(), dst, term.len()) };

        Self(ptr)
    }
}

impl Drop for OwnedTerm {
    fn drop(&mut self) {
        // SAFETY: the allocation is alive (ownership is unique and this is
        // the owner's drop) and its length prefix was initialized by
        // `OwnedTerm::new`.
        let len = usize::from(unsafe { self.0.cast::<u16>().read() });
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

    /// Read back the bytes stored in an [`OwnedTerm`], mirroring its in-memory
    /// format (`u16` length prefix at offset 0, then the term bytes).
    ///
    /// # Safety
    /// `t` must be a live [`OwnedTerm`] produced by [`OwnedTerm::new`].
    unsafe fn read_back(t: &OwnedTerm) -> Vec<u8> {
        // SAFETY: the prefix was initialized by `OwnedTerm::new`.
        let len = usize::from(unsafe { t.0.cast::<u16>().read() });
        // SAFETY: `ptr + TERM_LEN_PREFIX` is in bounds and holds `len` bytes.
        let src = unsafe { t.0.as_ptr().add(TERM_LEN_PREFIX) };
        // SAFETY: `src` is valid for `len` initialized bytes.
        unsafe { std::slice::from_raw_parts(src, len) }.to_vec()
    }

    #[test]
    fn roundtrip() {
        let term = OwnedTerm::new(b"hello");
        // SAFETY: `term` is live and built by `OwnedTerm::new`.
        assert_eq!(unsafe { read_back(&term) }, b"hello");
        // `term` drops here; miri checks the alloc/dealloc layout match.
    }

    #[test]
    fn empty_term() {
        let term = OwnedTerm::new(b"");
        // SAFETY: `term` is live and built by `OwnedTerm::new`.
        assert_eq!(unsafe { read_back(&term) }, b"");
    }

    #[test]
    fn larger_term() {
        let expected = vec![0xABu8; 300];
        let term = OwnedTerm::new(&expected);
        // SAFETY: `term` is live and built by `OwnedTerm::new`.
        assert_eq!(unsafe { read_back(&term) }, expected);
    }

    #[test]
    #[should_panic(expected = "caller rejects terms longer than u16::MAX")]
    fn too_long_panics() {
        // The panic fires at `u16::try_from` before any allocation happens.
        let term = vec![0u8; u16::MAX as usize + 1];
        let _ = OwnedTerm::new(&term);
    }
}
