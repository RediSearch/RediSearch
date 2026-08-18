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
//! - insert owned term under tag key ([`OwnedTerm`])
//! - for each suffix:
//!   - insert borrowed term under the suffix key ([`TermPtr`])
//!
//! # Keys and stored terms
//!
//! Keys are the NUL-free tag bytes and each of their suffixes. The stored *terms*
//! are separate allocations that [`OwnedTerm::new`] NUL-terminates itself.
//!

use std::{
    alloc::{Layout, alloc, dealloc, handle_alloc_error},
    ptr::NonNull,
};

use thin_vec::{AlignedU32, ThinVec};
use trie_rs::TrieMap;

use crate::Tag;

/// Layout of a tag term allocation.
fn tag_term_layout(size: usize) -> Layout {
    // A term is a plain byte buffer, so it needs no alignment beyond 1.
    let align = align_of::<u8>();

    Layout::from_size_align(size, align)
        .expect("a tag term is one byte longer than a tag, far below Layout's size limit")
}

/// Owning handle to a tag term allocation.
///
/// Dropping it frees the allocation.
#[derive(Debug)]
struct OwnedTerm(NonNull<u8>);

impl OwnedTerm {
    /// Copy `term` into a fresh, NUL-terminated allocation.
    ///
    /// The allocation is one byte longer than `term`, and its last byte is the
    /// terminator written here. `term` being a [`Tag`] — interior-NUL-free
    /// by construction — is what makes [`alloc_size`](Self::alloc_size) report
    /// the true allocation length rather than a short one.
    fn new(term: Tag<'_>) -> Self {
        let term = term.as_bytes();
        let layout = tag_term_layout(term.len() + 1);

        // SAFETY: `layout` has non-zero size — it is `term.len() + 1`.
        let ptr = unsafe { alloc(layout) };
        let Some(ptr) = NonNull::new(ptr) else {
            handle_alloc_error(layout);
        };

        // SAFETY: source and destination are valid for `term.len()` bytes
        // and cannot overlap because `dst` is a freshly allocated block.
        unsafe { std::ptr::copy_nonoverlapping(term.as_ptr(), ptr.as_ptr(), term.len()) };
        // SAFETY: the allocation holds `term.len() + 1` bytes, so offset
        // `term.len()` is the last one in bounds.
        let terminator = unsafe { ptr.as_ptr().add(term.len()) };
        // SAFETY: `terminator` is in bounds, as above, and freshly allocated.
        unsafe { terminator.write(0) };

        Self(ptr)
    }

    /// Full allocation size in bytes (term bytes + the trailing NUL).
    const fn alloc_size(&self) -> usize {
        // This cast doesn't change size, we care about only the NULL
        let ptr = self.0.as_ptr().cast::<std::ffi::c_char>().cast_const();
        // SAFETY: [`OwnedTerm::new`] NUL-terminates every allocation.
        unsafe { std::ffi::CStr::from_ptr(ptr) }
            .to_bytes_with_nul()
            .len()
    }
}

impl Drop for OwnedTerm {
    fn drop(&mut self) {
        let len = self.alloc_size();

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
pub(crate) struct TagSuffixIndex {
    /// The suffix entries
    entries: TrieMap<SuffixData>,
}

impl TagSuffixIndex {
    /// Create a new, empty index.
    pub const fn new() -> Self {
        Self {
            entries: TrieMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// [`OwnedTerm::new`], wrapping the NUL-free literals every test below
    /// passes into a [`Tag`].
    fn owned_term(term: &[u8]) -> OwnedTerm {
        OwnedTerm::new(Tag::new(term).expect("test literal is NUL-free"))
    }

    /// Read back the bytes stored in an [`OwnedTerm`], terminator included.
    fn read_back(t: &OwnedTerm) -> Vec<u8> {
        let len = t.alloc_size();

        // SAFETY: `t` is a live `OwnedTerm`, so its allocation holds `len`
        // initialized bytes.
        unsafe { std::slice::from_raw_parts(t.0.as_ptr(), len) }.to_vec()
    }

    /// [`OwnedTerm::new`] takes the NUL-free term and appends the terminator
    /// itself, so the stored bytes are one longer than the input.
    #[test]
    fn roundtrip() {
        let term = owned_term(b"hello");
        assert_eq!(read_back(&term), b"hello\0");
        // `term` drops here; miri checks the alloc/dealloc layout match.
    }

    /// The empty term still gets an allocation — just the terminator.
    #[test]
    fn empty_term_is_fine() {
        let term = owned_term(b"");
        assert_eq!(read_back(&term), b"\0");
    }

    #[test]
    fn larger_term_is_fine() {
        let term_bytes = vec![0xABu8; 300];
        let term = owned_term(&term_bytes);

        let mut expected = term_bytes;
        expected.push(0);
        assert_eq!(read_back(&term), expected);
    }

    #[test]
    fn owned_term_is_a_fresh_allocation() {
        let term_bytes = "foo".to_string();
        let term = owned_term(term_bytes.as_bytes());

        assert_ne!(term_bytes.as_ptr(), term.0.as_ptr().cast());
    }
}
