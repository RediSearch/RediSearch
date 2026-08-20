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
//! The values of this index are [`SuffixData`], which stores every member term of
//! its key as a [`TermPtr`] — a weak pointer — plus, when the key is a term of its
//! own, the [`OwnedTerm`] holding that term's allocation. [`OwnedTerm`] frees the
//! memory on drop, [`TermPtr`] doesn't, so adding and removing items from this trie
//! require order-aware operations.
//!
//! For instance, during the insertion:
//! - under the tag key, store the owned term ([`OwnedTerm`]) and append a pointer to
//!   it to that entry's member list
//! - for each suffix:
//!   - append a pointer to the term ([`TermPtr`]) to the suffix key's member list
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

use rqe_wildcard::WildcardPattern;
use thin_vec::{AlignedU32, ThinVec};
use trie_rs::{
    TrieMap,
    iter::{LendingIter, WildcardIter, filter::VisitAll},
};

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

    /// Build [`TermPtr`] from the current [`OwnedTerm`]
    const fn borrowed(&self) -> TermPtr {
        TermPtr(self.0)
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
pub(crate) struct TermPtr(NonNull<u8>);

impl TermPtr {
    /// Full allocation size in bytes (term bytes + the trailing NUL).
    ///
    /// # Safety
    /// The [`OwnedTerm`] this pointer was taken from must still be alive.
    pub const unsafe fn alloc_size(&self) -> usize {
        // This cast doesn't change size, we care about only the NULL
        let ptr = self.0.as_ptr().cast::<std::ffi::c_char>().cast_const();
        // SAFETY: the pointee is a live allocation from [`OwnedTerm::new`], which
        // NUL-terminates it.
        unsafe { std::ffi::CStr::from_ptr(ptr) }
            .to_bytes_with_nul()
            .len()
    }

    pub const fn as_ptr(&self) -> *const u8 {
        self.0.as_ptr()
    }
}

/// Payload of one trie entry.
#[derive(Debug, Default)]
pub(crate) struct SuffixData {
    /// `Some` iff this entry's key is itself a member: the owning handle of
    /// that member's tag term allocation. Every [`TermPtr`] to that term — in
    /// this entry's own [`members`](Self::members) and in each of its suffixes'
    /// — borrows from here.
    full_term: Option<OwnedTerm>,
    /// Every member term this entry's key belongs to, in registration order. See
    /// [`members`](Self::members) for why the order matters.
    members: ThinVec<TermPtr, AlignedU32>,
}

impl SuffixData {
    /// Every member term this entry's key belongs to — the terms it is a proper
    /// suffix of, and the term equal to the key itself when there is one — in the
    /// order [`TagSuffixIndex::add`] registered them.
    ///
    /// The order is observable: a [`crate::SuffixQuery::Wildcard`] expansion
    /// truncates this sequence at its `max_prefix_expansions`, so which members
    /// survive the cap depends on it.
    pub fn members(&self) -> impl Iterator<Item = TermPtr> + '_ {
        self.members.iter().copied()
    }
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

    /// Index `term` and every one of its suffixes.
    ///
    /// `term` is the tag value. The empty tag (`INDEXEMPTY`) is never indexed: it
    /// has no suffixes to look up.
    pub fn add(&mut self, term: Tag<'_>) {
        let bytes = term.as_bytes();
        if bytes.is_empty() {
            return;
        }

        // Don't store duplicates
        if self
            .entries
            .find(bytes)
            .is_some_and(|data| data.full_term.is_some())
        {
            return;
        }

        let owned = OwnedTerm::new(term);
        let ptr = owned.borrowed();

        // Store the OwnedTerm into the full tag term
        self.entries.insert_with(bytes, |slot| {
            let mut data = slot.unwrap_or_else(|| SuffixData {
                full_term: None,
                members: ThinVec::with_capacity(2),
            });
            // The term goes at the tail of the same member list as the references,
            // as C's `addSuffixTrieMap` appends it: when this entry already exists
            // because the key is a proper suffix of longer terms, those come first.
            data.members.push(ptr);
            // Keep "alive" the owned term
            data.full_term = Some(owned);

            data
        });

        // Process the suffixes as TermPtr
        for start in 1..bytes.len() {
            self.entries.insert_with(&bytes[start..], |slot| {
                let mut data = slot.unwrap_or_else(|| SuffixData {
                    full_term: None,
                    members: ThinVec::with_capacity(2),
                });
                data.members.push(ptr);
                data
            });
        }
    }

    /// Iterate over all `(suffix, data)` entries, in lexicographical order of
    /// the suffix.
    pub fn lending_iter(&self) -> LendingIter<'_, SuffixData, VisitAll> {
        self.entries.lending_iter()
    }

    /// Iterate over the `(suffix, data)` entries whose key starts with `prefix`,
    /// in lexicographical order.
    pub fn prefixed_iter(&self, prefix: &[u8]) -> trie_rs::iter::Iter<'_, SuffixData, VisitAll> {
        self.entries.prefixed_iter(prefix)
    }

    /// Iterate over all `(suffix, data)` entries whose suffix matches the
    /// wildcard `pattern`.
    pub fn wildcard_iter<'tm, 'p>(
        &'tm self,
        pattern: WildcardPattern<'p>,
    ) -> WildcardIter<'tm, 'p, SuffixData> {
        self.entries.wildcard_iter(pattern)
    }

    /// The entry keyed by exactly `key`, if any.
    pub fn find(&self, key: &[u8]) -> Option<&SuffixData> {
        self.entries.find(key)
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

    /// [`TagSuffixIndex::add`], with the same wrapping.
    fn add(idx: &mut TagSuffixIndex, term: &[u8]) {
        idx.add(Tag::new(term).expect("test literal is NUL-free"))
    }

    /// Read back the bytes stored in an [`OwnedTerm`], terminator included.
    fn read_back(t: &OwnedTerm) -> Vec<u8> {
        let len = t.alloc_size();

        // SAFETY: `t` is a live `OwnedTerm`, so its allocation holds `len`
        // initialized bytes.
        unsafe { std::slice::from_raw_parts(t.0.as_ptr(), len) }.to_vec()
    }

    /// The terms [`SuffixData::members`] yields, in order, with the terminator
    /// stripped.
    fn read_members(data: &SuffixData) -> Vec<Vec<u8>> {
        data.members()
            .map(|ptr| {
                // SAFETY: `data` is borrowed from a live index, so the `OwnedTerm`
                // each of its members points at is alive too.
                let len = unsafe { ptr.alloc_size() };
                // SAFETY: as above — the allocation holds `len` initialized bytes.
                let with_nul = unsafe { std::slice::from_raw_parts(ptr.as_ptr(), len) };
                with_nul[..len - 1].to_vec()
            })
            .collect()
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

    /// `add` keys the trie on the tag and each of its suffixes, all NUL-free: the
    /// terminator it stores is part of the value, never of a key, and no stray
    /// empty entry is created.
    #[test]
    fn add_stores_nul_free_keys_without_empty_entry() {
        let mut idx = TagSuffixIndex::new();
        add(&mut idx, b"foo");

        assert!(idx.find(b"foo").is_some());
        assert!(idx.find(b"oo").is_some());
        assert!(idx.find(b"o").is_some());
        // The stored terminator is not part of any key, and the empty suffix is
        // not an entry.
        assert!(idx.find(b"foo\0").is_none());
        assert!(idx.find(b"").is_none());
        assert!(idx.find(b"\0").is_none());
    }

    /// The empty tag (INDEXEMPTY) is never registered in the suffix trie, not even
    /// as an empty key.
    #[test]
    fn add_ignores_the_empty_tag() {
        let mut idx = TagSuffixIndex::new();
        add(&mut idx, b"");

        assert!(idx.find(b"").is_none());
        assert!(idx.find(b"\0").is_none());
    }

    /// Re-adding a term already in the trie must be ignored. The second insert
    /// would otherwise overwrite the entry's [`OwnedTerm`], freeing the
    /// allocation that every suffix entry's [`TermPtr`] still points at.
    /// [`TagIndex::commit`](crate::TagIndex::commit) runs once per document, so
    /// any tag value shared by two documents takes this path.
    #[test]
    fn add_of_an_already_indexed_term_is_ignored() {
        let mut idx = TagSuffixIndex::new();
        add(&mut idx, b"cat");
        add(&mut idx, b"cat");

        let term = idx.find(b"cat").expect("`cat` is indexed");
        assert_eq!(term.members().count(), 1, "one owned term, not two");

        for suffix in [b"at".as_slice(), b"t"] {
            let data = idx.find(suffix).expect("suffix is indexed");
            assert_eq!(
                data.members().count(),
                1,
                "`{}` must not gain a second reference to the same term",
                String::from_utf8_lossy(suffix)
            );
            assert!(
                data.members().eq(term.members()),
                "the surviving reference must point at the live term allocation"
            );
        }
    }

    /// A term whose own key already exists as a proper suffix of longer terms is
    /// registered *after* them, matching C's `addSuffixTrieMap` — which appends the
    /// full term to the same array as the references. A capped expansion truncates
    /// that order, so getting it wrong opens a different set of tag readers.
    #[test]
    fn a_term_registered_after_its_own_suffix_entry_comes_last() {
        let mut idx = TagSuffixIndex::new();
        for term in [b"beat".as_slice(), b"heat", b"eat"] {
            add(&mut idx, term);
        }

        let data = idx.find(b"eat").expect("`eat` is indexed");
        assert_eq!(
            read_members(data),
            [b"beat".to_vec(), b"heat".to_vec(), b"eat".to_vec()]
        );
    }

    /// The other order: the term creates its own entry, so it is the first member
    /// and later, longer terms are appended after it.
    #[test]
    fn a_term_registered_before_its_own_suffix_entry_comes_first() {
        let mut idx = TagSuffixIndex::new();
        for term in [b"eat".as_slice(), b"beat"] {
            add(&mut idx, term);
        }

        let data = idx.find(b"eat").expect("`eat` is indexed");
        assert_eq!(read_members(data), [b"eat".to_vec(), b"beat".to_vec()]);
    }
}
