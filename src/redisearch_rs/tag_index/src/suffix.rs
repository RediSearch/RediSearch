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
//! # Keys and stored terms
//!
//! Keys are the NUL-free tag bytes and each of their suffixes, matching the C
//! `addSuffixTrieMap`. The stored *terms* are separate allocations that
//! [`OwnedTerm::new`] NUL-terminates itself, mirroring C's `rm_strndup(str,
//! len)`. That terminator is why a [`TermPtr`] is usable as a C `char *`: query
//! expansion hands these pointers back to C, which calls `strlen` on them.
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
    /// Copy the NUL-free `term` into a fresh, NUL-terminated allocation.
    ///
    /// The allocation is one byte longer than `term`, and its last byte is the
    /// terminator written here — so every [`OwnedTerm`] upholds the invariant the
    /// [module docs](self) describe without the caller having to promise anything.
    fn new(term: &[u8]) -> Self {
        debug_assert!(
            !term.contains(&0),
            "tag terms are NUL-free; an interior NUL would make `alloc_size` report a short allocation"
        );

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
        // SAFETY: [`OwnedTerm::new`] NUL-terminates every allocation and rejects
        // interior NULs, so the scan stops at the terminator it wrote.
        unsafe { std::ffi::CStr::from_ptr(ptr) }
            .to_bytes_with_nul()
            .len()
    }

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
    fn belong_to(&self, owned: &OwnedTerm) -> bool {
        self.0 == owned.0
    }

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

/// Payload of one trie entry. Opaque outside this module: entries are only
/// handed out by reference, so callers can enumerate the suffixes without
/// touching the term bookkeeping.
#[derive(Debug, Default)]
pub(crate) struct SuffixData {
    /// `Some` iff this entry's key is itself a member: the owning handle of
    /// that member's tag term allocation.
    full_term: Option<OwnedTerm>,
    /// Every member this entry's key is a suffix of.
    pub(crate) refs: ThinVec<TermPtr, AlignedU32>,
}

impl SuffixData {
    /// Every member term this entry's key belongs to, mirroring the C
    /// `suffixData.array`: the term itself when the key is a full term
    /// (stored separately in [`Self::full_term`]) followed by every term the
    /// key is a *proper* suffix of ([`Self::refs`]).
    ///
    /// Unlike iterating [`Self::refs`] alone, this includes the full-term entry,
    /// so a term matched through its own key (e.g. `he*` matching `hero`) is not
    /// dropped.
    pub fn members(&self) -> impl Iterator<Item = TermPtr> + '_ {
        self.full_term
            .as_ref()
            .map(OwnedTerm::borrowed)
            .into_iter()
            .chain(self.refs.iter().copied())
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

    /// Index `term` and every one of its suffixes, keyed as the [module
    /// docs](self) describe.
    ///
    /// `term` is the NUL-free tag value. The empty tag (`INDEXEMPTY`) is never
    /// indexed — C asserts a non-empty length in `addSuffixTrieMap`.
    pub fn add(&mut self, term: &[u8]) {
        if term.is_empty() {
            return;
        }

        // Don't store duplicates
        if self
            .entries
            .find(term)
            .is_some_and(|data| data.full_term.is_some())
        {
            return;
        }

        let owned = OwnedTerm::new(term);
        let ptr = owned.borrowed();

        // Store the OwnedTerm into the full tag term
        self.entries.insert_with(term, |slot| {
            let mut data = slot.unwrap_or_else(|| SuffixData {
                full_term: None,
                refs: ThinVec::with_capacity(2),
            });
            data.full_term = Some(owned);

            data
        });

        // Process the suffixes as TermPtr
        for start in 1..term.len() {
            self.entries.insert_with(&term[start..], |slot| {
                let mut data = slot.unwrap_or_else(|| SuffixData {
                    full_term: None,
                    refs: ThinVec::with_capacity(2),
                });
                data.refs.push(ptr);
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
    /// in lexicographical order — how a contains query (`*foo*`) expands.
    pub fn prefixed_iter(&self, prefix: &[u8]) -> trie_rs::iter::Iter<'_, SuffixData, VisitAll> {
        self.entries.prefixed_iter(prefix)
    }

    /// Iterate over all `(suffix, data)` entries whose suffix matches the
    /// wildcard `pattern` (`*` and `?` metacharacters).
    pub fn wildcard_iter<'tm, 'p>(
        &'tm self,
        pattern: WildcardPattern<'p>,
    ) -> WildcardIter<'tm, 'p, SuffixData> {
        self.entries.wildcard_iter(pattern)
    }

    /// Remove `tag` and all of its suffixes from the trie, dropping the entries
    /// that no other term still relies on. Port of the C `deleteSuffixTrieMap`.
    ///
    /// `tag` is the NUL-free tag value (the values-trie key), matching the keys
    /// stored by [`add`](Self::add).
    pub fn delete(&mut self, tag: &[u8]) {
        debug_assert!(
            !tag.is_empty(),
            "empty string is likely a caller-level mistake"
        );

        // Taken from the `tag` entry on the first iteration and dropped when this
        // call returns, once no suffix entry points at it any more.
        let mut deleted_term = None;

        for j in 0..tag.len() {
            let data = self.entries.find_mut(&tag[j..]);
            debug_assert!(data.is_some(), "all suffixes must exist");
            // C asserts the same invariant but carries on harmlessly when the
            // entry is missing; do that rather than panicking inside the GC.
            let Some(data) = data else { continue };

            if j == 0 {
                deleted_term = data.full_term.take();
            }

            // Drop the references pointing at the term being deleted, keeping
            // every reference that belongs to a different term (C's
            // `removeSuffix`, which deletes the single array entry equal to the
            // deleted term). With no term to delete there is nothing to match:
            // C compares by content, and every `refs` entry reachable here
            // belongs to a strictly longer term.
            if let Some(deleted) = &deleted_term {
                data.refs.retain(|b| !b.belong_to(deleted));
            }

            if data.full_term.is_none() && data.refs.is_empty() {
                self.entries.remove(&tag[j..]);
            }
        }
    }

    /// The entry keyed by exactly `key`, if any — how a suffix query (`*foo`)
    /// expands, `key` being the suffix itself.
    pub fn find(&self, key: &[u8]) -> Option<&SuffixData> {
        self.entries.find(key)
    }

    /// Bytes the suffix trie occupies, counted into
    /// [`TagIndex::get_overhead`](crate::TagIndex::get_overhead).
    pub const fn mem_usage(&self) -> usize {
        self.entries.mem_usage()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let term = OwnedTerm::new(b"hello");
        assert_eq!(read_back(&term), b"hello\0");
        // `term` drops here; miri checks the alloc/dealloc layout match.
    }

    /// The empty term still gets an allocation — just the terminator.
    #[test]
    fn empty_term_is_fine() {
        let term = OwnedTerm::new(b"");
        assert_eq!(read_back(&term), b"\0");
    }

    #[test]
    fn larger_term_is_fine() {
        let term_bytes = vec![0xABu8; 300];
        let term = OwnedTerm::new(&term_bytes);

        let mut expected = term_bytes;
        expected.push(0);
        assert_eq!(read_back(&term), expected);
    }

    /// `add` keys the trie on the tag and each of its suffixes, all NUL-free
    /// (matching C's `addSuffixTrieMap`); the terminator it stores is part of the
    /// value, never of a key, and no stray empty entry is created.
    #[test]
    fn add_stores_nul_free_keys_without_empty_entry() {
        let mut idx = TagSuffixIndex::new();
        idx.add(b"foo");

        assert!(idx.find(b"foo").is_some());
        assert!(idx.find(b"oo").is_some());
        assert!(idx.find(b"o").is_some());
        // The stored terminator is not part of any key, and the empty suffix is
        // not an entry.
        assert!(idx.find(b"foo\0").is_none());
        assert!(idx.find(b"").is_none());
        assert!(idx.find(b"\0").is_none());
    }

    /// The empty tag (INDEXEMPTY) is never registered in the suffix trie —
    /// C asserts a non-empty length in `addSuffixTrieMap`.
    #[test]
    fn add_ignores_the_empty_tag() {
        let mut idx = TagSuffixIndex::new();
        idx.add(b"");

        assert!(idx.find(b"").is_none());
        assert!(idx.find(b"\0").is_none());
    }

    /// Deleting a tag that is merely a *suffix* of an indexed term must not
    /// panic. The entry exists but owns no term, so there is nothing to unlink —
    /// C's `deleteSuffixTrieMap` copes with a NULL `oldTerm` the same way.
    #[test]
    fn delete_of_a_suffix_only_entry_changes_nothing() {
        let mut idx = TagSuffixIndex::new();
        idx.add(b"cat");

        idx.delete(b"at");

        assert!(idx.find(b"cat").is_some(), "`cat` is untouched");
        assert_eq!(
            idx.find(b"at").expect("kept for `cat`").members().count(),
            1,
            "`at` still references `cat`"
        );
    }

    /// Deleting a term drops only the references pointing at that term, keeping
    /// the suffix entries that other terms still rely on (C's `removeSuffix`).
    #[test]
    fn delete_keeps_suffixes_still_used_by_other_terms() {
        let mut idx = TagSuffixIndex::new();
        // "cat" and "bat" share the suffixes "at" and "t".
        idx.add(b"cat");
        idx.add(b"bat");

        idx.delete(b"cat");

        // "cat" and its unique full-term entry are gone...
        assert!(idx.find(b"cat").is_none());
        // ...but the shared suffixes survive, now referencing only "bat".
        assert!(idx.find(b"bat").is_some());
        let at = idx.find(b"at").expect("shared suffix kept for `bat`");
        assert_eq!(at.members().count(), 1);
        let t = idx.find(b"t").expect("shared suffix kept for `bat`");
        assert_eq!(t.members().count(), 1);
    }
}
