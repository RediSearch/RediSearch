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
//! # Reported memory usage
//!
//! [`TagSuffixIndex::mem_usage`] covers every allocation the index owns:
//! - the trie nodes, each of which holds its [`SuffixData`] payload inline;
//! - the [`OwnedTerm`] allocation behind each payload's owned term;
//! - the heap block behind each payload's member list.
//!
//! A term is counted once, against the entry that owns it, not once per [`TermPtr`]
//! aliasing it.

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

/// Payload of one trie entry.
#[derive(Debug, Default)]
pub(crate) struct SuffixData {
    /// `Some` iff this entry's key is itself a member: the owning handle of
    /// that member's tag term allocation. Every [`TermPtr`] to that term borrows from here.
    full_term: Option<OwnedTerm>,
    /// Every member this entry's key is a suffix of.
    members: ThinVec<TermPtr, AlignedU32>,
}

impl SuffixData {
    /// Every member term this entry's key belongs to — the terms it is a proper
    /// suffix of, and the term equal to the key itself when there is one — in the
    /// order [`TagSuffixIndex::add`] registered them.
    ///
    /// The order is important because a [`crate::SuffixQuery::Wildcard`] expansion
    /// could truncate the sequence at its `max_prefix_expansions`.
    pub fn members(&self) -> impl Iterator<Item = TermPtr> + '_ {
        self.members.iter().copied()
    }

    /// Bytes this payload owns outside its trie node: the term allocation it holds,
    /// when it holds one, plus its member list's heap block.
    fn nested_mem_usage(&self) -> usize {
        self.full_term.as_ref().map_or(0, OwnedTerm::alloc_size) + self.members.mem_usage()
    }
}

#[derive(Debug, Default)]
pub(crate) struct TagSuffixIndex {
    /// The suffix entries
    entries: TrieMap<SuffixData>,
    /// Total [`SuffixData::nested_mem_usage`] over every entry.
    nested_mem_usage: usize,
}

impl TagSuffixIndex {
    /// Create a new, empty index.
    pub const fn new() -> Self {
        Self {
            entries: TrieMap::new(),
            nested_mem_usage: 0,
        }
    }

    /// Insert or update the entry keyed by `key`, applying `f` to its payload and
    /// folding the payload's growth into [`nested_mem_usage`](Self::nested_mem_usage).
    fn insert_tracked(&mut self, key: &[u8], f: impl FnOnce(&mut SuffixData)) {
        let mut delta = 0;

        self.entries.insert_with(key, |slot| {
            // Measured on `slot` rather than on `data` below, so that a brand-new entry
            // contributes the whole of its fresh member list to the delta instead of
            // having it netted out.
            let before = slot.as_ref().map_or(0, SuffixData::nested_mem_usage);
            let mut data = slot.unwrap_or_else(|| SuffixData {
                full_term: None,
                // Every entry gains a member immediately, and a suffix shared by two
                // terms is the common case.
                members: ThinVec::with_capacity(2),
            });

            f(&mut data);

            let after = data.nested_mem_usage();
            debug_assert!(
                after >= before,
                "an entry's payload allocations only ever grow: `add` bails out before \
                 re-registering a term, so `full_term` only goes `None` -> `Some` and \
                 `members` is only ever pushed to"
            );
            delta = after - before;

            data
        });

        self.nested_mem_usage += delta;
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
        self.insert_tracked(bytes, |data| {
            // The term goes at the tail of the same member list as the references,
            // as C's `addSuffixTrieMap` appends it: when this entry already exists
            // because the key is a proper suffix of longer terms, those come first.
            data.members.push(ptr);
            // Keep "alive" the owned term
            data.full_term = Some(owned);
        });

        // Process the suffixes as TermPtr
        for start in 1..bytes.len() {
            self.insert_tracked(&bytes[start..], |data| data.members.push(ptr));
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

    /// Remove `tag` and all of its suffixes from the trie, dropping the entries
    /// that no other term still relies on.
    ///
    /// `tag` is the tag value (the values-trie key), matching the keys stored by
    /// [`add`](Self::add).
    pub fn delete(&mut self, tag: Tag<'_>) {
        let tag = tag.as_bytes();
        debug_assert!(
            !tag.is_empty(),
            "empty string is likely a caller-level mistake"
        );

        // Taken from the `tag` entry on the first iteration.
        let mut deleted_term = None;

        for j in 0..tag.len() {
            let data = self.entries.find_mut(&tag[j..]);
            debug_assert!(data.is_some(), "all suffixes must exist");
            // A missing entry means this trie and the values trie disagree; skip
            // it rather than panicking inside the garbage collector.
            let Some(data) = data else { continue };

            if j == 0 {
                deleted_term = data.full_term.take();
            }

            // Drop the pointers to the term being deleted, keeping every one that
            // belongs to a different term.
            if let Some(deleted) = &deleted_term
                && let Some(deleted_index) = data.members.iter().position(|b| b.belong_to(deleted))
            {
                data.members.swap_remove(deleted_index);
            }

            // Don't keep empty `members`.
            if data.full_term.is_none() && data.members.is_empty() {
                self.entries.remove(&tag[j..]);
            }
        }

        // Freed only here: every entry that pointed at this allocation has given up
        // its `TermPtr`, so dropping it can no longer leave a dangling one behind.
        drop(deleted_term);
    }

    /// The entry keyed by exactly `key`, if any.
    pub fn find(&self, key: &[u8]) -> Option<&SuffixData> {
        self.entries.find(key)
    }

    /// Bytes the suffix trie occupies — see [the module's accounting
    /// rules](self#reported-memory-usage) for what that covers.
    pub const fn mem_usage(&self) -> usize {
        self.entries.mem_usage() + self.nested_mem_usage
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

    /// [`TagSuffixIndex::delete`], with the same wrapping.
    fn delete(idx: &mut TagSuffixIndex, term: &[u8]) {
        idx.delete(Tag::new(term).expect("test literal is NUL-free"))
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

    /// [`TagSuffixIndex::nested_mem_usage`], recomputed from scratch by walking every
    /// entry — the analogue of `TrieMap::recursive_mem_usage`, and the oracle the
    /// incrementally maintained counter is checked against.
    fn walked_nested_mem_usage(idx: &TagSuffixIndex) -> usize {
        idx.entries.values().map(SuffixData::nested_mem_usage).sum()
    }

    /// The counter must equal the walk, and `mem_usage` must be the trie's own figure
    /// plus that counter.
    fn assert_counter_is_exact(idx: &TagSuffixIndex) {
        assert_eq!(
            idx.nested_mem_usage,
            walked_nested_mem_usage(idx),
            "the incremental counter drifted from a full walk of the entries"
        );
        assert_eq!(
            idx.mem_usage(),
            idx.entries.mem_usage() + idx.nested_mem_usage
        );
    }

    #[test]
    fn empty_index_owns_no_payload_bytes() {
        let idx = TagSuffixIndex::new();

        assert_eq!(idx.nested_mem_usage, 0);
        assert_eq!(idx.mem_usage(), idx.entries.mem_usage());
    }

    /// A brand-new entry must contribute its whole member list, so the payload bytes of
    /// the very first term cannot be zero. Measuring the "before" size on the
    /// freshly-defaulted payload instead of on the vacant slot would net it out to zero.
    #[test]
    fn a_fresh_entrys_member_list_is_counted() {
        let mut idx = TagSuffixIndex::new();
        add(&mut idx, b"hello");

        assert!(
            idx.nested_mem_usage > 0,
            "five suffix entries own a member list each, plus one term allocation"
        );
        assert!(
            idx.mem_usage() > idx.entries.mem_usage(),
            "the payload allocations must lift the reported figure above the trie's own"
        );
        assert_counter_is_exact(&idx);
    }

    /// Registering a term that already exists as a proper suffix of a longer one sets
    /// `full_term` on an entry that is already there, so no trie node changes and the
    /// trie's own counter cannot see the new term allocation.
    #[test]
    fn a_term_taking_over_an_existing_suffix_entry_is_counted() {
        let mut idx = TagSuffixIndex::new();
        add(&mut idx, b"hello");
        let before = idx.mem_usage();

        add(&mut idx, b"lo");

        assert!(
            idx.mem_usage() >= before + b"lo\0".len(),
            "`lo` already had an entry, so its term allocation is the only guaranteed \
             growth — and it has to be counted"
        );
        assert_counter_is_exact(&idx);
    }

    /// Terms sharing a suffix push onto the shared entries' member lists without adding
    /// any trie node. The third sharer takes those lists past their initial capacity of
    /// two, so the counter has to follow the reallocation as well.
    #[test]
    fn members_pushed_onto_shared_entries_are_counted() {
        let mut idx = TagSuffixIndex::new();

        for term in [b"hello".as_slice(), b"jello", b"mello"] {
            add(&mut idx, term);
            assert_counter_is_exact(&idx);
        }

        let shared = idx.find(b"ello").expect("`ello` is a suffix of all three");
        assert_eq!(shared.members().count(), 3, "all three share this entry");
        assert!(
            shared.nested_mem_usage() > 2 * size_of::<TermPtr>(),
            "a third member must have grown the list beyond its initial capacity"
        );
    }

    /// The early return for an already-indexed term must not count its allocations twice
    /// — nor, since it allocates a term it then drops, count them at all.
    #[test]
    fn a_duplicate_add_leaves_the_figure_untouched() {
        let mut idx = TagSuffixIndex::new();
        add(&mut idx, b"hello");
        let after_first = idx.mem_usage();

        add(&mut idx, b"hello");

        assert_eq!(idx.mem_usage(), after_first);
        assert_counter_is_exact(&idx);
    }

    /// A longer term costs its extra term bytes *and* one member list per extra suffix,
    /// so the gap has to exceed the difference in term length alone.
    #[test]
    fn a_longer_term_costs_more_than_its_extra_bytes() {
        let short: &[u8] = b"short";
        let long: &[u8] = b"a_considerably_longer";

        let mut short_idx = TagSuffixIndex::new();
        add(&mut short_idx, short);

        let mut long_idx = TagSuffixIndex::new();
        add(&mut long_idx, long);

        assert!(
            long_idx.nested_mem_usage - short_idx.nested_mem_usage > long.len() - short.len(),
            "the extra suffix entries each own a member list on top of the longer term"
        );
        assert_counter_is_exact(&short_idx);
        assert_counter_is_exact(&long_idx);
    }

    /// Deleting a tag that is merely a *suffix* of an indexed term must not
    /// panic: the entry exists but owns no term, so there is nothing to unlink.
    #[test]
    fn delete_of_a_suffix_only_entry_changes_nothing() {
        let mut idx = TagSuffixIndex::new();
        add(&mut idx, b"cat");

        delete(&mut idx, b"at");

        assert!(idx.find(b"cat").is_some(), "`cat` is untouched");
        assert_eq!(
            idx.find(b"at").expect("kept for `cat`").members().count(),
            1,
            "`at` still references `cat`"
        );
    }

    /// Deleting a term drops only the references pointing at that term, keeping
    /// the suffix entries that other terms still rely on.
    #[test]
    fn delete_keeps_suffixes_still_used_by_other_terms() {
        let mut idx = TagSuffixIndex::new();
        // "cat" and "bat" share the suffixes "at" and "t".
        add(&mut idx, b"cat");
        add(&mut idx, b"bat");

        delete(&mut idx, b"cat");

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
