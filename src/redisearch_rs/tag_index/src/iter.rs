/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Iterators over the contents of a [`TagIndex`].
//!
//! [`TagIndexIterator`] walks the tag *values* (the keys of the values trie, or
//! the suffix trie), optionally filtered by a pattern and bounded by a timeout.
//! Its [`advance`](TagIndexIterator::advance) yields, for each tag, the borrowed key
//! together with the tag's [`InvertedIndex<DocIdsOnly>`] in memory mode (`None` in
//! disk mode or when walking the suffix trie, where the trie holds no in-memory
//! postings).
//!
//! [`TagValueReader`] reads the postings (document ids) of a single tag value.

use std::time::Instant;

use ffi::timespec;
use index_result::RSIndexResult;
use inverted_index::{IndexReader, IndexReaderCore, InvertedIndex, doc_ids_only::DocIdsOnly};
use lending_iterator::LendingIterator as _;
use trie_rs::iter::{ContainsLendingIter, LendingIter, WildcardLendingIter, filter::VisitAll};

use crate::{SuffixData, TagIndex};

/// Value type stored in the memory-mode values trie. Boxed so the heap
/// [`InvertedIndex`] address stays stable across trie restructuring — callers hold
/// it across mutations.
type BoxedInvertedIndex = Box<InvertedIndex<DocIdsOnly>>;

/// Predicate the suffix variants filter a full trie walk with. It owns a copy of
/// the queried suffix, so its closure type is unnameable and has to be boxed to
/// appear in an enum variant. This is a boxed *predicate*, not a boxed iterator —
/// dispatch over the iterator shapes below stays static.
type SuffixPredicate<V> = Box<dyn Fn(&(&[u8], &V)) -> bool>;

/// Which subset of tag values a [filtered iterator](TagIndex::value_iter_filtered)
/// walks. A tag matches when it starts with (`Prefix`), ends with (`Suffix`),
/// contains (`Contains`), or wildcard-matches (`Wildcard`) the pattern.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IterMode {
    /// Tags starting with the pattern.
    Prefix,
    /// Tags containing the pattern.
    Contains,
    /// Tags ending with the pattern.
    Suffix,
    /// Tags matching the wildcard pattern (`*` and `?` metacharacters).
    Wildcard,
}

/// The concrete tag-value iterator, one variant per underlying `trie_rs`
/// iterator shape, in both storage modes. Kept as an enum (rather than a
/// `Box<dyn>` trait object) so iteration dispatches statically.
///
/// Memory-mode variants carry [`BoxedInvertedIndex`] values whose stable heap address is
/// exposed to callers; disk-mode variants carry `()` (the trie holds only tag
/// presence, postings live on disk) and the suffix-trie variant carries opaque
/// [`SuffixData`] — both yield no value.
enum TagIndexIteratorImpl<'ti> {
    /// Memory mode, full iteration or prefix filter.
    MemAll(LendingIter<'ti, BoxedInvertedIndex, VisitAll>),
    /// Memory mode, entries whose key contains a fragment.
    MemContains(ContainsLendingIter<'ti, 'ti, BoxedInvertedIndex>),
    /// Memory mode, entries whose key matches a wildcard pattern.
    MemWildcard(WildcardLendingIter<'ti, 'ti, BoxedInvertedIndex>),
    /// Memory mode, entries whose key ends with a suffix. A trie cannot seek by
    /// suffix, so this pairs a full walk with the predicate the walk is filtered
    /// by; see [`advance`](TagIndexIterator::advance) for why the two are kept
    /// side by side rather than combined into a filtering adapter.
    MemSuffix(
        LendingIter<'ti, BoxedInvertedIndex, VisitAll>,
        SuffixPredicate<BoxedInvertedIndex>,
    ),
    /// Disk mode, full iteration or prefix filter.
    DiskAll(LendingIter<'ti, (), VisitAll>),
    /// Disk mode, entries whose key contains a fragment.
    DiskContains(ContainsLendingIter<'ti, 'ti, ()>),
    /// Disk mode, entries whose key matches a wildcard pattern.
    DiskWildcard(WildcardLendingIter<'ti, 'ti, ()>),
    /// Disk mode, entries whose key ends with a suffix, filtered like
    /// [`MemSuffix`](Self::MemSuffix).
    DiskSuffix(LendingIter<'ti, (), VisitAll>, SuffixPredicate<()>),
    /// Suffix-trie entries; the value is opaque bookkeeping.
    SuffixEntries(LendingIter<'ti, SuffixData, VisitAll>),
}

impl TagIndexIteratorImpl<'_> {
    /// Forward deadline to the behind iterator.
    fn set_timeout(&mut self, deadline: Option<Instant>) {
        match self {
            Self::MemAll(it) => it.set_timeout(deadline),
            Self::MemContains(it) => it.set_timeout(deadline),
            Self::MemWildcard(it) => it.set_timeout(deadline),
            Self::MemSuffix(it, _) => it.set_timeout(deadline),
            Self::DiskAll(it) => it.set_timeout(deadline),
            Self::DiskContains(it) => it.set_timeout(deadline),
            Self::DiskWildcard(it) => it.set_timeout(deadline),
            Self::DiskSuffix(it, _) => it.set_timeout(deadline),
            Self::SuffixEntries(it) => it.set_timeout(deadline),
        }
    }
}

/// An iterator over the values (tags) stored in a [`TagIndex`], returned by
/// [`TagIndex::value_iter`], [`TagIndex::value_iter_filtered`], and
/// [`TagIndex::suffix_value_iter`].
///
/// Drive it with [`advance`](Self::advance), which yields each tag together
/// with its value: in memory mode the tag's [`InvertedIndex<DocIdsOnly>`]
/// stored in the values trie, otherwise `None` (disk entries and suffix-trie
/// entries carry no in-memory value). Long affix expansions can be bounded with
/// [`set_timeout`](Self::set_timeout).
pub struct TagIndexIterator<'ti> {
    iter: TagIndexIteratorImpl<'ti>,
}

impl<'ti> TagIndexIterator<'ti> {
    /// Advance to the next entry, honoring the optional timeout, and return the
    /// key together with the value (when provided).
    /// Returns `None` at the end of the iteration, or when the timeout is
    /// reached. The key slice is borrowed from trie-internal storage and is
    /// invalidated by the next call.
    pub fn advance(&mut self) -> Option<(&[u8], Option<&InvertedIndex<DocIdsOnly>>)> {
        // The memory-mode value is a `&Box<InvertedIndex>`; callers hold and
        // dereference the heap `InvertedIndex`, so hand out that stable address
        // (the `Box`'s heap content, via deref coercion), not the box slot in
        // the trie node.
        fn mem_value(v: &BoxedInvertedIndex) -> &InvertedIndex<DocIdsOnly> {
            v
        }

        match &mut self.iter {
            TagIndexIteratorImpl::MemAll(it) => it.next().map(|(k, v)| (k, Some(mem_value(v)))),
            TagIndexIteratorImpl::MemContains(it) => {
                it.next().map(|(k, v)| (k, Some(mem_value(v))))
            }
            TagIndexIteratorImpl::MemWildcard(it) => {
                it.next().map(|(k, v)| (k, Some(mem_value(v))))
            }
            TagIndexIteratorImpl::MemSuffix(it, matches) => {
                it.find(&mut *matches).map(|(k, v)| (k, Some(mem_value(v))))
            }
            TagIndexIteratorImpl::DiskAll(it) => it.next().map(|(k, ())| (k, None)),
            TagIndexIteratorImpl::DiskContains(it) => it.next().map(|(k, ())| (k, None)),
            TagIndexIteratorImpl::DiskWildcard(it) => it.next().map(|(k, ())| (k, None)),
            TagIndexIteratorImpl::DiskSuffix(it, matches) => {
                it.find(&mut *matches).map(|(k, ())| (k, None))
            }
            TagIndexIteratorImpl::SuffixEntries(it) => it.next().map(|(k, _)| (k, None)),
        }
    }

    /// Set the deadline honored while iterating, or clear it with `None` —
    /// matching [`TagIndex::suffix_expand`](crate::TagIndex::suffix_expand).
    pub fn set_timeout(&mut self, timeout: Option<timespec>) {
        self.iter.set_timeout(crate::expansion_deadline(timeout));
    }
}

impl TagIndex {
    /// Iterate over all tag values, in lexicographical order.
    pub fn value_iter(&self) -> TagIndexIterator<'_> {
        let iter = if self.disk_mode() {
            TagIndexIteratorImpl::DiskAll(self.disk_iter_values())
        } else {
            TagIndexIteratorImpl::MemAll(self.iter())
        };
        TagIndexIterator { iter }
    }

    /// Iterate over the tag values matching `pattern` under `mode`, in
    /// lexicographical order.
    ///
    /// In disk mode the values trie holds only tag presence, so callers resolve
    /// each reader by tag string; in memory mode the yielded value is still
    /// exposed by [`advance`](TagIndexIterator::advance).
    ///
    /// `pattern` is borrowed for the iterator's lifetime by the prefix,
    /// contains, and wildcard modes (the suffix mode copies it).
    pub fn value_iter_filtered<'a>(
        &'a self,
        pattern: &'a [u8],
        iter_mode: IterMode,
    ) -> TagIndexIterator<'a> {
        // The suffix mode filters a full trie walk by an owned copy of the
        // pattern; the boxed predicate keeps the `Vec` alive for the iterator's
        // lifetime.
        fn suffix_predicate<V>(suffix: Vec<u8>) -> SuffixPredicate<V> {
            Box::new(move |(k, _): &(&[u8], &V)| k.ends_with(&suffix))
        }

        let iter = match (self.disk_mode(), iter_mode) {
            (true, IterMode::Prefix) => {
                TagIndexIteratorImpl::DiskAll(self.disk_prefixed_iter_values(pattern))
            }
            (true, IterMode::Contains) => {
                TagIndexIteratorImpl::DiskContains(self.disk_contains_iter_values(pattern))
            }
            (true, IterMode::Suffix) => TagIndexIteratorImpl::DiskSuffix(
                self.disk_iter_values(),
                suffix_predicate(pattern.to_vec()),
            ),
            (true, IterMode::Wildcard) => {
                TagIndexIteratorImpl::DiskWildcard(self.disk_wildcard_iter_values(pattern))
            }
            (false, IterMode::Prefix) => TagIndexIteratorImpl::MemAll(self.iter_prefix(pattern)),
            (false, IterMode::Contains) => {
                TagIndexIteratorImpl::MemContains(self.contains_iter_values(pattern))
            }
            (false, IterMode::Suffix) => {
                TagIndexIteratorImpl::MemSuffix(self.iter(), suffix_predicate(pattern.to_vec()))
            }
            (false, IterMode::Wildcard) => {
                TagIndexIteratorImpl::MemWildcard(self.wildcard_iter_values(pattern))
            }
        };

        TagIndexIterator { iter }
    }

    /// Iterate over all entries of the suffix index, in lexicographical order,
    /// or `None` when the index was created without `WITHSUFFIXTRIE`.
    pub fn suffix_value_iter(&self) -> Option<TagIndexIterator<'_>> {
        let iter = self.iter_suffix_entries()?;
        Some(TagIndexIterator {
            iter: TagIndexIteratorImpl::SuffixEntries(iter),
        })
    }
}

/// A reader over the postings (document ids) of a single tag value's
/// [`InvertedIndex<DocIdsOnly>`], driven with [`next_record`](Self::next_record).
pub struct TagValueReader<'trie> {
    reader: IndexReaderCore<'trie, DocIdsOnly>,
}

impl<'trie> TagValueReader<'trie> {
    /// Open a reader over `ii`'s postings.
    pub fn new(ii: &'trie InvertedIndex<DocIdsOnly>) -> Self {
        Self {
            reader: ii.reader(),
        }
    }

    /// Read the next record into `res`, returning `true` when a record was
    /// written and `false` at the end of the postings.
    pub fn next_record(&mut self, res: &mut RSIndexResult<'trie>) -> bool {
        self.reader.next_record(res).unwrap_or_default()
    }
}
