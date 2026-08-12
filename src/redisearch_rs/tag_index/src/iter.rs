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
//! [`ValueIterator`] walks the tag *values* (the keys of the values trie, or
//! the suffix trie), optionally filtered by a pattern and bounded by a timeout.
//! Its [`advance`](ValueIterator::advance) yields, for each tag, the borrowed key
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
use rqe_iterators::utils::duration_from_redis_timespec;
use trie_rs::iter::{ContainsLendingIter, LendingIter, WildcardLendingIter, filter::VisitAll};

use crate::{SuffixData, TagIndex};

/// Value type stored in the memory-mode values trie. Boxed so the heap
/// [`InvertedIndex`] address stays stable across trie restructuring — callers hold
/// it across mutations.
type MemValue = Box<InvertedIndex<DocIdsOnly>>;

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
/// Memory-mode variants carry [`MemValue`] values whose stable heap address is
/// exposed to callers; disk-mode variants carry `()` (the trie holds only tag
/// presence, postings live on disk) and the suffix-trie variant carries opaque
/// [`SuffixData`] — both yield no value.
enum ValueIteratorImpl<'ti> {
    /// Memory mode, full iteration or prefix filter.
    MemAll(LendingIter<'ti, MemValue, VisitAll>),
    /// Memory mode, entries whose key contains a fragment.
    MemContains(ContainsLendingIter<'ti, 'ti, MemValue>),
    /// Memory mode, entries whose key matches a wildcard pattern.
    MemWildcard(WildcardLendingIter<'ti, 'ti, MemValue>),
    /// Memory mode, entries whose key ends with a suffix. A trie cannot seek by
    /// suffix, so this pairs a full walk with the predicate the walk is filtered
    /// by; see [`advance`](ValueIterator::advance) for why the two are kept
    /// side by side rather than combined into a filtering adapter.
    MemSuffix(
        LendingIter<'ti, MemValue, VisitAll>,
        SuffixPredicate<MemValue>,
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

impl ValueIteratorImpl<'_> {
    /// Hand `deadline` to the underlying trie iterator, which enforces it inside
    /// its own traversal loop — see [`ValueIterator::set_timeout`].
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
pub struct ValueIterator<'ti> {
    iter: ValueIteratorImpl<'ti>,
}

impl<'ti> ValueIterator<'ti> {
    /// Advance to the next entry, honoring the optional timeout, and return the
    /// key together with the value:
    ///
    /// - memory-mode entries yield `Some(&InvertedIndex)` — the stable heap
    ///   address of the tag's posting list;
    /// - disk-mode and suffix-trie entries yield `None` (they carry no
    ///   in-memory value).
    ///
    /// Returns `None` at the end of the iteration, or when the timeout is
    /// reached. The key slice is borrowed from trie-internal storage and is
    /// invalidated by the next call.
    pub fn advance(&mut self) -> Option<(&[u8], Option<&InvertedIndex<DocIdsOnly>>)> {
        // The memory-mode value is a `&Box<InvertedIndex>`; callers hold and
        // dereference the heap `InvertedIndex`, so hand out that stable address
        // (the `Box`'s heap content, via deref coercion), not the box slot in
        // the trie node.
        fn mem_value(v: &MemValue) -> &InvertedIndex<DocIdsOnly> {
            v
        }

        match &mut self.iter {
            ValueIteratorImpl::MemAll(it) => it.next().map(|(k, v)| (k, Some(mem_value(v)))),
            ValueIteratorImpl::MemContains(it) => it.next().map(|(k, v)| (k, Some(mem_value(v)))),
            ValueIteratorImpl::MemWildcard(it) => it.next().map(|(k, v)| (k, Some(mem_value(v)))),
            // The suffix modes skip the non-matching entries with `find` — inside
            // the trie's own traversal loop, so the deadline `set_timeout`
            // installed is probed per *visited* entry. A filtering adapter wrapped
            // around the trie iterator would instead leave the whole skipped run
            // unbounded, since the deadline could then only be probed per *yielded*
            // match.
            ValueIteratorImpl::MemSuffix(it, matches) => {
                it.find(&mut *matches).map(|(k, v)| (k, Some(mem_value(v))))
            }
            ValueIteratorImpl::DiskAll(it) => it.next().map(|(k, ())| (k, None)),
            ValueIteratorImpl::DiskContains(it) => it.next().map(|(k, ())| (k, None)),
            ValueIteratorImpl::DiskWildcard(it) => it.next().map(|(k, ())| (k, None)),
            ValueIteratorImpl::DiskSuffix(it, matches) => {
                it.find(&mut *matches).map(|(k, ())| (k, None))
            }
            ValueIteratorImpl::SuffixEntries(it) => it.next().map(|(k, _)| (k, None)),
        }
    }

    /// Set the deadline honored while iterating (affix queries). Iteration ends,
    /// as if the trie were exhausted, once it is reached.
    ///
    /// `timeout` is an absolute `CLOCK_MONOTONIC_RAW` deadline. It is handed to the
    /// underlying trie iterator, which enforces it inside its traversal loop
    /// (amortized over a fixed number of steps, like
    /// [`expansion_timeout`](crate::expansion_timeout) does for suffix-trie
    /// expansion). That is what bounds a walk whose entries mostly do *not* match:
    /// the deadline is probed per visited entry, not per yielded match.
    ///
    /// An all-zero `timeout` clears the deadline rather than setting one: taken
    /// literally it is a deadline long past, which would abort the iteration on
    /// the first probe. The Redis "no timeout" sentinel is handled by
    /// [`duration_from_redis_timespec`].
    pub fn set_timeout(&mut self, timeout: timespec) {
        let deadline = if timeout.tv_sec == 0 && timeout.tv_nsec == 0 {
            None
        } else {
            duration_from_redis_timespec(timeout).map(|remaining| Instant::now() + remaining)
        };

        self.iter.set_timeout(deadline);
    }
}

impl TagIndex {
    /// Iterate over all tag values, in lexicographical order, in either mode.
    pub fn value_iter(&self) -> ValueIterator<'_> {
        let iter = if self.disk_mode() {
            ValueIteratorImpl::DiskAll(self.disk_iter_values())
        } else {
            ValueIteratorImpl::MemAll(self.iter_values())
        };
        ValueIterator { iter }
    }

    /// Iterate over the tag values matching `pattern` under `mode`, in
    /// lexicographical order.
    ///
    /// In disk mode the values trie holds only tag presence, so callers resolve
    /// each reader by tag string; in memory mode the yielded value is still
    /// exposed by [`advance`](ValueIterator::advance).
    ///
    /// `pattern` is borrowed for the iterator's lifetime by the prefix,
    /// contains, and wildcard modes (the suffix mode copies it).
    pub fn value_iter_filtered<'a>(
        &'a self,
        pattern: &'a [u8],
        mode: IterMode,
    ) -> ValueIterator<'a> {
        // The suffix mode filters a full trie walk by an owned copy of the
        // pattern; the boxed predicate keeps the `Vec` alive for the iterator's
        // lifetime.
        fn suffix_predicate<V>(suffix: Vec<u8>) -> SuffixPredicate<V> {
            Box::new(move |(k, _): &(&[u8], &V)| k.ends_with(&suffix))
        }

        let iter = if self.disk_mode() {
            match mode {
                IterMode::Prefix => {
                    ValueIteratorImpl::DiskAll(self.disk_prefixed_iter_values(pattern))
                }
                IterMode::Contains => {
                    ValueIteratorImpl::DiskContains(self.disk_contains_iter_values(pattern))
                }
                IterMode::Suffix => ValueIteratorImpl::DiskSuffix(
                    self.disk_iter_values(),
                    suffix_predicate(pattern.to_vec()),
                ),
                IterMode::Wildcard => {
                    ValueIteratorImpl::DiskWildcard(self.disk_wildcard_iter_values(pattern))
                }
            }
        } else {
            match mode {
                IterMode::Prefix => ValueIteratorImpl::MemAll(self.prefixed_iter_values(pattern)),
                IterMode::Contains => {
                    ValueIteratorImpl::MemContains(self.contains_iter_values(pattern))
                }
                IterMode::Suffix => ValueIteratorImpl::MemSuffix(
                    self.iter_values(),
                    suffix_predicate(pattern.to_vec()),
                ),
                IterMode::Wildcard => {
                    ValueIteratorImpl::MemWildcard(self.wildcard_iter_values(pattern))
                }
            }
        };

        ValueIterator { iter }
    }

    /// Iterate over all entries of the suffix index, in lexicographical order,
    /// or `None` when the index was created without `WITHSUFFIXTRIE`.
    ///
    /// Only the keys are meaningful; [`advance`](ValueIterator::advance) yields
    /// `None` for the value.
    pub fn suffix_value_iter(&self) -> Option<ValueIterator<'_>> {
        let iter = self.iter_suffix_entries()?;
        Some(ValueIterator {
            iter: ValueIteratorImpl::SuffixEntries(iter),
        })
    }
}

/// A reader over the postings (document ids) of a single tag value's
/// [`InvertedIndex<DocIdsOnly>`], driven with [`next`](Self::next).
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
    pub fn next(&mut self, res: &mut RSIndexResult<'trie>) -> bool {
        self.reader.next_record(res).unwrap_or_default()
    }
}
