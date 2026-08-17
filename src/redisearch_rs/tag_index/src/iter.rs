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
//! [`TagIndexIterator`] walks the tag *values* — the keys of the values trie —
//! optionally filtered by a pattern and bounded by a timeout. It is generic over
//! the trie's payload rather than over the index's storage mode, because that is
//! all the walk depends on: [`MemTagIndexIterator`] yields each tag together with
//! its [`InvertedIndex<DocIdsOnly>`], while [`DiskTagIndexIterator`] yields the tag
//! alone, its postings living on disk.
//!
//! [`TagValueReader`] reads the postings (document ids) of a single tag value.

use std::time::Instant;

use ffi::timespec;
use index_result::RSIndexResult;
use inverted_index::{IndexReader, IndexReaderCore, InvertedIndex, doc_ids_only::DocIdsOnly};
use lending_iterator::LendingIterator as _;
use rqe_wildcard::WildcardPattern;
use trie_rs::{
    TrieMap,
    iter::{ContainsLendingIter, LendingIter, WildcardLendingIter, filter::VisitAll},
};

use crate::{InMemoryMode, TagIndex};

/// Value type stored in the memory-mode values trie. Boxed so the heap
/// [`InvertedIndex`] address stays stable across trie restructuring — callers hold
/// it across mutations.
type BoxedInvertedIndex = Box<InvertedIndex<DocIdsOnly>>;

/// Predicate the suffix variant filters a full trie walk with. It owns a copy of
/// the queried suffix, so its closure type is unnameable and has to be boxed to
/// appear in an enum variant. This is a boxed *predicate*, not a boxed iterator —
/// dispatch over the iterator shapes below stays static.
type SuffixPredicate<'a, V> = Box<dyn Fn(&(&[u8], &V)) -> bool + 'a>;

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

/// The concrete tag-value iterator, one variant per underlying `trie_rs` iterator
/// shape. Kept as an enum (rather than a `Box<dyn>` trait object) so iteration
/// dispatches statically.
enum TagIndexIteratorImpl<'ti, Value> {
    /// Full iteration or prefix filter.
    All(LendingIter<'ti, Value, VisitAll>),
    /// Entries whose key contains a fragment.
    Contains(ContainsLendingIter<'ti, 'ti, Value>),
    /// Entries whose key matches a wildcard pattern.
    Wildcard(WildcardLendingIter<'ti, 'ti, Value>),
    /// Entries whose key ends with a suffix. A trie cannot seek by suffix, so this
    /// pairs a full walk with the predicate the walk is filtered by; see
    /// [`next_entry`](TagIndexIterator::next_entry) for why the two are kept side
    /// by side rather than combined into a filtering adapter.
    Suffix(
        LendingIter<'ti, Value, VisitAll>,
        SuffixPredicate<'ti, Value>,
    ),
}

impl<Value> TagIndexIteratorImpl<'_, Value> {
    /// Forward deadline to the behind iterator.
    fn set_timeout(&mut self, deadline: Option<Instant>) {
        match self {
            Self::All(it) => it.set_timeout(deadline),
            Self::Contains(it) => it.set_timeout(deadline),
            Self::Wildcard(it) => it.set_timeout(deadline),
            Self::Suffix(it, _) => it.set_timeout(deadline),
        }
    }
}

/// An iterator over the values (tags) stored in a [`TagIndex`], returned by
/// [`TagIndex::value_iter`] and [`TagIndex::value_iter_filtered`].
///
/// `V` is the payload the index's values trie stores per tag, so the storage mode
/// picks the instantiation: [`MemTagIndexIterator`] or [`DiskTagIndexIterator`].
///
/// Drive either with its `advance`, which returns `None` at the end of the
/// iteration or once the deadline set by [`set_timeout`](Self::set_timeout) — the
/// bound for long affix expansions — has passed. The key it yields is borrowed
/// from trie-internal storage, and is invalidated by the next call.
pub struct TagIndexIterator<'ti, Value> {
    iter: TagIndexIteratorImpl<'ti, Value>,
}

/// A [`TagIndexIterator`] over a memory-mode index, yielding each tag's postings
/// alongside it.
pub type MemTagIndexIterator<'ti> = TagIndexIterator<'ti, BoxedInvertedIndex>;

/// A [`TagIndexIterator`] over a disk-mode index, whose values trie records only
/// that a tag is present.
pub type DiskTagIndexIterator<'ti> = TagIndexIterator<'ti, ()>;

impl<'ti, Value> TagIndexIterator<'ti, Value> {
    /// The key and trie payload of the next entry, which each mode's `advance`
    /// projects onto what that mode can offer.
    ///
    /// The suffix variant filters a full walk in place with
    /// [`LendingIterator::find`](lending_iterator::LendingIterator::find) rather
    /// than through a `filter` adapter, because the borrow of the key it yields
    /// ends with the call: an adapter would have to hold it across iterations.
    fn next_entry(&mut self) -> Option<(&[u8], &Value)> {
        match &mut self.iter {
            TagIndexIteratorImpl::All(it) => it.next(),
            TagIndexIteratorImpl::Contains(it) => it.next(),
            TagIndexIteratorImpl::Wildcard(it) => it.next(),
            TagIndexIteratorImpl::Suffix(it, matches) => it.find(&mut *matches),
        }
    }

    /// Set the deadline honored while iterating, or clear it with `None` —
    /// matching the affix-expansion deadline used elsewhere in this crate.
    pub fn set_timeout(&mut self, timeout: Option<timespec>) {
        self.iter.set_timeout(crate::expansion_deadline(timeout));
    }
}

impl MemTagIndexIterator<'_> {
    /// Advance to the next entry and return the tag together with its postings, per
    /// [`TagIndexIterator`]'s iteration semantics.
    pub fn advance(&mut self) -> Option<(&[u8], &InvertedIndex<DocIdsOnly>)> {
        // The trie stores a `Box<InvertedIndex>`; callers hold and dereference the
        // heap `InvertedIndex`, so hand out that stable address (the `Box`'s heap
        // content) rather than the box slot in the trie node.
        self.next_entry().map(|(k, ii)| (k, &**ii))
    }
}

impl DiskTagIndexIterator<'_> {
    /// Advance to the next entry and return the tag, per [`TagIndexIterator`]'s
    /// iteration semantics.
    ///
    /// There is no value to yield: the trie records only that the tag exists, and
    /// its postings are read from disk by [`open_reader`](TagIndex::open_reader),
    /// keyed by this tag.
    pub fn advance(&mut self) -> Option<&[u8]> {
        self.next_entry().map(|(k, ())| k)
    }
}

/// Walk every entry of `values`, in lexicographical order of the key.
fn all_iter<Value>(values: &TrieMap<Value>) -> TagIndexIterator<'_, Value> {
    TagIndexIterator {
        iter: TagIndexIteratorImpl::All(values.lending_iter()),
    }
}

/// Walk the entries of `values` whose key matches `pattern` under `iter_mode`, in
/// lexicographical order of the key.
fn filtered_iter<'a, Value>(
    values: &'a TrieMap<Value>,
    pattern: &'a [u8],
    iter_mode: IterMode,
) -> TagIndexIterator<'a, Value> {
    // The suffix mode filters a full trie walk by an owned copy of the pattern; the
    // boxed predicate keeps the `Vec` alive for the iterator's lifetime.
    fn suffix_predicate<'a, V>(suffix: Vec<u8>) -> SuffixPredicate<'a, V> {
        Box::new(move |(k, _): &(&[u8], &V)| k.ends_with(&suffix))
    }

    let iter = match iter_mode {
        IterMode::Prefix => TagIndexIteratorImpl::All(values.prefixed_lending_iter(pattern)),
        IterMode::Contains => TagIndexIteratorImpl::Contains(values.contains_iter(pattern).into()),
        IterMode::Suffix => {
            TagIndexIteratorImpl::Suffix(values.lending_iter(), suffix_predicate(pattern.to_vec()))
        }
        IterMode::Wildcard => TagIndexIteratorImpl::Wildcard(
            values.wildcard_iter(WildcardPattern::parse(pattern)).into(),
        ),
    };

    TagIndexIterator { iter }
}

impl TagIndex<InMemoryMode> {
    /// Iterate over all `(tag, inverted index)` entries, in lexicographical order
    /// of the tag.
    pub fn value_iter(&self) -> MemTagIndexIterator<'_> {
        all_iter(&self.mode.values)
    }

    /// Iterate over the `(tag, inverted index)` entries whose tag matches
    /// `pattern` under `iter_mode`, in lexicographical order of the tag.
    ///
    /// `pattern` is borrowed for the iterator's lifetime by the prefix, contains,
    /// and wildcard modes (the suffix mode copies it).
    pub fn value_iter_filtered<'a>(
        &'a self,
        pattern: &'a [u8],
        iter_mode: IterMode,
    ) -> MemTagIndexIterator<'a> {
        filtered_iter(&self.mode.values, pattern, iter_mode)
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
