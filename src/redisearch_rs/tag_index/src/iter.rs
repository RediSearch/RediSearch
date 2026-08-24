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
//! [`SuffixEntryIterator`] walks the suffix trie instead, which both modes share
//! and which holds no postings either way.
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

use crate::{InMemoryMode, OnDiskMode, SuffixData, Tag, TagIndex, TagIndexMode};

/// Value type stored in the memory-mode values trie. Boxed so the heap
/// [`InvertedIndex`] address stays stable across trie restructuring — callers hold
/// it across mutations.
type BoxedInvertedIndex = Box<InvertedIndex<DocIdsOnly>>;

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
/// shape.
enum TagIndexIteratorImpl<'ti, Value> {
    /// Full iteration or prefix filter.
    All(LendingIter<'ti, Value, VisitAll>),
    /// Entries whose key contains a fragment.
    Contains(ContainsLendingIter<'ti, 'ti, Value>),
    /// Entries whose key matches a wildcard pattern.
    Wildcard(WildcardLendingIter<'ti, 'ti, Value>),
    /// Entries whose key ends with a suffix — the brute-force path a `*foo` query
    /// takes when the field was created without `WITHSUFFIXTRIE`, so there is no
    /// suffix trie to turn it into a prefix lookup.
    Suffix(LendingIter<'ti, Value, VisitAll>, Tag<'ti>),
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
/// `Value` is the payload the index's values trie stores per tag, so the storage mode
/// picks the instantiation: [`MemTagIndexIterator`] or [`DiskTagIndexIterator`].
///
/// Drive either with its `advance`, which returns `None` at the end of the
/// iteration or once the deadline set by [`set_timeout`](Self::set_timeout) has
/// passed. The tag it yields is borrowed from trie-internal storage, and is
/// invalidated by the next call.
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
    /// The tag and trie payload of the next entry, which each mode's `advance`
    /// projects onto what that mode can offer.
    ///
    /// The tag borrows from this call, not from the trie itself. It is invalidated by the next call.
    fn next_entry(&mut self) -> Option<(Tag<'_>, &Value)> {
        let (k, v) = match &mut self.iter {
            TagIndexIteratorImpl::All(it) => it.next(),
            TagIndexIteratorImpl::Contains(it) => it.next(),
            TagIndexIteratorImpl::Wildcard(it) => it.next(),
            TagIndexIteratorImpl::Suffix(it, suffix) => {
                let suffix = *suffix;
                it.find(move |(k, _)| k.ends_with(suffix.as_bytes()))
            }
        }?;
        // SAFETY: this walks a `TagIndex` values trie, which is only ever
        // populated through `Tag`-typed keys (see `TagIndex::index`), so every
        // key it yields satisfies `Tag`'s NUL-free invariant.
        Some((unsafe { Tag::new_unchecked(k) }, v))
    }

    /// Set the deadline honored while iterating, or clear it with `None`.
    pub fn set_timeout(&mut self, timeout: Option<timespec>) {
        self.iter.set_timeout(crate::expansion_deadline(timeout));
    }
}

impl MemTagIndexIterator<'_> {
    /// Advance to the next entry and return the tag together with its postings, per
    /// [`TagIndexIterator`]'s iteration semantics.
    pub fn advance(&mut self) -> Option<(Tag<'_>, &InvertedIndex<DocIdsOnly>)> {
        // The trie stores a `Box<InvertedIndex>`; callers hold and dereference the
        // heap `InvertedIndex`, so hand out that stable address.
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
    pub fn advance(&mut self) -> Option<Tag<'_>> {
        self.next_entry().map(|(k, ())| k)
    }
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
    /// `pattern` is borrowed for the iterator's lifetime.
    pub fn value_iter_filtered<'a>(
        &'a self,
        pattern: Tag<'a>,
        iter_mode: IterMode,
    ) -> MemTagIndexIterator<'a> {
        filtered_iter(&self.mode.values, pattern, iter_mode)
    }
}

impl TagIndex<OnDiskMode> {
    /// Iterate over all tags, in lexicographical order.
    pub fn value_iter(&self) -> DiskTagIndexIterator<'_> {
        all_iter(&self.mode.values)
    }

    /// Iterate over the tags matching `pattern` under `iter_mode`, in
    /// lexicographical order.
    ///
    /// The values trie holds only tag presence, so callers resolve each reader by
    /// tag string.
    ///
    /// `pattern` is borrowed for the iterator's lifetime by the prefix, contains,
    /// and wildcard modes (the suffix mode copies it).
    pub fn value_iter_filtered<'a>(
        &'a self,
        pattern: Tag<'a>,
        iter_mode: IterMode,
    ) -> DiskTagIndexIterator<'a> {
        filtered_iter(&self.mode.values, pattern, iter_mode)
    }
}

/// An iterator over the entries of a [`TagIndex`]'s
/// [suffix index](crate::TagSuffixIndex), returned by
/// [`TagIndex::suffix_value_iter`].
///
/// Yields the suffixes only — the suffix trie's payload is internal bookkeeping —
/// and is the same in both storage modes, which share the suffix trie.
pub struct SuffixEntryIterator<'ti> {
    iter: LendingIter<'ti, SuffixData, VisitAll>,
}

impl<'ti> SuffixEntryIterator<'ti> {
    /// Advance to the next suffix-trie entry, honoring the optional timeout.
    /// `None` at the end of the iteration, or when the timeout is reached.
    ///
    /// The suffix is borrowed from trie-internal storage and is invalidated by the
    /// next call. It is one suffix of an indexed tag, not necessarily a whole tag.
    pub fn advance(&mut self) -> Option<Tag<'_>> {
        let (k, _) = self.iter.next()?;
        // SAFETY: `TagSuffixIndex::add` keys this trie by `&bytes[start..]` slices
        // of a `Tag`, and a slice of NUL-free bytes is NUL-free.
        Some(unsafe { Tag::new_unchecked(k) })
    }

    /// Set the deadline honored while iterating, or clear it with `None`.
    pub fn set_timeout(&mut self, timeout: Option<timespec>) {
        self.iter.set_timeout(crate::expansion_deadline(timeout));
    }
}

impl<Mode: TagIndexMode> TagIndex<Mode> {
    /// Iterate over all entries of the suffix index, in lexicographical order, or
    /// `None` when the index was created without `WITHSUFFIXTRIE`.
    pub fn suffix_value_iter(&self) -> Option<SuffixEntryIterator<'_>> {
        Some(SuffixEntryIterator {
            iter: self.iter_suffix_entries()?,
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
    ///
    /// A decoding failure is an error, not an end of postings: collapsing the two
    /// would silently drop the tail of a corrupted posting list instead of letting
    /// the caller report it.
    pub fn next_record(&mut self, res: &mut RSIndexResult<'trie>) -> std::io::Result<bool> {
        self.reader.next_record(res)
    }
}

/// Iterate over all `(tag, value)` entries of a values trie, in lexicographical
/// order of the tag.
fn all_iter<Value>(values: &TrieMap<Value>) -> TagIndexIterator<'_, Value> {
    TagIndexIterator {
        iter: TagIndexIteratorImpl::All(values.lending_iter()),
    }
}

/// Iterate over the `(tag, value)` entries of a values trie whose tag matches
/// `pattern` under `iter_mode`, in lexicographical order of the tag.
///
/// `pattern` is borrowed for the iterator's lifetime.
fn filtered_iter<'a, Value>(
    values: &'a TrieMap<Value>,
    pattern: Tag<'a>,
    iter_mode: IterMode,
) -> TagIndexIterator<'a, Value> {
    let bytes = pattern.as_bytes();
    let iter = match iter_mode {
        IterMode::Prefix => TagIndexIteratorImpl::All(values.prefixed_lending_iter(bytes)),
        IterMode::Contains => TagIndexIteratorImpl::Contains(values.contains_iter(bytes).into()),
        // The walk carries the pattern itself; `next_entry` does the matching.
        IterMode::Suffix => TagIndexIteratorImpl::Suffix(values.lending_iter(), pattern),
        IterMode::Wildcard => TagIndexIteratorImpl::Wildcard(
            values.wildcard_iter(WildcardPattern::parse(bytes)).into(),
        ),
    };

    TagIndexIterator { iter }
}
