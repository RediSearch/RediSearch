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

use ffi::timespec;
use index_result::RSIndexResult;
use inverted_index::{IndexReader, IndexReaderCore, InvertedIndex, doc_ids_only::DocIdsOnly};
use lending_iterator::LendingIterator as _;
use trie_rs::iter::{LendingIter, PatternLendingIter, filter::VisitAll};

use crate::{InMemoryMode, SuffixData, Tag, TagIndex, TagIndexMode};

/// Value type stored in the memory-mode values trie. Boxed so the heap
/// [`InvertedIndex`] address stays stable across trie restructuring — callers hold
/// it across mutations.
type BoxedInvertedIndex = Box<InvertedIndex<DocIdsOnly>>;

/// Which subset of tag values a [filtered iterator](TagIndex::value_iter_filtered)
/// walks.
pub use trie_rs::iter::PatternMode as IterMode;

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
    iter: PatternLendingIter<'ti, 'ti, Value>,
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
        let (k, v) = self.iter.next()?;
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
        TagIndexIterator {
            iter: self.mode.values.lending_iter().into(),
        }
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
        let iter = self
            .mode
            .values
            .pattern_lending_iter(pattern.as_bytes(), iter_mode);

        TagIndexIterator { iter }
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
