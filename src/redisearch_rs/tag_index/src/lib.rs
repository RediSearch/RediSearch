/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! [`TagIndex`] is an index that indexes textual tags for documents.
//!
//! It supports two storage modes, chosen by the constructor used
//! ([`TagIndex::new_in_memory`] or [`TagIndex::new_on_disk`]):
//!
//! - **Memory mode** keeps the per-tag posting lists (document ids) inline in
//!   the values trie.
//! - **Disk mode** keeps the postings on disk behind the
//!   `SearchDisk_*` API; the values trie holds only tag *presence* sentinels.
//!   Writes stage onto a disk write batch ([`TagIndex::index`]), reads open a
//!   disk iterator ([`TagIndex::open_reader`]), and query expansion still walks
//!   the presence trie for matching keys before opening each reader by string.
//!
//! ## Tag bytes
//!
//! Every method here takes **NUL-free** tag bytes.
//!
//! The one place a terminator is visible is the other direction: the terms yielded
//! by [`TagIndex::suffix_expand`] carry theirs, so their pointers are usable as a
//! C `char *`. [`TagSuffixIndex`] owns that terminator and documents where it
//! comes from.
//!
//! [`TagIndex`] uses the same indexes as the full text but in a simpler manner. In fact:
//!
//! 1. An entire tag index resides in a single redis key, and doesn't have a key per term
//!
//! 2. We do not perform stemming on tags
//!
//! 3. The tokenization is simpler: The user can determine a separator (default to comma `,`),
//!    and we do whitespace trimming at the end of tags. Thus, tags can contain spaces (in the middle),
//!    punctuation marks, accents, etc. The only two transformations we perform are
//!    lower-casing (not unicode sensitive as of now), and whitespace trimming.
//!
//! 4. Tags cannot be found from a general full-text search. i.e. if a document has a field called "tags"
//!    with the values "foo" and "bar", searching for "foo" or "bar" without a special
//!    tag modifier (see below) will not return the document.
//!
//! 5. The index is much simpler and more compressed: We do not store frequencies, offset vectors of field flags.
//!    The index contains only document ids encoded as delta. This means that an entry in a tag index is usually one or two bytes long.
//!    This makes them very memory efficient and fast.
//!
//!
//! ## Creating a tag field
//!
//! Tag fields can be added to the schema in `FT.CREATE` with the following syntax:
//! ```text
//! FT.CREATE ... SCHEMA ... {field_name} TAG [SEPARATOR {sep}]
//! ```
//! `SEPARATOR` defaults to a comma (`,`), and can be any printable ascii character.  For example:
//! ```text
//! FT.CREATE idx SCHEMA tags TAG SEPARATOR ";"
//! ```
//!
//! An unlimited number of tag fields can be created per document, as long as the overall number of
//! fields is under 1024.
//!
//! ### Suffix and contains matching
//!
//! By default a tag query matches a tag either exactly (`@tags:{foo}`) or by
//! prefix (`@tags:{foo*}`, every tag starting with `foo`). Two more wildcard
//! forms are supported:
//!
//! - **Suffix** — `@tags:{*foo}` matches every tag that *ends* with `foo`.
//! - **Contains** (infix) — `@tags:{*foo*}` matches every tag that *contains* `foo`.
//!
//! A trie resolves a prefix quickly by walking down to the prefix node, but it
//! cannot do the same for a suffix or an infix without scanning every tag. To
//! make those queries efficient, the field can be created with a *suffix trie*:
//! ```text
//! FT.CREATE idx SCHEMA tags TAG WITHSUFFIXTRIE
//! ```
//! The suffix trie indexes *every suffix* of each tag, so a `*foo` / `*foo*`
//! query becomes a plain prefix lookup on the suffix trie (see
//! [`TagSuffixIndex`]).
//!
//! NB: suffix and contains queries also work without `WITHSUFFIXTRIE`, but they
//! fall back to a brute-force scan of the whole tag trie.
//!
//! ## Querying Tag Fields
//!
//! As mentioned above, just searching for a tag without any modifiers will not retrieve documents
//! containing it.
//! The syntax for matching tags in a query is as follows (the curly braces are part of the syntax in
//! this case):
//! ```text
//! @<field_name>:{ <tag> | <tag> | ...}
//! ```
//!  e.g.
//! ```text
//! @tags:{hello world | foo bar}
//! ```
//!  **IMPORTANT**: When specifying multiple tags in the same tag clause, the semantic meaning is a
//!    **UNION** of the documents containing any of the tags (as in an SQL `WHERE IN` clause).
//!    If you need to intersect tags, you should repeat several tag clauses.
//!    For example:
//! ```text
//! FT.SEARCH idx "@tags:{hello | world}"
//! ```
//! Will return documents containing either hello or world (or both). But:
//! ```text
//! FT.SEARCH idx "@tags:{hello} @tags:{world}"
//! ```
//! Will return documents containing **both tags**.
//!
//! Notice that since tags can contain spaces (the separator by default is a comma), so can tags in
//! the query.
//!
//! However, if a tag contains stopwords (for example, the tag `to be or not to be` will cause a
//! syntax error),
//! you can alternatively escape the spaces inside the tags to avoid syntax errors. In redis-cli and
//! some clients, a second escaping is needed:
//!
//! ```text
//! 127.0.0.1:6379> FT.SEARCH idx "@tags:{to\\ be\\ or\\ not\\ to\\ be}"
//! ```
//!

mod iter;
mod suffix;

pub use iter::{IterMode, TagValueReader, ValueIterator};

// Force-link the umbrella `redisearch_rs` crate so its `#[used]` symbol table keeps the
// Rust FFI functions that the linked C code (`libredisearch_c_bundle`) calls back into, and
// stub any remaining Redis module C symbols the tests pull in. Without the `extern crate`
// reference the umbrella rlib is dropped as unused and those symbols go undefined at link
// time. Mirrors `numeric_range_tree`/`query_eval`/`top_k`.
#[cfg(test)]
extern crate redisearch_rs;

#[cfg(test)]
redis_mock::mock_or_stub_missing_redis_c_symbols!();

use std::ffi::c_char;
use std::ptr::NonNull;
use std::time::Instant;

use ffi::{
    IndexFlags_Index_DocIdsOnly, QueryError, QueryIterator, RSToken, RedisSearchCtx,
    RedisSearchDiskIndexSpec, SearchDiskWriteBatchHandle, t_fieldIndex, timespec,
};
use field::{FieldExpirationPredicate, FieldFilterContext, FieldMaskOrIndex};
use index_result::RSIndexResult;
use inverted_index::{
    DocId, GcApplyInfo, GcScanDelta, IndexReader, InvertedIndex, doc_ids_only::DocIdsOnly,
};
use query_term::RSQueryTerm;
use redis_module::RedisModuleCtx;
use rqe_iterators::{
    FieldExpirationChecker,
    interop::RQEIteratorWrapper,
    inverted_index::{Tag, TagLookup},
    utils::duration_from_redis_timespec,
};
use rqe_wildcard::{MatchOutcome, WildcardPattern};
pub(crate) use suffix::{SuffixData, TagSuffixIndex};
use trie_rs::{
    TrieMap,
    iter::{
        ContainsLendingIter, LendingIter, RangeFilter, RangeLendingIter, WildcardLendingIter,
        filter::VisitAll,
    },
};

/// Identifies the way the data is stored
enum TagIndexMode {
    /// If the postings (doc_ids) are kept in memory
    InMemory {
        /// tag value -> document ids.
        values: TrieMap<Box<InvertedIndex<DocIdsOnly>>>,
    },
    /// If the postings (doc_ids) are kept on disk
    Disk {
        /// tag value -> (). It is used only to know whether a tag is there
        values: TrieMap<()>,
        /// Field id
        field_id: t_fieldIndex,
        /// Disk Index spec, valid for as long as this index lives — the
        /// invariant [`TagIndex::new_on_disk`] establishes.
        disk_index_spec: NonNull<RedisSearchDiskIndexSpec>,
    },
}

/// See the [crate documentation](self) for an overview.
pub struct TagIndex {
    /// Unique id generated at creation time.
    unique_id: u32,

    /// Suffix index, present only for fields created `WITHSUFFIXTRIE`.
    suffix: Option<TagSuffixIndex>,

    /// The mode: in memory / disk
    mode: TagIndexMode,
}

impl TagIndex {
    /// Create a new, empty index keeping its postings in memory.
    ///
    /// - `id` uniquely identifies this index.
    /// - `with_suffix` enables the [suffix index](TagSuffixIndex)
    ///   (`WITHSUFFIXTRIE`), so suffix (`*foo`) and contains (`*foo*`)
    ///   queries don't have to scan the whole tag trie.
    pub fn new_in_memory(id: u32, with_suffix: bool) -> Self {
        Self::with_mode(
            id,
            with_suffix,
            TagIndexMode::InMemory {
                values: TrieMap::new(),
            },
        )
    }

    /// Create a new, empty index keeping its postings on disk.
    ///
    /// `disk_spec` is paired with `field_id`, the field index the disk API
    /// calls need. `id` and `with_suffix` are as in
    /// [`new_in_memory`](Self::new_in_memory).
    ///
    /// # Safety
    ///
    /// `disk_spec` must point to a [valid] [`RedisSearchDiskIndexSpec`] that
    /// remains valid for the lifetime of the returned [`TagIndex`]: the disk
    /// paths hand it to the `SearchDisk_*` API, which dereferences it.
    pub unsafe fn new_on_disk(
        id: u32,
        disk_spec: NonNull<RedisSearchDiskIndexSpec>,
        field_id: t_fieldIndex,
        with_suffix: bool,
    ) -> Self {
        Self::with_mode(
            id,
            with_suffix,
            TagIndexMode::Disk {
                values: TrieMap::new(),
                field_id,
                disk_index_spec: disk_spec,
            },
        )
    }

    /// The part of construction both constructors share.
    fn with_mode(id: u32, with_suffix: bool, mode: TagIndexMode) -> Self {
        Self {
            unique_id: id,
            suffix: with_suffix.then(TagSuffixIndex::new),
            mode,
        }
    }

    /// The unique id this index was created with.
    pub const fn id(&self) -> u32 {
        self.unique_id
    }

    /// Returns `true` is suffix search is supported
    pub const fn has_suffix(&self) -> bool {
        self.suffix.is_some()
    }

    /// Returns `true` if the postings are backed by disk.
    pub const fn disk_mode(&self) -> bool {
        matches!(self.mode, TagIndexMode::Disk { .. })
    }

    /// How many distinct tags the index holds.
    pub const fn unique_values(&self) -> usize {
        match &self.mode {
            TagIndexMode::InMemory { values } => values.n_unique_keys(),
            TagIndexMode::Disk { values, .. } => values.n_unique_keys(),
        }
    }

    /// Index `doc_id` under each tag in `tags`.
    ///
    /// Returns the [`WritePostingsDelta`] the caller folds into the spec
    /// statistics (records, memory, blocks), or `None` when a disk-mode write
    /// fails.
    ///
    /// In memory mode the postings are written inline into the per-tag
    /// inverted index and `ctx`/`batch` are ignored; this always succeeds. In
    /// disk mode the postings are staged onto `batch` (committed later by
    /// `commitDocument`) and the returned delta is zero — the record count is
    /// tallied in [`commit`](Self::commit).
    ///
    /// `has_field_expiration` records whether this document carries a TTL on the
    /// field being indexed. It is stored per posting as
    /// [`RSIndexResult::has_field_expiration`] and gates the TTL re-check
    /// performed on read.
    ///
    /// # Safety
    ///
    /// Each tag must be NUL-free, in both modes.
    ///
    /// The two conditions below apply to disk mode only: memory mode places no
    /// requirement on `ctx`, `batch`, or the bytes past each tag.
    ///
    /// 1. `ctx` and `batch` must be the valid disk write context and batch handle
    ///    for the ongoing document write.
    /// 2. Each tag must borrow from a NUL-terminated buffer: the byte at
    ///    `tag.as_ptr().add(tag.len())` must be readable and zero, so the pointer
    ///    is usable as the `const char *` the disk API expects. Note this is a
    ///    property of the surrounding buffer, not of the tag bytes.
    pub unsafe fn index(
        &mut self,
        ctx: *const RedisModuleCtx,
        batch: *const SearchDiskWriteBatchHandle,
        tags: &[&[u8]],
        doc_id: DocId,
        has_field_expiration: bool,
    ) -> Option<WritePostingsDelta> {
        debug_assert!(
            tags.iter().all(|tag| !tag.contains(&0)),
            "tag bytes are NUL-free"
        );
        match &mut self.mode {
            TagIndexMode::Disk {
                field_id,
                disk_index_spec,
                ..
            } => {
                if tags.is_empty() {
                    return Some(WritePostingsDelta::default());
                }
                debug_assert!(!ctx.is_null(), "disk-mode indexing needs a write context");
                debug_assert!(
                    !batch.is_null(),
                    "disk-mode indexing needs a disk write batch"
                );
                debug_assert!(
                    tags.iter().all(|tag| {
                        // SAFETY: contract 2 promises the byte just past the tag
                        // is inside the buffer the tag borrows from.
                        let terminator = unsafe { tag.as_ptr().add(tag.len()) };
                        // SAFETY: as above — that byte is readable.
                        unsafe { *terminator == 0 }
                    }),
                    "every tag must borrow from a NUL-terminated buffer"
                );
                // Contract 2 puts a NUL just past each tag, so the tag pointer is
                // directly usable as the `const char *` the disk API expects.
                let mut value_ptrs: Vec<*const c_char> =
                    tags.iter().map(|tag| tag.as_ptr().cast()).collect();
                // SAFETY: `disk_index_spec` is a valid `RedisSearchDiskIndexSpec`
                // (invariant from `new_on_disk`); contract 1 covers `ctx`/`batch`;
                // and by contract 2 every pointer in `value_ptrs` addresses a
                // NUL-terminated string that outlives the call.
                let ok = unsafe {
                    ffi::SearchDisk_IndexTags(
                        ctx.cast_mut(),
                        disk_index_spec.as_ptr(),
                        batch.cast_mut(),
                        value_ptrs.as_mut_ptr(),
                        value_ptrs.len(),
                        doc_id,
                        *field_id,
                    )
                };
                ok.then(WritePostingsDelta::default)
            }
            TagIndexMode::InMemory { values } => {
                Some(write_postings(values, tags, doc_id, has_field_expiration))
            }
        }
    }

    /// Apply the per-tag metadata updates after [`TagIndex::index`]
    /// of a document write: register the tags in the values trie (disk mode
    /// only) and in the
    /// [suffix index](TagSuffixIndex), when enabled.
    ///
    /// Returns the number of records to fold into the spec statistics: in disk
    /// mode the postings are written to disk during this phase, so the
    /// committed tag values are counted here; in memory mode they were already
    /// counted by [`index`](Self::index), so `0` is returned.
    ///
    /// # Safety
    ///
    /// Each tag must be NUL-free. Here that is a
    /// safety requirement rather than a convention, because a `WITHSUFFIXTRIE`
    /// field feeds the tags to [`TagSuffixIndex::add`]; see there for why an
    /// interior NUL is undefined behaviour.
    pub unsafe fn commit(&mut self, tags: &[&[u8]]) -> u32 {
        let disk = self.disk_mode();
        for tag in tags {
            if let TagIndexMode::Disk { values, .. } = &mut self.mode {
                values.insert(tag, ());
            }
            if let Some(suffix) = &mut self.suffix {
                // SAFETY: this method's contract is `TagSuffixIndex::add`'s.
                unsafe { suffix.add(tag) };
            }
        }
        if disk { tags.len() as u32 } else { 0 }
    }

    /// Create a [`QueryIterator`] over the documents matching the given tag,
    /// reading from `ii`.
    ///
    /// `ii` is the inverted index already resolved for `tag` (e.g. while
    /// iterating the values trie), so no lookup is performed at construction
    /// time. The tag is still looked up again on every revalidation, to detect
    /// that the garbage collector removed or replaced the inverted index.
    ///
    /// `lookup` is the handle the iterator revalidates through;
    ///
    /// Returns a null pointer when `ii` holds no documents.
    ///
    /// # Panics
    /// Panics on a disk-mode index: the postings live on disk, so there is no
    /// in-memory inverted index to build a reader over.
    ///
    /// # Safety
    ///
    /// 1. `self` must outlive the returned iterator, and must not be mutated
    ///    while the iterator is in use except under the standard revalidation
    ///    protocol.
    /// 2. `ii` must be the inverted index currently stored in this index's
    ///    values trie for `tag`.
    /// 3. `sctx` and `sctx.spec` must be valid and outlive the returned
    ///    iterator.
    /// 4. `lookup` must hold a `self` reference.
    /// 5. The caller owns the returned iterator and must free it. (`it->Free(it)`).
    pub unsafe fn query_iterator_for_value(
        &self,
        sctx: NonNull<RedisSearchCtx>,
        tag: &[u8],
        ii: &InvertedIndex<DocIdsOnly>,
        weight: f64,
        field_index: t_fieldIndex,
        lookup: TrieLookup,
    ) -> *mut QueryIterator {
        let TagIndexMode::InMemory { values } = &self.mode else {
            unimplemented!()
        };

        if ii.unique_docs() == 0 {
            return std::ptr::null_mut();
        }

        // Same identity check as `gc`: the caller must hand us the trie's
        // current value for this tag.
        debug_assert!(
            values
                .find(tag)
                .map(Box::as_ref)
                .is_some_and(|v| std::ptr::eq(v, ii)),
            "ii must be the inverted index currently stored for tag"
        );

        // SAFETY: contracts 1 to 4 are exactly `get_reader`'s four
        // pre-conditions, and this function's caller upholds them.
        unsafe { self.get_reader(sctx, ii, tag, weight, field_index, lookup) }.as_ptr()
    }

    /// Iterate over all `(tag, inverted index)` entries, in lexicographical
    /// order of the tag.
    pub(crate) fn iter_values(&self) -> LendingIter<'_, Box<InvertedIndex<DocIdsOnly>>, VisitAll> {
        let TagIndexMode::InMemory { values } = &self.mode else {
            unimplemented!()
        };
        values.lending_iter()
    }

    /// Iterate over the `(tag, inverted index)` entries whose tag starts with
    /// `prefix`, in lexicographical order of the tag.
    pub(crate) fn prefixed_iter_values(
        &self,
        prefix: &[u8],
    ) -> LendingIter<'_, Box<InvertedIndex<DocIdsOnly>>, VisitAll> {
        let TagIndexMode::InMemory { values } = &self.mode else {
            unimplemented!()
        };
        values.prefixed_lending_iter(prefix)
    }

    /// Iterate over the `(tag, inverted index)` entries whose tag contains
    /// `fragment`, in lexicographical order of the tag.
    pub(crate) fn contains_iter_values<'tm, 't>(
        &'tm self,
        fragment: &'t [u8],
    ) -> ContainsLendingIter<'tm, 't, Box<InvertedIndex<DocIdsOnly>>> {
        let TagIndexMode::InMemory { values } = &self.mode else {
            unimplemented!()
        };
        values.contains_iter(fragment).into()
    }

    /// Iterate over the `(tag, inverted index)` entries whose tag matches the
    /// wildcard `pattern` (`*` and `?` metacharacters), in lexicographical
    /// order of the tag.
    pub(crate) fn wildcard_iter_values<'tm, 'p>(
        &'tm self,
        pattern: &'p [u8],
    ) -> WildcardLendingIter<'tm, 'p, Box<InvertedIndex<DocIdsOnly>>> {
        let TagIndexMode::InMemory { values } = &self.mode else {
            unimplemented!()
        };
        values.wildcard_iter(WildcardPattern::parse(pattern)).into()
    }

    /// Iterate over the `(tag, inverted index)` entries whose tag falls within
    /// `filter`'s boundaries, in lexicographical order of the tag.
    ///
    /// # Panics
    /// Panics on a disk-mode index, which has no in-memory postings to yield.
    pub fn range_iter_values<'tm, 'f>(
        &'tm self,
        filter: RangeFilter<'f>,
    ) -> RangeLendingIter<'tm, 'f, Box<InvertedIndex<DocIdsOnly>>> {
        let TagIndexMode::InMemory { values } = &self.mode else {
            unimplemented!()
        };
        values.range_iter(filter).into()
    }

    // --- Disk-mode value-trie iterators ---------------------------------
    //
    // In disk mode the values trie holds only tag *presence* (`()`), the
    // postings living on disk. Query expansion (prefix / contains / wildcard /
    // lex-range / suffix) and `FT.TAGVALS` still walk this trie for the
    // matching tag *keys*, then open each reader by tag string via the disk
    // API.

    /// Disk-mode counterpart of [`iter_values`](Self::iter_values).
    ///
    /// # Panics
    /// Panics on a memory-mode index;
    pub(crate) fn disk_iter_values(&self) -> LendingIter<'_, (), VisitAll> {
        let TagIndexMode::Disk { values, .. } = &self.mode else {
            unimplemented!()
        };
        values.lending_iter()
    }

    /// Disk-mode counterpart of
    /// [`prefixed_iter_values`](Self::prefixed_iter_values).
    ///
    /// # Panics
    /// Panics on a memory-mode index;
    pub(crate) fn disk_prefixed_iter_values(&self, prefix: &[u8]) -> LendingIter<'_, (), VisitAll> {
        let TagIndexMode::Disk { values, .. } = &self.mode else {
            unimplemented!()
        };
        values.prefixed_lending_iter(prefix)
    }

    /// Disk-mode counterpart of
    /// [`contains_iter_values`](Self::contains_iter_values).
    ///
    /// # Panics
    /// Panics on a memory-mode index;
    pub(crate) fn disk_contains_iter_values<'tm, 't>(
        &'tm self,
        fragment: &'t [u8],
    ) -> ContainsLendingIter<'tm, 't, ()> {
        let TagIndexMode::Disk { values, .. } = &self.mode else {
            unimplemented!()
        };
        values.contains_iter(fragment).into()
    }

    /// Disk-mode counterpart of
    /// [`wildcard_iter_values`](Self::wildcard_iter_values).
    ///
    /// # Panics
    /// Panics on a memory-mode index;
    pub(crate) fn disk_wildcard_iter_values<'tm, 'p>(
        &'tm self,
        pattern: &'p [u8],
    ) -> WildcardLendingIter<'tm, 'p, ()> {
        let TagIndexMode::Disk { values, .. } = &self.mode else {
            unimplemented!()
        };
        values.wildcard_iter(WildcardPattern::parse(pattern)).into()
    }

    /// Disk-mode counterpart of [`range_iter_values`](Self::range_iter_values).
    ///
    /// # Panics
    /// Panics on a memory-mode index;
    pub fn disk_range_iter_values<'tm, 'f>(
        &'tm self,
        filter: RangeFilter<'f>,
    ) -> RangeLendingIter<'tm, 'f, ()> {
        let TagIndexMode::Disk { values, .. } = &self.mode else {
            unimplemented!()
        };
        values.range_iter(filter).into()
    }

    /// Iterate over all the entries of the [suffix index](TagSuffixIndex), in
    /// lexicographical order of the suffix, or `None` when the index was
    /// created without `WITHSUFFIXTRIE`.
    pub(crate) fn iter_suffix_entries(&self) -> Option<LendingIter<'_, SuffixData, VisitAll>> {
        self.suffix.as_ref().map(TagSuffixIndex::lending_iter)
    }

    /// Apply a garbage-collection `delta` (computed by a fork GC scan) to the
    /// inverted index stored for `tag`. If no document is left afterwards,
    /// the tag is dropped from the values trie and from the
    /// [suffix index](TagSuffixIndex), when enabled.
    ///
    /// `value` is the inverted index the GC scan ran against: when the tag
    /// was removed or its index replaced in the meantime, the delta is stale
    /// and `None` is returned without applying anything.
    ///
    /// On success, returns the [`GcApplyInfo`] describing the applied changes.
    /// Its [`bytes_freed`](GcApplyInfo::bytes_freed) and
    /// [`block_count_delta`](GcApplyInfo::block_count_delta) already account for
    /// the whole posting list being dropped when the tag became empty.
    ///
    /// # Panics
    /// Panics on a disk-mode index;
    pub fn gc(
        &mut self,
        tag: &[u8],
        value: *const InvertedIndex<DocIdsOnly>,
        delta: GcScanDelta,
    ) -> Option<GcApplyInfo> {
        let TagIndexMode::InMemory { values } = &mut self.mode else {
            unreachable!("tag GC runs only in memory mode; disk uses GCPolicy_Disk");
        };
        let ii = values.find_mut(tag)?;
        // The posting list is boxed, so its heap address is stable across trie
        // restructuring; comparing it against the pointer the child scanned
        // detects the tag being removed or its index replaced meanwhile.
        if !std::ptr::eq(&**ii as *const InvertedIndex<DocIdsOnly>, value) {
            return None;
        }

        let mut info = ii.apply_gc(delta);

        if ii.unique_docs() == 0 {
            info.bytes_freed += ii.memory_usage();
            info.block_count_delta -= ii.number_of_blocks() as i64;

            self.remove_tag_value(tag);

            if let Some(suffix) = &mut self.suffix
                && !tag.is_empty()
            {
                suffix.delete(tag);
            }
        }

        Some(info)
    }

    /// Remove `tag` (and its postings) from the values trie.
    ///
    /// # Panics
    /// Panics on a disk-mode index;
    fn remove_tag_value(&mut self, tag: &[u8]) {
        let TagIndexMode::InMemory { values } = &mut self.mode else {
            unreachable!("tag deletion runs only in memory mode; disk uses GCPolicy_Disk");
        };
        values.remove(tag);
    }

    /// Create a [`QueryIterator`] over the documents matching `tag`, or `None`
    /// when the tag is absent or holds no documents.
    ///
    /// In memory mode the tag is resolved in the values trie and the postings are
    /// read inline. In disk mode the reader is built through the disk API, keyed by
    /// the tag string, and filters on the caller's `field_index` rather than the
    /// field recorded at write time.
    ///
    /// `lookup` is only used by the memory-mode iterator, which revalidates by
    /// re-resolving the tag in the values trie. The disk backend owns its reader
    /// and revalidates it itself, so the disk branch drops `lookup` unused.
    ///
    /// # Safety
    ///
    /// 1. `self` must outlive the returned iterator, and must not be mutated
    ///    while it is in use except under the standard revalidation protocol.
    ///    The memory-mode iterator is the one
    ///    [`query_iterator_for_value`](Self::query_iterator_for_value) builds and
    ///    shares its contract;
    /// 2. `sctx` and `sctx.spec` must be valid and outlive the returned iterator.
    /// 3. `status` must be null or point to a valid [`QueryError`]. Only the disk
    ///    branch writes it; memory mode leaves it untouched.
    /// 4. `lookup` must resolve `self`, checked by a `debug_assert!` in
    ///    [`get_reader`](Self::get_reader).
    /// 5. The caller owns the returned iterator and must free it.
    pub unsafe fn open_reader(
        &self,
        sctx: NonNull<RedisSearchCtx>,
        tag: &[u8],
        weight: f64,
        field_index: t_fieldIndex,
        lookup: TrieLookup,
        status: *mut QueryError,
    ) -> Option<NonNull<QueryIterator>> {
        match &self.mode {
            TagIndexMode::Disk {
                disk_index_spec, ..
            } => {
                // Postings live on disk: build the reader through the disk API,
                // keyed by the tag string.
                //
                // SAFETY: `RSToken` is a plain-old-data `#[repr(C)]` struct
                // whose all-zero bit pattern is a valid, unexpanded token; only
                // `str`/`len` are then set.
                let mut tok: RSToken = unsafe { std::mem::zeroed() };
                tok.str_ = tag.as_ptr().cast::<c_char>().cast_mut();
                tok.len = tag.len();
                // SAFETY: `disk_index_spec` is a valid `RedisSearchDiskIndexSpec`
                // (invariant from `new_on_disk`); `sctx` is valid for the call
                // (contract 2); `tok` borrows `tag` for the duration of the
                // call; `status` is null or valid (contract 3). The disk backend
                // owns the returned iterator, which C frees through its `Free`
                // callback.
                let it = unsafe {
                    ffi::SearchDisk_NewTagIterator(
                        disk_index_spec.as_ptr(),
                        sctx.as_ptr().cast_const(),
                        &tok,
                        field_index,
                        weight,
                        status,
                    )
                };
                NonNull::new(it)
            }
            TagIndexMode::InMemory { values } => {
                let a = values.find(tag);
                match a {
                    None => None,
                    Some(ii) if ii.unique_docs() == 0 => None,
                    Some(ii) => Some(
                        // SAFETY: `get_reader`'s contracts is this function's
                        // contracts
                        unsafe { self.get_reader(sctx, ii, tag, weight, field_index, lookup) },
                    ),
                }
            }
        }
    }

    /// Build the [`QueryIterator`] reading `ii`'s postings for `tag`.
    ///
    /// # Safety
    ///
    /// 1. `self` must outlive the returned iterator, and may be mutated while it
    ///    is alive only under the revalidation protocol described on
    ///    [`query_iterator_for_value`](Self::query_iterator_for_value).
    /// 2. `ii` must be the inverted index this index currently stores for `tag`.
    /// 3. `sctx` and `sctx.spec` must be valid and outlive the returned iterator.
    /// 4. `lookup` must resolve `self`.
    unsafe fn get_reader(
        &self,
        sctx: NonNull<RedisSearchCtx>,
        ii: &InvertedIndex<DocIdsOnly>,
        tag: &[u8],
        weight: f64,
        field_index: t_fieldIndex,
        lookup: TrieLookup,
    ) -> NonNull<QueryIterator> {
        let term = RSQueryTerm::new_bytes(tag, 0, 0);

        let filter_ctx = FieldFilterContext {
            field: FieldMaskOrIndex::Index(field_index),
            predicate: FieldExpirationPredicate::Default,
        };
        let reader = ii.reader();
        // SAFETY: contract 3 guarantees sctx/spec validity for the checker's
        // lifetime.
        let checker = unsafe { FieldExpirationChecker::new(sctx, filter_ctx, reader.flags()) };

        // Contract 4.
        debug_assert!(
            std::ptr::eq(
                lookup.index_ptr().as_ptr().cast_const(),
                std::ptr::from_ref(self)
            ),
            "lookup must resolve the index this reader reads from"
        );

        // SAFETY: contract 2 makes `reader` the current reader for `tag`,
        // contract 3 guarantees `sctx` and `sctx.spec` are valid, and contract 4
        // is `Tag::new`'s own requirement on `lookup`.
        let iterator = unsafe { Tag::new(reader, sctx, lookup, term, weight, checker) };
        NonNull::new(RQEIteratorWrapper::boxed_new(iterator))
            .expect("RQEIteratorWrapper::boxed_new never returns NULL pointer")
    }

    /// Bytes the index's tries occupy, as reported by `FT.INFO`: the values trie
    /// plus the suffix trie, in both modes.
    pub const fn get_overhead(&self) -> usize {
        let mut size = match &self.mode {
            TagIndexMode::InMemory { values } => values.mem_usage(),
            TagIndexMode::Disk { values, .. } => values.mem_usage(),
        };
        if let Some(suffix) = &self.suffix {
            size += suffix.mem_usage();
        }

        size
    }

    /// Expand a [`SuffixQuery`] against the [suffix index](TagSuffixIndex) into
    /// the concrete tag terms it matches, yielded lazily.
    ///
    /// Each yielded slice is the matched term including its trailing NUL, so its
    /// pointer is directly usable as a C `char*`.
    ///
    /// The two walking forms are bounded by `timeout`: the trie iterator stops at
    /// the deadline (see [`expansion_deadline`]), so the caller keeps the matches
    /// found so far.
    ///
    /// # Panics
    /// Panics if this index was created without `WITHSUFFIXTRIE`.
    pub fn suffix_expand<'a>(
        &'a self,
        query: SuffixQuery<'a>,
        timeout: Option<timespec>,
    ) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
        let suffix = self.suffix.as_ref().expect("suffix trie must exist");
        let deadline = expansion_deadline(timeout);

        // Captures nothing, so it is `Copy` and can be moved into every branch's
        // `flat_map` closure.
        let materialize = |p: suffix::TermPtr| {
            // SAFETY: `p` is a live `TermPtr` owned by this suffix trie (built
            // from `OwnedTerm::borrowed`), pointing to `alloc_size` initialized
            // bytes borrowed for the lifetime of `&self`.
            let len = unsafe { p.alloc_size() };
            // SAFETY: as above — `suffix` stores valid pointers.
            unsafe { std::slice::from_raw_parts(p.as_ptr(), len) }
        };

        match query {
            SuffixQuery::Suffix(tag) => Box::new(
                // Exact node lookup, so nothing to bound: a single node yields all
                // its members.
                suffix
                    .find(tag)
                    .into_iter()
                    .flat_map(move |data| data.members().map(materialize)),
            ),
            SuffixQuery::Contains(tag) => {
                let mut entries = suffix.prefixed_iter(tag);
                entries.set_timeout(deadline);
                Box::new(entries.flat_map(move |(_key, data)| data.members().map(materialize)))
            }
            SuffixQuery::Wildcard {
                pattern,
                max_prefix_expansions,
            } => {
                let full = WildcardPattern::parse(pattern.full);
                let mut entries = suffix.wildcard_iter(WildcardPattern::parse(&pattern.sub));
                entries.set_timeout(deadline);
                Box::new(
                    entries
                        .flat_map(move |(_key, data)| data.members().map(materialize))
                        // The anchor only narrows the walk, so re-check the whole
                        // pattern against the term itself, terminator excluded.
                        .filter(move |with_nul| {
                            full.matches(&with_nul[..with_nul.len() - 1]) == MatchOutcome::Match
                        })
                        // Cap the *matched* terms, overshooting by one as
                        // documented on the variant (`saturating_add` guards the
                        // no-cap sentinel).
                        .take((max_prefix_expansions as usize).saturating_add(1)),
                )
            }
        }
    }
}

// Handles tests reach the index internals through, kept apart from the production
// methods above and gated behind the `test-utils` feature so they stay out of the
// public API in release builds.
#[cfg(feature = "test-utils")]
impl TagIndex {
    /// Get the inverted index holding the postings for `tag`, if the tag is
    /// currently indexed.
    pub fn find_value(&self, tag: &[u8]) -> Option<&InvertedIndex<DocIdsOnly>> {
        let TagIndexMode::InMemory { values } = &self.mode else {
            unimplemented!()
        };

        values.find(tag).map(Box::as_ref)
    }

    /// Get a mutable reference to the inverted index holding the postings for
    /// `tag`, if the tag is currently indexed.
    pub fn find_value_mut(&mut self, tag: &[u8]) -> Option<&mut InvertedIndex<DocIdsOnly>> {
        let TagIndexMode::InMemory { values } = &mut self.mode else {
            unimplemented!()
        };

        values.find_mut(tag).map(Box::as_mut)
    }

    /// Get the inverted index for `tag`, registering a new empty one when the
    /// tag is not indexed yet and `create_if_missing` is set.
    pub fn open_index(
        &mut self,
        tag: &[u8],
        create_if_missing: bool,
    ) -> Option<&InvertedIndex<DocIdsOnly>> {
        let TagIndexMode::InMemory { values } = &mut self.mode else {
            unimplemented!()
        };

        if values.find(tag).is_none() {
            if !create_if_missing {
                return None;
            }
            values.insert(
                tag,
                Box::new(InvertedIndex::<DocIdsOnly>::new(
                    IndexFlags_Index_DocIdsOnly,
                )),
            );
        }

        values.find(tag).map(Box::as_ref)
    }

    /// Handle over [`remove_tag_value`](Self::remove_tag_value): lets tests
    /// simulate the garbage collector dropping every document for `tag`. The
    /// production path is [`gc`](Self::gc).
    pub fn delete_tag_value(&mut self, tag: &[u8]) {
        self.remove_tag_value(tag);
    }
}

/// A wildcard pattern prepared for a suffix-trie lookup: the most selective
/// literal token, expanded into the anchor sub-pattern used to walk the trie.
///
/// Holds the bytes the expansion of a [`SuffixQuery::Wildcard`] borrows, so they
/// outlive the call: the anchor `sub` (owned, since it may gain a trailing `*`)
/// and the original `full` pattern re-checked against each candidate.
#[derive(Debug)]
pub struct SuffixWildcardPattern<'p> {
    /// The whole original pattern, used to fully re-check each candidate term.
    full: &'p [u8],
    /// Anchor sub-pattern walked against the suffix trie: the chosen token
    /// bytes, plus a trailing `*` when the token is immediately followed by `*`
    /// in the original pattern.
    sub: Vec<u8>,
}

/// The pattern has no literal token usable as a suffix-trie anchor (e.g. it is
/// all `*`/`?`, or empty). The caller must fall back to a brute-force scan of the
/// whole tag trie.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NoAnchorToken;

impl<'p> SuffixWildcardPattern<'p> {
    /// Prepare `pattern` for a suffix-trie lookup, choosing the most selective
    /// literal token as the anchor. Returns [`NoAnchorToken`] when there is no
    /// usable literal token.
    pub fn new(pattern: &'p [u8]) -> Result<Self, NoAnchorToken> {
        // Pick the most selective literal token to anchor the trie lookup.
        let (tokenidx, tokenlen) = choose_token(pattern).ok_or(NoAnchorToken)?;

        // A `*` right after the token means we prefix-expand it.
        let has_star = pattern.get(tokenidx + tokenlen) == Some(&b'*');

        // Build the anchor sub-pattern used to walk the suffix trie. The keys
        // are the NUL-free suffixes (see `TagSuffixIndex::add`), so:
        // - prefix case (`token*`): keep the trailing `*` so it matches every
        //   suffix key starting with the token;
        // - exact case (`token`): match the token against the full suffix key.
        let mut sub = pattern[tokenidx..tokenidx + tokenlen].to_vec();
        if has_star {
            sub.push(b'*');
        }

        Ok(Self { full: pattern, sub })
    }

    /// The anchor sub-pattern [`TagIndex::suffix_expand`] walks the suffix trie
    /// with, as chosen by [`choose_token`].
    ///
    /// Which token wins only changes how much of the trie is visited, never the
    /// matched terms — every candidate is re-checked against [`Self::full`] — so
    /// nothing in production can observe the choice. Exposed so the tests can pin
    /// it.
    #[cfg(test)]
    pub(crate) fn anchor(&self) -> &[u8] {
        &self.sub
    }
}

/// Which query form [`TagIndex::suffix_expand`] expands, carrying the bytes it
/// expands. Tag bytes are NUL-free, as everywhere else here.
#[derive(Debug, Clone, Copy)]
pub enum SuffixQuery<'a> {
    /// Suffix query `*foo`: exact lookup of the suffix-trie node `foo`, returning
    /// every term that node belongs to.
    Suffix(&'a [u8]),
    /// Contains query `*foo*`: prefix-iterate every suffix-trie node whose key
    /// starts with `foo`, unioning the terms they belong to.
    Contains(&'a [u8]),
    /// Wildcard query (`*` and `?` metacharacters): walk the suffix trie with the
    /// pattern's anchor sub-pattern, then re-check the whole pattern against each
    /// candidate term.
    Wildcard {
        /// The prepared pattern, owning the bytes the expansion borrows.
        ///
        /// A pattern with no usable anchor token (e.g. `*`, `???`) is rejected up
        /// front by [`SuffixWildcardPattern::new`], so an expansion always has a
        /// valid anchor and an empty result means the anchor matched no term.
        pattern: &'a SuffixWildcardPattern<'a>,
        /// Cap on the *matched* terms, overshot by one: the count is checked
        /// before each match is yielded. [`u64::MAX`] is the no-cap sentinel.
        ///
        /// Only this form is capped during expansion; the other two leave it to
        /// their caller, which is how C splits it too
        /// (`TagIndex_GetSuffixWildcardMatches` takes `maxPrefixExpansions`,
        /// `TagIndex_GetSuffixMatches` does not).
        max_prefix_expansions: u64,
    },
}

fn expansion_deadline(timeout: Option<timespec>) -> Option<Instant> {
    let remaining = timeout.and_then(duration_from_redis_timespec)?;
    Some(Instant::now() + remaining)
}

/// Penalty applied when an anchor token is immediately followed by `*`:
/// iterating all of a node's children is expensive.
const SUFFIX_STARRED_ANCHOR_PENALTY: i32 = 5;

/// Split `pattern` on `*` into literal tokens and return the `(offset, len)` of
/// the most selective one, or `None` when there is no usable literal token
/// (e.g. the pattern is all `*`/`?`).
///
/// The score favors longer tokens and tokens later in the pattern, penalizes a
/// trailing `*` and every `?` inside the token; ties resolve to the later token.
fn choose_token(pattern: &[u8]) -> Option<(usize, usize)> {
    let len = pattern.len();

    // Collect the literal tokens between runs of `*`.
    let mut tokens: Vec<(usize, usize)> = Vec::new();
    let mut i = 0;
    while i < len {
        if pattern[i] != b'*' {
            let start = i;
            while i < len && pattern[i] != b'*' {
                i += 1;
            }
            tokens.push((start, i - start));
        }
        while i < len && pattern[i] == b'*' {
            i += 1;
        }
    }

    let mut best_score = i32::MIN;
    let mut best = None;
    for (idx, &(start, tlen)) in tokens.iter().enumerate() {
        // 1. longer tokens likely yield fewer results;
        // 2. tokens later in the pattern are likely more relevant.
        let mut score = tlen as i32 + idx as i32;

        // A trailing `*` forces iterating all children of the node.
        if pattern.get(start + tlen) == Some(&b'*') {
            score -= SUFFIX_STARRED_ANCHOR_PENALTY;
        }

        // Each `?` inside the token adds heavy branching.
        for &b in &pattern[start..start + tlen] {
            if b == b'?' {
                score -= 1;
            }
        }

        // `>=` keeps the later token on ties.
        if score >= best_score {
            best_score = score;
            best = Some((start, tlen));
        }
    }

    best
}

/// [`TagLookup`] over this crate's typed values trie, used by the iterators
/// returned from [`TagIndex::query_iterator_for_value`] to detect during
/// revalidation that the garbage collector removed or replaced a tag's
/// inverted index.
pub struct TrieLookup(NonNull<TagIndex>);

impl TrieLookup {
    /// Create a lookup over the given [`TagIndex`].
    ///
    /// # Safety
    ///
    /// 1. `idx` must be the pointer the index's owner holds — the one the
    ///    collector is handed to reach the same index — and **not** a pointer
    ///    derived from a `&TagIndex` or a `&mut TagIndex`.
    ///
    ///    The collector takes `&mut *idx` (`TagIndex2_GC`), and
    ///    [`find`](TagLookup::find) reads back through this pointer afterwards.
    ///    Taking that `&mut` invalidates every pointer derived above `idx`, so a
    ///    reborrowed one would be dead by the time `find` runs — undefined
    ///    behaviour under both Stacked and Tree Borrows. Passing `idx` itself
    ///    keeps the alternation raw → `&mut` → raw, which is permitted.
    ///
    ///    In production that pointer is the one the owning field spec holds in
    ///    `tagOpts.tagIndex`; the FFI entry points mint the lookup from the very
    ///    argument C hands them, and tests mint it from their `Box::into_raw`.
    ///
    /// 2. `idx` must stay valid for this lookup and any iterator holding it. The
    ///    index is freed only by `FieldSpec_Cleanup`, so holding the spec's read
    ///    lock for the iterator's lifetime is sufficient.
    ///
    /// 3. The index may only be mutated while the lookup is alive under the
    ///    standard revalidation protocol — i.e. between
    ///    [`revalidate`](rqe_iterators::RQEIterator::revalidate) calls, never
    ///    concurrently with a read — mirroring the contract of
    ///    [`TagIndex::query_iterator_for_value`].
    pub const unsafe fn new(idx: NonNull<TagIndex>) -> Self {
        Self(idx)
    }

    /// The index this lookup resolves tags in, for the identity check in
    /// [`TagIndex::get_reader`]. Returns the pointer without dereferencing it, so
    /// callers may compare it against an index reached another way.
    pub(crate) const fn index_ptr(&self) -> NonNull<TagIndex> {
        self.0
    }
}

impl TagLookup<DocIdsOnly> for TrieLookup {
    fn find(&self, tag: &[u8]) -> Option<&InvertedIndex<DocIdsOnly>> {
        // SAFETY: contracts 1 and 2 of `TrieLookup::new` — the pointer carries the
        // owner's provenance, so the collector's `&mut` did not revoke it, and the
        // index outlives the iterator holding this lookup. The reference dies with
        // this call, so nothing is outstanding across the next mutation.
        let tag_index = unsafe { self.0.as_ref() };

        let TagIndexMode::InMemory { values } = &tag_index.mode else {
            unimplemented!()
        };

        values.find(tag).map(Box::as_ref)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rqe_iterators::{NoOpChecker, RQEIterator, RQEValidateStatus};
    use rqe_iterators_test_utils::MockContext;

    /// The iterator built by [`TagIndex::query_iterator_for_value`] must abort
    /// revalidation once the garbage collector removed the tag's postings —
    /// [`TrieLookup`] no longer resolves the tag, so the reader is stale.
    ///
    /// The index is reached through raw pointers because the GC mutates it while
    /// the iterator is parked. That is the interleaving [`TrieLookup::new`]'s
    /// provenance contract exists for.
    #[test]
    fn revalidate_aborts_after_tag_removed() {
        let mock = MockContext::new(3, 3);
        let mut idx = TagIndex::new_in_memory(1, false);
        let tags: &[&[u8]] = &[b"team"];
        for doc_id in 1..=3 {
            // SAFETY: memory mode, so neither disk-mode condition of `index` applies.
            unsafe { idx.index(std::ptr::null(), std::ptr::null(), tags, doc_id, false) };
        }
        // Heap-allocate the index and go through raw pointers so it can be
        // mutated while the iterator holds a lookup back-pointer, as the query
        // layer does across GC cycles.
        let idx = Box::into_raw(Box::new(idx));

        // SAFETY: `idx` was just allocated and is not mutated while `ii` is in use.
        let ii = unsafe { &*idx }
            .find_value(b"team")
            .expect("tag was indexed");
        let term = RSQueryTerm::new_bytes(b"team", 0, 0);
        // SAFETY: `mock` provides a valid sctx/spec and `idx` stays valid for
        // the iterator's lifetime; it is only mutated between revalidations.
        let mut it = unsafe {
            Tag::new(
                ii.reader(),
                mock.sctx(),
                TrieLookup(NonNull::new(idx).expect("just allocated")),
                term,
                1.0,
                NoOpChecker,
            )
        };

        let status = it.revalidate(&mock.spec_read()).expect("revalidate failed");
        assert_eq!(status, RQEValidateStatus::Ok);

        // Simulate the garbage collector removing the tag's postings entirely.
        // SAFETY: the iterator is not touched during the mutation, per the
        // revalidation protocol.
        unsafe { (*idx).delete_tag_value(b"team") };

        let status = it.revalidate(&mock.spec_read()).expect("revalidate failed");
        assert_eq!(status, RQEValidateStatus::Aborted);

        drop(it);
        // SAFETY: allocated with `Box::into_raw` above; the iterator borrowing
        // into it has been dropped.
        drop(unsafe { Box::from_raw(idx) });
    }
}

// These tests exercise only the suffix-trie wildcard logic — no FFI-backed
// context is involved, so they are safe to run under miri as well.
#[cfg(test)]
mod suffix_wildcard_tests {
    use super::*;

    const NO_CAP: u64 = u64::MAX;

    /// Build an in-memory index with a suffix trie and commit `tags`.
    fn indexed(tags: &[&[u8]]) -> TagIndex {
        let mut idx = TagIndex::new_in_memory(1, true);
        // SAFETY: every tag these tests pass is a NUL-free literal, which is all
        // `commit` requires.
        unsafe { idx.commit(tags) };
        idx
    }

    /// Expand a wildcard query and return the matched terms as owned byte vectors
    /// with the trailing NUL stripped, sorted for order-independent comparison.
    fn matches(idx: &TagIndex, pattern: &[u8], cap: u64) -> Option<Vec<Vec<u8>>> {
        // A pattern with no usable anchor token is a `SuffixWildcardPattern::new`
        // error, surfaced here as `None`.
        let pattern = SuffixWildcardPattern::new(pattern).ok()?;
        let mut out: Vec<Vec<u8>> = idx
            .suffix_expand(
                SuffixQuery::Wildcard {
                    pattern: &pattern,
                    max_prefix_expansions: cap,
                },
                None,
            )
            .map(|t| t[..t.len() - 1].to_vec()) // drop trailing NUL
            .collect();
        out.sort();
        Some(out)
    }

    /// Which literal token anchors the suffix-trie walk, for each rule of
    /// [`choose_token`]'s selectivity scoring. A wrong choice is invisible in the
    /// matched terms — only in how much of the trie is walked — so these are the
    /// only assertions that can catch a regression in it.
    ///
    /// The score is `len + token_index`, minus
    /// [`SUFFIX_STARRED_ANCHOR_PENALTY`] when the token is immediately followed
    /// by `*`, minus one per `?` inside it; ties go to the later token.
    #[test]
    fn anchor_token_follows_the_selectivity_scoring() {
        for (pattern, expected, why) in [
            (&b"hello"[..], &b"hello"[..], "the only token"),
            (b"*llo", b"llo", "a leading `*` is not part of the token"),
            (
                b"he*",
                b"he*",
                "a token followed by `*` keeps it, to prefix-expand the trie",
            ),
            (
                b"abcdef*ghi",
                b"ghi",
                "the starred penalty (6 - 5 = 1) drops `abcdef` below `ghi` (3 + 1 = 4)",
            ),
            (
                b"abcdefghij*xyz",
                b"abcdefghij*",
                "a long enough token still wins the penalty (10 - 5 = 5 > 3 + 1)",
            ),
            (
                b"ab??????ij*xyz",
                b"xyz",
                "the same length, but six `?` take it to -1, below `xyz`",
            ),
            (
                b"abcdefg*z",
                b"z",
                "a tie (7 - 5 = 2 == 1 + 1) resolves to the later token",
            ),
        ] {
            let prepared =
                SuffixWildcardPattern::new(pattern).expect("pattern has a literal token");
            assert_eq!(
                prepared.anchor(),
                expected,
                "anchor for {:?}: {why}",
                String::from_utf8_lossy(pattern)
            );
        }
    }

    #[test]
    fn no_usable_token_returns_none() {
        let idx = indexed(&[b"hello"]);
        // Patterns made only of `*` (or empty) have no literal anchor, so the
        // caller must brute-force instead.
        assert_eq!(matches(&idx, b"*", NO_CAP), None);
        assert_eq!(matches(&idx, b"**", NO_CAP), None);
        assert_eq!(matches(&idx, b"", NO_CAP), None);
    }

    #[test]
    fn valid_token_no_match_returns_empty() {
        let idx = indexed(&[b"hello", b"world"]);
        assert_eq!(matches(&idx, b"*zzz", NO_CAP), Some(vec![]));
    }

    #[test]
    fn suffix_match() {
        let idx = indexed(&[b"hello", b"jello", b"world"]);
        assert_eq!(
            matches(&idx, b"*llo", NO_CAP),
            Some(vec![b"hello".to_vec(), b"jello".to_vec()])
        );
    }

    #[test]
    fn prefix_match_via_wildcard() {
        let idx = indexed(&[b"hello", b"hero", b"her", b"world"]);
        // `he*` must include `her` and `hero` (matched through their own full
        // keys, i.e. via `SuffixData::full_term`) as well as `hello`.
        assert_eq!(
            matches(&idx, b"he*", NO_CAP),
            Some(vec![b"hello".to_vec(), b"her".to_vec(), b"hero".to_vec()])
        );
    }

    #[test]
    fn contains_match() {
        let idx = indexed(&[b"abcXYZ", b"XYZabc", b"nomatch"]);
        assert_eq!(
            matches(&idx, b"*abc*", NO_CAP),
            Some(vec![b"XYZabc".to_vec(), b"abcXYZ".to_vec()])
        );
    }

    #[test]
    fn question_mark_matches_single_char() {
        let idx = indexed(&[b"cat", b"cot", b"coat"]);
        // `c?t` matches only the exactly-3-char terms `c_t`, not `coat`.
        assert_eq!(
            matches(&idx, b"c?t", NO_CAP),
            Some(vec![b"cat".to_vec(), b"cot".to_vec()])
        );
    }

    #[test]
    fn max_prefix_expansions_caps_results() {
        let idx = indexed(&[b"aa", b"ba", b"ca", b"da"]);
        // The cap is checked before each match is collected, so a cap of N yields
        // N + 1 entries.
        let pattern = SuffixWildcardPattern::new(b"*a").expect("valid token");
        let got = idx
            .suffix_expand(
                SuffixQuery::Wildcard {
                    pattern: &pattern,
                    max_prefix_expansions: 1,
                },
                None,
            )
            .count();
        assert_eq!(got, 2);
    }
}

/// Timeout handling of the suffix/wildcard expansion. On timeout both
/// functions stop and return the matches gathered so far (partial results).
///
/// The tests that actually reach the deadline are `#[cfg_attr(miri, ignore)]`:
/// probing it calls `clock_gettime(CLOCK_MONOTONIC_RAW)`, which miri does not
/// implement. The no-timeout tests stay in miri's reach.
#[cfg(test)]
mod expansion_timeout_tests {
    use super::*;

    const NO_CAP: u64 = u64::MAX;
    /// Comfortably larger than the trie iterators' clock-probe granularity (100
    /// traversal steps, a `trie_rs` internal), so a zero-budget deadline is
    /// guaranteed to trigger before the corpus is exhausted while still leaving
    /// many entries unprocessed.
    const CORPUS: usize = 300;

    /// A deadline that has already elapsed. Any `CLOCK_MONOTONIC_RAW` value one
    /// second after boot is in the past on a running system, so
    /// `duration_from_redis_timespec` maps it to a zero remaining budget.
    fn expired() -> timespec {
        timespec {
            tv_sec: 1,
            tv_nsec: 0,
        }
    }

    /// Build a `WITHSUFFIXTRIE` index over `CORPUS` distinct terms that all
    /// share the literal prefix `he`. `he*` (wildcard) visits one full-term
    /// suffix entry per term, and the contains-expansion `e` visits one
    /// proper-suffix entry per term — both more than the check granularity.
    fn big_index() -> (TagIndex, usize) {
        let owned: Vec<Vec<u8>> = (0..CORPUS)
            .map(|i| format!("he{i:05}").into_bytes())
            .collect();
        let tags: Vec<&[u8]> = owned.iter().map(|t| t.as_slice()).collect();
        let mut idx = TagIndex::new_in_memory(1, true);
        // SAFETY: the generated tags are ASCII, hence NUL-free.
        unsafe { idx.commit(&tags) };
        (idx, owned.len())
    }

    /// An uncapped wildcard query over `pattern`: these tests bound the expansion
    /// by the deadline, never by the expansion cap.
    fn wildcard<'p>(pattern: &'p SuffixWildcardPattern<'p>) -> SuffixQuery<'p> {
        SuffixQuery::Wildcard {
            pattern,
            max_prefix_expansions: NO_CAP,
        }
    }

    #[test]
    fn wildcard_no_timeout_returns_all() {
        let (idx, total) = big_index();
        let pattern = SuffixWildcardPattern::new(b"he*").expect("valid token");
        let got = idx.suffix_expand(wildcard(&pattern), None).count();
        assert_eq!(got, total, "every `he*` term must be expanded");
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn wildcard_times_out_with_partial_results() {
        let (idx, total) = big_index();
        // An already-elapsed deadline must not panic, and must yield a strict,
        // non-empty subset.
        let pattern = SuffixWildcardPattern::new(b"he*").expect("valid token");
        let got = idx
            .suffix_expand(wildcard(&pattern), Some(expired()))
            .count();
        assert!(got > 0, "the first granularity-1 entries are collected");
        assert!(got < total, "timeout must stop before the full expansion");
    }

    // `SuffixQuery::Contains` prefix-iterates the suffix trie, probing the
    // deadline once per entry. Each `heNNNNN` term contributes exactly one
    // suffix entry starting with `e` (`eNNNNN`), so `e` visits one entry per
    // term, above the check granularity.
    #[test]
    fn contains_no_timeout_returns_all() {
        let (idx, total) = big_index();
        let got = idx.suffix_expand(SuffixQuery::Contains(b"e"), None).count();
        assert_eq!(got, total, "every term containing `e` must be expanded");
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn contains_times_out_with_partial_results() {
        let (idx, total) = big_index();
        let got = idx
            .suffix_expand(SuffixQuery::Contains(b"e"), Some(expired()))
            .count();
        assert!(got > 0, "the first granularity-1 entries are collected");
        assert!(got < total, "timeout must stop before the full expansion");
    }

    #[test]
    fn expansion_deadline_opts_out_when_no_timeout() {
        // `None` is the `skipTimeoutChecks` path (set at the FFI boundary): it
        // must leave the walk unbounded. The Redis `time_t::MAX` "no timeout"
        // sentinel maps to `None` too, but that mapping lives in
        // `duration_from_redis_timespec` and is covered by its own crate tests.
        assert!(expansion_deadline(None).is_none());
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn expansion_deadline_of_an_elapsed_timeout_is_already_past() {
        let deadline = expansion_deadline(Some(expired())).expect("a real deadline");
        assert!(
            deadline <= Instant::now(),
            "an elapsed deadline must stop the walk on its first clock probe"
        );
    }
}

/// Deltas produced by writing a document's tag postings, to fold into the
/// spec statistics.
#[derive(Debug, Clone, Copy, Default)]
pub struct WritePostingsDelta {
    /// Bytes by which the inverted-index memory grew.
    pub size_delta: usize,
    /// Number of new (tag, doc) postings — counted only when the write added a
    /// document not already present in the tag's posting list, so a value
    /// repeated within a multi-value document is counted once.
    pub num_records: u32,
    /// Number of inverted-index blocks allocated.
    pub blocks_added: u32,
}

fn write_postings(
    values: &mut TrieMap<Box<InvertedIndex<DocIdsOnly>>>,
    tags: &[&[u8]],
    doc_id: DocId,
    has_field_expiration: bool,
) -> WritePostingsDelta {
    let mut delta = WritePostingsDelta::default();

    let mut record = RSIndexResult::build_virt().doc_id(doc_id).build();
    // The builder always clears this, so set it explicitly: `add_record` reads it
    // off the record to write the posting's expiration bit.
    record.has_field_expiration = has_field_expiration;
    for tag in tags {
        values.insert_with(tag, |slot| {
            let mut ii = slot.unwrap_or_else(|| {
                let ii = InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly);

                delta.size_delta += ii.memory_usage();

                Box::new(ii)
            });

            let docs_before = ii.unique_docs();
            let outcome = ii.add_record(&record).unwrap();
            // Count a record only when a new unique document was appended; a
            // duplicate doc id (e.g. a tag repeated in a multi-value field) is
            // skipped by `add_record` and must not be counted.
            if ii.unique_docs() > docs_before {
                delta.num_records += 1;
            }
            delta.blocks_added += outcome.blocks_added;
            delta.size_delta += outcome.mem_growth as usize;

            ii
        });
    }

    delta
}
