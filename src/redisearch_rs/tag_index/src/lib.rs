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
//! It supports two storage modes, chosen at construction by whether a
//! [`RedisSearchDiskIndexSpec`] is supplied (see [`TagIndex::new`]):
//!
//! - **Memory mode** keeps the per-tag posting lists (document ids) inline in
//!   the values trie.
//! - **Disk mode** (bigredis/Flex) keeps the postings on disk behind the
//!   `SearchDisk_*` API; the values trie holds only tag *presence* sentinels.
//!   Writes stage onto a disk write batch ([`TagIndex::index`]), reads open a
//!   disk iterator ([`TagIndex::open_reader`]), and query expansion still walks
//!   the presence trie for matching keys before opening each reader by string.
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

use ffi::{
    IndexFlags_Index_DocIdsOnly, QueryError, QueryIterator, RSToken, RedisModuleCtx,
    RedisSearchCtx, RedisSearchDiskIndexSpec, SearchDiskWriteBatchHandle, t_fieldIndex, timespec,
};
use field::{FieldExpirationPredicate, FieldFilterContext, FieldMaskOrIndex};
use index_result::RSIndexResult;
use inverted_index::{
    DocId, GcApplyInfo, GcScanDelta, IndexReader, InvertedIndex, doc_ids_only::DocIdsOnly,
};
use query_term::RSQueryTerm;
use rqe_iterators::{
    FieldExpirationChecker,
    interop::RQEIteratorWrapper,
    inverted_index::{Tag, TagLookup},
    utils::{
        AnyTimeoutContext, DeadlineTimeoutChecker, NoTimeoutChecker, TimeoutContext,
        duration_from_redis_timespec,
    },
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
        /// tag value → document ids.
        ///
        /// The posting list is boxed so it keeps a stable heap address even as
        /// the trie restructures on insert/remove. C holds these pointers
        /// across mutations — e.g. the fork GC captures a tag's inverted index
        /// while scanning and later re-checks pointer identity when applying
        /// the delta (see [`TagIndex::gc`]) — mirroring the pre-Rust C
        /// `TagIndex` which stored heap `InvertedIndex*`.
        values: TrieMap<Box<InvertedIndex<DocIdsOnly>>>,
    },
    /// If the postings (doc_ids) are kept on disk
    Disk {
        /// tag value → (). It is used only to know whether a tag is there
        values: TrieMap<()>,
        /// Field id
        field_id: t_fieldIndex,
        /// Disk Index spec
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
    /// Create a new, empty index.
    ///
    /// - `id` uniquely identifies this index.
    /// - `disk` selects the storage mode: `None` keeps the postings in
    ///   memory, `Some` stores them on disk and pairs the disk index spec
    ///   with the field index the disk API calls need.
    /// - `with_suffix` enables the [suffix index](TagSuffixIndex)
    ///   (`WITHSUFFIXTRIE`), so suffix (`*foo`) and contains (`*foo*`)
    ///   queries don't have to scan the whole tag trie.
    pub fn new(
        id: u32,
        disk: Option<(NonNull<RedisSearchDiskIndexSpec>, t_fieldIndex)>,
        with_suffix: bool,
    ) -> Self {
        let mode = if let Some((spec, field_id)) = disk {
            TagIndexMode::Disk {
                values: TrieMap::new(),
                field_id,
                disk_index_spec: spec,
            }
        } else {
            TagIndexMode::InMemory {
                values: TrieMap::new(),
            }
        };

        Self {
            unique_id: id,
            suffix: with_suffix.then(TagSuffixIndex::new),
            mode,
        }
    }

    /// Get the id.
    pub const fn id(&self) -> u32 {
        self.unique_id
    }

    /// Returns `true` is suffix search is supported
    pub const fn has_suffix(&self) -> bool {
        self.suffix.is_some()
    }

    /// Returns `true` if the postings are backed by disk (disk/Flex mode).
    ///
    /// Port of the C `TagIndex_HasDiskSpec` (`idx->diskSpec != NULL`).
    pub const fn disk_mode(&self) -> bool {
        matches!(self.mode, TagIndexMode::Disk { .. })
    }

    pub const fn unique_values(&self) -> usize {
        match &self.mode {
            TagIndexMode::InMemory { values } => values.n_unique_keys(),
            TagIndexMode::Disk { values, .. } => values.n_unique_keys(),
        }
    }

    /// Get the inverted index holding the postings for `tag`, if the tag is
    /// currently indexed.
    #[cfg(feature = "test-utils")]
    pub fn find_value(&self, tag: &[u8]) -> Option<&InvertedIndex<DocIdsOnly>> {
        let TagIndexMode::InMemory { values } = &self.mode else {
            unimplemented!()
        };

        values.find(tag).map(Box::as_ref)
    }

    /// Get a mutable reference to the inverted index holding the postings for
    /// `tag`, if the tag is currently indexed.
    ///
    /// Exposed only for tests (no production caller); gated behind the
    /// `test-utils` feature so it stays out of the public API in release builds.
    #[cfg(feature = "test-utils")]
    pub fn find_value_mut(&mut self, tag: &[u8]) -> Option<&mut InvertedIndex<DocIdsOnly>> {
        let TagIndexMode::InMemory { values } = &mut self.mode else {
            unimplemented!()
        };

        values.find_mut(tag).map(Box::as_mut)
    }

    /// Index `doc_id` under each tag in `tags` — phase 1 of a document write.
    ///
    /// Returns the [`WritePostingsDelta`] the caller folds into the spec
    /// statistics (records, memory, blocks), or `None` when a disk-mode write
    /// fails — mirroring the C `TagIndex_Index` returning `false`.
    ///
    /// In memory mode the postings are written inline into the per-tag
    /// inverted index and `ctx`/`batch` are ignored; this always succeeds. In
    /// disk mode the postings are staged onto `batch` (committed later by
    /// `commitDocument`) and the returned delta is zero — the record count is
    /// tallied in [`commit`](Self::commit), matching the C two-phase contract.
    ///
    /// In disk mode the caller must pass the valid disk write context `ctx` and
    /// batch handle `batch` for the ongoing document write, and each tag must
    /// borrow a NUL-terminated C string (as produced at the FFI boundary);
    /// memory mode places no requirement on `ctx`/`batch`.
    pub fn index(
        &mut self,
        ctx: *const RedisModuleCtx,
        batch: *const SearchDiskWriteBatchHandle,
        tags: &[&[u8]],
        doc_id: DocId,
    ) -> Option<WritePostingsDelta> {
        match &mut self.mode {
            TagIndexMode::Disk {
                field_id,
                disk_index_spec,
                ..
            } => {
                // Stage the postings onto `batch`; the trie/suffix/record
                // accounting runs afterwards in `commit`. Port of the C
                // `TagIndex_Index` disk branch (`SearchDisk_IndexTags`).
                if tags.is_empty() {
                    return Some(WritePostingsDelta::default());
                }
                // Each tag borrows a NUL-terminated C string, so its pointer is
                // directly usable as the `const char *` the disk API expects.
                let mut value_ptrs: Vec<*const c_char> =
                    tags.iter().map(|tag| tag.as_ptr().cast()).collect();
                // SAFETY: `disk_index_spec` is a valid `RedisSearchDiskIndexSpec`
                // (invariant from `new`); the caller guarantees `ctx`/`batch` are
                // the valid disk write context/batch; `value_ptrs` addresses
                // `tags.len()` valid NUL-terminated C strings that outlive the
                // call.
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
            TagIndexMode::InMemory { values } => Some(write_postings(values, tags, doc_id)),
        }
    }

    /// Get the inverted index for `tag`, registering a new empty one when the
    /// tag is not indexed yet and `create_if_missing` is set.
    #[cfg(feature = "test-utils")]
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

    /// Apply the per-tag metadata updates after [`TagIndex::index`] — phase 3
    /// of a document write: register the tags in the values trie (disk mode
    /// only — in memory mode the trie already holds the postings) and in the
    /// [suffix index](TagSuffixIndex), when enabled.
    ///
    /// Returns the number of records to fold into the spec statistics: in disk
    /// mode the postings are written to disk during this phase, so the
    /// committed tag values are counted here; in memory mode they were already
    /// counted by [`index`](Self::index), so `0` is returned. Port of the C
    /// `TagIndex_Commit` accounting.
    ///
    /// The tags carry their trailing NUL (the [suffix index](TagSuffixIndex)
    /// keys on NUL-terminated terms); the values trie is keyed NUL-free, like
    /// memory mode and like the tags queries look up.
    pub fn commit(&mut self, tags: &[&[u8]]) -> u32 {
        let disk = self.disk_mode();
        for tag in tags {
            if let TagIndexMode::Disk { values, .. } = &mut self.mode {
                let key = tag.strip_suffix(&[0u8]).unwrap_or(tag);
                values.insert(key, ());
            }
            if let Some(suffix) = &mut self.suffix {
                // SAFETY: `add` requires the term to be NUL-terminated, which
                // holds for the tag buffers C hands to `commit`.
                unsafe {
                    suffix.add(tag);
                }
            }
        }
        if disk { tags.len() as u32 } else { 0 }
    }

    /// Create a [`QueryIterator`] over the documents matching the given tag,
    /// reading from `ii`.
    ///
    /// Port of the memory-mode branch of the C `TagIndex_GetIteratorFromTrieMapValue`:
    /// `ii` is the inverted index already resolved for `tag` (e.g. while
    /// iterating the values trie), so no lookup is performed at construction
    /// time. The tag is still looked up again on every revalidation, to detect
    /// that the garbage collector removed or replaced the inverted index.
    ///
    /// Returns a null pointer when `ii` holds no documents.
    ///
    /// # Safety
    ///
    /// 1. `self` must outlive the returned iterator, and must not be mutated
    ///    while the iterator is in use except under the standard revalidation
    ///    protocol: mutations happen while the query is yielded under the
    ///    write lock, and the iterator's `Revalidate` callback runs before any
    ///    further read.
    /// 2. `ii` must be the inverted index currently stored in this index's
    ///    values trie for `tag`.
    /// 3. `sctx` and `sctx.spec` must be valid and outlive the returned
    ///    iterator.
    /// 4. The caller owns the returned iterator and must free it through its
    ///    `Free` callback (`it->Free(it)`).
    pub unsafe fn query_iterator_for_value(
        &self,
        sctx: NonNull<RedisSearchCtx>,
        tag: &[u8],
        ii: &InvertedIndex<DocIdsOnly>,
        weight: f64,
        field_index: t_fieldIndex,
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

        self.get_reader(sctx, ii, tag, weight, field_index).as_ptr()
    }

    /// Iterate over all `(tag, inverted index)` entries, in lexicographical
    /// order of the tag.
    ///
    /// Port of the memory-mode `TagIndex_IterateValues`.
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
    // API — mirroring master's `tag_index.c`, where the same iterators run
    // over `idx->values` regardless of mode. These mirror the memory-mode
    // methods above but over `TrieMap<()>`, so the FFI hands C a NULL value
    // pointer (the disk NULL sentinel) for each entry.

    /// Disk-mode counterpart of [`iter_values`](Self::iter_values).
    pub(crate) fn disk_iter_values(&self) -> LendingIter<'_, (), VisitAll> {
        let TagIndexMode::Disk { values, .. } = &self.mode else {
            unimplemented!()
        };
        values.lending_iter()
    }

    /// Disk-mode counterpart of
    /// [`prefixed_iter_values`](Self::prefixed_iter_values).
    pub(crate) fn disk_prefixed_iter_values(&self, prefix: &[u8]) -> LendingIter<'_, (), VisitAll> {
        let TagIndexMode::Disk { values, .. } = &self.mode else {
            unimplemented!()
        };
        values.prefixed_lending_iter(prefix)
    }

    /// Disk-mode counterpart of
    /// [`contains_iter_values`](Self::contains_iter_values).
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
    /// Its `bytes_freed`/`block_count_delta` already account for the whole
    /// posting list being dropped when the tag became empty.
    pub fn gc(
        &mut self,
        tag: &[u8],
        value: *const InvertedIndex<DocIdsOnly>,
        delta: GcScanDelta,
    ) -> Option<GcApplyInfo> {
        // Disk indexes never reach the fork-GC tag path: the spec selects
        // `GCPolicy_Disk` (see `spec.c`), so the disk backend owns deletion and
        // `TagIndex2_GC` is not called for them.
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

    /// Remove `tag` (and its postings) from the values trie, mirroring the
    /// garbage collector dropping every document indexed under it.
    ///
    /// Memory mode only: disk indexes use `GCPolicy_Disk` and never reach this
    /// path (see [`gc`](Self::gc)).
    fn remove_tag_value(&mut self, tag: &[u8]) {
        let TagIndexMode::InMemory { values } = &mut self.mode else {
            unreachable!("tag deletion runs only in memory mode; disk uses GCPolicy_Disk");
        };
        values.remove(tag);
    }

    /// Test-only handle over [`remove_tag_value`](Self::remove_tag_value): lets
    /// tests simulate the garbage collector dropping every document for `tag`.
    /// Gated behind the `test-utils` feature so it stays out of the public API in
    /// release builds (the production path is [`gc`](Self::gc)).
    #[cfg(feature = "test-utils")]
    pub fn delete_tag_value(&mut self, tag: &[u8]) {
        self.remove_tag_value(tag);
    }

    /// # Safety
    /// - status needs to be valid for SearchDisk_NewTagIterator function.
    pub unsafe fn open_reader(
        &self,
        sctx: NonNull<RedisSearchCtx>,
        tag: &[u8],
        weight: f64,
        field_index: t_fieldIndex,
        status: *mut QueryError,
    ) -> Option<NonNull<QueryIterator>> {
        match &self.mode {
            TagIndexMode::Disk {
                disk_index_spec, ..
            } => {
                // Postings live on disk: build the reader through the disk API,
                // keyed by the tag string. Port of the C `TagIndex_OpenReader`
                // disk branch. Note the reader uses the caller's `field_index`
                // (not the stored write-time field), matching the C code.
                //
                // SAFETY: `RSToken` is a plain-old-data `#[repr(C)]` struct
                // whose all-zero bit pattern is a valid, unexpanded token; we
                // then set only `str`/`len`, exactly as the C code does.
                let mut tok: RSToken = unsafe { std::mem::zeroed() };
                tok.str_ = tag.as_ptr().cast::<c_char>().cast_mut();
                tok.len = tag.len();
                // SAFETY: `disk_index_spec` is a valid `RedisSearchDiskIndexSpec`
                // (invariant from `new`); `sctx` is valid for the call
                // (contract 3); `tok` borrows `tag` for the duration of the
                // call; `status` may be null. The disk backend owns the
                // returned iterator, which C frees through its `Free` callback.
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
                    Some(ii) => Some(self.get_reader(sctx, ii, tag, weight, field_index)),
                }
            }
        }
    }

    fn get_reader(
        &self,
        sctx: NonNull<RedisSearchCtx>,
        ii: &InvertedIndex<DocIdsOnly>,
        tag: &[u8],
        weight: f64,
        field_index: t_fieldIndex,
    ) -> NonNull<QueryIterator> {
        let term = RSQueryTerm::new_bytes(tag, 0, 0);

        let filter_ctx = FieldFilterContext {
            field: FieldMaskOrIndex::Index(field_index),
            predicate: FieldExpirationPredicate::Default,
        };
        let reader = ii.reader();
        // SAFETY: 3. guarantees sctx/spec validity for the checker's lifetime.
        let checker = unsafe { FieldExpirationChecker::new(sctx, filter_ctx, reader.flags()) };

        // SAFETY: 1. guarantees the index (and thus the trie backing this
        // lookup) outlives the iterator.
        let lookup = TrieLookup(NonNull::from(self));

        // SAFETY: 3. guarantees `sctx` and `sctx.spec` are valid.
        let iterator = unsafe { Tag::new(reader, sctx, lookup, term, weight, checker) };
        NonNull::new(RQEIteratorWrapper::boxed_new(iterator))
            .expect("RQEIteratorWrapper::boxed_new never returns NULL pointer")
    }

    pub const fn get_overhead(&self) -> usize {
        // Port of the C `TagIndex_GetOverhead`: the values trie plus the suffix
        // trie, in both modes. In disk mode the values trie only holds tag
        // presence sentinels (the postings live on disk and are accounted for
        // by the disk backend), but the trie structure itself is still counted.
        let mut size = match &self.mode {
            TagIndexMode::InMemory { values } => values.mem_usage(),
            TagIndexMode::Disk { values, .. } => values.mem_usage(),
        };
        if let Some(suffix) = &self.suffix {
            size += suffix.mem_usage();
        }

        size
    }

    /// Expand a suffix (`*foo`) or contains (`*foo*`) tag query against the
    /// [suffix index](TagSuffixIndex) into the concrete tag terms it matches.
    ///
    /// Port of the memory-mode `GetList_SuffixTrieMap` (`src/suffix.c`). The
    /// `prefix` flag comes straight from the query node (`qn->pfx.prefix`) and
    /// selects the same two branches as C:
    ///
    /// - `!prefix` — suffix query `*foo`: exact lookup of the suffix-trie node
    ///   `foo`, returning every term that node belongs to.
    /// - `prefix` — contains query `*foo*`: prefix-iterate every suffix-trie
    ///   node whose key starts with `foo`, unioning the terms they belong to.
    ///
    /// In both cases the terms come from [`SuffixData::members`] (the term
    /// itself when the key is a full term, plus every term the key is a proper
    /// suffix of) — mirroring C's `data->array`.
    ///
    /// Each yielded slice is the matched term including its trailing NUL, so its
    /// pointer is directly usable as a C `char*`. Terms are yielded lazily; the
    /// two branches produce different iterator types, so the result is boxed.
    pub fn suffix_trie_map<'a>(
        &'a self,
        tag: &[u8],
        prefix: bool,
        timeout: Option<timespec>,
    ) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
        let Some(suffix) = &self.suffix else {
            panic!();
        };

        // Captures nothing, so it is `Copy` and can be moved into either branch's
        // `flat_map` closure.
        let materialize = |p: suffix::TermPtr| {
            // SAFETY: `p` is a live `TermPtr` owned by this suffix trie (built
            // from `OwnedTerm::borrowed`), pointing to `alloc_size` initialized
            // bytes borrowed for the lifetime of `&self`.
            let len = unsafe { p.alloc_size() };
            // SAFETY: as above — `suffix` stores valid pointers.
            unsafe { std::slice::from_raw_parts(p.as_ptr(), len) }
        };

        if !prefix {
            // Suffix query `*foo`: exact node lookup (C: `!prefix` branch). No
            // timeout loop — a single node yields all its members.
            Box::new(
                suffix
                    .find(tag)
                    .into_iter()
                    .flat_map(move |data| data.members().map(materialize)),
            )
        } else {
            // Contains query `*foo*`: prefix-iterate the suffix trie (C: `prefix`
            // branch, `TM_PREFIX_MODE`). Probe the deadline once per trie entry,
            // mirroring C's per `TrieMapIterator_Next` cadence; on timeout stop
            // and keep the partial matches collected so far. `scan` owns
            // `timeout_ctx` so its state persists across entries.
            let mut timeout_ctx = expansion_timeout(timeout);
            Box::new(
                suffix
                    .prefixed_iter(tag)
                    .scan((), move |_, (_key, data)| {
                        timeout_ctx.check_timeout().is_ok().then_some(data)
                    })
                    .flat_map(move |data| data.members().map(materialize)),
            )
        }
    }

    /// Expand the wildcard `pattern` against the [suffix index](TagSuffixIndex)
    /// into the concrete tag terms it matches, yielded lazily.
    ///
    /// Port of the memory-mode `GetList_SuffixTrieMap_Wildcard` (`src/suffix.c`).
    /// The "no usable anchor token" case (e.g. `*`, `???`) — which the C code
    /// signals with `BAD_POINTER` so the caller brute-forces — is decided up
    /// front by [`SuffixWildcardPattern::new`]; this method assumes a valid
    /// anchor. An empty iterator means the anchor token had no matching terms
    /// (C `NULL`).
    ///
    /// Each yielded slice is the matched term including its trailing NUL, so its
    /// pointer is directly usable as a C `char*` (consistent with
    /// [`suffix_trie_map`](Self::suffix_trie_map)).
    ///
    /// # Panics
    /// Panics if this index was created without `WITHSUFFIXTRIE`.
    pub fn suffix_wildcard<'a>(
        &'a self,
        pattern: &'a SuffixWildcardPattern<'a>,
        timeout: Option<timespec>,
        max_prefix_expansions: u64,
    ) -> impl Iterator<Item = &'a [u8]> + 'a {
        let suffix = self.suffix.as_ref().expect("suffix trie must exist");

        let full = WildcardPattern::parse(pattern.full);
        let timeout_ctx = expansion_timeout(timeout);

        suffix
            .wildcard_iter(WildcardPattern::parse(&pattern.sub))
            // Probe the deadline once per trie entry, mirroring C's per
            // `TrieMapIterator_Next` cadence; on timeout stop iterating and keep
            // only the matches collected so far. `scan` owns `timeout_ctx` so its
            // state persists across entries.
            .scan(timeout_ctx, |timeout_ctx, entry| {
                if timeout_ctx.check_timeout().is_err() {
                    None
                } else {
                    Some(entry)
                }
            })
            .flat_map(|(_key, data)| data.members())
            .map(|term| {
                // SAFETY: `term` is a live `TermPtr` owned by this suffix trie,
                // whose lifetime is tied to `&self`.
                let alloc = unsafe { term.alloc_size() };
                // SAFETY: `term` points to `alloc` initialized bytes owned by the
                // suffix trie, borrowed for the lifetime of `&self`.
                unsafe { std::slice::from_raw_parts(term.as_ptr(), alloc) }
            })
            // C matches against `strlen(term)` — exclude the trailing NUL.
            .filter(move |with_nul| {
                full.matches(&with_nul[..with_nul.len() - 1]) == MatchOutcome::Match
            })
            // Expansion cap on *matched* terms. The C loop checks `count > max`
            // before each push, so it collects up to `max + 1` matches; `take`
            // reproduces that (`saturating_add` guards the `u64::MAX` no-cap
            // sentinel).
            .take((max_prefix_expansions as usize).saturating_add(1))
    }
}

/// A wildcard pattern prepared for a suffix-trie lookup: the most selective
/// literal token, expanded into the anchor sub-pattern used to walk the trie.
///
/// Holds the bytes [`TagIndex::suffix_wildcard`]'s returned iterator borrows, so
/// they outlive the call: the anchor `sub` (owned, since it may gain a trailing
/// `*`) and the original `full` pattern re-checked against each candidate.
pub struct SuffixWildcardPattern<'p> {
    /// The whole original pattern, used to fully re-check each candidate term.
    full: &'p [u8],
    /// Anchor sub-pattern walked against the suffix trie: the chosen token
    /// bytes, plus a trailing `*` when the token is immediately followed by `*`
    /// in the original pattern.
    sub: Vec<u8>,
}

/// The pattern has no literal token usable as a suffix-trie anchor (e.g. it is
/// all `*`/`?`, or empty). The caller must fall back to a brute-force scan —
/// this is the C `BAD_POINTER` sentinel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NoAnchorToken;

impl<'p> SuffixWildcardPattern<'p> {
    /// Prepare `pattern` for a suffix-trie lookup, choosing the most selective
    /// literal token as the anchor. Returns [`NoAnchorToken`] when there is no
    /// usable literal token (C `BAD_POINTER`).
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
}

/// How many suffix-trie entries are walked between two consecutive clock
/// probes during expansion. Port of the C `TIMEOUT_COUNTER_LIMIT`
/// (`src/util/timeout.h`).
const TIMEOUT_CHECK_GRANULARITY: u32 = 100;

/// Build the timeout checker used while expanding a suffix/wildcard pattern.
///
/// `timeout` carries the absolute `CLOCK_MONOTONIC_RAW` deadline the C caller
/// computed. It yields [`NoTimeoutChecker`] — so expansion runs to completion — when
/// the caller opted out (`None`, set from `skipTimeoutChecks` at the FFI
/// boundary) or the deadline is the Redis "no timeout" sentinel
/// (`time_t::MAX`); see [`duration_from_redis_timespec`]. Otherwise the
/// remaining budget drives an amortized [`DeadlineTimeoutChecker`] that probes the
/// clock once every [`TIMEOUT_CHECK_GRANULARITY`] entries, matching C's cadence.
/// An already-elapsed deadline maps to a zero budget, i.e. it times out on the
/// first probe.
pub(crate) fn expansion_timeout(timeout: Option<timespec>) -> AnyTimeoutContext {
    match timeout.and_then(duration_from_redis_timespec) {
        Some(remaining) => AnyTimeoutContext::Clock(DeadlineTimeoutChecker::new(
            remaining,
            TIMEOUT_CHECK_GRANULARITY,
        )),
        None => AnyTimeoutContext::NoTimeout(NoTimeoutChecker),
    }
}

/// Penalty applied when an anchor token is immediately followed by `*`:
/// iterating all of a node's children is expensive. Port of the C
/// `SUFFIX_STARRED_ANCHOR_PENALTY` (`src/suffix.h`).
const SUFFIX_STARRED_ANCHOR_PENALTY: i32 = 5;

/// Split `pattern` on `*` into literal tokens and return the `(offset, len)` of
/// the most selective one, or `None` when there is no usable literal token
/// (e.g. the pattern is all `*`/`?`).
///
/// Port of the C `Suffix_ChooseToken` (`src/suffix.c`). The score favors longer
/// tokens and tokens later in the pattern, penalizes a trailing `*` and every
/// `?` inside the token; ties resolve to the later token.
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

        // `>=` keeps the later token on ties, matching C.
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
    /// `idx` must point to a valid [`TagIndex`] that outlives this lookup (and
    /// any iterator holding it). The index may only be mutated while the lookup
    /// is alive under the standard revalidation protocol — i.e. between
    /// [`revalidate`](rqe_iterators::RQEIterator::revalidate) calls, never
    /// concurrently with a read — mirroring the contract of
    /// [`TagIndex::query_iterator_for_value`].
    pub const unsafe fn new(idx: NonNull<TagIndex>) -> Self {
        Self(idx)
    }
}

impl TagLookup<DocIdsOnly> for TrieLookup {
    fn find(&self, tag: &[u8]) -> Option<&InvertedIndex<DocIdsOnly>> {
        // SAFETY: `TagIndex::query_iterator_for_value`'s contract — the index
        // outlives the iterator holding this lookup.
        let tag_index = unsafe { self.0.as_ref() };

        let TagIndexMode::InMemory { values } = &tag_index.mode else {
            unimplemented!()
        };

        values.find(tag).map(Box::as_ref)
    }
}

// Creating the iterator requires FFI-backed contexts and the revalidation
// protocol mutates the index behind a raw pointer, mirroring the C GC flow —
// neither is supported by miri.
#[cfg(all(test, not(miri)))]
mod tests {
    use super::*;
    use rqe_iterators::{NoOpChecker, RQEIterator, RQEValidateStatus};
    use rqe_iterators_test_utils::MockContext;

    /// The iterator built by [`TagIndex::query_iterator_for_value`] must abort
    /// revalidation once the garbage collector removed the tag's postings —
    /// [`TrieLookup`] no longer resolves the tag, so the reader is stale.
    #[test]
    fn revalidate_aborts_after_tag_removed() {
        let mock = MockContext::new(3, 3);
        let mut idx = TagIndex::new(1, None, false);
        let tags: &[&[u8]] = &[b"team"];
        for doc_id in 1..=3 {
            idx.index(std::ptr::null(), std::ptr::null(), tags, doc_id);
        }
        // Heap-allocate the index and go through raw pointers so it can be
        // mutated while the iterator holds a lookup back-pointer, mirroring
        // how C owns the index across GC cycles.
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

        let status = it
            .revalidate(&*mock.spec_read())
            .expect("revalidate failed");
        assert_eq!(status, RQEValidateStatus::Ok);

        // Simulate the garbage collector removing the tag's postings entirely.
        // SAFETY: the iterator is not touched during the mutation, per the
        // revalidation protocol.
        unsafe { (*idx).delete_tag_value(b"team") };

        let status = it
            .revalidate(&*mock.spec_read())
            .expect("revalidate failed");
        assert_eq!(status, RQEValidateStatus::Aborted);

        drop(it);
        // SAFETY: allocated with `Box::into_raw` above; the iterator borrowing
        // into it has been dropped.
        drop(unsafe { Box::from_raw(idx) });
    }
}

// These tests exercise only the suffix-trie wildcard logic (no C interop), so
// they are safe to run under miri as well.
#[cfg(test)]
mod suffix_wildcard_tests {
    use super::*;

    const NO_CAP: u64 = u64::MAX;

    /// Build an in-memory index with a suffix trie and commit `tags`.
    /// `commit` requires NUL-terminated tags for the suffix trie.
    fn indexed(tags: &[&[u8]]) -> TagIndex {
        let mut idx = TagIndex::new(1, None, true);
        idx.commit(tags);
        idx
    }

    /// Run `suffix_wildcard` and return the matched terms as owned byte vectors
    /// with the trailing NUL stripped, sorted for order-independent comparison.
    fn matches(idx: &TagIndex, pattern: &[u8], cap: u64) -> Option<Vec<Vec<u8>>> {
        // `None` (no usable anchor token) is now the `SuffixWildcardPattern::new`
        // error, mirroring the C `BAD_POINTER` signal.
        let pattern = SuffixWildcardPattern::new(pattern).ok()?;
        let mut out: Vec<Vec<u8>> = idx
            .suffix_wildcard(&pattern, None, cap)
            .map(|t| t[..t.len() - 1].to_vec()) // drop trailing NUL
            .collect();
        out.sort();
        Some(out)
    }

    #[test]
    fn no_usable_token_returns_none() {
        let idx = indexed(&[b"hello\0"]);
        // Patterns made only of `*` (or empty) have no literal anchor: this is
        // the C BAD_POINTER / brute-force-fallback signal.
        assert_eq!(matches(&idx, b"*", NO_CAP), None);
        assert_eq!(matches(&idx, b"**", NO_CAP), None);
        assert_eq!(matches(&idx, b"", NO_CAP), None);
    }

    #[test]
    fn valid_token_no_match_returns_empty() {
        let idx = indexed(&[b"hello\0", b"world\0"]);
        assert_eq!(matches(&idx, b"*zzz", NO_CAP), Some(vec![]));
    }

    #[test]
    fn suffix_match() {
        let idx = indexed(&[b"hello\0", b"jello\0", b"world\0"]);
        assert_eq!(
            matches(&idx, b"*llo", NO_CAP),
            Some(vec![b"hello".to_vec(), b"jello".to_vec()])
        );
    }

    #[test]
    fn prefix_match_via_wildcard() {
        let idx = indexed(&[b"hello\0", b"hero\0", b"her\0", b"world\0"]);
        // `he*` must include `her` and `hero` (matched through their own full
        // keys, i.e. via `SuffixData::full_term`) as well as `hello`.
        assert_eq!(
            matches(&idx, b"he*", NO_CAP),
            Some(vec![b"hello".to_vec(), b"her".to_vec(), b"hero".to_vec()])
        );
    }

    #[test]
    fn contains_match() {
        let idx = indexed(&[b"abcXYZ\0", b"XYZabc\0", b"nomatch\0"]);
        assert_eq!(
            matches(&idx, b"*abc*", NO_CAP),
            Some(vec![b"XYZabc".to_vec(), b"abcXYZ".to_vec()])
        );
    }

    #[test]
    fn question_mark_matches_single_char() {
        let idx = indexed(&[b"cat\0", b"cot\0", b"coat\0"]);
        // `c?t` matches only the exactly-3-char terms `c_t`, not `coat`.
        assert_eq!(
            matches(&idx, b"c?t", NO_CAP),
            Some(vec![b"cat".to_vec(), b"cot".to_vec()])
        );
    }

    #[test]
    fn max_prefix_expansions_caps_results() {
        let idx = indexed(&[b"aa\0", b"ba\0", b"ca\0", b"da\0"]);
        // Cap semantics mirror C `_getWildcardArray`: it stops once the result
        // length already exceeds the cap, so a cap of N yields N + 1 entries.
        let pattern = SuffixWildcardPattern::new(b"*a").expect("valid token");
        let got = idx.suffix_wildcard(&pattern, None, 1).count();
        assert_eq!(got, 2);
    }
}

/// Timeout handling of the suffix/wildcard expansion. On timeout both
/// functions stop and return the matches gathered so far (partial results),
/// mirroring the C `GetList_SuffixTrieMap*` behavior.
#[cfg(test)]
mod expansion_timeout_tests {
    use super::*;

    const NO_CAP: u64 = u64::MAX;
    /// Comfortably larger than [`TIMEOUT_CHECK_GRANULARITY`], so a zero-budget
    /// deadline is guaranteed to trigger before the corpus is exhausted while
    /// still leaving many entries unprocessed.
    const CORPUS: usize = TIMEOUT_CHECK_GRANULARITY as usize * 3;

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
    /// share the literal prefix `he` (NUL-terminated, as `commit` requires).
    /// `he*` (wildcard) visits one full-term suffix entry per term, and the
    /// contains-expansion `e` visits one proper-suffix entry per term — both
    /// more than the check granularity.
    fn big_index() -> (TagIndex, usize) {
        let owned: Vec<Vec<u8>> = (0..CORPUS)
            .map(|i| format!("he{i:05}\0").into_bytes())
            .collect();
        let tags: Vec<&[u8]> = owned.iter().map(|t| t.as_slice()).collect();
        let mut idx = TagIndex::new(1, None, true);
        idx.commit(&tags);
        (idx, owned.len())
    }

    #[test]
    fn suffix_wildcard_no_timeout_returns_all() {
        let (idx, total) = big_index();
        let pattern = SuffixWildcardPattern::new(b"he*").expect("valid token");
        let got = idx.suffix_wildcard(&pattern, None, NO_CAP).count();
        assert_eq!(got, total, "every `he*` term must be expanded");
    }

    #[test]
    fn suffix_wildcard_times_out_with_partial_results() {
        let (idx, total) = big_index();
        // An already-elapsed deadline must not panic (the old `unimplemented!()`
        // would have) and must yield a strict, non-empty subset.
        let pattern = SuffixWildcardPattern::new(b"he*").expect("valid token");
        let got = idx
            .suffix_wildcard(&pattern, Some(expired()), NO_CAP)
            .count();
        assert!(got > 0, "the first granularity-1 entries are collected");
        assert!(got < total, "timeout must stop before the full expansion");
    }

    // Contains-mode (`prefix == true`) prefix-iterates the suffix trie, probing
    // the deadline once per entry. Each `heNNNNN` term contributes exactly one
    // suffix entry starting with `e` (`eNNNNN`), so `e` visits one entry per
    // term, above the check granularity.
    #[test]
    fn suffix_trie_map_no_timeout_returns_all() {
        let (idx, total) = big_index();
        let got = idx.suffix_trie_map(b"e", true, None).count();
        assert_eq!(got, total, "every term containing `e` must be expanded");
    }

    #[test]
    fn suffix_trie_map_times_out_with_partial_results() {
        let (idx, total) = big_index();
        let got = idx.suffix_trie_map(b"e", true, Some(expired())).count();
        assert!(got > 0, "the first granularity-1 entries are collected");
        assert!(got < total, "timeout must stop before the full expansion");
    }

    #[test]
    fn expansion_timeout_opts_out_when_no_deadline() {
        // `None` is the `skipTimeoutChecks` path (set at the FFI boundary): it
        // must disable checks entirely. The Redis `time_t::MAX` "no timeout"
        // sentinel is likewise mapped to `NoTimeout`, but that mapping lives in
        // `duration_from_redis_timespec` and is covered by its own crate tests.
        assert!(matches!(
            expansion_timeout(None),
            AnyTimeoutContext::NoTimeout(_)
        ));
    }

    #[test]
    fn expansion_timeout_uses_clock_for_a_real_deadline() {
        let mut ctx = expansion_timeout(Some(expired()));
        assert!(matches!(ctx, AnyTimeoutContext::Clock(_)));
        // A zero-budget clock times out once the granularity counter is reached.
        let timed_out = (0..TIMEOUT_CHECK_GRANULARITY).any(|_| ctx.check_timeout().is_err());
        assert!(
            timed_out,
            "an elapsed deadline must time out within one window"
        );
    }
}

/// Deltas produced by writing a document's tag postings, to fold into the
/// spec statistics — mirrors the C `TagIndex_WritePostings` accounting.
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
) -> WritePostingsDelta {
    let mut delta = WritePostingsDelta::default();

    let record = RSIndexResult::build_virt().doc_id(doc_id).build();
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
