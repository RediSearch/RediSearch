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
//! It supports two storage modes. The mode is chosen by the constructor used
//! ([`TagIndex<InMemoryMode>::new`] or [`TagIndex<OnDiskMode>::new`]) and, since it never
//! changes afterwards, it is carried in the type — `TagIndex<`[`InMemoryMode`]`>`
//! or `TagIndex<`[`OnDiskMode`]`>`. Each
//! mode owns the values trie in the shape it needs, and a method that only makes
//! sense in one mode lives in that mode's `impl` block.
//!
//! - **Memory mode** keeps the per-tag posting lists (document ids) inline in
//!   the values trie.
//! - **Disk mode** keeps the postings on disk behind the
//!   `RSE` API; the values trie holds only tag *presence* sentinels.
//!
//! ## Tag bytes
//!
//! Tag values are [`Tag`], which carries exactly one guarantee: no
//! *interior* (or trailing) NUL byte. [`Tag::new`] enforces it; [`Tag::new_unchecked`]
//! trusts the caller instead.
//!
//! The terms yielded by [`TagIndex::suffix_expand`] are [`CStr`], borrowed from a
//! NUL-terminated allocation whose pointer is directly usable as a C `char *`.
//! [`TagSuffixIndex`] owns that allocation and documents where the terminator
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

// Temporary
#![expect(dead_code, reason = "read by methods added in a follow-up change")]

mod iter;
mod suffix;
mod unique_id;

pub use iter::{
    DiskTagIndexIterator, IterMode, MemTagIndexIterator, SuffixEntryIterator, TagValueReader,
};

// Force-link the umbrella `redisearch_rs` crate so its `#[used]` symbol table keeps the
// Rust FFI functions that the linked C code (`libredisearch_c_bundle`) calls back into, and
// stub any remaining Redis module C symbols the tests pull in. Without the `extern crate`
// reference the umbrella rlib is dropped as unused and those symbols go undefined at link
// time.
#[cfg(test)]
extern crate redisearch_rs;

#[cfg(test)]
redis_mock::mock_or_stub_missing_redis_c_symbols!();

use std::ffi::CStr;
use std::ptr::NonNull;
use std::time::Instant;

use ffi::{
    IndexFlags_Index_DocIdsOnly, RedisSearchCtx, RedisSearchDiskIndexSpec, t_fieldIndex, timespec,
};
use field::{FieldExpirationPredicate, FieldFilterContext, FieldMaskOrIndex};
use index_result::RSIndexResult;
#[cfg(feature = "test-utils")]
use inverted_index::RepairContext;
use inverted_index::{
    DocId, GcApplyInfo, GcScanDelta, IndexReader, IndexUniqueId, InvertedIndex,
    doc_ids_only::DocIdsOnly,
};
use query_term::RSQueryTerm;
use rqe_iterators::{
    FieldExpirationChecker,
    inverted_index::{Tag as TagIterator, TagLookup},
    utils::duration_from_redis_timespec,
};
use rqe_wildcard::{MatchOutcome, WildcardPattern};
pub(crate) use suffix::{SuffixData, TagSuffixIndex};
use trie_rs::{
    TrieMap,
    iter::{LendingIter, filter::VisitAll},
};
pub use unique_id::TagUniqueId;

/// A tag value: borrowed bytes guaranteed to contain no interior (neither trailing) NUL byte — see
/// the crate-level "Tag bytes" docs for exactly what that does and doesn't cover.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tag<'a>(&'a [u8]);

impl<'a> Tag<'a> {
    /// `None` if `bytes` contains an interior NUL byte.
    pub fn new(bytes: &'a [u8]) -> Option<Self> {
        (!bytes.contains(&0)).then_some(Self(bytes))
    }

    /// Builds a [`Tag`] without checking for an interior or trailing NUL byte.
    ///
    /// # Safety
    ///
    /// `bytes` must contain no interior or trailing NUL byte.
    pub unsafe fn new_unchecked(bytes: &'a [u8]) -> Self {
        debug_assert!(
            !bytes.contains(&0),
            "bytes must contain no interior NUL byte"
        );
        Self(bytes)
    }

    /// The underlying NUL-free bytes.
    pub const fn as_bytes(self) -> &'a [u8] {
        self.0
    }

    /// Pointer to the underlying NUL-free bytes.
    pub const fn as_ptr(self) -> *const u8 {
        self.0.as_ptr()
    }
}

mod sealed {
    pub trait Sealed {}
}

/// The way a [`TagIndex`] stores its postings, as a type.
///
/// This trait is sealed — only [`InMemoryMode`] and [`OnDiskMode`] implement it.
pub trait TagIndexMode: sealed::Sealed {}

/// Postings (doc_ids) kept in memory.
pub struct InMemoryMode {
    /// tag value -> document ids.
    ///
    /// Box here is required to address the safety rule of iterator.
    /// If suspended, the iterator requires to have a stable pointer to value.
    /// `Box<_>` has this guaranteed.
    values: TrieMap<Box<InvertedIndex<DocIdsOnly>>>,
}

impl sealed::Sealed for InMemoryMode {}
impl TagIndexMode for InMemoryMode {}

/// Postings (doc_ids) kept on disk behind the RSE API.
pub struct OnDiskMode {
    /// tag value -> (). It is used only to know whether a tag is there
    values: TrieMap<()>,
    /// Field id
    field_id: t_fieldIndex,
    /// Disk Index spec, valid for as long as this index lives.
    disk_index_spec: NonNull<RedisSearchDiskIndexSpec>,
}

impl sealed::Sealed for OnDiskMode {}
impl TagIndexMode for OnDiskMode {}

/// See the [crate documentation](self) for an overview.
pub struct TagIndex<M: TagIndexMode> {
    /// Unique id generated at creation time.
    unique_id: TagUniqueId,

    /// Suffix index, present only for fields created `WITHSUFFIXTRIE`.
    suffix: Option<TagSuffixIndex>,

    /// The storage mode, owning the values trie and whatever else that mode
    /// needs — the postings themselves in [`InMemoryMode`], the disk handles in
    /// [`OnDiskMode`].
    mode: M,
}

/// Deltas produced by writing a document's tag postings, to fold into the
/// spec statistics.
#[derive(Debug, Clone, Copy, Default)]
pub struct WritePostingsDelta {
    /// Bytes by which the inverted-index memory grew.
    pub size_delta: usize,
    /// Number of new (tag, doc) postings written by the in-memory path — see
    /// [`index`](TagIndex::index) for exactly what counts as "new".
    pub num_records: u32,
    /// Number of inverted-index blocks allocated.
    pub blocks_added: u32,
}

impl<M: TagIndexMode> TagIndex<M> {
    /// The part of construction every mode's constructor shares.
    fn with_mode(with_suffix: bool, mode: M) -> Self {
        Self {
            unique_id: TagUniqueId::next(),
            suffix: with_suffix.then(TagSuffixIndex::new),
            mode,
        }
    }

    /// The unique id generated when this index was created.
    pub const fn id(&self) -> TagUniqueId {
        self.unique_id
    }

    /// Returns `true` if suffix search is supported
    pub const fn has_suffix(&self) -> bool {
        self.suffix.is_some()
    }

    /// Bytes the [suffix index](TagSuffixIndex) occupies, or `0` when the index was
    /// created without `WITHSUFFIXTRIE`.
    const fn suffix_mem_usage(&self) -> usize {
        match &self.suffix {
            Some(suffix) => suffix.mem_usage(),
            None => 0,
        }
    }

    /// Register `tags` in the [suffix index](TagSuffixIndex), when enabled.
    fn add_tags_to_suffix(&mut self, tags: &[Tag<'_>]) {
        let Some(suffix) = &mut self.suffix else {
            return;
        };
        for tag in tags {
            suffix.add(*tag);
        }
    }

    /// Iterate over all the entries of the [suffix index](TagSuffixIndex), in
    /// lexicographical order of the suffix, or `None` when the index was
    /// created without `WITHSUFFIXTRIE`.
    pub(crate) fn iter_suffix_entries(&self) -> Option<LendingIter<'_, SuffixData, VisitAll>> {
        self.suffix.as_ref().map(TagSuffixIndex::lending_iter)
    }

    /// Expand a [`SuffixQuery`] against the [suffix index](TagSuffixIndex) into
    /// the concrete tag terms it matches, yielded lazily.
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
    ) -> Box<dyn Iterator<Item = &'a CStr> + 'a> {
        let suffix = self.suffix.as_ref().expect("suffix trie must exist");
        let deadline = expansion_deadline(timeout);

        let materialize = |p: suffix::TermPtr| {
            // SAFETY: `p` is a live `TermPtr` owned by this suffix trie (built
            // from `OwnedTerm::borrowed`), and `OwnedTerm::new` NUL-terminates
            // every allocation.
            unsafe { CStr::from_ptr(p.as_ptr().cast()) }
        };

        match query {
            SuffixQuery::Suffix(tag) => Box::new(
                // Exact node lookup, so nothing to bound: a single node yields all
                // its members.
                suffix
                    .find(tag.as_bytes())
                    .into_iter()
                    .flat_map(move |data| data.members().map(materialize)),
            ),
            SuffixQuery::Contains(tag) => {
                let mut entries = suffix.prefixed_iter(tag.as_bytes());
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
                        .filter(move |term| full.matches(term.to_bytes()) == MatchOutcome::Match)
                        // Cap the *matched* terms, overshooting by one to know whether
                        // there was at least one more expansion than the allowed maximum.
                        // See QueryError_SetReachedMaxPrefixExpansionsWarning usage in `query.c`
                        .take((max_prefix_expansions as usize).saturating_add(1)),
                )
            }
        }
    }
}

impl TagIndex<InMemoryMode> {
    /// Create a new, empty index keeping its postings in memory.
    ///
    /// `with_suffix` enables the [suffix index](TagSuffixIndex)
    /// (`WITHSUFFIXTRIE`), so suffix (`*foo`) and contains (`*foo*`)
    /// queries don't have to scan the whole tag trie.
    pub fn new(with_suffix: bool) -> Self {
        Self::with_mode(
            with_suffix,
            InMemoryMode {
                values: TrieMap::new(),
            },
        )
    }

    /// How many distinct tags the index holds.
    pub const fn n_tags(&self) -> usize {
        self.mode.values.n_unique_keys()
    }

    /// Bytes the index's tries occupy, the `FT.INFO` overhead figure. The per-tag inverted indexes are deliberately excluded:
    /// their bytes are accounted for in the spec's `invertedSize` statistic instead.
    pub const fn mem_usage(&self) -> usize {
        self.mode.values.mem_usage() + self.suffix_mem_usage()
    }

    /// Index `doc_id` under each tag in `tags`, writing the postings inline into
    /// the per-tag inverted index. The caller must follow up with
    /// [`commit`](Self::commit) on the same `tags` to complete the document
    /// write (it registers them in the suffix index, when enabled).
    ///
    /// Returns the [`WritePostingsDelta`] the caller folds into the spec
    /// statistics (records, memory, blocks).
    /// `num_records` counts a (tag, doc) posting as new only when it makes the tag's
    /// posting list grow by a document, so a value repeated within this document's
    /// own multi-value tags is counted once.
    ///
    /// `has_field_expiration` records whether this document carries a TTL on the
    /// field being indexed. It is stored per posting as
    /// [`RSIndexResult::has_field_expiration`] and gates the TTL re-check
    /// performed on read.
    ///
    /// `doc_id` should be greater than or equal to every `doc_id` already passed to `index` for
    /// this [`TagIndex`]. See [`InvertedIndex::add_record`] for more information.
    pub fn index(
        &mut self,
        tags: &[Tag<'_>],
        doc_id: DocId,
        has_field_expiration: bool,
    ) -> WritePostingsDelta {
        let mut delta = WritePostingsDelta::default();

        let record = RSIndexResult::build_virt()
            .doc_id(doc_id)
            .has_field_expiration(has_field_expiration)
            .build();
        for tag in tags {
            self.mode.values.insert_with(tag.as_bytes(), |slot| {
                let mut ii = slot.unwrap_or_else(|| {
                    let ii = InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly);
                    delta.size_delta += ii.memory_usage();
                    Box::new(ii)
                });

                debug_assert!(
                    ii.last_doc_id().is_none_or(|last| last <= doc_id),
                    "TagIndex::index called with a doc_id smaller than one already indexed for \
                    this tag; see its docs for the ordering it requires"
                );

                let docs_before = ii.unique_docs();
                let outcome = ii
                    .add_record(&record)
                    .expect("in-memory tag inverted index write cannot fail");
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

    /// Apply the per-tag metadata updates after the indexing phase of a document
    /// write: register the tags in the [suffix index](TagSuffixIndex), when enabled.
    ///
    /// Returns the number of records to fold into the spec statistics, always `0`:
    /// the postings were written, and counted, by [`index`](Self::index).
    pub fn commit(&mut self, tags: &[Tag<'_>]) -> u32 {
        self.add_tags_to_suffix(tags);
        0
    }

    /// Create an iterator over the documents matching `tag`, resolved in the values
    /// trie, or `None` when the tag is absent or holds no documents.
    ///
    /// `lookup` is the handle the iterator revalidates through, by re-resolving the
    /// tag in the values trie — that is how it detects that the garbage collector
    /// removed or replaced the inverted index it reads.
    ///
    /// # Safety
    ///
    /// 1. `self` must outlive the returned iterator, and must not be mutated while
    ///    it is in use except under the standard revalidation protocol.
    /// 2. `sctx` and `sctx.spec` must be valid and outlive the returned iterator.
    /// 3. `lookup` must resolve `self`, checked by a `debug_assert!` below.
    pub unsafe fn open_reader(
        &self,
        sctx: NonNull<RedisSearchCtx>,
        tag: Tag<'_>,
        weight: f64,
        field_index: t_fieldIndex,
        lookup: TrieLookup,
    ) -> Option<TagIterator<'_, DocIdsOnly, TrieLookup, FieldExpirationChecker>> {
        let ii = self.mode.values.find(tag.as_bytes())?;
        if ii.unique_docs() == 0 {
            return None;
        }

        let term = RSQueryTerm::new_bytes(tag.as_bytes(), 0, 0);

        let filter_ctx = FieldFilterContext {
            field: FieldMaskOrIndex::Index(field_index),
            predicate: FieldExpirationPredicate::Default,
        };
        let reader = ii.reader();
        // SAFETY: contract 2 guarantees sctx/spec validity for the checker's
        // lifetime.
        let checker = unsafe { FieldExpirationChecker::new(sctx, filter_ctx, reader.flags()) };

        // Contract 3.
        debug_assert!(
            std::ptr::eq(
                lookup.index_ptr().as_ptr().cast_const(),
                std::ptr::from_ref(self)
            ),
            "lookup must resolve the index this reader reads from"
        );

        // SAFETY: `reader` reads the inverted index just resolved for `tag`,
        // contract 2 guarantees `sctx` and `sctx.spec` are valid, and contract 3
        // is `TagIterator::new`'s own requirement on `lookup`.
        Some(unsafe { TagIterator::new(reader, sctx, lookup, term, weight, checker) })
    }

    /// Apply a garbage-collection `delta` (computed by a fork GC scan) to the
    /// inverted index stored for `tag`. If no document is left afterwards,
    /// the tag is dropped from the values trie and from the
    /// [suffix index](TagSuffixIndex), when enabled.
    ///
    /// `unique_id` is the [`IndexUniqueId`] of the inverted index the GC scan ran
    /// against, read via [`InvertedIndex::unique_id`] at scan time: when the tag
    /// was removed or its index replaced in the meantime, the delta is stale and
    /// `None` is returned without applying anything.
    ///
    /// A pointer to the scanned index would not do here in place of `unique_id`: the
    /// scan and this call are the two ends of a fork-GC cycle, so the tag's
    /// `Box<InvertedIndex<DocIdsOnly>>` can be freed and a new one allocated at the
    /// same address in between (the tag emptied, then re-added) — an ABA problem a
    /// raw pointer comparison cannot detect, but a monotonically-assigned unique ID
    /// can. See [`IndexUniqueId`] and [`RawIndexReaderCore::points_to_ii`], which
    /// guards the equivalent reader-revalidation case the same way.
    ///
    /// On success, returns the [`GcApplyInfo`] describing the applied changes.
    /// Its [`bytes_freed`](GcApplyInfo::bytes_freed) and
    /// [`block_count_delta`](GcApplyInfo::block_count_delta) already account for
    /// the whole posting list being dropped when the tag became empty.
    ///
    /// [`RawIndexReaderCore::points_to_ii`]: inverted_index::reader::RawIndexReaderCore::points_to_ii
    pub fn gc(
        &mut self,
        tag: &[u8],
        unique_id: IndexUniqueId,
        delta: GcScanDelta,
    ) -> Option<GcApplyInfo> {
        let ii = self.mode.values.find_mut(tag)?;
        // Detects the tag being removed or its index replaced meanwhile.
        if ii.unique_id() != unique_id {
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
    fn remove_tag_value(&mut self, tag: &[u8]) {
        self.mode.values.remove(tag);
    }
}

// More methods to access internals for test purposes.
#[cfg(feature = "test-utils")]
impl TagIndex<InMemoryMode> {
    /// Get the inverted index holding the postings for `tag`, if the tag is
    /// currently indexed.
    pub fn find_value(&self, tag: &[u8]) -> Option<&InvertedIndex<DocIdsOnly>> {
        self.mode.values.find(tag).map(Box::as_ref)
    }

    /// Simulate GC removing every posting under `tag`, leaving an empty (but
    /// still registered) inverted index behind — the state GC leaves once
    /// every document under a tag has been removed.
    pub fn gc_empty_value(&mut self, tag: &[u8]) {
        let ii = self.mode.values.find_mut(tag).expect("tag was indexed");
        let delta = ii
            .scan_gc(|_| false, None::<fn(&RSIndexResult, &RepairContext<'_>)>)
            .expect("scan_gc must not fail")
            .expect("every document was removed, so a delta is produced");
        ii.apply_gc(delta);
    }

    /// Simulate GC collecting a single document under `tag`, rewriting the
    /// inverted index in place. The index keeps its address and its unique id, so
    /// a reader suspended over it is *not* aborted on resume — it re-seeks
    /// instead, which is the branch [`gc_empty_value`](Self::gc_empty_value) and
    /// [`delete_tag_value`](Self::delete_tag_value) cannot reach.
    pub fn gc_remove_doc(&mut self, tag: &[u8], doc_id: DocId) {
        let ii = self.mode.values.find_mut(tag).expect("tag was indexed");
        let delta = ii
            .scan_gc(
                |d| d != doc_id,
                None::<fn(&RSIndexResult, &RepairContext<'_>)>,
            )
            .expect("scan_gc must not fail")
            .expect("a document was removed, so a delta is produced");
        assert_eq!(
            ii.apply_gc(delta).entries_removed,
            1,
            "the document must have been indexed under this tag"
        );
    }

    /// Whether `key` is registered in the [suffix index](TagSuffixIndex) — as a
    /// full tag term or as a suffix of one. `false` when suffix indexing is
    /// disabled.
    pub fn suffix_contains(&self, key: &[u8]) -> bool {
        self.suffix
            .as_ref()
            .is_some_and(|suffix| suffix.find(key).is_some())
    }

    /// Handle over [`remove_tag_value`](Self::remove_tag_value): lets tests
    /// simulate the garbage collector dropping every document for `tag`. The
    /// production path is [`gc`](Self::gc).
    pub fn delete_tag_value(&mut self, tag: &[u8]) {
        self.remove_tag_value(tag);
    }
}

impl TagIndex<OnDiskMode> {
    /// Create a new, empty index keeping its postings on disk.
    ///
    /// `disk_spec` is paired with `field_id`, the field index the disk API
    /// calls need. `with_suffix` is as in [`TagIndex<InMemoryMode>::new`].
    ///
    /// # Safety
    ///
    /// `disk_spec` must point to a *valid* [`RedisSearchDiskIndexSpec`] that
    /// remains valid for the lifetime of the returned [`TagIndex`]: the disk
    /// paths hand it to the `RSE` API, which dereferences it.
    pub unsafe fn new(
        disk_spec: NonNull<RedisSearchDiskIndexSpec>,
        field_id: t_fieldIndex,
        with_suffix: bool,
    ) -> Self {
        Self::with_mode(
            with_suffix,
            OnDiskMode {
                values: TrieMap::new(),
                field_id,
                disk_index_spec: disk_spec,
            },
        )
    }
}

pub(crate) fn expansion_deadline(timeout: Option<timespec>) -> Option<Instant> {
    let remaining = timeout.and_then(duration_from_redis_timespec)?;
    Some(Instant::now() + remaining)
}

/// [`TagLookup`] over this crate's typed values trie, used by the iterators
/// returned from [`TagIndex::open_reader`] to detect during revalidation that
/// the garbage collector removed or replaced a tag's inverted index.
///
/// Only a memory-mode index has an in-memory posting list to re-resolve a tag to.
pub struct TrieLookup(NonNull<TagIndex<InMemoryMode>>);

impl TrieLookup {
    /// Create a lookup over the given [`TagIndex`].
    ///
    /// # Safety
    ///
    /// 1. `idx` must be the pointer the index's owner holds — the one the
    ///    collector is handed to reach the same index, and **not** a pointer
    ///    derived from a `&TagIndex` or a `&mut TagIndex` to not break the
    ///    borrowing rules. Passing `idx` itself keeps the alternation
    ///    raw → `&mut` → raw, which is permitted.
    ///
    /// 2. `idx` must stay valid for this lookup and any iterator holding it.
    ///
    /// 3. The index may only be mutated while the lookup is alive under the
    ///    standard revalidation protocol.
    pub const unsafe fn new(idx: NonNull<TagIndex<InMemoryMode>>) -> Self {
        Self(idx)
    }

    /// The index this lookup resolves tags in.
    pub(crate) const fn index_ptr(&self) -> NonNull<TagIndex<InMemoryMode>> {
        self.0
    }
}

impl TagLookup<DocIdsOnly> for TrieLookup {
    fn find(&self, tag: &[u8]) -> Option<&InvertedIndex<DocIdsOnly>> {
        // SAFETY: contracts 1 and 2 of [`TrieLookup::new`] justify the reborrow's
        // address and lifetime. Contract 3 is what makes it sound against a
        // concurrent GC mutation.
        let tag_index = unsafe { self.0.as_ref() };

        tag_index.mode.values.find(tag).map(Box::as_ref)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rqe_iterators::{NoOpChecker, RQEIterator, RQEValidateStatus};
    use rqe_iterators_test_utils::MockContext;

    /// The iterator built by [`TagIndex::open_reader`] must abort
    /// revalidation once the garbage collector removed the tag's postings —
    /// [`TrieLookup`] no longer resolves the tag, so the reader is stale.
    ///
    /// The index is reached through raw pointers because the GC mutates it while
    /// the iterator is parked. That is the interleaving [`TrieLookup::new`]'s
    /// provenance contract exists for.
    #[test]
    fn revalidate_aborts_after_tag_removed() {
        let mock = MockContext::new(3, 3);
        let mut idx = TagIndex::<InMemoryMode>::new(false);
        let tags = &[Tag::new(b"team").unwrap()];
        for doc_id in 1..=3 {
            // `doc_id` is non-decreasing across loop iterations, as `index` requires.
            idx.index(tags, doc_id, false);
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
            TagIterator::new(
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

/// A wildcard pattern prepared for a suffix-trie lookup: the most selective
/// literal token, expanded into the anchor sub-pattern used to walk the trie.
#[derive(Debug)]
pub struct SuffixWildcardPattern<'p> {
    /// The whole original pattern, used to fully re-check each candidate term.
    full: &'p [u8],
    /// Anchor sub-pattern walked against the suffix trie: the chosen token
    /// bytes, plus a trailing `*` when the token is immediately followed by `*`
    /// in the original pattern.
    sub: Vec<u8>,
}

/// The pattern has no literal token usable as a suffix-trie anchor: every byte is
/// an unescaped `*`, or the pattern is empty. The caller must fall back to a
/// brute-force scan of the whole tag trie.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NoAnchorToken;

impl<'p> SuffixWildcardPattern<'p> {
    /// Prepare `pattern` for a suffix-trie lookup, choosing the most selective
    /// literal token as the anchor. Returns [`NoAnchorToken`] when there is no
    /// usable literal token.
    ///
    /// `pattern` arrives already unescaped — C runs `Wildcard_RemoveEscape` over
    /// it before the query reaches this crate — so it may end with a dangling
    /// backslash that escapes nothing.
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
    #[cfg(test)]
    pub(crate) fn anchor(&self) -> &[u8] {
        &self.sub
    }
}

/// Which query form [`TagIndex::suffix_expand`] expands, carrying the bytes it
/// expands.
#[derive(Debug, Clone, Copy)]
pub enum SuffixQuery<'a> {
    /// Suffix query `*foo`: exact lookup of the suffix-trie node `foo`, returning
    /// every term that node belongs to.
    Suffix(Tag<'a>),
    /// Contains query `*foo*`: prefix-iterate every suffix-trie node whose key
    /// starts with `foo`, unioning the terms they belong to.
    Contains(Tag<'a>),
    /// Wildcard query (`*` and `?` metacharacters): walk the suffix trie with the
    /// pattern's anchor sub-pattern, then re-check the whole pattern against each
    /// candidate term.
    Wildcard {
        /// The prepared pattern, owning the bytes the expansion borrows.
        ///
        /// A pattern with no usable anchor token (`*`, `**`, or empty) is rejected
        /// up front by [`SuffixWildcardPattern::new`], so an expansion always has
        /// a valid anchor and an empty result means the anchor matched no term.
        pattern: &'a SuffixWildcardPattern<'a>,
        /// Cap on the *matched* terms, overshot by one: the count is checked
        /// before each match is yielded. [`u64::MAX`] is the no-cap sentinel.
        max_prefix_expansions: u64,
    },
}

/// Penalty applied when an anchor token is immediately followed by `*`:
/// iterating all of a node's children is expensive.
const SUFFIX_STARRED_ANCHOR_PENALTY: i32 = ffi::SUFFIX_STARRED_ANCHOR_PENALTY as i32;

/// One literal token of a wildcard pattern: a maximal run of bytes holding no
/// unescaped `*`.
struct LiteralToken {
    /// Where the token starts in the pattern.
    start: usize,
    /// How long the token is, escape bytes included.
    len: usize,
    /// How many unescaped `?` the token holds.
    single_char_wildcards: usize,
}

/// Split `pattern` into the literal tokens between its unescaped `*`.
///
/// An escape and the byte it escapes are one literal unit — the same reading
/// [`WildcardPattern::parse`] gives them — so a `\*` stays inside its token
/// instead of ending it.
fn literal_tokens(pattern: &[u8]) -> Vec<LiteralToken> {
    let mut tokens: Vec<LiteralToken> = Vec::new();
    // `Some` while a token is open, i.e. while the last byte read was a literal.
    let mut open: Option<LiteralToken> = None;

    let mut i = 0;
    while i < pattern.len() {
        // A trailing `\` escapes nothing, and `WildcardPattern::parse` drops it.
        // Drop it here too — without closing the token it sits at the end of —
        // so the anchor and the full re-check read the same pattern.
        if pattern[i] == b'\\' && i + 1 == pattern.len() {
            break;
        }

        // How many bytes the unit at `i` spans, and whether it is a metacharacter.
        let (width, is_star, is_question) = match pattern[i] {
            b'\\' => (2, false, false),
            b'*' => (1, true, false),
            b'?' => (1, false, true),
            _ => (1, false, false),
        };

        if is_star {
            tokens.extend(open.take());
        } else {
            let token = open.get_or_insert(LiteralToken {
                start: i,
                len: 0,
                single_char_wildcards: 0,
            });
            token.len += width;
            token.single_char_wildcards += usize::from(is_question);
        }

        i += width;
    }
    tokens.extend(open);

    tokens
}

/// Return the `(offset, len)` of the most selective literal token of `pattern`,
/// or `None` when [`literal_tokens`] found none — a pattern of only unescaped
/// `*`, or an empty one. An unescaped `?` never leaves a pattern without an
/// anchor, since it does not end the token it sits in.
///
/// The score favors longer tokens and tokens later in the pattern, penalizes a
/// trailing `*` and every unescaped `?` inside the token; ties resolve to the
/// later token.
fn choose_token(pattern: &[u8]) -> Option<(usize, usize)> {
    let tokens = literal_tokens(pattern);

    let mut best_score = i32::MIN;
    let mut best = None;
    for (idx, token) in tokens.iter().enumerate() {
        // 1. longer tokens likely yield fewer results;
        // 2. tokens later in the pattern are likely more relevant.
        let mut score = token.len as i32 + idx as i32;

        // A trailing `*` forces iterating all children of the node. The byte
        // past a token is an unescaped `*` or nothing at all, by construction.
        if pattern.get(token.start + token.len) == Some(&b'*') {
            score -= SUFFIX_STARRED_ANCHOR_PENALTY;
        }

        // Each `?` inside the token adds heavy branching.
        score -= token.single_char_wildcards as i32;

        // `>=` keeps the later token on ties.
        if score >= best_score {
            best_score = score;
            best = Some((token.start, token.len));
        }
    }

    best
}

#[cfg(test)]
mod choose_token_tests {
    use super::*;

    const NO_CAP: u64 = u64::MAX;

    /// Build an in-memory index with a suffix trie and commit `tags`.
    fn indexed(tags: &[&[u8]]) -> TagIndex<InMemoryMode> {
        let mut idx = TagIndex::<InMemoryMode>::new(true);
        let tags: Vec<Tag<'_>> = tags
            .iter()
            .map(|t| Tag::new(t).expect("test literal is NUL-free"))
            .collect();
        idx.commit(&tags);
        idx
    }

    /// Expand a wildcard query and return the matched terms as owned byte vectors
    /// with the trailing NUL stripped, sorted for order-independent comparison.
    fn matches(idx: &TagIndex<InMemoryMode>, pattern: &[u8], cap: u64) -> Option<Vec<Vec<u8>>> {
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
            .map(|t| t.to_bytes().to_vec())
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
            (
                br"*foo\**",
                br"foo\**",
                "an escaped `*` is a literal, so it does not end the token",
            ),
            (
                br"abcdefg\?h*xyz",
                br"abcdefg\?h*",
                "an escaped `?` is a literal, so it takes no `?` penalty \
                 (10 - 5 = 5 > 3 + 1)",
            ),
            (
                b"abcdefg?h*xyz",
                b"xyz",
                "the same pattern with a live `?`: one shorter and penalized, so it \
                 drops below `xyz` (9 - 5 - 1 = 3 < 3 + 1)",
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
    fn multiple_stars_select_by_token_ordinal() {
        for (pattern, expected, why) in [
            (
                &b"**abcd**"[..],
                &b"abcd*"[..],
                "leading/trailing `*` runs collapse to a single token, which keeps its \
                 trailing `*` to prefix-expand the trie",
            ),
            (
                b"ab**cd",
                b"cd",
                "a double `*` still yields two tokens; the clean trailing `cd` wins \
                 over `ab`, which pays the trailing-`*` penalty",
            ),
            (
                b"ab*cd*ef",
                b"ef",
                "three tokens; the last, `ef`, avoids the trailing-`*` penalty every \
                 earlier token pays and wins",
            ),
            (
                b"a*b*cd*ef*ghij",
                b"ghij",
                "every token is scored, but the longest, `ghij`, still wins despite \
                 sitting last by character offset, not just by ordinal",
            ),
            (
                b"ab*cd*longestmiddle*ef",
                b"longestmiddle*",
                "the long middle token wins even carrying the trailing-`*` penalty, \
                 over the short clean trailing token",
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

    /// A dangling trailing backslash is what `Wildcard_RemoveEscape` leaves of a
    /// query for a literal backslash, so it reaches
    /// [`SuffixWildcardPattern::new`] and must be dropped rather than anchored
    /// on: [`WildcardPattern::parse`] drops it for the full recheck, and an
    /// anchor of `\` would select only terms that recheck then rejects.
    #[test]
    fn a_dangling_trailing_backslash_is_dropped_from_the_anchor() {
        let pattern = br"abc*\";
        let prepared = SuffixWildcardPattern::new(pattern).expect("`abc` anchors the pattern");
        assert_eq!(prepared.anchor(), b"abc*");

        // The two readings agree: with the backslash gone from both, `abc` is a
        // match under the full pattern too.
        assert_eq!(
            WildcardPattern::parse(pattern).matches(b"abc"),
            MatchOutcome::Match
        );
    }

    /// A backslash that does escape something still binds to the byte after it,
    /// so dropping the dangling case must not disturb the ordinary one.
    #[test]
    fn a_backslash_that_escapes_a_byte_stays_in_its_token() {
        let prepared = SuffixWildcardPattern::new(br"ab\*c").expect("the escaped `*` is a literal");
        assert_eq!(prepared.anchor(), br"ab\*c");
    }

    /// A `?` does not end the token it sits in, so a pattern of nothing but `?`
    /// anchors on itself instead of falling through to [`NoAnchorToken`] — the
    /// expansion goes through the suffix trie, not the caller's brute-force scan.
    /// C's `Suffix_ChooseToken` picks the same token.
    #[test]
    fn question_marks_alone_still_anchor() {
        let prepared = SuffixWildcardPattern::new(b"???").expect("one literal token of three `?`");
        assert_eq!(prepared.anchor(), b"???");

        let idx = indexed(&[b"cat", b"coat"]);
        // The anchor reaches `coat` through its three-byte suffix key `oat`, but
        // the full-pattern recheck rejects the four-byte term.
        assert_eq!(matches(&idx, b"???", NO_CAP), Some(vec![b"cat".to_vec()]));
    }
}

/// [`expansion_deadline`]'s opt-out and elapsed-deadline behavior — a
/// `pub(crate)` internal. The public-API timeout behavior of the wildcard and
/// contains expansions (partial results on an elapsed deadline, full results
/// with none) is covered by the `tag_index` crate's integration tests.
#[cfg(test)]
mod expansion_deadline_tests {
    use super::*;

    /// A deadline that has already elapsed. Any `CLOCK_MONOTONIC_RAW` value one
    /// second after boot is in the past on a running system, so
    /// `duration_from_redis_timespec` maps it to a zero remaining budget.
    fn expired() -> timespec {
        timespec {
            tv_sec: 1,
            tv_nsec: 0,
        }
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

#[cfg(test)]
mod mem_usage_tests {
    use super::*;

    #[test]
    fn mem_usage_adds_the_suffix_trie_exactly_once() {
        let tags: Vec<Tag<'_>> = [b"hello".as_slice(), b"world"]
            .iter()
            .map(|t| Tag::new(t).expect("test literal is NUL-free"))
            .collect();

        // Both indexes are indexed *and* committed, so the values trie is non-empty on
        // each side: were it missing from one of the two sums, the delta would not match.
        let mut with_suffix = TagIndex::<InMemoryMode>::new(true);
        with_suffix.index(&tags, 1, false);
        with_suffix.commit(&tags);

        let mut without_suffix = TagIndex::<InMemoryMode>::new(false);
        without_suffix.index(&tags, 1, false);
        without_suffix.commit(&tags);

        let mut suffix_alone = TagSuffixIndex::new();
        for tag in &tags {
            suffix_alone.add(*tag);
        }

        assert_eq!(
            with_suffix.mem_usage() - without_suffix.mem_usage(),
            suffix_alone.mem_usage(),
            "the suffix trie must contribute its bytes once, and the suffix flag must \
             leave the values trie untouched"
        );
    }
}
