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

mod suffix;
mod unique_id;

// Force-link the umbrella `redisearch_rs` crate so its `#[used]` symbol table keeps the
// Rust FFI functions that the linked C code (`libredisearch_c_bundle`) calls back into, and
// stub any remaining Redis module C symbols the tests pull in. Without the `extern crate`
// reference the umbrella rlib is dropped as unused and those symbols go undefined at link
// time.
#[cfg(test)]
extern crate redisearch_rs;

#[cfg(test)]
redis_mock::mock_or_stub_missing_redis_c_symbols!();

use std::ptr::NonNull;

use ffi::{IndexFlags_Index_DocIdsOnly, RedisSearchDiskIndexSpec, t_fieldIndex};
use index_result::RSIndexResult;
use inverted_index::{DocId, InvertedIndex, doc_ids_only::DocIdsOnly};
use suffix::TagSuffixIndex;
use trie_rs::TrieMap;
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
    /// [`index`](TagIndex::index) for exactly what counts as "new". A future
    /// on-disk `commit()` will count differently and should not assume this
    /// field's dedup guarantee.
    pub num_records: u32,
    /// Number of inverted-index blocks allocated.
    pub blocks_added: u32,
}

fn write_postings(
    values: &mut TrieMap<Box<InvertedIndex<DocIdsOnly>>>,
    tags: &[Tag<'_>],
    doc_id: DocId,
    has_field_expiration: bool,
) -> WritePostingsDelta {
    let mut delta = WritePostingsDelta::default();

    let mut record = RSIndexResult::build_virt().doc_id(doc_id).build();
    // The builder always clears this, so set it explicitly: `add_record` reads it
    // off the record to write the posting's expiration bit.
    record.has_field_expiration = has_field_expiration;
    for tag in tags {
        values.insert_with(tag.as_bytes(), |slot| {
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

    /// Register `tags` in the [suffix index](TagSuffixIndex), when enabled: the part
    /// of `commit` that does not depend on where the postings live.
    fn add_tags_to_suffix(&mut self, tags: &[Tag<'_>]) {
        let Some(suffix) = &mut self.suffix else {
            return;
        };
        for tag in tags {
            suffix.add(*tag);
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

    /// Index `doc_id` under each tag in `tags`, writing the postings inline into
    /// the per-tag inverted index.
    ///
    /// Returns the [`WritePostingsDelta`] the caller folds into the spec
    /// statistics (records, memory, blocks). This always succeeds.
    /// `num_records` counts a (tag, doc) posting as new only when it makes the tag's
    /// posting list grow by a document, so a value repeated within this document's
    /// own multi-value tags is counted once.
    ///
    /// `has_field_expiration` records whether this document carries a TTL on the
    /// field being indexed. It is stored per posting as
    /// [`RSIndexResult::has_field_expiration`] and gates the TTL re-check
    /// performed on read.
    pub fn index(
        &mut self,
        tags: &[Tag<'_>],
        doc_id: DocId,
        has_field_expiration: bool,
    ) -> WritePostingsDelta {
        write_postings(&mut self.mode.values, tags, doc_id, has_field_expiration)
    }

    /// Apply the per-tag metadata updates after the indexing phase of a document
    /// write: register the tags in the [suffix index](TagSuffixIndex), when enabled.
    ///
    /// Returns the number of records to fold into the spec statistics, always `0`:
    /// the postings were written, and counted, by [`index`](Self::index). Disk mode
    /// counts them here instead.
    pub fn commit(&mut self, tags: &[Tag<'_>]) -> u32 {
        self.add_tags_to_suffix(tags);
        0
    }
}

// Handles tests reach the index internals through, kept apart from the production
// methods above and gated behind the `test-utils` feature so they stay out of the
// public API in release builds.
#[cfg(feature = "test-utils")]
impl TagIndex<InMemoryMode> {
    /// Get the inverted index holding the postings for `tag`, if the tag is
    /// currently indexed.
    pub fn find_value(&self, tag: &[u8]) -> Option<&InvertedIndex<DocIdsOnly>> {
        self.mode.values.find(tag).map(Box::as_ref)
    }

    /// Get a mutable reference to the inverted index holding the postings for
    /// `tag`, if the tag is currently indexed.
    pub fn find_value_mut(&mut self, tag: &[u8]) -> Option<&mut InvertedIndex<DocIdsOnly>> {
        self.mode.values.find_mut(tag).map(Box::as_mut)
    }

    /// Get the inverted index for `tag`, registering a new empty one when the
    /// tag is not indexed yet and `create_if_missing` is set.
    pub fn open_index(
        &mut self,
        tag: &[u8],
        create_if_missing: bool,
    ) -> Option<&InvertedIndex<DocIdsOnly>> {
        let values = &mut self.mode.values;

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
