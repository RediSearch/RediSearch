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
//!   Writes stage onto a disk write batch ([`TagIndex::index`]), reads open a
//!   disk iterator ([`TagIndex::open_reader`]), and query expansion still walks
//!   the presence trie for matching keys before opening each reader by string.
//!
//! ## Tag bytes
//!
//! Tag values are [`Tag`], which carries exactly one guarantee: no
//! *interior* (or trailing) NUL byte. [`Tag::new`] enforces it; [`Tag::new_unchecked`]
//! trusts the caller instead.
//!
//! Disk-mode indexing ([`TagIndex::index`] on [`OnDiskMode`]) is the one
//! exception, and takes `&CStr` instead. In fact,
//! `SearchDisk_IndexTags` receives the tags as a `const char **` with a count.
//!
//! The terms yielded by [`TagIndex::suffix_expand`] are [`CStr`](std::ffi::CStr), borrowed from a
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

mod iter;
mod suffix;
mod tag;
mod unique_id;

use std::mem::ManuallyDrop;

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

pub(crate) use suffix::{SuffixData, TagSuffixIndex};
pub(crate) use tag::expansion_deadline;
pub use tag::{
    InMemoryMode, NoAnchorToken, OnDiskMode, SuffixQuery, SuffixWildcardPattern, Tag, TagIndex,
    TagIndexMode, TrieLookup, WritePostingsDelta,
};
pub use unique_id::TagUniqueId;

/// Which field of [`ErasedTagIndex`]'s storage union is live.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// The index keeps its postings in memory — see [`InMemoryMode`].
    InMemory,
    /// The index keeps its postings on disk — see [`OnDiskMode`].
    OnDisk,
}

/// The two index types, overlaid. Which field is live is recorded by the
/// [`ErasedTagIndex::mode`] discriminant stored next to it; a union has no drop
/// glue, so [`ErasedTagIndex`]'s [`Drop`] drops the live one by hand.
union Storage {
    in_memory: ManuallyDrop<TagIndex<InMemoryMode>>,
    on_disk: ManuallyDrop<TagIndex<OnDiskMode>>,
}

/// A [`TagIndex`] with its storage mode erased: one type for owners that hold a
/// single handle and learn the mode at runtime, such as the C module.
///
/// It is deliberately a discriminant plus a union rather than an `enum`. Matching on
/// an enum needs a reference to the handle, and the pointer to the in-memory index
/// would then be derived from that reference — which is exactly what
/// [`TrieLookup::new`]'s first contract forbids, because a writer taking a `&mut`
/// through the same handle would revoke it.
///
/// Every accessor is therefore an associated function taking the handle as a raw
/// pointer, not a `&self` method: they are raw place projections that form no
/// reference to the handle, so what they return carries the owner's own provenance.
/// `tag_index_ffi`'s `provenance` integration test is the regression guard for that
/// claim — it fails under `miri` as soon as an accessor is rewritten to go through a
/// reference.
pub struct ErasedTagIndex {
    mode: Mode,
    storage: Storage,
}

impl ErasedTagIndex {
    /// Erase the mode of an index that keeps its postings in memory.
    pub const fn new_in_memory(index: TagIndex<InMemoryMode>) -> Self {
        Self {
            mode: Mode::InMemory,
            storage: Storage {
                in_memory: ManuallyDrop::new(index),
            },
        }
    }

    /// Erase the mode of an index that keeps its postings on disk.
    pub const fn new_on_disk(index: TagIndex<OnDiskMode>) -> Self {
        Self {
            mode: Mode::OnDisk,
            storage: Storage {
                on_disk: ManuallyDrop::new(index),
            },
        }
    }

    /// The live storage mode, read without forming a reference to the handle.
    ///
    /// Reading through a `&ErasedTagIndex` would freeze the whole handle for the
    /// duration of the borrow, and any mutable pointer derived from it — such as the
    /// one [`in_memory_ptr`](Self::in_memory_ptr) hands to [`TrieLookup`] — would be
    /// invalid to write through. A raw read keeps the handle untagged.
    ///
    /// # Safety
    ///
    /// `handle` must point to a live [`ErasedTagIndex`].
    pub const unsafe fn mode(handle: *const Self) -> Mode {
        // SAFETY: the caller guarantees `handle` points to a live `ErasedTagIndex`,
        // so the place expression is valid. Taking its address is not a read.
        let mode = unsafe { &raw const (*handle).mode };
        // SAFETY: as above — the `mode` field of a live handle is initialised.
        unsafe { mode.read() }
    }

    /// The in-memory index inside `handle`.
    ///
    /// This is a raw place projection, not a reborrow: no reference to the handle
    /// exists at any point, so the result carries `handle`'s own provenance. That is
    /// what lets [`TrieLookup::new`]'s first contract be met — see the [type
    /// documentation](Self).
    ///
    /// # Safety
    ///
    /// `handle` must point to a live [`ErasedTagIndex`] whose mode is
    /// [`Mode::InMemory`].
    pub const unsafe fn in_memory_ptr(handle: *const Self) -> *mut TagIndex<InMemoryMode> {
        // SAFETY: the caller guarantees the handle is live and in memory mode, so
        // `storage.in_memory` is the initialised union field. `ManuallyDrop<T>` is
        // `repr(transparent)`, so the cast is a no-op on the address.
        unsafe { &raw mut (*handle.cast_mut()).storage.in_memory }.cast()
    }

    /// The on-disk index inside `handle`, projected as
    /// [`in_memory_ptr`](Self::in_memory_ptr) does.
    ///
    /// # Safety
    ///
    /// `handle` must point to a live [`ErasedTagIndex`] whose mode is
    /// [`Mode::OnDisk`].
    pub const unsafe fn on_disk_ptr(handle: *const Self) -> *mut TagIndex<OnDiskMode> {
        // SAFETY: as `in_memory_ptr`, for the other union field.
        unsafe { &raw mut (*handle.cast_mut()).storage.on_disk }.cast()
    }

    /// Borrow the in-memory index inside `handle`.
    ///
    /// # Safety
    ///
    /// 1. As [`in_memory_ptr`](Self::in_memory_ptr).
    /// 2. The index must not be mutated for `'a`.
    pub unsafe fn in_memory<'a>(handle: *const Self) -> &'a TagIndex<InMemoryMode> {
        // SAFETY: contract 1.
        let index = unsafe { Self::in_memory_ptr(handle) };
        // SAFETY: contracts 1 and 2.
        unsafe { &*index }
    }

    /// Borrow the in-memory index inside `handle` exclusively.
    ///
    /// # Safety
    ///
    /// 1. As [`in_memory_ptr`](Self::in_memory_ptr).
    /// 2. No other reference to the index may be live for `'a`.
    pub unsafe fn in_memory_mut<'a>(handle: *mut Self) -> &'a mut TagIndex<InMemoryMode> {
        // SAFETY: contract 1.
        let index = unsafe { Self::in_memory_ptr(handle) };
        // SAFETY: contracts 1 and 2.
        unsafe { &mut *index }
    }

    /// Borrow the on-disk index inside `handle`, as [`in_memory`](Self::in_memory)
    /// does.
    ///
    /// # Safety
    ///
    /// As [`in_memory`](Self::in_memory), for the other union field.
    pub unsafe fn on_disk<'a>(handle: *const Self) -> &'a TagIndex<OnDiskMode> {
        // SAFETY: contract 1.
        let index = unsafe { Self::on_disk_ptr(handle) };
        // SAFETY: contracts 1 and 2.
        unsafe { &*index }
    }

    /// Borrow the on-disk index inside `handle` exclusively, as
    /// [`in_memory_mut`](Self::in_memory_mut) does.
    ///
    /// # Safety
    ///
    /// As [`in_memory_mut`](Self::in_memory_mut), for the other union field.
    pub unsafe fn on_disk_mut<'a>(handle: *mut Self) -> &'a mut TagIndex<OnDiskMode> {
        // SAFETY: contract 1.
        let index = unsafe { Self::on_disk_ptr(handle) };
        // SAFETY: contracts 1 and 2.
        unsafe { &mut *index }
    }
}

impl Drop for ErasedTagIndex {
    fn drop(&mut self) {
        // A union has no drop glue, so the live field is dropped by hand. Nothing can
        // still borrow it: `&mut self` here means this is the last use of the handle.
        match self.mode {
            Mode::InMemory => {
                // SAFETY: the discriminant says this is the initialised field.
                let field = unsafe { &mut self.storage.in_memory };
                // SAFETY: `drop` runs at most once, so the field is dropped once.
                unsafe { ManuallyDrop::drop(field) };
            }
            Mode::OnDisk => {
                // SAFETY: as above, for the other field.
                let field = unsafe { &mut self.storage.on_disk };
                // SAFETY: as above.
                unsafe { ManuallyDrop::drop(field) };
            }
        }
    }
}
