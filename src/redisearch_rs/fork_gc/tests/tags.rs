/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for the tags fork-GC scanner: what the child writes, what the parent
//! decodes, and how a decoded message reaches the right tag index.
//!
//! The fixture uses the C [`ffi::TagIndex`] on this branch, exercising the same
//! C tag-index APIs that the scanner uses in production.

// Every test here builds a synthetic spec, which means `NewHiddenString`,
// `HiddenString_Free` and `DocTable_Exists` — C functions miri cannot run. The
// FFI calls into the C tag index cannot run under miri.
#![cfg(not(miri))]

use std::ffi::{CStr, CString};
use std::io::Cursor;
use std::mem::{self, ManuallyDrop};

use field_spec::{FieldSpecBuilder, FieldSpecType, FieldSpecTypes};
use fork_gc::HandleError;
use fork_gc::tags::{apply_tag_entry, collect_tags, receive_tag_entry};
use index_spec::{IndexSpecReadGuard, IndexSpecWriteGuard};
use inverted_index::{DocId, opaque::InvertedIndex};
use serde::Serialize as _;

// Link both Rust-provided and C-provided symbols
extern crate redisearch_rs;
// Provide Redis allocator shims so the C dict functions can allocate memory.
redis_mock::mock_or_stub_missing_redis_c_symbols!();

/// Test-only owner for a C [`ffi::TagIndex`] and its C API operations.
struct TestTagIndex(*mut ffi::TagIndex);

impl TestTagIndex {
    /// Create a memory-mode C tag index holding `tags`, each carrying documents
    /// `1..=docs`.
    fn memory(field: &mut ffi::FieldSpec, tags: &[&[u8]], docs: DocId, with_suffix: bool) -> Self {
        // SAFETY: `field` is a TAG field and a null disk spec selects the in-memory
        // mode. The result is owned by this helper.
        let result =
            Self(unsafe { ffi::TagIndex_Ensure(field, std::ptr::null_mut(), with_suffix) });
        assert!(!result.0.is_null(), "TagIndex_Ensure returned null");
        result.populate(tags, docs, with_suffix);
        result
    }

    /// Create a disk-backed C tag index. Its postings are not available to fork GC.
    fn disk(field: &mut ffi::FieldSpec) -> Self {
        let disk_spec = std::ptr::NonNull::<ffi::RedisSearchDiskIndexSpec>::dangling().as_ptr();
        // SAFETY: `field` is a TAG field and the non-null disk spec selects disk mode.
        let result = Self(unsafe { ffi::TagIndex_Ensure(field, disk_spec, false) });
        assert!(!result.0.is_null(), "TagIndex_Ensure returned null");
        result
    }

    fn as_ref(&self) -> &ffi::TagIndex {
        // SAFETY: TestTagIndex owns this live C allocation for its whole lifetime.
        unsafe { &*self.0 }
    }

    /// Populate this memory-mode index through the production C indexing API.
    fn populate(&self, tags: &[&[u8]], docs: DocId, with_suffix: bool) {
        // Discarded: the fixture only needs postings and trie entries, not the
        // accounting gathered during document indexing.
        // SAFETY: all-zero is valid for IndexStats. The C path below only
        // updates its scalar counters.
        let mut stats: ffi::IndexStats = unsafe { mem::zeroed() };

        for &tag in tags {
            let tag = CString::new(tag).expect("test tag is NUL-free");
            let mut tag_ptr = tag.as_ptr();
            for doc_id in 1..=docs {
                let index_ctx = ffi::TagIndexIndexCtx {
                    batch: std::ptr::null_mut(),
                    values: &mut tag_ptr,
                    n: 1,
                    docId: doc_id,
                    hasFieldExpiration: false,
                    stats: &mut stats,
                };
                // SAFETY: the helper owns a live memory-mode C tag index and
                // the context points to the live, NUL-terminated test tag.
                assert!(unsafe { ffi::TagIndex_Index(std::ptr::null_mut(), self.0, &index_ctx) });
                // SAFETY: the tag index and tag pointer are still live; stats
                // is exclusively owned by this fixture.
                unsafe { ffi::TagIndex_Commit(self.0, &mut tag_ptr, 1, &mut stats) };
            }

            if docs == 0 {
                let mut size = 0;
                // SAFETY: this helper owns the C tag index and `tag_ptr` is
                // readable for `tag`'s byte length. Creating an empty posting
                // list lets the scanner verify it produces no delta for it.
                let ii = unsafe {
                    ffi::TagIndex_OpenIndex(self.0, tag_ptr, tag.as_bytes().len(), 1, &mut size)
                };
                assert!(!ii.is_null(), "TagIndex_OpenIndex returned null");
                if with_suffix && !tag.is_empty() {
                    // SAFETY: this index has a suffix trie and C owns its
                    // insertion through the commit API.
                    unsafe { ffi::TagIndex_Commit(self.0, &mut tag_ptr, 1, &mut stats) };
                }
            }
        }
    }

    fn posting_list(&self, tag: &[u8]) -> Option<&InvertedIndex> {
        let mut size = 0;
        // SAFETY: this helper owns the live C tag index and `tag` is readable
        // for its length. A zero creation flag leaves the trie unchanged.
        let ii = unsafe {
            ffi::TagIndex_OpenIndex(self.0, tag.as_ptr().cast(), tag.len(), 0, &mut size)
        };
        // SAFETY: TRIEMAP_NOTFOUND is the C trie's initialized not-found
        // sentinel for the duration of the process.
        if ii.is_null() || ii.cast() == unsafe { ffi::TRIEMAP_NOTFOUND } {
            return None;
        }
        // SAFETY: a successful C tag-index lookup returns the Rust opaque
        // inverted index stored in this helper-owned values trie.
        Some(unsafe { &*ii.cast::<InvertedIndex>() })
    }
}

impl Drop for TestTagIndex {
    fn drop(&mut self) {
        // SAFETY: this helper owns the C tag index and frees it exactly once.
        unsafe { ffi::TagIndex_Free(self.0) };
    }
}

/// One field of a synthetic spec.
struct Field {
    spec: ffi::FieldSpec,
    tag_index: Option<TestTagIndex>,
}

impl Field {
    fn tag(name: &'static CStr, tags: &[&[u8]], docs: DocId, with_suffix: bool) -> Self {
        let mut spec = FieldSpecBuilder::new(name)
            .with_types(FieldSpecType::Tag.into())
            .finish();
        let tag_index = TestTagIndex::memory(&mut spec, tags, docs, with_suffix);
        Self {
            spec,
            tag_index: Some(tag_index),
        }
    }

    fn tag_without_index(name: &'static CStr) -> Self {
        Self {
            spec: FieldSpecBuilder::new(name)
                .with_types(FieldSpecType::Tag.into())
                .finish(),
            tag_index: None,
        }
    }

    fn disk_tag(name: &'static CStr) -> Self {
        let mut spec = FieldSpecBuilder::new(name)
            .with_types(FieldSpecType::Tag.into())
            .finish();
        let tag_index = TestTagIndex::disk(&mut spec);
        Self {
            spec,
            tag_index: Some(tag_index),
        }
    }

    /// A field of some other type, which the scanner must not look at.
    fn other(name: &'static CStr, types: FieldSpecTypes) -> Self {
        Self {
            spec: FieldSpecBuilder::new(name).with_types(types).finish(),
            tag_index: None,
        }
    }
}

/// An `IndexSpec` owning the field array and the tag indexes it points at.
///
/// The spec's `docs` (DocTable) is left zeroed, so `DocTable_Exists` returns
/// `false` for every doc ID — every indexed document is treated as deleted, which
/// makes each populated tag produce a GC delta.
struct TestSpec {
    spec: ffi::IndexSpec,
    // Keeps the heap buffer `spec.fields` points at alive, and owns the
    // `HiddenString` each field name/path points at (freed in `Drop`).
    field_specs: Vec<ffi::FieldSpec>,
    // Owns the C tag indexes the field specs point at.
    tag_indexes: Vec<TestTagIndex>,
}

impl TestSpec {
    fn create(mut fields: Vec<Field>) -> TestSpec {
        let mut tag_indexes = Vec::new();
        for field in &mut fields {
            if let Some(tag_index) = field.tag_index.take() {
                tag_indexes.push(tag_index);
            }
        }
        let mut field_specs: Vec<_> = fields.into_iter().map(|field| field.spec).collect();

        // SAFETY: a zeroed IndexSpec is valid for the field access the scanner
        // performs; `fields`/`numFields` are set to a valid array below.
        let mut spec: ffi::IndexSpec = unsafe { mem::zeroed() };

        // The Vec's heap buffer stays put when `field_specs` is moved into the
        // struct, so this pointer remains valid for the lifetime of `TestSpec`.
        spec.fields = field_specs.as_mut_ptr();
        spec.numFields = field_specs.len() as u16;

        // `apply_tag_entry`'s caller subtracts from these counters, so give the
        // synthetic spec enough indexed records and bytes for the removals.
        spec.stats.numRecords = 1_000_000;
        spec.stats.invertedSize = 1_000_000;

        TestSpec {
            spec,
            field_specs,
            tag_indexes,
        }
    }

    /// Borrow the spec as a read guard, as the collect path receives it.
    fn read_guard(&self) -> ManuallyDrop<IndexSpecReadGuard<'_>> {
        // SAFETY: `spec` is a valid IndexSpec that outlives the guard, and this
        // single-threaded test never mutates it while the guard is held.
        unsafe { IndexSpecReadGuard::from_locked(&self.spec) }
    }

    /// Borrow the spec as a write guard, as the apply path receives it.
    fn write_guard(&mut self) -> ManuallyDrop<IndexSpecWriteGuard<'_>> {
        // SAFETY: each test owns the fixture exclusively and accesses it only
        // through the returned guard until that guard goes out of scope.
        unsafe { IndexSpecWriteGuard::from_locked_mut(&mut self.spec) }
    }

    /// The tag index of the `n`-th index-owning field.
    fn tag_index(&self, n: usize) -> &ffi::TagIndex {
        self.tag_indexes[n].as_ref()
    }

    /// The posting list under `tag` in the `n`-th tag index, if it exists.
    fn posting_list(&self, n: usize, tag: &[u8]) -> Option<&InvertedIndex> {
        self.tag_indexes[n].posting_list(tag)
    }

    /// Simulate the field dropping its in-memory C tag index before apply.
    fn remove_tag_index(&mut self, field: usize) {
        self.field_specs[field].__bindgen_anon_1.tagOpts.tagIndex = std::ptr::null_mut();
    }
}

impl Drop for TestSpec {
    fn drop(&mut self) {
        // Free the `HiddenString` allocated by `NewHiddenString` for each field.
        // `FieldSpecBuilder::new` sets `fieldName == fieldPath` (a single
        // allocation, taking ownership of the name), so free it exactly once per
        // field. Leaking it instead makes the tests hang under LeakSanitizer.
        for fs in &self.field_specs {
            // SAFETY: `fieldName` was created by `NewHiddenString(.., true)` and
            // equals `fieldPath`, so this frees the one owned allocation once.
            unsafe { ffi::HiddenString_Free(fs.fieldName, true) };
        }
    }
}

/// Run the child-side tag collector and return its complete output.
fn collect_tags_from(spec: &TestSpec) -> Vec<u8> {
    let mut buf = Vec::new();
    collect_tags(&mut buf, &spec.read_guard()).expect("writing to a Vec cannot fail");
    buf
}

/// Decode every message with the real reader, asserting it consumes the complete stream.
fn receive_all(buf: &[u8]) -> Vec<fork_gc::tags::TagEntry> {
    let mut cursor = Cursor::new(buf);
    let mut entries = Vec::new();
    while let Some(entry) = receive_tag_entry(&mut cursor).unwrap() {
        entries.push(entry);
    }
    assert_eq!(
        cursor.position(),
        buf.len() as u64,
        "trailing bytes after the end marker"
    );
    entries
}

#[test]
fn a_spec_without_fields_writes_only_the_end_marker() {
    let test = TestSpec::create(Vec::new());
    assert!(receive_all(&collect_tags_from(&test)).is_empty());
}

#[test]
fn fields_that_are_not_tag_fields_are_skipped() {
    let test = TestSpec::create(vec![
        Field::other(c"n", FieldSpecType::Numeric.into()),
        Field::other(c"t", FieldSpecType::Fulltext.into()),
    ]);
    assert!(receive_all(&collect_tags_from(&test)).is_empty());
}

#[test]
fn a_tag_field_whose_index_was_never_created_is_skipped() {
    let test = TestSpec::create(vec![Field::tag_without_index(c"tags")]);
    assert!(receive_all(&collect_tags_from(&test)).is_empty());
}

#[test]
fn a_disk_backed_tag_field_is_skipped() {
    let test = TestSpec::create(vec![Field::disk_tag(c"tags")]);
    assert!(receive_all(&collect_tags_from(&test)).is_empty());
}

/// A tag field that has never indexed a document has no values to walk.
#[test]
fn a_tag_field_with_no_values_is_skipped() {
    let test = TestSpec::create(vec![Field::tag(c"tags", &[], 0, false)]);
    assert!(receive_all(&collect_tags_from(&test)).is_empty());
}

#[test]
fn each_dirty_tag_carries_its_own_routing_data() {
    let test = TestSpec::create(vec![
        Field::other(c"n", FieldSpecType::Numeric.into()),
        Field::tag(c"colours", &[b"red", b"blue"], 3, false),
        Field::tag(c"sizes", &[b"xl"], 3, false),
    ]);

    let colours = test.tag_index(0).uniqueId;
    let sizes = test.tag_index(1).uniqueId;

    let entries = receive_all(&collect_tags_from(&test));
    let expected: &[(&[u8], u32, &[u8])] = &[
        // Tags come out in the values trie's lexicographical order.
        (b"colours", colours, b"blue"),
        (b"colours", colours, b"red"),
        (b"sizes", sizes, b"xl"),
    ];
    assert_eq!(entries.len(), expected.len());
    for (entry, &(field_name, tag_index_unique_id, tag)) in entries.iter().zip(expected) {
        assert_eq!(&*entry.field_name, field_name);
        assert_eq!(entry.tag_index_unique_id, tag_index_unique_id);
        assert_eq!(&*entry.tag, tag);
    }
}

/// `INDEXEMPTY` indexes the empty string, which is distinct from the MessagePack
/// `None` end marker that ends the stream.
#[test]
fn the_empty_tag_round_trips() {
    let test = TestSpec::create(vec![Field::tag(c"tags", &[b""], 3, false)]);

    let entries = receive_all(&collect_tags_from(&test));
    assert_eq!(entries.len(), 1);
    assert_eq!(&*entries[0].field_name, b"tags");
    assert_eq!(entries[0].tag_index_unique_id, test.tag_index(0).uniqueId);
    assert!(entries[0].tag.is_empty());
}

/// The unique id the child ships is the posting list's own, which is what
/// the C tag-GC apply path checks the delta against.
#[test]
fn the_message_carries_the_scanned_posting_lists_id() {
    let test = TestSpec::create(vec![Field::tag(c"tags", &[b"red"], 3, false)]);
    let expected = test
        .posting_list(0, b"red")
        .expect("the tag is indexed")
        .unique_id();

    let entries = receive_all(&collect_tags_from(&test));
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].inverted_index_unique_id, u32::from(expected));
}

/// A tag that loses its last document is dropped, and what that freed is
/// reported back so the caller can subtract it from the spec totals.
#[test]
fn applying_a_delta_drops_an_emptied_tag_and_reports_what_it_freed() {
    let mut test = TestSpec::create(vec![Field::tag(c"tags", &[b"red"], 3, false)]);

    let mut entries = receive_all(&collect_tags_from(&test));
    assert_eq!(entries.len(), 1);

    let stats = apply_tag_entry(entries.remove(0), &mut test.write_guard()).unwrap();

    assert_eq!(stats.records_removed, 3, "every document was deleted");
    assert!(
        stats.bytes_collected > 0,
        "the dropped posting list's memory is reported"
    );
    assert!(
        stats.block_count_delta < 0,
        "the dropped posting list's blocks are reported"
    );
    assert!(
        test.posting_list(0, b"red").is_none(),
        "the emptied tag is gone from the values trie"
    );
}

/// The field name is what routes a message to its index, so a name the spec does
/// not know is a parent-side error rather than a silent no-op.
#[test]
fn a_message_for_an_unknown_field_is_rejected() {
    let mut test = TestSpec::create(vec![Field::tag(c"tags", &[b"red"], 3, false)]);
    let mut entries = receive_all(&collect_tags_from(&test));
    entries[0].field_name = Box::from(&b"other"[..]);

    let err = apply_tag_entry(entries.remove(0), &mut test.write_guard()).unwrap_err();
    assert!(matches!(err, HandleError::ApplyError(_)));
    assert_eq!(
        err.to_string(),
        "no field in the spec matches the scanned field name"
    );
}

/// A field can remain in the spec while its in-memory tag index disappears
/// before the parent applies the child's message.
#[test]
fn a_message_for_a_field_without_an_in_memory_tag_index_is_rejected() {
    let mut test = TestSpec::create(vec![Field::tag(c"tags", &[b"red"], 3, false)]);
    let mut entries = receive_all(&collect_tags_from(&test));

    test.remove_tag_index(0);

    let err = apply_tag_entry(entries.remove(0), &mut test.write_guard()).unwrap_err();
    assert_eq!(
        err.to_string(),
        "the field no longer has an in-memory tag index"
    );
}

/// A field whose whole tag index was replaced since the scan — dropping and
/// recreating the field — must not have the stale deltas applied to it.
#[test]
fn a_message_for_a_replaced_tag_index_is_rejected() {
    let mut test = TestSpec::create(vec![Field::tag(c"tags", &[b"red"], 3, false)]);
    let mut entries = receive_all(&collect_tags_from(&test));
    entries[0].tag_index_unique_id = entries[0].tag_index_unique_id.wrapping_add(1);

    let err = apply_tag_entry(entries.remove(0), &mut test.write_guard()).unwrap_err();
    assert_eq!(
        err.to_string(),
        "the field's tag index is not the one the child scanned"
    );
}

/// The C tag-GC apply path rejecting the delta — the tag gone, or its posting list
/// replaced — surfaces as a parent-side error rather than being ignored.
#[test]
fn a_message_for_a_replaced_posting_list_is_rejected() {
    let mut test = TestSpec::create(vec![Field::tag(c"tags", &[b"red", b"blue"], 3, false)]);
    let mut entries = receive_all(&collect_tags_from(&test));
    assert_eq!(entries.len(), 2);

    // Stand in for `red`'s posting list having been replaced since the scan.
    let blue = entries[0].inverted_index_unique_id;
    let mut red = entries.remove(1);
    assert_eq!(&*red.tag, b"red");
    red.inverted_index_unique_id = blue;

    let err = apply_tag_entry(red, &mut test.write_guard()).unwrap_err();
    assert_eq!(
        err.to_string(),
        "the tag's posting list is not the one the child scanned"
    );
}

/// The child's stream can be cut short at any point if it dies mid-write, so
/// every prefix of a message has to come back as a codec error rather than a
/// partial apply.
#[test]
fn a_truncated_message_is_a_codec_error() {
    let test = TestSpec::create(vec![Field::tag(c"tags", &[b"red"], 3, false)]);
    let buf = collect_tags_from(&test);

    // Where the one message ends; the bytes after it are the stream end marker,
    // and a prefix that reaches this far is a whole message, not a truncated one.
    let message_end = {
        let mut cursor = Cursor::new(&buf);
        receive_tag_entry(&mut cursor).unwrap().unwrap();
        cursor.position() as usize
    };

    for len in 1..message_end {
        match receive_tag_entry(&mut Cursor::new(&buf[..len])) {
            Err(HandleError::Codec { .. }) => {}
            other => panic!("a {len}-byte prefix must be a codec error, got {other:?}"),
        }
    }
}

/// An empty stream is not a valid tag message: it is a truncated MessagePack
/// value, not the `None` end marker a child sends after a complete stream.
#[test]
fn an_empty_stream_is_a_codec_error() {
    let err = receive_tag_entry(&mut Cursor::new([])).unwrap_err();
    assert!(matches!(err, HandleError::Codec { .. }));
}

/// The tag-stream end marker is one complete MessagePack value, so it must not
/// consume following bytes.
#[test]
fn the_end_marker_leaves_following_bytes_unread() {
    let mut buf = Vec::new();
    Option::<fork_gc::tags::TagEntry<&[u8]>>::None
        .serialize(&mut rmp_serde::Serializer::new(&mut buf))
        .unwrap();
    let end_marker_len = buf.len();
    let following = [0x01, 0x02, 0x03];
    buf.extend_from_slice(&following);

    let mut cursor = Cursor::new(&buf);
    assert!(receive_tag_entry(&mut cursor).unwrap().is_none());
    assert_eq!(cursor.position(), end_marker_len as u64);
    assert_eq!(&buf[cursor.position() as usize..], following);
}
