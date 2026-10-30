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
use std::io::{self, Cursor, Read, Write};
use std::mem::{self, ManuallyDrop};

use field_spec::{FieldSpecBuilder, FieldSpecType, FieldSpecTypes};
use fork_gc::HandleError;
use fork_gc::tags::{
    TagEntry, TagError, TagFieldHeader, apply_tag_entry, collect_tags, receive_field_header,
    receive_tag_entry,
};
use index_result::RSIndexResult;
use index_spec::{IndexSpecReadGuard, IndexSpecWriteGuard};
use inverted_index::reader::IndexReader;
use inverted_index::{DocId, opaque::InvertedIndex};
use lending_iterator::LendingIterator;
use serde::Serialize as _;
use trie_rs::TrieMapOpaque;

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
        // SAFETY: this fixture owns the C tag index and its Rust values trie.
        let values = unsafe { &*self.as_ref().values.cast::<TrieMapOpaque>() };
        let ii = values.find(tag)?;
        // SAFETY: tag postings are live opaque Rust inverted indexes owned by the trie.
        Some(unsafe { ii.cast::<InvertedIndex>().as_ref() })
    }

    fn posting_list_mut(&mut self, tag: &[u8]) -> &mut InvertedIndex {
        // SAFETY: the exclusive fixture borrow excludes other accesses to its postings.
        let values = unsafe { &*self.as_ref().values.cast::<TrieMapOpaque>() };
        let ii = values.find(tag).unwrap();
        // SAFETY: the trie owns the live posting list and this fixture is exclusively borrowed.
        unsafe { &mut *ii.cast::<InvertedIndex>().as_ptr() }
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
/// Documents are absent until [`TestSpec::set_live_docs`] installs their metadata.
struct TestSpec {
    spec: ffi::IndexSpec,
    // Keeps the heap buffer `spec.fields` points at alive, and owns the
    // `HiddenString` each field name/path points at (freed in `Drop`).
    field_specs: Vec<ffi::FieldSpec>,
    // Owns the C tag indexes the field specs point at.
    tag_indexes: Vec<TestTagIndex>,
    documents: Vec<ffi::RSDocumentMetadata>,
    buckets: Vec<ffi::DMDChain>,
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
            documents: Vec::new(),
            buckets: Vec::new(),
        }
    }

    fn set_live_docs(&mut self, ids: impl IntoIterator<Item = DocId>) {
        self.documents = ids
            .into_iter()
            .map(|id| {
                // SAFETY: zero is valid for the scalar fields and nullable pointers;
                // DocTable_Exists only reads id, flags and nextInChain.
                let mut document: ffi::RSDocumentMetadata = unsafe { mem::zeroed() };
                document.id = id;
                document
            })
            .collect();
        let max_id = self.documents.iter().map(|d| d.id).max().unwrap_or(0);
        self.buckets = (0..=max_id)
            .map(|_| ffi::DMDChain {
                root: std::ptr::null_mut(),
            })
            .collect();
        for document in &mut self.documents {
            self.buckets[document.id as usize].root = document;
        }
        self.spec.docs.maxDocId = max_id;
        self.spec.docs.maxSize = max_id + 1;
        self.spec.docs.cap = self.buckets.len();
        self.spec.docs.buckets = self.buckets.as_mut_ptr();
    }

    fn append(&mut self, id: DocId) {
        let ii = self.tag_indexes[0].posting_list_mut(b"red");
        inverted_index::ii_dispatch!(
            ii,
            add_record,
            &RSIndexResult::build_virt().doc_id(id).build()
        )
        .unwrap();
    }

    fn fill_to_blocks(&mut self, blocks: usize) -> DocId {
        let mut last = *self.ids().last().unwrap();
        while self.posting_list(0, b"red").unwrap().number_of_blocks() < blocks {
            last += 1;
            self.append(last);
        }
        last
    }

    fn ids(&self) -> Vec<DocId> {
        fn read_ids<'a>(mut reader: impl IndexReader<'a>) -> Vec<DocId> {
            let mut result = RSIndexResult::build_virt().build();
            let mut ids = Vec::new();
            while reader.next_record(&mut result).unwrap() {
                ids.push(result.doc_id);
            }
            ids
        }
        match self.posting_list(0, b"red") {
            Some(InvertedIndex::DocIdsOnly(ii)) => read_ids(ii.reader()),
            Some(InvertedIndex::RawDocIdsOnly(ii)) => read_ids(ii.reader()),
            None => Vec::new(),
            _ => panic!("expected a document-ID posting list"),
        }
    }

    fn scan(&self) -> (TagFieldHeader, TagEntry) {
        let mut entries = receive_all(&collect_tags_from(self));
        assert_eq!(entries.len(), 1);
        entries.remove(0)
    }

    fn apply_and_check(
        &mut self,
        (field, entry): (TagFieldHeader, TagEntry),
        expected: Vec<DocId>,
        denied: u64,
    ) {
        let ii = self.posting_list(0, b"red").unwrap();
        let (bytes_before, blocks_before, docs_before) =
            (ii.memory_usage(), ii.number_of_blocks(), ii.unique_docs());
        let stats = apply_tag_entry(&field, entry, &mut self.write_guard()).unwrap();
        assert_eq!(self.ids(), expected);
        let (bytes_after, blocks_after) = self
            .posting_list(0, b"red")
            .map_or((0, 0), |ii| (ii.memory_usage(), ii.number_of_blocks()));
        assert_eq!(stats.records_removed, docs_before as usize - expected.len());
        assert_eq!(
            bytes_before + stats.bytes_allocated,
            bytes_after + stats.bytes_collected
        );
        assert_eq!(
            stats.block_count_delta,
            blocks_after as i64 - blocks_before as i64
        );
        assert_eq!(stats.blocks_denied, denied);
    }

    /// Borrow the spec as a read guard, as the collect path receives it.
    const fn read_guard(&self) -> ManuallyDrop<IndexSpecReadGuard<'_>> {
        // SAFETY: `spec` is a valid IndexSpec that outlives the guard, and this
        // single-threaded test never mutates it while the guard is held.
        unsafe { IndexSpecReadGuard::from_locked(&self.spec) }
    }

    /// Borrow the spec as a write guard, as the apply path receives it.
    const fn write_guard(&mut self) -> ManuallyDrop<IndexSpecWriteGuard<'_>> {
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
fn receive_all(buf: &[u8]) -> Vec<(TagFieldHeader, TagEntry)> {
    let mut cursor = Cursor::new(buf);
    let mut entries = Vec::new();
    while let Some(field) = receive_field_header(&mut cursor).unwrap() {
        while let Some(entry) = receive_tag_entry(&mut cursor).unwrap() {
            entries.push((field.clone(), entry));
        }
    }
    assert_eq!(
        cursor.position(),
        buf.len() as u64,
        "trailing bytes after the end marker"
    );
    entries
}

#[test]
fn fields_without_collectable_tag_indexes_write_only_the_end_marker() {
    for fields in [
        vec![],
        vec![
            Field::other(c"n", FieldSpecType::Numeric.into()),
            Field::other(c"t", FieldSpecType::Fulltext.into()),
        ],
        vec![Field::tag_without_index(c"tags")],
        vec![Field::disk_tag(c"tags")],
        vec![Field::tag(c"tags", &[], 0, false)],
    ] {
        let test = TestSpec::create(fields);
        assert!(receive_all(&collect_tags_from(&test)).is_empty());
    }
}

#[test]
fn dirty_tags_share_their_fields_routing_data() {
    let test = TestSpec::create(vec![
        Field::other(c"n", FieldSpecType::Numeric.into()),
        Field::tag(c"colours", &[b"red", b"blue"], 3, false),
        Field::tag(c"sizes", &[b"xl"], 3, false),
    ]);

    let colours = test.tag_index(0).uniqueId;
    let sizes = test.tag_index(1).uniqueId;

    let bytes = collect_tags_from(&test);
    let mut reader = Cursor::new(&bytes);
    for (name, unique_id, tags) in [
        (
            b"colours".as_slice(),
            colours,
            vec![b"blue".as_slice(), b"red"],
        ),
        (b"sizes".as_slice(), sizes, vec![b"xl".as_slice()]),
    ] {
        let field = receive_field_header(&mut reader).unwrap().unwrap();
        assert_eq!(&*field.field_name, name);
        assert_eq!(field.tag_index_unique_id, unique_id);
        for tag in tags {
            let entry = receive_tag_entry(&mut reader).unwrap().unwrap();
            assert_eq!(&*entry.tag, tag);
        }
        assert!(receive_tag_entry(&mut reader).unwrap().is_none());
    }
    assert!(receive_field_header(&mut reader).unwrap().is_none());
    assert_eq!(reader.position(), bytes.len() as u64);
}

/// `INDEXEMPTY` indexes the empty string, distinct from either end marker.
#[test]
fn the_empty_tag_round_trips() {
    let test = TestSpec::create(vec![Field::tag(c"tags", &[b""], 3, false)]);

    let entries = receive_all(&collect_tags_from(&test));
    assert_eq!(entries.len(), 1);
    assert_eq!(&*entries[0].0.field_name, b"tags");
    assert_eq!(entries[0].0.tag_index_unique_id, test.tag_index(0).uniqueId);
    assert!(entries[0].1.tag.is_empty());
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
    assert_eq!(entries[0].1.inverted_index_unique_id, u32::from(expected));
}

/// A tag that loses its last document is dropped, and what that freed is
/// reported back so the caller can subtract it from the spec totals.
#[test]
fn applying_a_delta_drops_an_emptied_tag_and_reports_what_it_freed() {
    let mut test = TestSpec::create(vec![Field::tag(c"tags", &[b"red"], 3, false)]);

    let mut entries = receive_all(&collect_tags_from(&test));
    assert_eq!(entries.len(), 1);

    let (field, entry) = entries.remove(0);
    let stats = apply_tag_entry(&field, entry, &mut test.write_guard()).unwrap();

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
    entries[0].0.field_name = Box::from(&b"other"[..]);

    let (field, entry) = entries.remove(0);
    let err = apply_tag_entry(&field, entry, &mut test.write_guard()).unwrap_err();
    assert!(matches!(err, HandleError::Custom(TagError { .. })));
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

    let (field, entry) = entries.remove(0);
    let err = apply_tag_entry(&field, entry, &mut test.write_guard()).unwrap_err();
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
    entries[0].0.tag_index_unique_id = entries[0].0.tag_index_unique_id.wrapping_add(1);

    let (field, entry) = entries.remove(0);
    let err = apply_tag_entry(&field, entry, &mut test.write_guard()).unwrap_err();
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
    let blue = entries[0].1.inverted_index_unique_id;
    let (field, mut red) = entries.remove(1);
    assert_eq!(&*red.tag, b"red");
    red.inverted_index_unique_id = blue;

    let err = apply_tag_entry(&field, red, &mut test.write_guard()).unwrap_err();
    assert_eq!(
        err.to_string(),
        "the tag's posting list is not the one the child scanned"
    );
}

/// A child that dies mid-stream must be reported as a codec error, including
/// when it omits a field or scanner terminator.
#[test]
fn a_truncated_message_is_a_codec_error() {
    let test = TestSpec::create(vec![Field::tag(c"tags", &[b"red"], 3, false)]);
    let buf = collect_tags_from(&test);

    for len in 0..buf.len() {
        let mut reader = Cursor::new(&buf[..len]);
        let mut decode = || -> Result<(), HandleError<TagError>> {
            while receive_field_header(&mut reader)?.is_some() {
                while receive_tag_entry(&mut reader)?.is_some() {}
            }
            Ok(())
        };
        assert!(
            matches!(decode(), Err(HandleError::Codec { .. })),
            "a {len}-byte prefix must be a codec error"
        );
    }
}

#[test]
fn an_empty_stream_or_delta_frame_is_a_codec_error() {
    for bytes in [Vec::new(), vec![0; size_of::<usize>()]] {
        let err = receive_tag_entry(&mut Cursor::new(bytes)).unwrap_err();
        assert!(matches!(err, HandleError::Codec { .. }));
    }
}

/// The next scanner's bytes must remain unread.
#[test]
fn the_end_marker_leaves_following_bytes_unread() {
    let test = TestSpec::create(vec![Field::tag(c"tags", &[b"red", b"blue"], 3, false)]);
    let mut buf = collect_tags_from(&test);
    let stream_len = buf.len();
    let following = [0x01, 0x02, 0x03];
    buf.extend_from_slice(&following);

    let mut cursor = Cursor::new(&buf);
    while receive_field_header(&mut cursor).unwrap().is_some() {
        while receive_tag_entry(&mut cursor).unwrap().is_some() {}
    }
    assert_eq!(cursor.position(), stream_len as u64);
    assert_eq!(&buf[cursor.position() as usize..], following);
}

#[test]
fn a_message_for_a_non_tag_field_is_rejected_before_union_access() {
    for ty in [
        FieldSpecType::Fulltext,
        FieldSpecType::Numeric,
        FieldSpecType::Geo,
        FieldSpecType::Vector,
        FieldSpecType::Geometry,
    ] {
        let mut other = Field::other(c"other", ty.into());
        // Non-TAG union storage must never be interpreted as a TagIndex pointer.
        other.spec.__bindgen_anon_1.tagOpts.tagIndex = std::ptr::without_provenance_mut(1);
        let mut test = TestSpec::create(vec![Field::tag(c"tags", &[b"red"], 3, false), other]);
        let (mut field, entry) = test.scan();
        field.field_name = Box::from(&b"other"[..]);
        let error = apply_tag_entry(&field, entry, &mut test.write_guard()).unwrap_err();
        assert_eq!(
            error.to_string(),
            "the field no longer has an in-memory tag index"
        );
        assert_eq!(test.ids(), vec![1, 2, 3]);
    }
}

#[derive(Default)]
struct CountingWriter {
    bytes: Vec<u8>,
    calls: usize,
}
impl Write for CountingWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.calls += 1;
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
struct CountingReader<'a> {
    bytes: &'a [u8],
    calls: usize,
}
impl Read for CountingReader<'_> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.calls += 1;
        self.bytes.read(bytes)
    }
}

#[test]
fn routing_bytes_use_bulk_io_and_preserve_binary_values() {
    let test = TestSpec::create(vec![Field::tag(c"tags", &[b"red"], 3, false)]);
    let mut counts = Vec::new();
    for len in [16, 4096] {
        let (mut field, mut entry) = test.scan();
        let bytes: Box<[u8]> = (0..len).map(|i| (i % 256) as u8).collect();
        field.field_name = bytes.clone();
        entry.tag = bytes.clone();
        let mut writer = CountingWriter::default();
        Some(field)
            .serialize(&mut rmp_serde::Serializer::new(&mut writer))
            .unwrap();
        let header_writes = writer.calls;
        entry.encode(&mut writer).unwrap();
        assert_eq!(writer.calls - header_writes, 2);
        let mut reader = CountingReader {
            bytes: &writer.bytes,
            calls: 0,
        };
        let field = receive_field_header(&mut reader).unwrap().unwrap();
        let header_reads = reader.calls;
        let decoded = receive_tag_entry(&mut reader).unwrap().unwrap();
        assert_eq!(reader.calls - header_reads, 2);
        assert_eq!(field.field_name, bytes);
        assert_eq!(decoded.tag, bytes);
        assert!(reader.bytes.is_empty());
        counts.push((writer.calls, reader.calls));
    }
    assert_eq!(
        counts[0].0, counts[1].0,
        "writes must not scale with tag length"
    );
    assert!(
        counts[1].1 < 64,
        "binary payloads must be read in chunks, not byte by byte"
    );
}

#[test]
fn live_postings_produce_no_delta() {
    let mut test = TestSpec::create(vec![Field::tag(c"tags", &[b"red"], 3, false)]);
    test.set_live_docs(1..=3);
    assert!(receive_all(&collect_tags_from(&test)).is_empty());
}

#[test]
fn deletion_after_scan_waits_for_the_next_cycle() {
    let mut test = TestSpec::create(vec![Field::tag(c"tags", &[b"red"], 2, false)]);
    test.set_live_docs([2]);
    let entry = test.scan();
    test.set_live_docs([]);
    test.apply_and_check(entry, vec![2], 0);
    let entry = test.scan();
    test.apply_and_check(entry, vec![], 0);
}

#[test]
fn removing_all_blocks_retains_the_last_block_if_appended_to() {
    let mut test = TestSpec::create(vec![Field::tag(c"tags", &[b"red"], 1, false)]);
    let last = test.fill_to_blocks(2);
    let entry = test.scan();
    test.append(last + 1);
    test.apply_and_check(entry, vec![last, last + 1], 1);
}

#[test]
fn removing_middle_block_and_repairing_first_preserves_appended_last() {
    let mut test = TestSpec::create(vec![Field::tag(c"tags", &[b"red"], 1, false)]);
    let middle = test.fill_to_blocks(2);
    let last = test.fill_to_blocks(3);
    test.append(last + 1);
    test.set_live_docs((2..middle).chain([last]));
    let entry = test.scan();
    test.append(last + 2);
    test.apply_and_check(entry, (2..middle).chain(last..=last + 2).collect(), 1);
}

#[test]
fn removing_middle_block_preserves_new_blocks() {
    let mut test = TestSpec::create(vec![Field::tag(c"tags", &[b"red"], 1, false)]);
    let middle = test.fill_to_blocks(2);
    let last = test.fill_to_blocks(3);
    test.set_live_docs((1..middle).chain([last]));
    let entry = test.scan();
    let new_last = test.fill_to_blocks(4);
    test.apply_and_check(entry, (1..middle).chain(last..=new_last).collect(), 1);
}

#[test]
fn emptied_tags_remove_suffixes_but_empty_tags_have_none() {
    for tag in [b"red".as_slice(), b""] {
        let mut test = TestSpec::create(vec![Field::tag(c"tags", &[tag], 3, true)]);
        let (field, entry) = test.scan();
        apply_tag_entry(&field, entry, &mut test.write_guard()).unwrap();
        assert!(test.posting_list(0, tag).is_none());
        // SAFETY: the fixture owns the suffix trie for this field.
        let suffix = unsafe { &*test.tag_index(0).suffix.cast::<TrieMapOpaque>() };
        assert!(suffix.0.lending_iter().next().is_none());
    }
}

#[test]
fn missing_tag_and_disk_backed_parent_are_rejected() {
    let mut test = TestSpec::create(vec![Field::tag(c"tags", &[b"red"], 3, false)]);
    let (field, mut entry) = test.scan();
    entry.tag = Box::from(&b"absent"[..]);
    assert_eq!(
        apply_tag_entry(&field, entry, &mut test.write_guard())
            .unwrap_err()
            .to_string(),
        "the tag was removed before the delta could be applied"
    );
    let (field, entry) = test.scan();
    let mut disk = TestSpec::create(vec![Field::disk_tag(c"tags")]);
    assert_eq!(
        apply_tag_entry(&field, entry, &mut disk.write_guard())
            .unwrap_err()
            .to_string(),
        "the field no longer has an in-memory tag index"
    );
}

#[test]
fn failed_writes_are_propagated() {
    let test = TestSpec::create(vec![Field::tag(c"tags", &[b"red"], 3, false)]);
    for len in 0..collect_tags_from(&test).len() {
        let mut bytes = vec![0; len];
        assert!(collect_tags(&mut bytes.as_mut_slice(), &test.read_guard()).is_err());
    }
}
