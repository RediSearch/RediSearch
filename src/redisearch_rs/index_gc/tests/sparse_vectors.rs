/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for the sparse vector GC scanner: what the fork child writes, how the
//! parent applies it, and a full thread GC cycle.

// Every test builds a synthetic spec, which needs C functions miri cannot run.
#![cfg(not(miri))]

use std::ffi::CStr;
use std::io::Cursor;
use std::mem::{self, ManuallyDrop};

use deleted_docs::DeletedDocs;
use dict::{KeysDictType, MissingFieldDictType, OwnedDict};
use field_spec::{FieldSpecBuilder, FieldSpecType};
use index_gc::fork::wire::deserialize;
use index_gc::scanners::sparse_vectors::{
    SparseVectorEntry, SparseVectorsCursor, apply_sparse_vector_entry, collect_sparse_vectors,
    scan_sparse_vectors_after,
};
use index_gc::spec_access::SpecWriteAccess;
use index_gc::stats::GcStatsSink;
use index_gc::thread::{SpecLockAccess, run_cycle};
use index_gc::{GcApplyStats, GcError};
use index_spec::{IndexSpecReadGuard, IndexSpecWriteGuard};
use inverted_index::DocId;
use sparse_vector_index::{SparseMetric, SparseVectorIndex};

// Link both Rust-provided and C-provided symbols
extern crate redisearch_rs;
redis_mock::mock_or_stub_missing_redis_c_symbols!();

const DIM: u64 = 100;

/// A spec whose fields are sparse vector fields (or, for `None`, a TAG field
/// with no index), each owning a [`SparseVectorIndex`].
struct TestSpec {
    spec: ffi::IndexSpec,
    field_specs: Vec<ffi::FieldSpec>,
    _keys_dict: OwnedDict<KeysDictType>,
    _missing_dict: OwnedDict<MissingFieldDictType>,
}

impl TestSpec {
    /// `fields[i]` lists the `(doc_id, vector)` pairs of field `i`; `gone` are
    /// the deleted documents the thread GC consults.
    fn create(fields: &[Option<&[(DocId, &[(u32, f32)])]>], gone: &[DocId]) -> Self {
        let mut field_specs: Vec<ffi::FieldSpec> = fields
            .iter()
            .enumerate()
            .map(|(i, docs)| {
                let name: &'static CStr = [c"a", c"b", c"c"][i];
                let Some(docs) = docs else {
                    return FieldSpecBuilder::new(name)
                        .with_types(FieldSpecType::Tag.into())
                        .finish();
                };
                let mut fs = FieldSpecBuilder::new(name)
                    .with_types(FieldSpecType::SparseVector.into())
                    .finish();
                let mut index = SparseVectorIndex::new(DIM, SparseMetric::InnerProduct);
                for (doc_id, v) in *docs {
                    index.add(*doc_id, v).unwrap();
                }
                fs.__bindgen_anon_1.sparseVecOpts.sparseVecIndex =
                    Box::into_raw(Box::new(index)).cast();
                fs
            })
            .collect();

        // SAFETY: a zeroed IndexSpec is valid for what the scanners read; the
        // fields, dicts, terms trie and deleted set are set below.
        let mut spec: ffi::IndexSpec = unsafe { mem::zeroed() };
        spec.fields = field_specs.as_mut_ptr();
        spec.numFields = field_specs.len() as u16;
        // Applying subtracts from these counters.
        spec.stats.numRecords = 1_000_000;
        spec.stats.invertedSize = 1_000_000;
        let keys_dict = OwnedDict::<KeysDictType>::create();
        let missing_dict = OwnedDict::<MissingFieldDictType>::create();
        spec.keysDict = keys_dict.as_mut_ptr();
        spec.missing.indexes = missing_dict.as_mut_ptr();
        // SAFETY: `NewTrie` returns a fresh, valid, empty terms trie.
        spec.terms = unsafe { ffi::NewTrie(None, ffi::TrieSortMode_Trie_Sort_Lex) };
        spec.docs.deletedDocs =
            Box::into_raw(Box::new(DeletedDocs::from_ids(gone.iter().copied()))).cast();

        Self {
            spec,
            field_specs,
            _keys_dict: keys_dict,
            _missing_dict: missing_dict,
        }
    }

    fn index(&self, field: usize) -> &SparseVectorIndex {
        // SAFETY: `create` made every field it is asked about a sparse field.
        let ptr = unsafe {
            self.field_specs[field]
                .__bindgen_anon_1
                .sparseVecOpts
                .sparseVecIndex
        };
        // SAFETY: `create` stored a boxed index in every sparse field.
        unsafe { &*ptr.cast::<SparseVectorIndex>() }
    }

    /// Swap field `field`'s index for an empty one, as recreating it would.
    /// The old one is leaked: the tests only compare IDs afterwards.
    fn replace_index(&mut self, field: usize) {
        self.field_specs[field]
            .__bindgen_anon_1
            .sparseVecOpts
            .sparseVecIndex = Box::into_raw(Box::new(SparseVectorIndex::new(
            DIM,
            SparseMetric::InnerProduct,
        )))
        .cast();
    }

    fn read_guard(&self) -> ManuallyDrop<IndexSpecReadGuard<'_>> {
        // SAFETY: `spec` outlives the guard, and the single-threaded test does
        // not mutate it while the guard is held.
        unsafe { IndexSpecReadGuard::from_locked(&self.spec) }
    }

    fn write_guard(&mut self) -> ManuallyDrop<IndexSpecWriteGuard<'_>> {
        // SAFETY: the test owns the fixture exclusively.
        unsafe { IndexSpecWriteGuard::from_locked_mut(&mut self.spec) }
    }

    /// The fork child's output, decoded the way the parent decodes it.
    fn collect(&self, gone: &[DocId]) -> Vec<SparseVectorEntry> {
        let mut buf = Vec::new();
        collect_sparse_vectors(&mut buf, &self.read_guard(), |id: DocId| {
            !gone.contains(&id)
        })
        .unwrap();
        let mut cursor = Cursor::new(&buf);
        let mut entries = Vec::new();
        while let Some(entry) =
            deserialize::<Option<SparseVectorEntry>>(&mut cursor, "decoding sparse vector entry")
                .unwrap()
        {
            entries.push(entry);
        }
        assert_eq!(
            cursor.position(),
            buf.len() as u64,
            "bytes after the end marker"
        );
        entries
    }
}

impl SpecWriteAccess for TestSpec {
    fn with_write<T>(
        &mut self,
        apply: impl FnOnce(&mut IndexSpecWriteGuard<'_>) -> Result<T, GcError>,
    ) -> Result<T, GcError> {
        apply(&mut self.write_guard())
    }
}

impl SpecLockAccess for TestSpec {
    fn with_read<T>(
        &mut self,
        scan: impl FnOnce(&IndexSpecReadGuard<'_>) -> T,
    ) -> Result<T, GcError> {
        Ok(scan(&self.read_guard()))
    }
}

impl Drop for TestSpec {
    fn drop(&mut self) {
        // SAFETY: `create` made this trie and boxed this set; nothing else frees them.
        unsafe { ffi::TrieType_Free(self.spec.terms.cast()) };
        // SAFETY: as above.
        unsafe {
            drop(Box::from_raw(
                self.spec.docs.deletedDocs.cast::<DeletedDocs>(),
            ))
        };
        for fs in &self.field_specs {
            let is_sparse = fs.types() & ffi::FieldType_INDEXFLD_T_SPARSE_VECTOR != 0;
            // SAFETY: read only for sparse fields, whose active member it is.
            let index = if is_sparse {
                unsafe { fs.__bindgen_anon_1.sparseVecOpts.sparseVecIndex }
            } else {
                std::ptr::null_mut()
            };
            if !index.is_null() {
                // SAFETY: `create` or `replace_index` boxed this index.
                drop(unsafe { Box::from_raw(index.cast::<SparseVectorIndex>()) });
            }
            // SAFETY: `FieldSpecBuilder::new` made one owned name per field.
            unsafe { ffi::HiddenString_Free(fs.fieldName, true) };
        }
    }
}

#[derive(Default)]
struct Recorder(GcApplyStats);

impl GcStatsSink for Recorder {
    fn record(&mut self, stats: &GcApplyStats) {
        self.0.record(*stats);
    }
}

const DOCS: &[(DocId, &[(u32, f32)])] = &[
    (1, &[(1, 1.0), (2, 1.0)]),
    (2, &[(2, 1.0), (3, 1.0)]),
    (3, &[(1, 1.0)]),
];

#[test]
fn nothing_gone_writes_only_the_end_marker() {
    let test = TestSpec::create(&[None, Some(DOCS)], &[]);
    assert!(test.collect(&[]).is_empty());
}

#[test]
fn each_dirty_dimension_carries_its_routing_data() {
    let test = TestSpec::create(&[None, Some(DOCS)], &[]);
    let mut entries = test.collect(&[2]);
    entries.sort_by_key(|e| e.dim_id);
    let index = test.index(1);
    let got: Vec<_> = entries
        .iter()
        .map(|e| (e.field_index, e.index_unique_id, e.dim_id, e.list_unique_id))
        .collect();
    let expected: Vec<_> = [2, 3]
        .into_iter()
        .map(|dim| {
            let list = u32::from(index.get_dimension(dim).unwrap().unique_id());
            (1, index.unique_id(), dim, list)
        })
        .collect();
    assert_eq!(got, expected);
}

#[test]
fn applying_drops_an_emptied_dimension_and_reports_what_it_freed() {
    let mut test = TestSpec::create(&[Some(DOCS)], &[]);
    let memory_before = test.index(0).memory_usage();
    let mut total = GcApplyStats::default();
    for entry in test.collect(&[2]) {
        total.record(apply_sparse_vector_entry(entry, &mut test.write_guard()).unwrap());
    }
    let index = test.index(0);
    assert_eq!(index.dimension_ids_after(None), vec![1, 2]);
    assert_eq!(index.num_entries(), 3);
    assert_eq!(total.records_removed, 2);
    assert_eq!(
        memory_before + total.bytes_allocated,
        index.memory_usage() + total.bytes_collected
    );
}

#[test]
fn a_message_for_a_replaced_index_is_rejected() {
    let mut test = TestSpec::create(&[Some(DOCS)], &[]);
    let entry = test.collect(&[2]).remove(0);
    test.replace_index(0);
    assert!(matches!(
        apply_sparse_vector_entry(entry, &mut test.write_guard()),
        Err(GcError::TargetGone(_))
    ));
}

#[test]
fn a_message_for_a_field_that_is_not_sparse_is_rejected() {
    let mut test = TestSpec::create(&[None, Some(DOCS)], &[]);
    let mut entry = test.collect(&[2]).remove(0);
    entry.field_index = 0;
    assert!(matches!(
        apply_sparse_vector_entry(entry, &mut test.write_guard()),
        Err(GcError::TargetGone(_))
    ));
}

#[test]
fn the_thread_walk_resumes_after_the_last_dimension_it_visited() {
    let test = TestSpec::create(&[Some(DOCS), Some(DOCS)], &[]);
    let live = |id: DocId| id != 2;
    let mut cursor = SparseVectorsCursor::default();
    let mut found = Vec::new();
    let mut walked = Vec::new();
    // Stop after every dimension: 2 fields x 3 dimensions.
    while scan_sparse_vectors_after(&test.read_guard(), &mut cursor, live, || true, &mut found) {
        walked.extend(found.drain(..).map(|s| (s.field_index, s.dim_id)));
    }
    walked.extend(found.drain(..).map(|s| (s.field_index, s.dim_id)));
    // A closure cannot rule a range clean (see `DocLiveness::any_gone`), so every
    // dimension is copied; what matters is that each is visited once, in order.
    assert_eq!(walked, vec![(0, 1), (0, 2), (0, 3), (1, 1), (1, 2), (1, 3)]);
}

#[test]
fn a_thread_gc_cycle_collects_every_sparse_field() {
    let mut test = TestSpec::create(&[Some(DOCS), None, Some(DOCS)], &[1, 2]);
    let mut recorder = Recorder::default();
    run_cycle(&mut test, &mut recorder, true).unwrap();
    for field in [0, 2] {
        let index = test.index(field);
        assert_eq!(index.dimension_ids_after(None), vec![1], "field {field}");
        assert_eq!(
            index.num_entries(),
            1,
            "only doc 3 remains in field {field}"
        );
    }
    assert_eq!(recorder.0.records_removed, 2 * 4);
}
