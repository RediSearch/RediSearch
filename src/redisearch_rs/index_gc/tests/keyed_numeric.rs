/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for the MAP NUMERIC GC scanner: what the fork child writes, how the parent
//! applies it, and a full thread GC cycle.

// Every test builds a synthetic spec, which needs C functions miri cannot run.
#![cfg(not(miri))]

use std::ffi::CStr;
use std::io::Cursor;
use std::mem::{self, ManuallyDrop};

use deleted_docs::DeletedDocs;
use dict::{KeysDictType, MissingFieldDictType, OwnedDict};
use field_spec::{FieldSpecBuilder, FieldSpecType};
use index_gc::fork::wire::deserialize;
use index_gc::scanners::keyed_numeric::{
    KeyedTreeId, collect_keyed_numeric, handle_keyed_numeric_with,
};
use index_gc::scanners::numeric::NumericNodeDelta;
use index_gc::spec_access::SpecWriteAccess;
use index_gc::stats::GcStatsSink;
use index_gc::thread::{SpecLockAccess, run_cycle};
use index_gc::{GcApplyStats, GcError, HandleOutcome};
use index_spec::{IndexSpecReadGuard, IndexSpecWriteGuard};
use inverted_index::DocId;
use keyed_numeric_index::KeyedNumericIndex;

// Link both Rust-provided and C-provided symbols
extern crate redisearch_rs;
redis_mock::mock_or_stub_missing_redis_c_symbols!();

/// What `(doc_id, value)` pairs each key of a field holds.
type Key = (&'static [u8], &'static [(DocId, f64)]);

/// A spec whose fields are MAP NUMERIC fields (or, for `None`, a TAG field with no
/// index), each owning a [`KeyedNumericIndex`].
struct TestSpec {
    spec: ffi::IndexSpec,
    field_specs: Vec<ffi::FieldSpec>,
    _keys_dict: OwnedDict<KeysDictType>,
    _missing_dict: OwnedDict<MissingFieldDictType>,
    /// Indexes `replace_index` swapped out, kept so they are freed with the fixture.
    replaced: Vec<Box<KeyedNumericIndex>>,
}

fn filled_index(keys: &[Key]) -> Box<KeyedNumericIndex> {
    let mut index = Box::new(KeyedNumericIndex::new(false));
    for (key, pairs) in keys {
        for (doc_id, value) in *pairs {
            index.add(*doc_id, key, *value, false, 0);
        }
    }
    index
}

impl TestSpec {
    /// `fields[i]` lists the keys of field `i`; `gone` are the deleted documents the
    /// thread GC consults.
    fn create(fields: &[Option<&[Key]>], gone: &[DocId]) -> Self {
        let mut field_specs: Vec<ffi::FieldSpec> = fields
            .iter()
            .enumerate()
            .map(|(i, keys)| {
                let name: &'static CStr = [c"a", c"b", c"c"][i];
                let Some(keys) = keys else {
                    return FieldSpecBuilder::new(name)
                        .with_types(FieldSpecType::Tag.into())
                        .finish();
                };
                let mut fs = FieldSpecBuilder::new(name)
                    .with_types(FieldSpecType::MapNumeric.into())
                    .finish();
                fs.__bindgen_anon_1.mapNumericOpts.index = Box::into_raw(filled_index(keys)).cast();
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
            replaced: Vec::new(),
        }
    }

    fn index_mut(&mut self, field: usize) -> &mut KeyedNumericIndex {
        // SAFETY: `create` made every field it is asked about a MAP NUMERIC field.
        let ptr = unsafe {
            self.field_specs[field]
                .__bindgen_anon_1
                .mapNumericOpts
                .index
        };
        // SAFETY: `create` stored a boxed index in every such field, and the fixture
        // is borrowed exclusively.
        unsafe { &mut *ptr.cast::<KeyedNumericIndex>() }
    }

    fn index(&self, field: usize) -> &KeyedNumericIndex {
        // SAFETY: as in `index_mut`.
        let ptr = unsafe {
            self.field_specs[field]
                .__bindgen_anon_1
                .mapNumericOpts
                .index
        };
        // SAFETY: as in `index_mut`.
        unsafe { &*ptr.cast::<KeyedNumericIndex>() }
    }

    /// Swap field `field`'s index for one holding `keys`, as recreating the field would.
    fn replace_index(&mut self, field: usize, keys: &[Key]) {
        let fs = &mut self.field_specs[field];
        // SAFETY: `field` is a MAP NUMERIC field, so `mapNumericOpts` is the active member.
        let old = unsafe { fs.__bindgen_anon_1.mapNumericOpts.index };
        fs.__bindgen_anon_1.mapNumericOpts.index = Box::into_raw(filled_index(keys)).cast();
        // SAFETY: `create` or an earlier `replace_index` boxed `old`, and the field no
        // longer points at it.
        self.replaced
            .push(unsafe { Box::from_raw(old.cast::<KeyedNumericIndex>()) });
    }

    fn read_guard(&self) -> ManuallyDrop<IndexSpecReadGuard<'_>> {
        // SAFETY: `spec` outlives the guard, and the single-threaded test does not
        // mutate it while the guard is held.
        unsafe { IndexSpecReadGuard::from_locked(&self.spec) }
    }

    fn write_guard(&mut self) -> ManuallyDrop<IndexSpecWriteGuard<'_>> {
        // SAFETY: the test owns the fixture exclusively.
        unsafe { IndexSpecWriteGuard::from_locked_mut(&mut self.spec) }
    }

    /// The fork child's output.
    fn collect_bytes(&self, gone: &[DocId]) -> Vec<u8> {
        let mut buf = Vec::new();
        collect_keyed_numeric(&mut buf, &self.read_guard(), |id: DocId| {
            !gone.contains(&id)
        })
        .unwrap();
        buf
    }

    /// The fork child's output, decoded the way the parent decodes it: every tree's
    /// header with its node count.
    fn collect(&self, gone: &[DocId]) -> Vec<(KeyedTreeId, usize)> {
        let buf = self.collect_bytes(gone);
        let mut cursor = Cursor::new(&buf);
        let mut trees = Vec::new();
        while let Some(id) =
            deserialize::<Option<KeyedTreeId>>(&mut cursor, "decoding keyed tree").unwrap()
        {
            let mut nodes = 0;
            while deserialize::<Option<NumericNodeDelta>>(&mut cursor, "decoding node")
                .unwrap()
                .is_some()
            {
                nodes += 1;
            }
            trees.push((id, nodes));
        }
        assert_eq!(
            cursor.position(),
            buf.len() as u64,
            "bytes after the end marker"
        );
        trees
    }

    /// The parent's side for one key, flushing its statistics to the spec as the fork
    /// parent does.
    fn apply_next(
        &mut self,
        cursor: &mut Cursor<&[u8]>,
        stats: &mut GcApplyStats,
        compact: bool,
    ) -> Result<HandleOutcome, GcError> {
        handle_keyed_numeric_with(cursor, self, stats, compact, |guard, key_stats| {
            key_stats.apply_to_spec(guard);
        })
    }

    /// Add one pair and charge the spec's statistics as the indexer does.
    fn add_charged(&mut self, field: usize, doc_id: DocId, key: &[u8], value: f64) {
        let r = self.index_mut(field).add(doc_id, key, value, false, 0);
        let stats = &mut self.spec.stats;
        stats.invertedSize = stats
            .invertedSize
            .wrapping_add_signed(r.size_delta as isize);
        stats.numRecords = stats
            .numRecords
            .wrapping_add_signed(r.num_records_delta as isize);
        stats.totalInvertedIndexBlocks = stats
            .totalInvertedIndexBlocks
            .wrapping_add_signed(r.block_count_delta as isize);
    }

    /// The spec statistics a keyed field's entries are charged to.
    fn charged(&self) -> (usize, usize, usize) {
        let stats = &self.spec.stats;
        (
            stats.invertedSize,
            stats.numRecords,
            stats.totalInvertedIndexBlocks,
        )
    }

    /// The parent's side: apply a whole stream, returning the stats and the number of
    /// trees it held.
    fn apply_stream(&mut self, bytes: &[u8], compact: bool) -> (GcApplyStats, usize) {
        let mut cursor = Cursor::new(bytes);
        let mut stats = GcApplyStats::default();
        let mut trees = 0;
        while self.apply_next(&mut cursor, &mut stats, compact).unwrap() == HandleOutcome::Collected
        {
            trees += 1;
        }
        assert_eq!(
            cursor.position(),
            bytes.len() as u64,
            "the parent left stream bytes unread"
        );
        (stats, trees)
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
            let is_map = fs.types() & ffi::FieldType_INDEXFLD_T_MAP_NUMERIC != 0;
            let index = if is_map {
                // SAFETY: read only for MAP NUMERIC fields, whose active member it is.
                unsafe { fs.__bindgen_anon_1.mapNumericOpts.index }
            } else {
                std::ptr::null_mut()
            };
            if !index.is_null() {
                // SAFETY: `create` or `replace_index` boxed this index.
                drop(unsafe { Box::from_raw(index.cast::<KeyedNumericIndex>()) });
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

/// `x` holds documents 1 and 2; `y` holds 3 and 4; `z` holds 5.
const KEYS: &[Key] = &[
    (b"x", &[(1, 10.0), (2, 20.0)]),
    (b"y", &[(3, 1.0), (4, 2.0)]),
    (b"z", &[(5, 7.0)]),
];

fn entries(spec: &TestSpec, field: usize, key: &[u8]) -> Option<usize> {
    spec.index(field).get(key).map(|tree| tree.num_entries())
}

fn assert_memory_exact(spec: &TestSpec, field: usize) {
    let index = spec.index(field);
    assert_eq!(index.memory_usage(), index.computed_memory_usage());
}

#[test]
fn nothing_gone_writes_only_the_end_marker() {
    let test = TestSpec::create(&[None, Some(KEYS)], &[]);
    assert!(test.collect(&[]).is_empty());
}

#[test]
fn only_keys_with_garbage_are_sent_with_their_routing_data() {
    let test = TestSpec::create(&[None, Some(KEYS)], &[]);
    let trees = test.collect(&[1, 2]);
    let index = test.index(1);
    let x = index.get(b"x").unwrap();
    assert_eq!(trees.len(), 1);
    let (id, nodes) = &trees[0];
    assert_eq!(
        *id,
        KeyedTreeId {
            field_index: 1,
            index_unique_id: u32::from(index.unique_id()),
            key: b"x".to_vec(),
            tree_unique_id: u32::from(x.unique_id()),
        }
    );
    assert!(*nodes > 0);
}

#[test]
fn deltas_apply_per_key_and_leave_the_other_keys_alone() {
    let mut test = TestSpec::create(&[Some(KEYS)], &[]);
    let bytes = test.collect_bytes(&[2, 3]);
    let (stats, trees) = test.apply_stream(&bytes, true);
    assert_eq!(trees, 2);
    assert_eq!(stats.records_removed, 2);
    assert_eq!(entries(&test, 0, b"x"), Some(1));
    assert_eq!(entries(&test, 0, b"y"), Some(1));
    assert_eq!(entries(&test, 0, b"z"), Some(1));
    assert_memory_exact(&test, 0);
}

#[test]
fn an_emptied_key_is_removed_and_the_memory_accounting_stays_exact() {
    let mut test = TestSpec::create(&[Some(KEYS)], &[]);
    let memory_before = test.index(0).memory_usage();
    let bytes = test.collect_bytes(&[3, 4]);
    let (stats, _) = test.apply_stream(&bytes, true);
    let index = test.index(0);
    assert!(index.get(b"y").is_none());
    assert_eq!(index.num_keys(), 2);
    assert_eq!(stats.records_removed, 2);
    assert!(index.memory_usage() < memory_before);
    assert_memory_exact(&test, 0);
}

#[test]
fn an_emptied_key_is_removed_even_without_compaction() {
    let mut test = TestSpec::create(&[Some(KEYS)], &[]);
    let bytes = test.collect_bytes(&[5]);
    test.apply_stream(&bytes, false);
    assert!(test.index(0).get(b"z").is_none());
    assert_memory_exact(&test, 0);
}

#[test]
fn a_stream_for_a_key_gone_since_the_scan_is_skipped_and_fully_read() {
    let mut test = TestSpec::create(&[Some(KEYS)], &[]);
    let bytes = test.collect_bytes(&[1, 2, 5]);
    let (first, _) = test.apply_stream(&bytes, true);
    assert_eq!(first.records_removed, 3);
    assert!(test.index(0).get(b"x").is_none());
    // The same stream again: both keys it names are gone.
    let (second, trees) = test.apply_stream(&bytes, true);
    assert_eq!(trees, 2);
    assert_eq!(second, GcApplyStats::default());
}

#[test]
fn a_stream_for_a_key_recreated_since_the_scan_is_skipped() {
    let mut test = TestSpec::create(&[Some(KEYS)], &[]);
    let bytes = test.collect_bytes(&[1, 2]);
    test.apply_stream(&bytes, true);
    assert!(test.index(0).get(b"x").is_none());

    // The key comes back with a tree of its own, holding what the old stream names.
    let index = test.index_mut(0);
    index.add(6, b"x", 10.0, false, 0);
    index.add(7, b"x", 20.0, false, 0);

    let (stats, trees) = test.apply_stream(&bytes, true);
    assert_eq!(trees, 1);
    assert_eq!(stats, GcApplyStats::default());
    assert_eq!(entries(&test, 0, b"x"), Some(2));
    assert_memory_exact(&test, 0);
}

#[test]
fn a_stream_for_a_replaced_index_is_skipped() {
    let mut test = TestSpec::create(&[Some(KEYS)], &[]);
    let bytes = test.collect_bytes(&[1, 2]);
    test.replace_index(0, KEYS);
    let (stats, trees) = test.apply_stream(&bytes, true);
    assert_eq!(trees, 1);
    assert_eq!(stats, GcApplyStats::default());
    assert_eq!(entries(&test, 0, b"x"), Some(2));
}

#[test]
fn a_stream_for_a_field_that_is_not_keyed_is_skipped() {
    let mut test = TestSpec::create(&[None, Some(KEYS)], &[]);
    let bytes = test.collect_bytes(&[1, 2]);
    // Field 1 becomes the TAG-like field 0's position: re-encode with field 0.
    let mut cursor = Cursor::new(&bytes);
    let mut id = deserialize::<Option<KeyedTreeId>>(&mut cursor, "id")
        .unwrap()
        .unwrap();
    id.field_index = 0;
    let mut forged = Vec::new();
    index_gc::fork::wire::serialize(&mut forged, Some(&id)).unwrap();
    forged.extend_from_slice(&bytes[cursor.position() as usize..]);
    let (stats, _) = test.apply_stream(&forged, true);
    assert_eq!(stats, GcApplyStats::default());
    assert_eq!(entries(&test, 1, b"x"), Some(2));
}

#[test]
fn a_thread_gc_cycle_collects_every_keyed_field_and_drops_emptied_keys() {
    let mut test = TestSpec::create(&[Some(KEYS), None, Some(KEYS)], &[1, 2, 3]);
    let mut recorder = Recorder::default();
    run_cycle(&mut test, &mut recorder, true).unwrap();
    for field in [0, 2] {
        assert!(test.index(field).get(b"x").is_none(), "field {field}");
        assert_eq!(entries(&test, field, b"y"), Some(1), "field {field}");
        assert_eq!(entries(&test, field, b"z"), Some(1), "field {field}");
        assert_memory_exact(&test, field);
    }
    assert_eq!(recorder.0.records_removed, 2 * 3);
}

/// Enough distinct values that the key's tree splits.
const MANY: DocId = 2000;

/// A spec holding the key `keep` and, after the baseline was taken, the key `big` with
/// [`MANY`] documents, all charged to the spec's statistics.
fn spec_with_a_split_key() -> (TestSpec, (usize, usize, usize)) {
    let mut test = TestSpec::create(&[Some(&[])], &[]);
    test.add_charged(0, MANY + 1, b"keep", 1.0);
    let baseline = test.charged();
    for doc_id in 1..=MANY {
        test.add_charged(0, doc_id, b"big", doc_id as f64);
    }
    assert!(test.index(0).get(b"big").unwrap().num_ranges() > 1);
    assert_ne!(test.charged(), baseline);
    (test, baseline)
}

fn big_docs() -> Vec<DocId> {
    (1..=MANY).collect()
}

fn fork_removes_a_split_key_exactly(compact: bool) {
    let (mut test, baseline) = spec_with_a_split_key();
    let bytes = test.collect_bytes(&big_docs());
    test.apply_stream(&bytes, compact);
    assert!(test.index(0).get(b"big").is_none());
    assert_eq!(test.charged(), baseline);
    assert_memory_exact(&test, 0);
}

#[test]
fn removing_a_split_key_returns_the_spec_statistics_to_the_baseline() {
    fork_removes_a_split_key_exactly(true);
}

#[test]
fn removing_a_split_key_without_compaction_returns_the_spec_statistics_to_the_baseline() {
    fork_removes_a_split_key_exactly(false);
}

#[test]
fn the_thread_gc_returns_the_spec_statistics_to_the_baseline_too() {
    for compact in [true, false] {
        let (mut test, baseline) = spec_with_a_split_key();
        test.spec.docs.deletedDocs =
            Box::into_raw(Box::new(DeletedDocs::from_ids(big_docs()))).cast();
        let mut recorder = Recorder::default();
        run_cycle(&mut test, &mut recorder, compact).unwrap();
        assert!(test.index(0).get(b"big").is_none());
        assert_eq!(test.charged(), baseline, "compact {compact}");
        assert_memory_exact(&test, 0);
    }
}

#[test]
fn a_stale_multi_node_stream_is_drained_and_the_next_key_still_applies() {
    let (mut test, _) = spec_with_a_split_key();
    test.add_charged(0, MANY + 2, b"y", 3.0);
    test.add_charged(0, MANY + 3, b"y", 4.0);
    let mut gone = big_docs();
    gone.push(MANY + 2);
    let bytes = test.collect_bytes(&gone);
    // `big` is recreated with a tree of its own after the scan.
    let old = test.index(0).get(b"big").unwrap().unique_id();
    test.index_mut(0).update_tree(b"big", old, |tree| {
        numeric_range_tree::test_utils::gc_all_ranges(tree, &|_| false);
    });
    assert!(test.index_mut(0).remove_key_if_empty(b"big", old));
    test.add_charged(0, MANY + 10, b"big", 1.0);

    let (_, trees) = test.apply_stream(&bytes, true);
    assert_eq!(trees, 2);
    assert_eq!(entries(&test, 0, b"big"), Some(1));
    assert_eq!(entries(&test, 0, b"y"), Some(1));
}

#[test]
fn a_stream_cut_after_a_header_is_a_codec_error() {
    let (test, _) = spec_with_a_split_key();
    let bytes = test.collect_bytes(&big_docs());
    let mut cursor = Cursor::new(bytes.as_slice());
    deserialize::<Option<KeyedTreeId>>(&mut cursor, "header").unwrap();
    let header_end = cursor.position() as usize;
    for cut in [header_end, header_end + 3] {
        let mut test = TestSpec::create(&[Some(&[])], &[]);
        let mut cursor = Cursor::new(&bytes[..cut]);
        let error = test
            .apply_next(&mut cursor, &mut GcApplyStats::default(), true)
            .unwrap_err();
        assert!(
            matches!(error, GcError::Codec { .. }),
            "cut at {cut}: {error}"
        );
    }
}

#[test]
fn a_cycle_takes_the_keyed_holds_only_on_a_spec_with_a_keyed_field() {
    let mut recorder = Recorder::default();
    let without = run_cycle(&mut TestSpec::create(&[None], &[1]), &mut recorder, true).unwrap();
    let with = run_cycle(
        &mut TestSpec::create(&[Some(KEYS)], &[1]),
        &mut recorder,
        true,
    )
    .unwrap();
    // Reads: deleted set, terms, numeric list, tags, missing fields, existing docs.
    assert_eq!(without.read_locks, 6);
    // Writes: the prune.
    assert_eq!(without.write_locks, 1);
    // A keyed field adds listing its keys, one snapshot chunk and the split check, then
    // one hold to apply its delta and one to finish its tree.
    assert_eq!(with.read_locks, without.read_locks + 3);
    assert_eq!(with.write_locks, without.write_locks + 2);
}
