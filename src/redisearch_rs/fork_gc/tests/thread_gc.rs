/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#![cfg(not(miri))] // The fixture calls C trie and dict functions.

use std::{
    ffi::{CStr, CString, c_char},
    mem,
    ptr::{self, NonNull},
};

use deleted_docs::{DeletedDocs, RoaringTreemap};
use dict::{KeysDictType, MissingFieldDictType, OwnedDict};
use ffi::IndexFlags_Index_DocIdsOnly;
use field_spec::{FieldSpecBuilder, FieldSpecType};
use fork_gc::{
    GcApplyStats, HandleError,
    stats::GcStatsSink,
    thread_gc::{
        DELTAS_PER_APPLY, DeletedIds, KEYS_PER_HOLD, LockReport, SpecLockAccess, run_cycle,
    },
    util::SpecWriteAccess,
};
use hidden_string::OwnedHiddenString;
use index_result::RSIndexResult;
use index_spec::{IndexSpecReadGuard, IndexSpecWriteGuard};
use inverted_index::opaque::InvertedIndex as OpaqueInvertedIndex;
use inverted_index::{InvertedIndex, doc_ids_only::DocIdsOnly};
use numeric_range_tree::NumericRangeTree;
use numeric_range_tree::test_utils::{build_single_leaf_tree, build_tree, scan_all_node_deltas};

// Link both Rust-provided and C-provided symbols
extern crate redisearch_rs;
// Provide Redis allocator shims so the C dict/trie functions can allocate memory.
redis_mock::mock_or_stub_missing_redis_c_symbols!();

/// A synthetic spec with terms and numeric fields whose docs are all deleted.
///
/// The doc table is left zeroed and every ID the fixture may index is in its
/// deleted set, so every populated index has GC work. Locks are simulated: the fixture has
/// exclusive access, counts lock attempts, and can fail one of them or append
/// a record just before a write lock, as a concurrent writer would.
struct TestSpec {
    spec: ffi::IndexSpec,
    keys_dict: OwnedDict<KeysDictType>,
    _missing_dict: OwnedDict<MissingFieldDictType>,
    // Keeps the heap buffer `spec.fields` points at alive; names freed in `Drop`.
    field_specs: Vec<ffi::FieldSpec>,
    #[expect(
        clippy::vec_box,
        reason = "the field specs retain pointers to individually pinned tree allocations"
    )]
    trees: Vec<Box<NumericRangeTree>>,
    lock_attempts: usize,
    /// Set by the first read lock; the concurrent-writer hooks below fire only on a
    /// write lock that follows a scan.
    scanned: bool,
    fail_lock_on_attempt: Option<usize>,
    append_before_write: Option<(&'static [u8], u64)>,
    remove_before_write: Option<&'static [u8]>,
    /// `(tree, doc id, max depth with a range)`: add the doc with its ID as value
    /// to the tree, as a writer would; at a leaf's split edge this splits it.
    add_numeric_before_write: Option<(usize, u64, usize)>,
    /// Record the ID in the deleted set, as a deletion during the cycle would.
    record_before_write: Option<u64>,
}

impl TestSpec {
    fn create(
        terms: impl IntoIterator<Item = (&'static [u8], u64)>,
        numeric_fields: Vec<(&'static CStr, NumericRangeTree)>,
    ) -> Self {
        let keys_dict = OwnedDict::<KeysDictType>::create();
        let missing_dict = OwnedDict::<MissingFieldDictType>::create();

        let mut trees = Vec::new();
        let mut field_specs = Vec::new();
        for (name, tree) in numeric_fields {
            let mut fs = FieldSpecBuilder::new(name)
                .with_types(FieldSpecType::Numeric.into())
                .finish();
            let mut boxed = Box::new(tree);
            fs.tree = NonNull::from(&mut *boxed)
                .cast::<ffi::NumericRangeTree>()
                .as_ptr();
            trees.push(boxed);
            field_specs.push(fs);
        }

        // SAFETY: a zeroed IndexSpec is valid for the fields this fixture sets
        // below; every other field is left zero/null and unused by the scanners.
        let mut spec: ffi::IndexSpec = unsafe { mem::zeroed() };
        spec.flags = IndexFlags_Index_DocIdsOnly;
        // SAFETY: `NewHiddenString` copies the static string because `takeOwnership` is true.
        spec.specName =
            unsafe { ffi::NewHiddenString(c"idx".as_ptr(), c"idx".count_bytes(), true) };
        spec.obfuscatedName = CString::new("Index@test").unwrap().into_raw();
        // SAFETY: `NewTrie` returns a fresh, valid, empty terms trie with no payloads.
        spec.terms = unsafe { ffi::NewTrie(None, ffi::TrieSortMode_Trie_Sort_Lex) };
        spec.keysDict = keys_dict.as_mut_ptr();
        spec.missing.indexes = missing_dict.as_mut_ptr();
        // The Vec's heap buffer stays put when moved into the struct.
        spec.fields = field_specs.as_mut_ptr();
        spec.numFields = field_specs.len() as u16;
        // Flushing stats subtracts from these, so leave room for the removals.
        spec.stats.numRecords = 1_000_000;
        spec.stats.invertedSize = 1_000_000;
        spec.stats.termsSize = 1_000_000;
        spec.stats.scoring.numTerms = 1_000_000;
        // Every doc this fixture indexes is deleted: the whole ID range is in the set the
        // thread GC consults instead of the zeroed doc table.
        spec.docs.deletedDocs = Box::into_raw(Box::new(DeletedDocs::from_ids(1..=1024))).cast();

        let mut test_spec = Self {
            spec,
            keys_dict,
            _missing_dict: missing_dict,
            field_specs,
            trees,
            lock_attempts: 0,
            scanned: false,
            fail_lock_on_attempt: None,
            append_before_write: None,
            remove_before_write: None,
            add_numeric_before_write: None,
            record_before_write: None,
        };
        for (term, doc_count) in terms {
            test_spec.insert_term(term, doc_count);
        }
        test_spec
    }

    fn insert_term(&mut self, term: &[u8], doc_count: u64) {
        // SAFETY: `self.spec.terms` is live; `term` points to `term.len()` valid
        // bytes; a null payload is accepted by the terms trie.
        unsafe {
            ffi::Trie_InsertStringBuffer(
                self.spec.terms,
                term.as_ptr().cast::<c_char>(),
                term.len(),
                1.0,
                0,
                ptr::null_mut(),
                doc_count as usize,
            );
        }
        let mut ii = InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly);
        for doc_id in 1..=doc_count {
            ii.add_record(&RSIndexResult::build_virt().doc_id(doc_id).build())
                .unwrap();
        }
        self.keys_dict
            .try_insert(term, Box::new(OpaqueInvertedIndex::DocIdsOnly(ii)))
            .unwrap_or_else(|_| panic!("term already present in keysDict"));
    }

    /// Give the spec a missing-docs index for `field` and an existing-docs
    /// index, each holding `doc_count` docs.
    fn add_doc_set_indexes(&mut self, field: &CStr, doc_count: u64) {
        let hidden = OwnedHiddenString::new(field);
        self._missing_dict
            .try_insert(&hidden, doc_ids_only_index(doc_count))
            .unwrap_or_else(|_| panic!("field already present in the missing-docs dict"));
        self.spec.existingDocs = Box::into_raw(doc_ids_only_index(doc_count)).cast();
    }

    fn has_term(&self, term: &[u8]) -> bool {
        self.keys_dict.fetch(term).is_some()
    }

    fn deleted_docs(&mut self) -> &mut DeletedDocs {
        // SAFETY: `create` boxed this set, and the fixture owns it until `Drop`.
        unsafe { &mut *self.spec.docs.deletedDocs.cast::<DeletedDocs>() }
    }

    fn attempt_lock(&mut self) -> Result<(), HandleError> {
        self.lock_attempts += 1;
        if self.fail_lock_on_attempt == Some(self.lock_attempts) {
            return Err(HandleError::SpecDeleted);
        }
        Ok(())
    }
}

impl SpecWriteAccess for TestSpec {
    fn with_write<T>(
        &mut self,
        apply: impl FnOnce(&mut IndexSpecWriteGuard<'_>) -> Result<T, HandleError>,
    ) -> Result<T, HandleError> {
        self.attempt_lock()?;
        if self.scanned {
            self.concurrent_writer();
        }
        // SAFETY: the fixture has exclusive access to `self.spec`; `ManuallyDrop`
        // keeps the guard from releasing a lock that was never taken.
        let mut guard = unsafe { IndexSpecWriteGuard::from_locked_mut(&mut self.spec) };
        apply(&mut guard)
    }
}

impl TestSpec {
    /// What a writer did between the scan and this write lock, if a test asked for one.
    fn concurrent_writer(&mut self) {
        if let Some((term, doc_id)) = self.append_before_write.take() {
            let Some(OpaqueInvertedIndex::DocIdsOnly(ii)) = self.keys_dict.fetch_mut(term) else {
                panic!("the appended-to term must be a doc-ids-only index");
            };
            ii.add_record(&RSIndexResult::build_virt().doc_id(doc_id).build())
                .unwrap();
        }
        if let Some(term) = self.remove_before_write.take() {
            assert!(
                self.keys_dict.remove(term),
                "the removed term must be indexed"
            );
        }
        if let Some((tree, doc_id, max_depth_range)) = self.add_numeric_before_write.take() {
            self.trees[tree].add(doc_id, doc_id as f64, false, false, max_depth_range);
        }
        if let Some(doc_id) = self.record_before_write.take() {
            self.deleted_docs().record(doc_id);
        }
    }
}

impl SpecLockAccess for TestSpec {
    fn with_read<T>(
        &mut self,
        scan: impl FnOnce(&IndexSpecReadGuard<'_>) -> T,
    ) -> Result<T, HandleError> {
        self.attempt_lock()?;
        self.scanned = true;
        // SAFETY: the fixture owns `spec` and does not mutate it while the guard is held.
        let guard = unsafe { IndexSpecReadGuard::from_locked(&self.spec) };
        Ok(scan(&guard))
    }
}

impl Drop for TestSpec {
    fn drop(&mut self) {
        // SAFETY: all pointers were allocated by `create` and are freed once.
        unsafe {
            drop(Box::from_raw(
                self.spec.docs.deletedDocs.cast::<DeletedDocs>(),
            ));
            ffi::TrieType_Free(self.spec.terms.cast());
            ffi::HiddenString_Free(self.spec.specName, true);
            drop(CString::from_raw(self.spec.obfuscatedName));
            if !self.spec.existingDocs.is_null() {
                drop(Box::from_raw(
                    self.spec.existingDocs.cast::<OpaqueInvertedIndex>(),
                ));
            }
            for fs in &self.field_specs {
                // `FieldSpecBuilder` makes `fieldName == fieldPath`, one owned allocation.
                ffi::HiddenString_Free(fs.fieldName, true);
            }
        }
    }
}

fn doc_ids_only_index(doc_count: u64) -> Box<OpaqueInvertedIndex> {
    let mut ii = InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly);
    for doc_id in 1..=doc_count {
        ii.add_record(&RSIndexResult::build_virt().doc_id(doc_id).build())
            .unwrap();
    }
    Box::new(OpaqueInvertedIndex::DocIdsOnly(ii))
}

/// Collects every stats flush the driver makes.
#[derive(Default)]
struct Recorder(Vec<GcApplyStats>);

impl Recorder {
    fn total(&self) -> GcApplyStats {
        let mut total = GcApplyStats::default();
        for stats in &self.0 {
            total.record(*stats);
        }
        total
    }
}

impl GcStatsSink for Recorder {
    fn record(&mut self, stats: &GcApplyStats) {
        self.0.push(*stats);
    }
}

/// Read locks a cycle takes on a spec with only terms: one to read the deleted
/// set, `term_chunks` to walk the terms, and one each to list numeric trees,
/// walk the (absent) tag values, list missing fields and scan the (absent)
/// existing-docs index.
const fn terms_only_read_locks(term_chunks: u64) -> u64 {
    1 + term_chunks + 4
}

/// Write locks a cycle takes: `apply_chunks` to apply deltas, and one to prune
/// the deleted set.
const fn write_locks(apply_chunks: u64) -> u64 {
    apply_chunks + 1
}

/// Read locks a cycle takes on a spec with one numeric field and nothing else:
/// one to read the deleted set, one to walk the (absent) terms, one to list the
/// tree's nodes, `node_chunks` to snapshot them, one to check the tree for
/// leaves split since, then one each for tags, missing fields and existing docs.
const fn numeric_only_read_locks(node_chunks: u64) -> u64 {
    1 + 1 + 1 + node_chunks + 1 + 3
}

#[test]
fn a_cycle_collects_every_term_and_drops_the_emptied_ones() {
    let mut spec = TestSpec::create([(&b"apple"[..], 3), (&b"grape"[..], 2)], vec![]);
    let mut recorder = Recorder::default();

    run_cycle(&mut spec, &mut recorder, true).unwrap();

    assert!(!spec.has_term(b"apple"));
    assert!(!spec.has_term(b"grape"));
    let total = recorder.total();
    assert_eq!(total.terms_removed, 2);
    assert_eq!(total.records_removed, 5);
    assert_eq!(spec.spec.stats.scoring.numTerms, 1_000_000 - 2);
    assert!(
        spec.deleted_docs().is_empty(),
        "every deleted ID was collected, so none needs another look"
    );
}

/// A set with nothing in it means nothing to collect: the cycle ends after
/// reading it.
#[test]
fn an_empty_set_ends_the_cycle_at_once() {
    let mut spec = TestSpec::create([(&b"apple"[..], 3)], vec![]);
    *spec.deleted_docs() = DeletedDocs::new();
    let mut recorder = Recorder::default();

    let report = run_cycle(&mut spec, &mut recorder, true).unwrap();

    assert_eq!((report.read_locks, report.write_locks), (1, 0));
    assert!(spec.has_term(b"apple"));
}

/// A read lock visits [`KEYS_PER_HOLD`] keys and is released; one more key
/// takes a second chunk. Both chunks' deltas land under one write lock, since
/// together they stay under [`DELTAS_PER_APPLY`].
#[test]
fn a_read_lock_visits_a_bounded_number_of_keys() {
    let terms = (0..=KEYS_PER_HOLD).map(|i| {
        (
            &*Box::leak(format!("t{i:04}").into_boxed_str()).as_bytes(),
            1,
        )
    });
    let mut spec = TestSpec::create(terms, vec![]);
    let mut recorder = Recorder::default();

    let report = run_cycle(&mut spec, &mut recorder, true).unwrap();

    assert_eq!(report.read_locks, terms_only_read_locks(2));
    assert_eq!(report.write_locks, write_locks(1));
    assert_eq!(recorder.total().terms_removed, KEYS_PER_HOLD + 1);
}

/// Deltas accumulate across read chunks and are applied once [`DELTAS_PER_APPLY`]
/// of them are pending: one more term than that takes a second write lock.
#[test]
fn deltas_are_applied_in_batches() {
    let terms = (0..=DELTAS_PER_APPLY).map(|i| {
        (
            &*Box::leak(format!("t{i:05}").into_boxed_str()).as_bytes(),
            1,
        )
    });
    let mut spec = TestSpec::create(terms, vec![]);
    let mut recorder = Recorder::default();

    let report = run_cycle(&mut spec, &mut recorder, true).unwrap();

    let chunks = (DELTAS_PER_APPLY + 1).div_ceil(KEYS_PER_HOLD) as u64;
    assert_eq!(report.read_locks, terms_only_read_locks(chunks));
    assert_eq!(report.write_locks, write_locks(2));
    assert_eq!(
        recorder.total().terms_removed as usize,
        DELTAS_PER_APPLY + 1
    );
}

/// Fewer keys than a hold visits: one read lock scans every term and one write
/// lock applies them all.
#[test]
fn a_scanners_few_keys_take_one_read_lock_and_one_write_lock() {
    let mut spec = TestSpec::create([(&b"a"[..], 1), (&b"b"[..], 1), (&b"c"[..], 1)], vec![]);
    let mut recorder = Recorder::default();

    let report = run_cycle(&mut spec, &mut recorder, true).unwrap();

    assert_eq!(
        report,
        LockReport {
            read_locks: terms_only_read_locks(1),
            write_locks: write_locks(1),
            max_read_hold: report.max_read_hold,
            max_write_hold: report.max_write_hold,
        }
    );
    assert_eq!(recorder.0.len(), 1, "one flush per applying write lock");
}

/// A spec dropped mid-cycle ends the cycle with the failing scanner named, and
/// what earlier scanners applied stays applied and counted.
#[test]
fn a_spec_dropped_mid_cycle_keeps_what_was_already_collected() {
    let mut spec = TestSpec::create(
        [(&b"a"[..], 1)],
        vec![(c"price", build_single_leaf_tree(4))],
    );
    // Lock attempts: read the deleted set, scan the terms, apply them, list the
    // numeric trees (fails).
    spec.fail_lock_on_attempt = Some(4);
    let mut recorder = Recorder::default();

    let error = run_cycle(&mut spec, &mut recorder, true).unwrap_err();

    assert!(matches!(error, ("numeric", HandleError::SpecDeleted)));
    assert!(!spec.has_term(b"a"));
    assert_eq!(spec.trees[0].num_entries(), 4);
    assert_eq!(recorder.total().terms_removed, 1);
    assert_eq!(
        spec.deleted_docs().len(),
        1024,
        "an interrupted cycle prunes nothing"
    );
}

/// A record appended between a term's scan and its apply grows the scanned last
/// block, so its delta is stale; the apply rescans the block as it is now, so the
/// deleted records go, the live appended one stays, and nothing is denied.
#[test]
fn an_append_after_the_scan_is_rescanned_when_the_delta_is_applied() {
    let mut spec = TestSpec::create([(&b"apple"[..], 2)], vec![]);
    // Beyond the IDs the fixture lists as deleted: a live document.
    spec.append_before_write = Some((b"apple", 2000));
    let mut recorder = Recorder::default();

    run_cycle(&mut spec, &mut recorder, true).unwrap();

    assert!(spec.has_term(b"apple"));
    let total = recorder.total();
    assert_eq!(total.blocks_denied, 0);
    assert_eq!(total.records_removed, 2);
}

/// Numeric node deltas are applied, and a tree GC emptied is compacted when
/// asked to.
#[test]
fn numeric_trees_are_collected_and_compacted() {
    let mut spec = TestSpec::create([], vec![(c"price", build_single_leaf_tree(4))]);
    let mut recorder = Recorder::default();

    let report = run_cycle(&mut spec, &mut recorder, true).unwrap();

    assert_eq!(spec.trees[0].num_entries(), 0);
    assert_eq!(recorder.total().records_removed, 4);
    // One write lock applies the node deltas, one compacts the tree; compacting a
    // single leaf changes nothing, so only the first flushes stats.
    assert_eq!(report.write_locks, write_locks(2));
    assert_eq!(recorder.0.len(), 1);
}

/// Without compaction a numeric field takes only the write lock that applies
/// its deltas.
#[test]
fn numeric_compaction_is_skipped_when_not_requested() {
    let mut spec = TestSpec::create([], vec![(c"price", build_single_leaf_tree(4))]);
    let mut recorder = Recorder::default();

    let report = run_cycle(&mut spec, &mut recorder, false).unwrap();

    assert_eq!(report.write_locks, write_locks(1));
    assert_eq!(spec.trees[0].num_entries(), 0);
}

/// A term whose inverted index vanished between its scan and its apply is
/// skipped; the rest of the chunk and of the cycle still apply. Only this GC
/// removes an index, so its entries are gone with it and nothing is owed.
#[test]
fn a_delta_whose_target_vanished_is_skipped() {
    let mut spec = TestSpec::create([(&b"apple"[..], 1), (&b"grape"[..], 2)], vec![]);
    spec.remove_before_write = Some(b"apple");
    let mut recorder = Recorder::default();

    run_cycle(&mut spec, &mut recorder, true).unwrap();

    assert!(!spec.has_term(b"grape"));
    assert_eq!(recorder.total().terms_removed, 1);
    assert!(
        spec.deleted_docs().is_empty(),
        "apple's doc 1 left with its index; grape's docs 1 and 2 were collected"
    );
}

/// The missing-docs and existing-docs indexes are collected, and emptied ones
/// are dropped.
#[test]
fn doc_set_indexes_are_collected_and_dropped_once_empty() {
    let mut spec = TestSpec::create([], vec![]);
    spec.add_doc_set_indexes(c"m", 3);
    let mut recorder = Recorder::default();

    run_cycle(&mut spec, &mut recorder, true).unwrap();

    assert!(
        spec._missing_dict
            .fetch(&OwnedHiddenString::new(c"m"))
            .is_none()
    );
    assert!(spec.spec.existingDocs.is_null());
    assert!(recorder.total().bytes_collected > 0);
}

/// The smallest tree whose next sequential doc splits its single leaf, with
/// internal nodes down to `max_depth_range` keeping a range of their own.
fn tree_at_split_edge(max_depth_range: usize) -> (NumericRangeTree, u64) {
    let mut n = 1;
    loop {
        if build_tree(n, false, max_depth_range).num_leaves() > 1 {
            return (build_tree(n - 1, false, max_depth_range), n);
        }
        n += 1;
    }
}

/// A leaf that splits between the listing of a tree's nodes and their apply copies
/// its entries, dead ones included, into leaves this cycle never scans: their IDs
/// stay in the set, and the next cycle collects the copies.
#[test]
fn ids_copied_by_a_leaf_split_mid_cycle_stay_in_the_set() {
    let (tree, next_doc) = tree_at_split_edge(1);
    let mut spec = TestSpec::create([], vec![(c"price", tree)]);
    // Every doc in the leaf is deleted; the one the writer adds is live.
    *spec.deleted_docs() = DeletedDocs::from_ids(1..next_doc);
    spec.add_numeric_before_write = Some((0, next_doc, 1));
    let live = |id: u64| id == next_doc;
    let mut recorder = Recorder::default();

    let report = run_cycle(&mut spec, &mut recorder, false).unwrap();

    assert!(spec.trees[0].num_leaves() > 1, "the writer split the leaf");
    assert_eq!(report.read_locks, numeric_only_read_locks(1));
    assert!(
        !scan_all_node_deltas(&spec.trees[0], &live).is_empty(),
        "the new leaves hold copies of the dead entries"
    );
    assert!(
        !spec.deleted_docs().is_empty(),
        "the IDs the copies may hold are kept for the next cycle"
    );

    run_cycle(&mut spec, &mut recorder, false).unwrap();

    assert!(scan_all_node_deltas(&spec.trees[0], &live).is_empty());
    assert!(spec.deleted_docs().is_empty());
}

/// A numeric apply has no liveness source to rescan with, so a leaf whose last
/// block grew between its scan and its apply is denied, and the IDs that block
/// spans stay in the set for the next cycle.
#[test]
fn a_denied_numeric_last_block_keeps_its_ids_in_the_set() {
    let mut spec = TestSpec::create([], vec![(c"price", build_single_leaf_tree(4))]);
    // A live doc appended to the leaf's only block.
    spec.add_numeric_before_write = Some((0, 2000, 0));
    let mut recorder = Recorder::default();

    run_cycle(&mut spec, &mut recorder, false).unwrap();

    assert_eq!(recorder.total().blocks_denied, 1);
    assert_eq!(
        spec.deleted_docs().snapshot(),
        (1..=4).collect(),
        "the block spanned docs 1 to 4, all of them owed another look"
    );
}

/// An ID recorded while the cycle runs is not among the ones it collected, so
/// the pruning at the end leaves it in the set.
#[test]
fn an_id_recorded_during_the_cycle_survives_the_pruning() {
    let mut spec = TestSpec::create([(&b"apple"[..], 3)], vec![]);
    spec.record_before_write = Some(5000);
    let mut recorder = Recorder::default();

    run_cycle(&mut spec, &mut recorder, true).unwrap();

    assert!(!spec.has_term(b"apple"));
    assert_eq!(spec.deleted_docs().snapshot(), [5000].into_iter().collect());
}

/// A spec dropped when a chunk's write lock is taken ends the cycle with nothing
/// applied for that chunk and the deleted set as it was.
#[test]
fn a_spec_dropped_before_an_apply_leaves_the_set_as_it_was() {
    let mut spec = TestSpec::create([(&b"a"[..], 1), (&b"b"[..], 1)], vec![]);
    // Lock attempts: read the deleted set, scan the terms, apply them (fails).
    spec.fail_lock_on_attempt = Some(3);
    let mut recorder = Recorder::default();

    let error = run_cycle(&mut spec, &mut recorder, true).unwrap_err();

    assert!(matches!(error, ("terms", HandleError::SpecDeleted)));
    assert!(spec.has_term(b"a"));
    assert!(spec.has_term(b"b"));
    assert!(recorder.0.is_empty());
    assert_eq!(spec.deleted_docs().len(), 1024);
}

/// The set keeps one bitmap per high 32 bits of the ID; a range count must add
/// up across that boundary, and a range with no gone ID must count zero.
#[test]
fn deleted_ids_count_gone_ids_across_the_bitmap_boundary() {
    use inverted_index::DocLiveness;
    const HI: u64 = 1 << 32;
    let ids: RoaringTreemap = [5, HI - 2, HI - 1, HI, HI + 1, 2 * HI + 7]
        .into_iter()
        .collect();
    let live = DeletedIds::new(&ids);

    assert!(!live.exists(HI - 1));
    assert!(live.exists(HI + 2));
    assert_eq!(live.gone_in_range(HI - 2, HI + 1), Some(4));
    assert_eq!(live.gone_in_range(HI - 1, HI), Some(2));
    assert_eq!(live.gone_in_range(6, HI - 3), Some(0));
    assert!(!live.any_gone(6, HI - 3));
    assert_eq!(live.gone_in_range(0, 3 * HI), Some(6));
    assert_eq!(live.gone_in_range(HI + 2, 2 * HI + 6), Some(0));
}

/// Put a document in a real doc table and return its ID; the table keeps the only
/// reference.
fn put(docs: &mut ffi::DocTable, key: &str) -> u64 {
    // SAFETY: `docs` is a live table and the key bytes outlive the call; a null payload
    // means none.
    let dmd = unsafe {
        ffi::DocTable_Put(
            docs,
            key.as_ptr().cast(),
            key.len(),
            1.0,
            0,
            ptr::null(),
            0,
            document::DocumentType::Hash,
        )
    };
    assert!(!dmd.is_null());
    // SAFETY: `DocTable_Put` returns a live DMD with a reference for the caller on top of
    // the table's; this single-threaded test hands it back without an atomic.
    unsafe {
        (*dmd).ref_count -= 1;
        (*dmd).id
    }
}

/// Delete a document from a real doc table, as `IndexSpec_DeleteDoc` does under the
/// write lock.
fn delete(docs: &mut ffi::DocTable, id: u64) {
    // SAFETY: `docs` is a live table; the removed DMD's only reference moves to the caller.
    unsafe {
        let dmd = ffi::DocTable_DeleteById(docs, id);
        assert!(!dmd.is_null(), "doc {id} is in the table");
        ffi::DMD_Free(dmd);
    }
}

/// A doc table created to track deletions has a set from the start that receives
/// the IDs the C doc table deletes; the guards see it. One created without has none.
#[test]
fn the_deleted_set_records_what_the_doc_table_deletes() {
    // `DocTable_Put` hashes the key to its cluster slot through the Redis API.
    redis_mock::init_redis_module_mock();
    // SAFETY: a zeroed IndexSpec is valid for the doc-table access below.
    let mut spec: ffi::IndexSpec = unsafe { mem::zeroed() };
    // SAFETY: a fresh table; freed below.
    spec.docs = unsafe { ffi::NewDocTable(4, 4, true) };
    for key in ["a", "b", "c"] {
        put(&mut spec.docs, key);
    }
    {
        // SAFETY: the spec is valid and not mutated while the guard lives.
        let read = unsafe { IndexSpecReadGuard::from_locked(&spec) };
        let set = read.deleted_docs().expect("created with the table");
        assert!(set.is_empty());
    }

    delete(&mut spec.docs, 2);

    {
        // SAFETY: the spec is valid and exclusively accessed while the guard lives.
        let mut write = unsafe { IndexSpecWriteGuard::from_locked_mut(&mut spec) };
        let set = write.deleted_docs_mut().expect("created with the table");
        assert_eq!(set.snapshot(), [2].into_iter().collect());
    }
    // SAFETY: the table was made by `NewDocTable` above; this frees it and its set once.
    unsafe { ffi::DocTable_Free(&mut spec.docs) };

    // SAFETY: as above, for a table that does not track deletions.
    spec.docs = unsafe { ffi::NewDocTable(4, 4, false) };
    {
        // SAFETY: as above.
        let read = unsafe { IndexSpecReadGuard::from_locked(&spec) };
        assert!(read.deleted_docs().is_none());
    }
    // SAFETY: as above.
    unsafe { ffi::DocTable_Free(&mut spec.docs) };
}
