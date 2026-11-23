/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Collecting a NESTED field through a spec: a whole thread-GC cycle, and the fork parent's
//! handling of a field's stream.

#![cfg(not(miri))] // The fixture calls C trie and dict functions.

use std::{ffi::CString, io::Cursor, mem, ptr};

use deleted_docs::DeletedDocs;
use dict::{KeysDictType, MissingFieldDictType, OwnedDict};
use ffi::IndexFlags_Index_DocIdsOnly;
use field_spec::{FieldSpecBuilder, FieldSpecType};
use index_gc::{
    GcApplyStats, GcError, HandleOutcome,
    fork::wire::serialize,
    scanners::nested::{
        NestedField, NestedTerminalDelta, TerminalWork, WireStep, apply_nested_terminal_with,
        handle_nested_with, scan_terminal,
    },
    spec_access::SpecWriteAccess,
    stats::GcStatsSink,
    thread::{SpecLockAccess, run_cycle},
};
use index_spec::{IndexSpecReadGuard, IndexSpecWriteGuard};
use nested_index::{
    Element, FloatCompression, IndexParams, Key, Level, NestedIndex, PathStep, TagLevel,
    TerminalKind, TerminalValue,
};

extern crate redisearch_rs;
redis_mock::mock_or_stub_missing_redis_c_symbols!();

const PARAMS: IndexParams = IndexParams { max_depth_range: 0 };

/// `color` and `size` tag levels over numeric terminals.
fn priced() -> NestedIndex {
    NestedIndex::new(
        vec![Level::Tag(TagLevel::new()), Level::Tag(TagLevel::new())],
        TerminalKind::Numeric(FloatCompression::Off),
    )
    .unwrap()
}

/// Index `doc_id` as one element `(color, size) → value`.
fn put(index: &mut NestedIndex, doc_id: u64, color: &str, size: &str, value: f64) {
    let path = [Key::Tag(color.as_bytes()), Key::Tag(size.as_bytes())];
    let element = Element {
        path: &path,
        value: TerminalValue::Numeric(value),
    };
    index.index_document(doc_id, &[element], PARAMS).unwrap();
}

fn path(color: &str, size: &str) -> Vec<PathStep> {
    vec![
        PathStep::Tag(color.as_bytes().to_vec()),
        PathStep::Tag(size.as_bytes().to_vec()),
    ]
}

type Writer = Box<dyn FnOnce(&mut NestedIndex)>;

/// A spec with one NESTED field, whose deleted set lists the documents to collect. Locks are
/// simulated: the fixture has exclusive access, and can run a writer just before the first
/// write lock that follows a scan, as a concurrent writer would.
struct NestedSpec {
    spec: ffi::IndexSpec,
    _keys_dict: OwnedDict<KeysDictType>,
    _missing_dict: OwnedDict<MissingFieldDictType>,
    // Keeps the heap buffer `spec.fields` points at alive.
    field_specs: Vec<ffi::FieldSpec>,
    scanned: bool,
    before_write: Option<Writer>,
    /// Read holds taken so far.
    reads: usize,
    /// Write holds taken so far.
    writes: usize,
    /// A writer to run just before the read hold of that number, counting from 1.
    before_read: Option<(usize, Writer)>,
}

impl NestedSpec {
    fn create(index: NestedIndex, deleted: impl IntoIterator<Item = u64>) -> Self {
        let keys_dict = OwnedDict::<KeysDictType>::create();
        let missing_dict = OwnedDict::<MissingFieldDictType>::create();
        let mut fs = FieldSpecBuilder::new(c"items")
            .with_types(FieldSpecType::Nested.into())
            .finish();
        // Only the index is read by the GC; the definition stays null.
        fs.__bindgen_anon_1.nestedOpts.def = ptr::null_mut();
        fs.__bindgen_anon_1.nestedOpts.nestedIndex = Box::into_raw(Box::new(index)).cast();
        let mut field_specs = vec![fs];

        // SAFETY: a zeroed IndexSpec is valid for the fields set below; the scanners use no
        // other.
        let mut spec: ffi::IndexSpec = unsafe { mem::zeroed() };
        spec.flags = IndexFlags_Index_DocIdsOnly;
        // SAFETY: `NewHiddenString` copies the static string because `takeOwnership` is true.
        spec.specName =
            unsafe { ffi::NewHiddenString(c"idx".as_ptr(), c"idx".count_bytes(), true) };
        spec.obfuscatedName = CString::new("Index@test").unwrap().into_raw();
        // SAFETY: `NewTrie` returns a fresh, empty terms trie.
        spec.terms = unsafe { ffi::NewTrie(None, ffi::TrieSortMode_Trie_Sort_Lex) };
        spec.keysDict = keys_dict.as_mut_ptr();
        spec.missing.indexes = missing_dict.as_mut_ptr();
        spec.fields = field_specs.as_mut_ptr();
        spec.numFields = 1;
        // Flushing stats subtracts from these, so leave room for the removals.
        spec.stats.numRecords = 1_000_000;
        spec.stats.invertedSize = 1_000_000;
        spec.stats.totalInvertedIndexBlocks = 1_000_000;
        spec.docs.deletedDocs = Box::into_raw(Box::new(DeletedDocs::from_ids(deleted))).cast();
        Self {
            spec,
            _keys_dict: keys_dict,
            _missing_dict: missing_dict,
            field_specs,
            scanned: false,
            before_write: None,
            reads: 0,
            writes: 0,
            before_read: None,
        }
    }

    #[expect(
        clippy::multiple_unsafe_ops_per_block,
        reason = "one SAFETY argument covers reading the union member and the pointer"
    )]
    fn index(&mut self) -> &mut NestedIndex {
        // SAFETY: `create` installed the field's index, which the fixture owns until `Drop`.
        unsafe {
            self.field_specs[0]
                .__bindgen_anon_1
                .nestedOpts
                .nestedIndex
                .cast::<NestedIndex>()
                .as_mut()
        }
        .expect("`create` installed an index")
    }

    fn deleted_docs(&mut self) -> &mut DeletedDocs {
        // SAFETY: `create` boxed this set, and the fixture owns it until `Drop`.
        unsafe { self.spec.docs.deletedDocs.cast::<DeletedDocs>().as_mut() }
            .expect("`create` boxed the set")
    }
}

impl SpecWriteAccess for NestedSpec {
    fn with_write<T>(
        &mut self,
        apply: impl FnOnce(&mut IndexSpecWriteGuard<'_>) -> Result<T, GcError>,
    ) -> Result<T, GcError> {
        self.writes += 1;
        if self.scanned
            && let Some(writer) = self.before_write.take()
        {
            writer(self.index());
        }
        // SAFETY: the fixture has exclusive access to `self.spec`; `ManuallyDrop` keeps the
        // guard from releasing a lock that was never taken.
        let mut guard = unsafe { IndexSpecWriteGuard::from_locked_mut(&mut self.spec) };
        apply(&mut guard)
    }
}

impl SpecLockAccess for NestedSpec {
    fn with_read<T>(
        &mut self,
        scan: impl FnOnce(&IndexSpecReadGuard<'_>) -> T,
    ) -> Result<T, GcError> {
        self.scanned = true;
        self.reads += 1;
        if self
            .before_read
            .as_ref()
            .is_some_and(|(at, _)| *at == self.reads)
        {
            let (_, writer) = self.before_read.take().expect("just checked");
            writer(self.index());
        }
        // SAFETY: the fixture owns `spec` and does not mutate it while the guard is held.
        let guard = unsafe { IndexSpecReadGuard::from_locked(&self.spec) };
        Ok(scan(&guard))
    }
}

impl Drop for NestedSpec {
    #[expect(
        clippy::multiple_unsafe_ops_per_block,
        reason = "one SAFETY argument covers freeing everything `create` allocated"
    )]
    fn drop(&mut self) {
        // SAFETY: all pointers were allocated by `create` and are freed once.
        unsafe {
            drop(Box::from_raw(self.index()));
            drop(Box::from_raw(
                self.spec.docs.deletedDocs.cast::<DeletedDocs>(),
            ));
            ffi::TrieType_Free(self.spec.terms.cast());
            ffi::HiddenString_Free(self.spec.specName, true);
            drop(CString::from_raw(self.spec.obfuscatedName));
            // `FieldSpecBuilder` makes `fieldName == fieldPath`, one owned allocation.
            ffi::HiddenString_Free(self.field_specs[0].fieldName, true);
        }
    }
}

/// Collects every stats flush.
#[derive(Default)]
struct Recorder(GcApplyStats);

impl GcStatsSink for Recorder {
    fn record(&mut self, stats: &GcApplyStats) {
        self.0.record(*stats);
    }
}

#[test]
fn a_thread_cycle_collects_terminals_and_drops_the_emptied_ones() {
    let mut index = priced();
    put(&mut index, 1, "red", "m", 1.0);
    put(&mut index, 2, "red", "l", 2.0);
    put(&mut index, 3, "blue", "m", 3.0);
    let blue = index
        .routing()
        .get(&[Key::Tag(b"blue"), Key::Tag(b"m")])
        .unwrap()
        .unwrap()
        .alive()
        .clone();
    let mut spec = NestedSpec::create(index, [2, 3]);
    let mut recorder = Recorder::default();

    let report = run_cycle(&mut spec, &mut recorder, true).unwrap();

    assert_eq!(spec.index().routing().num_terminals(), 1);
    assert_eq!(spec.index().num_records(), 1);
    assert!(!blue.is_alive(), "the dropped terminal revoked its token");
    assert_eq!(recorder.0.records_removed, 2);
    assert!(recorder.0.bytes_collected > 0);
    assert_eq!(
        spec.spec.stats.numRecords,
        1_000_000 - 2,
        "the spec counts the removed records"
    );
    assert_eq!((report.retained, report.pruned), (0, 2));
    assert!(spec.deleted_docs().is_empty());
}

#[test]
fn a_leaf_split_between_scan_and_apply_keeps_every_deleted_id() {
    // The most documents one terminal's leaf holds before it splits.
    let at_edge = (1..)
        .find(|&n| {
            let mut index = priced();
            for doc_id in 1..=n {
                put(&mut index, doc_id, "red", "m", doc_id as f64);
            }
            let terminal = index.routing().get(&[Key::Tag(b"red"), Key::Tag(b"m")]);
            terminal.unwrap().unwrap().numeric().unwrap().num_leaves() > 1
        })
        .unwrap()
        - 1;
    let mut index = priced();
    for doc_id in 1..=at_edge {
        put(&mut index, doc_id, "red", "m", doc_id as f64);
    }
    let mut spec = NestedSpec::create(index, 1..=at_edge);
    let next_doc = at_edge + 1;
    // A writer splits the leaf after the scan, so the scanned node is gone at apply.
    spec.before_write = Some(Box::new(move |index: &mut NestedIndex| {
        let tree = index
            .terminal_at_mut(&path("red", "m"))
            .unwrap()
            .numeric_mut()
            .unwrap();
        tree.add(
            next_doc,
            next_doc as f64,
            false,
            false,
            PARAMS.max_depth_range,
        );
        assert!(tree.num_leaves() > 1, "the writer split the leaf");
    }));
    let mut recorder = Recorder::default();

    let report = run_cycle(&mut spec, &mut recorder, true).unwrap();

    assert_eq!(
        report.retained, at_edge,
        "every deleted ID waits for the next cycle"
    );
    assert_eq!(report.pruned, 0);
}

#[test]
fn the_fork_parent_skips_a_stale_delta_and_reads_the_rest_of_the_stream() {
    let mut index = priced();
    put(&mut index, 1, "red", "m", 1.0);
    put(&mut index, 2, "blue", "m", 2.0);
    let live = |id: u64| id == 1;
    let mut deltas = Vec::new();
    index.routing().for_each_terminal(|path, terminal| {
        if let Some(work) = scan_terminal(terminal, live).work {
            deltas.push(NestedTerminalDelta {
                path: path.iter().map(WireStep::from).collect(),
                work,
            });
        }
    });
    let blue = deltas.pop().expect("blue holds the gone document");
    let mut spec = NestedSpec::create(index, []);

    // A delta for a path no terminal is at, then the real one.
    let mut wire = Vec::new();
    serialize(&mut wire, Some(NestedField { field_index: 0 })).unwrap();
    let stale = path("green", "m").iter().map(WireStep::from).collect();
    let stale_work = {
        let mut index = priced();
        put(&mut index, 1, "green", "m", 1.0);
        work_of_only_terminal(&index)
    };
    serialize(
        &mut wire,
        Some(NestedTerminalDelta {
            path: stale,
            work: stale_work,
        }),
    )
    .unwrap();
    serialize(&mut wire, Some(blue)).unwrap();
    serialize(&mut wire, None::<NestedTerminalDelta>).unwrap();
    serialize(&mut wire, None::<NestedField>).unwrap();

    let mut reader = Cursor::new(wire);
    let mut stats = GcApplyStats::default();
    let outcome = handle_nested_with(&mut reader, &mut spec, &mut stats, true).unwrap();
    assert_eq!(outcome, HandleOutcome::Collected);
    assert_eq!(stats.records_removed, 1);
    assert_eq!(
        spec.index().routing().num_terminals(),
        1,
        "blue was dropped"
    );
    let outcome = handle_nested_with(&mut reader, &mut spec, &mut stats, true).unwrap();
    assert_eq!(outcome, HandleOutcome::Done);
}

/// The scan of the only terminal of `index`, with every document gone.
fn work_of_only_terminal(index: &NestedIndex) -> TerminalWork {
    let mut work = None;
    index
        .routing()
        .for_each_terminal(|_, terminal| work = scan_terminal(terminal, |_| false).work);
    work.expect("the terminal holds a gone document")
}

#[test]
fn a_delta_never_lands_on_another_terminal() {
    let mut index = priced();
    put(&mut index, 1, "red", "m", 1.0);
    let TerminalWork::Numeric { unique_id, nodes } = work_of_only_terminal(&index) else {
        panic!("the terminal is numeric");
    };
    let mut spec = NestedSpec::create(index, []);
    let apply = |spec: &mut NestedSpec, field_index, delta| {
        spec.with_write(|guard| {
            Ok(apply_nested_terminal_with(guard, field_index, delta, None)
                .map(|applied| applied.stats))
        })
        .unwrap()
    };

    // A tree other than the one scanned, as a replacement at the same path would be.
    let wrong_tree = NestedTerminalDelta {
        path: path("red", "m").iter().map(WireStep::from).collect(),
        work: TerminalWork::Numeric {
            unique_id: unique_id.wrapping_add(1),
            nodes,
        },
    };
    assert!(matches!(
        apply(&mut spec, 0, wrong_tree),
        Err(GcError::TargetGone(_))
    ));
    // Tag work on a numeric terminal.
    let wrong_kind = NestedTerminalDelta {
        path: path("red", "m").iter().map(WireStep::from).collect(),
        work: TerminalWork::Tag(Vec::new()),
    };
    assert!(matches!(
        apply(&mut spec, 0, wrong_kind),
        Err(GcError::TargetGone(_))
    ));
    // A field the spec does not have.
    let no_field = NestedTerminalDelta {
        path: Vec::new(),
        work: TerminalWork::Tag(Vec::new()),
    };
    assert!(matches!(
        apply(&mut spec, 1, no_field),
        Err(GcError::TargetGone(_))
    ));
    assert_eq!(spec.index().num_records(), 1, "nothing was removed");
}

#[test]
fn an_out_of_range_bucket_names_no_bucket() {
    let mut index = NestedIndex::new(
        vec![Level::Tag(TagLevel::new())],
        TerminalKind::Numeric(FloatCompression::Off),
    )
    .unwrap();
    let key = [Key::Tag(b"red")];
    let element = Element {
        path: &key,
        value: TerminalValue::Numeric(1.0),
    };
    index.index_document(1, &[element], PARAMS).unwrap();
    let step = PathStep::from(WireStep::Bucket(u64::MAX));
    assert!(index.terminal_at_mut(&[step]).is_none());
}

/// A `color` tag level over tag terminals, with document `doc_id` holding `value` under
/// `color`.
fn tagged(elements: impl IntoIterator<Item = (u64, String, String)>) -> NestedIndex {
    let mut index = NestedIndex::new(vec![Level::Tag(TagLevel::new())], TerminalKind::Tag).unwrap();
    for (doc_id, color, value) in elements {
        let path = [Key::Tag(color.as_bytes())];
        let element = Element {
            path: &path,
            value: TerminalValue::Tag(nested_index::Tag::new(value.as_bytes()).unwrap()),
        };
        index.index_document(doc_id, &[element], PARAMS).unwrap();
    }
    index
}

/// Read holds a cycle over a spec with one NESTED field takes besides those of the nested
/// chunks: the deleted set, one each for the (absent) terms, numeric trees, tag values and
/// sparse vectors, the listing of the NESTED terminals, and one each for missing fields and
/// the existing-docs index.
const OTHER_READ_LOCKS: u64 = 8;

#[test]
fn a_read_hold_counts_the_posting_lists_it_visits() {
    // Two terminals of `KEYS_PER_HOLD` values each: the first fills a hold.
    let per_terminal = index_gc::thread::KEYS_PER_HOLD as u64;
    let elements = ["red", "blue"]
        .into_iter()
        .enumerate()
        .flat_map(|(t, color)| {
            (0..per_terminal).map(move |v| {
                let doc_id = t as u64 * per_terminal + v + 1;
                (doc_id, color.to_string(), format!("v{v:03}"))
            })
        });
    let mut spec = NestedSpec::create(tagged(elements), 1..=2 * per_terminal);
    let report = run_cycle(&mut spec, &mut Recorder::default(), true).unwrap();
    assert_eq!(report.read_locks, OTHER_READ_LOCKS + 2);
    assert_eq!(spec.index().routing().num_terminals(), 0);

    // The same number of documents on one value per terminal fit one hold.
    let elements = (1..=2 * per_terminal).map(|doc_id| {
        let color = if doc_id % 2 == 0 { "red" } else { "blue" };
        (doc_id, color.to_string(), "v".to_string())
    });
    let mut spec = NestedSpec::create(tagged(elements), 1..=2 * per_terminal);
    let report = run_cycle(&mut spec, &mut Recorder::default(), true).unwrap();
    assert_eq!(report.read_locks, OTHER_READ_LOCKS + 1);
}

#[test]
fn finishing_a_field_the_spec_does_not_have_is_skipped() {
    let mut spec = NestedSpec::create(priced(), []);
    let pruned = spec.with_write(|guard| {
        Ok(index_gc::scanners::nested::finish_nested_field(
            guard, 1, false,
        ))
    });
    assert!(matches!(pruned, Ok(Err(GcError::TargetGone(_)))));
}

#[test]
fn a_terminal_with_more_lists_than_a_hold_visits_is_walked_across_holds() {
    // One terminal of `2 * KEYS_PER_HOLD + 1` values: three holds.
    let lists = 2 * index_gc::thread::KEYS_PER_HOLD as u64 + 1;
    let elements = (1..=lists).map(|doc_id| (doc_id, "red".to_string(), format!("v{doc_id:04}")));
    let mut spec = NestedSpec::create(tagged(elements), 1..=lists);
    let mut recorder = Recorder::default();
    let report = run_cycle(&mut spec, &mut recorder, true).unwrap();
    assert_eq!(report.read_locks, OTHER_READ_LOCKS + 3);
    assert_eq!(recorder.0.records_removed as u64, lists);
    assert_eq!(spec.index().routing().num_terminals(), 0);
    assert_eq!((report.retained, report.pruned), (0, lists));
}

#[test]
fn a_posting_list_that_fails_to_decode_keeps_its_ids_for_the_next_cycle() {
    let elements = (1..=4).map(|doc_id| (doc_id, "red".to_string(), "v".to_string()));
    let mut spec = NestedSpec::create(tagged(elements), 1..=4);
    let list = spec
        .index()
        .terminal_at_mut(&[PathStep::Tag(b"red".to_vec())])
        .and_then(|terminal| terminal.tag_mut())
        .and_then(|tags| tags.find_value_mut(b"v"))
        .expect("red holds v");
    // A varint whose continuation bit promises a byte that is not there.
    inverted_index::test_utils::overwrite_block_buffer(list, 0, &[0x80]);
    let mut recorder = Recorder::default();

    let report = run_cycle(&mut spec, &mut recorder, true).unwrap();

    assert_eq!((report.retained, report.pruned), (4, 0));
    assert_eq!(recorder.0.records_removed, 0);
    assert_eq!(spec.index().routing().num_terminals(), 1);
}

#[test]
fn a_leaf_split_between_two_holds_of_a_walk_keeps_its_deleted_ids() {
    // One terminal whose tree has more nodes than a hold visits, so its walk takes several.
    let per_hold = index_gc::thread::KEYS_PER_HOLD;
    let mut index = priced();
    let mut docs = 0;
    let tree_nodes = |index: &NestedIndex| {
        let terminal = index.routing().get(&[Key::Tag(b"red"), Key::Tag(b"m")]);
        terminal.unwrap().map_or(0, |terminal| {
            terminal
                .numeric()
                .unwrap()
                .indexed_post_order_iter()
                .count()
        })
    };
    while tree_nodes(&index) <= 2 * per_hold {
        docs += 1;
        put(&mut index, docs, "red", "m", docs as f64);
    }
    // Delete the documents of the last leaf the walk visits, which a later hold copies.
    let terminal = index
        .routing()
        .get(&[Key::Tag(b"red"), Key::Tag(b"m")])
        .unwrap()
        .unwrap();
    let last_leaf = terminal
        .numeric()
        .unwrap()
        .indexed_post_order_iter()
        .filter_map(|(_, node)| node.range())
        .last()
        .unwrap()
        .entries()
        .doc_id_range()
        .unwrap();
    let deleted = last_leaf.clone().count() as u64;
    let mut spec = NestedSpec::create(index, last_leaf);
    // Before the walk's second hold, a writer splits that leaf: the hold then finds no range
    // to copy there, and no delta of the walk lands on it.
    spec.before_read = Some((
        OTHER_READ_LOCKS as usize,
        Box::new(move |index: &mut NestedIndex| {
            let tree = index
                .terminal_at_mut(&path("red", "m"))
                .unwrap()
                .numeric_mut()
                .unwrap();
            let leaves = tree.num_leaves();
            let mut doc_id = docs;
            while tree.num_leaves() == leaves {
                doc_id += 1;
                tree.add(doc_id, doc_id as f64, false, false, PARAMS.max_depth_range);
            }
        }),
    ));
    let mut recorder = Recorder::default();

    let report = run_cycle(&mut spec, &mut recorder, true).unwrap();

    assert!(spec.before_read.is_none(), "the writer ran");
    assert_eq!(
        report.retained, deleted,
        "the split leaf's copies of the deleted entries wait for the next cycle"
    );
}

#[test]
fn a_cycle_with_nothing_deleted_finishes_a_collection_left_unfinished() {
    let mut index = priced();
    put(&mut index, 1, "red", "m", 1.0);
    // A collection that emptied the terminal and stopped before its finish.
    index
        .collect_terminal(&path("red", "m"), |terminal| {
            let removed = scan_and_apply_all(terminal);
            (removed, removed)
        })
        .unwrap();
    assert!(index.has_collected());
    let mut spec = NestedSpec::create(index, []);
    let report = run_cycle(&mut spec, &mut Recorder::default(), true).unwrap();
    assert_eq!(spec.index().routing().num_terminals(), 0);
    assert!(!spec.index().has_collected());
    assert_eq!((report.read_locks, report.write_locks), (1, 1));
}

/// Remove every posting of `terminal`, all its documents gone, and return how many.
fn scan_and_apply_all(terminal: &mut nested_index::Terminal) -> usize {
    let nested_index::PostingsMut::Numeric(tree) = terminal.postings_mut() else {
        panic!("a numeric terminal");
    };
    let deltas: Vec<_> = tree
        .indexed_post_order_iter()
        .filter_map(|(node, n)| n.scan_gc(|_| false).map(|delta| (node, delta)))
        .collect();
    deltas
        .into_iter()
        .map(|(node, delta)| {
            tree.apply_gc_to_node_with(node, delta, None)
                .unwrap()
                .index_gc_info
                .entries_removed
        })
        .sum()
}

#[test]
fn many_terminals_are_found_one_hold_at_a_time() {
    // `2 * KEYS_PER_HOLD + 1` terminals of one value each: three holds, none listing them all.
    let terminals = 2 * index_gc::thread::KEYS_PER_HOLD as u64 + 1;
    let elements = (1..=terminals).map(|doc_id| (doc_id, format!("c{doc_id:04}"), "v".to_string()));
    let mut spec = NestedSpec::create(tagged(elements), 1..=terminals);
    let report = run_cycle(&mut spec, &mut Recorder::default(), true).unwrap();
    assert_eq!(report.read_locks, OTHER_READ_LOCKS + 3);
    assert_eq!(spec.index().routing().num_terminals(), 0);
    assert_eq!((report.retained, report.pruned), (0, terminals));
}

#[test]
fn the_fork_parent_applies_a_large_terminal_in_pieces() {
    let values = 2 * index_gc::scanners::nested::WORK_PER_APPLY as u64 + 1;
    let elements = (1..=values).map(|doc_id| (doc_id, "red".to_string(), format!("v{doc_id:04}")));
    let index = tagged(elements);
    let work = work_of_only_terminal(&index);
    let mut spec = NestedSpec::create(index, []);
    let mut wire = Vec::new();
    serialize(&mut wire, Some(NestedField { field_index: 0 })).unwrap();
    serialize(
        &mut wire,
        Some(NestedTerminalDelta {
            path: vec![WireStep::Tag(b"red".to_vec())],
            work,
        }),
    )
    .unwrap();
    serialize(&mut wire, None::<NestedTerminalDelta>).unwrap();

    let mut stats = GcApplyStats::default();
    let outcome = handle_nested_with(&mut Cursor::new(wire), &mut spec, &mut stats, true).unwrap();
    assert_eq!(outcome, HandleOutcome::Collected);
    assert_eq!(stats.records_removed as u64, values);
    // Three pieces of the terminal's values, then the finish that drops it.
    assert_eq!(spec.writes, 4);
    assert_eq!(spec.index().routing().num_terminals(), 0);
}
