/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ffi::CStr;
use std::io::Cursor;
use std::mem::{self, ManuallyDrop};
use std::ptr::NonNull;

use field_spec::{FieldSpecBuilder, FieldSpecType, FieldSpecTypes};
use fork_gc::numeric::{NumericField, NumericNodeDelta, collect_numeric, handle_numeric_with};
use fork_gc::util::SpecWriteAccess;
use fork_gc::{GcApplyStats, HandleError, HandleOutcome};
use index_spec::{IndexSpecReadGuard, IndexSpecWriteGuard};
use numeric_range_tree::NumericRangeTree;
use numeric_range_tree::test_utils::{build_single_leaf_tree, build_tree_at_split_edge};
use serde::Serialize as _;

// Link both Rust-provided and C-provided symbols
extern crate redisearch_rs;
// Provide Redis allocator shims so the C dict functions can allocate memory.
redis_mock::mock_or_stub_missing_redis_c_symbols!();

/// An `IndexSpec` owning the field array and numeric trees it points at.
///
/// The spec's `docs` (DocTable) is left zeroed, so `DocTable_Exists` returns
/// `false` for every doc ID — every recorded doc is treated as deleted, which
/// makes each populated tree produce a GC delta.
struct TestSpec {
    spec: ffi::IndexSpec,
    // Keeps the heap buffer `spec.fields` points at alive, and owns the
    // `HiddenString` each field name/path points at (freed in `Drop`).
    field_specs: Vec<ffi::FieldSpec>,
    // Owns the trees the field specs point at; each `Box` is a fixed heap
    // allocation, so the raw pointers stored in `fs.tree` stay valid as more
    // trees are pushed.
    #[expect(
        clippy::vec_box,
        reason = "the field specs retain pointers to individually pinned tree allocations"
    )]
    trees: Vec<Box<NumericRangeTree>>,
    fail_lock_on_attempt: Option<usize>,
    lock_attempts: usize,
}

impl TestSpec {
    /// Build an `IndexSpec` whose fields have the given name, type, and optional numeric tree.
    fn create(fields: Vec<(&CStr, FieldSpecTypes, Option<NumericRangeTree>)>) -> TestSpec {
        let mut trees = Vec::new();
        let mut field_specs = Vec::new();
        for (name, types, tree) in fields {
            let mut fs = FieldSpecBuilder::new(name).with_types(types).finish();
            if let Some(tree) = tree {
                let mut boxed = Box::new(tree);
                fs.tree = NonNull::from(&mut *boxed)
                    .cast::<ffi::NumericRangeTree>()
                    .as_ptr();
                trees.push(boxed);
            }
            field_specs.push(fs);
        }

        // SAFETY: a zeroed IndexSpec is valid for the read-only field access the
        // collect path performs; `fields`/`numFields` are set to a valid array below.
        let mut spec: ffi::IndexSpec = unsafe { mem::zeroed() };

        // The Vec's heap buffer stays put when `field_specs` is moved into the
        // struct, so this pointer remains valid for the lifetime of `TestSpec`.
        spec.fields = field_specs.as_mut_ptr();
        spec.numFields = field_specs.len() as u16;

        // The full handler updates these counters after applying deltas. Give
        // the synthetic spec enough indexed records and bytes for removals.
        spec.stats.numRecords = 1_000_000;
        spec.stats.invertedSize = 1_000_000;

        TestSpec {
            spec,
            field_specs,
            trees,
            lock_attempts: 0,
            fail_lock_on_attempt: None,
        }
    }

    /// Borrow the spec as a read guard, as the collect path receives it.
    fn read_guard(&self) -> ManuallyDrop<IndexSpecReadGuard<'_>> {
        // SAFETY: `spec` is a valid IndexSpec that outlives the guard, and this
        // single-threaded test never mutates it while the guard is held.
        unsafe { IndexSpecReadGuard::from_locked(&self.spec) }
    }
}

impl SpecWriteAccess for TestSpec {
    fn with_write<T, C>(
        &mut self,
        apply: impl FnOnce(&mut IndexSpecWriteGuard<'_>) -> Result<T, HandleError<C>>,
    ) -> Result<T, HandleError<C>> {
        self.lock_attempts += 1;
        if self.fail_lock_on_attempt == Some(self.lock_attempts) {
            return Err(HandleError::SpecDeleted);
        }
        // SAFETY: this test accessor has exclusive access to `self.spec`.
        // `ManuallyDrop` prevents releasing a lock that the test did not acquire.
        let mut guard = unsafe { IndexSpecWriteGuard::from_locked_mut(&mut self.spec) };
        apply(&mut guard)
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

/// A single decoded field from the wire: header + node entries.
struct DecodedField {
    name: Vec<u8>,
    unique_id: u32,
    entries: Vec<NumericNodeDelta>,
}

fn build_two_leaf_tree_with_gc_work() -> NumericRangeTree {
    let (mut tree, split_doc) = build_tree_at_split_edge();
    tree.add(split_doc, split_doc as f64, false, false, 0);
    assert_eq!(tree.num_leaves(), 2);
    tree
}

/// Read one field and its node stream, or `None` at the end of the field stream.
fn read_field(cursor: &mut Cursor<&Vec<u8>>) -> Option<DecodedField> {
    match rmp_serde::from_read::<_, Option<NumericField<Box<[u8]>>>>(&mut *cursor).unwrap() {
        None => None,
        Some(NumericField {
            field_name,
            unique_id,
        }) => {
            let mut entries = Vec::new();
            while let Some(node) =
                rmp_serde::from_read::<_, Option<NumericNodeDelta>>(&mut *cursor).unwrap()
            {
                entries.push(node);
            }
            Some(DecodedField {
                name: field_name.into_vec(),
                unique_id,
                entries,
            })
        }
    }
}

/// Decode the full `collect_numeric` output into a list of fields, asserting
/// the stream is fully consumed at the global terminator.
fn decode_fields(buf: &Vec<u8>) -> Vec<DecodedField> {
    let mut cursor = Cursor::new(buf);
    let mut fields = Vec::new();
    while let Some(field) = read_field(&mut cursor) {
        fields.push(field);
    }
    assert_eq!(
        cursor.position(),
        buf.len() as u64,
        "trailing bytes after the global terminator"
    );
    fields
}

/// Collect a complete numeric stream from `test`.
fn collect_stream(test: &TestSpec) -> Vec<u8> {
    let mut buf = Vec::new();
    collect_numeric(&mut buf, &test.read_guard()).unwrap();
    buf
}

/// Encode one field and the outer stream terminator.
fn encode_field(field: DecodedField) -> Vec<u8> {
    let mut buf = Vec::new();
    Some(NumericField {
        field_name: &field.name,
        unique_id: field.unique_id,
    })
    .serialize(&mut rmp_serde::Serializer::new(&mut buf))
    .unwrap();
    for entry in field.entries {
        Some(entry)
            .serialize(&mut rmp_serde::Serializer::new(&mut buf))
            .unwrap();
    }
    Option::<NumericNodeDelta>::None
        .serialize(&mut rmp_serde::Serializer::new(&mut buf))
        .unwrap();
    Option::<NumericField<&[u8]>>::None
        .serialize(&mut rmp_serde::Serializer::new(&mut buf))
        .unwrap();
    buf
}

/// A spec with no fields writes only the global terminator.
#[test]
#[cfg_attr(miri, ignore = "calls C functions DocTable_Exists / NewHiddenString")]
fn no_fields_writes_only_terminator() {
    let test = TestSpec::create(vec![]);

    let mut buf = Vec::new();
    collect_numeric(&mut buf, &test.read_guard()).unwrap();

    assert!(decode_fields(&buf).is_empty());
}

/// A numeric field whose tree was never initialised (null `tree`) is skipped.
#[test]
#[cfg_attr(miri, ignore = "calls C functions DocTable_Exists / NewHiddenString")]
fn field_without_tree_is_skipped() {
    let test = TestSpec::create(vec![(c"price", FieldSpecType::Numeric.into(), None)]);

    let mut buf = Vec::new();
    collect_numeric(&mut buf, &test.read_guard()).unwrap();

    assert!(decode_fields(&buf).is_empty());
}

/// A field that is neither NUMERIC nor GEO is skipped even if it has a tree.
#[test]
#[cfg_attr(miri, ignore = "calls C functions DocTable_Exists / NewHiddenString")]
fn non_numeric_field_is_skipped() {
    let test = TestSpec::create(vec![(
        c"body",
        FieldSpecType::Fulltext.into(),
        Some(build_single_leaf_tree(2)),
    )]);

    let mut buf = Vec::new();
    collect_numeric(&mut buf, &test.read_guard()).unwrap();

    assert!(decode_fields(&buf).is_empty());
}

/// A numeric field with deleted docs writes a header (name + unique id) and one
/// node delta per tree node before the terminators.
#[test]
#[cfg_attr(miri, ignore = "calls C functions DocTable_Exists / NewHiddenString")]
fn numeric_field_writes_header_and_node_deltas() {
    // A tree split into two leaves, so the field emits two node deltas. Built with
    // `max_depth_range == 0`, the internal node keeps no range, leaving exactly the
    // two leaves with GC work once every doc is deleted.
    let tree = build_two_leaf_tree_with_gc_work();

    let test = TestSpec::create(vec![(c"price", FieldSpecType::Numeric.into(), Some(tree))]);

    let expected_unique_id = u32::from(test.trees[0].unique_id());

    let mut buf = Vec::new();
    collect_numeric(&mut buf, &test.read_guard()).unwrap();

    let fields = decode_fields(&buf);
    assert_eq!(fields.len(), 1);
    assert_eq!(fields[0].name, b"price");
    assert_eq!(fields[0].unique_id, expected_unique_id);
    // Both leaves have GC work, so the field's node stream carries two entries.
    assert_eq!(fields[0].entries.len(), 2);
}

/// NUMERIC and GEO fields are both collected, each with its own header and
/// terminated node stream, in field-array order.
#[test]
#[cfg_attr(miri, ignore = "calls C functions DocTable_Exists / NewHiddenString")]
fn numeric_and_geo_fields_are_both_collected() {
    let test = TestSpec::create(vec![
        (
            c"price",
            FieldSpecType::Numeric.into(),
            Some(build_single_leaf_tree(2)),
        ),
        (
            c"location",
            FieldSpecType::Geo.into(),
            Some(build_single_leaf_tree(2)),
        ),
    ]);

    let mut buf = Vec::new();
    collect_numeric(&mut buf, &test.read_guard()).unwrap();

    let fields = decode_fields(&buf);
    let names: Vec<&[u8]> = fields.iter().map(|f| f.name.as_slice()).collect();
    assert_eq!(names, [b"price".as_slice(), b"location".as_slice()]);
    for field in &fields {
        assert_eq!(field.entries.len(), 1);
    }
}

/// The node-stream handler rejects a record of the wrong shape.
#[test]
fn handle_numeric_with_rejects_a_wrongly_shaped_node_record() {
    let mut buf = Vec::new();
    Some(NumericField {
        field_name: b"price",
        unique_id: 0,
    })
    .serialize(&mut rmp_serde::Serializer::new(&mut buf))
    .unwrap();
    Some(0u32)
        .serialize(&mut rmp_serde::Serializer::new(&mut buf))
        .unwrap();
    let mut test = TestSpec::create(vec![(
        c"price",
        FieldSpecType::Numeric.into(),
        Some(NumericRangeTree::new(false)),
    )]);
    let mut stats = GcApplyStats::default();

    let error =
        handle_numeric_with(&mut Cursor::new(buf), &mut test, &mut stats, false).unwrap_err();
    let HandleError::Codec { msg, source } = error else {
        panic!("expected a codec error, got {error:?}");
    };
    assert_eq!(msg, "decoding numeric node");
    assert!(!source.to_string().is_empty());
}

/// The node-stream handler preserves MessagePack decoding failures.
#[test]
fn handle_numeric_with_preserves_deserialization_error() {
    let invalid_message_pack = [0xc1];
    let mut buf = Vec::new();
    Some(NumericField {
        field_name: b"price",
        unique_id: 0,
    })
    .serialize(&mut rmp_serde::Serializer::new(&mut buf))
    .unwrap();
    buf.extend(invalid_message_pack);
    let mut test = TestSpec::create(vec![(
        c"price",
        FieldSpecType::Numeric.into(),
        Some(NumericRangeTree::new(false)),
    )]);
    let mut stats = GcApplyStats::default();

    let error =
        handle_numeric_with(&mut Cursor::new(buf), &mut test, &mut stats, false).unwrap_err();
    assert!(matches!(
        error,
        HandleError::Codec {
            msg: "decoding numeric node",
            ..
        }
    ));
}

/// With trimming requested, the handler applies every node under its own lock
/// and then compacts the tree, dropping the emptied leaves and their blocks.
#[test]
#[cfg_attr(miri, ignore = "calls C functions DocTable_Exists / NewHiddenString")]
fn handle_numeric_with_applies_field_stream_and_compacts() {
    let mut test = TestSpec::create(vec![(
        c"price",
        FieldSpecType::Numeric.into(),
        Some(build_two_leaf_tree_with_gc_work()),
    )]);
    let mut cursor = Cursor::new(collect_stream(&test));

    let mut stats = GcApplyStats::default();
    let outcome = handle_numeric_with(&mut cursor, &mut test, &mut stats, true).unwrap();

    assert_eq!(outcome, HandleOutcome::Collected);
    assert_eq!(
        test.lock_attempts, 3,
        "two node applications plus final compaction"
    );
    // Every doc is deleted, so both leaves empty out and the tree becomes
    // sparse, and compaction collapses it back to a single leaf. Note that
    // this contributes nothing to `block_count_delta`: the node-level GC has
    // already freed the blocks of the leaf being dropped.
    assert_eq!(test.trees[0].num_leaves(), 1);

    let mut stats = GcApplyStats::default();
    let outcome = handle_numeric_with(&mut cursor, &mut test, &mut stats, true).unwrap();
    assert_eq!(outcome, HandleOutcome::Done);
    assert_eq!(stats, GcApplyStats::default());
    assert_eq!(
        test.lock_attempts, 3,
        "the global terminator does not acquire the spec"
    );
}

/// Statistics from already-applied nodes survive a later failed weak-reference
/// promotion, allowing the production adapter to merge partial progress.
#[test]
#[cfg_attr(miri, ignore = "calls C functions DocTable_Exists / NewHiddenString")]
fn handle_numeric_with_preserves_progress_when_spec_is_deleted() {
    let mut test = TestSpec::create(vec![(
        c"price",
        FieldSpecType::Numeric.into(),
        Some(build_two_leaf_tree_with_gc_work()),
    )]);
    let mut buf = Vec::new();
    collect_numeric(&mut buf, &test.read_guard()).unwrap();
    test.fail_lock_on_attempt = Some(2);

    let mut stats = GcApplyStats::default();
    let error =
        handle_numeric_with(&mut Cursor::new(buf), &mut test, &mut stats, false).unwrap_err();

    assert!(matches!(error, HandleError::SpecDeleted));
    assert_eq!(test.lock_attempts, 2);
    assert!(
        stats.bytes_collected > stats.bytes_allocated,
        "the first node's statistics remain available to the adapter"
    );
}

/// A spec deleted between the last node and the compaction pass is reported as
/// such, leaving the tree untrimmed.
#[test]
#[cfg_attr(miri, ignore = "calls C functions DocTable_Exists / NewHiddenString")]
fn handle_numeric_with_reports_spec_deleted_during_compaction() {
    let mut test = TestSpec::create(vec![(
        c"price",
        FieldSpecType::Numeric.into(),
        Some(build_two_leaf_tree_with_gc_work()),
    )]);
    let buf = collect_stream(&test);
    // The first two writes apply the nodes; the third is the compaction pass.
    test.fail_lock_on_attempt = Some(3);

    let mut stats = GcApplyStats::default();
    let error =
        handle_numeric_with(&mut Cursor::new(buf), &mut test, &mut stats, true).unwrap_err();

    assert!(matches!(error, HandleError::SpecDeleted));
    assert_eq!(test.lock_attempts, 3);
    assert!(
        stats.bytes_collected > 0,
        "both nodes were applied before the failure"
    );
    assert_eq!(
        test.trees[0].num_leaves(),
        2,
        "compaction never ran, so no leaf was trimmed"
    );
}

/// End-to-end: collect the delta for a numeric field whose docs are all
/// deleted, then apply it back to the same tree and confirm GC freed bytes
/// and that the stream ends at the global terminator.
#[test]
#[cfg_attr(miri, ignore = "calls C functions DocTable_Exists / NewHiddenString")]
fn roundtrip_applies_gc_to_tree() {
    let mut test = TestSpec::create(vec![
        (c"body", FieldSpecType::Fulltext.into(), None),
        (
            c"price",
            FieldSpecType::Numeric.into(),
            Some(build_single_leaf_tree(3)),
        ),
    ]);

    // Child side: collect the field's deltas into a buffer.
    let mut buf = Vec::new();
    {
        let guard = test.read_guard();
        collect_numeric(&mut buf, &guard).unwrap();
    }

    let entries_before = test.trees[0].num_entries();

    // Parent side: apply the field through the same handler used by the FFI adapter.
    let mut cursor = Cursor::new(&buf);
    let mut stats = GcApplyStats::default();
    let outcome = handle_numeric_with(&mut cursor, &mut test, &mut stats, false).unwrap();

    assert_eq!(outcome, HandleOutcome::Collected);
    assert_eq!(test.lock_attempts, 1, "compaction was disabled");
    assert!(stats.bytes_collected > 0);
    assert!(test.trees[0].num_entries() < entries_before);

    assert_eq!(
        handle_numeric_with(&mut cursor, &mut test, &mut GcApplyStats::default(), false).unwrap(),
        HandleOutcome::Done
    );
}

/// A stale arena index is counted as a miss rather than failing the field stream.
#[test]
#[cfg_attr(miri, ignore = "calls C functions DocTable_Exists / NewHiddenString")]
fn handle_numeric_with_counts_stale_node_as_missed() {
    let mut test = TestSpec::create(vec![(
        c"price",
        FieldSpecType::Numeric.into(),
        Some(build_single_leaf_tree(2)),
    )]);
    let mut field = decode_fields(&collect_stream(&test)).pop().unwrap();
    field.entries[0].generation = field.entries[0].generation.wrapping_add(1);

    let entries_before = test.trees[0].num_entries();
    let mut stats = GcApplyStats::default();
    let outcome = handle_numeric_with(
        &mut Cursor::new(encode_field(field)),
        &mut test,
        &mut stats,
        false,
    )
    .unwrap();

    assert_eq!(outcome, HandleOutcome::Collected);
    assert_eq!(stats.numeric_nodes_missed, 1);
    assert_eq!(test.trees[0].num_entries(), entries_before);
}

/// Statistics from an applied node survive a truncated later entry.
#[test]
#[cfg_attr(miri, ignore = "calls C functions DocTable_Exists / NewHiddenString")]
fn handle_numeric_with_preserves_progress_on_truncated_stream() {
    let mut test = TestSpec::create(vec![(
        c"price",
        FieldSpecType::Numeric.into(),
        Some(build_two_leaf_tree_with_gc_work()),
    )]);
    let mut field = decode_fields(&collect_stream(&test)).pop().unwrap();
    let second = field.entries.pop().unwrap();
    let first = field.entries.pop().unwrap();

    let mut buf = Vec::new();
    Some(NumericField {
        field_name: &field.name,
        unique_id: field.unique_id,
    })
    .serialize(&mut rmp_serde::Serializer::new(&mut buf))
    .unwrap();
    Some(first)
        .serialize(&mut rmp_serde::Serializer::new(&mut buf))
        .unwrap();
    let mut second_bytes = Vec::new();
    Some(second)
        .serialize(&mut rmp_serde::Serializer::new(&mut second_bytes))
        .unwrap();
    buf.extend_from_slice(&second_bytes[..1]);

    let mut stats = GcApplyStats::default();
    let error =
        handle_numeric_with(&mut Cursor::new(buf), &mut test, &mut stats, false).unwrap_err();

    let HandleError::Codec { msg, source } = error else {
        panic!("expected a codec error, got {error:?}");
    };
    assert_eq!(msg, "decoding numeric node");
    assert!(!source.to_string().is_empty());
    assert!(stats.bytes_collected > stats.bytes_allocated);
}

/// An initialised tree without GC work emits a field with an empty node stream.
#[test]
#[cfg_attr(miri, ignore = "calls C functions DocTable_Exists / NewHiddenString")]
fn handle_numeric_with_accepts_empty_node_stream_without_locking_spec() {
    let mut test = TestSpec::create(vec![(
        c"price",
        FieldSpecType::Numeric.into(),
        Some(NumericRangeTree::new(false)),
    )]);
    let buf = collect_stream(&test);
    let fields = decode_fields(&buf);
    assert_eq!(fields.len(), 1);
    assert!(fields[0].entries.is_empty());

    let mut stats = GcApplyStats::default();
    let outcome = handle_numeric_with(&mut Cursor::new(buf), &mut test, &mut stats, true).unwrap();

    assert_eq!(outcome, HandleOutcome::Collected);
    assert_eq!(test.lock_attempts, 0);
    assert_eq!(stats, GcApplyStats::default());
}

#[cfg(not(miri))]
mod round_trip {
    use super::*;
    use inverted_index::GcScanDelta;
    use numeric_range_tree::{Hll, NodeGcDelta};
    use proptest::prelude::*;

    proptest! {
        /// A `NumericNodeDelta` survives a MessagePack round trip unchanged.
        #[test]
        fn numeric_node_delta_round_trips(
            position in any::<u32>(),
            generation in any::<u32>(),
            registers_with_last_block in any::<[u8; Hll::size()]>(),
            registers_without_last_block in any::<[u8; Hll::size()]>(),
        ) {
            let node = NumericNodeDelta {
                position,
                generation,
                delta: NodeGcDelta {
                    delta: GcScanDelta::empty_for_testing(),
                    registers_with_last_block,
                    registers_without_last_block,
                },
            };

            let mut buf = Vec::new();
            Some(node)
                .serialize(&mut rmp_serde::Serializer::new(&mut buf))
                .unwrap();

            let mut cursor = Cursor::new(&buf);
            let decoded = rmp_serde::from_read::<_, Option<NumericNodeDelta>>(&mut cursor)
                .unwrap()
                .expect("a node record");

            prop_assert_eq!(decoded.position, position);
            prop_assert_eq!(decoded.generation, generation);
            prop_assert_eq!(decoded.delta.delta, GcScanDelta::empty_for_testing());
            prop_assert_eq!(decoded.delta.registers_with_last_block, registers_with_last_block);
            prop_assert_eq!(
                decoded.delta.registers_without_last_block,
                registers_without_last_block
            );
            prop_assert_eq!(cursor.position(), buf.len() as u64, "decode left trailing bytes");
        }
    }
}
