/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ffi::CStr;
use std::io::{Cursor, Read};
use std::mem::{self, ManuallyDrop, size_of};
use std::ptr::NonNull;

use field_spec::{FieldSpecBuilder, FieldSpecType, FieldSpecTypes};
use fork_gc::Frame;
use fork_gc::numeric::collect_numeric;
use index_spec::IndexSpecReadGuard;
use inverted_index::GcScanDelta;
use numeric_range_tree::test_utils::{build_single_leaf_tree, build_tree_at_split_edge};
use numeric_range_tree::{Hll, NumericRangeTree};
use serde::Deserialize as _;

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
    // Keeps the heap buffer `spec.fields` points at alive; never read directly.
    _field_specs: Vec<ffi::FieldSpec>,
    // Owns the trees the field specs point at; each `Box` is a fixed heap
    // allocation, so the raw pointers stored in `fs.tree` stay valid as more
    // trees are pushed.
    trees: Vec<Box<NumericRangeTree>>,
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
        spec.numFields = field_specs.len() as u16;
        // The Vec's heap buffer stays put when `field_specs` is moved into the
        // struct, so this pointer remains valid for the lifetime of `TestSpec`.
        spec.fields = field_specs.as_mut_ptr();

        TestSpec {
            spec,
            _field_specs: field_specs,
            trees,
        }
    }

    /// Borrow the spec as a read guard, as the collect path receives it.
    fn read_guard(&self) -> ManuallyDrop<IndexSpecReadGuard<'_>> {
        // SAFETY: `spec` is a valid IndexSpec that outlives the guard, and this
        // single-threaded test never mutates it while the guard is held.
        unsafe { IndexSpecReadGuard::from_locked(&self.spec) }
    }
}

/// A single decoded numeric node entry from the wire.
struct NodeEntry {
    _position: u32,
    _generation: u32,
    _delta: GcScanDelta,
}

/// A single decoded field from the wire: header + node entries.
struct DecodedField {
    name: Vec<u8>,
    unique_id: u64,
    entries: Vec<NodeEntry>,
}

fn read_arr<const N: usize>(reader: &mut impl Read) -> [u8; N] {
    let mut buf = [0u8; N];
    reader.read_exact(&mut buf).unwrap();
    buf
}

/// Decode one node entry given its already-read `node_len` prefix.
///
/// Splits the trailing HLL register bytes off the payload and confirms the
/// msgpack delta is consumed *exactly* up to that boundary — i.e. `node_len`,
/// the delta, and the two register arrays are all in agreement.
fn read_entry(cursor: &mut Cursor<&Vec<u8>>, node_len: usize) -> NodeEntry {
    let position = u32::from_ne_bytes(read_arr(cursor));
    let generation = u32::from_ne_bytes(read_arr(cursor));

    let payload_len = node_len - 2 * size_of::<u32>();
    let mut payload = vec![0u8; payload_len];
    cursor.read_exact(&mut payload).unwrap();

    let register_bytes = Hll::size() * 2;
    assert!(
        payload_len >= register_bytes,
        "payload too short to contain the HLL registers"
    );
    let (msgpack, registers) = payload.split_at(payload_len - register_bytes);
    assert_eq!(registers.len(), register_bytes);

    // Deserialize the delta from a cursor and assert it stops exactly where the
    // register tail begins. This is the same split the C parent performs, so it
    // guards against `node_len` drifting out of sync with the written bytes.
    let mut delta_cursor = Cursor::new(msgpack);
    let delta =
        GcScanDelta::deserialize(&mut rmp_serde::Deserializer::new(&mut delta_cursor)).unwrap();
    assert_eq!(
        delta_cursor.position(),
        msgpack.len() as u64,
        "delta encoding and HLL registers are misaligned"
    );

    NodeEntry {
        _position: position,
        _generation: generation,
        _delta: delta,
    }
}

/// Read one field's header and node stream, or `None` at the global terminator.
fn read_field(cursor: &mut Cursor<&Vec<u8>>) -> Option<DecodedField> {
    match Frame::decode(cursor).unwrap() {
        Frame::Terminator => None,
        Frame::Data(name) => {
            let unique_id = u64::from_ne_bytes(read_arr(cursor));
            let mut entries = Vec::new();
            loop {
                let node_len = usize::from_ne_bytes(read_arr(cursor));
                // The per-field node stream is terminated by a `usize::MAX` prefix.
                if node_len == usize::MAX {
                    break;
                }
                entries.push(read_entry(cursor, node_len));
            }
            Some(DecodedField {
                name: name.into_inner().into_vec(),
                unique_id,
                entries,
            })
        }
        other => panic!("expected Data frame or global terminator, got {other:?}"),
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
    let (mut tree, split_doc) = build_tree_at_split_edge();
    tree.add(split_doc, split_doc as f64, false, 0);
    assert_eq!(tree.num_leaves(), 2);

    let test = TestSpec::create(vec![(c"price", FieldSpecType::Numeric.into(), Some(tree))]);

    // The unique id is zero-extended from the tree's u32 id to a u64 on the wire.
    let expected_unique_id: u64 = u32::from(test.trees[0].unique_id()).into();

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
