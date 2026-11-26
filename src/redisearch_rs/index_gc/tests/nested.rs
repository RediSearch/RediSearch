/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! What the NESTED scanner sends for a terminal, and that it survives the pipe.

use std::io::Cursor;

use index_gc::fork::wire::{deserialize, serialize};
use index_gc::scanners::nested::{NestedField, NestedTerminalDelta, TerminalWork, scan_terminal};
use nested_index::{
    Element, FloatCompression, IndexParams, NestedIndex, Tag, TagLevel, TerminalKind, TerminalValue,
};
use serde_bytes::ByteBuf;

extern crate redisearch_rs;
redis_mock::mock_or_stub_missing_redis_c_symbols!();

const PARAMS: IndexParams = IndexParams { max_depth_range: 0 };

/// A `color` and a `size` tag level over `terminal`, with documents 1 and 2 on one path.
fn index_with(
    terminal: TerminalKind,
    value: impl Fn(u64) -> TerminalValue<'static>,
) -> NestedIndex {
    let levels = vec![TagLevel::new(), TagLevel::new()];
    let mut index = NestedIndex::new(levels, terminal).unwrap();
    let path: [&[u8]; 2] = [b"red", b"m"];
    for doc_id in 1..=2 {
        let element = Element {
            path: &path,
            value: value(doc_id),
        };
        index.index_document(doc_id, &[element], PARAMS).unwrap();
    }
    index
}

fn only_terminal(index: &NestedIndex) -> (Vec<Vec<u8>>, Option<TerminalWork>) {
    let mut found = Vec::new();
    index.routing().for_each_terminal(|path, terminal| {
        found.push((path.to_vec(), scan_terminal(terminal, |id| id != 2).work))
    });
    assert_eq!(found.len(), 1);
    found.pop().unwrap()
}

#[test]
fn a_numeric_terminal_sends_its_nodes() {
    let index = index_with(TerminalKind::Numeric(FloatCompression::Off), |id| {
        TerminalValue::Numeric(id as f64)
    });
    let (path, work) = only_terminal(&index);
    assert_eq!(path, [b"red".to_vec(), b"m".to_vec()]);
    assert!(matches!(work, Some(TerminalWork::Numeric { ref nodes, .. }) if !nodes.is_empty()));

    let mut terminal = None;
    index
        .routing()
        .for_each_terminal(|_, t| terminal = Some(scan_terminal(t, |_| true)));
    let scan = terminal.unwrap();
    assert_eq!(scan.lists_scanned, 1, "the tree's one range was scanned");
    assert!(
        scan.work.is_none() && scan.failed.is_empty(),
        "nothing is gone, so there is nothing to send"
    );
}

#[test]
fn a_tag_terminal_sends_its_values_through_the_pipe() {
    let index = index_with(TerminalKind::Tag, |id| {
        TerminalValue::Tag(Tag::new(if id == 1 { b"steel" } else { b"wood" }).unwrap())
    });
    let (path, work) = only_terminal(&index);
    let Some(TerminalWork::Tag(values)) = work else {
        panic!("a tag terminal sends tag values");
    };
    assert_eq!(
        values.iter().map(|v| v.tag.as_slice()).collect::<Vec<_>>(),
        [b"wood"]
    );

    let mut wire = Vec::new();
    serialize(&mut wire, Some(NestedField { field_index: 3 })).unwrap();
    serialize(
        &mut wire,
        Some(NestedTerminalDelta {
            path: path.iter().cloned().map(ByteBuf::from).collect(),
            work: TerminalWork::Tag(values),
        }),
    )
    .unwrap();
    serialize(&mut wire, None::<NestedTerminalDelta>).unwrap();

    let mut reader = Cursor::new(wire);
    let field: Option<NestedField> = deserialize(&mut reader, "field").unwrap();
    assert_eq!(field.map(|f| f.field_index), Some(3));
    let delta: Option<NestedTerminalDelta> = deserialize(&mut reader, "delta").unwrap();
    let delta = delta.expect("one terminal was sent");
    let received: Vec<Vec<u8>> = delta.path.into_iter().map(ByteBuf::into_vec).collect();
    assert_eq!(received, path);
    assert!(matches!(delta.work, TerminalWork::Tag(ref values) if values.len() == 1));
    let end: Option<NestedTerminalDelta> = deserialize(&mut reader, "end").unwrap();
    assert!(end.is_none());
}
