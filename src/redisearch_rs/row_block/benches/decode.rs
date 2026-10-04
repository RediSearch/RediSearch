/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Decoding one shard chunk the way the coordinator does, with strings copied out of the
//! block and with strings borrowed from it.

// Link both Rust-provided and C-provided symbols
extern crate redisearch_rs;
// Mock or stub the ones that aren't provided by the line above
redis_mock::mock_or_stub_missing_redis_c_symbols!();

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use rlookup::{RLookup, RLookupKeyFlags, RLookupRow};
use row_block::{Block, ColumnFilter, RowBlockWriter, RowReader, TrioMember};
use std::{ffi::CString, hint::black_box, ptr::NonNull};
use value::{SharedBuffer, SharedValue};

/// Rows in a chunk, as a shard sends them by default.
const ROWS: usize = 1000;

/// A chunk shaped like a `LOAD` of a few document fields: two numbers and three strings of
/// the lengths a category, a title and a short tag list typically have.
fn chunk() -> (RLookup<'static>, Vec<u8>) {
    let names = ["id", "price", "category", "title", "tags"];
    let mut lookup = RLookup::new();
    for name in names {
        lookup
            .get_key_write(CString::new(name).unwrap(), RLookupKeyFlags::empty())
            .unwrap();
    }

    let mut writer = RowBlockWriter::new();
    writer
        .write_schema(&lookup, ColumnFilter::default())
        .unwrap();
    for i in 0..ROWS {
        let values = [
            SharedValue::new_num(i as f64),
            SharedValue::new_num(i as f64 * 1.25),
            SharedValue::new_string(format!("category-{}", i % 17).into_bytes()),
            SharedValue::new_string(
                format!("A title of about forty bytes, number {i:05}").into_bytes(),
            ),
            SharedValue::new_string(format!("red,green,blue,{}", i % 7).into_bytes()),
        ];
        let mut row = RLookupRow::new();
        for (key, value) in lookup.iter().zip(values) {
            row.write_key(key, value);
        }
        writer.write_row(&lookup, &row, TrioMember::Middle).unwrap();
    }
    (lookup, writer.as_bytes().to_vec())
}

/// Decodes every row of `block` and drops it, as rows that stream through the coordinator.
fn decode(rows: &[u8], block: &Block<'_>, buffer: Option<&SharedBuffer>) {
    let mut reader = match buffer {
        Some(buffer) => RowReader::sharing(rows, block.kinds(), buffer),
        None => RowReader::new(rows, block.kinds()),
    };
    while !reader.is_exhausted() {
        reader
            .read_row(|col, value| {
                black_box((col, value));
            })
            .unwrap();
    }
}

/// `bytes` in a buffer of its own, shared.
fn shared(bytes: &[u8]) -> SharedBuffer {
    let ptr = redis_mock::allocator::alloc_shim(bytes.len()).cast::<u8>();
    let ptr = NonNull::new(ptr).unwrap();
    // SAFETY: a fresh allocation of `bytes.len()` bytes.
    unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr.as_ptr(), bytes.len()) };
    // SAFETY: the allocation is ours alone, and `release` frees it the way it was made.
    unsafe { SharedBuffer::from_raw(ptr, bytes.len(), release, None) }.unwrap()
}

/// # Safety
///
/// 1. `ptr` must come from [`shared`] and not be freed since.
unsafe fn release(ptr: NonNull<u8>, _len: usize) {
    redis_mock::allocator::free_shim(ptr.as_ptr().cast());
}

fn bench_decode(c: &mut Criterion) {
    let (_lookup, bytes) = chunk();
    let block = Block::parse(&bytes).unwrap();
    let rows_at = bytes.len() - block.row_bytes().len();
    let values = ROWS * block.columns().len();
    let strings = ROWS * 3;
    println!(
        "row block: {} bytes, {:.1} bytes/row; {:.1} bytes/row with a tag per value and \
         unterminated strings",
        bytes.len(),
        block.row_bytes().len() as f64 / ROWS as f64,
        (block.row_bytes().len() + values - strings) as f64 / ROWS as f64,
    );

    let mut group = c.benchmark_group("row_block_decode");
    group.throughput(Throughput::Elements(ROWS as u64));
    group.bench_function("copied_strings", |b| {
        b.iter(|| decode(block.row_bytes(), &block, None));
    });
    group.bench_function("shared_strings", |b| {
        b.iter_batched(
            || shared(&bytes),
            |buffer| {
                let rows = &buffer.as_bytes()[rows_at..];
                decode(rows, &block, Some(&buffer));
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

criterion_group!(benches, bench_decode);
criterion_main!(benches);
