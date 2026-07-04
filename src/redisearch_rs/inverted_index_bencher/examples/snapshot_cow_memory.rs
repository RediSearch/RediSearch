/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Byte-level memory report for the copy-on-write snapshot mechanism (feature branch).
//!
//! Answers the "memory cost of static copy-on-write" question directly, in bytes,
//! via a counting global allocator plus the index's own `GcApplyInfo` accounting.
//! See `docs/design/snapshot-cow-benchmark-plan.md`:
//!
//! - **C1 — per-snapshot reader copy:** heap bytes allocated by one `snapshot()`
//!   (deep copy of the `in_progress` tail buffer + shallow `pending` Vec clone;
//!   `sealed` is a refcount bump = 0 bytes).
//! - **C2 — GC copy-of-pinned-blocks:** extra bytes `apply_gc` allocates when `k`
//!   snapshots pin the blocks (deep-cloned) vs the `k = 0` baseline (blocks moved).
//! - **C3 — retention amplification:** heap outstanding while the snapshots are held
//!   across the GC cycle vs after they are dropped (old blocks that cannot be freed).
//!
//! Run: `cargo run --release --example snapshot_cow_memory -p inverted_index_bencher`

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use ffi::IndexFlags_Index_DocIdsOnly;
use index_result::RSIndexResult;
use inverted_index::{InvertedIndex, numeric::Numeric};

// -- Counting global allocator -------------------------------------------------

static ALLOCATED: AtomicUsize = AtomicUsize::new(0);
static FREED: AtomicUsize = AtomicUsize::new(0);

struct Counting;

// SAFETY: forwards every call to the system allocator unchanged, only adding
// relaxed counter updates; the returned pointers and layouts are exactly those
// System produces, so all GlobalAlloc invariants are preserved.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATED.fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        FREED.fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOCATED.fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if new_size >= layout.size() {
            ALLOCATED.fetch_add(new_size - layout.size(), Ordering::Relaxed);
        } else {
            FREED.fetch_add(layout.size() - new_size, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// Net heap bytes currently outstanding (allocated minus freed).
fn outstanding() -> isize {
    ALLOCATED.load(Ordering::Relaxed) as isize - FREED.load(Ordering::Relaxed) as isize
}

/// Cumulative heap bytes allocated so far.
fn allocated() -> usize {
    ALLOCATED.load(Ordering::Relaxed)
}

// -- Fixtures ------------------------------------------------------------------

const RECORD_COUNTS: [u64; 2] = [10_000, 100_000];
const PINNED: usize = 8;

fn doc_exist(doc_id: u64, total: u64) -> bool {
    doc_id >= (total * 30 / 100)
}

fn build_index(n: u64) -> InvertedIndex<Numeric> {
    let mut ii = InvertedIndex::<Numeric>::new(IndexFlags_Index_DocIdsOnly);
    for doc_id in 0..n {
        ii.add_record(
            &RSIndexResult::build_numeric(doc_id as f64 / 10.0)
                .doc_id(doc_id)
                .build(),
        )
        .unwrap();
    }
    ii
}

fn scan(ii: &InvertedIndex<Numeric>, n: u64) -> inverted_index::GcScanDelta {
    ii.scan_gc(
        |d| doc_exist(d, n),
        None::<fn(&RSIndexResult, &inverted_index::RepairContext<'_>)>,
    )
    .unwrap()
    .unwrap()
}

// -- Report --------------------------------------------------------------------

fn main() {
    println!(
        "{:>9} | {:>14} | {:>14} | {:>16} | {:>16} | {:>14}",
        "records", "index_bytes", "C1_snapshot", "C2_gc_move(k=0)", "C2_gc_cow(k=8)", "C3_retained"
    );
    println!("{}", "-".repeat(100));

    for n in RECORD_COUNTS {
        let ii = build_index(n);
        let index_bytes = ii.memory_usage();

        // C1: heap bytes one snapshot allocates (in_progress deep copy + pending Vec).
        let before = outstanding();
        let snap = ii.snapshot();
        let c1 = outstanding() - before;
        drop(snap);

        // C2 baseline: apply_gc with no live snapshots — surviving blocks are moved,
        // not cloned. Measured as heap allocated during apply_gc (allocator ground
        // truth — the index's own `GcApplyInfo.bytes_allocated` does not count the
        // COW block clones, so it reads 0 here and understates C2).
        let mut ii0 = build_index(n);
        let d0 = scan(&ii0, n);
        let a0 = allocated();
        ii0.apply_gc(d0);
        let gc_move = allocated() - a0;

        // C2 COW: apply_gc while PINNED snapshots pin the blocks — forces deep clone.
        let mut iik = build_index(n);
        let dk = scan(&iik, n);
        let before_gc = outstanding();
        let snaps: Vec<_> = (0..PINNED).map(|_| iik.snapshot()).collect();
        let ak = allocated();
        iik.apply_gc(dk);
        let gc_cow = allocated() - ak;
        // C3: heap still outstanding while the snapshots pin the superseded blocks.
        let retained_held = outstanding() - before_gc;
        drop(snaps);
        // After dropping the snapshots, the superseded blocks are freed; the delta
        // between held and dropped is the retention amplification.
        let retained_dropped = outstanding() - before_gc;
        let c3 = retained_held - retained_dropped;

        println!(
            "{:>9} | {:>14} | {:>14} | {:>16} | {:>16} | {:>14}",
            n, index_bytes, c1, gc_move, gc_cow, c3
        );
    }

    println!(
        "\nC1 = per-reader snapshot copy · C2 = GC block-clone cost (k=8 pins vs k=0) \
         · C3 = bytes pinned alive by 8 held snapshots across a GC cycle"
    );
}
