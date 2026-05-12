/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Unit tests for the min-max heap building blocks shared by the COLLECT
//! reducer's bounded SORTBY path.

extern crate redisearch_rs;

use std::cmp::Ordering;
use min_max_heap::MinMaxHeap;
use reducers::collect::heap::{EntryKey, HeapEntry};
use value::SharedValue;

redis_mock::mock_or_stub_missing_redis_c_symbols!();

/// Builds an [`EntryKey`] from `f64`s — `f64::NAN` projects to
/// [`Value::Null`] so tests can exercise the missing-worst policy.
fn key(vals: &[f64], asc: u64) -> EntryKey {
    let snapshot: Box<[SharedValue]> = vals
        .iter()
        .map(|v| {
            if v.is_nan() {
                SharedValue::null_static()
            } else {
                SharedValue::new_num(*v)
            }
        })
        .collect();
    EntryKey::new(snapshot, asc)
}

/// Bit `i` ASC.
const fn asc(i: u8) -> u64 {
    1u64 << i
}

#[test]
fn ord_single_asc_smaller_is_better() {
    // ASC + a < b → `a` is "better", so `a > b` under our `Ord`.
    let a = key(&[1.0], asc(0));
    let b = key(&[2.0], asc(0));
    assert_eq!(a.cmp(&b), Ordering::Greater);
    assert_eq!(b.cmp(&a), Ordering::Less);
}

#[test]
fn ord_single_desc_larger_is_better() {
    // DESC (bit 0 clear) + a > b → `a` is better.
    let a = key(&[2.0], 0);
    let b = key(&[1.0], 0);
    assert_eq!(a.cmp(&b), Ordering::Greater);
    assert_eq!(b.cmp(&a), Ordering::Less);
}

#[test]
fn ord_equal_keys_tie() {
    let a = key(&[1.0, 2.0], asc(0) | asc(1));
    let b = key(&[1.0, 2.0], asc(0) | asc(1));
    assert_eq!(a.cmp(&b), Ordering::Equal);
}

#[test]
fn ord_first_key_decides() {
    // ASC + ASC. First key differs, second ignored.
    let a = key(&[1.0, 99.0], asc(0) | asc(1));
    let b = key(&[2.0, 0.0], asc(0) | asc(1));
    assert_eq!(a.cmp(&b), Ordering::Greater);
}

#[test]
fn ord_second_key_decides_when_first_ties() {
    // ASC + ASC. First ties, second decides.
    let a = key(&[1.0, 5.0], asc(0) | asc(1));
    let b = key(&[1.0, 7.0], asc(0) | asc(1));
    assert_eq!(a.cmp(&b), Ordering::Greater);
}

#[test]
fn ord_mixed_directions() {
    // Bit 0 ASC, bit 1 DESC. First ties → second (DESC) decides:
    // a=20 vs b=10 under DESC → a is better.
    let a = key(&[1.0, 20.0], asc(0));
    let b = key(&[1.0, 10.0], asc(0));
    assert_eq!(a.cmp(&b), Ordering::Greater);
}

#[test]
fn ord_missing_ranks_worst_under_asc() {
    // ASC + a present, b missing → b is worst → a > b.
    let a = key(&[1.0], asc(0));
    let b = key(&[f64::NAN], asc(0));
    assert_eq!(a.cmp(&b), Ordering::Greater);
    assert_eq!(b.cmp(&a), Ordering::Less);
}

#[test]
fn ord_missing_ranks_worst_under_desc_too() {
    // DESC + a present, b missing → b is *still* worst (direction-agnostic).
    let a = key(&[1.0], 0);
    let b = key(&[f64::NAN], 0);
    assert_eq!(a.cmp(&b), Ordering::Greater);
}

#[test]
fn ord_both_missing_falls_through() {
    let a = key(&[f64::NAN, 5.0], asc(0) | asc(1));
    let b = key(&[f64::NAN, 7.0], asc(0) | asc(1));
    // Both missing in slot 0 → tie → slot 1 (ASC, 5<7) decides → a > b.
    assert_eq!(a.cmp(&b), Ordering::Greater);
}

#[test]
fn heap_entry_ord_delegates_to_key() {
    // Payload differs; comparator must ignore it entirely.
    let a = HeapEntry::new(key(&[1.0], asc(0)), "payload-a");
    let b = HeapEntry::new(key(&[2.0], asc(0)), "payload-b");
    assert_eq!(a.cmp(&b), Ordering::Greater);
}

#[test]
fn heap_entry_into_parts_decomposes() {
    let e = HeapEntry::new(key(&[7.0], asc(0)), 42u64);
    let (sort_vals, projected) = e.into_parts();
    assert_eq!(sort_vals[0].as_num(), Some(7.0));
    assert_eq!(projected, 42);
}

/// Smoke test: drive the wrapped [`MinMaxHeap<HeapEntry<T>>`][min_max_heap::MinMaxHeap]
/// through the operations [`reducers::collect::storage::Storage::Heap`] needs
/// (`peek_min`, `peek_max`, `push_pop_min`, `iter`, `drain_desc`).
#[test]
fn min_max_heap_top_k_under_asc() {
    let mut heap: MinMaxHeap<HeapEntry<u64>> = MinMaxHeap::with_capacity(3);
    // ASC bit 0 set → smaller is better. Top-3 of {5,1,4,2,3} = {1,2,3}.
    let push = |heap: &mut MinMaxHeap<HeapEntry<u64>>, v: f64| {
        heap.push(HeapEntry::new(key(&[v], asc(0)), v as u64));
    };
    push(&mut heap, 5.0);
    push(&mut heap, 1.0);
    push(&mut heap, 4.0);

    // peek_min = worst surviving = 5; peek_max = best = 1.
    assert_eq!(*heap.peek_min().unwrap().projected(), 5);
    assert_eq!(*heap.peek_max().unwrap().projected(), 1);

    // Bounded skip-or-replace against the worst.
    for v in [2.0, 3.0] {
        let cand = HeapEntry::new(key(&[v], asc(0)), v as u64);
        // Only replace if candidate beats current worst.
        if cand.cmp(heap.peek_min().unwrap()) == Ordering::Greater {
            heap.push_pop_min(cand);
        }
    }

    // Unsorted iter — assert membership.
    let mut members: Vec<u64> = heap.iter().map(|e| *e.projected()).collect();
    members.sort_unstable();
    assert_eq!(members, vec![1, 2, 3]);

    // Sorted drain best→worst = ASC top-K = 1,2,3.
    let drained: Vec<u64> = heap.drain_desc().map(|e| e.into_parts().1).collect();
    assert_eq!(drained, vec![1, 2, 3]);
}
