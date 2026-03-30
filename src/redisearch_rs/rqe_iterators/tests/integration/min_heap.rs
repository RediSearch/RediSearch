/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use rqe_iterators::util::DocIdMinHeap;

#[test]
fn test_new_heap_is_empty() {
    let heap = DocIdMinHeap::new();
    assert!(heap.is_empty());
    assert_eq!(heap.len(), 0);
    assert_eq!(heap.peek(), None);
}

#[test]
fn test_push_and_peek() {
    let mut heap = DocIdMinHeap::new();
    heap.push(10, 0);
    assert_eq!(heap.peek(), Some((10, 0)));
    assert_eq!(heap.len(), 1);
}

#[test]
fn test_push_maintains_min_heap() {
    let mut heap = DocIdMinHeap::new();
    heap.push(10, 0);
    heap.push(5, 1);
    heap.push(15, 2);
    heap.push(3, 3);

    assert_eq!(heap.peek(), Some((3, 3)));
}

#[test]
fn test_pop_returns_minimum() {
    let mut heap = DocIdMinHeap::new();
    heap.push(10, 0);
    heap.push(5, 1);
    heap.push(15, 2);

    assert_eq!(heap.pop(), Some((5, 1)));
    assert_eq!(heap.pop(), Some((10, 0)));
    assert_eq!(heap.pop(), Some((15, 2)));
    assert_eq!(heap.pop(), None);
}

#[test]
fn test_pop_empty_returns_none() {
    let mut heap = DocIdMinHeap::new();
    assert_eq!(heap.pop(), None);
}

#[test]
fn test_replace_root_larger_value() {
    let mut heap = DocIdMinHeap::new();
    heap.push(5, 0);
    heap.push(10, 1);
    heap.push(15, 2);

    // Replace root (5) with larger value (20)
    heap.replace_root(20, 3);

    // New minimum should be 10
    assert_eq!(heap.peek(), Some((10, 1)));
}

#[test]
fn test_replace_root_smaller_value() {
    let mut heap = DocIdMinHeap::new();
    heap.push(10, 0);
    heap.push(20, 1);
    heap.push(30, 2);

    // Replace root (10) with smaller value (5)
    heap.replace_root(5, 3);

    // Root should still be at top (smallest)
    assert_eq!(heap.peek(), Some((5, 3)));
}

#[test]
fn test_clear() {
    let mut heap = DocIdMinHeap::new();
    heap.push(10, 0);
    heap.push(5, 1);
    heap.clear();

    assert!(heap.is_empty());
    assert_eq!(heap.peek(), None);
}

#[test]
fn test_with_capacity() {
    let heap = DocIdMinHeap::with_capacity(100);
    assert!(heap.is_empty());
}

#[test]
fn test_sorted_extraction() {
    // Test that repeated pop() returns elements in sorted order
    let mut heap = DocIdMinHeap::new();
    let values = [50, 30, 70, 10, 90, 20, 80, 40, 60];

    for (idx, &val) in values.iter().enumerate() {
        heap.push(val, idx);
    }

    let mut extracted = Vec::new();
    while let Some((doc_id, _)) = heap.pop() {
        extracted.push(doc_id);
    }

    // Should be sorted in ascending order
    let mut sorted = extracted.clone();
    sorted.sort();
    assert_eq!(extracted, sorted);
}

#[test]
fn test_data_access() {
    let mut heap = DocIdMinHeap::new();
    heap.push(5, 0);
    heap.push(10, 1);
    heap.push(15, 2);

    // Access underlying data directly
    let data = heap.data();
    assert_eq!(data.len(), 3);
    // Root should have minimum doc_id
    assert_eq!(data[0].0, 5);
}

