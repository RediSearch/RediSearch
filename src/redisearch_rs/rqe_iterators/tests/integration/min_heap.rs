/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use rqe_iterators::utils::DocIdMinHeap;

/// Covers: `new`, `default`, `with_capacity`, `push`, `peek`, `len`,
/// `is_empty`, `clear`, and `[]`.
#[test]
fn construction_and_basic_ops() {
    // new() / default() both start empty.
    let heap_new = DocIdMinHeap::new();
    let heap_def = DocIdMinHeap::default();
    let heap_cap = DocIdMinHeap::with_capacity(100);
    for heap in [&heap_new, &heap_def, &heap_cap] {
        assert!(heap.is_empty());
        assert_eq!(heap.len(), 0);
        assert_eq!(heap.peek(), None);
    }

    // push / peek / len / is_empty
    let mut heap = heap_new;
    heap.push(10, 0);
    assert_eq!(heap.peek(), Some((10, 0)));
    assert_eq!(heap.len(), 1);
    assert!(!heap.is_empty());

    heap.push(5, 1);
    heap.push(15, 2);
    heap.push(3, 3);
    assert_eq!(heap.peek(), Some((3, 3)), "peek must return minimum");

    // [] — root element is the minimum.
    assert_eq!(heap.len(), 4);
    assert_eq!(heap[0].0, 3);

    // clear()
    heap.clear();
    assert!(heap.is_empty());
    assert_eq!(heap.peek(), None);
}

/// Covers: pop from multi-element heap (sorted extraction), single-element
/// pop (the `last_idx == 0` early-return in sift_down), pop on empty heap,
/// and duplicate doc_ids.
#[test]
fn pop_all_variants() {
    let mut heap = DocIdMinHeap::new();

    // Pop on empty heap.
    assert_eq!(heap.pop(), None);

    // Multi-element: sorted extraction.
    for &(doc, idx) in &[(10, 0), (5, 1), (15, 2)] {
        heap.push(doc, idx);
    }
    assert_eq!(heap.pop(), Some((5, 1)));
    assert_eq!(heap.pop(), Some((10, 0)));
    assert_eq!(heap.pop(), Some((15, 2)));
    assert_eq!(heap.pop(), None);

    // Single-element pop (exercises `last_idx == 0` branch).
    heap.push(42, 7);
    assert_eq!(heap.pop(), Some((42, 7)));
    assert!(heap.is_empty());

    // Duplicate doc_ids — only the minimum should be distinct.
    for &(doc, idx) in &[(10, 0), (10, 1), (10, 2), (5, 3), (10, 4)] {
        heap.push(doc, idx);
    }
    assert_eq!(heap.pop(), Some((5, 3)));
    let mut remaining_indices = Vec::new();
    while let Some((doc, idx)) = heap.pop() {
        assert_eq!(doc, 10);
        remaining_indices.push(idx);
    }
    remaining_indices.sort();
    assert_eq!(remaining_indices, vec![0, 1, 2, 4]);
}

/// Covers: `replace_root` with a smaller value (stays at root), a larger
/// value (sifts past all children), a mid-range value (sifts to the middle),
/// and on a single-element heap (sift_down early return when `len <= 1`).
#[test]
fn replace_root() {
    let mut heap = DocIdMinHeap::new();

    // Single-element: sift_down len=1 early return.
    heap.push(100, 0);
    heap.replace_root(50, 1);
    assert_eq!(heap.peek(), Some((50, 1)));
    assert_eq!(heap.len(), 1);
    heap.clear();

    // Build a 4-element heap: [5, 10, 20, 30].
    for &(doc, idx) in &[(5, 0), (10, 1), (20, 2), (30, 3)] {
        heap.push(doc, idx);
    }

    // Smaller value — new root stays on top.
    heap.replace_root(2, 10);
    assert_eq!(heap.peek(), Some((2, 10)));

    // Larger value — sifts past all existing children.
    heap.replace_root(99, 11);
    assert_eq!(heap.peek(), Some((10, 1)));

    // Mid-range value — lands between existing children.
    heap.replace_root(15, 12);
    assert_eq!(heap.peek(), Some((15, 12)));

    // Drain and verify sorted order.
    let mut extracted = Vec::new();
    while let Some((doc, _)) = heap.pop() {
        extracted.push(doc);
    }
    assert!(
        extracted.is_sorted(),
        "expected sorted extraction, got {extracted:?}"
    );
}

/// Stress test: 100 reverse-ordered inserts followed by sorted extraction.
/// Exercises sift_up on every insert and sift_down on every pop.
#[test]
fn sorted_extraction_stress() {
    let mut heap = DocIdMinHeap::new();
    for i in (0u64..100).rev() {
        heap.push(i, i as usize);
    }
    assert_eq!(heap.len(), 100);

    let mut extracted = Vec::new();
    while let Some((doc_id, _)) = heap.pop() {
        extracted.push(doc_id);
    }
    assert!(extracted.is_sorted(), "expected sorted extraction");
    assert_eq!(extracted.len(), 100);
}
