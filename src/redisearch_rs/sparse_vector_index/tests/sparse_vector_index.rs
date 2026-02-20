/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Integration tests for the sparse vector index.

mod c_mocks;

use sparse_vector_index::{SparseVectorIndex, SparseVectorQueryIterator};

#[test]
fn test_new_index() {
    let index = SparseVectorIndex::new();
    assert_eq!(index.num_docs(), 0);
    assert_eq!(index.num_dimensions(), 0);
    assert!(index.is_empty());
}

#[test]
fn test_default_index() {
    let index = SparseVectorIndex::default();
    assert_eq!(index.num_docs(), 0);
    assert_eq!(index.num_dimensions(), 0);
    assert!(index.is_empty());
}

#[test]
fn test_add_single_document() {
    let mut index = SparseVectorIndex::new();

    let entries = [(10, 0.5), (20, 0.3), (30, 0.8)];
    let bytes = index.add(1, &entries).unwrap();

    assert!(bytes > 0);
    assert_eq!(index.num_docs(), 1);
    assert_eq!(index.num_dimensions(), 3);
    assert!(!index.is_empty());

    // Check that dimensions exist
    assert!(index.get_dimension(10).is_some());
    assert!(index.get_dimension(20).is_some());
    assert!(index.get_dimension(30).is_some());
    assert!(index.get_dimension(40).is_none());

    // Check that dimensions have no entry
    assert!(index.get_dimension(1).is_none());
    assert!(index.get_dimension(2).is_none());
    assert!(index.get_dimension(3).is_none());
}

#[test]
fn test_add_multiple_documents() {
    let mut index = SparseVectorIndex::new();

    // Doc 1: dimensions 10, 20
    index.add(1, &[(10, 0.5), (20, 0.3)]).unwrap();
    // Doc 2: dimensions 20, 30 (shares dimension 20 with doc 1)
    index.add(2, &[(20, 0.7), (30, 0.4)]).unwrap();

    assert_eq!(index.num_docs(), 2);
    assert_eq!(index.num_dimensions(), 3);

    // Dimension 20 should have 2 entries
    let dim_20 = index.get_dimension(20).unwrap();
    assert_eq!(dim_20.number_of_entries(), 2);
}

#[test]
fn test_total_entries() {
    let mut index = SparseVectorIndex::new();

    index.add(1, &[(10, 0.5), (20, 0.3)]).unwrap();
    index.add(2, &[(20, 0.7), (30, 0.4), (40, 0.1)]).unwrap();

    // Total: 2 + 3 = 5 entries
    assert_eq!(index.total_entries(), 5);
}

#[test]
fn test_dimension_ids() {
    let mut index = SparseVectorIndex::new();

    index.add(1, &[(10, 0.5), (20, 0.3), (30, 0.8)]).unwrap();

    let mut dim_ids: Vec<_> = index.dimension_ids().collect();
    dim_ids.sort();
    assert_eq!(dim_ids, vec![10, 20, 30]);
}

#[test]
fn test_memory_usage_grows() {
    let mut index = SparseVectorIndex::new();

    let initial_mem = index.memory_usage();
    assert_eq!(initial_mem, 0);

    index.add(1, &[(10, 0.5), (20, 0.3)]).unwrap();
    let after_first = index.memory_usage();
    assert!(after_first > initial_mem);

    index.add(2, &[(10, 0.7), (30, 0.4)]).unwrap();
    let after_second = index.memory_usage();
    assert!(after_second > after_first);
}

// ============================================================================
// Query Tests
// ============================================================================

#[test]
fn test_query_empty_index() {
    let index = SparseVectorIndex::new();
    let query = [(10, 1.0), (20, 0.5)];

    // Query on empty index should return None (no dimensions exist)
    let iter = SparseVectorQueryIterator::new(&index, &query);
    assert!(iter.is_none());
}

#[test]
fn test_query_empty_query() {
    let mut index = SparseVectorIndex::new();
    index.add(1, &[(10, 0.5), (20, 0.3)]).unwrap();

    // Empty query should return None
    let iter = SparseVectorQueryIterator::new(&index, &[]);
    assert!(iter.is_none());
}

#[test]
fn test_query_missing_dimension() {
    let mut index = SparseVectorIndex::new();
    index.add(1, &[(10, 0.5), (20, 0.3)]).unwrap();

    // Query with dimension not in index should return None
    let query = [(10, 1.0), (99, 0.5)]; // dimension 99 doesn't exist
    let iter = SparseVectorQueryIterator::new(&index, &query);
    assert!(iter.is_none());
}

#[test]
fn test_query_single_document_single_dimension() {
    let mut index = SparseVectorIndex::new();
    index.add(1, &[(10, 0.5)]).unwrap();

    let query = [(10, 2.0)];
    let mut iter = SparseVectorQueryIterator::new(&index, &query).unwrap();

    // Should find doc 1
    let result = iter.read().unwrap().unwrap();
    assert_eq!(result.doc_id, 1);

    // No more documents
    assert!(iter.read().unwrap().is_none());
}

#[test]
fn test_query_single_document_multiple_dimensions() {
    let mut index = SparseVectorIndex::new();
    index.add(1, &[(10, 0.5), (20, 0.3), (30, 0.8)]).unwrap();

    let query = [(10, 1.0), (20, 2.0), (30, 0.5)];
    let mut iter = SparseVectorQueryIterator::new(&index, &query).unwrap();

    // Should find doc 1
    let result = iter.read().unwrap().unwrap();
    assert_eq!(result.doc_id, 1);

    assert!(iter.read().unwrap().is_none());
}

#[test]
fn test_query_multiple_documents_all_match() {
    let mut index = SparseVectorIndex::new();
    // Both docs have dimensions 10 and 20
    index.add(1, &[(10, 0.5), (20, 0.3)]).unwrap();
    index.add(2, &[(10, 0.2), (20, 0.8)]).unwrap();

    let query = [(10, 1.0), (20, 0.5)];
    let mut iter = SparseVectorQueryIterator::new(&index, &query).unwrap();

    // Doc 1
    let result1 = iter.read().unwrap().unwrap();
    assert_eq!(result1.doc_id, 1);

    // Doc 2
    let result2 = iter.read().unwrap().unwrap();
    assert_eq!(result2.doc_id, 2);

    assert!(iter.read().unwrap().is_none());
}

#[test]
fn test_query_intersection_filters_documents() {
    let mut index = SparseVectorIndex::new();
    // Doc 1: has dimensions 10, 20
    index.add(1, &[(10, 0.5), (20, 0.3)]).unwrap();
    // Doc 2: has dimensions 20, 30 (missing dimension 10)
    index.add(2, &[(20, 0.7), (30, 0.4)]).unwrap();
    // Doc 3: has dimensions 10, 20, 30
    index.add(3, &[(10, 0.1), (20, 0.2), (30, 0.9)]).unwrap();

    // Query for dimensions 10 and 20 - only docs 1 and 3 have both
    let query = [(10, 1.0), (20, 2.0)];
    let mut iter = SparseVectorQueryIterator::new(&index, &query).unwrap();

    // Doc 1
    let result1 = iter.read().unwrap().unwrap();
    assert_eq!(result1.doc_id, 1);

    // Doc 3
    let result2 = iter.read().unwrap().unwrap();
    assert_eq!(result2.doc_id, 3);

    // Doc 2 is skipped because it doesn't have dimension 10
    assert!(iter.read().unwrap().is_none());
}

#[test]
fn test_query_partial_dimension_match() {
    let mut index = SparseVectorIndex::new();
    // Doc 1: has only dimension 10
    index.add(1, &[(10, 0.5)]).unwrap();
    // Doc 2: has only dimension 20
    index.add(2, &[(20, 0.7)]).unwrap();

    // Query for both dimensions - no document has both
    let query = [(10, 1.0), (20, 2.0)];
    let mut iter = SparseVectorQueryIterator::new(&index, &query).unwrap();

    // No documents match (intersection is empty)
    assert!(iter.read().unwrap().is_none());
}
