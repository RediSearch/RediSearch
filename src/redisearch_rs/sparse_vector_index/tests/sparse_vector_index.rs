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

use sparse_vector_index::SparseVectorIndex;

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
