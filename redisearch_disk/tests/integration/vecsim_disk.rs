//! Tests for the vecsim_disk FFI wrapper, specifically the EdgeListMergeOperator.
//!
//! These tests verify that the Rust FFI wrapper correctly calls the C++ merge operator.

use redisearch_disk::vecsim_disk::EdgeListMergeOperator;
use tempfile::TempDir;

/// Edge size in bytes (u32 = 4 bytes)
const EDGE_SIZE: usize = std::mem::size_of::<u32>();

/// Operation codes matching C++ EdgeListMergeOperator
const OP_APPEND: u8 = b'A';
const OP_DELETE: u8 = b'D';

/// Create an APPEND operand for a single edge (matches C++ CreateAppendOperand)
fn create_append_operand(edge: u32) -> Vec<u8> {
    let mut operand = Vec::with_capacity(1 + EDGE_SIZE);
    operand.push(OP_APPEND);
    operand.extend_from_slice(&edge.to_le_bytes());
    operand
}

/// Create a DELETE operand for a single edge (matches C++ CreateDeleteOperand)
fn create_delete_operand(edge: u32) -> Vec<u8> {
    let mut operand = Vec::with_capacity(1 + EDGE_SIZE);
    operand.push(OP_DELETE);
    operand.extend_from_slice(&edge.to_le_bytes());
    operand
}

/// Extract edges from a merged value (edge list without operation prefix)
fn extract_edges(value: &[u8]) -> Vec<u32> {
    assert!(
        value.len() % EDGE_SIZE == 0,
        "edge list length {} is not a multiple of EDGE_SIZE ({}), possible data corruption",
        value.len(),
        EDGE_SIZE
    );
    value
        .chunks_exact(EDGE_SIZE)
        .map(|chunk| u32::from_le_bytes(chunk.try_into().unwrap()))
        .collect()
}

/// Create a temp SpeedB database with EdgeListMergeOperator configured
fn create_temp_db_with_merge_operator() -> (TempDir, speedb::DB) {
    let path = TempDir::new().unwrap();
    let mut opts = speedb::Options::default();
    opts.create_if_missing(true);
    // Use separate full_merge and partial_merge functions (matches production code in lib.rs)
    // This is critical: partial_merge preserves operand format ('A'/'D' prefix) while
    // full_merge produces raw edge bytes (final value format).
    opts.set_merge_operator(
        "EdgeListMergeOperator",
        EdgeListMergeOperator::full_merge_fn(),
        EdgeListMergeOperator::partial_merge_fn(),
    );

    let db = speedb::DB::open(&opts, &path).unwrap();
    (path, db)
}

/// Tests basic append operations: single append, multiple appends, append order preservation
#[test]
fn test_edge_merge_append_operations() {
    let (_path, db) = create_temp_db_with_merge_operator();

    // Single append
    db.merge(b"key1", &create_append_operand(42)).unwrap();
    let edges = extract_edges(&db.get(b"key1").unwrap().unwrap());
    assert_eq!(edges, vec![42], "single append failed");

    // Multiple appends - verify order preservation
    db.merge(b"key2", &create_append_operand(10)).unwrap();
    db.merge(b"key2", &create_append_operand(20)).unwrap();
    db.merge(b"key2", &create_append_operand(30)).unwrap();
    let edges = extract_edges(&db.get(b"key2").unwrap().unwrap());
    assert_eq!(
        edges,
        vec![10, 20, 30],
        "multiple appends should preserve order"
    );

    // Append to existing value (PUT then MERGE)
    let initial: Vec<u8> = [1u32, 2u32, 3u32]
        .iter()
        .flat_map(|e| e.to_le_bytes())
        .collect();
    db.put(b"key3", &initial).unwrap();
    db.merge(b"key3", &create_append_operand(100)).unwrap();
    let edges = extract_edges(&db.get(b"key3").unwrap().unwrap());
    assert_eq!(edges, vec![1, 2, 3, 100], "append to existing value failed");
}

/// Tests delete operations: basic delete, delete from existing, and edge cases
#[test]
fn test_edge_merge_delete_operations() {
    let (_path, db) = create_temp_db_with_merge_operator();

    // Setup: append edges then delete middle one
    db.merge(b"key1", &create_append_operand(10)).unwrap();
    db.merge(b"key1", &create_append_operand(20)).unwrap();
    db.merge(b"key1", &create_append_operand(30)).unwrap();
    db.merge(b"key1", &create_delete_operand(20)).unwrap();
    let edges = extract_edges(&db.get(b"key1").unwrap().unwrap());
    assert_eq!(edges, vec![10, 30], "delete middle edge failed");

    // Delete from PUT value
    let initial: Vec<u8> = [10u32, 20u32, 30u32]
        .iter()
        .flat_map(|e| e.to_le_bytes())
        .collect();
    db.put(b"key2", &initial).unwrap();
    db.merge(b"key2", &create_delete_operand(20)).unwrap();
    let edges = extract_edges(&db.get(b"key2").unwrap().unwrap());
    assert_eq!(edges, vec![10, 30], "delete from existing value failed");

    // Delete non-existent edge (should be no-op, not crash)
    db.merge(b"key2", &create_delete_operand(999)).unwrap();
    let edges = extract_edges(&db.get(b"key2").unwrap().unwrap());
    assert_eq!(
        edges,
        vec![10, 30],
        "delete non-existent edge should be no-op"
    );

    // Multiple deletes of same edge (second delete is no-op)
    db.merge(b"key2", &create_delete_operand(10)).unwrap();
    db.merge(b"key2", &create_delete_operand(10)).unwrap(); // Already deleted
    let edges = extract_edges(&db.get(b"key2").unwrap().unwrap());
    assert_eq!(edges, vec![30], "multiple deletes of same edge failed");

    // Delete all edges - result should be empty
    db.merge(b"key2", &create_delete_operand(30)).unwrap();
    let value = db.get(b"key2").unwrap().unwrap();
    assert!(
        value.is_empty(),
        "deleting all edges should result in empty value"
    );
}

/// Tests edge cases: empty initial state, large edge lists
#[test]
fn test_edge_merge_edge_cases() {
    let (_path, db) = create_temp_db_with_merge_operator();

    // Delete from non-existent key (no existing value) - should handle gracefully
    db.merge(b"empty", &create_delete_operand(42)).unwrap();
    let value = db.get(b"empty").unwrap().unwrap();
    assert!(
        value.is_empty(),
        "delete from empty should result in empty value"
    );

    // Large edge list (100 edges) - basic stress test
    for i in 0..100u32 {
        db.merge(b"large", &create_append_operand(i)).unwrap();
    }
    let edges = extract_edges(&db.get(b"large").unwrap().unwrap());
    assert_eq!(edges.len(), 100, "large edge list should have 100 edges");
    assert_eq!(edges[0], 0, "first edge should be 0");
    assert_eq!(edges[99], 99, "last edge should be 99");

    // Delete from large list
    db.merge(b"large", &create_delete_operand(50)).unwrap();
    let edges = extract_edges(&db.get(b"large").unwrap().unwrap());
    assert_eq!(edges.len(), 99, "should have 99 edges after delete");
    assert!(!edges.contains(&50), "edge 50 should be deleted");
}
