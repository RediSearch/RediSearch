//! Tests for ArchivedBlock - the zero-copy deserialization of term postings list blocks.

use redisearch_disk::index_spec::inverted_index::term::archive::ArchivedBlock;

/// Helper to create a test block with the given doc_ids
fn create_test_block(doc_ids: &[u64]) -> ArchivedBlock {
    let num_docs = doc_ids.len();
    let mut bytes: Vec<u8> = Vec::with_capacity(2 + num_docs * 32);
    bytes.push(0u8); // version 0
    bytes.push(num_docs as u8); // number of docs

    // Write all doc IDs first
    for &doc_id in doc_ids {
        bytes.extend_from_slice(&doc_id.to_le_bytes());
    }

    // Write metadata for each doc
    for (i, _) in doc_ids.iter().enumerate() {
        bytes.extend_from_slice(&((i + 1) as u128).to_le_bytes()); // field_mask
        bytes.extend_from_slice(&((i + 1) as u64).to_le_bytes()); // frequency
    }

    ArchivedBlock::from_bytes(bytes.into())
}

#[test]
fn deserialize_version_0() {
    // Create a byte array representing a version 0 Block with 2 docs
    let mut bytes: Vec<u8> = Vec::with_capacity(2 + 2 * 32);
    bytes.extend_from_slice(&[0u8]); // version 0
    bytes.extend_from_slice(&2u8.to_le_bytes()); // number of docs = 2

    // Doc 1 ID
    bytes.extend_from_slice(&1u64.to_le_bytes()); // doc_id = 1

    // Doc 2 Id
    bytes.extend_from_slice(&2u64.to_le_bytes()); // doc_id = 2

    // Doc 1 Metadata
    bytes.extend_from_slice(&0x10000000000000000000000000000001u128.to_le_bytes()); // field_mask
    bytes.extend_from_slice(&5u64.to_le_bytes()); // frequency = 5

    // Doc 2 Metadata
    bytes.extend_from_slice(&0xFFFFu128.to_le_bytes()); // field_mask
    bytes.extend_from_slice(&10u64.to_le_bytes()); // frequency = 10

    let archived_block = ArchivedBlock::from_bytes(bytes.into());
    assert_eq!(archived_block.version(), 0);
    assert_eq!(archived_block.num_docs(), 2);

    let doc1 = archived_block.get(0).unwrap();
    assert_eq!(doc1.doc_id(), 1);
    assert_eq!(doc1.field_mask(), 0x10000000000000000000000000000001);
    assert_eq!(doc1.frequency(), 5);

    let doc2 = archived_block.get(1).unwrap();
    assert_eq!(doc2.doc_id(), 2);
    assert_eq!(doc2.field_mask(), 0xFFFF);
    assert_eq!(doc2.frequency(), 10);

    assert!(archived_block.get(2).is_none());
}

#[test]
fn binary_search_finds_existing_doc() {
    let block = create_test_block(&[10, 20, 30, 40, 50]);

    // Search for existing doc_ids
    assert_eq!(block.binary_search_by_key(0, &10, |d| d.doc_id()), Ok(0));
    assert_eq!(block.binary_search_by_key(0, &30, |d| d.doc_id()), Ok(2));
    assert_eq!(block.binary_search_by_key(0, &50, |d| d.doc_id()), Ok(4));
}

#[test]
fn binary_search_returns_insert_position_for_missing_doc() {
    let block = create_test_block(&[10, 20, 30, 40, 50]);

    // Search for non-existing doc_ids - should return Err with insert position
    assert_eq!(block.binary_search_by_key(0, &5, |d| d.doc_id()), Err(0)); // before first
    assert_eq!(block.binary_search_by_key(0, &15, |d| d.doc_id()), Err(1)); // between 10 and 20
    assert_eq!(block.binary_search_by_key(0, &25, |d| d.doc_id()), Err(2)); // between 20 and 30
    assert_eq!(block.binary_search_by_key(0, &55, |d| d.doc_id()), Err(5)); // after last
}

#[test]
fn binary_search_with_start_index() {
    let block = create_test_block(&[10, 20, 30, 40, 50]);

    // Search starting from index 2 (doc_id=30)
    assert_eq!(block.binary_search_by_key(2, &30, |d| d.doc_id()), Ok(2));
    assert_eq!(block.binary_search_by_key(2, &40, |d| d.doc_id()), Ok(3));
    assert_eq!(block.binary_search_by_key(2, &50, |d| d.doc_id()), Ok(4));

    // Search for doc before start_index - returns insert position relative to searched range
    assert_eq!(block.binary_search_by_key(2, &25, |d| d.doc_id()), Err(2));
    assert_eq!(block.binary_search_by_key(2, &35, |d| d.doc_id()), Err(3));
}

#[test]
fn binary_search_single_element_block() {
    let block = create_test_block(&[42]);

    assert_eq!(block.binary_search_by_key(0, &42, |d| d.doc_id()), Ok(0));
    assert_eq!(block.binary_search_by_key(0, &10, |d| d.doc_id()), Err(0));
    assert_eq!(block.binary_search_by_key(0, &100, |d| d.doc_id()), Err(1));
}

#[test]
fn get_returns_none_for_out_of_bounds() {
    let block = create_test_block(&[10, 20, 30]);

    assert!(block.get(0).is_some());
    assert!(block.get(2).is_some());
    assert!(block.get(3).is_none());
    assert!(block.get(255).is_none());
}

#[test]
fn get_unchecked_returns_correct_doc() {
    let block = create_test_block(&[10, 20, 30]);

    assert_eq!(block.get_unchecked(0).doc_id(), 10);
    assert_eq!(block.get_unchecked(1).doc_id(), 20);
    assert_eq!(block.get_unchecked(2).doc_id(), 30);
}

#[test]
fn num_docs_returns_correct_count() {
    let block = create_test_block(&[10, 20, 30]);
    assert_eq!(block.num_docs(), 3);

    let empty_block = create_test_block(&[]);
    assert_eq!(empty_block.num_docs(), 0);

    let single_block = create_test_block(&[42]);
    assert_eq!(single_block.num_docs(), 1);
}

#[test]
fn version_returns_zero() {
    let block = create_test_block(&[10, 20]);
    assert_eq!(block.version(), 0);
}

#[test]
fn last_returns_last_doc() {
    let block = create_test_block(&[10, 20, 30]);
    assert_eq!(block.last().unwrap().doc_id(), 30);

    let single_block = create_test_block(&[42]);
    assert_eq!(single_block.last().unwrap().doc_id(), 42);

    let empty_block = create_test_block(&[]);
    assert!(empty_block.last().is_none());
}

#[test]
fn iter_returns_all_docs_in_order() {
    let block = create_test_block(&[10, 20, 30, 40]);

    let doc_ids: Vec<u64> = block.iter().map(|d| d.doc_id()).collect();
    assert_eq!(doc_ids, vec![10, 20, 30, 40]);
}

#[test]
fn iter_has_correct_length() {
    let block = create_test_block(&[10, 20, 30]);
    assert_eq!(block.iter().len(), 3);

    let empty_block = create_test_block(&[]);
    assert_eq!(empty_block.iter().len(), 0);
}
