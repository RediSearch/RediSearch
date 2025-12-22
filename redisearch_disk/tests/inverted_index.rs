// Skip this entire test file when running with Miri since it uses FFI (RocksDB)
#![cfg(not(miri))]

mod c_mocks;

use ffi::{t_docId, t_fieldMask};
use redisearch_disk::{
    index_spec::inverted_index::InvertedIndex, search_disk::SpeedbMultithreadedDatabase,
};
use rqe_iterators::{RQEIterator, RQEValidateStatus, SkipToOutcome};
use tempfile::TempDir;

const FIELD_MASK_ALL: t_fieldMask = t_fieldMask::MAX;
const FIELD_MASK_NONE: t_fieldMask = 0;

fn get_temp_inverted_index() -> (TempDir, InvertedIndex) {
    let path = TempDir::new().unwrap();
    let mut opts = speedb::Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);

    let db = SpeedbMultithreadedDatabase::open_cf(&opts, &path, ["test_inverted_index"]).unwrap();

    (
        path,
        InvertedIndex::new(db, "test_inverted_index".to_string()),
    )
}

#[test]
fn basic() {
    let (_temp_dir, mut ii) = get_temp_inverted_index();

    // Add a term with some documents
    ii.insert("term1".to_string(), 1, 0b1, 5).unwrap();
    ii.insert("term1".to_string(), 3, 0b1, 2).unwrap();

    // Get the iterator for a term
    let mut it = ii.term_iterator("term1", FIELD_MASK_ALL, 1.23).unwrap();

    // Check the initial state of the iterator
    assert_eq!(it.num_estimated(), 3);
    assert!(!it.at_eof());
    assert_eq!(it.last_doc_id(), 0);

    // Read the first document
    let result = it.read().unwrap().unwrap();
    assert_eq!(result.doc_id, 1);
    assert_eq!(result.weight, 1.23);
    assert!(!it.at_eof());
    assert_eq!(it.last_doc_id(), 1);

    // Read the second document
    let result = it.read().unwrap().unwrap();
    assert_eq!(result.doc_id, 3);
    assert_eq!(result.weight, 1.23);
    assert!(!it.at_eof());
    assert_eq!(it.last_doc_id(), 3);

    // Read past the end of the iterator
    assert!(it.read().unwrap().is_none());
    assert_eq!(it.current().unwrap().doc_id, 3);
    assert!(it.at_eof());
    assert_eq!(it.current().unwrap().weight, 1.23);
    assert_eq!(it.last_doc_id(), 3);

    // Reset to test skip to
    it.rewind();
    assert!(!it.at_eof());
    assert_eq!(it.last_doc_id(), 0);

    // Skip to document ID 2
    let SkipToOutcome::NotFound(result) = it.skip_to(2).unwrap().unwrap() else {
        panic!("Expected NotFound outcome");
    };
    assert_eq!(result.doc_id, 3);
    assert_eq!(result.weight, 1.23);
    assert!(!it.at_eof());
    assert_eq!(it.last_doc_id(), 3);

    // Skip to document ID 3
    it.rewind();
    let SkipToOutcome::Found(result) = it.skip_to(3).unwrap().unwrap() else {
        panic!("Expected Found outcome");
    };
    assert_eq!(result.doc_id, 3);
    assert_eq!(result.weight, 1.23);
    assert!(!it.at_eof());
    assert_eq!(it.last_doc_id(), 3);

    // Skip to past the end of the iterator
    assert!(it.skip_to(4).unwrap().is_none());
    assert_eq!(it.current().unwrap().doc_id, 3);
    assert_eq!(it.current().unwrap().weight, 1.23);
    assert!(it.at_eof());
    assert_eq!(it.last_doc_id(), 3);

    // Test revalidation - it will never be needed since our snapshot is static
    assert_eq!(it.revalidate().unwrap(), RQEValidateStatus::Ok);

    // Test a term that does not exist
    let mut it = ii
        .term_iterator("nonexistent", FIELD_MASK_ALL, 4.56)
        .unwrap();

    // Read from the iterator
    assert!(it.read().unwrap().is_none());
    assert!(it.at_eof());
    assert_eq!(it.last_doc_id(), 0);
}

unsafe fn drain_iterator<'index>(mut it: impl RQEIterator<'index>) -> Vec<t_docId> {
    let mut doc_ids = Vec::new();

    while let Ok(Some(result)) = it.read() {
        doc_ids.push(result.doc_id);
    }

    doc_ids
}

#[test]
#[allow(clippy::undocumented_unsafe_blocks)]
fn field_mask() {
    let (_temp_dir, mut ii) = get_temp_inverted_index();

    // Add a term with some documents, each in a separate field.
    ii.insert("term1".to_string(), 1, 0b00000001, 0).unwrap();
    ii.insert("term1".to_string(), 2, 0b00000010, 0).unwrap();
    ii.insert("term1".to_string(), 3, 0b00000100, 0).unwrap();
    ii.insert("term1".to_string(), 4, 0b00001000, 0).unwrap();
    ii.insert("term1".to_string(), 5, 0b00010000, 0).unwrap();
    ii.insert("term1".to_string(), 6, 0b00100000, 0).unwrap();
    ii.insert("term1".to_string(), 7, 0b01000000, 0).unwrap();
    ii.insert("term1".to_string(), 8, 0b10000000, 0).unwrap();

    // Get the iterator for a term, for all fields.
    let it = ii.term_iterator("term1", FIELD_MASK_ALL, 1.23).unwrap();

    let doc_ids = unsafe { drain_iterator(it) };
    assert_eq!(&doc_ids, &[1, 2, 3, 4, 5, 6, 7, 8]);

    // Get the iterator for a term, for some fields.
    let it = ii.term_iterator("term1", 0b10101010, 4.56).unwrap();

    let doc_ids = unsafe { drain_iterator(it) };
    assert_eq!(&doc_ids, &[2, 4, 6, 8]);

    // Get the iterator for a term, for no fields.
    let it = ii.term_iterator("term1", FIELD_MASK_NONE, 7.89).unwrap();

    let doc_ids = unsafe { drain_iterator(it) };
    assert_eq!(doc_ids.len(), 0);
}

#[test]
fn empty_iterator() {
    let (_temp_dir, mut ii) = get_temp_inverted_index();

    {
        // Get the iterator when there are no terms
        let it = ii.term_iterator("some_term", FIELD_MASK_ALL, 1.00).unwrap();

        assert_eq!(it.num_estimated(), 0);
    }

    ii.insert("term".to_string(), 1, 0b1, 5).unwrap();

    // Get the iterator for another term
    let mut it = ii
        .term_iterator("another_term", FIELD_MASK_ALL, 1.00)
        .unwrap();

    assert_eq!(it.num_estimated(), 0);

    // There should be nothing in the iterator
    assert!(it.read().unwrap().is_none());
}

// Make sure an iterator does not start reading into a next term's data
#[test]
fn iterator_stays_with_term() {
    let (_temp_dir, mut ii) = get_temp_inverted_index();

    // Add a term with some documents
    ii.insert("term1".to_string(), 1, 0b1, 5).unwrap();
    ii.insert("term2".to_string(), 3, 0b1, 2).unwrap();

    // Get the iterator for a term
    let mut it = ii.term_iterator("term1", FIELD_MASK_ALL, 1.23).unwrap();

    // Read the first document
    let result = it.read().unwrap().unwrap();
    assert_eq!(result.doc_id, 1);
    assert_eq!(it.num_estimated(), 1);

    // It should not read into term2's data
    assert!(it.read().unwrap().is_none());
}

// Make sure the iterator does not spill into terms with similar prefixes
#[test]
fn iterator_term_with_underscore() {
    let (_temp_dir, mut ii) = get_temp_inverted_index();

    // Add a term with some documents
    ii.insert("term".to_string(), 1, 0b1, 5).unwrap();
    ii.insert("term_somemore".to_string(), 3, 0b1, 2).unwrap();

    // Get the iterator for a term
    let mut it = ii.term_iterator("term", FIELD_MASK_ALL, 1.23).unwrap();

    // Read the first document
    let result = it.read().unwrap().unwrap();
    assert_eq!(result.doc_id, 1);
    assert_eq!(it.num_estimated(), 1);

    // It should not read into term_somemore's data
    assert!(it.read().unwrap().is_none());
}
