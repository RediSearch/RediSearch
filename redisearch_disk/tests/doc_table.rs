// Skip this entire test file when running with Miri since it uses FFI (RocksDB)
#![cfg(not(miri))]

mod c_mocks;

use document::DocumentType;
use redisearch_disk::{
    database::SpeedbMultithreadedDatabase,
    index_spec::{
        deleted_ids::DeletedIdsStore,
        doc_table::{DocTable, DocumentMetadata},
    },
};
use rqe_iterators::{RQEIterator, SkipToOutcome};
use tempfile::TempDir;

fn get_temp_doc_table() -> DocTable {
    let path = TempDir::new().unwrap();
    let mut opts = speedb::Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);

    let db =
        SpeedbMultithreadedDatabase::open_cf(&opts, path, ["doc_table", "reverse_lookup"]).unwrap();

    DocTable::new(DocumentType::Hash, db, DeletedIdsStore::default()).unwrap()
}

#[test]
fn adding_documents() {
    let doc_table = get_temp_doc_table();

    let doc_1 = DocumentMetadata {
        key: b"doc1".to_vec(),
        score: 1.2,
        flags: 3,
        max_term_freq: 4,
        doc_len: 1,
    };

    let doc_2 = DocumentMetadata {
        key: b"doc2".to_vec(),
        score: 5.6,
        flags: 7,
        max_term_freq: 8,
        doc_len: 3,
    };

    let (doc_id1, old_len1) = doc_table
        .insert_document(
            doc_1.key.as_slice(),
            doc_1.score,
            doc_1.flags,
            doc_1.max_term_freq,
            doc_1.doc_len,
        )
        .unwrap();
    let (doc_id2, old_len2) = doc_table
        .insert_document(
            doc_2.key.as_slice(),
            doc_2.score,
            doc_2.flags,
            doc_2.max_term_freq,
            doc_2.doc_len,
        )
        .unwrap();

    assert_eq!(doc_id1, 1);
    assert_eq!(doc_id2, 2);
    assert_eq!(old_len1, 0, "old_len should be 0 for new document");
    assert_eq!(old_len2, 0, "old_len should be 0 for new document");

    assert!(!doc_table.is_deleted(doc_id1));
    assert!(!doc_table.is_deleted(doc_id2));

    assert_eq!(
        doc_table.get_document_metadata(doc_id1).unwrap().unwrap(),
        doc_1
    );
    assert_eq!(
        doc_table.get_document_metadata(doc_id2).unwrap().unwrap(),
        doc_2
    );
}

#[test]
fn updating_documents() {
    let doc_table = get_temp_doc_table();

    let doc = DocumentMetadata {
        key: b"doc1".to_vec(),
        score: 1.2,
        flags: 3,
        max_term_freq: 4,
        doc_len: 5,
    };

    let (doc_id1, old_len1) = doc_table
        .insert_document(
            doc.key.as_slice(),
            doc.score,
            doc.flags,
            doc.max_term_freq,
            doc.doc_len * 2,
        )
        .unwrap();

    // First insert should not populate old_len (no previous document)
    assert_eq!(
        old_len1, 0,
        "old_len should be 0 for first insert (no previous document)"
    );

    // insert the same key twice
    let (doc_id2, old_len2) = doc_table
        .insert_document(
            doc.key.as_slice(),
            doc.score,
            doc.flags,
            doc.max_term_freq,
            doc.doc_len,
        )
        .unwrap();

    assert_eq!(doc_id1, 1);
    assert_eq!(
        doc_id2, 2,
        "New document ID should be assigned even for same key"
    );
    assert_eq!(
        old_len2,
        doc.doc_len * 2,
        "old_len should be populated with previous doc_len"
    );

    assert!(
        doc_table.is_deleted(doc_id1),
        "Old document should be removed"
    );
    assert!(!doc_table.is_deleted(doc_id2), "New document should exist");

    assert_eq!(
        doc_table.get_document_metadata(doc_id1).unwrap(),
        None,
        "Old document metadata should be None"
    );
    assert_eq!(
        doc_table.get_document_metadata(doc_id2).unwrap().unwrap(),
        doc
    );
}

#[test]
fn deleting_documents() {
    let doc_table = get_temp_doc_table();

    let doc_1 = DocumentMetadata {
        key: b"doc1".to_vec(),
        score: 1.2,
        flags: 3,
        max_term_freq: 4,
        doc_len: 5,
    };

    let doc_2 = DocumentMetadata {
        key: b"doc2".to_vec(),
        score: 5.6,
        flags: 7,
        max_term_freq: 8,
        doc_len: 10,
    };

    // Insert two documents
    let (doc_id1, old_len1) = doc_table
        .insert_document(
            doc_1.key.as_slice(),
            doc_1.score,
            doc_1.flags,
            doc_1.max_term_freq,
            doc_1.doc_len,
        )
        .unwrap();
    assert_eq!(
        old_len1, 0,
        "old_len should be 0 when inserting a new document"
    );

    let (doc_id2, old_len2) = doc_table
        .insert_document(
            doc_2.key.as_slice(),
            doc_2.score,
            doc_2.flags,
            doc_2.max_term_freq,
            doc_2.doc_len,
        )
        .unwrap();
    assert_eq!(
        old_len2, 0,
        "old_len should be 0 when inserting a new document"
    );

    assert_eq!(doc_id1, 1);
    assert_eq!(doc_id2, 2);

    // Verify both documents exist
    assert!(!doc_table.is_deleted(doc_id1));
    assert!(!doc_table.is_deleted(doc_id2));

    // Delete the first document and verify old_len and id are populated
    let (deleted_id, old_len) = doc_table.delete_document_by_key(doc_1.key).unwrap();
    assert_eq!(
        old_len, 5,
        "old_len should be populated with the deleted document's length"
    );
    assert_eq!(
        deleted_id, doc_id1,
        "deleted_id should be populated with the deleted document's ID"
    );

    // Verify the first document is deleted
    assert!(
        doc_table.is_deleted(doc_id1),
        "Deleted document should not be in the table"
    );
    assert_eq!(
        doc_table.get_document_metadata(doc_id1).unwrap(),
        None,
        "Deleted document metadata should be None"
    );

    // Verify the second document still exists
    assert!(
        !doc_table.is_deleted(doc_id2),
        "Second document should still exist"
    );
    assert_eq!(
        doc_table.get_document_metadata(doc_id2).unwrap().unwrap(),
        doc_2
    );

    // Delete the second document and verify the returned values
    let (deleted_id, old_len) = doc_table.delete_document_by_key(b"doc2").unwrap();
    assert_eq!(
        deleted_id, doc_id2,
        "deleted_id should be populated with the deleted document's ID"
    );
    assert_eq!(
        old_len, 10,
        "old_len should be populated with the deleted document's length"
    );

    // Verify both documents are deleted
    assert!(doc_table.is_deleted(doc_id1));
    assert!(doc_table.is_deleted(doc_id2));

    // Deleting a non-existent document should not panic and should return INVALID_DOC_ID (0) and 0 for old_len
    let (deleted_id, old_len) = doc_table.delete_document_by_key(b"non_existent").unwrap();
    assert_eq!(
        old_len, 0,
        "old_len should be 0 when deleting non-existent document"
    );
    assert_eq!(
        deleted_id, 0,
        "deleted_id should be INVALID_DOC_ID (0) when deleting non-existent document"
    );
    assert!(
        !doc_table.is_deleted(deleted_id),
        "Non-existent document should not be in the table and should not have been marked as deleted"
    );
}

#[test]
fn iterator() {
    let doc_table = get_temp_doc_table();

    // Insert multiple documents
    for i in 1..=4 {
        let key = format!("doc{}", i);
        let (_, old_len) = doc_table
            .insert_document(key.as_bytes(), i as f32, i * 10, i * 100, i * 1000)
            .unwrap();
        assert_eq!(old_len, 0, "old_len should be 0 for new documents");
    }

    let mut iter = doc_table.wildcard_iterator(5.0).unwrap();

    assert_eq!(iter.num_estimated(), 4);

    // Read all documents using the iterator
    for i in 1..=3 {
        let result = iter.read().unwrap().unwrap();
        assert_eq!(result.doc_id, i);
        assert_eq!(result.weight, 5.0);
        assert!(!iter.at_eof());
        assert_eq!(iter.last_doc_id(), i);
    }

    let result = iter.read().unwrap().unwrap();
    assert_eq!(result.doc_id, 4);
    assert!(!iter.at_eof());
    assert_eq!(iter.last_doc_id(), 4);

    // Attempt to read past the end
    assert!(iter.read().unwrap().is_none());
    assert!(iter.at_eof());

    // Rewind and read again
    iter.rewind();

    // Read the first document again
    let result = iter.read().unwrap().unwrap();
    assert_eq!(result.doc_id, 1);

    // Skip to a specific document
    let SkipToOutcome::Found(result) = iter.skip_to(3).unwrap().unwrap() else {
        panic!("Expected Found outcome");
    };
    assert_eq!(result.doc_id, 3);
}

#[test]
fn updating_documents_lengths() {
    let doc_table = get_temp_doc_table();

    let doc = DocumentMetadata {
        key: b"doc1".to_vec(),
        score: 1.2,
        flags: 3,
        max_term_freq: 4,
        doc_len: 100,
    };

    // Insert the first document
    let (doc_id1, old_len1) = doc_table
        .insert_document(
            doc.key.as_slice(),
            doc.score,
            doc.flags,
            doc.max_term_freq,
            doc.doc_len,
        )
        .unwrap();

    assert_eq!(doc_id1, 1);
    assert_eq!(old_len1, 0, "old_len should be 0 for new document");

    // Update the same document
    let (doc_id2, old_len2) = doc_table
        .insert_document(
            doc.key.as_slice(),
            doc.score,
            doc.flags,
            doc.max_term_freq,
            200, // new doc_len
        )
        .unwrap();

    assert_eq!(doc_id2, 2, "New document ID should be assigned");
    assert_eq!(
        old_len2, doc.doc_len,
        "old_len should be the previous doc_len"
    );

    // Verify the old document is deleted
    assert!(doc_table.is_deleted(doc_id1));

    // Verify the new document has the updated length
    let new_metadata = doc_table.get_document_metadata(doc_id2).unwrap().unwrap();
    assert_eq!(new_metadata.doc_len, 200);
}
