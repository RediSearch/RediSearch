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

    let db = SpeedbMultithreadedDatabase::open_cf(&opts, path, ["doc_table"]).unwrap();

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

    let doc_id1 = doc_table
        .insert_document(
            doc_1.key.as_slice(),
            doc_1.score,
            doc_1.flags,
            doc_1.max_term_freq,
            doc_1.doc_len,
        )
        .unwrap();
    let doc_id2 = doc_table
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

    let doc_id1 = doc_table
        .insert_document(
            doc.key.as_slice(),
            doc.score,
            doc.flags,
            doc.max_term_freq,
            doc.doc_len,
        )
        .unwrap();
    // insert the same key twice
    let doc_id2 = doc_table
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
    let doc_id1 = doc_table
        .insert_document(
            doc_1.key.as_slice(),
            doc_1.score,
            doc_1.flags,
            doc_1.max_term_freq,
            doc_1.doc_len,
        )
        .unwrap();
    let doc_id2 = doc_table
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

    // Verify both documents exist
    assert!(!doc_table.is_deleted(doc_id1));
    assert!(!doc_table.is_deleted(doc_id2));

    // Delete the first document
    doc_table.delete_document(doc_id1).unwrap();

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

    // Delete the second document
    doc_table.delete_document(doc_id2).unwrap();

    // Verify both documents are deleted
    assert!(doc_table.is_deleted(doc_id1));
    assert!(doc_table.is_deleted(doc_id2));

    // Deleting a non-existent document should not panic
    doc_table.delete_document(999).unwrap();
    assert!(
        doc_table.is_deleted(999),
        "Non-existent document should not be in the table"
    );
}

#[test]
fn iterator() {
    let doc_table = get_temp_doc_table();

    // Insert multiple documents
    for i in 1..=4 {
        let key = format!("doc{}", i);
        doc_table
            .insert_document(key.as_bytes(), i as f32, i * 10, i * 100, i * 1000)
            .unwrap();
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
fn last_document_id_recovery() {
    // Create a persistent directory that won't be automatically deleted
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    // First session: create doc table and add documents
    let last_doc_id = {
        let mut opts = speedb::Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let db = SpeedbMultithreadedDatabase::open_cf(&opts, &db_path, ["doc_table"]).unwrap();
        let doc_table = DocTable::new(DocumentType::Hash, db, DeletedIdsStore::default()).unwrap();

        // Insert a few documents
        let doc_id1 = doc_table.insert_document(b"doc1", 1.0, 0, 1, 1).unwrap();
        let doc_id2 = doc_table.insert_document(b"doc2", 2.0, 0, 2, 2).unwrap();
        let doc_id3 = doc_table.insert_document(b"doc3", 3.0, 0, 3, 3).unwrap();

        assert_eq!(doc_id1, 1);
        assert_eq!(doc_id2, 2);
        assert_eq!(doc_id3, 3);

        doc_id3
        // doc_table and db are dropped here
    };

    // Second session: reopen the database and verify last_document_id recovery
    {
        let mut opts = speedb::Options::default();
        opts.create_if_missing(false);
        opts.create_missing_column_families(false);

        let db = SpeedbMultithreadedDatabase::open_cf(&opts, &db_path, ["doc_table"]).unwrap();
        let doc_table = DocTable::new(DocumentType::Hash, db, DeletedIdsStore::default()).unwrap();

        // Verify existing documents are still there
        assert_eq!(
            doc_table.get_document_metadata(1).unwrap().unwrap().key,
            b"doc1".to_vec()
        );
        assert_eq!(
            doc_table.get_document_metadata(2).unwrap().unwrap().key,
            b"doc2".to_vec()
        );
        assert_eq!(
            doc_table.get_document_metadata(3).unwrap().unwrap().key,
            b"doc3".to_vec()
        );

        // Insert a new document - it should get ID 4 (last_doc_id + 1)
        let doc_id4 = doc_table.insert_document(b"doc4", 4.0, 0, 4, 4).unwrap();

        assert_eq!(
            doc_id4,
            last_doc_id + 1,
            "New document ID should be one more than the last ID from previous session"
        );

        // Verify the new document was inserted correctly
        assert_eq!(
            doc_table
                .get_document_metadata(doc_id4)
                .unwrap()
                .unwrap()
                .key,
            b"doc4".to_vec()
        );
    }
}

#[test]
fn last_document_id_recovery_with_deletions() {
    // Create a persistent directory
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    // First session: create doc table, add documents, and delete some
    {
        let mut opts = speedb::Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let db = SpeedbMultithreadedDatabase::open_cf(&opts, &db_path, ["doc_table"]).unwrap();
        let doc_table = DocTable::new(DocumentType::Hash, db, DeletedIdsStore::default()).unwrap();

        // Insert documents
        let doc_id1 = doc_table.insert_document(b"doc1", 1.0, 0, 1, 1).unwrap();
        let doc_id2 = doc_table.insert_document(b"doc2", 2.0, 0, 2, 2).unwrap();
        let doc_id3 = doc_table.insert_document(b"doc3", 3.0, 0, 3, 3).unwrap();
        let doc_id4 = doc_table.insert_document(b"doc4", 4.0, 0, 4, 4).unwrap();
        let doc_id5 = doc_table.insert_document(b"doc5", 5.0, 0, 5, 5).unwrap();

        assert_eq!(doc_id1, 1);
        assert_eq!(doc_id2, 2);
        assert_eq!(doc_id3, 3);
        assert_eq!(doc_id4, 4);
        assert_eq!(doc_id5, 5);

        // Delete some documents (including the last one)
        doc_table.delete_document(doc_id2).unwrap();
        doc_table.delete_document(doc_id5).unwrap();

        // doc_table and db are dropped here
    }

    // Second session: reopen and verify last_document_id recovery
    // Note: When the last document is deleted, recovery uses the highest remaining doc ID
    {
        let mut opts = speedb::Options::default();
        opts.create_if_missing(false);
        opts.create_missing_column_families(false);

        let db = SpeedbMultithreadedDatabase::open_cf(&opts, &db_path, ["doc_table"]).unwrap();
        let doc_table = DocTable::new(DocumentType::Hash, db, DeletedIdsStore::default()).unwrap();

        // Insert a new document - it should get ID 5 (highest remaining doc ID was 4, so next is 5)
        let new_doc_id = doc_table.insert_document(b"doc6", 6.0, 0, 6, 6).unwrap();

        assert_eq!(
            new_doc_id, 5,
            "New document ID should be one more than the highest existing doc ID (4), which is 5"
        );

        // Verify existing non-deleted documents
        assert_eq!(
            doc_table.get_document_metadata(1).unwrap().unwrap().key,
            b"doc1".to_vec()
        );
        assert_eq!(
            doc_table.get_document_metadata(3).unwrap().unwrap().key,
            b"doc3".to_vec()
        );
        assert_eq!(
            doc_table.get_document_metadata(4).unwrap().unwrap().key,
            b"doc4".to_vec()
        );

        // Verify deleted documents are gone
        assert_eq!(doc_table.get_document_metadata(2).unwrap(), None);

        // Verify new document (which reused ID 5)
        assert_eq!(
            doc_table.get_document_metadata(5).unwrap().unwrap().key,
            b"doc6".to_vec()
        );
    }
}

#[test]
fn last_document_id_recovery_empty_table() {
    // Create a persistent directory
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    // First session: create an empty doc table
    {
        let mut opts = speedb::Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let db = SpeedbMultithreadedDatabase::open_cf(&opts, &db_path, ["doc_table"]).unwrap();
        let _doc_table = DocTable::new(DocumentType::Hash, db, DeletedIdsStore::default());

        // Don't insert any documents
        // doc_table and db are dropped here
    }

    // Second session: reopen and verify first document gets ID 1
    {
        let mut opts = speedb::Options::default();
        opts.create_if_missing(false);
        opts.create_missing_column_families(false);

        let db = SpeedbMultithreadedDatabase::open_cf(&opts, &db_path, ["doc_table"]).unwrap();
        let doc_table = DocTable::new(DocumentType::Hash, db, DeletedIdsStore::default()).unwrap();

        // Insert first document - should get ID 1
        let doc_id1 = doc_table.insert_document(b"doc1", 1.0, 0, 1, 1).unwrap();

        assert_eq!(
            doc_id1, 1,
            "First document in an empty table should get ID 1"
        );
    }
}
