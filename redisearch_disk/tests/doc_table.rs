// Skip this entire test file when running with Miri since it uses FFI (RocksDB)
#![cfg(not(miri))]

mod c_mocks;

use document::DocumentType;
use redisearch_disk::{
    database::SpeedbMultithreadedDatabase,
    index_spec::{
        deleted_ids::DeletedIdsStore,
        doc_table::{AsyncReadPool, DocTable, DocumentFlag, DocumentFlags, DocumentMetadata},
    },
};
use rqe_iterators::{RQEIterator, SkipToOutcome};
use std::time::{Duration, UNIX_EPOCH};
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
        flags: DocumentFlag::Deleted | DocumentFlag::HasPayload,
        max_term_freq: 4,
        doc_len: 1,
        expiration: None,
    };

    let doc_2 = DocumentMetadata {
        key: b"doc2".to_vec(),
        score: 5.6,
        flags: DocumentFlag::Deleted
            | DocumentFlag::HasPayload
            | DocumentFlag::HasSortVector
            | DocumentFlag::HasExpiration,
        max_term_freq: 8,
        doc_len: 3,
        expiration: Some(UNIX_EPOCH + Duration::new(1706460000, 123456789)),
    };

    let (doc_id1, old_len1) = doc_table
        .insert_document(
            doc_1.key.as_slice(),
            doc_1.score,
            doc_1.flags,
            doc_1.max_term_freq,
            doc_1.doc_len,
            doc_1.expiration,
        )
        .unwrap();
    let (doc_id2, old_len2) = doc_table
        .insert_document(
            doc_2.key.as_slice(),
            doc_2.score,
            doc_2.flags,
            doc_2.max_term_freq,
            doc_2.doc_len,
            doc_2.expiration,
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
        flags: DocumentFlag::Deleted | DocumentFlag::HasPayload,
        max_term_freq: 4,
        doc_len: 5,
        expiration: None,
    };

    let (doc_id1, old_len1) = doc_table
        .insert_document(
            doc.key.as_slice(),
            doc.score,
            doc.flags,
            doc.max_term_freq,
            doc.doc_len * 2,
            doc.expiration,
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
            doc.expiration,
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
        flags: DocumentFlag::Deleted | DocumentFlag::HasPayload,
        max_term_freq: 4,
        doc_len: 5,
        expiration: None,
    };

    let doc_2 = DocumentMetadata {
        key: b"doc2".to_vec(),
        score: 5.6,
        flags: DocumentFlag::Deleted | DocumentFlag::HasPayload | DocumentFlag::HasSortVector,
        max_term_freq: 8,
        doc_len: 10,
        expiration: None,
    };

    // Insert two documents
    let (doc_id1, old_len1) = doc_table
        .insert_document(
            doc_1.key.as_slice(),
            doc_1.score,
            doc_1.flags,
            doc_1.max_term_freq,
            doc_1.doc_len,
            doc_1.expiration,
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
            doc_2.expiration,
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
            .insert_document(
                key.as_bytes(),
                i as f32,
                DocumentFlag::Deleted | DocumentFlag::HasPayload,
                i * 100,
                i * 1000,
                None,
            )
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

    // Insert a few more docs
    for i in 5..=7 {
        let key = format!("doc{}", i);
        let (_, old_len) = doc_table
            .insert_document(
                key.as_bytes(),
                i as f32,
                DocumentFlag::HasPayload.into(),
                i * 100,
                i * 1000,
                None,
            )
            .unwrap();
        assert_eq!(old_len, 0, "old_len should be 0 for new documents");
    }

    // Delete a document to create a gap in the document IDs
    doc_table.delete_document(5).unwrap();

    let mut iter = doc_table.wildcard_iterator(5.0).unwrap();

    // Skip to a specific document
    let SkipToOutcome::Found(result) = iter.skip_to(3).unwrap().unwrap() else {
        panic!("Expected Found outcome");
    };
    assert_eq!(result.doc_id, 3);

    // Skip to a document that does not exist
    let SkipToOutcome::NotFound(result) = iter.skip_to(5).unwrap().unwrap() else {
        panic!("Expected NotFound outcome");
    };

    assert_eq!(
        result.doc_id, 6,
        "Since doc ID 5 does not exist, the next_doc_id should be 6"
    );

    // Skip to the document past the end
    let SkipToOutcome::Found(result) = iter.skip_to(7).unwrap().unwrap() else {
        panic!("Expected Found outcome");
    };

    assert_eq!(result.doc_id, 7);

    // Skip to a document ID that is greater than any existing document
    if iter.skip_to(8).unwrap() != None {
        panic!("Expected None when skipping past the end of the document IDs");
    };
}

#[test]
fn updating_documents_lengths() {
    let doc_table = get_temp_doc_table();

    let doc = DocumentMetadata {
        key: b"doc1".to_vec(),
        score: 1.2,
        flags: DocumentFlag::Deleted | DocumentFlag::HasPayload,
        max_term_freq: 4,
        doc_len: 100,
        expiration: None,
    };

    // Insert the first document
    let (doc_id1, old_len1) = doc_table
        .insert_document(
            doc.key.as_slice(),
            doc.score,
            doc.flags,
            doc.max_term_freq,
            doc.doc_len,
            doc.expiration,
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
            doc.expiration,
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

/// Creates a DocTable configured for async IO testing.
/// Panics if async IO is not available.
fn get_async_temp_doc_table() -> DocTable {
    assert!(
        speedb::DB::is_async_io_available(),
        "Async IO is not available, install liburing-dev to enable it"
    );
    get_temp_doc_table()
}

#[tokio::test]
async fn async_get_document_metadata_found() {
    let doc_table = get_async_temp_doc_table();

    let doc = DocumentMetadata {
        key: b"async_doc".to_vec(),
        score: 3.14,
        flags: DocumentFlag::Deleted | DocumentFlag::HasPayload,
        max_term_freq: 10,
        doc_len: 500,
        expiration: None,
    };

    // Insert a document
    let (doc_id, _) = doc_table
        .insert_document(
            doc.key.as_slice(),
            doc.score,
            doc.flags,
            doc.max_term_freq,
            doc.doc_len,
            None,
        )
        .unwrap();

    // Request async read and await the result
    let result = doc_table.request_document_metadata_async(doc_id).await;
    let retrieved_doc = result.unwrap().expect("Document should be found");

    assert_eq!(retrieved_doc, doc);
}

#[tokio::test]
async fn async_get_document_metadata_not_found() {
    let doc_table = get_async_temp_doc_table();

    // Request async read for a non-existent document
    let non_existent_doc_id = 999;
    let result = doc_table
        .request_document_metadata_async(non_existent_doc_id)
        .await;
    assert!(
        result.unwrap().is_none(),
        "Non-existent document should return None"
    );
}

#[tokio::test]
async fn async_get_multiple_documents_concurrent() {
    let doc_table = get_async_temp_doc_table();

    // Insert multiple documents
    let docs: Vec<DocumentMetadata> = (1..=5)
        .map(|i| DocumentMetadata {
            key: format!("concurrent_doc_{}", i).into_bytes(),
            score: i as f32 * 1.5,
            flags: DocumentFlags::empty(),
            max_term_freq: i * 100,
            doc_len: i * 1000,
            expiration: None,
        })
        .collect();

    let doc_ids: Vec<_> = docs
        .iter()
        .map(|doc| {
            doc_table
                .insert_document(
                    doc.key.as_slice(),
                    doc.score,
                    doc.flags,
                    doc.max_term_freq,
                    doc.doc_len,
                    None,
                )
                .unwrap()
                .0
        })
        .collect();

    // Request all async reads concurrently
    let futures: Vec<_> = doc_ids
        .iter()
        .map(|&doc_id| doc_table.request_document_metadata_async(doc_id))
        .collect();

    // Wait for all results
    for (i, future) in futures.into_iter().enumerate() {
        let result = future.await;
        let retrieved_doc = result.unwrap().expect("Document should be found");
        assert_eq!(retrieved_doc, docs[i], "Document {} should match", i + 1);
    }
}

#[tokio::test]
async fn async_get_deleted_document() {
    let doc_table = get_async_temp_doc_table();

    let doc = DocumentMetadata {
        key: b"to_delete".to_vec(),
        score: 1.0,
        flags: DocumentFlags::empty(),
        max_term_freq: 1,
        doc_len: 1,
        expiration: None,
    };

    // Insert and then delete a document
    let (doc_id, _) = doc_table
        .insert_document(
            doc.key.as_slice(),
            doc.score,
            doc.flags,
            doc.max_term_freq,
            doc.doc_len,
            None,
        )
        .unwrap();

    doc_table.delete_document_by_key(doc.key.clone()).unwrap();

    // Request async read for the deleted document and await result
    let result = doc_table.request_document_metadata_async(doc_id).await;
    assert!(
        result.unwrap().is_none(),
        "Deleted document should return None"
    );
}

#[tokio::test]
async fn async_request_then_do_other_work() {
    let doc_table = get_async_temp_doc_table();

    let doc = DocumentMetadata {
        key: b"async_work".to_vec(),
        score: 2.5,
        flags: DocumentFlags::empty(),
        max_term_freq: 50,
        doc_len: 250,
        expiration: None,
    };

    let (doc_id, _) = doc_table
        .insert_document(
            doc.key.as_slice(),
            doc.score,
            doc.flags,
            doc.max_term_freq,
            doc.doc_len,
            None,
        )
        .unwrap();

    // Request async read
    let future = doc_table.request_document_metadata_async(doc_id);

    // Simulate doing other work while the async read is in progress
    let mut sum = 0u64;
    for i in 0..1000 {
        sum = sum.wrapping_add(i);
    }
    assert!(sum > 0, "Just to prevent optimization");

    // Now await the result
    let result = future.await;
    let retrieved_doc = result.unwrap().expect("Document should be found");
    assert_eq!(retrieved_doc, doc);
}

// ============================================================================
// Async DMD Read Pool Tests
// ============================================================================

#[test]
fn pool_basic_add_and_poll() {
    let doc_table = get_async_temp_doc_table();

    // Insert a document
    let doc = DocumentMetadata {
        key: b"pool_test_doc".to_vec(),
        score: 1.0,
        flags: DocumentFlags::empty(),
        max_term_freq: 1,
        doc_len: 1,
        expiration: None,
    };

    let (doc_id, _) = doc_table
        .insert_document(
            doc.key.as_slice(),
            doc.score,
            doc.flags,
            doc.max_term_freq,
            doc.doc_len,
            None,
        )
        .unwrap();

    // Create a pool and add a read
    let mut pool = AsyncReadPool::new(&doc_table, 10).expect("Failed to create pool");
    assert!(pool.add_read(doc_id, 42), "Should be able to add read");

    // Poll with timeout to get the result
    let mut result_dmd: Option<DocumentMetadata> = None;
    let mut result_user_data: u64 = 0;

    let poll_result = pool.poll_with_callbacks(
        1000,
        1,
        |_doc_id, dmd, user_data| {
            result_dmd = Some(dmd);
            result_user_data = user_data;
        },
        |_| true,
    );

    assert_eq!(poll_result, 0);
    assert_eq!(result_dmd.unwrap(), doc);
    assert_eq!(result_user_data, 42);
}

#[test]
fn pool_capacity_limit() {
    let doc_table = get_async_temp_doc_table();

    // Create a pool with small capacity
    let mut pool = AsyncReadPool::new(&doc_table, 3).expect("Failed to create pool");

    // Add up to capacity
    assert!(pool.add_read(1, 0));
    assert!(pool.add_read(2, 0));
    assert!(pool.add_read(3, 0));

    // Should fail when at capacity
    assert!(!pool.add_read(4, 0), "Should fail when pool is at capacity");
}

#[test]
fn pool_multiple_documents() {
    let doc_table = get_async_temp_doc_table();

    // Insert multiple documents
    let docs: Vec<DocumentMetadata> = (0..5)
        .map(|i| DocumentMetadata {
            key: format!("doc_{}", i).into_bytes(),
            score: i as f32,
            flags: DocumentFlags::empty(),
            max_term_freq: i as u32,
            doc_len: i as u32,
            expiration: None,
        })
        .collect();

    let doc_ids: Vec<_> = docs
        .iter()
        .map(|doc| {
            doc_table
                .insert_document(
                    doc.key.as_slice(),
                    doc.score,
                    doc.flags,
                    doc.max_term_freq,
                    doc.doc_len,
                    None,
                )
                .unwrap()
                .0
        })
        .collect();

    // Create pool and add all reads
    let mut pool = AsyncReadPool::new(&doc_table, 10).expect("Failed to create pool");
    for (i, doc_id) in doc_ids.iter().enumerate() {
        assert!(pool.add_read(*doc_id, i as u64));
    }

    // Poll until we get all results
    let mut all_results: Vec<(DocumentMetadata, u64)> = Vec::new();

    loop {
        let poll_result = pool.poll_with_callbacks(
            1000,
            10,
            |_doc_id, dmd, user_data| {
                all_results.push((dmd, user_data));
            },
            |_| true,
        );
        if poll_result == 0 {
            break;
        }
    }

    assert_eq!(all_results.len(), 5);

    // Verify all documents were retrieved (order may vary)
    for doc in &docs {
        assert!(
            all_results.iter().any(|(dmd, _)| dmd.key == doc.key),
            "Document {:?} not found in results",
            doc.key
        );
    }
}

#[test]
fn pool_non_blocking_poll() {
    let doc_table = get_async_temp_doc_table();

    let mut pool = AsyncReadPool::new(&doc_table, 10).expect("Failed to create pool");

    // Poll empty pool - should return immediately with 0
    let poll_result = pool.poll_with_callbacks(0, 1, |_, _, _| {}, |_| true);
    assert_eq!(poll_result, 0);
}

#[test]
fn pool_not_found_documents_omitted() {
    let doc_table = get_async_temp_doc_table();

    // Insert one document
    let doc = DocumentMetadata {
        key: b"existing_doc".to_vec(),
        score: 1.0,
        flags: DocumentFlags::empty(),
        max_term_freq: 1,
        doc_len: 1,
        expiration: None,
    };

    let (existing_id, _) = doc_table
        .insert_document(
            doc.key.as_slice(),
            doc.score,
            doc.flags,
            doc.max_term_freq,
            doc.doc_len,
            None,
        )
        .unwrap();

    // Create pool and add reads for existing and non-existing documents
    let mut pool = AsyncReadPool::new(&doc_table, 10).expect("Failed to create pool");
    pool.add_read(999999, 0); // Non-existing
    pool.add_read(existing_id, 1); // Existing
    pool.add_read(888888, 2); // Non-existing

    // Poll until done
    let mut all_results: Vec<(DocumentMetadata, u64)> = Vec::new();

    loop {
        let poll_result = pool.poll_with_callbacks(
            1000,
            10,
            |_doc_id, dmd, user_data| {
                all_results.push((dmd, user_data));
            },
            |_| true,
        );
        if poll_result == 0 {
            break;
        }
    }

    // Only the existing document should be in results
    assert_eq!(all_results.len(), 1);
    assert_eq!(all_results[0].0, doc);
    assert_eq!(all_results[0].1, 1);
}

/// Returns the number of threads in the current process by reading /proc/self/task
fn get_thread_count() -> usize {
    std::fs::read_dir("/proc/self/task")
        .map(|entries| entries.count())
        .unwrap_or(1)
}

/// Test that AsyncReadPool doesn't spawn additional threads.
/// The tokio runtime is configured with new_current_thread() which should
/// run all tasks on the calling thread without spawning worker threads.
#[test]
fn pool_does_not_spawn_threads() {
    // Create doc_table first (this may spawn threads for SpeedB)
    let doc_table = get_async_temp_doc_table();

    // Insert a document
    let doc = DocumentMetadata {
        key: b"thread_test_doc".to_vec(),
        score: 1.0,
        flags: DocumentFlags::empty(),
        max_term_freq: 1,
        doc_len: 1,
        expiration: None,
    };

    let (doc_id, _) = doc_table
        .insert_document(
            doc.key.as_slice(),
            doc.score,
            doc.flags,
            doc.max_term_freq,
            doc.doc_len,
            None,
        )
        .unwrap();

    // Measure thread count after doc_table setup but before pool creation
    let thread_count_after_setup = get_thread_count();

    // Create a pool - this creates the tokio runtime
    let mut pool = AsyncReadPool::new(&doc_table, 10).expect("Failed to create pool");

    let thread_count_after_pool = get_thread_count();

    // Add reads and poll - exercises the runtime
    for i in 0..5 {
        pool.add_read(doc_id, i);
    }

    let _ = pool.poll_with_callbacks(1000, 10, |_, _, _| {}, |_| true);

    let thread_count_after_work = get_thread_count();

    // Thread count should not increase from creating the pool or doing work
    // We compare against after_setup to isolate the pool's effect
    assert_eq!(
        thread_count_after_setup, thread_count_after_pool,
        "Creating AsyncReadPool should not spawn threads",
    );
    assert_eq!(
        thread_count_after_pool, thread_count_after_work,
        "Using AsyncReadPool should not spawn threads",
    );
}
