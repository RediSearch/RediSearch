use std::collections::HashSet;

use document::DocumentType;
use ffi::{t_docId, t_fieldMask};
use query_term::RSQueryTerm;
use redisearch_disk::disk_context::DiskContext;
use redisearch_disk::index_spec::{IndexSpec, deleted_ids::DeletedIdsStore};
use rqe_iterators::RQEIterator;
use tempfile::TempDir;

const FIELD_MASK_ALL: t_fieldMask = t_fieldMask::MAX;

/// Helper function to create a temporary directory for testing
fn get_temp_dir() -> TempDir {
    TempDir::new().unwrap()
}

/// Helper to create a full IndexSpec for testing.
/// Returns the TempDir (to keep it alive) and the IndexSpec.
fn create_test_index(name: &str) -> (TempDir, IndexSpec) {
    let temp_dir = get_temp_dir();
    let base_path = temp_dir.path().join(name);
    let disk_context = DiskContext::new(&base_path, false);
    let deleted_ids = DeletedIdsStore::new();

    let index = IndexSpec::new(
        name.to_string(),
        DocumentType::Hash,
        &disk_context,
        deleted_ids,
    )
    .expect("Failed to create IndexSpec");

    (temp_dir, index)
}

/// Creates an RSQueryTerm for testing purposes.
/// The caller does not need to free the term - it will be freed when the iterator is dropped.
fn create_query_term(term_str: &str) -> Box<RSQueryTerm> {
    RSQueryTerm::new(term_str.as_bytes(), 0, 0)
}

/// Collects all doc_ids from an inverted index term using the term iterator.
fn collect_term_doc_ids(index: &IndexSpec, term: &str) -> HashSet<t_docId> {
    let mut doc_ids = HashSet::new();
    let query_term = create_query_term(term);
    let mut it = index
        .inverted_index()
        .term_iterator(query_term, FIELD_MASK_ALL, 1.0)
        .unwrap();
    while let Ok(Some(result)) = it.read() {
        doc_ids.insert(result.doc_id);
    }
    doc_ids
}

#[test]
fn test_index_spec_normal_drop_does_not_delete() {
    // Create a temporary directory for the database
    let temp_dir = get_temp_dir();
    let base_path = temp_dir.path().join("test_index_normal_drop");

    // Create a DiskContext with the base path
    let disk_context = DiskContext::new(&base_path, false);

    // Create an IndexSpec
    let index_name = "test_index_normal_drop".to_string();
    let document_type = DocumentType::Hash;
    let deleted_ids = DeletedIdsStore::new();

    let index_spec = IndexSpec::new(
        index_name.clone(),
        document_type,
        &disk_context,
        deleted_ids,
    )
    .expect("Failed to create IndexSpec");

    // The actual database path is constructed by appending _{index_name}_{doc_type} to base_path
    // For example: /tmp/test_index_normal_drop_myindex_hash
    let db_path = {
        let mut path_os = base_path.as_os_str().to_os_string();
        path_os.push(format!("_{}_{}", index_name, document_type));
        std::path::PathBuf::from(path_os)
    };

    // Verify the database directory exists
    assert!(db_path.exists(), "Database directory should exist");

    // Let the index spec go out of scope (normal drop, not calling .drop())
    drop(index_spec);

    // Verify the database directory still exists (normal drop should NOT delete)
    assert!(
        db_path.exists(),
        "Database directory should still exist after normal drop"
    );
}

#[test]
fn test_index_spec_marked_for_deletion_deletes_files() {
    // Create a temporary directory for the database
    let temp_dir = get_temp_dir();
    let base_path = temp_dir.path().join("test_index_marked_deletion");

    // Create a DiskContext with the base path
    let disk_context = DiskContext::new(&base_path, false);

    // Create an IndexSpec
    let index_name = "test_index_marked_deletion".to_string();
    let document_type = DocumentType::Hash;
    let deleted_ids = DeletedIdsStore::new();

    let index_spec = IndexSpec::new(
        index_name.clone(),
        document_type,
        &disk_context,
        deleted_ids,
    )
    .expect("Failed to create IndexSpec");

    // The actual database path is constructed by appending _{index_name}_{doc_type} to base_path
    let db_path = {
        let mut path_os = base_path.as_os_str().to_os_string();
        path_os.push(format!("_{}_{}", index_name, document_type));
        std::path::PathBuf::from(path_os)
    };

    // Verify the database directory exists
    assert!(db_path.exists(), "Database directory should exist");

    // Mark the index for deletion
    index_spec.mark_for_deletion();

    // Drop the index spec
    drop(index_spec);

    // Verify the database directory has been deleted
    assert!(
        !db_path.exists(),
        "Database directory should be deleted after marking for deletion and dropping"
    );
}

#[test]
fn compact_text_inverted_index_removes_deleted_documents() {
    let (_temp_dir, index) = create_test_index("compact_ii_test");

    // Add documents to the inverted index
    let term = "hello";
    let all_doc_ids: Vec<u64> = (1..=100).collect();
    let docs_to_delete: HashSet<u64> = [5, 10, 25, 50, 75, 99].into_iter().collect();

    for &doc_id in &all_doc_ids {
        index
            .inverted_index()
            .insert(term.to_string(), doc_id, 0b1, 1)
            .unwrap();
    }

    // Mark some documents as deleted
    for &doc_id in &docs_to_delete {
        index.doc_table().delete_document(doc_id).unwrap();
    }

    // Verify deleted docs exist before compaction
    let docs_before = collect_term_doc_ids(&index, term);
    for &doc_id in &all_doc_ids {
        assert!(
            docs_before.contains(&doc_id),
            "Doc {} should exist before compaction",
            doc_id
        );
    }

    // Trigger compaction directly via IndexSpec method
    index.compact_text_inverted_index();

    // Verify deleted docs are removed
    let docs_after = collect_term_doc_ids(&index, term);

    // Verify non-deleted docs still exist
    let expected_remaining: HashSet<u64> = all_doc_ids
        .iter()
        .copied()
        .filter(|id| !docs_to_delete.contains(id))
        .collect();
    assert_eq!(docs_after, expected_remaining);
}
