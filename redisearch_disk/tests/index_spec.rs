// Skip this entire test file when running with Miri since it uses FFI (RocksDB)
#![cfg(not(miri))]

mod c_mocks;

use document::DocumentType;
use redisearch_disk::index_spec::{IndexSpec, deleted_ids::DeletedIdsStore};
use tempfile::TempDir;

/// Helper function to create a temporary directory for testing
fn get_temp_dir() -> TempDir {
    TempDir::new().unwrap()
}

#[test]
fn test_index_spec_normal_drop_does_not_delete() {
    // Create a temporary directory for the database
    let temp_dir = get_temp_dir();
    let base_path = temp_dir.path().join("test_index_normal_drop");

    // Create an IndexSpec
    let index_name = "test_index_normal_drop".to_string();
    let document_type = DocumentType::Hash;
    let deleted_ids = DeletedIdsStore::new();

    let index_spec = IndexSpec::new(index_name.clone(), document_type, &base_path, deleted_ids)
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

    // Create an IndexSpec
    let index_name = "test_index_marked_deletion".to_string();
    let document_type = DocumentType::Hash;
    let deleted_ids = DeletedIdsStore::new();

    let mut index_spec = IndexSpec::new(index_name.clone(), document_type, &base_path, deleted_ids)
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
