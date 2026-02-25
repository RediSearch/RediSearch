use std::io::Cursor;
use std::sync::{Arc, RwLock};

use ffi::t_docId;
use redis_module::raw::{RedisModuleIO, load_unsigned, save_unsigned};
use roaring::RoaringTreemap;

/// The DeletedIds struct uses a roaring treemap to efficiently store and query
/// which document IDs have been deleted from the index.
///
/// Roaring treemaps provide excellent compression and fast operations for sparse
/// sets of integers, making them ideal for tracking deleted document IDs.
#[derive(Default, Debug, Clone)]
pub struct DeletedIds {
    /// Roaring treemap storing the deleted document IDs
    ids: RoaringTreemap,
}

impl DeletedIds {
    /// Creates a new empty DeletedIds set.
    pub fn new() -> Self {
        Self {
            ids: RoaringTreemap::new(),
        }
    }

    /// Marks a document ID as deleted.
    ///
    /// # Arguments
    /// * `doc_id` - The document ID to mark as deleted
    ///
    /// # Returns
    /// `true` if the document ID was newly inserted (wasn't already deleted),
    /// `false` if it was already marked as deleted
    pub fn mark_deleted(&mut self, doc_id: t_docId) -> bool {
        self.ids.insert(doc_id)
    }

    /// Checks if a document ID is marked as deleted.
    ///
    /// # Arguments
    /// * `doc_id` - The document ID to check
    ///
    /// # Returns
    /// `true` if the document ID is marked as deleted, `false` otherwise
    pub fn is_deleted(&self, doc_id: t_docId) -> bool {
        self.ids.contains(doc_id)
    }

    /// Removes a document ID from the deleted set (undeletes it).
    ///
    /// # Arguments
    /// * `doc_id` - The document ID to remove from the deleted set
    ///
    /// # Returns
    /// `true` if the document ID was present and removed, `false` if it wasn't in the set
    pub fn remove(&mut self, doc_id: t_docId) -> bool {
        self.ids.remove(doc_id)
    }

    /// Removes multiple document IDs from the deleted set.
    ///
    /// This is more efficient than calling `remove` repeatedly as it only
    /// acquires the lock once.
    ///
    /// # Arguments
    /// * `doc_ids` - An iterator of document IDs (or references) to remove from the deleted set
    ///
    /// # Returns
    /// The number of document IDs that were present and removed
    pub fn remove_batch(&mut self, doc_ids: impl IntoIterator<Item = t_docId>) -> usize {
        doc_ids
            .into_iter()
            .filter(|doc_id| self.ids.remove(*doc_id))
            .count()
    }

    /// Returns the count of deleted document IDs.
    ///
    /// # Returns
    /// The number of deleted document IDs
    fn len(&self) -> u64 {
        self.ids.len()
    }

    /// Checks if there are no deleted document IDs.
    ///
    /// # Returns
    /// `true` if there are no deleted IDs, `false` otherwise
    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    /// Collects all deleted document IDs into a vector.
    ///
    /// # Returns
    /// A vector containing all deleted document IDs in sorted order
    fn collect_all(&self) -> Vec<t_docId> {
        self.ids.iter().collect()
    }

    /// Saves the deleted IDs to RDB.
    ///
    /// The format is:
    /// 1. Size of serialized data (u64)
    /// 2. Serialized RoaringTreemap bytes
    ///
    /// # Arguments
    /// * `rdb` - The RedisModuleIO handle for RDB operations. This should only be called
    ///   with a valid pointer provided by Redis in an RDB save callback context.
    ///
    /// # Panics
    /// Returns an error if serialization fails.
    pub fn save_to_rdb(&self, rdb: *mut RedisModuleIO) -> Result<(), String> {
        // Serialize the RoaringTreemap to bytes
        let mut serialized = Vec::new();
        self.ids
            .serialize_into(&mut serialized)
            .map_err(|e| format!("Failed to serialize deleted IDs: {}", e))?;

        // Save the size first, then the bytes
        save_unsigned(rdb, serialized.len() as u64);
        redis_module::raw::save_slice(rdb, &serialized);

        Ok(())
    }

    /// Loads the deleted IDs from RDB.
    ///
    /// # Arguments
    /// * `rdb` - The RedisModuleIO handle for RDB operations. This should only be called
    ///   with a valid pointer provided by Redis in an RDB load callback context.
    ///
    /// # Returns
    /// A new `DeletedIds` instance loaded from RDB
    ///
    /// # Errors
    /// Returns an error if loading fails.
    pub fn load_from_rdb(rdb: *mut RedisModuleIO) -> Result<Self, String> {
        // Load the size of the serialized data
        let size = load_unsigned(rdb)
            .map_err(|e| format!("Failed to load deleted IDs size from RDB: {:?}", e))?;

        // Load the serialized bytes
        let buffer = redis_module::raw::load_string_buffer(rdb)
            .map_err(|e| format!("Failed to load deleted IDs buffer from RDB: {:?}", e))?;

        // Verify the size matches
        let buffer_slice = buffer.as_ref();
        if buffer_slice.len() != size as usize {
            return Err(format!(
                "Deleted IDs buffer size mismatch: expected {}, got {}",
                size,
                buffer_slice.len()
            ));
        }

        // Deserialize the RoaringTreemap
        let cursor = Cursor::new(buffer_slice);
        let ids = RoaringTreemap::deserialize_from(cursor)
            .map_err(|e| format!("Failed to deserialize deleted IDs: {}", e))?;

        Ok(DeletedIds { ids })
    }
}

/// Simple thread-safe store for [`DeletedIds`].
#[derive(Default, Clone)]
pub struct DeletedIdsStore(Arc<RwLock<DeletedIds>>);

impl DeletedIdsStore {
    pub fn new() -> Self {
        Self::with_deleted_ids(DeletedIds::new())
    }

    pub fn with_deleted_ids(deleted_ids: DeletedIds) -> Self {
        DeletedIdsStore(Arc::new(RwLock::new(deleted_ids)))
    }

    pub fn mark_deleted(&self, doc_id: t_docId) -> bool {
        self.0.write().unwrap().mark_deleted(doc_id)
    }

    pub fn is_deleted(&self, doc_id: t_docId) -> bool {
        self.0.read().unwrap().is_deleted(doc_id)
    }

    pub fn remove(&self, doc_id: t_docId) -> bool {
        self.0.write().unwrap().remove(doc_id)
    }

    /// Removes multiple document IDs from the deleted set.
    ///
    /// This is more efficient than calling `remove` repeatedly as it only
    /// acquires the lock once.
    ///
    /// # Arguments
    /// * `doc_ids` - An iterator of document IDs (or references) to remove from the deleted set
    ///
    /// # Returns
    /// The number of document IDs that were present and removed
    pub fn remove_batch(&self, doc_ids: impl IntoIterator<Item = t_docId>) -> usize {
        self.0.write().unwrap().remove_batch(doc_ids)
    }

    /// Returns the count of deleted document IDs.
    ///
    /// # Returns
    /// The number of deleted document IDs
    pub fn len(&self) -> u64 {
        self.0.read().unwrap().len()
    }

    /// Returns whether the deleted IDs store is empty.
    ///
    /// # Returns
    /// `true` if there are no deleted document IDs, `false` otherwise
    pub fn is_empty(&self) -> bool {
        self.0.read().unwrap().is_empty()
    }

    /// Collects all deleted document IDs into a vector.
    ///
    /// # Returns
    /// A vector containing all deleted document IDs in sorted order
    pub fn collect_all(&self) -> Vec<t_docId> {
        self.0.read().unwrap().collect_all()
    }

    /// Saves the deleted IDs to RDB.
    ///
    /// # Arguments
    /// * `rdb` - The RedisModuleIO handle for RDB operations. This should only be called
    ///   with a valid pointer provided by Redis in an RDB save callback context.
    ///
    /// # Panics
    /// Panics if the read lock cannot be acquired.
    pub fn save_to_rdb(&self, rdb: *mut RedisModuleIO) -> Result<(), String> {
        self.0
            .read()
            .expect("Failed to acquire deleted-ids read lock")
            .save_to_rdb(rdb)
    }

    /// Loads the deleted IDs from RDB and returns a new DeletedIdsStore.
    ///
    /// # Arguments
    /// * `rdb` - The RedisModuleIO handle for RDB operations. This should only be called
    ///   with a valid pointer provided by Redis in an RDB load callback context.
    ///
    /// # Returns
    /// A new `DeletedIdsStore` instance loaded from RDB
    pub fn load_from_rdb(rdb: *mut RedisModuleIO) -> Result<Self, String> {
        let deleted_ids = DeletedIds::load_from_rdb(rdb)?;
        Ok(DeletedIdsStore::with_deleted_ids(deleted_ids))
    }

    /// Replaces the current deleted IDs with the ones loaded from RDB.
    ///
    /// # Arguments
    /// * `rdb` - The RedisModuleIO handle for RDB operations. This should only be called
    ///   with a valid pointer provided by Redis in an RDB load callback context.
    ///
    /// # Panics
    /// Panics if the write lock cannot be acquired.
    pub fn replace_from_rdb(&self, rdb: *mut RedisModuleIO) -> Result<(), String> {
        let deleted_ids = DeletedIds::load_from_rdb(rdb)?;
        *self
            .0
            .write()
            .expect("Failed to acquire deleted-ids write lock") = deleted_ids;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mark_deleted() {
        let mut deleted_ids = DeletedIds::new();

        // First insertion should return true
        assert!(deleted_ids.mark_deleted(1));
        assert!(deleted_ids.mark_deleted(5));
        assert!(deleted_ids.mark_deleted(100));

        // Duplicate insertion should return false
        assert!(!deleted_ids.mark_deleted(1));
        assert!(!deleted_ids.mark_deleted(5));
    }

    #[test]
    fn test_is_deleted() {
        let mut deleted_ids = DeletedIds::new();

        assert!(!deleted_ids.is_deleted(1));

        deleted_ids.mark_deleted(1);
        assert!(deleted_ids.is_deleted(1));
        assert!(!deleted_ids.is_deleted(2));
    }

    #[test]
    fn test_remove() {
        let mut deleted_ids = DeletedIds::new();

        deleted_ids.mark_deleted(1);
        assert!(deleted_ids.is_deleted(1));

        // Remove should return true and remove the ID
        assert!(deleted_ids.remove(1));
        assert!(!deleted_ids.is_deleted(1));

        // Removing again should return false
        assert!(!deleted_ids.remove(1));
    }

    #[test]
    fn test_roaring_treemap_serialization() {
        // Test that RoaringTreemap serialization works correctly
        let mut deleted_ids = DeletedIds::new();
        deleted_ids.mark_deleted(1);
        deleted_ids.mark_deleted(5);
        deleted_ids.mark_deleted(100);
        deleted_ids.mark_deleted(1000);
        deleted_ids.mark_deleted(u64::MAX);

        // Serialize to bytes
        let mut serialized = Vec::new();
        deleted_ids
            .ids
            .serialize_into(&mut serialized)
            .expect("Failed to serialize");

        // Deserialize from bytes
        let cursor = Cursor::new(&serialized[..]);
        let loaded_treemap =
            RoaringTreemap::deserialize_from(cursor).expect("Failed to deserialize");

        // Verify the loaded data matches the original
        assert!(loaded_treemap.contains(1));
        assert!(loaded_treemap.contains(5));
        assert!(loaded_treemap.contains(100));
        assert!(loaded_treemap.contains(1000));
        assert!(loaded_treemap.contains(u64::MAX));
        assert!(!loaded_treemap.contains(2));
        assert!(!loaded_treemap.contains(99));
    }

    #[test]
    fn test_empty_deleted_ids_serialization() {
        // Test that empty DeletedIds can be serialized and deserialized
        let deleted_ids = DeletedIds::new();

        // Serialize to bytes
        let mut serialized = Vec::new();
        deleted_ids
            .ids
            .serialize_into(&mut serialized)
            .expect("Failed to serialize");

        // Deserialize from bytes
        let cursor = Cursor::new(&serialized[..]);
        let loaded_treemap =
            RoaringTreemap::deserialize_from(cursor).expect("Failed to deserialize");

        // Verify the loaded data is empty
        assert!(loaded_treemap.is_empty());
        assert_eq!(loaded_treemap.len(), 0);
    }

    #[test]
    fn test_large_deleted_ids_set() {
        // Test with a larger set of deleted IDs
        let mut deleted_ids = DeletedIds::new();

        // Add a range of IDs
        for i in (0..10000).step_by(7) {
            deleted_ids.mark_deleted(i);
        }

        // Serialize to bytes
        let mut serialized = Vec::new();
        deleted_ids
            .ids
            .serialize_into(&mut serialized)
            .expect("Failed to serialize");

        // Deserialize from bytes
        let cursor = Cursor::new(&serialized[..]);
        let loaded_treemap =
            RoaringTreemap::deserialize_from(cursor).expect("Failed to deserialize");

        // Verify some of the loaded data
        for i in (0..10000).step_by(7) {
            assert!(loaded_treemap.contains(i), "ID {} should be present", i);
            // Verify some IDs that shouldn't be present
            assert!(
                !loaded_treemap.contains(i + 1),
                "ID {} should not be present",
                i + 1
            );
        }
    }
}
