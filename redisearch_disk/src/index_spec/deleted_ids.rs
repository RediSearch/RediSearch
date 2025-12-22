use std::sync::{Arc, RwLock};

use ffi::t_docId;
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
}
