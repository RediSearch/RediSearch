use std::sync::Arc;

use ffi::t_docId;
use speedb::{BoundColumnFamily, ColumnFamilyDescriptor};

use crate::{database::SpeedbMultithreadedDatabase, key_traits::AsKeyExt};

use super::{
    DeletedIdsStore, InvertedIndexKey, KEY_DELIMITER,
    block_traits::{IndexConfig, SerializableBlock},
};

/// A generic inverted index that works with any block type implementing IndexConfig.
/// This eliminates code duplication between term and tag inverted indexes.
pub struct GenericInvertedIndex<Config> {
    /// The Speedb database where we store the inverted index.
    database: SpeedbMultithreadedDatabase,

    /// The name of the column family where we store the inverted index.
    cf_name: String,
}

impl<Config: IndexConfig> GenericInvertedIndex<Config> {
    /// Creates a new generic inverted index with the given Speedb database.
    pub fn new(database: SpeedbMultithreadedDatabase) -> Self {
        // Verify the column family exists
        database
            .cf_handle(Config::COLUMN_FAMILY_NAME)
            .expect("Inverted index column family should exist");

        GenericInvertedIndex {
            database,
            cf_name: Config::COLUMN_FAMILY_NAME.to_string(),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Returns the Speedb column family handle for the inverted index.
    pub fn cf_handle(&self) -> Arc<BoundColumnFamily<'_>> {
        // SAFETY: we verified the column family exists in `new()`
        self.database.cf_handle(&self.cf_name).unwrap()
    }

    /// Strip everything after the last [`KEY_DELIMITER`], which is the key
    /// without the last doc_id, but keeping the delimiter.
    pub fn strip_after_last_delimiter(src: &[u8]) -> &[u8] {
        let last_key_delimiter_idx = src
            .iter()
            .rposition(|byte| *byte == KEY_DELIMITER)
            .expect("slice doesn't contain KEY_DELIMITER");

        src.split_at(last_key_delimiter_idx + 1).0
    }

    /// Returns whether `src` contains a [`KEY_DELIMITER`].
    pub fn contains_key_delimiter(src: &[u8]) -> bool {
        src.contains(&KEY_DELIMITER)
    }

    /// Creates a column family descriptor for this inverted index type.
    pub fn cf_descriptor(deleted_ids: Option<DeletedIdsStore>) -> ColumnFamilyDescriptor {
        Config::cf_descriptor(deleted_ids)
    }

    /// Creates a key for the inverted index.
    fn key(prefix: &str, last_doc_id: Option<t_docId>) -> InvertedIndexKey<'_> {
        InvertedIndexKey { term, last_doc_id }
    }

    /// Inserts a document into the postings list for the given item (prefix).
    /// The document is wrapped in the appropriate block type and serialized.
    pub fn insert(
        &self,
        prefix: String,
        doc_id: t_docId,
        doc: <Config::SerializableBlock as SerializableBlock>::Document,
    ) -> Result<(), speedb::Error>
    where
        <Config::SerializableBlock as SerializableBlock>::Document: Into<Config::SerializableBlock>,
    {
        let key = Self::key(&prefix, Some(doc_id));
        let block: Config::SerializableBlock = doc.into();
        let serialized = block.serialize();

        self.database
            .put_cf(&self.cf_handle(), key.as_key(), serialized)?;

        Ok(())
    }

    /// Get a reference to the underlying database
    pub fn database(&self) -> &SpeedbMultithreadedDatabase {
        &self.database
    }
}
