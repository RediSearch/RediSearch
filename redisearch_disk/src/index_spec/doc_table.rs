mod doc_table_reader;
mod document_metadata;

use doc_table_reader::DocTableReader;
pub use doc_table_reader::ReaderCreateError as DocTableReaderCreateError;
// Re-export public types
pub use document_metadata::DocumentMetadata;
use inverted_index::RSIndexResult;

use speedb::{BlockBasedOptions, Cache, ColumnFamilyDescriptor, Options as SpeedbDbOptions};

use crate::{
    database::{Speedb, SpeedbMultithreadedDatabase},
    index_spec::{Key, deleted_ids::DeletedIdsStore},
    key_traits::{AsKeyExt, FromKeyExt},
};
use document::DocumentType;
use ffi::t_docId;
use rqe_iterators::inverted_index::InvIndIterator;
use speedb::{BoundColumnFamily, IteratorMode};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use document_metadata::ArchivedDocumentMetadata;

/// The DocTable struct represents a mapping from document IDs to the Redis key and document
/// metadata. It is used to look up documents by their IDs and to generate new document IDs
/// for documents added to the index.
pub struct DocTable {
    /// The last assigned document ID in the index (atomic for thread-safe ID generation)
    last_document_id: AtomicU64,

    /// The type of documents that are indexed.
    document_type: DocumentType,

    /// The Speedb database instance used for storage.
    database: SpeedbMultithreadedDatabase,

    /// The name of the Speedb column family used for the document table.
    ///
    /// We can't currently store the column family handle directly because it has a lifetime
    /// tied to the database instance, which complicates ownership (does not compile).
    cf_name: String,

    /// Set of deleted document IDs tracked using a roaring bitmap
    deleted_ids: DeletedIdsStore,
}

impl DocTable {
    const COLUMN_FAMILY_NAME: &str = "doc_table";

    pub fn new(
        document_type: DocumentType,
        database: SpeedbMultithreadedDatabase,
        deleted_ids: DeletedIdsStore,
    ) -> Result<Self, speedb::Error> {
        // Recover the last document ID by reading the last entry in the column family
        let last_document_id = {
            // Verify the column family exists and get a handle
            let cf_handle = database
                .cf_handle(Self::COLUMN_FAMILY_NAME)
                .expect("Doc table column family should exist");

            database
                .iterator_cf(&cf_handle, IteratorMode::End)
                .next()
                .transpose()?
                .map(|(key, _value)| {
                    // Start from the next ID after the last one
                    t_docId::from_key(&key)
                        .map(|id| id + 1)
                        .expect("we control the key format, parsing the key should never fail")
                })
                .unwrap_or(1) // Start from 1 if the table is empty
        };

        Ok(Self {
            last_document_id: AtomicU64::new(last_document_id),
            document_type,
            database,
            cf_name: Self::COLUMN_FAMILY_NAME.to_string(),
            deleted_ids,
        })
    }

    /// Returns the column family descriptor for the document table.
    pub fn cf_descriptor(
        cache_size: usize,
        bloom_filter_bits_per_key: f64,
    ) -> ColumnFamilyDescriptor {
        let mut block_based_options = BlockBasedOptions::default();
        let block_cache = Cache::new_lru_cache(cache_size);
        block_based_options.set_block_cache(&block_cache);

        // the second parameter is ignored
        // see: https://github.com/facebook/rocksdb/blob/35148aca91cda84d6fa9b295eb5500d6d965dca6/include/rocksdb/filter_policy.h#L155
        block_based_options.set_bloom_filter(bloom_filter_bits_per_key, false);
        block_based_options.set_cache_index_and_filter_blocks(true);
        // FIXME(enricozb): the c++ poc sets
        //   blockBasedOptions.block_align = true;
        // but this doesn't exist in the rust api.
        let mut cf_options = SpeedbDbOptions::default();
        cf_options.set_block_based_table_factory(&block_based_options);

        ColumnFamilyDescriptor::new(Self::COLUMN_FAMILY_NAME, cf_options)
    }

    /// Returns the Speedb column family handle for the document table.
    fn cf_handle(&self) -> Arc<BoundColumnFamily<'_>> {
        // SAFETY: we verified the column family exists in `new()`
        self.database.cf_handle(&self.cf_name).unwrap()
    }

    /// Inserts a new document to the document table to generate a new document ID.
    /// If a document with the same key already exists, its old document ID is marked as deleted.
    /// We expect the caller to hold the redis global lock and for that reason two threads can't
    /// call this function at the same time,(and of course the same key).
    pub fn insert_document(
        &self,
        key: impl Into<Key>,
        score: f32,
        flags: u32,
        max_freq: u32,
    ) -> Result<t_docId, speedb::Error> {
        let key: Key = key.into();

        if let Some(old_doc_id) = self.find_doc_id_by_key(&key) {
            self.delete_document(old_doc_id)?;
        }

        let new_doc_id = self.last_document_id.fetch_add(1, Ordering::Relaxed);

        let entry_key = new_doc_id.as_key();
        let dmd_bytes = DocumentMetadata {
            key,
            score,
            flags,
            max_freq,
        }
        .serialize();

        // If this fails we would have a gap in the doc IDs, but it's not a big deal.
        self.database
            .put_cf(&self.cf_handle(), entry_key, dmd_bytes)?;

        Ok(new_doc_id)
    }

    /// Finds the document ID associated with the given document key, if it exists.
    fn find_doc_id_by_key(&self, key: &Key) -> Option<t_docId> {
        self.database
            .iterator_cf(&self.cf_handle(), IteratorMode::Start)
            .flat_map(|item| item.ok())
            .find(|(_key_bytes, value_bytes)| {
                let dmd = ArchivedDocumentMetadata::from_bytes(value_bytes);

                dmd.key() == *key
            })
            .map(|(key_bytes, _)| {
                t_docId::from_key(&key_bytes)
                    .expect("we control the key format, parsing the key should never fail")
            })
    }

    /// Checks if the document table contains a document with the given document ID.
    pub fn is_deleted(&self, doc_id: t_docId) -> bool {
        self.deleted_ids.is_deleted(doc_id)
    }

    /// Retrieves the document metadata for the given document ID, if it exists.
    pub fn get_document_metadata(
        &self,
        doc_id: t_docId,
    ) -> Result<Option<DocumentMetadata>, speedb::Error> {
        let dmd = self
            .database
            .get_cf(&self.cf_handle(), doc_id.as_key())?
            .map(|dmd_bytes| DocumentMetadata::deserialize(&dmd_bytes));

        Ok(dmd)
    }

    /// Returns the type of documents that are indexed.
    pub fn document_type(&self) -> DocumentType {
        self.document_type
    }

    /// Deletes a document by removing it from the document table and marking its ID
    /// as deleted.
    /// This is needed because the inverted index structure can hold the id of a document.
    /// When the document is deleted, we need to mark its ID as deleted so we know to
    /// not return it in the search results when iterating over the inverted index.
    ///
    /// # Arguments
    /// * `doc_id` - The document ID to delete
    ///
    /// # Returns
    /// `Ok(())` if the operation succeeded (regardless of whether the document existed),
    /// or an error if the database operation fails.
    pub fn delete_document(&self, doc_id: t_docId) -> Result<(), speedb::Error> {
        self.database
            .delete_cf(&self.cf_handle(), doc_id.as_key())?;

        self.deleted_ids.mark_deleted(doc_id);

        Ok(())
    }

    /// Returns a wildcard iterator over all documents in the document table.
    pub fn wildcard_iterator(
        &self,
        weight: f64,
    ) -> Result<InvIndIterator<'_, DocTableReader<'_, Speedb>>, DocTableReaderCreateError> {
        let iterator = self
            .database
            .iterator_cf(&self.cf_handle(), IteratorMode::Start);

        let reader = DocTableReader::new(iterator)?;

        let iter = InvIndIterator::new(reader, RSIndexResult::virt().weight(weight), None);

        Ok(iter)
    }
}
