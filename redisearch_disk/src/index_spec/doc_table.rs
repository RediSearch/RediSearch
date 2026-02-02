mod doc_table_reader;
mod document_flags;
mod document_metadata;

use doc_table_reader::DocTableReader;
pub use doc_table_reader::ReaderCreateError as DocTableReaderCreateError;
// Re-export public types
pub use document_flags::{DocumentFlag, DocumentFlags, flags_from_oss, flags_to_oss};
pub use document_metadata::DocumentMetadata;
use inverted_index::RSIndexResult;

use speedb::{
    BlockBasedOptions, Cache, ColumnFamilyDescriptor, IteratorMode, Options as SpeedbDbOptions,
    WriteBatch, WriteOptions,
};

use crate::{
    INVALID_DOC_ID,
    database::{ColumnFamilyGuard, Speedb, SpeedbMultithreadedDatabase},
    index_spec::{Key, deleted_ids::DeletedIdsStore},
    key_traits::{AsKeyExt, FromKeyExt},
    value_traits::ValueExt,
};
use document::DocumentType;
use ffi::t_docId;
use rqe_iterators::inverted_index::InvIndIterator;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

use redis_module::raw::{RedisModuleIO, load_unsigned, save_unsigned};

use super::RDBVersion;

use crate::metrics::CFMetrics;

/// The DocTable struct represents a mapping from document IDs to the Redis key and document
/// metadata. It is used to look up documents by their IDs and to generate new document IDs
/// for documents added to the index.
pub struct DocTable {
    /// The colunm family handle for the document table
    /// All CF handles must be declared before database so they're dropped first,
    /// ensuring the database has the last reference to clean up properly
    cf: ColumnFamilyGuard,

    /// The column family handle for the reverse lookup table.
    reverse_lookup_cf: ColumnFamilyGuard,

    /// The last assigned document ID in the index (atomic for thread-safe ID generation)
    last_document_id: AtomicU64,

    /// The type of documents that are indexed.
    document_type: DocumentType,

    /// Write options for the document table
    write_options: WriteOptions,

    /// Set of deleted document IDs tracked using a roaring bitmap
    deleted_ids: DeletedIdsStore,

    /// The Speedb database instance used for storage.
    /// It needs to be declared last so it's dropped after CF handles
    database: SpeedbMultithreadedDatabase,
}

impl DocTable {
    const COLUMN_FAMILY_NAME: &str = "doc_table";
    const REVERSE_LOOKUP_COLUMN_FAMILY_NAME: &str = "reverse_lookup";

    pub fn new(
        document_type: DocumentType,
        database: SpeedbMultithreadedDatabase,
        deleted_ids: DeletedIdsStore,
    ) -> Result<Self, speedb::Error> {
        // SAFETY: The database field is declared after cf fields in the struct,
        // so cf handles will be dropped first, ensuring proper cleanup order.
        let cf = unsafe { database.cf_guard(Self::COLUMN_FAMILY_NAME) }
            .expect("Document table column family should exist");
        // SAFETY: same as above
        let reverse_lookup_cf =
            unsafe { database.cf_guard(Self::REVERSE_LOOKUP_COLUMN_FAMILY_NAME) }
                .expect("Reverse lookup column family should exist");

        let mut write_options = WriteOptions::default();
        write_options.disable_wal(true);
        Ok(Self {
            cf,
            reverse_lookup_cf,
            last_document_id: AtomicU64::new(1),
            document_type,
            write_options,
            deleted_ids,
            database,
        })
    }

    /// Returns the column family descriptor for the document table.
    ///
    /// # Arguments
    /// * `cache` - Shared block cache for all databases (managed by DiskContext)
    /// * `bloom_filter_bits_per_key` - Number of bits per key for the bloom filter
    pub fn cf_descriptor(cache: &Cache, bloom_filter_bits_per_key: f64) -> ColumnFamilyDescriptor {
        let mut block_based_options = BlockBasedOptions::default();
        block_based_options.set_block_cache(cache);

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

    pub fn reverse_lookup_cf_descriptor() -> ColumnFamilyDescriptor {
        ColumnFamilyDescriptor::new(
            Self::REVERSE_LOOKUP_COLUMN_FAMILY_NAME,
            SpeedbDbOptions::default(),
        )
    }

    pub fn collect_deleted_ids(&self) -> Vec<t_docId> {
        self.deleted_ids.collect_all()
    }

    /// Inserts a new document to the document table to generate a new document ID.
    /// If a document with the same key already exists, its old document ID is marked as deleted.
    /// We expect the caller to hold the redis global lock and for that reason two threads can't
    /// call this function at the same time,(and of course the same key).
    ///
    /// # Returns
    /// * `Ok((new_doc_id, old_doc_len))` - A tuple containing:
    ///   - `new_doc_id`: The newly assigned document ID
    ///   - `old_doc_len`: The length of the old document if it existed, or 0 if this is a new document
    /// * `Err(speedb::Error)` - If the database operation fails
    pub fn insert_document(
        &self,
        key: impl Into<Key>,
        score: f32,
        flags: DocumentFlags,
        max_term_freq: u32,
        doc_len: u32,
        expiration: Option<SystemTime>,
    ) -> Result<(t_docId, u32), speedb::Error> {
        let key: Key = key.into();

        // Check if document with same key exists (for update case)
        let old_doc_id = self.find_doc_id_by_key(&key);

        // If we have an old document, retrieve its length
        let old_doc_len = old_doc_id
            .map(|old_id| self.get_document_metadata(old_id))
            .transpose()?
            .flatten()
            .map_or(0, |m| m.doc_len);

        let new_doc_id = self.last_document_id.fetch_add(1, Ordering::Relaxed);

        let metadata = DocumentMetadata {
            key: key.clone(),
            score,
            flags,
            max_term_freq,
            doc_len,
            expiration,
        };

        // Use a batch write to ensure atomicity: the old document deletion (if updating),
        // new document metadata, and reverse lookup are all written together.
        // This prevents inconsistent state if any operation fails.
        let mut batch = WriteBatch::default();

        // If updating an existing document, delete the old doc_table entry in the same batch
        if let Some(old_id) = old_doc_id {
            batch.delete_cf(&self.cf, old_id.as_key());
        }

        batch.put_cf(&self.cf, new_doc_id.as_key(), metadata.as_speedb_value());
        batch.put_cf(&self.reverse_lookup_cf, &key, new_doc_id.as_key());

        self.database.write_opt(batch, &self.write_options)?;

        // Mark old document as deleted only after successful batch write
        // This is safe to do outside the batch since it's an in-memory operation
        if let Some(old_id) = old_doc_id {
            self.deleted_ids.mark_deleted(old_id);
        }

        Ok((new_doc_id, old_doc_len))
    }

    /// Finds the document ID associated with the given document key, if it exists.
    fn find_doc_id_by_key(&self, key: &Key) -> Option<t_docId> {
        self.database
            .get_cf(
                &self.reverse_lookup_cf,
                <Vec<u8> as AsRef<[u8]>>::as_ref(key),
            )
            .ok()?
            .map(|doc_id_bytes| {
                t_docId::from_key(&doc_id_bytes)
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
            .get_cf(&self.cf, doc_id.as_key())?
            .map(|value| DocumentMetadata::from_speedb_value(&value));

        Ok(dmd)
    }

    /// Returns the type of documents that are indexed.
    pub fn document_type(&self) -> DocumentType {
        self.document_type
    }

    /// Deletes a document by its key.
    ///
    /// Performs a lookup by key to find the document ID, then deletes the document metadata
    /// from the document table, removes the key from the reverse lookup table, and marks the
    /// document ID as deleted in the deleted-ids set.
    ///
    /// # Arguments
    /// * `key` - The document key to delete
    ///
    /// # Returns
    /// * `Ok((doc_id, old_doc_len))` - A tuple containing:
    ///   - `doc_id`: The deleted document ID if the document existed, or `INVALID_DOC_ID` (0) if not found
    ///   - `old_doc_len`: The length of the deleted document, or 0 if the document was not found
    /// * `Err(speedb::Error)` - If the database operation fails
    pub fn delete_document_by_key(
        &self,
        key: impl Into<Key>,
    ) -> Result<(t_docId, u32), speedb::Error> {
        let key: Key = key.into();
        if let Some(doc_id) = self.find_doc_id_by_key(&key) {
            // Retrieve the old document length before deletion
            let old_doc_len = self.get_document_metadata(doc_id)?.map_or(0, |m| m.doc_len);

            // Use a batch write to ensure atomicity: both the document metadata and reverse lookup
            // are deleted together.
            let mut batch = WriteBatch::default();
            batch.delete_cf(&self.cf, doc_id.as_key());
            batch.delete_cf(
                &self.reverse_lookup_cf,
                <Vec<u8> as AsRef<[u8]>>::as_ref(&key),
            );

            self.database.write_opt(batch, &self.write_options)?;

            // Mark the document as deleted in the bitmap after successful database write
            self.deleted_ids.mark_deleted(doc_id);
            Ok((doc_id, old_doc_len))
        } else {
            Ok((INVALID_DOC_ID, 0))
        }
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
    #[allow(dead_code)]
    pub fn delete_document(&self, doc_id: t_docId) -> Result<(), speedb::Error> {
        self.database.delete_cf(&self.cf, doc_id.as_key())?;

        self.deleted_ids.mark_deleted(doc_id);

        Ok(())
    }

    /// Returns a wildcard iterator over all documents in the document table.
    pub fn wildcard_iterator(
        &self,
        weight: f64,
    ) -> Result<InvIndIterator<'_, DocTableReader<'_, Speedb>>, DocTableReaderCreateError> {
        let iterator = self.database.iterator_cf(&self.cf, IteratorMode::Start);

        let reader = DocTableReader::new(iterator)?;

        let iter = InvIndIterator::new(reader, RSIndexResult::virt().weight(weight), None);

        Ok(iter)
    }

    pub fn get_last_doc_id(&self) -> t_docId {
        self.last_document_id.load(Ordering::Relaxed)
    }

    /// Returns a reference to the deleted IDs store.
    pub fn deleted_ids_len(&self) -> u64 {
        self.deleted_ids.len()
    }

    /// Saves the index spec disk-related state to RDB.
    ///
    /// Saves:
    /// - max_doc_id: The maximum document ID that has been assigned
    /// - deleted_ids: The set of deleted document IDs
    ///
    /// # Arguments
    /// * `rdb` - The RedisModuleIO handle for RDB operations. This should only be called
    ///   with a valid pointer provided by Redis in an RDB save callback context.
    ///
    /// # Errors
    /// Returns an error if saving fails.
    pub fn save_to_rdb(&self, rdb: *mut RedisModuleIO) -> Result<(), String> {
        // Save max_doc_id
        let max_doc_id = self.get_last_doc_id();

        save_unsigned(rdb, max_doc_id);

        // Save deleted IDs
        self.deleted_ids.save_to_rdb(rdb)?;

        Ok(())
    }

    /// Loads the doc table disk-related state from RDB.
    ///
    /// Loads:
    /// - max_doc_id: The maximum document ID that has been assigned
    /// - deleted_ids: The set of deleted document IDs
    ///
    /// # Arguments
    /// * `rdb` - The RedisModuleIO handle for RDB operations
    /// * `version` - The RDB version
    /// * `doc_table` - Optional reference to the doc table to populate. If None,
    ///   just consumes RDB stream
    ///
    /// # Errors
    /// Returns an error if loading fails.
    pub fn load_from_rdb_static(
        rdb: *mut RedisModuleIO,
        version: RDBVersion,
        doc_table: Option<&DocTable>,
    ) -> Result<(), String> {
        match version {
            RDBVersion::Initial => {
                // Always read max_doc_id from RDB
                let max_doc_id = load_unsigned(rdb)
                    .map_err(|e| format!("Failed to load max_doc_id from RDB: {:?}", e))?;

                // Always read deleted IDs from RDB and optionally apply to index
                if let Some(dt) = doc_table {
                    // Apply the loaded data to the index
                    dt.deleted_ids.replace_from_rdb(rdb)?;
                    dt.last_document_id.store(max_doc_id, Ordering::Relaxed);
                } else {
                    // Just consume the RDB stream without applying data
                    let _deleted_ids = DeletedIdsStore::load_from_rdb(rdb)?;
                }

                Ok(())
            }
        }
    }

    /// Collect metrics for the document table column family.
    pub fn collect_metrics(&self) -> crate::metrics::CFMetrics {
        CFMetrics::collect(&self.database, &self.cf)
    }
}
