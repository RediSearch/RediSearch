//! This module defines the IndexSpec struct, which represents the specification of an index,
//! including its name and document type. Each `IndexSpec` also has its own unique inverted index
//! mapping terms to postings lists and a document table mapping document IDs to document metadata.

pub mod deleted_ids;
pub mod doc_table;
pub mod inverted_index;

use crate::compaction::TextCompactionCollector;
use crate::database::SpeedbMultithreadedDatabase;
use crate::disk_context::DiskContext;
use crate::index_spec::deleted_ids::DeletedIdsStore;
use document::DocumentType;
use ffi::{IteratorType_INV_IDX_TAG_ITERATOR, QueryIterator, t_fieldIndex};
use rqe_iterators_interop::RQEIteratorWrapper;
use speedb::{
    ColumnFamilyDescriptor, DEFAULT_COLUMN_FAMILY_NAME, Error as SpeedbError,
    Options as SpeedbDbOptions,
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use self::doc_table::DocTable;
use self::inverted_index::block_traits::TermIndexCfConfig;
use self::inverted_index::{InvertedIndex, TagIndexConfig, TagInvertedIndex};

use redis_module::raw::{RedisModuleIO, load_unsigned, save_unsigned};

/// The type used to represent document keys. A document key is a unique identifier which main
/// Redis can use to refer to a document. In most cases, this is just the document's Redis key.
/// However, in some cases (e.g., when using hashes or JSON documents), the document key may
/// be a composite of the Redis key and a path within the document.
pub type Key = Vec<u8>;

/// Asserts that IndexSpec is Send + Sync. This is important because IndexSpec may be shared
/// across multiple threads in a multithreaded environment.
const _: () = {
    const fn assert_thread_safe<T: Send + Sync>() {}

    assert_thread_safe::<IndexSpec>();
};

/// The IndexSpec struct represents the specification of an index, including its name and document
/// type. It contains an inverted index mapping terms to postings lists and a document table
/// mapping document IDs to document metadata.
///
/// Each IndexSpec owns its own Speedb database.
pub struct IndexSpec {
    /// The name of the index
    name: String,
    /// The inverted index mapping terms to the inverted index blocks (for fulltext fields)
    term_inverted_index: InvertedIndex,
    /// Tag inverted indexes, one per tag field (keyed by field_index)
    tag_inverted_indexes: RwLock<HashMap<t_fieldIndex, Arc<TagInvertedIndex>>>,
    /// The document table mapping document IDs to document metadata
    doc_table: DocTable,

    /// Store for deleted IDs, needed for creating new tag indexes
    deleted_ids: DeletedIdsStore,
    /// The Speedb database used by this index, makes it easier to drop the database when the index is dropped
    database: SpeedbMultithreadedDatabase,
}

impl IndexSpec {
    const DOC_TABLE_BLOOM_FILTER_BITS_PER_KEY: f64 = 10.0;

    /// Creates a new IndexSpec with the given name and document type.
    ///
    /// This will create or open a Speedb database at `{base_path}_{index_name}_{doc_type}/`
    /// with column families for doc_table, fulltext, and any existing tag fields.
    ///
    /// # Arguments
    /// * `name` - The name of the index
    /// * `document_type` - The type of documents this index will contain
    /// * `disk_context` - Reference to the shared disk context containing base path, cache, and write buffer manager
    /// * `deleted_ids` - Store for tracking deleted document IDs
    pub fn new(
        name: String,
        document_type: DocumentType,
        disk_context: &DiskContext,
        deleted_ids: DeletedIdsStore,
    ) -> Result<Self, SpeedbError> {
        // Create database path: {base_path}_{index_name}_{doc_type}
        // We use PathBuf operations to preserve non-UTF-8 bytes on Unix systems.
        // Converting to string with .display() would perform lossy UTF-8 conversion.
        let db_path = PathBuf::from(disk_context.base_path());

        // Append the suffix to the path using OsString to avoid UTF-8 conversion
        let mut path_os = db_path.into_os_string();
        path_os.push(format!("_{}_{}", name, &document_type));
        let db_path = PathBuf::from(path_os);

        // Configure database options
        let mut db_options = SpeedbDbOptions::default();
        db_options.create_if_missing(true);
        db_options.create_missing_column_families(true);
        // Use the shared WriteBufferManager to limit memory usage across all databases.
        // This replaces the per-database write_buffer_size setting, providing global
        // memory control instead of per-database limits.
        db_options.set_write_buffer_manager(disk_context.write_buffer_manager());

        // Create the text compaction collector (shared with text merge operator)
        let text_compaction_collector = TextCompactionCollector::new();

        // Discover existing tag field CFs before opening the database
        // RocksDB/Speedb requires all existing CFs to be opened
        let existing_tag_fields: Vec<t_fieldIndex> =
            SpeedbMultithreadedDatabase::list_cf(&db_options, &db_path)
                .unwrap_or_default() // Database doesn't exist yet, no existing tag CFs
                .iter()
                .filter_map(|cf_name| TagIndexConfig::parse_cf_name(cf_name))
                .collect();

        // Build CF descriptors for core CFs plus any existing tag CFs
        // This handles both new databases and partially-created databases gracefully:
        // - For new databases: creates all CFs
        // - For existing complete databases: opens with correct options
        // - For partially-created databases: creates missing CFs
        let mut cf_descriptors = vec![
            ColumnFamilyDescriptor::new(DEFAULT_COLUMN_FAMILY_NAME, SpeedbDbOptions::default()),
            DocTable::cf_descriptor(
                disk_context.cache(),
                Self::DOC_TABLE_BLOOM_FILTER_BITS_PER_KEY,
            ),
            DocTable::reverse_lookup_cf_descriptor(),
            InvertedIndex::cf_descriptor(TermIndexCfConfig::new(
                deleted_ids.clone(),
                text_compaction_collector.clone(),
            )),
        ];

        // Add descriptors for existing tag field CFs
        for &field_index in &existing_tag_fields {
            cf_descriptors.push(TagIndexConfig::cf_descriptor(
                field_index,
                deleted_ids.clone(),
            ));
        }

        let database = SpeedbMultithreadedDatabase::open_cf_descriptors(
            &db_options,
            &db_path,
            cf_descriptors,
        )?;

        // Create TagInvertedIndex for each existing tag field
        let mut tag_inverted_indexes = HashMap::new();
        for &field_index in &existing_tag_fields {
            tag_inverted_indexes.insert(
                field_index,
                Arc::new(TagInvertedIndex::new(database.clone(), field_index)),
            );
        }

        Ok(Self {
            name,
            doc_table: DocTable::new(document_type, database.clone(), deleted_ids.clone())?,
            term_inverted_index: InvertedIndex::new(database.clone(), text_compaction_collector),
            tag_inverted_indexes: RwLock::new(tag_inverted_indexes),
            deleted_ids,
            database,
        })
    }

    /// Returns the name of the index.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns a reference to the inverted index for this index (fulltext fields).
    pub fn term_index(&self) -> &InvertedIndex {
        &self.term_inverted_index
    }

    /// Gets or creates a tag inverted index for the given field index.
    ///
    /// If the tag index doesn't exist, creates a new column family and TagInvertedIndex first.
    /// Returns a cloned Arc, so the caller can use it without holding any locks.
    ///
    /// Use this method for write operations (indexing). For read operations (queries),
    /// use [`get_tag_index`] instead to avoid creating column families as a side effect.
    pub fn tag_index(
        &self,
        field_index: t_fieldIndex,
    ) -> Result<Arc<TagInvertedIndex>, SpeedbError> {
        // Fast path: check if index already exists with read lock
        if let Some(index) = self.get_tag_index(field_index) {
            return Ok(index);
        }

        // Slow path: need to create the index
        // unwrap: RwLock is never poisoned - we don't panic while holding the lock
        let mut indexes = self.tag_inverted_indexes.write().unwrap();

        // Double-check after acquiring write lock
        if let std::collections::hash_map::Entry::Vacant(entry) = indexes.entry(field_index) {
            let cf_name = TagIndexConfig::cf_name(field_index);
            let cf_options = TagIndexConfig::cf_options(self.deleted_ids.clone());

            // Create the column family in the database
            self.database.create_cf(&cf_name, &cf_options)?;

            // Create the TagInvertedIndex
            let tag_index = Arc::new(TagInvertedIndex::new(self.database.clone(), field_index));
            entry.insert(tag_index);
        }

        Ok(Arc::clone(indexes.get(&field_index).unwrap()))
    }

    /// Gets an existing tag inverted index for the given field index.
    ///
    /// Returns `None` if the tag index doesn't exist. This is a read-only operation
    /// that doesn't create any database state.
    ///
    /// Use this method for read operations (queries). For write operations (indexing),
    /// use [`tag_index`] instead which will create the index if it doesn't exist.
    fn get_tag_index(&self, field_index: t_fieldIndex) -> Option<Arc<TagInvertedIndex>> {
        // unwrap: RwLock is never poisoned - we don't panic while holding the lock
        let indexes = self.tag_inverted_indexes.read().unwrap();
        indexes.get(&field_index).map(Arc::clone)
    }

    /// Creates a new tag iterator for the given field and tag.
    ///
    /// Returns `None` if no tag index exists for this field.
    /// Returns `Some(Ok(ptr))` on success, `Some(Err(e))` if iterator creation failed.
    pub fn new_tag_iterator(
        &self,
        field_index: t_fieldIndex,
        tag: &str,
        weight: f64,
    ) -> Option<Result<*mut QueryIterator, inverted_index::TagPostingReaderCreateError>> {
        let tag_index = self.get_tag_index(field_index)?;
        Some(
            tag_index
                .tag_iterator(tag, weight)
                .map(|iter| RQEIteratorWrapper::boxed_new(IteratorType_INV_IDX_TAG_ITERATOR, iter)),
        )
    }

    /// Returns a reference to the document table for this index.
    pub fn doc_table(&self) -> &DocTable {
        &self.doc_table
    }

    /// Marks the index for deletion. When the index is dropped, the database files will be destroyed.
    pub fn mark_for_deletion(&self) {
        self.database.mark_for_deletion();
    }

    /// Saves the index's disk-related state to the RDB file.
    ///
    /// See [`RDBVersion`] for the detailed RDB format specification.
    ///
    /// # Arguments
    /// * `rdb` - The RedisModuleIO handle for RDB operations
    ///
    /// # Returns
    /// `Ok(())` if successful, `Err` with error message otherwise
    pub fn save_to_rdb(&self, rdb: *mut RedisModuleIO) -> Result<(), String> {
        // RDB-Version
        save_unsigned(rdb, RDBVersion::CURRENT as u64);

        // Doc-table related state (currently the only state we save)
        self.doc_table.save_to_rdb(rdb)
    }

    /// Loads index spec's disk-related state from RDB.
    ///
    /// See [`RDBVersion`] for the detailed RDB format specification.
    ///
    /// # Arguments
    /// * `rdb` - The RedisModuleIO handle for RDB operations
    /// * `index` - Optional reference to the index to populate. If None, just
    ///   consumes RDB stream
    ///
    /// # Returns
    /// `Ok(())` if successful, `Err` with error message otherwise
    pub fn load_from_rdb_static(
        rdb: *mut RedisModuleIO,
        index: Option<&IndexSpec>,
    ) -> Result<(), String> {
        // RDB-version
        let rdb_version = load_unsigned(rdb)
            .map_err(|e| format!("Failed to load RDB version from RDB: {:?}", e))?;

        let version = RDBVersion::from_u64(rdb_version)?;

        match version {
            RDBVersion::Initial => {
                // Doc-table related state (currently the only state we load)
                DocTable::load_from_rdb_static(rdb, version, index.map(|i| i.doc_table()))
            }
        }
    }

    /// Returns the database handle for this index.
    pub fn database(&self) -> SpeedbMultithreadedDatabase {
        self.database.clone()
    }

    /// Triggers a full compaction on all tag inverted indexes.
    ///
    /// This runs the merge operator on each tag column family, filtering out
    /// deleted document IDs to reclaim disk space and improve query performance.
    fn compact_tag_inverted_indexes(&self) {
        // unwrap: RwLock is never poisoned - we don't panic while holding the lock
        let indexes = self.tag_inverted_indexes.read().unwrap();
        for (_, tag_index) in indexes.iter() {
            tag_index.compact_full();
        }
    }

    /// Triggers full compaction on all indexes (text and tag) and applies the delta.
    ///
    /// This method takes a snapshot of `deleted_ids` before compaction starts, then
    /// performs compaction on both text and tag inverted indexes. After compaction
    /// completes, all doc IDs from the snapshot are removed from `deleted_ids`.
    ///
    /// # Why Snapshot-Based Cleanup?
    ///
    /// We assume that all deleted IDs in the snapshot are handled during compaction
    /// because the compaction process visits all keys in the indexes. The merge operator
    /// filters out any deleted doc IDs it encounters, so after a full compaction pass,
    /// all deleted IDs have been processed.
    ///
    /// Note: Having `deleted_ids` be a superset of the actual doc IDs in the indexes
    /// (e.g., due to replication timing) does not cause any logical harm - the merge
    /// operator simply skips doc IDs that don't exist in a given index.
    ///
    /// # Arguments
    /// * `c_index_spec` - Pointer to the C `IndexSpec` struct for FFI calls
    ///
    /// # Safety
    /// `c_index_spec` must be a valid pointer to a C `IndexSpec` struct that remains
    /// valid for the duration of this function call.
    pub unsafe fn compact_all(&self, c_index_spec: *mut ffi::IndexSpec) {
        let snapshot_deleted_ids = self.doc_table.deleted_ids().collect_all();

        // 1. Run TEXT compaction (clears collector, compacts, and applies delta internally)
        // SAFETY: Caller guarantees c_index_spec is valid.
        unsafe { self.term_inverted_index.compact_full(c_index_spec) };

        // 2. Run TAG compaction
        self.compact_tag_inverted_indexes();

        // 3. Remove the deleted_id that have been cleaned.
        let removed_count = self
            .doc_table
            .deleted_ids()
            .remove_batch(snapshot_deleted_ids);

        tracing::debug!(removed_count, "Removed doc IDs from DeletedIds");

        tracing::debug!(
            index_name = self.name(),
            "Compaction with delta application complete"
        );
    }

    /// Collects metrics for the text inverted index column family.
    ///
    /// Returns an [`InvertedIndexMetrics`] with column family metrics and compaction metrics.
    ///
    /// This is a read-only operation that doesn't create any database state.
    ///
    /// Use this method for read operations (queries). For write operations (indexing),
    /// use [`compact_text_inverted_index`] instead which will create the index if it doesn't exist.
    pub fn collect_term_inverted_index_metrics(&self) -> crate::metrics::InvertedIndexMetrics {
        use crate::metrics::InvertedIndexMetrics;
        InvertedIndexMetrics {
            column_family: self.term_inverted_index.collect_metrics(),
            compaction: self.term_inverted_index.get_compaction_metrics(),
        }
    }

    /// Collects aggregated metrics from all tag inverted indexes.
    ///
    /// Returns a single [`InvertedIndexMetrics`] that combines column family metrics
    /// and compaction metrics from all tag fields.
    pub fn collect_tag_inverted_index_metrics(&self) -> crate::metrics::InvertedIndexMetrics {
        use crate::metrics::{ColumnFamilyMetrics, CompactionMetrics, InvertedIndexMetrics};

        // unwrap: RwLock is never poisoned - we don't panic while holding the lock
        let indexes = self.tag_inverted_indexes.read().unwrap();

        let mut total_cf = ColumnFamilyMetrics::default();
        let mut total_compaction = CompactionMetrics::default();

        for tag_index in indexes.values() {
            total_cf += tag_index.collect_metrics();
            total_compaction += tag_index.get_compaction_metrics();
        }

        InvertedIndexMetrics {
            column_family: total_cf,
            compaction: total_compaction,
        }
    }
}

/// RDB format version for index disk-related state serialization.
#[repr(u64)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::FromRepr)]
pub enum RDBVersion {
    /// ## Version `Initial`
    ///
    /// The disk-related fields are serialized/deserialized in the following order:
    ///
    /// | Component       | Type   |
    /// |-----------------|--------|
    /// | Version         | u64    |
    /// | Max doc-id      | u64    |
    /// | Delete-ids      |        |
    ///     | Size        | u64    |
    ///     | Data        | buffer |
    ///
    /// 1. **RDB Version** (u64):
    ///    - The version number of the RDB format
    ///
    /// 2. **Max Document ID** (u64):
    ///    - The highest document ID that has been assigned
    ///    - Used to ensure new documents get unique IDs after restart
    ///    - Restored to the doc table's `last_document_id` atomic counter on load
    ///
    /// 3. **Deleted IDs** (variable length):
    ///    - A RoaringTreemap of deleted document IDs
    ///    - Format:
    ///      - **Size** (u64): The byte length of the serialized RoaringTreemap
    ///      - **Data** (bytes): The serialized RoaringTreemap in its native binary format
    Initial = 1,
}

impl RDBVersion {
    pub const CURRENT: Self = Self::Initial;

    pub fn from_u64(value: u64) -> Result<Self, String> {
        Self::from_repr(value).ok_or_else(|| format!("Unsupported RDB version: {}", value))
    }
}
