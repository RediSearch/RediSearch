//! This module defines the IndexSpec struct, which represents the specification of an index,
//! including its name and document type. Each `IndexSpec` also has its own unique inverted index
//! mapping terms to postings lists and a document table mapping document IDs to document metadata.

pub mod deleted_ids;
pub mod doc_table;
pub mod inverted_index;

use crate::database::SpeedbMultithreadedDatabase;
use crate::index_spec::deleted_ids::DeletedIdsStore;
use document::DocumentType;
use speedb::{
    ColumnFamilyDescriptor, DEFAULT_COLUMN_FAMILY_NAME, Error as SpeedbError,
    Options as SpeedbDbOptions,
};
use std::path::{Path, PathBuf};

use self::doc_table::DocTable;
use self::inverted_index::InvertedIndex;

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
    /// The inverted index mapping terms to the inverted index blocks
    inverted_index: InvertedIndex,
    /// The document table mapping document IDs to document metadata
    doc_table: DocTable,

    /// The Speedb database used by this index, makes it easier to drop the database when the index is dropped
    database: SpeedbMultithreadedDatabase,
}

impl IndexSpec {
    const DB_WRITE_BUFFER_SIZE: usize = 5000 * 1024 * 1024; // 5 GB;
    const DOC_TABLE_CACHE_SIZE: usize = 300 * 1024 * 1024; // 300 MB;
    const DOC_TABLE_BLOOM_FILTER_BITS_PER_KEY: f64 = 10.0;

    /// Creates a new IndexSpec with the given name and document type.
    ///
    /// This will create or open a Speedb database at `{base_path}_{index_name}_{doc_type}/`
    /// with two column families: "doc_table" and "fulltext".
    pub fn new(
        name: String,
        document_type: DocumentType,
        base_path: impl AsRef<Path>,
        deleted_ids: DeletedIdsStore,
    ) -> Result<Self, SpeedbError> {
        // Create database path: {base_path}_{index_name}_{doc_type}
        // We use PathBuf operations to preserve non-UTF-8 bytes on Unix systems.
        // Converting to string with .display() would perform lossy UTF-8 conversion.
        let db_path = PathBuf::from(base_path.as_ref());

        // Append the suffix to the path using OsString to avoid UTF-8 conversion
        let mut path_os = db_path.into_os_string();
        path_os.push(format!("_{}_{}", name, &document_type));
        let db_path = PathBuf::from(path_os);

        // Configure database options
        let mut db_options = SpeedbDbOptions::default();
        db_options.create_if_missing(true);
        db_options.create_missing_column_families(true);
        db_options.set_write_buffer_size(Self::DB_WRITE_BUFFER_SIZE);

        // Always use open_cf_descriptors with create_missing_column_families=true
        // This handles both new databases and partially-created databases gracefully:
        // - For new databases: creates all CFs
        // - For existing complete databases: opens with correct options
        // - For partially-created databases: creates missing CFs
        let cf_descriptors = vec![
            ColumnFamilyDescriptor::new(DEFAULT_COLUMN_FAMILY_NAME, SpeedbDbOptions::default()),
            DocTable::cf_descriptor(
                Self::DOC_TABLE_CACHE_SIZE,
                Self::DOC_TABLE_BLOOM_FILTER_BITS_PER_KEY,
            ),
            DocTable::reverse_lookup_cf_descriptor(),
            InvertedIndex::cf_descriptor(deleted_ids.clone()),
        ];
        let database =
            SpeedbMultithreadedDatabase::open_cf_descriptors(&db_options, db_path, cf_descriptors)?;

        Ok(Self {
            name,
            doc_table: DocTable::new(document_type, database.clone(), deleted_ids)?,
            inverted_index: InvertedIndex::new(database.clone()),
            database,
        })
    }

    /// Returns the name of the index.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns a reference to the inverted index for this index.
    pub fn inverted_index(&self) -> &InvertedIndex {
        &self.inverted_index
    }

    /// Returns a reference to the document table for this index.
    pub fn doc_table(&self) -> &DocTable {
        &self.doc_table
    }

    /// Marks the index for deletion. When the index is dropped, the database files will be destroyed.
    pub fn mark_for_deletion(&mut self) {
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
