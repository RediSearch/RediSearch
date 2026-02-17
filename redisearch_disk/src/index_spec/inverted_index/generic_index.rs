use ffi::t_docId;
use speedb::{ColumnFamilyDescriptor, WriteOptions};
use std::time::Instant;

use crate::{
    database::{ColumnFamilyGuard, SpeedbMultithreadedDatabase},
    key_traits::AsKeyExt,
    metrics::{AtomicCompactionMetrics, CompactionMetrics},
};

use super::{
    DeletedIdsStore, InvertedIndexKey,
    block_traits::{IndexConfig, SerializableBlock},
};

/// A generic inverted index that works with any block type implementing IndexConfig.
/// This eliminates code duplication between term and tag inverted indexes.
pub struct GenericInvertedIndex<Config> {
    /// The column family handle for the inverted index
    /// Must be declared before database
    cf: ColumnFamilyGuard,

    /// Write options for inverted index operations (WAL disabled for performance)
    write_options: WriteOptions,

    /// The Speedb database where we store the inverted index.
    database: SpeedbMultithreadedDatabase,

    /// Cumulative compaction metrics (cycles, time).
    compaction_metrics: AtomicCompactionMetrics,

    /// Phantom data to associate the config type (required by Rust since Config isn't stored directly)
    _phantom: std::marker::PhantomData<Config>,
}

impl<Config: IndexConfig> GenericInvertedIndex<Config> {
    /// Creates a new generic inverted index with the given Speedb database.
    pub fn new(database: SpeedbMultithreadedDatabase) -> Self {
        // SAFETY: The database field is declared after cf in the struct,
        // so cf will be dropped first, ensuring proper cleanup order.
        let cf = unsafe { database.cf_guard(Config::COLUMN_FAMILY_NAME) }
            .expect("Inverted index column family should exist");

        let mut write_options = WriteOptions::default();
        write_options.disable_wal(true);

        GenericInvertedIndex {
            cf,
            write_options,
            database,
            compaction_metrics: AtomicCompactionMetrics::default(),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Returns a reference to the column family handle.
    pub fn cf_handle(&self) -> &ColumnFamilyGuard {
        &self.cf
    }

    /// Strip the doc_id suffix from the key, leaving just the term.
    /// Keys are structured as: [term bytes][delimiter (1 byte)][doc_id (8 bytes big-endian)]
    pub fn strip_doc_id_suffix(src: &[u8]) -> &[u8] {
        debug_assert!(
            src.len() >= InvertedIndexKey::DOC_ID_KEY_SIZE,
            "key must be at least {} bytes to contain a doc_id",
            InvertedIndexKey::DOC_ID_KEY_SIZE
        );
        &src[..src.len() - InvertedIndexKey::DOC_ID_KEY_SIZE]
    }

    /// Returns whether `src` is long enough to contain a doc_id suffix.
    pub fn has_doc_id_suffix(src: &[u8]) -> bool {
        src.len() >= InvertedIndexKey::DOC_ID_KEY_SIZE
    }

    /// Creates a column family descriptor for this inverted index type.
    pub fn cf_descriptor(deleted_ids: Option<DeletedIdsStore>) -> ColumnFamilyDescriptor {
        Config::cf_descriptor(deleted_ids)
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
        let key = InvertedIndexKey::new(&prefix, Some(doc_id));
        let block: Config::SerializableBlock = doc.into();
        let serialized = block.serialize();

        self.database
            .put_cf_opt(&self.cf, key.as_key(), serialized, &self.write_options)?;

        Ok(())
    }

    /// Get a reference to the underlying database
    pub fn database(&self) -> &SpeedbMultithreadedDatabase {
        &self.database
    }

    /// Triggers a full compaction on this index's column family.
    pub fn compact_full(&self) {
        tracing::info!(
            cf = Config::COLUMN_FAMILY_NAME,
            "Starting full compaction on inverted index column family"
        );
        let start = Instant::now();
        self.database
            .compact_range_cf(&self.cf, None::<&[u8]>, None::<&[u8]>);
        let elapsed_ms = start.elapsed().as_millis() as u64;
        self.compaction_metrics.record(CompactionMetrics {
            cycles: 1,
            ms_run: elapsed_ms,
        });
        tracing::info!(
            cf = Config::COLUMN_FAMILY_NAME,
            "Completed full compaction on inverted index column family"
        );
    }

    /// Returns cumulative compaction metrics for this index.
    pub fn get_compaction_metrics(&self) -> CompactionMetrics {
        self.compaction_metrics.load()
    }
}
