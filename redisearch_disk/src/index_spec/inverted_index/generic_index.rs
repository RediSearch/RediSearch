use ffi::t_docId;
use speedb::{BottommostLevelCompaction, ColumnFamilyDescriptor, CompactOptions, WriteOptions};
use std::time::Instant;

use crate::{
    database::{ColumnFamilyGuard, SpeedbMultithreadedDatabase},
    key_traits::AsKeyExt,
    metrics::{AtomicCompactionMetrics, CompactionMetrics},
};

use super::{
    InvertedIndexKey,
    block_traits::{IndexConfig, SerializableBlock},
};

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
pub fn has_potentially_doc_id_suffix(src: &[u8]) -> bool {
    src.len() >= InvertedIndexKey::DOC_ID_KEY_SIZE
}

/// A generic inverted index that works with any block type implementing IndexConfig.
/// This eliminates code duplication between term and tag inverted indexes.
pub struct GenericInvertedIndex<Config> {
    /// The column family handle for the inverted index
    /// Must be declared before database
    cf: ColumnFamilyGuard,

    /// The column family name (stored for logging)
    cf_name: String,

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
    /// Creates a new generic inverted index with the given Speedb database and column family name.
    pub fn new(database: SpeedbMultithreadedDatabase, cf_name: &str) -> Self {
        // SAFETY: The database field is declared after cf in the struct,
        // so cf will be dropped first, ensuring proper cleanup order.
        let cf = unsafe { database.cf_guard(cf_name) }
            .expect("Inverted index column family should exist");

        let mut write_options = WriteOptions::default();
        write_options.disable_wal(true);

        GenericInvertedIndex {
            cf,
            cf_name: cf_name.to_string(),
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

    /// Creates a column family descriptor for this inverted index type.
    pub fn cf_descriptor(config: Config::CfConfig) -> ColumnFamilyDescriptor {
        Config::cf_descriptor(config)
    }

    /// Inserts a document into the postings list for the given item (prefix).
    /// The document is wrapped in the appropriate block type and serialized.
    pub fn insert(
        &self,
        prefix: &str,
        doc_id: t_docId,
        doc: <Config::SerializableBlock as SerializableBlock>::Document,
    ) -> Result<(), speedb::Error>
    where
        <Config::SerializableBlock as SerializableBlock>::Document: Into<Config::SerializableBlock>,
    {
        let key = InvertedIndexKey::new(prefix, Some(doc_id));
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
    ///
    /// Uses `BottommostLevelCompaction::Force` to prevent trivial moves that
    /// would skip the compaction iterator and bypass the merge operator.
    pub fn compact_full(&self) {
        tracing::info!(
            cf = %self.cf_name,
            "Starting full compaction on inverted index column family"
        );
        let start = Instant::now();
        let mut compact_opts = CompactOptions::default();
        compact_opts.set_bottommost_level_compaction(BottommostLevelCompaction::Force);
        self.database
            .compact_range_cf_opt(&self.cf, None::<&[u8]>, None::<&[u8]>, &compact_opts);
        let elapsed_ms = start.elapsed().as_millis() as u64;
        self.compaction_metrics.record(CompactionMetrics {
            cycles: 1,
            ms_run: elapsed_ms,
        });
        tracing::info!(
            cf = %self.cf_name,
            "Completed full compaction on inverted index column family"
        );
    }

    /// Returns cumulative compaction metrics for this index.
    pub fn get_compaction_metrics(&self) -> CompactionMetrics {
        self.compaction_metrics.load()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strip_doc_id_suffix_basic() {
        // "foo" + delimiter + 8 bytes of doc_id
        let key = b"foo\x00\x00\x00\x00\x00\x00\x00\x00\x28"; // doc_id = 40
        let expected = b"foo";
        assert_eq!(strip_doc_id_suffix(key), expected);
    }

    #[test]
    fn test_strip_doc_id_suffix_with_underscore_in_term() {
        // "alpha_beta" + delimiter + 8 bytes of doc_id
        let key = b"alpha_beta\x00\x00\x00\x00\x00\x00\x00\x00\x7B"; // doc_id = 123
        let expected = b"alpha_beta";
        assert_eq!(strip_doc_id_suffix(key), expected);
    }

    #[test]
    fn test_strip_doc_id_suffix_with_delimiter_byte_in_doc_id() {
        // Test that doc_id containing 0x5F ('_') doesn't break extraction
        // doc_id = 95 has 0x5F as its last byte
        let key = b"term\x00\x00\x00\x00\x00\x00\x00\x00\x5F"; // doc_id = 95
        let expected = b"term";
        assert_eq!(strip_doc_id_suffix(key), expected);
    }

    #[test]
    fn test_has_potentially_doc_id_suffix() {
        // With delimiter, DOC_ID_KEY_SIZE is 9 bytes (1 delimiter + 8 doc_id)
        assert!(has_potentially_doc_id_suffix(
            b"term\x00\x00\x00\x00\x00\x00\x00\x00\x01"
        ));
        assert!(has_potentially_doc_id_suffix(
            b"\x00\x00\x00\x00\x00\x00\x00\x00\x01"
        )); // just 9 bytes (delimiter + doc_id)
        assert!(!has_potentially_doc_id_suffix(b"short")); // less than 9 bytes
        assert!(!has_potentially_doc_id_suffix(b"")); // empty
    }
}
