//! Metrics collection for RocksDB/SpeeDB column families and database-level statistics.
//!
//! This module provides structured access to RocksDB metrics, with clear separation between:
//! - **Column-family metrics**: Specific to individual column families
//! - **Database-level metrics**: Shared across the entire database instance
//!
//! All metrics are non-string (integer) properties that can be queried efficiently.

use crate::database::SpeedbMultithreadedDatabase;
use ffi::DiskColumnFamilyMetrics;
use speedb::{AsColumnFamilyRef, properties};
use tracing::warn;

/// Column-family specific metrics.
///
/// These metrics are specific to individual column families and include
/// memtable, compaction, data size, and other CF-level statistics.
#[derive(Debug, Default)]
pub struct CFMetrics {
    // Memtable metrics
    pub num_immutable_memtables: u64,
    pub num_immutable_memtables_flushed: u64,
    pub mem_table_flush_pending: u64,
    pub active_memtable_size: u64,
    pub size_all_mem_tables: u64,
    pub num_entries_active_memtable: u64,
    pub num_entries_imm_memtables: u64,
    pub num_deletes_active_memtable: u64,
    pub num_deletes_imm_memtables: u64,

    // Compaction metrics
    pub compaction_pending: u64,
    pub num_running_compactions: u64,
    pub num_running_flushes: u64,
    pub estimate_pending_compaction_bytes: u64,

    // Data size estimates
    pub estimate_num_keys: u64,
    pub estimate_live_data_size: u64,
    pub live_sst_files_size: u64,

    // Version tracking
    pub num_live_versions: u64,

    // Memory usage of table readers
    pub estimate_table_readers_mem: u64,
}

impl CFMetrics {
    /// Collect metrics for a specific column family.
    ///
    /// # Arguments
    /// * `db` - The database instance
    /// * `cf` - The column family handle
    pub fn collect(db: &SpeedbMultithreadedDatabase, cf: &impl AsColumnFamilyRef) -> Self {
        Self {
            // Memtable metrics
            num_immutable_memtables: get_int_property_cf(
                db,
                cf,
                properties::NUM_IMMUTABLE_MEM_TABLE,
            ),
            num_immutable_memtables_flushed: get_int_property_cf(
                db,
                cf,
                properties::NUM_IMMUTABLE_MEM_TABLE_FLUSHED,
            ),
            mem_table_flush_pending: get_int_property_cf(
                db,
                cf,
                properties::MEM_TABLE_FLUSH_PENDING,
            ),
            active_memtable_size: get_int_property_cf(
                db,
                cf,
                properties::CUR_SIZE_ACTIVE_MEM_TABLE,
            ),
            size_all_mem_tables: get_int_property_cf(db, cf, properties::SIZE_ALL_MEM_TABLES),
            num_entries_active_memtable: get_int_property_cf(
                db,
                cf,
                properties::NUM_ENTRIES_ACTIVE_MEM_TABLE,
            ),
            num_entries_imm_memtables: get_int_property_cf(
                db,
                cf,
                properties::NUM_ENTRIES_IMM_MEM_TABLES,
            ),
            num_deletes_active_memtable: get_int_property_cf(
                db,
                cf,
                properties::NUM_DELETES_ACTIVE_MEM_TABLE,
            ),
            num_deletes_imm_memtables: get_int_property_cf(
                db,
                cf,
                properties::NUM_DELETES_IMM_MEM_TABLES,
            ),

            // Compaction metrics
            compaction_pending: get_int_property_cf(db, cf, properties::COMPACTION_PENDING),
            num_running_compactions: get_int_property_cf(
                db,
                cf,
                properties::NUM_RUNNING_COMPACTIONS,
            ),
            num_running_flushes: get_int_property_cf(db, cf, properties::NUM_RUNNING_FLUSHES),
            estimate_pending_compaction_bytes: get_int_property_cf(
                db,
                cf,
                properties::ESTIMATE_PENDING_COMPACTION_BYTES,
            ),

            // Data size estimates
            estimate_num_keys: get_int_property_cf(db, cf, properties::ESTIMATE_NUM_KEYS),
            estimate_live_data_size: get_int_property_cf(
                db,
                cf,
                properties::ESTIMATE_LIVE_DATA_SIZE,
            ),
            live_sst_files_size: get_int_property_cf(db, cf, properties::LIVE_SST_FILES_SIZE),

            // Version tracking
            num_live_versions: get_int_property_cf(db, cf, properties::NUM_LIVE_VERSIONS),

            // Memory usage of table readers
            estimate_table_readers_mem: get_int_property_cf(
                db,
                cf,
                properties::ESTIMATE_TABLE_READERS_MEM,
            ),
        }
    }

    /// Populate a `DiskColumnFamilyMetrics` struct from this CF metrics snapshot.
    ///
    /// This is a thin conversion layer between the Rust-side `CFMetrics` and the
    /// C-facing `DiskColumnFamilyMetrics` struct exposed via the disk API.
    pub fn populate_metrics(&self, out: &mut DiskColumnFamilyMetrics) {
        // Memtable metrics
        out.num_immutable_memtables = self.num_immutable_memtables;
        out.num_immutable_memtables_flushed = self.num_immutable_memtables_flushed;
        out.mem_table_flush_pending = self.mem_table_flush_pending;
        out.active_memtable_size = self.active_memtable_size;
        out.size_all_mem_tables = self.size_all_mem_tables;
        out.num_entries_active_memtable = self.num_entries_active_memtable;
        out.num_entries_imm_memtables = self.num_entries_imm_memtables;
        out.num_deletes_active_memtable = self.num_deletes_active_memtable;
        out.num_deletes_imm_memtables = self.num_deletes_imm_memtables;

        // Compaction metrics
        out.compaction_pending = self.compaction_pending;
        out.num_running_compactions = self.num_running_compactions;
        out.num_running_flushes = self.num_running_flushes;
        out.estimate_pending_compaction_bytes = self.estimate_pending_compaction_bytes;

        // Data size estimates
        out.estimate_num_keys = self.estimate_num_keys;
        out.estimate_live_data_size = self.estimate_live_data_size;
        out.live_sst_files_size = self.live_sst_files_size;

        // Version tracking
        out.num_live_versions = self.num_live_versions;

        // Memory usage of table readers
        out.estimate_table_readers_mem = self.estimate_table_readers_mem;
    }
}

// Helper functions for property retrieval

/// Get an integer property for a specific column family.
fn get_int_property_cf(
    db: &SpeedbMultithreadedDatabase,
    cf: &impl AsColumnFamilyRef,
    property: &speedb::properties::PropName,
) -> u64 {
    match db.property_int_value_cf(cf, property) {
        Ok(value) => value.unwrap_or(0),
        Err(e) => {
            warn!(
                error = %e,
                property = property.as_str(),
                "Failed to get integer property for column family"
            );
            0
        }
    }
}
