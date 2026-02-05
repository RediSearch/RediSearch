//! Column-family specific metrics for RocksDB/SpeeDB.

use std::ops::AddAssign;

use crate::database::SpeedbMultithreadedDatabase;
use speedb::{AsColumnFamilyRef, properties};
use tracing::warn;

/// Column-family specific metrics.
///
/// These metrics are specific to individual column families and include
/// memtable, compaction, data size, and other CF-level statistics.
#[derive(Debug, Default, Clone)]
pub struct Metrics {
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

impl Metrics {
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

    /// Returns the total memory used by this column family's disk components.
    ///
    /// This includes:
    /// 1. The mem-tables' size
    /// 2. The estimate of the tables' readers memory
    /// 3. SST files' size
    pub fn total_memory(&self) -> u64 {
        self.size_all_mem_tables + self.estimate_table_readers_mem + self.live_sst_files_size
    }
}

impl AddAssign for Metrics {
    fn add_assign(&mut self, other: Self) {
        self.num_immutable_memtables += other.num_immutable_memtables;
        self.num_immutable_memtables_flushed += other.num_immutable_memtables_flushed;
        self.mem_table_flush_pending += other.mem_table_flush_pending;
        self.active_memtable_size += other.active_memtable_size;
        self.size_all_mem_tables += other.size_all_mem_tables;
        self.num_entries_active_memtable += other.num_entries_active_memtable;
        self.num_entries_imm_memtables += other.num_entries_imm_memtables;
        self.num_deletes_active_memtable += other.num_deletes_active_memtable;
        self.num_deletes_imm_memtables += other.num_deletes_imm_memtables;
        self.compaction_pending += other.compaction_pending;
        self.num_running_compactions += other.num_running_compactions;
        self.num_running_flushes += other.num_running_flushes;
        self.estimate_pending_compaction_bytes += other.estimate_pending_compaction_bytes;
        self.estimate_num_keys += other.estimate_num_keys;
        self.estimate_live_data_size += other.estimate_live_data_size;
        self.live_sst_files_size += other.live_sst_files_size;
        self.num_live_versions += other.num_live_versions;
        self.estimate_table_readers_mem += other.estimate_table_readers_mem;
    }
}

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
