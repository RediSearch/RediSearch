use std::collections::HashMap;
use std::path::{Path, PathBuf};

use speedb::{Cache, WriteBufferManager};

use crate::index_spec::IndexSpec;
use crate::info_sink::InfoSink;
use crate::metrics::{AsyncReadMetrics, ColumnFamilyMetrics, IndexMetrics};

/// Default memory limit for write buffers and block cache across all databases.
/// This limits the total memory used by memtables and block caches.
///
/// This value is set to 2 GB, which should provide a reasonable balance between
/// memory usage and write performance for most workloads.
const DEFAULT_MEMORY_LIMIT: usize = 2 * 1024 * 1024 * 1024; // 2 GB

/// Shared context for disk storage operations.
///
/// This struct holds:
/// - The base path for creating individual index databases
/// - A shared Cache for block caching across all databases
/// - A shared WriteBufferManager to limit memory usage (memtables + cache) across all databases
/// - Whether async IO is supported
/// - Collected metrics for all indexes
pub struct DiskContext {
    /// The base path where index databases will be created.
    base_path: PathBuf,
    /// Shared WriteBufferManager for all databases.
    /// This ensures that the total memory used by memtables and caches
    /// is limited across all databases.
    write_buffer_manager: WriteBufferManager,
    /// Shared block cache for all databases.
    /// This cache is tracked by the WriteBufferManager for total memory accounting.
    cache: Cache,
    /// Whether async IO is supported by the underlying storage.
    is_async_io_supported: bool,
    /// Map from index name to its collected metrics.
    index_metrics: HashMap<String, IndexMetrics>,
}

impl DiskContext {
    /// Creates a new DiskContext with the given base path.
    ///
    /// This creates:
    /// - A shared LRU cache for block caching
    /// - A WriteBufferManager that tracks memory usage against the cache
    ///
    /// The WriteBufferManager is configured with:
    /// - `allow_stall`: true - stalls writes when memory usage exceeds the limit
    /// - `initiate_flushes`: false - does not initiate flushes automatically
    pub fn new(base_path: impl Into<PathBuf>, is_async_io_supported: bool) -> Self {
        let cache = Cache::new_lru_cache(DEFAULT_MEMORY_LIMIT);
        let write_buffer_manager = WriteBufferManager::new(
            DEFAULT_MEMORY_LIMIT,
            Some(&cache), // cache costing enabled
            true,         // allow_stall
            false,        // initiate_flushes
            0,            // max_num_parallel_flushes (0 = default)
            0,            // start_delay_percent
        );

        Self {
            base_path: base_path.into(),
            cache,
            write_buffer_manager,
            is_async_io_supported,
            index_metrics: HashMap::new(),
        }
    }

    /// Returns the base path as a Path reference.
    pub fn base_path(&self) -> &Path {
        &self.base_path
    }

    /// Returns a reference to the shared block cache.
    pub fn cache(&self) -> &Cache {
        &self.cache
    }

    /// Returns a reference to the shared WriteBufferManager.
    pub fn write_buffer_manager(&self) -> &WriteBufferManager {
        &self.write_buffer_manager
    }

    /// Returns whether async IO is supported.
    pub fn is_async_io_supported(&self) -> bool {
        self.is_async_io_supported
    }

    /// Collects metrics for an index and stores them in the internal map.
    ///
    /// Returns the total memory used by this index's disk components.
    pub fn collect_index_metrics(&mut self, index: &IndexSpec) -> u64 {
        let metrics = IndexMetrics {
            doc_table: index.doc_table().collect_metrics(),
            inverted_index: index.inverted_index().collect_metrics(),
            async_read: index.doc_table().get_async_read_metrics(),
        };
        self.store_index_metrics(index.name(), metrics)
    }

    /// Stores pre-computed metrics for an index.
    ///
    /// Returns the total memory used by this index's disk components.
    /// This is useful for testing where metrics can be injected directly.
    pub fn store_index_metrics(&mut self, index_name: &str, metrics: IndexMetrics) -> u64 {
        let total_memory = metrics.total_memory();
        self.index_metrics.insert(index_name.to_string(), metrics);
        total_memory
    }

    /// Removes metrics for an index from the internal map.
    ///
    /// Should be called when an index is closed/deleted.
    pub fn remove_index_metrics(&mut self, index_name: &str) {
        self.index_metrics.remove(index_name);
    }

    /// Outputs aggregated disk metrics to Redis INFO.
    ///
    /// Iterates over all collected index metrics, aggregates them, and outputs
    /// to the Redis INFO context via the provided `InfoSink`.
    pub fn output_info_metrics(&self, sink: &mut impl InfoSink) {
        // Aggregate all metrics
        let mut doc_table_total = ColumnFamilyMetrics::default();
        let mut inverted_index_total = ColumnFamilyMetrics::default();
        let mut async_read_total = AsyncReadMetrics::default();

        for metrics in self.index_metrics.values() {
            doc_table_total += metrics.doc_table.clone();
            inverted_index_total += metrics.inverted_index.clone();
            async_read_total += metrics.async_read;
        }

        // Add disk section
        sink.with_section(c"disk", |sink| {
            // Doc table metrics
            sink.with_dict(c"disk_doc_table", |sink| {
                Self::output_cf_metrics(sink, &doc_table_total);
                Self::output_async_read_metrics(sink, &async_read_total);
            });

            // Inverted index metrics
            sink.with_dict(c"disk_text_inverted_index", |sink| {
                Self::output_cf_metrics(sink, &inverted_index_total);
            });
        });
    }

    /// Helper to output a single ColumnFamilyMetrics to INFO.
    fn output_cf_metrics(sink: &mut impl InfoSink, metrics: &ColumnFamilyMetrics) {
        // Memtable metrics
        sink.add_u64(c"num_immutable_memtables", metrics.num_immutable_memtables);
        sink.add_u64(
            c"num_immutable_memtables_flushed",
            metrics.num_immutable_memtables_flushed,
        );
        sink.add_u64(c"mem_table_flush_pending", metrics.mem_table_flush_pending);
        sink.add_u64(c"active_memtable_size", metrics.active_memtable_size);
        sink.add_u64(c"size_all_mem_tables", metrics.size_all_mem_tables);
        sink.add_u64(
            c"num_entries_active_memtable",
            metrics.num_entries_active_memtable,
        );
        sink.add_u64(
            c"num_entries_imm_memtables",
            metrics.num_entries_imm_memtables,
        );
        sink.add_u64(
            c"num_deletes_active_memtable",
            metrics.num_deletes_active_memtable,
        );
        sink.add_u64(
            c"num_deletes_imm_memtables",
            metrics.num_deletes_imm_memtables,
        );

        // Compaction metrics
        sink.add_u64(c"compaction_pending", metrics.compaction_pending);
        sink.add_u64(c"num_running_compactions", metrics.num_running_compactions);
        sink.add_u64(c"num_running_flushes", metrics.num_running_flushes);
        sink.add_u64(
            c"estimate_pending_compaction_bytes",
            metrics.estimate_pending_compaction_bytes,
        );

        // Data size estimates
        sink.add_u64(c"estimate_num_keys", metrics.estimate_num_keys);
        sink.add_u64(c"estimate_live_data_size", metrics.estimate_live_data_size);
        sink.add_u64(c"live_sst_files_size", metrics.live_sst_files_size);

        // Version tracking
        sink.add_u64(c"num_live_versions", metrics.num_live_versions);

        // Memory usage
        sink.add_u64(
            c"estimate_table_readers_mem",
            metrics.estimate_table_readers_mem,
        );
    }

    /// Helper to output async read metrics to INFO.
    fn output_async_read_metrics(sink: &mut impl InfoSink, metrics: &AsyncReadMetrics) {
        sink.add_u64(
            c"async_total_reads_requested",
            metrics.total_reads_requested,
        );
        sink.add_u64(c"async_reads_found", metrics.reads_found);
        sink.add_u64(c"async_reads_not_found", metrics.reads_not_found);
        sink.add_u64(c"async_reads_errors", metrics.reads_errors);
    }
}
