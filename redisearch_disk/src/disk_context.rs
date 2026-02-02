use std::path::{Path, PathBuf};

use speedb::{Cache, WriteBufferManager};

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
    pub fn new(base_path: impl Into<PathBuf>) -> Self {
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
}
