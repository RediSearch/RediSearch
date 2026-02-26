pub mod compaction;
pub mod database;
pub mod disk_context;
pub mod document_id_key;
pub mod index_spec;
pub mod info_sink;
pub mod key_traits;

pub mod metrics;
pub mod utils;
pub mod value_traits;
pub mod vecsim_disk;

pub use info_sink::InfoSink;

use crate::disk_context::DiskContext;
use crate::index_spec::IndexSpec;
use crate::index_spec::deleted_ids::DeletedIdsStore;
use crate::index_spec::doc_table::{DocumentFlag, flags_from_oss, flags_to_oss};
use crate::vecsim_disk::{EdgeListMergeOperator, SpeeDBHandles, VecSimDisk_CreateIndex};
use document::DocumentType;
use ffi::{
    AllocateKeyCallback, BasicDiskAPI, DocTableDiskAPI, IndexDiskAPI,
    IteratorType_INV_IDX_TERM_ITERATOR, IteratorType_INV_IDX_WILDCARD_ITERATOR, MetricsDiskAPI,
    QueryIterator, REDISMODULE_ERR, REDISMODULE_OK, RSDocumentMetadata, RSToken, RedisModuleCtx,
    RedisModuleInfoCtx, RedisSearchDisk, RedisSearchDiskAPI, RedisSearchDiskIndexSpec, ThrottleCB,
    VecSimDiskContext, VecSimParamsDisk, VectorDiskAPI, t_docId, t_fieldIndex, t_fieldMask,
};
use query_term::RSQueryTerm;
use rqe_iterators_interop::RQEIteratorWrapper;

use std::ffi::{OsStr, c_char, c_void};
use std::time::{Duration, UNIX_EPOCH};
use tracing::{debug, error, info, warn};

use crate::utils::{compute_disk_path, get_redis_config_value};

/// Registers the Redis module allocator as the global allocator for the application.
#[cfg(feature = "redis_allocator")]
#[global_allocator]
static REDIS_MODULE_ALLOCATOR: redis_module::alloc::RedisAlloc = redis_module::alloc::RedisAlloc;

const INVALID_DOC_ID: t_docId = 0;
const INVALID_NANOSECONDS_THRESHOLD: u32 = 1_000_000_000;

/// Validates that a timespec represents a valid positive time value.
/// Returns `true` if valid, or `false` if invalid.
fn validate_timespec(ts: &ffi::timespec) -> bool {
    ts.tv_sec >= 0
        && ts.tv_nsec >= 0
        && (ts.tv_sec > 0 || ts.tv_nsec > 0)
        && ts.tv_nsec < INVALID_NANOSECONDS_THRESHOLD as i64
}

/// Converts a timespec to a SystemTime.
fn timespec_to_system_time(ts: &ffi::timespec) -> std::time::SystemTime {
    UNIX_EPOCH + Duration::new(ts.tv_sec as u64, ts.tv_nsec as u32)
}

/// Converts a C string pointer and length to a Rust &str.
/// Returns None if ptr is null, len is 0, or the bytes are not valid UTF-8.
///
/// # Safety
/// `ptr` must point to `len` valid bytes.
unsafe fn c_str_to_str<'a>(ptr: *const c_char, len: usize) -> Option<&'a str> {
    if ptr.is_null() || len == 0 {
        return None;
    }
    // SAFETY: Caller guarantees ptr points to len valid bytes, and we checked ptr is not null
    let slice = unsafe { std::slice::from_raw_parts(ptr as *const u8, len) };
    std::str::from_utf8(slice).ok()
}

#[unsafe(no_mangle)]
pub extern "C" fn SearchDisk_HasAPI() -> bool {
    true
}

// Extern declaration for C++ throttle callback setter
unsafe extern "C" {
    fn VecSimDisk_SetThrottleCallbacks(enable: ThrottleCB, disable: ThrottleCB);
}

/// Sets throttle callbacks for vector disk tiered indexes.
extern "C" fn set_throttle_callbacks(enable: ThrottleCB, disable: ThrottleCB) {
    // SAFETY: VecSimDisk_SetThrottleCallbacks stores these callback pointers.
    // The callbacks are provided by RediSearch and remain valid for the module lifetime.
    unsafe {
        VecSimDisk_SetThrottleCallbacks(enable, disable);
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn SearchDisk_GetAPI() -> *mut RedisSearchDiskAPI {
    debug!("Getting search on disk API");

    static mut API: RedisSearchDiskAPI = RedisSearchDiskAPI {
        basic: BasicDiskAPI {
            open: Some(open),
            close: Some(close),
            openIndexSpec: Some(index_spec_open),
            closeIndexSpec: Some(index_spec_close),
            indexSpecRdbSave: Some(index_spec_rdb_save),
            indexSpecRdbLoad: Some(index_spec_rdb_load),
            isAsyncIOSupported: Some(is_async_io_supported),
            setThrottleCallbacks: Some(set_throttle_callbacks),
        },
        index: IndexDiskAPI {
            markToBeDeleted: Some(index_spec_mark_to_be_deleted),
            indexTerm: Some(index_spec_index_term),
            indexTags: Some(index_spec_index_tags),
            deleteDocument: Some(index_spec_delete_document),
            newTermIterator: Some(index_spec_new_term_iterator),
            newTagIterator: Some(index_spec_new_tag_iterator),
            newWildcardIterator: Some(index_spec_new_wildcard_iterator),
            runGC: Some(index_spec_run_gc),
        },
        docTable: DocTableDiskAPI {
            putDocument: Some(index_spec_put_doc),
            isDocIdDeleted: Some(index_spec_is_doc_id_deleted),
            getDocumentMetadata: Some(index_spec_get_document_metadata),
            getMaxDocId: Some(index_spec_get_max_doc_id),
            getDeletedIdsCount: Some(index_spec_get_deleted_ids_count),
            getDeletedIds: Some(index_spec_get_deleted_ids),
            createAsyncReadPool: Some(index_spec_create_async_read_pool),
            addAsyncRead: Some(index_spec_add_async_read),
            pollAsyncReads: Some(index_spec_poll_async_reads),
            freeAsyncReadPool: Some(index_spec_free_async_read_pool),
        },
        vector: VectorDiskAPI {
            createVectorIndex: Some(vector_create_index),
            freeVectorIndex: Some(vector_free_index),
        },
        metrics: MetricsDiskAPI {
            collectIndexMetrics: Some(collect_index_metrics),
            outputInfoMetrics: Some(output_info_metrics),
        },
    };

    &raw mut API
}

/// Opens the on-disk index db.
///
/// This function retrieves the `bigredis-path` configuration from Redis,
/// computes the disk storage path by extracting the parent directory and
/// appending "/redisearch", then creates the DiskContext for individual index databases.
///
/// # Safety
/// 1. `ctx` must be a valid RedisModuleCtx pointer.
extern "C" fn open(ctx: *mut RedisModuleCtx) -> *mut RedisSearchDisk {
    let is_async_io_supported = speedb::DB::is_async_io_available();
    info!(is_async_io_supported, "opening search disk");

    // Safety: ctx is a valid RedisModuleCtx pointer (see safety point 1 above)
    let redis_ctx = redis_module::Context::new(ctx as *mut redis_module::raw::RedisModuleCtx);

    // Get the bigredis-path configuration from Redis
    let bigredis_path = match get_redis_config_value(&redis_ctx, "bigredis-path") {
        Some(path) if !path.is_empty() => path,
        Some(_) => {
            error!("bigredis-path configuration is empty, cannot initialize disk storage");
            return std::ptr::null_mut();
        }
        None => {
            error!("bigredis-path configuration not set, cannot initialize disk storage");
            return std::ptr::null_mut();
        }
    };

    // Compute the disk path: extract parent directory and append "/redisearch"
    let disk_path = match compute_disk_path(&bigredis_path) {
        Some(path) => path,
        None => {
            error!(
                "bigredis-path '{}' is invalid (cannot extract parent directory)",
                bigredis_path
            );
            return std::ptr::null_mut();
        }
    };

    debug!("RediSearch disk storage path: {}", disk_path);

    DiskContext::new(OsStr::new(&disk_path), is_async_io_supported).into_ptr()
}

/// Closes the on-disk index db.
///
/// # Safety
/// 1. `disk_ptr` must have been returned from [`open`].
/// 2. If `disk_ptr` is null (because [`open`] failed), this function is a no-op.
extern "C" fn close(disk_ptr: *mut RedisSearchDisk) {
    // Handle null pointer case (when open failed)
    if disk_ptr.is_null() {
        warn!("close called with null pointer, skipping");
        return;
    }

    // Safety: see safety point 1 above.
    let _ = unsafe { DiskContext::from_ptr(disk_ptr) };

    debug!("closing search disk");
}

/// Checks if async I/O is supported by the underlying storage engine.
///
/// Returns true if io_uring is available and properly initialized, false otherwise.
///
/// # Safety
/// 1. `disk` must have been returned from [`open`].
extern "C" fn is_async_io_supported(disk: *mut RedisSearchDisk) -> bool {
    // Safety: see safety point 1 above.
    let Some(disk_ctx) = (unsafe { DiskContext::try_as_ref(disk) }) else {
        warn!("is_async_io_supported called with null disk pointer");
        return false;
    };
    disk_ctx.is_async_io_supported()
}

/// Opens an index.
///
/// # Safety
/// 1. `index_name` must point to a valid buffer of at least `index_name_len` bytes.
/// 2. `disk` must have been returned from [`open`].
/// 3. If `disk` is null (because [`open`] failed), this function returns null.
extern "C" fn index_spec_open(
    disk: *mut RedisSearchDisk,
    index_name: *const c_char,
    index_name_len: usize,
    document_type: DocumentType,
) -> *mut RedisSearchDiskIndexSpec {
    // Handle null pointer case (when open failed)
    // Safety: See safety point 2 above.
    let Some(disk_context) = (unsafe { DiskContext::try_as_ref(disk) }) else {
        warn!("index_spec_open called with null disk pointer, returning null");
        return std::ptr::null_mut();
    };
    let deleted_ids = DeletedIdsStore::new();

    // Safety: See safety point 1 above.
    let index_name = unsafe {
        let slice = std::slice::from_raw_parts(index_name.cast::<u8>(), index_name_len);
        match std::str::from_utf8(slice) {
            Ok(s) => s.to_string(),
            Err(error) => {
                error!(
                    error = &error as &dyn std::error::Error,
                    "index_name is not valid UTF-8"
                );
                return std::ptr::null_mut();
            }
        }
    };

    // Create the IndexSpec, which will create its own database
    // Pass the disk context which contains the shared cache and WriteBufferManager
    match IndexSpec::new(index_name.clone(), document_type, disk_context, deleted_ids) {
        Ok(index_spec) => Box::into_raw(Box::new(index_spec)) as *mut RedisSearchDiskIndexSpec,
        Err(error) => {
            error!(
                index_name,
                error = &error as &dyn std::error::Error,
                "failed to create column families for index"
            );
            std::ptr::null_mut()
        }
    }
}

/// Closes an index and removes its metrics from the disk context.
///
/// # Safety
/// 1. `disk` must have been returned from [`open`].
/// 2. `index` must have been returned from [`index_spec_open`].
/// 3. If `index` is null (because [`index_spec_open`] failed), this function is a no-op.
extern "C" fn index_spec_close(disk: *mut RedisSearchDisk, index: *mut RedisSearchDiskIndexSpec) {
    // Handle null pointer case (when index_spec_open failed)
    if index.is_null() {
        warn!("index_spec_close called with null index pointer, skipping");
        return;
    }

    let box_ptr = index as *mut IndexSpec;
    // Safety: See safety point 2 above.
    let name = unsafe { (*box_ptr).name().to_string() };
    debug!(index_name = %name, "closing index spec");

    // Remove the index's metrics from the disk context
    if !disk.is_null() {
        // Safety: Caller guarantees disk is a valid DiskContext pointer.
        let disk_ctx = unsafe { &mut *(disk as *mut DiskContext) };
        disk_ctx.remove_index_metrics(&name);
    }

    // Drop the index. If it was marked for deletion, the database's Drop implementation
    // Safety: See safety point 2 above.
    drop(unsafe { Box::from_raw(box_ptr) });
}

/// Marks the index as to be deleted.
///
/// # Safety
/// 1. `index` must have been returned from [`index_spec_open`].
extern "C" fn index_spec_mark_to_be_deleted(index: *mut RedisSearchDiskIndexSpec) {
    // Safety: See safety point 1 above.
    let index = unsafe { IndexSpec::try_as_ref(index) };

    if let Some(index) = index {
        debug!(index_name = index.name(), "requesting deletion on close");
        index.mark_for_deletion();
    } else {
        warn!("index_spec_mark_to_be_deleted called with null pointer, skipping");
    }
}

/// Runs a GC compaction cycle on the disk index.
///
/// Synchronously runs a full compaction on the fulltext inverted index column family,
/// removing entries for deleted documents, and applies the compaction delta to update
/// in-memory structures via FFI calls on the C IndexSpec.
///
/// # Safety
/// 1. `index` must have been returned from [`index_spec_open`].
/// 2. `c_index_spec` must be a valid pointer to a C `IndexSpec` struct.
unsafe extern "C" fn index_spec_run_gc(
    index: *mut RedisSearchDiskIndexSpec,
    c_index_spec: *mut c_void,
) {
    // SAFETY: See safety point 1 above.
    let Some(index) = (unsafe { IndexSpec::try_as_ref(index) }) else {
        warn!("index_spec_run_gc called with null index pointer, skipping");
        return;
    };

    // Check for null c_index_spec pointer to avoid undefined behavior.
    if c_index_spec.is_null() {
        warn!("index_spec_run_gc called with null c_index_spec pointer, skipping");
        return;
    }

    debug!(
        index_name = index.name(),
        "running GC compaction with delta application"
    );

    // SAFETY: See safety point 2 above - c_index_spec must be valid.
    // We cast from c_void to the typed ffi::IndexSpec pointer.
    unsafe { index.compact_text_inverted_index(c_index_spec.cast::<ffi::IndexSpec>()) };
    index.compact_tag_inverted_indexes();
}

/// Saves index spec's disk-related state to RDB.
///
/// # Safety
/// 1. `rdb` must be a valid RedisModuleIO pointer provided by Redis in an RDB save callback.
/// 2. `index` must have been returned from [`index_spec_open`].
extern "C" fn index_spec_rdb_save(
    rdb: *mut ffi::RedisModuleIO,
    index: *mut RedisSearchDiskIndexSpec,
) {
    if rdb.is_null() {
        error!("RDB handle is null");
        return;
    }

    // Safety: see safety point 2 above.
    let Some(index) = (unsafe { IndexSpec::try_as_ref(index) }) else {
        error!("index pointer is null");
        return;
    };

    debug!(index_name = index.name(), "saving index spec to RDB");

    // Cast ffi::RedisModuleIO to redis_module::raw::RedisModuleIO
    // Safety: Both types are opaque pointers to the same underlying C type
    let rdb_redis_module = rdb as *mut redis_module::raw::RedisModuleIO;

    match index.save_to_rdb(rdb_redis_module) {
        Ok(_) => {
            debug!(
                index_name = index.name(),
                "successfully saved index spec to RDB"
            );
        }
        Err(e) => {
            error!(index_name = index.name(), error = %e, "failed to save index spec to RDB");
        }
    }
}

/// Loads index spec's disk-related state from RDB.
/// If `index` is null, we just consume the RDB stream without creating the
/// index-related structures. This is in order not to raise a loading error upon
/// duplicate indexes.
///
/// # Safety
/// 1. `rdb` must be a valid RedisModuleIO pointer provided by Redis in an RDB load callback.
/// 2. If `index` is non-null, it must have been returned from [`index_spec_open`].
///
/// # Returns
/// `REDISMODULE_OK` if the index spec was successfully loaded from the RDB,
/// `REDISMODULE_ERR` otherwise.
extern "C" fn index_spec_rdb_load(
    rdb: *mut ffi::RedisModuleIO,
    index: *mut RedisSearchDiskIndexSpec,
) -> u32 {
    if rdb.is_null() {
        error!("RDB handle is null");
        return REDISMODULE_ERR;
    }

    // Cast ffi::RedisModuleIO to redis_module::raw::RedisModuleIO
    // Safety: Both types are opaque pointers to the same underlying C type
    let rdb_redis_module = rdb as *mut redis_module::raw::RedisModuleIO;

    // Get the index reference if we have a valid pointer
    let index_ref = if !index.is_null() {
        // Safety: see safety point 2 above.
        let Some(idx) = (unsafe { IndexSpec::try_as_ref(index) }) else {
            error!("failed to convert index pointer");
            return REDISMODULE_ERR;
        };
        debug!(index_name = idx.name(), "loading index spec from RDB");
        Some(idx)
    } else {
        debug!("consuming RDB data without creating index (duplicate index)");
        None
    };

    // Always call the static method which handles both cases
    match IndexSpec::load_from_rdb_static(rdb_redis_module, index_ref) {
        Ok(_) => {
            if let Some(idx) = index_ref {
                debug!(
                    index_name = idx.name(),
                    "successfully loaded index spec from RDB"
                );
            } else {
                debug!("successfully consumed RDB data without creating index");
            }
            REDISMODULE_OK
        }
        Err(e) => {
            if let Some(idx) = index_ref {
                error!(index_name = idx.name(), error = %e, "failed to load index spec from RDB");
            } else {
                error!(error = %e, "failed to consume RDB data");
            }
            REDISMODULE_ERR
        }
    }
}

/// Indexes a term for fulltext search.
///
/// # Safety
/// 1. `index` must have been returned from [`index_spec_open`].
/// 2. `term` must point to a valid buffer of at least `term_len` bytes.
extern "C" fn index_spec_index_term(
    index: *mut RedisSearchDiskIndexSpec,
    term: *const c_char,
    term_len: usize,
    doc_id: t_docId,
    field_mask: t_fieldMask,
    frequency: u32,
) -> bool {
    // Safety: see safety point 2 above.
    let term = unsafe {
        let slice = std::slice::from_raw_parts(term.cast::<u8>(), term_len);
        match std::str::from_utf8(slice) {
            Ok(s) => s,
            Err(error) => {
                error!(
                    error = &error as &dyn std::error::Error,
                    "term is not valid UTF-8"
                );
                return false;
            }
        }
    };
    // Safety: see safety point 1 above.
    let Some(index) = (unsafe { IndexSpec::try_as_ref(index) }) else {
        error!("index pointer is null");
        return false;
    };

    debug!(term, doc_id, field_mask, "index_spec_index_term");

    match index
        .term_index()
        .insert(term, doc_id, field_mask, frequency)
    {
        Ok(()) => true,
        Err(error) => {
            error!(
                error = &error as &dyn std::error::Error,
                "failed to index text term"
            );
            false
        }
    }
}

/// Indexes multiple tag values for a document.
///
/// # Safety
/// 1. `index` must have been returned from [`index_spec_open`].
/// 2. `values` must point to an array of `num_values` valid C string pointers.
/// 3. Each string pointer in `values` must be a valid null-terminated C string.
extern "C" fn index_spec_index_tags(
    index: *mut RedisSearchDiskIndexSpec,
    values: *mut *const c_char,
    num_values: usize,
    doc_id: t_docId,
    field_index: t_fieldIndex,
) -> bool {
    if values.is_null() || num_values == 0 {
        // Nothing to index
        return true;
    }

    // Safety: see safety point 1 above.
    let Some(index) = (unsafe { IndexSpec::try_as_ref(index) }) else {
        error!("index pointer is null");
        return false;
    };

    // Get or create the tag index once, outside the loop
    let tag_index = match index.tag_index(field_index) {
        Ok(idx) => idx,
        Err(error) => {
            error!(
                error = &error as &dyn std::error::Error,
                "failed to get or create tag index"
            );
            return false;
        }
    };

    // Safety: see safety points 2 and 3 above.
    let values_slice = unsafe { std::slice::from_raw_parts(values, num_values) };

    for &value_ptr in values_slice {
        if value_ptr.is_null() {
            continue;
        }

        // Safety: value_ptr is a valid null-terminated C string (see safety point 3)
        let c_str = unsafe { std::ffi::CStr::from_ptr(value_ptr) };
        let tag = match c_str.to_str() {
            Ok(s) => s,
            Err(error) => {
                error!(
                    error = &error as &dyn std::error::Error,
                    "tag value is not valid UTF-8"
                );
                return false;
            }
        };

        if let Err(error) = tag_index.insert(tag, doc_id) {
            error!(
                error = &error as &dyn std::error::Error,
                "failed to index tag"
            );
            return false;
        }
    }

    true
}

/// Deletes a document by key, looking up its doc ID, removing it from the doc table and marking its ID as deleted in `index`.
///
/// # Safety
/// 1. `index` must have been returned from [`index_spec_open`].
/// 2. `key` must point to a valid buffer of at least `key_len` bytes.
/// 3. `old_len` must be either null or point to a valid `u32`.
/// 4. `id` must be either null or point to a valid `t_docId`.
extern "C" fn index_spec_delete_document(
    index: *mut RedisSearchDiskIndexSpec,
    key: *const c_char,
    key_len: usize,
    old_len: *mut u32,
    id: *mut t_docId,
) {
    if key.is_null() {
        error!("key pointer is null");
        return;
    }

    // Safety: see safety point 2 above.
    let key_slice = unsafe { std::slice::from_raw_parts(key.cast::<u8>(), key_len) };
    let key_vec = key_slice.to_vec();

    // Safety: see safety point 1 above.
    let Some(index) = (unsafe { IndexSpec::try_as_ref(index) }) else {
        error!("index pointer is null");
        return;
    };

    // Delete the doc via the `delete_document_by_key` API
    match index.doc_table().delete_document_by_key(key_slice) {
        Ok((deleted_id, deleted_doc_len)) => {
            // Safety: see safety point 3 above.
            // If the caller provided an old_len pointer, write the old document length to it
            if let Some(old_len_ref) = unsafe { old_len.as_mut() } {
                *old_len_ref = deleted_doc_len;
            }

            // Safety: see safety point 4 above.
            // If the caller provided an id pointer, write the deleted doc ID to it
            if let Some(id_ref) = unsafe { id.as_mut() } {
                *id_ref = deleted_id;
            }
        }
        Err(e) => {
            error!(error = %e, key = ?key_vec, "failed to delete document");
        }
    }
}

/// Inserts a new document.
///
/// # Safety
/// 1. `index` must have been returned from [`index_spec_open`].
/// 2. `key` must point to a valid buffer of at least `key_len` bytes.
/// 3. `old_len` must be either null or point to a valid `u32`.
/// 4. If `HasExpiration` flag is set, `expiration` must be a valid positive timespec:
///    tv_sec >= 0, tv_nsec >= 0, tv_sec > 0 or tv_nsec > 0, and tv_nsec < 1 billion.
/// 5. If `HasExpiration` flag is not set, `expiration` must be zeroed.
extern "C" fn index_spec_put_doc(
    index: *mut RedisSearchDiskIndexSpec,
    key: *const c_char,
    key_len: usize,
    score: f32,
    flags: u32,
    max_term_freq: u32,
    doc_len: u32,
    old_len: *mut u32,
    expiration: ffi::timespec,
) -> t_docId {
    // Safety: see safety point 2 above.
    let key = unsafe { std::slice::from_raw_parts(key.cast::<u8>(), key_len) };

    debug!(
        key,
        score, flags, max_term_freq, doc_len, "index_spec_put_doc"
    );

    // Safety: see safety point 1 above.
    let Some(index) = (unsafe { IndexSpec::try_as_ref(index) }) else {
        error!("index pointer is null");
        return INVALID_DOC_ID;
    };

    let flags = flags_from_oss(flags);

    // Convert timespec to Option<SystemTime> based on HasExpiration flag
    let expiration = if flags.contains(DocumentFlag::HasExpiration) {
        debug_assert!(
            validate_timespec(&expiration),
            "invalid expiration timespec"
        );
        Some(timespec_to_system_time(&expiration))
    } else {
        if expiration.tv_sec != 0 || expiration.tv_nsec != 0 {
            error!("expiration time must be 0 when HasExpiration flag is not set");
            return INVALID_DOC_ID;
        }
        None
    };

    match index
        .doc_table()
        .insert_document(key, score, flags, max_term_freq, doc_len, expiration)
    {
        Ok((new_doc_id, old_doc_len)) => {
            // Safety: see safety point 3 above.
            // If the caller provided an old_len pointer, write the old document length to it
            if let Some(old_len_ref) = unsafe { old_len.as_mut() } {
                *old_len_ref = old_doc_len;
            }
            new_doc_id
        }
        Err(error) => {
            error!(
                error = &error as &dyn std::error::Error,
                "failed to insert document"
            );
            INVALID_DOC_ID
        }
    }
}

/// Creates a new term iterator for `index`.
///
/// # Safety
/// 1. `index` must have been returned from [`index_spec_open`].
/// 2. `query_term` must be a valid pointer to an `RSQueryTerm` created by `NewQueryTerm`.
///    Ownership of the `RSQueryTerm` is transferred to the iterator and will be freed when the
///    iterator is freed. The caller must NOT call `Term_Free` on the pointer after this call.
unsafe extern "C" fn index_spec_new_term_iterator(
    index: *mut RedisSearchDiskIndexSpec,
    query_term: *mut ffi::RSQueryTerm,
    field_mask: t_fieldMask,
    weight: f64,
) -> *mut QueryIterator {
    if query_term.is_null() {
        error!("query_term pointer is null");
        return std::ptr::null_mut();
    }

    // SAFETY: query_term is guaranteed to be valid by safety point 2.
    // ffi::RSQueryTerm and query_term::RSQueryTerm have the same ABI layout.
    // Take ownership immediately to ensure proper cleanup on all error paths.
    let query_term_box: Box<RSQueryTerm> = unsafe { Box::from_raw(query_term.cast()) };

    let Some(slice) = query_term_box.as_bytes() else {
        error!("query_term has null string pointer");
        return std::ptr::null_mut();
    };
    let term_str = match std::str::from_utf8(slice) {
        Ok(s) => s,
        Err(error) => {
            error!(
                error = &error as &dyn std::error::Error,
                "term is not valid UTF-8"
            );
            return std::ptr::null_mut();
        }
    };

    let idf = query_term_box.idf();
    let bm25_idf = query_term_box.bm25_idf();

    debug!(
        term_str,
        field_mask, weight, idf, bm25_idf, "index_spec_new_term_iterator"
    );

    // SAFETY: see safety point 1 above.
    let Some(index) = (unsafe { IndexSpec::try_as_ref(index) }) else {
        error!("index pointer is null");
        return std::ptr::null_mut();
    };

    match index
        .term_index()
        .term_iterator(query_term_box, field_mask, weight)
    {
        Ok(iterator) => RQEIteratorWrapper::boxed_new(IteratorType_INV_IDX_TERM_ITERATOR, iterator),
        Err(error) => {
            error!(
                error = &error as &dyn std::error::Error,
                "failed to create term iterator"
            );
            std::ptr::null_mut()
        }
    }
}

/// Creates a new wildcard iterator for `index`.
///
/// # Safety
/// 1. `index` must have been returned from [`index_spec_open`].
extern "C" fn index_spec_new_wildcard_iterator(
    index: *mut RedisSearchDiskIndexSpec,
    weight: f64,
) -> *mut QueryIterator {
    debug!(weight, "index_spec_new_wildcard_iterator");

    // Safety: see safety point 1 above.
    let Some(index) = (unsafe { IndexSpec::try_as_ref(index) }) else {
        error!("index pointer is null");
        return std::ptr::null_mut();
    };

    match index.doc_table().wildcard_iterator(weight) {
        Ok(iterator) => {
            RQEIteratorWrapper::boxed_new(IteratorType_INV_IDX_WILDCARD_ITERATOR, iterator)
        }
        Err(error) => {
            error!(
                error = &error as &dyn std::error::Error,
                "failed to create wildcard iterator"
            );
            std::ptr::null_mut()
        }
    }
}

/// Creates a new tag iterator for `index`.
///
/// # Safety
/// 1. `index` must have been returned from [`index_spec_open`].
/// 2. `token` must be a valid pointer to an `RSToken`.
extern "C" fn index_spec_new_tag_iterator(
    index: *mut RedisSearchDiskIndexSpec,
    token: *const RSToken,
    field_index: t_fieldIndex,
    weight: f64,
) -> *mut QueryIterator {
    if token.is_null() {
        error!("token pointer is null");
        return std::ptr::null_mut();
    }

    // SAFETY: token is guaranteed to be valid by safety point 2.
    let tok = unsafe { &*token };
    // SAFETY: tok.str_ and tok.len are valid as token is guaranteed valid by safety point 2.
    let slice = unsafe { std::slice::from_raw_parts(tok.str_ as *const u8, tok.len) };
    let tag_str = match std::str::from_utf8(slice) {
        Ok(s) => s,
        Err(error) => {
            error!(
                error = &error as &dyn std::error::Error,
                "tag is not valid UTF-8"
            );
            return std::ptr::null_mut();
        }
    };

    // Safety: see safety point 1 above.
    let Some(index) = (unsafe { IndexSpec::try_as_ref(index) }) else {
        error!("index pointer is null");
        return std::ptr::null_mut();
    };

    match index.new_tag_iterator(field_index, tag_str, weight) {
        Some(Ok(ptr)) => ptr,
        Some(Err(error)) => {
            error!(
                error = &error as &dyn std::error::Error,
                "failed to create tag iterator"
            );
            std::ptr::null_mut()
        }
        // Tag index doesn't exist - return null iterator (no results)
        None => std::ptr::null_mut(),
    }
}

/// Returns whether `doc_id` is deleted in `index`.
///
/// # Safety
/// 1. `index` must have been returned from [`index_spec_open`].
extern "C" fn index_spec_is_doc_id_deleted(
    index: *mut RedisSearchDiskIndexSpec,
    doc_id: t_docId,
) -> bool {
    debug!(doc_id, "checking if document with id has been deleted");

    // Safety: see safety point 1 above.
    let Some(index) = (unsafe { IndexSpec::try_as_ref(index) }) else {
        error!("index pointer is null");
        return false;
    };

    index.doc_table().is_deleted(doc_id)
}

/// Populates an allocated RSDocumentMetadata struct with data from DocumentMetadata.
fn populate_dmd(
    dmd_ref: &mut ffi::RSDocumentMetadata,
    doc_id: ffi::t_docId,
    dmd: &index_spec::doc_table::DocumentMetadata,
    document_type: ffi::DocumentType,
) {
    dmd_ref.id = doc_id;
    // keyPtr is already set by allocate_dmd callback
    dmd_ref.score = dmd.score;
    dmd_ref.set_maxTermFreq(dmd.max_term_freq);
    dmd_ref.set_flags(flags_to_oss(dmd.flags));
    dmd_ref.set_docLen(dmd.doc_len);
    dmd_ref.set_type(document_type);
}

/// Retrieves the document metadata for `doc_id` and writes it to `dmd`.
/// Returns false if the document is not found or has expired.
///
/// # Safety
/// 1. `index` must have been returned from [`index_spec_open`].
/// 2. `dmd` must be a valid pointer to a [`RSDocumentMetadata`].
extern "C" fn index_spec_get_document_metadata(
    index: *mut RedisSearchDiskIndexSpec,
    doc_id: t_docId,
    dmd: *mut RSDocumentMetadata,
    allocate_key: AllocateKeyCallback,
    current_time: ffi::timespec,
) -> bool {
    debug!(doc_id, "getting metadata for document with id");

    debug_assert!(!dmd.is_null(), "dmd should not be a NULL pointer");
    debug_assert!(
        allocate_key.is_some(),
        "allocate_key callback should be set"
    );

    // Safety: see safety point 2 above.
    let dmd = unsafe { &mut *dmd };
    let Some(allocate_key) = allocate_key else {
        error!("allocate_key callback is None");
        return false;
    };
    // Safety: see safety point 1 above.
    let Some(index) = (unsafe { IndexSpec::try_as_ref(index) }) else {
        error!("index pointer is null");
        return false;
    };

    let doc_table = index.doc_table();
    let Ok(Some(doc_table_dmd)) = doc_table.get_document_metadata(doc_id) else {
        return false;
    };

    // Check if document has expired
    if let Some(expiration) = doc_table_dmd.expiration {
        debug_assert!(
            validate_timespec(&current_time),
            "invalid current_time timespec"
        );
        let current = timespec_to_system_time(&current_time);
        if expiration <= current {
            debug!(doc_id, "document has expired");
            return false;
        }
    }

    dmd.id = doc_id;
    // Safety: `doc_table_dmd.key` is an owned Vec<u8>, and `allocate_key` only
    // performs a read operation over this Vec<u8>.
    dmd.keyPtr = unsafe {
        allocate_key(
            doc_table_dmd.key.as_ptr().cast::<c_void>(),
            doc_table_dmd.key.len(),
        )
    };
    populate_dmd(dmd, doc_id, &doc_table_dmd, doc_table.document_type());

    true
}

// ============================================================================
// Async Read Pool API
// ============================================================================

/// Opaque handle for the async read pool.
/// Used to manage async document metadata reads and populate results.
struct AsyncReadPoolHandle {
    pool: index_spec::doc_table::AsyncReadPool<'static>,
    document_type: DocumentType,
}

/// Creates an async read pool for batched document metadata reads.
///
/// # Safety
/// 1. `index` must have been returned from [`index_spec_open`].
/// 2. The returned pool handle must be freed before the index is closed.
///
/// # Returns
/// An opaque pointer to the pool handle, or null on error.
unsafe extern "C" fn index_spec_create_async_read_pool(
    index: *mut RedisSearchDiskIndexSpec,
    max_concurrent: u16,
) -> ffi::RedisSearchDiskAsyncReadPool {
    debug!(max_concurrent, "creating async read pool");

    // Safety: see safety point 1 above.
    let Some(index) = (unsafe { IndexSpec::try_as_ref(index) }) else {
        error!("index pointer is null");
        return std::ptr::null();
    };

    let doc_table = index.doc_table();
    let document_type = doc_table.document_type();

    // Safety: We extend the lifetime to 'static; see safety point 2 above.
    let doc_table_static: &'static index_spec::doc_table::DocTable =
        unsafe { std::mem::transmute(doc_table) };

    let Some(pool) = index_spec::doc_table::AsyncReadPool::new(doc_table_static, max_concurrent)
    else {
        error!("failed to create tokio runtime for async read pool");
        return std::ptr::null();
    };

    let handle = Box::new(AsyncReadPoolHandle {
        pool,
        document_type,
    });

    Box::into_raw(handle) as ffi::RedisSearchDiskAsyncReadPool
}

/// Adds an async read request to the pool for the given document ID.
///
/// # Safety
/// 1. `pool` must have been returned from [`index_spec_create_async_read_pool`].
///
/// # Returns
/// `true` if the request was added, `false` if the pool is at capacity.
unsafe extern "C" fn index_spec_add_async_read(
    pool: ffi::RedisSearchDiskAsyncReadPool,
    doc_id: t_docId,
    user_data: u64,
) -> bool {
    let pool = pool as *mut c_void;
    if pool.is_null() {
        error!("pool pointer is null");
        return false;
    }

    // Safety: see safety point 1 above.
    let handle = unsafe { &mut *(pool as *mut AsyncReadPoolHandle) };

    handle.pool.add_read(doc_id, user_data)
}

/// Validated inputs for poll_async_reads.
struct PollAsyncReadsInputs<'a> {
    handle: &'a mut AsyncReadPoolHandle,
    results_slice: &'a mut [ffi::AsyncReadResult],
    failed_slice: &'a mut [u64],
    allocate_dmd: unsafe extern "C" fn(*const c_void, usize) -> *mut ffi::RSDocumentMetadata,
}

/// Validates inputs for poll_async_reads, returning None on validation failure.
///
/// # Safety
/// 1. `pool` must have been returned from [`index_spec_create_async_read_pool`].
/// 2. `results` must be a valid pointer to an array of `results_capacity` AsyncReadResult (capacity > 0).
/// 3. `failed_user_data` must be a valid pointer to an array of `failed_capacity` u64 (capacity > 0).
/// 4. `allocate_dmd` must be a valid callback function.
unsafe fn validate_poll_inputs<'a>(
    pool: ffi::RedisSearchDiskAsyncReadPool,
    results: *mut ffi::AsyncReadResult,
    results_capacity: u16,
    failed_user_data: *mut u64,
    failed_capacity: u16,
    allocate_dmd: ffi::AllocateDMDCallback,
) -> Option<PollAsyncReadsInputs<'a>> {
    let pool = pool as *mut c_void;
    if pool.is_null() {
        error!("pool pointer is null");
        return None;
    }

    if results.is_null() || results_capacity == 0 {
        error!("results buffer is null or empty");
        return None;
    }

    if failed_user_data.is_null() || failed_capacity == 0 {
        error!("failed_user_data buffer is null or empty");
        return None;
    }

    let allocate_dmd = allocate_dmd?;

    // Safety: caller guarantees pool is valid.
    let handle = unsafe { &mut *(pool as *mut AsyncReadPoolHandle) };

    // Safety: caller guarantees results is valid with results_capacity elements.
    let results_slice =
        unsafe { std::slice::from_raw_parts_mut(results, results_capacity as usize) };

    // SAFETY: Caller guarantees failed_user_data is valid with failed_capacity elements.
    let failed_slice =
        unsafe { std::slice::from_raw_parts_mut(failed_user_data, failed_capacity as usize) };

    Some(PollAsyncReadsInputs {
        handle,
        results_slice,
        failed_slice,
        allocate_dmd,
    })
}

/// Polls the pool for ready results.
///
/// # TODO
/// This function does not check document expiration, unlike the synchronous
/// `index_spec_get_document_metadata` which filters out expired documents.
/// To fix this, we need to:
/// 1. Add a `current_time: ffi::timespec` parameter
/// 2. Add expiration check before calling `populate_dmd`
/// 3. Update the C-side FFI definition and callers
///
/// # Safety
/// 1. `pool` must have been returned from [`index_spec_create_async_read_pool`].
/// 2. `results` must be a valid pointer to an array of `results_capacity` AsyncReadResult.
/// 3. `failed_user_data` must be a valid pointer to an array of `failed_capacity` u64.
/// 4. `expiration_point` must be a valid timespec for expiration checking.
/// 5. `allocate_dmd` must be a valid callback function.
///
/// # Returns
/// AsyncPollResult with counts of ready, failed, and pending reads.
unsafe extern "C" fn index_spec_poll_async_reads(
    pool: ffi::RedisSearchDiskAsyncReadPool,
    timeout_ms: u32,
    results: *mut ffi::AsyncReadResult,
    results_capacity: u16,
    failed_user_data: *mut u64,
    failed_capacity: u16,
    expiration_point: ffi::timespec,
    allocate_dmd: ffi::AllocateDMDCallback,
) -> ffi::AsyncPollResult {
    let empty_result = ffi::AsyncPollResult {
        ready_count: 0,
        failed_count: 0,
        pending_count: 0,
    };

    // Safety: caller guarantees all pointers are valid per the safety requirements.
    let Some(PollAsyncReadsInputs {
        handle,
        results_slice,
        failed_slice,
        allocate_dmd,
    }) = (unsafe {
        validate_poll_inputs(
            pool,
            results,
            results_capacity,
            failed_user_data,
            failed_capacity,
            allocate_dmd,
        )
    })
    else {
        return empty_result;
    };

    let document_type = handle.document_type;
    let mut ready_count = 0u16;
    let mut failed_count = 0u16;
    let expiration_time = timespec_to_system_time(&expiration_point);

    let pending_count = handle.pool.poll_with_callbacks(
        timeout_ms,
        results_capacity,
        expiration_time,
        |doc_id, dmd, user_data| {
            // SAFETY: allocate_dmd is a C callback provided by the caller. The caller guarantees
            // it is a valid function pointer. We pass a valid pointer to the key data and its length.
            let allocated_dmd =
                unsafe { allocate_dmd(dmd.key.as_ptr().cast::<c_void>(), dmd.key.len()) };
            // SAFETY: We check for null via as_mut() which returns None for null pointers.
            let Some(dmd_ref) = (unsafe { allocated_dmd.as_mut() }) else {
                error!("Failed allocating document metadata, this is unexpected. user provided data will probably leak!");
                return;
            };

            let result = &mut results_slice[ready_count as usize];
            ready_count += 1;

            populate_dmd(dmd_ref, doc_id, &dmd, document_type);
            result.dmd = allocated_dmd;
            result.user_data = user_data;
        },
        |user_data| {
            if (failed_count as usize) < failed_slice.len() {
                failed_slice[failed_count as usize] = user_data;
                failed_count += 1;
                // Stop if we just filled the last slot
                (failed_count as usize) < failed_slice.len()
            } else {
                false // Already full (shouldn't happen if we stop correctly)
            }
        },
    );

    ffi::AsyncPollResult {
        ready_count,
        failed_count,
        pending_count,
    }
}

/// Frees the async read pool and cancels any pending reads.
///
/// # Safety
/// 1. `pool` must have been returned from [`index_spec_create_async_read_pool`].
/// 2. `pool` must not be used after this call.
unsafe extern "C" fn index_spec_free_async_read_pool(pool: ffi::RedisSearchDiskAsyncReadPool) {
    let pool = pool as *mut c_void;
    if pool.is_null() {
        return;
    }

    debug!("freeing async read pool");

    // Safety: see safety point 1 above. We take ownership and drop it.
    let _ = unsafe { Box::from_raw(pool as *mut AsyncReadPoolHandle) };
}

/// Gets the maximum document ID for `index`.
///
/// # Safety
/// 1. `index` must have been returned from [`index_spec_open`].
extern "C" fn index_spec_get_max_doc_id(index: *mut RedisSearchDiskIndexSpec) -> t_docId {
    debug!("getting max doc id");

    // Safety: see safety point 1 above.
    let Some(index) = (unsafe { IndexSpec::try_as_ref(index) }) else {
        error!("index pointer is null");
        return INVALID_DOC_ID;
    };

    index.doc_table().get_last_doc_id()
}

/// Gets the count of deleted document IDs for `index`.
///
/// # Safety
/// 1. `index` must have been returned from [`index_spec_open`].
extern "C" fn index_spec_get_deleted_ids_count(index: *mut RedisSearchDiskIndexSpec) -> u64 {
    debug!("getting deleted ids count");

    // Safety: see safety point 1 above.
    let Some(index) = (unsafe { IndexSpec::try_as_ref(index) }) else {
        error!("index pointer is null");
        return 0;
    };

    index.doc_table().deleted_ids_len()
}

/// Gets all deleted document IDs for `index`. Used for debugging(!)
///
/// # Safety
/// 1. `index` must have been returned from [`index_spec_open`].
/// 2. `buffer` must point to a valid buffer of at least `buffer_size` elements.
extern "C" fn index_spec_get_deleted_ids(
    index: *mut RedisSearchDiskIndexSpec,
    buffer: *mut t_docId,
    buffer_size: usize,
) -> usize {
    debug!(buffer_size, "getting deleted ids");

    if buffer.is_null() {
        error!("buffer pointer is null");
        return 0;
    }

    // Safety: see safety point 2 above.
    let buf_slice: &mut [u64] = unsafe { std::slice::from_raw_parts_mut(buffer, buffer_size) };

    // Safety: see safety point 1 above.
    let Some(index) = (unsafe { IndexSpec::try_as_ref(index) }) else {
        error!("index pointer is null");
        return 0;
    };

    let deleted_ids = index.doc_table().collect_deleted_ids();

    utils::fill_buf(buf_slice, &deleted_ids[..])
}

/// FFI methods and associated functions for converting a [`DiskContext`] to and
/// from the corresponding types on the C-side.
impl DiskContext {
    /// Leaks `self` into an opaque pointer type.
    fn into_ptr(self) -> *mut RedisSearchDisk {
        let disk_context = Box::new(self);

        Box::into_raw(disk_context).cast::<RedisSearchDisk>()
    }

    /// Casts an opaque `RedisSearchDisk` to an immutable `DiskContext` reference.
    /// Does not consume the pointer.
    ///
    /// # Safety
    /// 1. `ptr` must have been returned from [`Self::into_ptr`].
    ///
    /// Returns `None` if `ptr` is null.
    unsafe fn try_as_ref<'a>(ptr: *mut RedisSearchDisk) -> Option<&'a Self> {
        let disk_context: *const Self = ptr.cast();
        // Safety: see safety point 1 above.
        unsafe { disk_context.as_ref() }
    }

    /// Casts an opaque `RedisSearchDisk` to an owned `DiskContext`.
    /// Consumes the provided pointer.
    ///
    /// # Safety
    /// 1. `ptr` must have been returned from [`Self::into_ptr`].
    unsafe fn from_ptr(ptr: *mut RedisSearchDisk) -> Self {
        let disk_context = ptr.cast();
        // Safety: see safety point 1 above.
        let disk_context = unsafe { Box::from_raw(disk_context) };

        *disk_context
    }
}

/// FFI: obtain a reference to the index from a raw pointer produced by [`index_spec_open`] via [`try_as_ref`](Self::try_as_ref).
impl IndexSpec {
    /// Returns a shared reference to the index from a raw pointer produced by [`index_spec_open`].
    /// Succeeds regardless of ref count. Returns `None` if `ptr` is null.
    ///
    /// # Safety
    /// 1. `ptr` must point to a valid `IndexSpec` returned from [`index_spec_open`], and must not be used after the index is closed.
    pub(crate) unsafe fn try_as_ref<'a>(ptr: *mut RedisSearchDiskIndexSpec) -> Option<&'a Self> {
        // Safety: See safety point 1 above.
        unsafe { (ptr as *const Self).as_ref() }
    }
}

/// Creates a disk-based vector index.
///
/// The returned pointer is a `VecSimIndex*` that can be used with all standard
/// `VecSimIndex_*` functions (AddVector, TopKQuery, etc.) due to polymorphism.
extern "C" fn vector_create_index(
    index: *mut RedisSearchDiskIndexSpec,
    params: *const VecSimParamsDisk,
) -> *mut c_void {
    // SAFETY: Caller (RediSearch C code) guarantees index is a valid pointer we created
    let Some(index_spec) = (unsafe { IndexSpec::try_as_ref(index) }) else {
        error!("vector_create_index: index is null");
        return std::ptr::null_mut();
    };

    // SAFETY: Caller guarantees params is a valid pointer to VecSimParamsDisk
    let params_ref = match unsafe { params.as_ref() } {
        Some(p) => p,
        None => {
            error!("vector_create_index: params is null");
            return std::ptr::null_mut();
        }
    };

    // SAFETY: diskContext is a valid pointer set by the caller (C code), and we check for null immediately after
    let disk_ctx_ref = match unsafe { params_ref.diskContext.as_ref() } {
        Some(d) => d,
        None => {
            error!("vector_create_index: diskContext is null");
            return std::ptr::null_mut();
        }
    };

    // SAFETY: params_ref.indexName points to indexNameLen valid bytes (guaranteed by caller)
    let field_name = unsafe { c_str_to_str(disk_ctx_ref.indexName, disk_ctx_ref.indexNameLen) };
    let Some(field_name) = field_name else {
        error!("vector_create_index: field name is null or invalid UTF-8");
        return std::ptr::null_mut();
    };

    debug!(
        index = index_spec.name(),
        field_name, "creating vector index"
    );

    let database = index_spec.database();
    let cf_name = format!("{}:{}", index_spec.name(), field_name);

    // Create column family if needed - with EdgeListMergeOperator for HNSW incoming edges
    if database.cf_handle(&cf_name).is_none() {
        let mut cf_options = speedb::Options::default();
        // The EdgeListMergeOperator handles APPEND and DELETE operations for HNSW edge lists.
        // This is required for batch_merge_incoming_edges in HNSWStorage to work correctly.
        //
        // IMPORTANT: We use set_merge_operator (not set_merge_operator_associative) to provide
        // separate full_merge and partial_merge functions. The C++ EdgeListMergeOperator uses
        // different formats for operands ('A'/'D' prefixed) and values (raw edge bytes).
        // - FullMerge: Produces raw edge bytes (final value format)
        // - PartialMerge: Preserves operand format (for intermediate compaction results)
        // Using the same function for both (as associative does) causes compaction crashes
        // because partial merge results are stored as operands but would be in value format.
        cf_options.set_merge_operator(
            "EdgeListMergeOperator",
            EdgeListMergeOperator::full_merge_fn(),
            EdgeListMergeOperator::partial_merge_fn(),
            true,
        );
        if database.create_cf(&cf_name, &cf_options).is_err() {
            error!("vector_create_index: failed to create CF");
            return std::ptr::null_mut();
        }
    }

    let db_ptr = database.as_raw_db();
    let Some(cf_ptr) = database.cf_handle_raw(&cf_name) else {
        error!("vector_create_index: failed to get CF handle");
        return std::ptr::null_mut();
    };

    // C++ copies these pointer values immediately in the constructor, so stack allocation is fine
    let storage = SpeeDBHandles {
        db: db_ptr.cast(),
        cf: cf_ptr.cast(),
    };

    // Create a local copy of diskContext with our storage pointer.
    // This avoids modifying the caller's diskContext, which would create a dangling pointer
    // after this function returns (since storage is stack-allocated).
    let mut local_disk_ctx = VecSimDiskContext {
        storage: &storage as *const SpeeDBHandles as *mut c_void,
        ..*disk_ctx_ref
    };

    // Create params pointing to our local diskContext.
    let params_with_storage = VecSimParamsDisk {
        diskContext: &mut local_disk_ctx as *mut VecSimDiskContext,
        ..*params_ref
    };

    // SAFETY: params_with_storage is a valid VecSimParamsDisk with valid storage pointers
    let cpp_index = unsafe {
        VecSimDisk_CreateIndex(&params_with_storage as *const VecSimParamsDisk as *const c_void)
    };

    if cpp_index.is_null() {
        error!("vector_create_index: VecSimDisk_CreateIndex returned null");
        return std::ptr::null_mut();
    }

    cpp_index
}

/// Frees a disk-based vector index.
extern "C" fn vector_free_index(vec_index: *mut c_void) {
    if vec_index.is_null() {
        warn!("vector_free_index: called with null pointer, skipping");
        return;
    }

    debug!("vector_free_index: freeing disk-based vector index");

    // SAFETY: vec_index is a valid pointer returned by VecSimDisk_CreateIndex (checked not null above)
    unsafe { vecsim_disk::VecSimDisk_FreeIndex(vec_index) };
}

/// Collects metrics for an index and stores them in the disk context.
///
/// Returns the total memory used by this index's disk components.
///
/// # Safety
/// 1. `disk` must be a valid pointer to a DiskContext
/// 2. `index` must be a valid pointer to an IndexSpec
unsafe extern "C" fn collect_index_metrics(
    disk: *mut RedisSearchDisk,
    index: *mut RedisSearchDiskIndexSpec,
) -> u64 {
    if disk.is_null() || index.is_null() {
        error!("collect_index_metrics: null pointer passed");
        return 0;
    }

    // SAFETY: Caller guarantees disk is a valid DiskContext pointer
    let disk_ctx = unsafe { &mut *(disk as *mut DiskContext) };

    // SAFETY: Caller guarantees index is a valid IndexSpec pointer
    let Some(index_spec) = (unsafe { IndexSpec::try_as_ref(index) }) else {
        error!("collect_index_metrics: failed to convert index pointer");
        return 0;
    };

    disk_ctx.collect_index_metrics(index_spec)
}

/// Outputs aggregated disk metrics to Redis INFO.
///
/// # Safety
/// 1. `disk` must be a valid pointer to a DiskContext
/// 2. `ctx` must be a valid pointer to a RedisModuleInfoCtx
unsafe extern "C" fn output_info_metrics(disk: *mut RedisSearchDisk, ctx: *mut RedisModuleInfoCtx) {
    if disk.is_null() || ctx.is_null() {
        error!("output_info_metrics: null pointer passed");
        return;
    }

    // SAFETY: Caller guarantees disk is a valid DiskContext pointer
    let disk_ctx = unsafe { &*(disk as *const DiskContext) };

    // SAFETY: Caller guarantees ctx is a valid RedisModuleInfoCtx pointer
    let mut ctx_ref = unsafe { &mut *ctx };
    disk_ctx.output_info_metrics(&mut ctx_ref);
}
