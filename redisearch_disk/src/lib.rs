pub mod database;
pub mod disk_context;
pub mod document_id_key;
pub mod index_spec;
pub mod key_traits;
pub mod merge_op;
pub mod metrics;
pub mod utils;
pub mod value_traits;
pub mod vecsim_disk;

use crate::disk_context::DiskContext;
use crate::index_spec::IndexSpec;
use crate::index_spec::deleted_ids::DeletedIdsStore;
use crate::index_spec::doc_table::{DocumentFlag, flags_from_oss, flags_to_oss};
use crate::vecsim_disk::{SpeeDBHandles, VecSimDisk_CreateIndex};
use document::DocumentType;
use ffi::{
    AllocateKeyCallback, BasicDiskAPI, DiskColumnFamilyMetrics, DocTableDiskAPI, IndexDiskAPI,
    IteratorType_INV_IDX_TERM_ITERATOR, IteratorType_INV_IDX_WILDCARD_ITERATOR, MetricsDiskAPI,
    QueryIterator, REDISMODULE_ERR, REDISMODULE_OK, RSDocumentMetadata, RedisModuleCtx,
    RedisSearchDisk, RedisSearchDiskAPI, RedisSearchDiskIndexSpec, VecSimDiskContext,
    VecSimParamsDisk, VectorDiskAPI, t_docId, t_fieldMask,
};
use rqe_iterators_interop::RQEIteratorWrapper;

use std::ffi::{OsStr, c_char, c_void};
use std::time::{Duration, UNIX_EPOCH};
use tracing::{debug, error, warn};

use crate::utils::{compute_disk_path, get_redis_config_value};

/// Registers the Redis module allocator as the global allocator for the application.
#[cfg(feature = "redis_allocator")]
#[global_allocator]
static REDIS_MODULE_ALLOCATOR: redis_module::alloc::RedisAlloc = redis_module::alloc::RedisAlloc;

const INVALID_DOC_ID: t_docId = 0;
const INVALID_NANOSECONDS_THRESHOLD: u32 = 1_000_000_000;

/// Validates that a timespec represents a valid positive time value.
/// Returns `true` if valid, or `false` if invalid.
#[cfg(debug_assertions)]
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
        },
        index: IndexDiskAPI {
            indexDocument: Some(index_spec_index_doc),
            deleteDocument: Some(index_spec_delete_document),
            newTermIterator: Some(index_spec_new_term_iterator),
            newWildcardIterator: Some(index_spec_new_wildcard_iterator),
            markToBeDeleted: Some(index_spec_mark_to_be_deleted),
        },
        docTable: DocTableDiskAPI {
            putDocument: Some(index_spec_put_doc),
            isDocIdDeleted: Some(index_spec_is_doc_id_deleted),
            getDocumentMetadata: Some(index_spec_get_document_metadata),
            getMaxDocId: Some(index_spec_get_max_doc_id),
            getDeletedIdsCount: Some(index_spec_get_deleted_ids_count),
            getDeletedIds: Some(index_spec_get_deleted_ids),
        },
        vector: VectorDiskAPI {
            createVectorIndex: Some(vector_create_index),
            freeVectorIndex: Some(vector_free_index),
        },
        metrics: MetricsDiskAPI {
            collectDocTableMetrics: Some(collect_doc_table_metrics),
            collectTextInvertedIndexMetrics: Some(collect_inverted_index_metrics),
        },
    };

    &raw mut API
}

/// Opens the on-disk index db.
///
/// This function retrieves the `bigredis-path` configuration from Redis,
/// computes the disk storage path by extracting the parent directory and
/// appending "/redisearch", then creates the PathPrefix for individual index databases.
///
/// # Safety
/// 1. `ctx` must be a valid RedisModuleCtx pointer.
extern "C" fn open(ctx: *mut RedisModuleCtx) -> *mut RedisSearchDisk {
    debug!("opening search disk");

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

    DiskContext::new(OsStr::new(&disk_path)).into_ptr()
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
    let Some(disk_context) = (unsafe { DiskContext::try_as_mut(disk) }) else {
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
        Ok(index_spec) => index_spec.into_ptr(),
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

/// Closes an index.
///
/// # Safety
/// 1. `index` must have been returned from [`index_spec_open`].
/// 2. If `index` is null (because [`index_spec_open`] failed), this function is a no-op.
extern "C" fn index_spec_close(index: *mut RedisSearchDiskIndexSpec) {
    // Handle null pointer case (when index_spec_open failed)
    if index.is_null() {
        warn!("index_spec_close called with null pointer, skipping");
        return;
    }

    // Safety: See safety point 1 above.
    let index = unsafe { IndexSpec::from_ptr(index) };

    debug!(index_name = index.name(), "closing index spec");

    // Drop the index. If it was marked for deletion, the database's Drop implementation
    // will automatically destroy the database files.
    drop(index);
}

/// Marks the index as to be deleted.
///
/// # Safety
/// 1. `index` must have been returned from [`index_spec_open`].
extern "C" fn index_spec_mark_to_be_deleted(index: *mut RedisSearchDiskIndexSpec) {
    // Safety: See safety point 1 above.
    let index = unsafe { IndexSpec::try_as_mut(index) };

    if let Some(index) = index {
        debug!(index_name = index.name(), "requesting deletion on close");
        index.mark_for_deletion();
    } else {
        warn!("index_spec_mark_to_be_deleted called with null pointer, skipping");
    }
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
    let Some(index) = (unsafe { IndexSpec::try_as_mut(index) }) else {
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
        let Some(idx) = (unsafe { IndexSpec::try_as_mut(index) }) else {
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
    match IndexSpec::load_from_rdb_static(rdb_redis_module, index_ref.as_deref()) {
        Ok(_) => {
            if let Some(ref idx) = index_ref {
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
            if let Some(ref idx) = index_ref {
                error!(index_name = idx.name(), error = %e, "failed to load index spec from RDB");
            } else {
                error!(error = %e, "failed to consume RDB data");
            }
            REDISMODULE_ERR
        }
    }
}

/// Indexes a new document.
///
/// # Safety
/// 1. `index` must have been returned from [`index_spec_open`].
/// 2. `term` must point to a valid buffer of at least `term_len` bytes.
extern "C" fn index_spec_index_doc(
    index: *mut RedisSearchDiskIndexSpec,
    term: *const c_char,
    term_len: usize,
    doc_id: t_docId,
    field_mask: t_fieldMask,
    _frequency: u32,
) -> bool {
    // Safety: see safety point 2 above.
    let term = unsafe {
        let slice = std::slice::from_raw_parts(term.cast::<u8>(), term_len);
        match std::str::from_utf8(slice) {
            Ok(s) => s.to_string(),
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
    let Some(index) = (unsafe { IndexSpec::try_as_mut(index) }) else {
        error!("index pointer is null");
        return false;
    };

    debug!(term, doc_id, field_mask, "index_spec_index_doc");

    match index.inverted_index().insert(term, doc_id, field_mask, 0) {
        Ok(()) => true,
        Err(error) => {
            error!(
                error = &error as &dyn std::error::Error,
                "failed to index document"
            );
            false
        }
    }
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
    let Some(index) = (unsafe { IndexSpec::try_as_mut(index) }) else {
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
    let Some(index) = (unsafe { IndexSpec::try_as_mut(index) }) else {
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
/// 2. `term` must point to a valid buffer of at least `term_len` bytes.
extern "C" fn index_spec_new_term_iterator(
    index: *mut RedisSearchDiskIndexSpec,
    term: *const c_char,
    term_len: usize,
    field_mask: t_fieldMask,
    weight: f64,
    _idf: f64,
    _bm25_idf: f64,
) -> *mut QueryIterator {
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
                return std::ptr::null_mut();
            }
        }
    };

    debug!(term, field_mask, weight, "index_spec_new_term_iterator");

    // Safety: see safety point 1 above.
    let Some(index) = (unsafe { IndexSpec::try_as_mut(index) }) else {
        error!("index pointer is null");
        return std::ptr::null_mut();
    };

    match index
        .inverted_index()
        .term_iterator(term, field_mask, weight)
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
    let Some(index) = (unsafe { IndexSpec::try_as_mut(index) }) else {
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
    let Some(index) = (unsafe { IndexSpec::try_as_mut(index) }) else {
        error!("index pointer is null");
        return false;
    };

    index.doc_table().is_deleted(doc_id)
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
    let Some(index) = (unsafe { IndexSpec::try_as_mut(index) }) else {
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
    dmd.score = doc_table_dmd.score;
    dmd.set_maxTermFreq(doc_table_dmd.max_term_freq);
    dmd.set_flags(flags_to_oss(doc_table_dmd.flags));
    dmd.set_docLen(doc_table_dmd.doc_len);
    dmd.set_type(doc_table.document_type());

    true
}

/// Gets the maximum document ID for `index`.
///
/// # Safety
/// 1. `index` must have been returned from [`index_spec_open`].
extern "C" fn index_spec_get_max_doc_id(index: *mut RedisSearchDiskIndexSpec) -> t_docId {
    debug!("getting max doc id");

    // Safety: see safety point 1 above.
    let Some(index) = (unsafe { IndexSpec::try_as_mut(index) }) else {
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
    let Some(index) = (unsafe { IndexSpec::try_as_mut(index) }) else {
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
    let Some(index) = (unsafe { IndexSpec::try_as_mut(index) }) else {
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

    /// Casts an opaque `RedisSearchDisk` to a mutable `DiskContext` reference.
    /// Does not consume the pointer.
    ///
    /// # Safety
    /// 1. `ptr` must have been returned from [`Self::into_ptr`].
    /// 2. `ptr` must not be aliased.
    ///
    /// Returns `None` if `ptr` is null.
    unsafe fn try_as_mut<'a>(ptr: *mut RedisSearchDisk) -> Option<&'a mut Self> {
        let disk_context: *mut Self = ptr.cast();
        // Safety: see safety point 1 above.
        unsafe { disk_context.as_mut() }
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

/// FFI methods and associated functions for converting an [`IndexSpec`] to and
/// from the corresponding types on the C-side.
impl IndexSpec {
    /// Casts an opaque `RedisSearchDiskIndexSpec` to a mutable `IndexSpec` reference.
    /// Does not consume the pointer.
    ///
    /// # Safety
    /// 1. `ptr` must have been returned from [`Self::into_ptr`].
    ///
    /// Returns `None` if `ptr` is null.
    unsafe fn try_as_mut<'a>(ptr: *mut RedisSearchDiskIndexSpec) -> Option<&'a mut Self> {
        let index: *mut Self = ptr.cast();
        // Safety: see safety point 1 above.
        unsafe { index.as_mut() }
    }

    /// Leaks `self` into an opaque pointer type.
    fn into_ptr(self) -> *mut RedisSearchDiskIndexSpec {
        let index = Box::new(self);

        Box::into_raw(index).cast::<RedisSearchDiskIndexSpec>()
    }

    /// Casts an opaque `RedisSearchDiskIndexSpec` to an owned `IndexSpec`.
    /// Consumes the provided pointer.
    ///
    /// # Safety
    /// 1. `ptr` must have been returned from [`Self::into_ptr`].
    unsafe fn from_ptr(ptr: *mut RedisSearchDiskIndexSpec) -> Self {
        let index = ptr.cast();
        // Safety: see safety point 1 above.
        let index = unsafe { Box::from_raw(index) };

        *index
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
    let Some(index_spec) = (unsafe { IndexSpec::try_as_mut(index) }) else {
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

    // Create column family if needed
    if database.cf_handle(&cf_name).is_none()
        && database
            .create_cf(&cf_name, &speedb::Options::default())
            .is_err()
    {
        error!("vector_create_index: failed to create CF");
        return std::ptr::null_mut();
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

/// Collects metrics for the doc_table column family.
///
/// # Safety
/// 1. `index` must be a valid pointer to an IndexSpec
/// 2. `metrics` must be a valid pointer to a DiskColumnFamilyMetrics struct
unsafe extern "C" fn collect_doc_table_metrics(
    index: *mut RedisSearchDiskIndexSpec,
    metrics: *mut DiskColumnFamilyMetrics,
) -> bool {
    if index.is_null() || metrics.is_null() {
        error!("collect_doc_table_metrics: null pointer passed");
        return false;
    }

    // SAFETY: Caller guarantees index is a valid IndexSpec pointer
    let Some(index_spec) = (unsafe { IndexSpec::try_as_mut(index) }) else {
        error!("collect_doc_table_metrics: failed to convert index pointer");
        return false;
    };

    // SAFETY: `metrics` was checked for null above, and the caller guarantees
    // it points to a valid `DiskColumnFamilyMetrics` instance with the expected
    // layout for the duration of this call.
    let metrics_ref: &mut DiskColumnFamilyMetrics = unsafe { &mut *metrics };

    index_spec
        .doc_table()
        .collect_metrics()
        .populate_metrics(metrics_ref);

    true
}

/// Collects metrics for the inverted_index (fulltext) column family.
///
/// # Safety
/// 1. `index` must be a valid pointer to an IndexSpec
/// 2. `metrics` must be a valid pointer to a DiskColumnFamilyMetrics struct
unsafe extern "C" fn collect_inverted_index_metrics(
    index: *mut RedisSearchDiskIndexSpec,
    metrics: *mut DiskColumnFamilyMetrics,
) -> bool {
    if index.is_null() || metrics.is_null() {
        error!("collect_inverted_index_metrics: null pointer passed");
        return false;
    }

    // SAFETY: Caller guarantees index is a valid IndexSpec pointer
    let Some(index_spec) = (unsafe { IndexSpec::try_as_mut(index) }) else {
        error!("collect_inverted_index_metrics: failed to convert index pointer");
        return false;
    };

    // SAFETY: `metrics` was checked for null above, and the caller guarantees
    // it points to a valid `DiskColumnFamilyMetrics` instance with the expected
    // layout for the duration of this call.
    let metrics_ref: &mut DiskColumnFamilyMetrics = unsafe { &mut *metrics };

    index_spec
        .inverted_index()
        .collect_metrics()
        .populate_metrics(metrics_ref);

    true
}
