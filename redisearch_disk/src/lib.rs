pub mod database;
pub mod document_id_key;
pub mod index_spec;
pub mod key_traits;
pub mod merge_op;
pub mod path_prefix;
pub mod vecsim_disk;

use crate::index_spec::IndexSpec;
use crate::index_spec::deleted_ids::DeletedIdsStore;
use crate::path_prefix::PathPrefix;
use crate::vecsim_disk::{SpeeDBHandles, VecSimDisk_CreateIndex};
use document::DocumentType;
use ffi::{
    AllocateKeyCallback, BasicDiskAPI, DocTableDiskAPI, IndexDiskAPI,
    IteratorType_INV_IDX_ITERATOR, QueryIterator, RSDocumentMetadata, RedisModuleCtx,
    RedisSearchDisk, RedisSearchDiskAPI, RedisSearchDiskIndexSpec, VecSimHNSWDiskParams,
    VectorDiskAPI, t_docId, t_fieldMask,
};
use rqe_iterators_interop::RQEIteratorWrapper;

use std::ffi::{CStr, OsStr, c_char, c_void};
use tracing::{debug, error, warn};

/// Registers the Redis module allocator as the global allocator for the application.
#[cfg(feature = "redis_allocator")]
#[global_allocator]
static REDIS_MODULE_ALLOCATOR: redis_module::alloc::RedisAlloc = redis_module::alloc::RedisAlloc;

const INVALID_DOC_ID: t_docId = 0;

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
            openIndexSpec: Some(index_spec_open),
            closeIndexSpec: Some(index_spec_close),
            close: Some(close),
        },
        index: IndexDiskAPI {
            indexDocument: Some(index_spec_index_doc),
            newTermIterator: Some(index_spec_new_term_iterator),
            newWildcardIterator: Some(index_spec_new_wildcard_iterator),
            markToBeDeleted: Some(index_spec_mark_to_be_deleted),
        },
        docTable: DocTableDiskAPI {
            putDocument: Some(index_spec_put_doc),
            isDocIdDeleted: Some(index_spec_is_doc_id_deleted),
            getDocumentMetadata: Some(index_spec_get_document_metadata),
        },
        vector: VectorDiskAPI {
            createVectorIndex: Some(vector_create_index),
            freeVectorIndex: Some(vector_free_index),
        },
    };

    &raw mut API
}

/// Opens the on-disk index db.
///
/// This stores the base path prefix for creating individual index databases.
///
/// # Safety
/// 1. `path` must be a null-terminated c-string. Additionally, it must contain
///    bytes which are a valid filesystem path for the target platform.
extern "C" fn open(_ctx: *mut RedisModuleCtx, db_path: *const c_char) -> *mut RedisSearchDisk {
    debug!("opening search disk");

    // Safety: see safety point 1 above.
    let db_path_cstr = unsafe { CStr::from_ptr(db_path) };
    // Safety: see safety point 1 above.
    let db_path_osstr = unsafe { OsStr::from_encoded_bytes_unchecked(db_path_cstr.to_bytes()) };

    // Pass the path directly to PathPrefix without converting to String.
    // This preserves the exact bytes of the path, which is important on Unix
    // systems where paths can contain arbitrary bytes that aren't valid UTF-8.
    PathPrefix::new(db_path_osstr).into_ptr()
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
    let _ = unsafe { PathPrefix::from_ptr(disk_ptr) };

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
    let Some(path_prefix) = (unsafe { PathPrefix::try_as_mut(disk) }) else {
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
    match IndexSpec::new(
        index_name.clone(),
        document_type,
        path_prefix.as_path(),
        deleted_ids,
    ) {
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

/// Inserts a new document.
///
/// # Safety
/// 1. `index` must have been returned from [`index_spec_open`].
/// 2. `key` must point to a valid buffer of at least `key_len` bytes.
extern "C" fn index_spec_put_doc(
    index: *mut RedisSearchDiskIndexSpec,
    key: *const c_char,
    key_len: usize,
    score: f32,
    flags: u32,
    max_term_freq: u32,
    doc_len: u32,
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

    index
        .doc_table()
        .insert_document(key, score, flags, max_term_freq, doc_len)
        .unwrap_or_else(|error| {
            error!(
                error = &error as &dyn std::error::Error,
                "failed to insert document"
            );
            INVALID_DOC_ID
        })
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
        Ok(iterator) => RQEIteratorWrapper::boxed_new(IteratorType_INV_IDX_ITERATOR, iterator),
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
        Ok(iterator) => RQEIteratorWrapper::boxed_new(IteratorType_INV_IDX_ITERATOR, iterator),
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
///
/// # Safety
/// 1. `index` must have been returned from [`index_spec_open`].
/// 2. `dmd` must be a valid pointer to a [`RSDocumentMetadata`].
extern "C" fn index_spec_get_document_metadata(
    index: *mut RedisSearchDiskIndexSpec,
    doc_id: t_docId,
    dmd: *mut RSDocumentMetadata,
    allocate_key: AllocateKeyCallback,
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
    dmd.set_flags(doc_table_dmd.flags);
    dmd.set_docLen(doc_table_dmd.doc_len);
    dmd.set_type(doc_table.document_type());

    true
}

/// FFI methods and associated functions for converting a [`PathPrefix`] to and
/// from the corresponding types on the C-side.
impl PathPrefix {
    /// Leaks `self` into an opaque pointer type.
    fn into_ptr(self) -> *mut RedisSearchDisk {
        let path_prefix = Box::new(self);

        Box::into_raw(path_prefix).cast::<RedisSearchDisk>()
    }

    /// Casts an opaque `RedisSearchDisk` to a mutable `PathPrefix` reference.
    /// Does not consume the pointer.
    ///
    /// # Safety
    /// 1. `ptr` must have been returned from [`Self::into_ptr`].
    /// 2. `ptr` must not be aliased.
    ///
    /// Returns `None` if `ptr` is null.
    unsafe fn try_as_mut<'a>(ptr: *mut RedisSearchDisk) -> Option<&'a mut Self> {
        let path_prefix: *mut Self = ptr.cast();
        // Safety: see safety point 1 above.
        unsafe { path_prefix.as_mut() }
    }

    /// Casts an opaque `RedisSearchDisk` to an owned `PathPrefix`.
    /// Consumes the provided pointer.
    ///
    /// # Safety
    /// 1. `ptr` must have been returned from [`Self::into_ptr`].
    unsafe fn from_ptr(ptr: *mut RedisSearchDisk) -> Self {
        let path_prefix = ptr.cast();
        // Safety: see safety point 1 above.
        let path_prefix = unsafe { Box::from_raw(path_prefix) };

        *path_prefix
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
    params: *const VecSimHNSWDiskParams,
) -> *mut c_void {
    // SAFETY: Caller (RediSearch C code) guarantees index is a valid pointer we created
    let Some(index_spec) = (unsafe { IndexSpec::try_as_mut(index) }) else {
        error!("vector_create_index: index is null");
        return std::ptr::null_mut();
    };

    // SAFETY: Caller guarantees params is a valid pointer to VecSimHNSWDiskParams
    let params_ref = match unsafe { params.as_ref() } {
        Some(p) => p,
        None => {
            error!("vector_create_index: params is null");
            return std::ptr::null_mut();
        }
    };

    // SAFETY: params_ref.indexName points to indexNameLen valid bytes (guaranteed by caller)
    let field_name = unsafe { c_str_to_str(params_ref.indexName, params_ref.indexNameLen) };
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

    // C++ copies these values, so stack allocation is fine
    let storage = SpeeDBHandles {
        db: db_ptr.cast(),
        cf: cf_ptr.cast(),
    };

    let mut params_with_storage = *params_ref;
    params_with_storage.storage = &storage as *const SpeeDBHandles as *mut c_void;

    // SAFETY: params_with_storage is a valid VecSimHNSWDiskParams with valid storage pointers
    let cpp_index = unsafe {
        VecSimDisk_CreateIndex(&params_with_storage as *const VecSimHNSWDiskParams as *const c_void)
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
