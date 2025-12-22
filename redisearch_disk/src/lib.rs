pub mod document_id_key;
pub mod index_spec;
pub mod merge_op;
pub mod search_disk;

use crate::index_spec::IndexSpec;
use crate::index_spec::deleted_ids::DeletedIdsStore;
use crate::search_disk::SearchDisk;
use ffi::{
    AllocateKeyCallback, BasicDiskAPI, DocTableDiskAPI, IndexDiskAPI,
    IteratorType_INV_IDX_ITERATOR, QueryIterator, RSDocumentMetadata, RedisModuleCtx,
    RedisSearchDisk, RedisSearchDiskAPI, RedisSearchDiskIndexSpec, t_docId, t_fieldMask,
};
use rqe_iterators_interop::RQEIteratorWrapper;
use std::ffi::{CStr, OsStr, c_char, c_void};
use tracing::{debug, error, warn};

const INVALID_DOC_ID: t_docId = 0;

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
        },
        docTable: DocTableDiskAPI {
            putDocument: Some(index_spec_put_doc),
            isDocIdDeleted: Some(index_spec_is_doc_id_deleted),
            getDocumentMetadata: Some(index_spec_get_document_metadata),
        },
    };

    &raw mut API
}

/// Opens the on-disk index db.
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
    SearchDisk::new(db_path_osstr)
        .map(SearchDisk::into_ptr)
        .unwrap_or_else(|error| {
            error!(
                error = &error as &dyn std::error::Error,
                "failed to create disk database"
            );
            std::ptr::null_mut()
        })
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
    let _ = unsafe { SearchDisk::from_ptr(disk_ptr) };

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
    document_type: ffi::DocumentType,
) -> *mut RedisSearchDiskIndexSpec {
    // Handle null pointer case (when open failed)
    // Safety: See safety point 2 above.
    let Some(search_disk) = (unsafe { SearchDisk::try_as_mut(disk) }) else {
        warn!("index_spec_open called with null disk pointer, returning null");
        return std::ptr::null_mut();
    };
    let deleted_ids = DeletedIdsStore::new();

    // Safety: See safety point 1 above.
    let index_name = unsafe {
        let slice = std::slice::from_raw_parts(index_name.cast::<u8>(), index_name_len);
        match std::str::from_utf8(slice) {
            Ok(s) => s.to_string(),
            Err(e) => {
                debug!("index_name is not valid UTF-8: {e}");
                return std::ptr::null_mut();
            }
        }
    };

    let (doc_table_cf_name, inverted_index_cf_name) = match search_disk
        .create_column_families_for_index(&index_name, document_type, deleted_ids.clone())
    {
        Ok(cf_families) => cf_families,
        Err(error) => {
            error!(
                index_name,
                error = &error as &dyn std::error::Error,
                "failed to create column families for index"
            );
            return std::ptr::null_mut();
        }
    };

    debug!(index_name, document_type, "opening index spec");
    IndexSpec::new(
        index_name,
        document_type.into(),
        search_disk.database(),
        doc_table_cf_name,
        inverted_index_cf_name,
        deleted_ids,
    )
    .into_ptr()
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
            Err(e) => {
                debug!("term is not valid UTF-8: {e}");
                return false;
            }
        }
    };
    // Safety: see safety point 1 above.
    let Some(index) = (unsafe { IndexSpec::try_as_mut(index) }) else {
        debug!("index pointer is null");
        return false;
    };

    debug!(term, doc_id, field_mask, "index_spec_index_doc");

    match index
        .inverted_index_mut()
        .insert(term, doc_id, field_mask, 0)
    {
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
    max_freq: u32,
) -> t_docId {
    // Safety: see safety point 2 above.
    let key = unsafe { std::slice::from_raw_parts(key.cast::<u8>(), key_len) };

    debug!(key, score, flags, max_freq, "index_spec_put_doc");

    // Safety: see safety point 1 above.
    let Some(index) = (unsafe { IndexSpec::try_as_mut(index) }) else {
        debug!("index pointer is null");
        return INVALID_DOC_ID;
    };

    index
        .doc_table_mut()
        .insert_document(key, score, flags, max_freq)
        .unwrap_or_else(|err| {
            debug!("failed to insert document: {err}");
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
            Err(e) => {
                debug!("term is not valid UTF-8: {e}");
                return std::ptr::null_mut();
            }
        }
    };

    debug!(term, field_mask, weight, "index_spec_new_term_iterator");

    // Safety: see safety point 1 above.
    let Some(index) = (unsafe { IndexSpec::try_as_mut(index) }) else {
        debug!("index pointer is null");
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
        debug!("index pointer is null");
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
        debug!("index pointer is null");
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
        debug!("allocate_key callback is None");
        return false;
    };
    // Safety: see safety point 1 above.
    let Some(index) = (unsafe { IndexSpec::try_as_mut(index) }) else {
        debug!("index pointer is null");
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
    dmd.set_maxFreq(doc_table_dmd.max_freq);
    dmd.set_flags(doc_table_dmd.flags);
    dmd.set_type(doc_table.document_type() as ffi::DocumentType);

    true
}

/// FFI methods and associated functions for converting a [`SearchDisk`] to and
/// from the corresponding types on the C-side.
impl SearchDisk {
    /// Leaks `self` into an opaque pointer type.
    fn into_ptr(self) -> *mut RedisSearchDisk {
        let search_disk = Box::new(self);

        Box::into_raw(search_disk).cast::<RedisSearchDisk>()
    }

    /// Casts an opaque `RedisSearchDisk` to a mutable `SearchDisk` reference.
    /// Does not consume the pointer.
    ///
    /// # Safety
    /// 1. `ptr` must have been returned from [`Self::into_ptr`].
    /// 2. `ptr` must not be aliased.
    ///
    /// Returns `None` if `ptr` is null.
    unsafe fn try_as_mut<'a>(ptr: *mut RedisSearchDisk) -> Option<&'a mut Self> {
        let search_disk: *mut Self = ptr.cast();
        // Safety: see safety point 1 above.
        unsafe { search_disk.as_mut() }
    }

    /// Casts an opaque `RedisSearchDisk` to an owned `SearchDisk`.
    /// Consumes the provided pointer.
    ///
    /// # Safety
    /// 1. `ptr` must have been returned from [`Self::into_ptr`].
    unsafe fn from_ptr(ptr: *mut RedisSearchDisk) -> Self {
        let search_disk = ptr.cast();
        // Safety: see safety point 1 above.
        let search_disk = unsafe { Box::from_raw(search_disk) };

        *search_disk
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
