/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use redis_module::RedisString;
use sorting_vector::RSSortingVector;
use value::{RSValueFFI, RSValueTrait};

use crate::{
    RLookup, RLookupRow,
    bindings::{DocumentType, RLookupLoadMode},
};

mod ccalls;
mod hash;

/// Populate the provided `dst_row` by loading a document (either a [Redis hash] or JSON object).
/// Either all keys are loaded or only the individual keys given in `options.keys`.
///
/// If the key given in `options.dmd.keyPtr` does not exist it will be created in the lookup table.
///
/// ## Arguments
///
/// If `options.mode` is `RLookupLoadMode::AllKeys` the hash or JSON object is loaded in its entirety
/// otherwise only the individual keys given in `options.keys` are loaded.
///
/// ## Code Paths and Status of Porting
///
/// There are four code paths for loading documents, below is further documentation per code path,
/// depending on the document type and the loading mode:
///
/// 1. Hash document, load all fields, Scan-Cursor-API: This code path is fully implemented in Rust using the `h_get_all` and `h_get_all_scan` function.
/// 2. Hash document, load all fields, fallback Call-API: This code path is fully implemented in Rust using the `h_get_all` and `h_get_all_fallback` function.
/// 3. JSON document, load all fields: This code path is not yet ported to Rust and still uses the C implementation via FFI, see MOD-11050.
/// 4. Individual keys (either hash or JSON document): This code path is not yet ported to Rust and still uses the C implementation via FFI, see MOD-11051.
///
/// ### Hash Document, Load All Fields, Scan-Cursor-API
///
/// Populate the provided `dst_row` by loading all field-value pairs from the [Redis hash] with the key given in `options.dmd.keyPtr`.
///
/// Internally either a scan cursor API or a fallback Call API is used, depending on the Redis version and deployment type. In the CRDT
/// deployment the scan cursor API is not available, so we always use the fallback Call API. The scan cursor API is available
/// from Redis v6.0.6 and above.
///
/// [Redis hash]: https://redis.io/docs/latest/develop/data-types/hashes/
///
/// ### Hash Document, Load All Fields, Fallback Call-API
///
/// Populate the provided `dst_row` by loading all field-value pairs from the [Redis hash] with the key given in `key_str`
///
/// HGETALL implementation using the scan cursor API that is available
/// from Redis v6.0.6 and above.
///
/// [Redis hash]: https://redis.io/docs/latest/develop/data-types/hashes/
///
/// ### JSON Document, Load All Fields
///
/// Calls in the C code to load all fields from a JSON document.
///
/// ### Individual Keys (Either Hash or JSON Document)
///
/// Calls in the C code to load individual keys from either a hash or JSON document.
///
/// ## References
///
/// [Redis hash]: https://redis.io/docs/latest/develop/data-types/hashes/
pub fn load_document<'a>(
    lookup: &mut RLookup<'a>,
    dst_row: &mut RLookupRow<'a, RSValueFFI>,
    options: &LoadDocumentOptions<'a>,
) -> Result<(), LoadDocumentError> {
    let pimpl = LoadDocumentContextImpl;
    load_document_int(lookup, dst_row, options, &pimpl)
}

// the following function is only used internally to allow for mocking out the context in tests
#[inline(always)]
fn load_document_int<'a, C, V>(
    lookup: &mut RLookup<'a>,
    dst_row: &mut RLookupRow<'a, V>,
    options: &LoadDocumentOptions<'a, V>,
    context: &C,
) -> Result<(), LoadDocumentError>
where
    C: LoadDocumentContext<V>,
    V: RSValueTrait,
{
    dst_row.set_sorting_vector(options.sorting_vector);

    match options.mode {
        RLookupLoadMode::AllKeys => {
            if options.document_type == DocumentType::Hash {
                let key_str = context.hval_provide_keystr(options)?;

                if context.has_scan_key_feature() && !context.is_crdt() {
                    context.hval_get_all_scan(lookup, dst_row, options, key_str)?;
                } else {
                    context.hval_get_all_fallback(lookup, dst_row, options, key_str)?;
                }
            } else {
                context.json_get_all(lookup, dst_row, options)?;
            }
        }
        RLookupLoadMode::KeyList | RLookupLoadMode::SortingVectorKeys => {
            context.load_individual_keys(lookup, dst_row, options)?;
        }
        _ => {
            return Err(LoadDocumentError::invalid_arguments(Some(
                "Invalid load mode".to_string(),
            )));
        }
    };

    Ok(())
}

/// A verified version of [RLookupLoadOptions] that also provides an easier access to the variables.
pub struct LoadDocumentOptions<'a, T: RSValueTrait = RSValueFFI> {
    context: NonNull<redis_module::raw::RedisModuleCtx>,
    sorting_vector: &'a RSSortingVector<T>,
    document_type: DocumentType,
    key_ptr: Option<NonNull<std::ffi::c_char>>,
    mode: RLookupLoadMode,

    #[expect(unused, reason = "Used in follow-up PRs")]
    force_load: bool,
    force_string: bool,

    tmp_cstruct: Option<NonNull<ffi::RLookupLoadOptions>>,
}

pub struct LoadDocumentOptionsBuilder<'a, T: RSValueTrait = RSValueFFI> {
    context: Option<NonNull<redis_module::raw::RedisModuleCtx>>,
    sorting_vector: &'a RSSortingVector<T>,
    document_type: DocumentType,
    key_ptr: Option<NonNull<std::ffi::c_char>>,
    mode: RLookupLoadMode,
    force_load: bool,
    force_string: bool,

    tmp_cstruct: Option<NonNull<ffi::RLookupLoadOptions>>,
}

impl<'a, T: RSValueTrait> LoadDocumentOptionsBuilder<'a, T> {
    #[cfg_attr(not(test), expect(unused, reason = "Used in follow-up PRs"))]
    pub fn new(
        ctx: *mut redis_module::raw::RedisModuleCtx,
        sv: &'a RSSortingVector<T>,
        doc_type: DocumentType,
    ) -> Self {
        Self {
            context: NonNull::new(ctx),
            sorting_vector: sv,
            document_type: doc_type,
            key_ptr: None,
            mode: RLookupLoadMode::AllKeys,
            force_load: false,
            force_string: false,
            tmp_cstruct: None,
        }
    }

    #[expect(unused, reason = "Used in follow-up PRs")]
    pub fn with_force_load(mut self) -> Self {
        self.force_load = true;
        self
    }

    #[expect(unused, reason = "Used in follow-up PRs")]
    pub fn with_force_string(mut self) -> Self {
        self.force_string = true;
        self
    }

    #[cfg_attr(not(test), expect(unused, reason = "Used in follow-up PRs"))]
    pub fn set_mode(mut self, mode: RLookupLoadMode) -> Self {
        self.mode = mode;
        self
    }

    #[cfg_attr(not(test), expect(unused, reason = "Used in follow-up PRs"))]
    pub fn with_key_ptr(mut self, key_ptr: *const std::ffi::c_char) -> Self {
        self.key_ptr = NonNull::new(key_ptr as *mut std::ffi::c_char);
        self
    }

    #[expect(unused, reason = "Used in follow-up PRs")]
    pub fn override_tmp_cstruct(mut self, cstruct: NonNull<ffi::RLookupLoadOptions>) -> Self {
        self.tmp_cstruct = Some(cstruct);
        self
    }

    #[cfg_attr(not(test), expect(unused, reason = "Used in follow-up PRs"))]
    pub fn build(self) -> Result<LoadDocumentOptions<'a, T>, LoadDocumentError> {
        if self.context.is_none() {
            return Err(LoadDocumentError::invalid_arguments(Some(
                "Context is null".to_string(),
            )));
        }

        if self.mode != RLookupLoadMode::AllKeys && self.key_ptr.is_none() {
            return Err(LoadDocumentError::invalid_arguments(Some(
                "Key pointer is null".to_string(),
            )));
        }

        Ok(LoadDocumentOptions {
            context: self.context.unwrap(),
            sorting_vector: self.sorting_vector,
            document_type: self.document_type,
            key_ptr: self.key_ptr,
            mode: self.mode,
            force_load: self.force_load,
            force_string: self.force_string,
            // Temporary C struct for time when part is C and part is Rust:
            tmp_cstruct: self.tmp_cstruct,
        })
    }
}

#[derive(Debug)]
pub enum LoadDocumentError {
    /// Key does not exist
    KeyDoesNotExist {
        #[cfg(debug_assertions)]
        key: Option<String>,
    },

    /// Key is not an hash document
    KeyIsNoHash {
        #[cfg(debug_assertions)]
        key: Option<String>,
    },

    /// Invalid call arguments were provided
    InvalidArguments {
        #[cfg(debug_assertions)]
        details: Option<String>,
    },

    /// Neither the scan API nor the fallback Call API is available
    FallbackAPINotAvailable,

    /// A temporary error that occurred in the C code, e.g. C was called and returned [`ffi::REDISMODULE_ERR`].
    FromCCode,
}

impl std::fmt::Display for LoadDocumentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::KeyDoesNotExist {
                #[cfg(debug_assertions)]
                key,
            } => {
                #[cfg(debug_assertions)]
                if let Some(key) = key {
                    write!(f, "Key does not exist: {}", key)
                } else {
                    write!(f, "Key does not exist")
                }
                #[cfg(not(debug_assertions))]
                write!(f, "Key does not exist")
            }
            Self::KeyIsNoHash {
                #[cfg(debug_assertions)]
                key,
            } => {
                #[cfg(debug_assertions)]
                if let Some(key) = key {
                    write!(f, "Key is not a hash document: {}", key)
                } else {
                    write!(f, "Key is not a hash document")
                }
                #[cfg(not(debug_assertions))]
                write!(f, "Key is not a hash document")
            }
            Self::InvalidArguments {
                #[cfg(debug_assertions)]
                details,
            } => {
                #[cfg(debug_assertions)]
                if let Some(details) = details {
                    write!(f, "Invalid arguments: {}", details)
                } else {
                    write!(f, "Invalid arguments")
                }
                #[cfg(not(debug_assertions))]
                write!(f, "Invalid arguments")
            }
            Self::FallbackAPINotAvailable => write!(
                f,
                "Neither the `Scan` nor the `Call` API is available for hash based documents"
            ),
            Self::FromCCode => write!(f, "An error occurred in the c code"),
        }
    }
}

impl LoadDocumentError {
    pub fn key_does_not_exist(key: Option<String>) -> Self {
        Self::KeyDoesNotExist {
            #[cfg(debug_assertions)]
            key,
        }
    }

    pub fn key_is_no_hash(key: Option<String>) -> Self {
        Self::KeyIsNoHash {
            #[cfg(debug_assertions)]
            key,
        }
    }

    pub fn invalid_arguments(details: Option<String>) -> Self {
        Self::InvalidArguments {
            #[cfg(debug_assertions)]
            details,
        }
    }
}

/// A trait that abstracts over the context in which the document loading happens, e.g. production code or tests.
///
/// The trait is generic over the value type `V` that is used to represent the values in the loaded document. For
/// tests this has to be [RSValueMock], for production code this is typically [RSValueFFI].
///
///
trait LoadDocumentContext<V: RSValueTrait> {
    /// Provide a [RedisString] for the key given in `options.key_ptr`.
    fn hval_provide_keystr(
        &self,
        options: &LoadDocumentOptions<'_, V>,
    ) -> Result<RedisString, LoadDocumentError>;

    /// Load all field-value pairs from a hash document using the scan cursor API.
    fn hval_get_all_scan<'a>(
        &self,
        lookup: &mut RLookup<'a>,
        dst_row: &mut RLookupRow<'a, V>,
        options: &LoadDocumentOptions<'a, V>,
        key_str: RedisString,
    ) -> Result<(), LoadDocumentError>;

    /// Load all field-value pairs from a hash document using the fallback Call API.
    fn hval_get_all_fallback<'a>(
        &self,
        lookup: &mut RLookup<'a>,
        dst_row: &mut RLookupRow<'a, V>,
        options: &LoadDocumentOptions<'a, V>,
        key_str: RedisString,
    ) -> Result<(), LoadDocumentError>;

    /// Load all field-value pairs from a JSON document.
    fn json_get_all<'a>(
        &self,
        lookup: &mut RLookup<'a>,
        dst_row: &mut RLookupRow<'a, V>,
        options: &LoadDocumentOptions<'a, V>,
    ) -> Result<(), LoadDocumentError>;

    /// Load individual keys from either a hash or JSON document.
    fn load_individual_keys<'a>(
        &self,
        lookup: &mut RLookup<'a>,
        dst_row: &mut RLookupRow<'a, V>,
        options: &LoadDocumentOptions<'a, V>,
    ) -> Result<(), LoadDocumentError>;

    /// Returns true if the current context is a CRDT deployment.
    fn is_crdt(&self) -> bool;

    /// Returns true if the current context provides the scan key feature of a Redis deployment.
    fn has_scan_key_feature(&self) -> bool;
}

struct LoadDocumentContextImpl;
impl LoadDocumentContext<RSValueFFI> for LoadDocumentContextImpl {
    fn hval_provide_keystr(
        &self,
        options: &LoadDocumentOptions<'_>,
    ) -> Result<RedisString, LoadDocumentError> {
        hash::provide_keystr(options)
    }

    fn hval_get_all_scan(
        &self,
        lookup: &mut RLookup,
        dst_row: &mut RLookupRow<'_, RSValueFFI>,
        options: &LoadDocumentOptions,
        key_str: RedisString,
    ) -> Result<(), LoadDocumentError> {
        hash::get_all_scan(lookup, dst_row, options, key_str)
    }

    fn hval_get_all_fallback(
        &self,
        lookup: &mut RLookup,
        dst_row: &mut RLookupRow<'_, RSValueFFI>,
        options: &LoadDocumentOptions,
        key_str: RedisString,
    ) -> Result<(), LoadDocumentError> {
        hash::get_all_fallback(lookup, dst_row, options, key_str)
    }

    fn json_get_all(
        &self,
        lookup: &mut RLookup,
        dst_row: &mut RLookupRow<'_, RSValueFFI>,
        options: &LoadDocumentOptions,
    ) -> Result<(), LoadDocumentError> {
        ccalls::json_get_all(lookup, dst_row, options)
    }

    fn load_individual_keys(
        &self,
        lookup: &mut RLookup,
        dst_row: &mut RLookupRow<'_, RSValueFFI>,
        options: &LoadDocumentOptions,
    ) -> Result<(), LoadDocumentError> {
        ccalls::load_individual_keys(lookup, dst_row, options)
    }

    fn is_crdt(&self) -> bool {
        hash::is_crdt()
    }

    fn has_scan_key_feature(&self) -> bool {
        hash::has_scan_key_feature()
    }
}

#[cfg(test)]
mod tests {
    use mockall::{automock, predicate::*};
    use value::RSValueMock;

    use super::*;

    /// A mock implementation of the [LoadDocumentContext] trait for testing purposes.
    #[expect(unused, reason = "Used for automock generation")]
    struct LoadDocCtx;
    #[automock]
    impl LoadDocumentContext<RSValueMock> for LoadDocCtx {
        fn hval_provide_keystr<'a>(
            &self,
            _options: &LoadDocumentOptions<'a, RSValueMock>,
        ) -> Result<RedisString, LoadDocumentError> {
            unimplemented!()
        }

        fn hval_get_all_scan<'a>(
            &self,
            _lookup: &mut RLookup<'a>,
            _dst_row: &mut RLookupRow<'a, RSValueMock>,
            _options: &LoadDocumentOptions<'a, RSValueMock>,
            _key_str: RedisString,
        ) -> Result<(), LoadDocumentError> {
            unimplemented!()
        }

        fn hval_get_all_fallback<'a>(
            &self,
            _lookup: &mut RLookup<'a>,
            _dst_row: &mut RLookupRow<'a, RSValueMock>,
            _options: &LoadDocumentOptions<'a, RSValueMock>,
            _key_str: RedisString,
        ) -> Result<(), LoadDocumentError> {
            unimplemented!()
        }

        fn json_get_all<'a>(
            &self,
            _lookup: &mut RLookup<'a>,
            _dst_row: &mut RLookupRow<'a, RSValueMock>,
            _options: &LoadDocumentOptions<'a, RSValueMock>,
        ) -> Result<(), LoadDocumentError> {
            unimplemented!()
        }

        fn load_individual_keys<'a>(
            &self,
            _lookup: &mut RLookup<'a>,
            _dst_row: &mut RLookupRow<'a, RSValueMock>,
            _options: &LoadDocumentOptions<'a, RSValueMock>,
        ) -> Result<(), LoadDocumentError> {
            unimplemented!()
        }

        fn is_crdt(&self) -> bool {
            unimplemented!()
        }

        fn has_scan_key_feature(&self) -> bool {
            unimplemented!()
        }
    }

    #[test]
    fn load_document_hash_scan_api() -> Result<(), LoadDocumentError> {
        let sv = RSSortingVector::new(0);

        // use a mock pointer for the context, we won't dereference it
        let rm_ctx_mock = 0x1234 as *mut redis_module::raw::RedisModuleCtx;
        type TOpt<'a> = LoadDocumentOptions<'a, RSValueMock>;

        let opt: TOpt =
            LoadDocumentOptionsBuilder::new(rm_ctx_mock, &sv, DocumentType::Hash).build()?;

        let mut seq = mockall::Sequence::new();
        let mut mock = MockLoadDocCtx::new();

        // no crdt and has scan feature --> chooses scan api code path
        mock.expect_is_crdt().return_once(|| false);
        mock.expect_has_scan_key_feature().return_once(|| true);

        // 1. expect to get a redis string for the key, we provide a null pointer here
        // 2. expect to call the scan api
        mock.expect_hval_provide_keystr()
            .once()
            .return_once(|_| {
                Ok(RedisString::from_redis_module_string(
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                ))
            })
            .in_sequence(&mut seq);
        mock.expect_hval_get_all_scan()
            .once()
            .return_once(|_, _, _, _| Ok(()))
            .in_sequence(&mut seq);

        let res = load_document_int(&mut RLookup::new(), &mut RLookupRow::new(), &opt, &mock);
        assert!(res.is_ok());

        Ok(())
    }

    #[test]
    fn load_document_hash_fallback_api() -> Result<(), LoadDocumentError> {
        let sv = RSSortingVector::new(0);
        let rm_ctx_mock = 0x1234 as *mut redis_module::raw::RedisModuleCtx;
        type TOpt<'a> = LoadDocumentOptions<'a, RSValueMock>;

        let opt: TOpt =
            LoadDocumentOptionsBuilder::new(rm_ctx_mock, &sv, DocumentType::Hash).build()?;

        let mut seq = mockall::Sequence::new();
        let mut mock = MockLoadDocCtx::new();

        mock.expect_is_crdt().return_once(|| true);
        mock.expect_has_scan_key_feature().return_once(|| true);

        mock.expect_hval_provide_keystr()
            .once()
            .return_once(|_| {
                Ok(RedisString::from_redis_module_string(
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                ))
            })
            .in_sequence(&mut seq);
        mock.expect_hval_get_all_fallback()
            .once()
            .return_once(|_, _, _, _| Ok(()))
            .in_sequence(&mut seq);

        let res = load_document_int(&mut RLookup::new(), &mut RLookupRow::new(), &opt, &mock);
        assert!(res.is_ok());

        Ok(())
    }

    #[test]
    fn load_document_hash_fallback_api_no_scan_feature() -> Result<(), LoadDocumentError> {
        let sv = RSSortingVector::new(0);
        let rm_ctx_mock = 0x1234 as *mut redis_module::raw::RedisModuleCtx;
        type TOpt<'a> = LoadDocumentOptions<'a, RSValueMock>;

        let opt: TOpt =
            LoadDocumentOptionsBuilder::new(rm_ctx_mock, &sv, DocumentType::Hash).build()?;

        let mut seq = mockall::Sequence::new();
        let mut mock = MockLoadDocCtx::new();

        mock.expect_is_crdt().return_once(|| false);
        mock.expect_has_scan_key_feature().return_once(|| false);

        mock.expect_hval_provide_keystr()
            .once()
            .return_once(|_| {
                Ok(RedisString::from_redis_module_string(
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                ))
            })
            .in_sequence(&mut seq);
        mock.expect_hval_get_all_fallback()
            .once()
            .return_once(|_, _, _, _| Ok(()))
            .in_sequence(&mut seq);

        let res = load_document_int(&mut RLookup::new(), &mut RLookupRow::new(), &opt, &mock);
        assert!(res.is_ok());

        Ok(())
    }

    #[test]
    fn load_document_json_all_keys() -> Result<(), LoadDocumentError> {
        let sv = RSSortingVector::new(0);
        let rm_ctx_mock = 0x1234 as *mut redis_module::raw::RedisModuleCtx;
        type TOpt<'a> = LoadDocumentOptions<'a, RSValueMock>;

        let opt: TOpt =
            LoadDocumentOptionsBuilder::new(rm_ctx_mock, &sv, DocumentType::Json).build()?;

        let mut mock = MockLoadDocCtx::new();

        mock.expect_json_get_all()
            .once()
            .return_once(|_, _, _| Ok(()));

        let res = load_document_int(&mut RLookup::new(), &mut RLookupRow::new(), &opt, &mock);
        assert!(res.is_ok());

        Ok(())
    }

    #[test]
    fn load_document_individual_keys_keylist() -> Result<(), LoadDocumentError> {
        let sv = RSSortingVector::new(0);
        let rm_ctx_mock = 0x1234 as *mut redis_module::raw::RedisModuleCtx;
        type TOpt<'a> = LoadDocumentOptions<'a, RSValueMock>;

        let key_ptr = 0x5678 as *const std::ffi::c_char;
        let opt: TOpt = LoadDocumentOptionsBuilder::new(rm_ctx_mock, &sv, DocumentType::Hash)
            .set_mode(RLookupLoadMode::KeyList)
            .with_key_ptr(key_ptr)
            .build()?;

        let mut mock = MockLoadDocCtx::new();

        mock.expect_load_individual_keys()
            .once()
            .return_once(|_, _, _| Ok(()));

        let res = load_document_int(&mut RLookup::new(), &mut RLookupRow::new(), &opt, &mock);
        assert!(res.is_ok());

        Ok(())
    }

    #[test]
    fn load_document_individual_keys_sorting_vector() -> Result<(), LoadDocumentError> {
        let sv = RSSortingVector::new(0);
        let rm_ctx_mock = 0x1234 as *mut redis_module::raw::RedisModuleCtx;
        type TOpt<'a> = LoadDocumentOptions<'a, RSValueMock>;

        let key_ptr = 0x5678 as *const std::ffi::c_char;
        let opt: TOpt = LoadDocumentOptionsBuilder::new(rm_ctx_mock, &sv, DocumentType::Hash)
            .set_mode(RLookupLoadMode::SortingVectorKeys)
            .with_key_ptr(key_ptr)
            .build()?;

        let mut mock = MockLoadDocCtx::new();

        mock.expect_load_individual_keys()
            .once()
            .return_once(|_, _, _| Ok(()));

        let res = load_document_int(&mut RLookup::new(), &mut RLookupRow::new(), &opt, &mock);
        assert!(res.is_ok());

        Ok(())
    }

    #[test]
    fn load_document_error_invalid_mode() -> Result<(), LoadDocumentError> {
        let sv = RSSortingVector::new(0);
        let rm_ctx_mock = 0x1234 as *mut redis_module::raw::RedisModuleCtx;
        type TOpt<'a> = LoadDocumentOptions<'a, RSValueMock>;

        // Create options with an unsupported mode (we'll use a custom invalid mode)
        let key_ptr = 0x5678 as *const std::ffi::c_char;
        let invalid_opt: TOpt =
            LoadDocumentOptionsBuilder::new(rm_ctx_mock, &sv, DocumentType::Hash)
                .with_key_ptr(key_ptr)
                .set_mode(unsafe { std::mem::transmute(99u32) })
                .build()?;

        let mock = MockLoadDocCtx::new();

        let res = load_document_int(
            &mut RLookup::new(),
            &mut RLookupRow::new(),
            &invalid_opt,
            &mock,
        );
        assert!(res.is_err());
        if let Err(LoadDocumentError::InvalidArguments { .. }) = res {
            // Expected error
        } else {
            panic!("Expected InvalidArguments error");
        }

        Ok(())
    }
}
