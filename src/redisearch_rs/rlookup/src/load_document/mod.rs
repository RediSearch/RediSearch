/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

mod hash;

use std::ptr::NonNull;

use redis_module::RedisString;
use sorting_vector::RSSortingVector;
use value::{RSValueFFI, RSValueTrait};

use crate::{
    RLookup, RLookupRow,
    bindings::{DocumentType, RLookupCoerceType, RLookupLoadMode},
};

/// Populate the provided `dst_row` by loading a either a redis hash or json key (either a [Redis hash] or [JSON Document]).
///
/// Either all keys are loaded or only the individual keys given in `options.keys`.
/// If the key given in `options.dmd.keyPtr` does not exist it will be created in the lookup table.
///
/// Further information about redis data-types:
///
/// 1. [Redis hash](https://redis.io/docs/latest/develop/data-types/hashes/)
/// 2. [JSON Document](https://redis.io/docs/latest/develop/data-types/json/)
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
/// 1. Hash document, load all fields, Scan-Cursor-API: This code path is fully implemented in Rust, see `[hash::get_all_scan`]
/// 2. Hash document, load all fields, fallback Call-API: This code path is fully implemented in Rust, see `[hash::get_all_fallback`].
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
    let context = hash::LoadDocumentImpl;
    load_document_int(lookup, dst_row, options, &context)
}

// the following function is only used internally to allow for mocking out the context in tests
#[inline(always)]
fn load_document_int<'a, C>(
    lookup: &mut RLookup<'a>,
    dst_row: &mut RLookupRow<'a, C::V>,
    options: &LoadDocumentOptions<'a, C::V>,
    context: &C,
) -> Result<(), LoadDocumentError>
where
    C: LoadDocumentContext,
{
    dst_row.set_sorting_vector(options.sorting_vector);

    match options.mode {
        RLookupLoadMode::AllKeys => {
            match options.document_type {
                DocumentType::Hash => {
                    let key_ptr =
                        options
                            .key_ptr
                            .ok_or(LoadDocumentError::invalid_arguments(Some(
                                "Key pointer is null".to_string(),
                            )))?;

                    // Safety: We assume the caller provided options with a key pointer containing a sds string.
                    let sds_len = unsafe { ffi::sdslen__(key_ptr.as_ptr()) };

                    // Safety: The sds string is prefixed with its length, key_ptr directly points to the string data.
                    let key_str =
                        unsafe { RedisString::from_raw_parts(None, key_ptr.as_ptr(), sds_len) };

                    if context.has_scan_key_feature() && !context.is_crdt() {
                        hash::get_all_scan(lookup, dst_row, options, key_str, context)?;
                    } else {
                        hash::get_all_fallback(lookup, dst_row, options, key_str, context)?;
                    }
                }
                DocumentType::Json => {
                    context.load_json(lookup, dst_row, options)?;
                }
                DocumentType::Unsupported => {
                    return Err(LoadDocumentError::invalid_arguments(Some(
                        "Unsupported document type".to_string(),
                    )));
                }
            }
        }
        RLookupLoadMode::KeyList | RLookupLoadMode::SortingVectorKeys => {
            context.load_individual_keys(lookup, dst_row, options)?;
        }
    };

    Ok(())
}

/// A context object that allows test mocking
pub trait LoadDocumentContext {
    /// The RSValue type used in this context
    type V: RSValueTrait;

    /// Returns true if the current context is for a CRDT deployment
    fn is_crdt(&self) -> bool;

    /// Returns true if the current context has the ScanKeyCursor feature available
    fn has_scan_key_feature(&self) -> bool;

    /// Generates an RSValue of type [Self::V] from the provided source, which is either a hval or a Call API reply element.
    fn generate_value(&self, src: ValueSrc, ct: RLookupCoerceType) -> Self::V;

    /// Load all fields from a JSON document into the provided `dst_row`
    /// implemented here as bind this function from C and don't want to do so in tests.
    fn load_json(
        &self,
        lookup: &mut RLookup<'_>,
        dst_row: &mut RLookupRow<'_, Self::V>,
        options: &LoadDocumentOptions<'_, Self::V>,
    ) -> Result<(), LoadDocumentError>;

    /// Load all fields from a JSON document into the provided `dst_row`
    /// implemented here as bind this function from C and don't want to do so in tests.
    fn load_individual_keys(
        &self,
        lookup: &mut RLookup<'_>,
        dst_row: &mut RLookupRow<'_, Self::V>,
        options: &LoadDocumentOptions<'_, Self::V>,
    ) -> Result<(), LoadDocumentError>;
}

pub enum ValueSrc<'a> {
    /// From CALL API (HGETALL reply element)
    ReplyElem(*mut redis_module::RedisModuleCallReply),
    /// From ScanKeyCursor (hval / RedisModuleString)
    HVal(&'a RedisString),
    // Add JSON variants later if needed, e.g.:
    // JsonValue(*mut c_void),
}

/// A verified version of [ffi::RLookupLoadOptions] that provides easy access to required variables.
///
/// Type parameter T is the RSValue type used for the [RSSortingVector] underlying the option, defaulting to [RSValueFFI].
pub struct LoadDocumentOptions<'a, T: RSValueTrait = RSValueFFI> {
    context: Option<NonNull<redis_module::raw::RedisModuleCtx>>,
    sorting_vector: &'a RSSortingVector<T>,
    document_type: DocumentType,
    key_ptr: Option<NonNull<std::ffi::c_char>>,

    mode: RLookupLoadMode,

    #[expect(unused, reason = "Used in follow-up PRs")]
    force_load: bool,
    force_string: bool,

    /// Temporary C struct provided by C and used when called back in C from Rust
    #[expect(unused, reason = "Used in follow-up PRs")]
    tmp_cstruct: Option<NonNull<ffi::RLookupLoadOptions>>,
}

/// A builder that guarantees that only valid versions of [LoadDocumentOptions] are created.
///
/// Will be called with raw types from C code, so we need to validate the inputs before creating the options.
pub struct LoadDocumentOptionsBuilder<'a, T: RSValueTrait = RSValueFFI> {
    context: Option<NonNull<redis_module::raw::RedisModuleCtx>>,
    sorting_vector: &'a RSSortingVector<T>,
    document_type: DocumentType,
    key_ptr: Option<NonNull<std::ffi::c_char>>,
    mode: u32,
    force_load: bool,
    force_string: bool,

    tmp_cstruct: Option<NonNull<ffi::RLookupLoadOptions>>,
}

impl<'a, T: RSValueTrait> LoadDocumentOptionsBuilder<'a, T> {
    #[cfg_attr(not(test), expect(unused, reason = "Used in follow-up PRs"))]
    pub const fn new(
        ctx: *mut redis_module::raw::RedisModuleCtx,
        sv: &'a RSSortingVector<T>,
        doc_type: DocumentType,
    ) -> Self {
        let ctx = if ctx.is_null() {
            None
        } else {
            NonNull::new(ctx)
        };
        Self {
            context: ctx,
            sorting_vector: sv,
            document_type: doc_type,
            key_ptr: None,
            mode: RLookupLoadMode::AllKeys as u32,
            force_load: false,
            force_string: false,
            tmp_cstruct: None,
        }
    }

    #[expect(unused, reason = "Used in follow-up PRs")]
    pub const fn with_force_load(mut self) -> Self {
        self.force_load = true;
        self
    }

    #[expect(unused, reason = "Used in follow-up PRs")]
    pub const fn with_force_string(mut self) -> Self {
        self.force_string = true;
        self
    }

    #[cfg_attr(not(test), expect(unused, reason = "Used in follow-up PRs"))]
    pub const fn set_mode(mut self, mode: u32) -> Self {
        self.mode = mode;
        self
    }

    #[cfg_attr(not(test), expect(unused, reason = "Used in follow-up PRs"))]
    pub const fn with_key_ptr(mut self, key_ptr: *const std::ffi::c_char) -> Self {
        self.key_ptr = NonNull::new(key_ptr as *mut std::ffi::c_char);
        self
    }

    #[expect(unused, reason = "Used in follow-up PRs")]
    pub const fn override_tmp_cstruct(mut self, cstruct: NonNull<ffi::RLookupLoadOptions>) -> Self {
        self.tmp_cstruct = Some(cstruct);
        self
    }

    #[cfg_attr(not(test), expect(unused, reason = "Used in follow-up PRs"))]
    pub fn build(self) -> Result<LoadDocumentOptions<'a, T>, LoadDocumentError> {
        // key ptr is provided from different sources depending on the document type and mode
        // here we ensure it is set in the options
        if self.key_ptr.is_none() {
            return Err(LoadDocumentError::invalid_arguments(Some(
                "Key pointer is null".to_string(),
            )));
        }

        let Some(mode) = RLookupLoadMode::from_repr(self.mode) else {
            return Err(LoadDocumentError::invalid_arguments(Some(
                "Invalid load mode".to_string(),
            )));
        };

        Ok(LoadDocumentOptions {
            context: self.context,
            sorting_vector: self.sorting_vector,
            document_type: self.document_type,
            key_ptr: self.key_ptr,
            mode,
            force_load: self.force_load,
            force_string: self.force_string,

            // Temporary C struct provided by C and used when called back in C from Rust
            tmp_cstruct: self.tmp_cstruct,
        })
    }
}

/// Sub-errors for invalid key errors.
///
/// The Scan and the Call API provide error information
/// in different level of detail for why a key is invalid.
///
/// Therefore we have specific sub-errors here and one general sub error.
#[derive(Debug, Clone, PartialEq, Eq, strum::Display)]
pub enum KeyError {
    DoesNotExist,
    IsNoHash,
    IsNoJson,
    DoesNotExistOrIsWrongType,
}

/// Errors that can occur when loading a document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LoadDocumentError {
    /// Invalid Key has been used
    InvalidKey {
        sub_error: KeyError,
        #[cfg(debug_assertions)]
        key: Option<String>,
    },

    /// Invalid call arguments were provided
    InvalidArguments {
        #[cfg(debug_assertions)]
        details: Option<String>,
    },

    /// Neither the scan API nor the fallback Call API is available
    CallAPIError,

    /// A temporary error that occurred in the C code, e.g. C was called and returned [`ffi::REDISMODULE_ERR`].
    FromCCode,
}

impl std::fmt::Display for LoadDocumentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidKey {
                sub_error,
                #[cfg(debug_assertions)]
                key,
            } => {
                #[cfg(debug_assertions)]
                if let Some(key) = key {
                    write!(f, "Invalid key ({}): {}", sub_error, key)
                } else {
                    write!(f, "Invalid key: {}", sub_error)
                }
                #[cfg(not(debug_assertions))]
                write!(f, "Invalid key: {}", sub_error)
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
            Self::CallAPIError => write!(
                f,
                "Neither the `Scan` nor the `Call` API is available for hash based documents"
            ),
            Self::FromCCode => write!(f, "An error occurred in the c code"),
        }
    }
}

impl LoadDocumentError {
    #[cfg(debug_assertions)]
    pub const fn key_does_not_exist(key: Option<String>) -> Self {
        Self::InvalidKey {
            sub_error: KeyError::DoesNotExist,
            #[cfg(debug_assertions)]
            key,
        }
    }

    #[cfg(not(debug_assertions))]
    pub fn key_does_not_exist(_key: Option<String>) -> Self {
        Self::InvalidKey {
            sub_error: KeyError::DoesNotExist,
        }
    }

    #[cfg(debug_assertions)]
    pub const fn key_is_no_hash(key: Option<String>) -> Self {
        Self::InvalidKey {
            sub_error: KeyError::IsNoHash,
            #[cfg(debug_assertions)]
            key,
        }
    }

    #[cfg(not(debug_assertions))]
    pub fn key_is_no_hash(_key: Option<String>) -> Self {
        Self::InvalidKey {
            sub_error: KeyError::IsNoHash,
        }
    }

    #[cfg(debug_assertions)]
    pub const fn key_is_no_json(key: Option<String>) -> Self {
        Self::InvalidKey {
            sub_error: KeyError::IsNoJson,
            #[cfg(debug_assertions)]
            key,
        }
    }

    #[cfg(not(debug_assertions))]
    pub fn key_is_no_json(_key: Option<String>) -> Self {
        Self::InvalidKey {
            sub_error: KeyError::IsNoJson,
        }
    }

    #[cfg(debug_assertions)]
    pub const fn key_is_somehow_invalid(key: Option<String>) -> Self {
        Self::InvalidKey {
            sub_error: KeyError::DoesNotExistOrIsWrongType,
            key,
        }
    }

    #[cfg(not(debug_assertions))]
    pub fn key_is_somehow_invalid(_key: Option<String>) -> Self {
        Self::InvalidKey {
            sub_error: KeyError::DoesNotExistOrIsWrongType,
        }
    }

    #[cfg(debug_assertions)]
    pub const fn invalid_arguments(details: Option<String>) -> Self {
        Self::InvalidArguments { details }
    }

    #[cfg(not(debug_assertions))]
    pub fn invalid_arguments(_details: Option<String>) -> Self {
        Self::InvalidArguments {}
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    #[cfg(not(miri))]
    use redis_mock::{TestContext, TestContextBuilder};
    use value::RSValueMock;

    /// A test context implementation for load document tests, allowing to mock out various features.
    ///
    /// It mocks out variables, like whether the context is for a CRDT deployment or whether the ScanKeyCursor is available.
    ///
    /// Additionally the the tests depend on mocked versions of the RedisModule C functions, see mock.rs files.
    pub struct LoadDocumentTestContext {
        /// indicates whether the context is for a CRDT deployment
        is_crdt: bool,

        /// indicates whether the ScanKeyCursor feature is available
        has_scan_key_feature: bool,

        /// This is the underlying Redis module mock context
        #[cfg(not(miri))]
        redis_ctx: Option<TestContext>,
    }

    impl LoadDocumentTestContext {
        #[cfg_attr(miri, expect(unused, reason = "no used by tests with miri"))]
        pub fn with_scan_key_feature(&mut self, has_scan_key_feature: bool) -> &mut Self {
            self.has_scan_key_feature = has_scan_key_feature;
            self
        }

        #[expect(unused, reason = "Used in follow-up PRs")]
        pub fn with_crdt(&mut self, is_crdt: bool) -> &mut Self {
            self.is_crdt = is_crdt;
            self
        }

        #[cfg(not(miri))]
        pub fn construct_redis_test_ctx<F>(&mut self, f: F)
        where
            F: FnOnce(&mut TestContextBuilder),
        {
            let mut builder = TestContext::builder();
            f(&mut builder);
            self.redis_ctx = Some(builder.build());
        }
    }

    impl Default for LoadDocumentTestContext {
        fn default() -> Self {
            Self {
                is_crdt: false,
                has_scan_key_feature: true,
                #[cfg(not(miri))]
                redis_ctx: None,
            }
        }
    }

    impl LoadDocumentContext for LoadDocumentTestContext {
        type V = RSValueMock;

        fn is_crdt(&self) -> bool {
            self.is_crdt
        }

        fn has_scan_key_feature(&self) -> bool {
            self.has_scan_key_feature
        }

        fn generate_value(&self, src: ValueSrc, _ct: RLookupCoerceType) -> Self::V {
            match src {
                ValueSrc::ReplyElem(reply_ptr) => {
                    // Convert the RedisModuleCallReply to a string value
                    let mut len: usize = 0;
                    // Safety: Static mutable access to function pointers is idempotent after initialization
                    let ftor = unsafe { ffi::RedisModule_CallReplyStringPtr.unwrap() };
                    // Safety: We assume the caller provided a valid call reply pointer.
                    let string_ptr =
                        unsafe { ftor(reply_ptr as *mut ffi::RedisModuleCallReply, &mut len) };
                    if string_ptr.is_null() {
                        RSValueMock::create_string(String::new())
                    } else {
                        // Safety: We assume the return of RedisModule_CallReplyStringPtr is valid for len bytes.
                        let slice =
                            unsafe { std::slice::from_raw_parts(string_ptr as *const u8, len) };
                        let string = String::from_utf8_lossy(slice).to_string();
                        RSValueMock::create_string(string)
                    }
                }
                ValueSrc::HVal(redis_string) => {
                    let string = redis_string.to_string();
                    RSValueMock::create_string(string)
                }
            }
        }

        fn load_json(
            &self,
            _lookup: &mut RLookup<'_>,
            _dst_row: &mut RLookupRow<'_, Self::V>,
            _options: &LoadDocumentOptions<'_, Self::V>,
        ) -> Result<(), LoadDocumentError> {
            Ok(())
        }

        fn load_individual_keys(
            &self,
            _lookup: &mut RLookup<'_>,
            _dst_row: &mut RLookupRow<'_, Self::V>,
            _options: &LoadDocumentOptions<'_, Self::V>,
        ) -> Result<(), LoadDocumentError> {
            Ok(())
        }
    }

    #[cfg(not(miri))]
    mod excluded_from_miri {

        use super::super::*;
        use super::*;

        use redis_module::KeyType;
        use value::RSValueMock;

        // write a test that simulates loading two fields into an empty row and lookup
        //
        // the test can be used by both the scan key api and the fallback call api code paths
        // therefore we use specialized mocking in the mock module
        fn two_fields_empty_row_and_lookup(
            ctx: &mut LoadDocumentTestContext,
        ) -> Result<(), LoadDocumentError> {
            ctx.construct_redis_test_ctx(|ctx| {
                ctx.with_key_type(&KeyType::Hash);
                ctx.set_key_values(vec![
                    (c"field1".to_owned(), c"value1".to_owned()),
                    (c"field2".to_owned(), c"value2".to_owned()),
                ]);
            });

            let test_ctx = &mut ctx.redis_ctx;
            let redis_ctx =
                std::ptr::from_mut(test_ctx).cast::<redis_module::raw::RedisModuleCtx>();
            let sv = RSSortingVector::new(0);
            let key_ptr = c"TestKey";
            type TOpt<'a> = LoadDocumentOptions<'a, RSValueMock>;
            let opt: TOpt = LoadDocumentOptionsBuilder::new(
                redis_ctx as *mut TestContext as *mut redis_module::raw::RedisModuleCtx,
                &sv,
                DocumentType::Hash,
            )
            .set_mode(RLookupLoadMode::AllKeys as u32)
            .with_key_ptr(key_ptr.as_ptr() as *const _)
            .build()?;

            let mut lookup = RLookup::new();
            let mut row = RLookupRow::new(&lookup);
            load_document_int(&mut lookup, &mut row, &opt, ctx)?;

            assert_eq!(row.len(), 2);

            let cursor = lookup.find_key_by_name(c"field1").unwrap();
            let rlk = cursor.current().unwrap();
            assert_eq!(
                row.get(rlk),
                Some(&RSValueMock::create_string("value1".to_owned()))
            );

            let cursor = lookup.find_key_by_name(c"field2").unwrap();
            let rlk = cursor.current().unwrap();
            assert_eq!(
                row.get(rlk),
                Some(&RSValueMock::create_string("value2".to_owned()))
            );

            Ok(())
        }

        #[test]
        fn add_two_fields_hash_scan() -> Result<(), LoadDocumentError> {
            // Safety: Initialization of function pointers multiple times is idempotent
            redis_mock::init_redis_module_mock();
            let mut ctx = LoadDocumentTestContext::default();
            ctx.with_scan_key_feature(true);
            two_fields_empty_row_and_lookup(&mut ctx)
        }

        #[test]
        fn add_two_fields_hash_fallback() -> Result<(), LoadDocumentError> {
            redis_mock::init_redis_module_mock();
            let mut ctx = LoadDocumentTestContext::default();
            ctx.with_scan_key_feature(false);
            two_fields_empty_row_and_lookup(&mut ctx)
        }

        #[test]
        fn error_paths() -> Result<(), LoadDocumentError> {
            redis_mock::init_redis_module_mock();
            let mut ctx = LoadDocumentTestContext::default();
            let key_name = c"TestKey";

            struct TestCase {
                has_scan_key_feature: bool,
                key_type: KeyType,
                expected_error: Result<(), LoadDocumentError>,
            }

            let cases = vec![
                TestCase {
                    has_scan_key_feature: true,
                    key_type: KeyType::Empty,
                    expected_error: Err(LoadDocumentError::InvalidKey {
                        sub_error: KeyError::DoesNotExist,
                        #[cfg(debug_assertions)]
                        key: Some("TestKey".to_string()),
                    }),
                },
                TestCase {
                    has_scan_key_feature: false,
                    key_type: KeyType::Empty,
                    expected_error: Err(LoadDocumentError::InvalidKey {
                        sub_error: KeyError::DoesNotExistOrIsWrongType,
                        #[cfg(debug_assertions)]
                        key: Some("TestKey".to_string()),
                    }),
                },
                TestCase {
                    has_scan_key_feature: true,
                    key_type: KeyType::String,
                    expected_error: Err(LoadDocumentError::InvalidKey {
                        sub_error: KeyError::IsNoHash,
                        #[cfg(debug_assertions)]
                        key: Some("TestKey".to_string()),
                    }),
                },
                TestCase {
                    has_scan_key_feature: false,
                    key_type: KeyType::String,
                    expected_error: Err(LoadDocumentError::InvalidKey {
                        sub_error: KeyError::DoesNotExistOrIsWrongType,
                        #[cfg(debug_assertions)]
                        key: Some("TestKey".to_string()),
                    }),
                },
            ];

            for case in &cases {
                ctx.with_scan_key_feature(case.has_scan_key_feature);
                ctx.construct_redis_test_ctx(|ctx| {
                    ctx.set_key_values(vec![]);
                    ctx.with_key_type(&case.key_type);
                });

                let test_ctx = &mut ctx.redis_ctx;
                let redis_ctx =
                    std::ptr::from_mut(test_ctx).cast::<redis_module::raw::RedisModuleCtx>();
                let sv = RSSortingVector::new(0);
                type TOpt<'a> = LoadDocumentOptions<'a, RSValueMock>;
                let opt: TOpt = LoadDocumentOptionsBuilder::new(
                    redis_ctx as *mut TestContext as *mut redis_module::raw::RedisModuleCtx,
                    &sv,
                    DocumentType::Hash,
                )
                .set_mode(RLookupLoadMode::AllKeys as u32)
                .with_key_ptr(key_name.as_ptr() as *const _)
                .build()?;

                let mut lookup = RLookup::new();
                let mut row = RLookupRow::new(&lookup);
                let res = load_document_int(&mut lookup, &mut row, &opt, &ctx);
                if res != case.expected_error {
                    println!(
                        "Test case failed: has_scan_key_feature = {}, key_type = {:?}",
                        case.has_scan_key_feature, case.key_type
                    );
                }
                assert_eq!(res, case.expected_error);
            }

            Ok(())
        }
    }

    #[test]
    fn error_invalid_mode() -> Result<(), LoadDocumentError> {
        let sv = RSSortingVector::new(0);
        let key_ptr = c"TestKey";
        type TRes<'a> = Result<LoadDocumentOptions<'a, RSValueMock>, LoadDocumentError>;

        let invalid_opt: TRes =
            LoadDocumentOptionsBuilder::new(std::ptr::null_mut(), &sv, DocumentType::Hash)
                // Safety: We set an invalid mode for testing purposes
                .set_mode(42) // Invalid mode
                .with_key_ptr(key_ptr.as_ptr() as *const _)
                .build();

        assert!(matches!(
            invalid_opt,
            Err(LoadDocumentError::InvalidArguments { .. })
        ));

        Ok(())
    }

    #[test]
    fn error_missing_keyptr_for_hash_codepaths() -> Result<(), LoadDocumentError> {
        let sv = RSSortingVector::new(0);
        type TRes<'a> = Result<LoadDocumentOptions<'a, RSValueMock>, LoadDocumentError>;
        let res: TRes =
            LoadDocumentOptionsBuilder::new(std::ptr::null_mut(), &sv, DocumentType::Hash).build();

        assert!(matches!(
            res,
            Err(LoadDocumentError::InvalidArguments { .. })
        ));

        Ok(())
    }
}
