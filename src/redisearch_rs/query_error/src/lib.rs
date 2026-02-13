/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ffi::{CStr, CString};
use std::fmt::{Debug, Display};
use strum::{EnumCount, FromRepr};

/// Returns the maximum valid numeric value for [`QueryErrorCode`].
///
/// This is intended for C/C++ tests/tools that want to iterate over all codes without
/// hardcoding the current "last" variant.
///
/// NOTE: This assumes [`QueryErrorCode`] uses a contiguous `repr(u8)` starting at 0.
pub const fn query_error_code_max_value() -> u8 {
    (QueryErrorCode::COUNT - 1) as u8
}

/// cbindgen:prefix-with-name
/// cbindgen:rename-all=ScreamingSnakeCase
#[derive(Clone, Copy, Default, EnumCount, FromRepr, PartialEq, Eq)]
#[repr(u8)]
pub enum QueryErrorCode {
    #[default]
    Ok = 0,
    Generic,
    Syntax,
    ParseArgs,
    AddArgs,
    Expr,
    Keyword,
    NoResults,
    BadAttr,
    Inval,
    BuildPlan,
    ConstructPipeline,
    NoReducer,
    ReducerGeneric,
    AggPlan,
    CursorAlloc,
    ReducerInit,
    QString,
    NoPropKey,
    NoPropVal,
    NoDoc,
    NoOption,
    RedisKeyType,
    InvalPath,
    IndexExists,
    BadOption,
    BadOrderOption,
    Limit,
    NoIndex,
    DocExists,
    DocNotAdded,
    DupField,
    GeoFormat,
    NoDistribute,
    UnsuppType,
    TimedOut,
    NoParam,
    DupParam,
    BadVal,
    NonHybrid,
    HybridNonExist,
    AdhocWithBatchSize,
    AdhocWithEfRuntime,
    NonRange,
    Missing,
    Mismatch,
    DroppedBackground,
    AliasConflict,
    IndexBgOOMFail,
    WeightNotAllowed,
    VectorNotAllowed,
    OutOfMemory,
    UnavailableSlots,
    FlexLimitNumberOfIndexes,
    FlexUnsupportedField,
    FlexUnsupportedFTCreateArgument,
    DiskCreation,
    FlexSkipInitialScanMissingArgument,
    VectorBlobSizeMismatch,
    VectorLenBad,
    NumericValueInvalid,
    ArgUnrecognized,
    GeoCoordinatesInvalid,
    JsonTypeBad,
    ClusterNoResponses,
}

impl Debug for QueryErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{self}")
    }
}

impl Display for QueryErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", self.to_c_str().to_str().unwrap())
    }
}

impl QueryErrorCode {
    pub const fn is_ok(self) -> bool {
        matches!(self, Self::Ok)
    }

    /// Returns the error prefix string (e.g. `"SEARCH_TIMEOUT: "`).
    /// For `Ok`, returns an empty string.
    pub const fn prefix_c_str(self) -> &'static CStr {
        self.strings().prefix
    }

    /// Returns the default error message without prefix (e.g. `"Timeout limit was reached"`).
    pub const fn default_message_c_str(self) -> &'static CStr {
        self.strings().default_msg
    }

    /// Returns the full default error string: prefix + message
    /// (e.g. `"SEARCH_TIMEOUT: Timeout limit was reached"`).
    pub const fn to_c_str(self) -> &'static CStr {
        self.strings().default_full_msg
    }

    /// Each variant maps to three static strings: prefix, default message,
    /// and the full default string (prefix + message concatenated at compile time).
    /// The prefix is an explicit constant per variant — it does not need to match
    /// the Rust variant name. A future PR may align them.
    const fn strings(self) -> ErrorCodeStrings {
        match self {
            Self::Ok => ErrorCodeStrings {
                prefix: c"",
                default_msg: c"Success (not an error)",
                default_full_msg: c"Success (not an error)",
            },
            Self::Generic => ErrorCodeStrings {
                prefix: c"SEARCH_GENERIC: ",
                default_msg: c"Generic error evaluating the query",
                default_full_msg: c"SEARCH_GENERIC: Generic error evaluating the query",
            },
            Self::Syntax => ErrorCodeStrings {
                prefix: c"SEARCH_SYNTAX: ",
                default_msg: c"Parsing/Syntax error for query string",
                default_full_msg: c"SEARCH_SYNTAX: Parsing/Syntax error for query string",
            },
            Self::ParseArgs => ErrorCodeStrings {
                prefix: c"SEARCH_PARSE_ARGS: ",
                default_msg: c"Error parsing query/aggregation arguments",
                default_full_msg: c"SEARCH_PARSE_ARGS: Error parsing query/aggregation arguments",
            },
            Self::AddArgs => ErrorCodeStrings {
                prefix: c"SEARCH_ADD_ARGS: ",
                default_msg: c"Error parsing document indexing arguments",
                default_full_msg: c"SEARCH_ADD_ARGS: Error parsing document indexing arguments",
            },
            Self::Expr => ErrorCodeStrings {
                prefix: c"SEARCH_EXPR: ",
                default_msg: c"Parsing/Evaluating dynamic expression failed",
                default_full_msg: c"SEARCH_EXPR: Parsing/Evaluating dynamic expression failed",
            },
            Self::Keyword => ErrorCodeStrings {
                prefix: c"SEARCH_KEYWORD: ",
                default_msg: c"Could not handle query keyword",
                default_full_msg: c"SEARCH_KEYWORD: Could not handle query keyword",
            },
            Self::NoResults => ErrorCodeStrings {
                prefix: c"SEARCH_NO_RESULTS: ",
                default_msg: c"Query matches no results",
                default_full_msg: c"SEARCH_NO_RESULTS: Query matches no results",
            },
            Self::BadAttr => ErrorCodeStrings {
                prefix: c"SEARCH_ATTR_BAD: ",
                default_msg: c"Attribute not supported for term",
                default_full_msg: c"SEARCH_ATTR_BAD: Attribute not supported for term",
            },
            Self::Inval => ErrorCodeStrings {
                prefix: c"SEARCH_QUERY_BAD: ",
                default_msg: c"Could not validate the query nodes (bad attribute?)",
                default_full_msg: c"SEARCH_QUERY_BAD: Could not validate the query nodes (bad attribute?)",
            },
            Self::BuildPlan => ErrorCodeStrings {
                prefix: c"SEARCH_BUILD_PLAN: ",
                default_msg: c"Could not build plan from query",
                default_full_msg: c"SEARCH_BUILD_PLAN: Could not build plan from query",
            },
            Self::ConstructPipeline => ErrorCodeStrings {
                prefix: c"SEARCH_CONSTRUCT_PIPELINE: ",
                default_msg: c"Could not construct query pipeline",
                default_full_msg: c"SEARCH_CONSTRUCT_PIPELINE: Could not construct query pipeline",
            },
            Self::NoReducer => ErrorCodeStrings {
                prefix: c"SEARCH_REDUCER_NOT_FOUND: ",
                default_msg: c"Reducer not found",
                default_full_msg: c"SEARCH_REDUCER_NOT_FOUND: Reducer not found",
            },
            Self::ReducerGeneric => ErrorCodeStrings {
                prefix: c"SEARCH_REDUCER_ERROR: ",
                default_msg: c"Generic reducer error",
                default_full_msg: c"SEARCH_REDUCER_ERROR: Generic reducer error",
            },
            Self::AggPlan => ErrorCodeStrings {
                prefix: c"SEARCH_AGG_PLAN: ",
                default_msg: c"Could not plan aggregation request",
                default_full_msg: c"SEARCH_AGG_PLAN: Could not plan aggregation request",
            },
            Self::CursorAlloc => ErrorCodeStrings {
                prefix: c"SEARCH_CURSOR_ALLOC_FAILED: ",
                default_msg: c"Could not allocate a cursor",
                default_full_msg: c"SEARCH_CURSOR_ALLOC_FAILED: Could not allocate a cursor",
            },
            Self::ReducerInit => ErrorCodeStrings {
                prefix: c"SEARCH_REDUCER_INIT_FAILED: ",
                default_msg: c"Could not initialize reducer",
                default_full_msg: c"SEARCH_REDUCER_INIT_FAILED: Could not initialize reducer",
            },
            Self::QString => ErrorCodeStrings {
                prefix: c"SEARCH_QUERY_STRING_BAD: ",
                default_msg: c"Bad query string",
                default_full_msg: c"SEARCH_QUERY_STRING_BAD: Bad query string",
            },
            Self::NoPropKey => ErrorCodeStrings {
                prefix: c"SEARCH_PROP_NOT_FOUND: ",
                default_msg: c"Property not loaded nor in pipeline",
                default_full_msg: c"SEARCH_PROP_NOT_FOUND: Property not loaded nor in pipeline",
            },
            Self::NoPropVal => ErrorCodeStrings {
                prefix: c"SEARCH_VALUE_NOT_FOUND: ",
                default_msg: c"Value not found in result (not a hard error)",
                default_full_msg: c"SEARCH_VALUE_NOT_FOUND: Value not found in result (not a hard error)",
            },
            Self::NoDoc => ErrorCodeStrings {
                prefix: c"SEARCH_DOC_NOT_FOUND: ",
                default_msg: c"Document not found",
                default_full_msg: c"SEARCH_DOC_NOT_FOUND: Document not found",
            },
            Self::NoOption => ErrorCodeStrings {
                prefix: c"SEARCH_OPTION_INVALID: ",
                default_msg: c"Invalid option",
                default_full_msg: c"SEARCH_OPTION_INVALID: Invalid option",
            },
            Self::RedisKeyType => ErrorCodeStrings {
                prefix: c"SEARCH_REDIS_KEY_TYPE_BAD: ",
                default_msg: c"Invalid Redis key",
                default_full_msg: c"SEARCH_REDIS_KEY_TYPE_BAD: Invalid Redis key",
            },
            Self::InvalPath => ErrorCodeStrings {
                prefix: c"SEARCH_PATH_BAD: ",
                default_msg: c"Invalid path",
                default_full_msg: c"SEARCH_PATH_BAD: Invalid path",
            },
            Self::IndexExists => ErrorCodeStrings {
                prefix: c"SEARCH_INDEX_EXISTS: ",
                default_msg: c"Index already exists",
                default_full_msg: c"SEARCH_INDEX_EXISTS: Index already exists",
            },
            Self::BadOption => ErrorCodeStrings {
                prefix: c"SEARCH_OPTION_BAD: ",
                default_msg: c"Option not supported for current mode",
                default_full_msg: c"SEARCH_OPTION_BAD: Option not supported for current mode",
            },
            Self::BadOrderOption => ErrorCodeStrings {
                prefix: c"SEARCH_ORDER_OPTION_BAD: ",
                default_msg: c"Path with undefined ordering does not support slop/inorder",
                default_full_msg: c"SEARCH_ORDER_OPTION_BAD: Path with undefined ordering does not support slop/inorder",
            },
            Self::Limit => ErrorCodeStrings {
                prefix: c"SEARCH_LIMIT_OVER: ",
                default_msg: c"Limit exceeded",
                default_full_msg: c"SEARCH_LIMIT_OVER: Limit exceeded",
            },
            Self::NoIndex => ErrorCodeStrings {
                prefix: c"SEARCH_INDEX_NOT_FOUND: ",
                default_msg: c"Index not found",
                default_full_msg: c"SEARCH_INDEX_NOT_FOUND: Index not found",
            },
            Self::DocExists => ErrorCodeStrings {
                prefix: c"SEARCH_DOCUMENT_EXISTS: ",
                default_msg: c"Document already exists",
                default_full_msg: c"SEARCH_DOCUMENT_EXISTS: Document already exists",
            },
            Self::DocNotAdded => ErrorCodeStrings {
                prefix: c"SEARCH_DOCUMENT_NOT_ADDED: ",
                default_msg: c"Document was not added because condition was unmet",
                default_full_msg: c"SEARCH_DOCUMENT_NOT_ADDED: Document was not added because condition was unmet",
            },
            Self::DupField => ErrorCodeStrings {
                prefix: c"SEARCH_FIELD_DUP: ",
                default_msg: c"Field was specified twice",
                default_full_msg: c"SEARCH_FIELD_DUP: Field was specified twice",
            },
            Self::GeoFormat => ErrorCodeStrings {
                prefix: c"SEARCH_GEO_FORMAT_BAD: ",
                default_msg: c"Invalid lon/lat format. Use \"lon lat\" or \"lon,lat\"",
                default_full_msg: c"SEARCH_GEO_FORMAT_BAD: Invalid lon/lat format. Use \"lon lat\" or \"lon,lat\"",
            },
            Self::NoDistribute => ErrorCodeStrings {
                prefix: c"SEARCH_DIST_FAILED: ",
                default_msg: c"Could not distribute the operation",
                default_full_msg: c"SEARCH_DIST_FAILED: Could not distribute the operation",
            },
            Self::UnsuppType => ErrorCodeStrings {
                prefix: c"SEARCH_TYPE_UNSUP: ",
                default_msg: c"Unsupported index type",
                default_full_msg: c"SEARCH_TYPE_UNSUP: Unsupported index type",
            },
            Self::TimedOut => ErrorCodeStrings {
                prefix: c"SEARCH_TIMEOUT: ",
                default_msg: c"Timeout limit was reached",
                default_full_msg: c"SEARCH_TIMEOUT: Timeout limit was reached",
            },
            Self::NoParam => ErrorCodeStrings {
                prefix: c"SEARCH_PARAM_NOT_FOUND: ",
                default_msg: c"Parameter not found",
                default_full_msg: c"SEARCH_PARAM_NOT_FOUND: Parameter not found",
            },
            Self::DupParam => ErrorCodeStrings {
                prefix: c"SEARCH_PARAM_DUP: ",
                default_msg: c"Parameter was specified twice",
                default_full_msg: c"SEARCH_PARAM_DUP: Parameter was specified twice",
            },
            Self::BadVal => ErrorCodeStrings {
                prefix: c"SEARCH_VALUE_BAD: ",
                default_msg: c"Invalid value was given",
                default_full_msg: c"SEARCH_VALUE_BAD: Invalid value was given",
            },
            Self::NonHybrid => ErrorCodeStrings {
                prefix: c"SEARCH_HYBRID_ATTR_NON_HYBRID: ",
                default_msg: c"hybrid query attributes were sent for a non-hybrid query",
                default_full_msg: c"SEARCH_HYBRID_ATTR_NON_HYBRID: hybrid query attributes were sent for a non-hybrid query",
            },
            Self::HybridNonExist => ErrorCodeStrings {
                prefix: c"SEARCH_HYBRID_POLICY_BAD: ",
                default_msg: c"invalid hybrid policy was given",
                default_full_msg: c"SEARCH_HYBRID_POLICY_BAD: invalid hybrid policy was given",
            },
            Self::AdhocWithBatchSize => ErrorCodeStrings {
                prefix: c"SEARCH_ADHOC_BATCH_SIZE_IRRELEVANT: ",
                default_msg: c"'batch size' is irrelevant for the selected policy",
                default_full_msg: c"SEARCH_ADHOC_BATCH_SIZE_IRRELEVANT: 'batch size' is irrelevant for the selected policy",
            },
            Self::AdhocWithEfRuntime => ErrorCodeStrings {
                prefix: c"SEARCH_ADHOC_EF_RUNTIME_IRRELEVANT: ",
                default_msg: c"'EF_RUNTIME' is irrelevant for the selected policy",
                default_full_msg: c"SEARCH_ADHOC_EF_RUNTIME_IRRELEVANT: 'EF_RUNTIME' is irrelevant for the selected policy",
            },
            Self::NonRange => ErrorCodeStrings {
                prefix: c"SEARCH_RANGE_ATTR_NON_RANGE: ",
                default_msg: c"range query attributes were sent for a non-range query",
                default_full_msg: c"SEARCH_RANGE_ATTR_NON_RANGE: range query attributes were sent for a non-range query",
            },
            Self::Missing => ErrorCodeStrings {
                prefix: c"SEARCH_FIELD_MISSING_REQ: ",
                default_msg: c"'ismissing' requires field to be defined with 'INDEXMISSING'",
                default_full_msg: c"SEARCH_FIELD_MISSING_REQ: 'ismissing' requires field to be defined with 'INDEXMISSING'",
            },
            Self::Mismatch => ErrorCodeStrings {
                prefix: c"SEARCH_INDEX_MISMATCH: ",
                default_msg: c"Index mismatch: Shard index is different than queried index",
                default_full_msg: c"SEARCH_INDEX_MISMATCH: Index mismatch: Shard index is different than queried index",
            },
            Self::DroppedBackground => ErrorCodeStrings {
                prefix: c"SEARCH_INDEX_DROPPED_BG: ",
                default_msg: c"The index was dropped before the query could be executed",
                default_full_msg: c"SEARCH_INDEX_DROPPED_BG: The index was dropped before the query could be executed",
            },
            Self::AliasConflict => ErrorCodeStrings {
                prefix: c"SEARCH_ALIAS_CONFLICT: ",
                default_msg: c"Alias conflicts with an existing index name",
                default_full_msg: c"SEARCH_ALIAS_CONFLICT: Alias conflicts with an existing index name",
            },
            Self::IndexBgOOMFail => ErrorCodeStrings {
                prefix: c"SEARCH_INDEX_BG_OOM_FAIL: ",
                default_msg: c"Index background scan did not complete due to OOM",
                default_full_msg: c"SEARCH_INDEX_BG_OOM_FAIL: Index background scan did not complete due to OOM",
            },
            Self::WeightNotAllowed => ErrorCodeStrings {
                prefix: c"SEARCH_WEIGHT_NOT_ALLOWED: ",
                default_msg: c"Weight attributes are not allowed",
                default_full_msg: c"SEARCH_WEIGHT_NOT_ALLOWED: Weight attributes are not allowed",
            },
            Self::VectorNotAllowed => ErrorCodeStrings {
                prefix: c"SEARCH_VECTOR_NOT_ALLOWED: ",
                default_msg: c"Vector queries are not allowed",
                default_full_msg: c"SEARCH_VECTOR_NOT_ALLOWED: Vector queries are not allowed",
            },
            Self::OutOfMemory => ErrorCodeStrings {
                prefix: c"SEARCH_OOM: ",
                default_msg: c"Not enough memory available to execute the query",
                default_full_msg: c"SEARCH_OOM: Not enough memory available to execute the query",
            },
            Self::UnavailableSlots => ErrorCodeStrings {
                prefix: c"SEARCH_SLOTS_UNAVAIL: ",
                default_msg: c"Query requires unavailable slots",
                default_full_msg: c"SEARCH_SLOTS_UNAVAIL: Query requires unavailable slots",
            },
            Self::FlexLimitNumberOfIndexes => ErrorCodeStrings {
                prefix: c"SEARCH_FLEX_LIMIT_NUMBER_OF_INDEXES: ",
                default_msg: c"Flex index limit was reached",
                default_full_msg: c"SEARCH_FLEX_LIMIT_NUMBER_OF_INDEXES: Flex index limit was reached",
            },
            Self::FlexUnsupportedField => ErrorCodeStrings {
                prefix: c"SEARCH_FLEX_UNSUPPORTED_FIELD: ",
                default_msg: c"Unsupported field for Flex index",
                default_full_msg: c"SEARCH_FLEX_UNSUPPORTED_FIELD: Unsupported field for Flex index",
            },
            Self::FlexUnsupportedFTCreateArgument => ErrorCodeStrings {
                prefix: c"SEARCH_FLEX_UNSUPPORTED_FT_CREATE_ARGUMENT: ",
                default_msg: c"Unsupported FT.CREATE argument for Flex index",
                default_full_msg: c"SEARCH_FLEX_UNSUPPORTED_FT_CREATE_ARGUMENT: Unsupported FT.CREATE argument for Flex index",
            },
            Self::DiskCreation => ErrorCodeStrings {
                prefix: c"SEARCH_DISK_CREATION: ",
                default_msg: c"Could not create disk index",
                default_full_msg: c"SEARCH_DISK_CREATION: Could not create disk index",
            },
            Self::FlexSkipInitialScanMissingArgument => ErrorCodeStrings {
                prefix: c"SEARCH_FLEX_SKIP_INITIAL_SCAN_MISSING_ARGUMENT: ",
                default_msg: c"Flex index requires SKIPINITIALSCAN argument",
                default_full_msg: c"SEARCH_FLEX_SKIP_INITIAL_SCAN_MISSING_ARGUMENT: Flex index requires SKIPINITIALSCAN argument",
            },
            Self::VectorBlobSizeMismatch => ErrorCodeStrings {
                prefix: c"SEARCH_VECTOR_BLOB_SIZE_MISMATCH: ",
                default_msg: c"Vector blob size does not match expected dimensions",
                default_full_msg: c"SEARCH_VECTOR_BLOB_SIZE_MISMATCH: Vector blob size does not match expected dimensions",
            },
            Self::VectorLenBad => ErrorCodeStrings {
                prefix: c"SEARCH_VECTOR_LEN_BAD: ",
                default_msg: c"Invalid vector length",
                default_full_msg: c"SEARCH_VECTOR_LEN_BAD: Invalid vector length",
            },
            Self::NumericValueInvalid => ErrorCodeStrings {
                prefix: c"SEARCH_NUMERIC_VALUE_INVALID: ",
                default_msg: c"Invalid numeric value",
                default_full_msg: c"SEARCH_NUMERIC_VALUE_INVALID: Invalid numeric value",
            },
            Self::ArgUnrecognized => ErrorCodeStrings {
                prefix: c"SEARCH_ARG_UNRECOGNIZED: ",
                default_msg: c"Unknown argument",
                default_full_msg: c"SEARCH_ARG_UNRECOGNIZED: Unknown argument",
            },
            Self::GeoCoordinatesInvalid => ErrorCodeStrings {
                prefix: c"SEARCH_GEO_COORDINATES_INVALID: ",
                default_msg: c"Invalid geo coordinates",
                default_full_msg: c"SEARCH_GEO_COORDINATES_INVALID: Invalid geo coordinates",
            },
            Self::JsonTypeBad => ErrorCodeStrings {
                prefix: c"SEARCH_JSON_TYPE_INVALID: ",
                default_msg: c"Invalid JSON type",
                default_full_msg: c"SEARCH_JSON_TYPE_INVALID: Invalid JSON type",
            },
            Self::ClusterNoResponses => ErrorCodeStrings {
                prefix: c"SEARCH_CLUSTER_NO_RESPONSES: ",
                default_msg: c"no responses received",
                default_full_msg: c"SEARCH_CLUSTER_NO_RESPONSES: no responses received",
            },
        }
    }
}

/// Static string triplet for each error code variant.
struct ErrorCodeStrings {
    /// The error prefix including trailing ": " (e.g. `"SEARCH_TIMEOUT: "`).
    /// Empty for `Ok`.
    prefix: &'static CStr,
    /// The default human-readable message without prefix.
    /// Can be overridden at runtime via `QueryError_SetWithUserDataFmt`.
    default_msg: &'static CStr,
    /// The full default string: prefix + default_msg concatenated at compile time.
    default_full_msg: &'static CStr,
}

#[derive(Clone, Debug, Default)]
pub struct QueryError {
    // FIXME: once QueryError is no longer depended on by C code this should be
    // an Option<QueryErrorCode>.
    code: QueryErrorCode,
    // FIXME: once QueryError is no longer depended on by C code, these CString
    // members should be using the traditional String.
    public_message: Option<CString>,
    private_message: Option<CString>,

    warnings: Warnings,
}

impl QueryError {
    pub const fn is_ok(&self) -> bool {
        self.code.is_ok()
    }

    pub const fn code(&self) -> QueryErrorCode {
        self.code
    }

    pub const fn set_code(&mut self, code: QueryErrorCode) {
        if !self.is_ok() {
            return;
        }

        self.code = code;
    }

    pub fn public_message(&self) -> Option<&CStr> {
        self.public_message.as_deref()
    }

    pub fn private_message(&self) -> Option<&CStr> {
        self.private_message.as_deref()
    }

    pub fn set_private_message(&mut self, private_message: Option<CString>) {
        self.private_message = private_message
    }

    pub fn set_code_and_message(&mut self, code: QueryErrorCode, message: Option<CString>) {
        if !self.is_ok() {
            return;
        }

        self.code = code;
        self.public_message = message.clone();
        self.private_message = message;
    }

    pub const fn warnings(&self) -> &Warnings {
        &self.warnings
    }

    pub const fn warnings_mut(&mut self) -> &mut Warnings {
        &mut self.warnings
    }

    /// Clears error code and messages, but _not_ warnings.
    pub fn clear(&mut self) {
        self.code = QueryErrorCode::default();
        self.private_message = None;
        self.public_message = None;
    }
}

// Enum for query warnings
// Unlike QueryErrorCode, this enum is not tied to any API or string mapping.
// Its current purpose is only to serve as a lightweight identifier that can
// be passed to functions and easily handled via switch/case logic.
/// cbindgen:prefix-with-name
/// cbindgen:rename-all=ScreamingSnakeCase
#[derive(Clone, Copy, Debug, Default, FromRepr, PartialEq, Eq)]
#[repr(u8)]
pub enum QueryWarningCode {
    #[default]
    Ok = 0,
    TimedOut,
    ReachedMaxPrefixExpansions,
    OutOfMemoryShard,
    OutOfMemoryCoord,
    UnavailableSlots,
    AsmInaccurateResults,
}

impl QueryWarningCode {
    pub const fn is_ok(self) -> bool {
        matches!(self, Self::Ok)
    }
    pub const fn to_c_str(self) -> &'static CStr {
        match self {
            Self::Ok => c"Success (not a warning)",
            Self::TimedOut => c"Timeout limit was reached",
            Self::ReachedMaxPrefixExpansions => c"Max prefix expansions limit was reached",
            Self::OutOfMemoryShard => {
                c"Shard failed to execute the query due to insufficient memory"
            }
            Self::OutOfMemoryCoord => {
                c"One or more shards failed to execute the query due to insufficient memory"
            }
            Self::UnavailableSlots => c"Query requires unavailable slots",
            Self::AsmInaccurateResults => {
                c"Query execution exceeded maximum delay for RediSearch to delay key trimming. Results may be incomplete due to Atomic Slot Migration."
            }
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct Warnings {
    reached_max_prefix_expansions: bool,
    out_of_memory: bool,
}

impl Warnings {
    pub const fn reached_max_prefix_expansions(&self) -> bool {
        self.reached_max_prefix_expansions
    }

    pub const fn set_reached_max_prefix_expansions(&mut self) {
        self.reached_max_prefix_expansions = true;
    }

    pub const fn out_of_memory(&self) -> bool {
        self.out_of_memory
    }

    pub const fn set_out_of_memory(&mut self) {
        self.out_of_memory = true;
    }
}

pub mod opaque {
    use super::QueryError;
    use c_ffi_utils::opaque::{Size, Transmute};

    /// An opaque query error which can be passed by value to C.
    ///
    /// The size and alignment of this struct must match the Rust `QueryError`
    /// structure exactly.
    #[repr(C, align(8))]
    pub struct OpaqueQueryError(Size<38>);

    // Safety: `OpaqueQueryError` is defined as a `MaybeUninit` slice of
    // bytes with the same size and alignment as `QueryError`, so any valid
    // `QueryError` has a bit pattern which is a valid `OpaqueQueryError`.
    unsafe impl Transmute<QueryError> for OpaqueQueryError {}

    c_ffi_utils::opaque!(QueryError, OpaqueQueryError);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify that `default_full_msg` equals `prefix + default_msg` for every variant.
    /// This catches any drift when a prefix or message is updated without updating the full string.
    #[test]
    fn error_code_full_msg_equals_prefix_plus_default_msg() {
        for code_u8 in 0..=query_error_code_max_value() {
            let code = QueryErrorCode::from_repr(code_u8)
                .unwrap_or_else(|| panic!("invalid code {code_u8}"));

            let prefix = code
                .prefix_c_str()
                .to_str()
                .expect("prefix is not valid UTF-8");
            let msg = code
                .default_message_c_str()
                .to_str()
                .expect("default_msg is not valid UTF-8");
            let full = code
                .to_c_str()
                .to_str()
                .expect("default_full_msg is not valid UTF-8");

            let expected = format!("{prefix}{msg}");
            assert_eq!(
                full, expected,
                "Mismatch for {code:?}: default_full_msg = {full:?}, but prefix + default_msg = {expected:?}"
            );
        }
    }
}
