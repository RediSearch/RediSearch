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
use strum::FromRepr;

/// cbindgen:prefix-with-name
/// cbindgen:rename-all=ScreamingSnakeCase
#[derive(Clone, Copy, Default, FromRepr, PartialEq, Eq)]
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
    NotNumeric,
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
    UnknownIndex,
    DroppedBackground,
    AliasConflict,
    IndexBgOOMFail,
    WeightNotAllowed,
    VectorNotAllowed,
    OutOfMemory,
    UnavailableSlots,
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

    // TODO(enricozb): this should be moved to either a thiserror or strum macro.
    // This is done as &'static CStr because we need to provide *const c_char
    // representations of the error codes for FFI into C code.
    pub const fn to_c_str(self) -> &'static CStr {
        match self {
            Self::Ok => c"Success (not an error)",
            Self::Generic => c"SEARCH_GENERIC_ERR: Generic error evaluating the query",
            Self::Syntax => c"SEARCH_SYNTAX_ERR: Parsing/Syntax error for query string",
            Self::ParseArgs => c"SEARCH_ARG_PARSE_ERR: Error parsing query/aggregation arguments",
            Self::AddArgs => c"SEARCH_DOC_INDEX_ARG_ERR: Error parsing document indexing arguments",
            Self::Expr => c"SEARCH_EXPR_ERR: Parsing/Evaluating dynamic expression failed",
            Self::Keyword => c"SEARCH_KEYWORD_ERR: Could not handle query keyword",
            Self::NoResults => c"SEARCH_NO_RESULTS: Query matches no results",
            Self::BadAttr => c"SEARCH_ATTR_BAD: Attribute not supported for term",
            Self::Inval => c"SEARCH_QUERY_INVALID: Could not validate the query nodes (bad attribute?)",
            Self::BuildPlan => c"SEARCH_PLAN_BUILD_ERR: Could not build plan from query",
            Self::ConstructPipeline => c"SEARCH_PIPELINE_CONSTRUCT_ERR: Could not construct query pipeline",
            Self::NoReducer => c"SEARCH_REDUCER_MISSING: Missing reducer",
            Self::ReducerGeneric => c"SEARCH_REDUCER_ERR: Generic reducer error",
            Self::AggPlan => c"SEARCH_AGG_PLAN_ERR: Could not plan aggregation request",
            Self::CursorAlloc => c"SEARCH_CURSOR_ALLOC_ERR: Could not allocate a cursor",
            Self::ReducerInit => c"SEARCH_REDUCER_INIT_ERR: Could not initialize reducer",
            Self::QString => c"SEARCH_QUERY_STRING_BAD: Bad query string",
            Self::NoPropKey => c"SEARCH_PROP_NOT_FOUND: Property does not exist in schema",
            Self::NoPropVal => c"SEARCH_PROP_VALUE_NOT_FOUND: Value was not found in result (not a hard error)",
            Self::NoDoc => c"SEARCH_DOC_NOT_FOUND: Document does not exist",
            Self::NoOption => c"SEARCH_OPTION_INVALID: Invalid option",
            Self::RedisKeyType => c"SEARCH_REDIS_KEY_TYPE_INVALID: Invalid Redis key",
            Self::InvalPath => c"SEARCH_PATH_INVALID: Invalid path",
            Self::IndexExists => c"SEARCH_INDEX_EXISTS: Index already exists",
            Self::BadOption => c"SEARCH_OPTION_BAD: Option not supported for current mode",
            Self::BadOrderOption => c"SEARCH_ORDER_OPTION_BAD: Path with undefined ordering does not support slop/inorder",
            Self::Limit => c"SEARCH_LIMIT_EXCEEDED: Limit exceeded",
            Self::NoIndex => c"SEARCH_INDEX_NOT_FOUND: Index not found",
            Self::DocExists => c"SEARCH_DOC_EXISTS: Document already exists",
            Self::DocNotAdded => c"SEARCH_DOC_NOT_ADDED: Document was not added because condition was unmet",
            Self::DupField => c"SEARCH_FIELD_DUP: Field was specified twice",
            Self::GeoFormat => cr#"SEARCH_GEO_FORMAT_BAD: Invalid lon/lat format. Use "lon lat" or "lon,lat""#,
            Self::NoDistribute => c"SEARCH_DIST_FAILED: Could not distribute the operation",
            Self::UnsuppType => c"SEARCH_INDEX_TYPE_UNSUPPORTED: Unsupported index type",
            Self::NotNumeric => c"SEARCH_NUMERIC_CONVERSION_ERR: Could not convert value to a number",
            Self::TimedOut => c"SEARCH_TIMEOUT: Timeout limit was reached",
            Self::NoParam => c"SEARCH_PARAM_NOT_FOUND: Parameter not found",
            Self::DupParam => c"SEARCH_PARAM_DUP: Parameter was specified twice",
            Self::BadVal => c"SEARCH_VALUE_BAD: Invalid value was given",
            Self::NonHybrid => c"SEARCH_HYBRID_ATTR_NON_HYBRID: hybrid query attributes were sent for a non-hybrid query",
            Self::HybridNonExist => c"SEARCH_HYBRID_POLICY_INVALID: invalid hybrid policy was given",
            Self::AdhocWithBatchSize => c"SEARCH_ADHOC_BATCH_SIZE_IRRELEVANT: 'batch size' is irrelevant for the selected policy",
            Self::AdhocWithEfRuntime => c"SEARCH_ADHOC_EF_RUNTIME_IRRELEVANT: 'EF_RUNTIME' is irrelevant for the selected policy",
            Self::NonRange => c"SEARCH_RANGE_ATTR_NON_RANGE: range query attributes were sent for a non-range query",
            Self::Missing => c"SEARCH_MISSING_REQUIRES_INDEXMISSING: 'ismissing' requires field to be defined with 'INDEXMISSING'",
            Self::Mismatch => c"SEARCH_INDEX_MISMATCH: Index mismatch: Shard index is different than queried index",
            Self::UnknownIndex => c"SEARCH_INDEX_NOT_FOUND: Index not found",
            Self::DroppedBackground => c"SEARCH_INDEX_DROPPED_BACKGROUND: The index was dropped before the query could be executed",
            Self::AliasConflict => c"SEARCH_ALIAS_CONFLICT: Alias conflicts with an existing index name",
            Self::IndexBgOOMFail => c"SEARCH_INDEX_BG_OOM_FAIL: Index background scan did not complete due to OOM",
            Self::WeightNotAllowed => c"SEARCH_WEIGHT_NOT_ALLOWED: Weight attributes are not allowed",
            Self::VectorNotAllowed => c"SEARCH_VECTOR_NOT_ALLOWED: Vector queries are not allowed",
            Self::OutOfMemory => c"SEARCH_OOM: Not enough memory available to execute the query",
            Self::UnavailableSlots => c"SEARCH_SLOTS_UNAVAILABLE: Query requires unavailable slots",
        }
    }
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
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u8)]
pub enum QueryWarningCode {
    #[default]
    Ok = 0,
    TimedOut,
    ReachedMaxPrefixExpansions,
    OutOfMemoryShard,
    OutOfMemoryCoord,
}

impl QueryWarningCode {
    pub const fn is_ok(self) -> bool {
        matches!(self, Self::Ok)
    }
    pub const fn to_c_str(self) -> &'static CStr {
        match self {
            Self::Ok => c"Success (not a warning)",
            Self::TimedOut => c"SEARCH_TIMEOUT: Timeout limit was reached",
            Self::ReachedMaxPrefixExpansions => c"SEARCH_PREFIX_EXPANSIONS_LIMIT: Max prefix expansions limit was reached",
            Self::OutOfMemoryShard => {
                c"SEARCH_OOM_SHARD: Shard failed to execute the query due to insufficient memory"
            }
            Self::OutOfMemoryCoord => {
                c"SEARCH_OOM_COORD: One or more shards failed to execute the query due to insufficient memory"
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
