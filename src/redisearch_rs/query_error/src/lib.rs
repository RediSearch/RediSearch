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
            Self::Generic => c"SEARCH_GENERIC: Generic error evaluating the query",
            Self::Syntax => c"SEARCH_SYNTAX: Parsing/Syntax error for query string",
            Self::ParseArgs => c"SEARCH_PARSE_ARGS: Error parsing query/aggregation arguments",
            Self::AddArgs => c"SEARCH_ADD_ARGS: Error parsing document indexing arguments",
            Self::Expr => c"SEARCH_EXPR: Parsing/Evaluating dynamic expression failed",
            Self::Keyword => c"SEARCH_KEYWORD: Could not handle query keyword",
            Self::NoResults => c"SEARCH_NO_RESULTS: Query matches no results",
            Self::BadAttr => c"SEARCH_BAD_ATTR: Attribute not supported for term",
            Self::Inval => c"SEARCH_INVAL: Could not validate the query nodes (bad attribute?)",
            Self::BuildPlan => c"SEARCH_BUILD_PLAN: Could not build plan from query",
            Self::ConstructPipeline => c"SEARCH_CONSTRUCT_PIPELINE: Could not construct query pipeline",
            Self::NoReducer => c"SEARCH_NO_REDUCER: Reducer not found",
            Self::ReducerGeneric => c"SEARCH_REDUCER_GENERIC: Generic reducer error",
            Self::AggPlan => c"SEARCH_AGG_PLAN: Could not plan aggregation request",
            Self::CursorAlloc => c"SEARCH_CURSOR_ALLOC: Could not allocate a cursor",
            Self::ReducerInit => c"SEARCH_REDUCER_INIT: Could not initialize reducer",
            Self::QString => c"SEARCH_QSTRING: Bad query string",
            Self::NoPropKey => c"SEARCH_NO_PROP_KEY: Property not found in schema",
            Self::NoPropVal => c"SEARCH_NO_PROP_VAL: Value not found in result (not a hard error)",
            Self::NoDoc => c"SEARCH_NO_DOC: Document not found",
            Self::NoOption => c"SEARCH_NO_OPTION: Invalid option",
            Self::RedisKeyType => c"SEARCH_REDIS_KEY_TYPE: Invalid Redis key",
            Self::InvalPath => c"SEARCH_INVAL_PATH: Invalid path",
            Self::IndexExists => c"SEARCH_INDEX_EXISTS: Index already exists",
            Self::BadOption => c"SEARCH_BAD_OPTION: Option not supported for current mode",
            Self::BadOrderOption => c"SEARCH_BAD_ORDER_OPTION: Path with undefined ordering does not support slop/inorder",
            Self::Limit => c"SEARCH_LIMIT: Limit exceeded",
            Self::NoIndex => c"SEARCH_INDEX_NOT_FOUND: Index not found",
            Self::DocExists => c"SEARCH_DOC_EXISTS: Document already exists",
            Self::DocNotAdded => c"SEARCH_DOC_NOT_ADDED: Document was not added because condition was unmet",
            Self::DupField => c"SEARCH_DUP_FIELD: Field was specified twice",
            Self::GeoFormat => cr#"SEARCH_GEO_FORMAT: Invalid lon/lat format. Use "lon lat" or "lon,lat""#,
            Self::NoDistribute => c"SEARCH_NO_DISTRIBUTE: Could not distribute the operation",
            Self::UnsuppType => c"SEARCH_UNSUPP_TYPE: Unsupported index type",
            Self::NotNumeric => c"SEARCH_NOT_NUMERIC: Could not convert value to a number",
            Self::TimedOut => c"SEARCH_TIMED_OUT: Timeout limit was reached",
            Self::NoParam => c"SEARCH_PARAM_NOT_FOUND: Parameter not found",
            Self::DupParam => c"SEARCH_DUP_PARAM: Parameter was specified twice",
            Self::BadVal => c"SEARCH_BAD_VAL: Invalid value was given",
            Self::NonHybrid => c"SEARCH_NON_HYBRID: hybrid query attributes were sent for a non-hybrid query",
            Self::HybridNonExist => c"SEARCH_HYBRID_NON_EXIST: invalid hybrid policy was given",
            Self::AdhocWithBatchSize => c"SEARCH_ADHOC_WITH_BATCH_SIZE: 'batch size' is irrelevant for 'ADHOC_BF' policy",
            Self::AdhocWithEfRuntime => c"SEARCH_ADHOC_WITH_EF_RUNTIME: 'EF_RUNTIME' is irrelevant for 'ADHOC_BF' policy",
            Self::NonRange => c"SEARCH_NON_RANGE: range query attributes were sent for a non-range query",
            Self::Missing => c"SEARCH_MISSING: 'ismissing' requires field to be defined with 'INDEXMISSING'",
            Self::Mismatch => c"SEARCH_MISMATCH: Index mismatch: Shard index is different than queried index",
            Self::UnknownIndex => c"SEARCH_INDEX_NOT_FOUND: Index not found",
            Self::DroppedBackground => c"SEARCH_DROPPED_BACKGROUND: The index was dropped before the query could be executed",
            Self::AliasConflict => c"SEARCH_ALIAS_CONFLICT: Alias conflicts with an existing index name",
            Self::IndexBgOOMFail => c"SEARCH_INDEX_BG_OOM_FAIL: Index background scan did not complete due to OOM",
            Self::WeightNotAllowed => c"SEARCH_WEIGHT_NOT_ALLOWED: Weight attributes are not allowed",
            Self::VectorNotAllowed => c"SEARCH_VECTOR_NOT_ALLOWED: Vector queries are not allowed",
            Self::OutOfMemory => c"SEARCH_OUT_OF_MEMORY: Not enough memory available to execute the query",
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

    pub fn clear(&mut self) {
        *self = Self::default();
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
