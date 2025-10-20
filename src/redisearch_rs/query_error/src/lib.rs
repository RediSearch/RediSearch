/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use derive_more::TryFrom;
use std::ffi::{CStr, CString};
use std::fmt::{Debug, Display};

/// cbindgen:prefix-with-name
/// cbindgen:rename-all=ScreamingSnakeCase
#[derive(Clone, Copy, Default, PartialEq, Eq, TryFrom)]
#[try_from(repr)]
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
    pub fn is_ok(self) -> bool {
        matches!(self, Self::Ok)
    }

    // TODO(enricozb): this should be moved to either a thiserror or strum macro.
    // This is done as &'static CStr because we need to provide *const c_char
    // representations of the error codes for FFI into C code.
    pub fn to_c_str(self) -> &'static CStr {
        match self {
            Self::Ok => c"Success (not an error)",
            Self::Generic => c"Generic error evaluating the query",
            Self::Syntax => c"Parsing/Syntax error for query string",
            Self::ParseArgs => c"Error parsing query/aggregation arguments",
            Self::AddArgs => c"Error parsing document indexing arguments",
            Self::Expr => c"Parsing/Evaluating dynamic expression failed",
            Self::Keyword => c"Could not handle query keyword",
            Self::NoResults => c"Query matches no results",
            Self::BadAttr => c"Attribute not supported for term",
            Self::Inval => c"Could not validate the query nodes (bad attribute?)",
            Self::BuildPlan => c"Could not build plan from query",
            Self::ConstructPipeline => c"Could not construct query pipeline",
            Self::NoReducer => c"Missing reducer",
            Self::ReducerGeneric => c"Generic reducer error",
            Self::AggPlan => c"Could not plan aggregation request",
            Self::CursorAlloc => c"Could not allocate a cursor",
            Self::ReducerInit => c"Could not initialize reducer",
            Self::QString => c"Bad query string",
            Self::NoPropKey => c"Property does not exist in schema",
            Self::NoPropVal => c"Value was not found in result (not a hard error)",
            Self::NoDoc => c"Document does not exist",
            Self::NoOption => c"Invalid option",
            Self::RedisKeyType => c"Invalid Redis key",
            Self::InvalPath => c"Invalid path",
            Self::IndexExists => c"Index already exists",
            Self::BadOption => c"Option not supported for current mode",
            Self::BadOrderOption => c"Path with undefined ordering does not support slop/inorder",
            Self::Limit => c"Limit exceeded",
            Self::NoIndex => c"Index not found",
            Self::DocExists => c"Document already exists",
            Self::DocNotAdded => c"Document was not added because condition was unmet",
            Self::DupField => c"Field was specified twice",
            Self::GeoFormat => cr#"Invalid lon/lat format. Use "lon lat" or "lon,lat""#,
            Self::NoDistribute => c"Could not distribute the operation",
            Self::UnsuppType => c"Unsupported index type",
            Self::NotNumeric => c"Could not convert value to a number",
            Self::TimedOut => c"Timeout limit was reached",
            Self::NoParam => c"Parameter not found",
            Self::DupParam => c"Parameter was specified twice",
            Self::BadVal => c"Invalid value was given",
            Self::NonHybrid => c"Hybrid query attributes were sent for a non-hybrid query",
            Self::HybridNonExist => c"Invalid hybrid policy was given",
            Self::AdhocWithBatchSize => c"'Batch size' is irrelevant for 'ADHOC_BF' policy",
            Self::AdhocWithEfRuntime => c"'EF_RUNTIME' is irrelevant for 'ADHOC_BF' policy",
            Self::NonRange => c"Range query attributes were sent for a non-range query",
            Self::Missing => c"'IsMissing' requires field to be defined with 'INDEXMISSING'",
            Self::Mismatch => c"Index mismatch: Shard index is different than queried index",
            Self::UnknownIndex => c"Unknown index name",
            Self::DroppedBackground => c"The index was dropped before the query could be executed",
            Self::AliasConflict => c"Alias conflicts with an existing index name",
            Self::IndexBgOOMFail => c"Index background scan did not complete due to OOM",
            Self::WeightNotAllowed => c"Weight attributes are not allowed",
            Self::VectorNotAllowed => c"Vector queries are not allowed",
            Self::OutOfMemory => c"Not enough memory available to execute the query",
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
    pub fn is_ok(&self) -> bool {
        self.code.is_ok()
    }

    pub fn code(&self) -> QueryErrorCode {
        self.code
    }

    pub fn set_code(&mut self, code: QueryErrorCode) {
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

    pub fn warnings(&self) -> &Warnings {
        &self.warnings
    }

    pub fn warnings_mut(&mut self) -> &mut Warnings {
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
    pub fn reached_max_prefix_expansions(&self) -> bool {
        self.reached_max_prefix_expansions
    }

    pub fn set_reached_max_prefix_expansions(&mut self) {
        self.reached_max_prefix_expansions = true;
    }

    pub fn out_of_memory(&self) -> bool {
        self.out_of_memory
    }

    pub fn set_out_of_memory(&mut self) {
        self.out_of_memory = true;
    }
}
