/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

/// cbindgen:prefix-with-name
/// cbindgen:rename-all=ScreamingSnakeCase
#[repr(C)]
#[derive(thiserror::Error, Debug)]
pub enum QueryErrorCode {
    #[error("Generic error evaluating the query")]
    Generic,
    #[error("Parsing/Syntax error for query string")]
    Syntax,
    #[error("Error parsing query/aggregation arguments")]
    ParseArgs,
    #[error("Error parsing document indexing arguments")]
    AddArgs,
    #[error("Parsing/Evaluating dynamic expression failed")]
    Expr,
    #[error("Could not handle query keyword")]
    Keyword,
    #[error("Query matches no results")]
    NoResults,
    #[error("Attribute not supported for term")]
    BadAttr,
    #[error("Could not validate the query nodes (bad attribute?)")]
    Inval,
    #[error("Could not build plan from query")]
    BuildPlan,
    #[error("Could not construct query pipeline")]
    ConstructPipeline,
    #[error("Missing reducer")]
    NoReducer,
    #[error("Generic reducer error")]
    ReducerGeneric,
    #[error("Could not plan aggregation request")]
    AggPlan,
    #[error("Could not allocate a cursor")]
    CursorAlloc,
    #[error("Could not initialize reducer")]
    ReducerInit,
    #[error("Bad query string")]
    QString,
    #[error("Property does not exist in schema")]
    NoPropKey,
    #[error("Value was not found in result (not a hard error)")]
    NoPropVal,
    #[error("Document does not exist")]
    NoDoc,
    #[error("Invalid option")]
    NoOption,
    #[error("Invalid Redis key")]
    RedisKeyType,
    #[error("Invalid path")]
    InvalPath,
    #[error("Index already exists")]
    IndexExists,
    #[error("Option not supported for current mode")]
    BadOption,
    #[error("Path with undefined ordering does not support slop/inorder")]
    BadOrderOption,
    #[error("Limit exceeded")]
    Limit,
    #[error("Index not found")]
    NoIndex,
    #[error("Document already exists")]
    DocExists,
    #[error("Document was not added because condition was unmet")]
    DocNotAdded,
    #[error("Field was specified twice")]
    DupField,
    #[error(r#"Invalid lon/lat format. Use "lon lat" or "lon,lat""#)]
    GeoFormat,
    #[error("Could not distribute the operation")]
    NoDistribute,
    #[error("Unsupported index type")]
    UnsuppType,
    #[error("Could not convert value to a number")]
    NotNumeric,
    #[error("Timeout limit was reached")]
    TimedOut,
    #[error("Parameter not found")]
    NoParam,
    #[error("Parameter was specified twice")]
    DupParam,
    #[error("Invalid value was given")]
    BadVal,
    #[error("Hybrid query attributes were sent for a non-hybrid query")]
    NonHybrid,
    #[error("Invalid hybrid policy was given")]
    HybridNonExist,
    #[error("'Batch size' is irrelevant for 'ADHOC_BF' policy")]
    AdhocWithBatchSize,
    #[error("'EF_RUNTIME' is irrelevant for 'ADHOC_BF' policy")]
    AdhocWithEfRuntime,
    #[error("Range query attributes were sent for a non-range query")]
    NonRange,
    #[error("'IsMissing' requires field to be defined with 'INDEXMISSING'")]
    Missing,
    #[error("Index mismatch: Shard index is different than queried index")]
    MissMatch,
    #[error("Unknown index name")]
    UnknownIndex,
    #[error("The index was dropped before the query could be executed")]
    DroppedBackground,
    #[error("Alias conflicts with an existing index name")]
    AliasConflict,
    #[error("Index background scan did not complete due to OOM")]
    IndexBgOOMFail,
    #[error("Weight attributes are not allowed")]
    WeightNotAllowed,
    #[error("Vector queries are not allowed")]
    VectorNotAllowed,
    #[error("Not enough memory available to execute the query")]
    OutOfMemory,
}

#[repr(C)]
pub struct QueryError {
    code: QueryErrorCode,
    message: Option<String>,
    detail: Option<String>,

    reached_max_prefix_expansions: bool,
}

mimic::impl_mimic!(QueryError);
