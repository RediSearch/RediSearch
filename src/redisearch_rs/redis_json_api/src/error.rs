/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Error types for RedisJSON API operations.

use crate::JsonType;

/// A specialized Result type for RedisJSON API operations.
pub type Result<T> = std::result::Result<T, JsonApiError>;

/// Errors that can occur when using the RedisJSON API.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum JsonApiError {
    /// The RedisJSON module is not loaded.
    #[error("RedisJSON module is not loaded")]
    ModuleNotLoaded,

    /// The API version is not supported.
    #[error("unsupported RedisJSON API version: {version} (minimum: {minimum})")]
    UnsupportedVersion {
        /// The actual API version.
        version: i32,
        /// The minimum required version.
        minimum: i32,
    },

    /// A type mismatch occurred.
    #[error("type mismatch: expected {expected:?}, got {actual:?}")]
    TypeMismatch {
        /// The expected JSON type.
        expected: JsonType,
        /// The actual JSON type.
        actual: JsonType,
    },

    /// The operation failed for an unspecified reason.
    #[error("operation failed")]
    OperationFailed,

    /// Index out of bounds for array access.
    #[error("index {index} out of bounds for array of length {length}")]
    IndexOutOfBounds {
        /// The requested index.
        index: usize,
        /// The array length.
        length: usize,
    },

    /// The JSON value is not an object.
    #[error("value is not an object")]
    NotAnObject,

    /// The JSON value is not an array.
    #[error("value is not an array")]
    NotAnArray,

    /// Path parsing failed.
    #[error("failed to parse JSON path")]
    PathParseError,

    /// The key does not exist or is not JSON.
    #[error("key does not exist or is not a JSON type")]
    KeyNotFound,
}
