/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! FFI shim for [`query_node_type::QueryNodeType`].
//!
//! This crate exists solely so that cbindgen can generate `query_node_type.h`,
//! a standalone C header with no transitive includes. See the
//! [`query_node_type`] crate-level docs for the rationale.

// Re-export so cbindgen picks it up.
pub use query_node_type::QueryNodeType;
