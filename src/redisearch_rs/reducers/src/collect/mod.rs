/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! COLLECT reducer variants: remote (field collection) and local (merge).
//!
//! C parses the reducer arguments and constructs the appropriate variant via
//! `reducers_ffi`.
//!
//! ## Terminology: `Local` vs `Remote`
//!
//! Naming follows the C planner's `ReducerOptions::is_local` /
//! `PLN_GroupStep`: the names tell *which side of the distributed `GROUPBY`
//! split* the reducer runs on, not the cluster topology. Read "local" as
//! "local to the merge step", not "the local node". See [`local`] and
//! [`remote`] for per-variant details.

pub(crate) mod common;
pub mod local;
pub mod remote;

pub use local::{LocalCollectCtx, LocalCollectReducer};
pub use remote::{RemoteCollectCtx, RemoteCollectReducer};
