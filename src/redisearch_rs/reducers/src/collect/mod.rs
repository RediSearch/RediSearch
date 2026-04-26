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

pub(crate) mod common;
pub mod local;
pub mod remote;

pub use local::{LocalCollectCtx, LocalCollectReducer};
pub use remote::{RemoteCollectCtx, RemoteCollectReducer};
