/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The forking GC policy: a child process walks the index and streams deltas
//! over a pipe, the parent applies them as they arrive.

pub mod frame;
pub mod io_result_ext;
pub mod orchestration;
pub mod pipe;
pub mod wire;
