/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Coverage-guided fuzzing of stateful programs against a server process.

pub mod coverage;
pub mod draw;
pub mod edit;
pub mod interrupt;
pub mod legacy;
pub mod model;
pub mod process;
pub mod system;

#[cfg(test)]
mod toy;
