/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Coverage-guided fuzzing of stateful programs against a server process.
//!
//! A [`Program`](program::Program) is a sequence of ops sent over several
//! connections, each awaiting its reply or leaving it for later. Ops are raw
//! commands, barriers, or the target's own [`TypedOp`](program::TypedOp)s,
//! which lower to commands. A target supplies its typed ops, a
//! [`Server`](server::Server) that says how to start and talk to the server,
//! and a generator; this crate supplies the rest:
//!
//! - [`run::execute`] runs a program against a fresh server with edge
//!   coverage ([`coverage`], `shim/coverage.c`) and judges the run
//!   ([`judge`]).
//! - [`edit`] mutates programs, lists smaller ones, and reduces a failing one.
//! - [`campaign::fuzz`] searches for programs that reach new edges.
//! - [`findings::Findings`] keeps one directory per finding.
//! - [`session`] composes these into replaying, reducing, and fuzzing.

pub mod campaign;
pub mod coverage;
pub mod edit;
pub mod findings;
pub mod judge;
pub mod process;
pub mod program;
pub mod run;
pub mod server;
pub mod session;
pub mod vocabulary;
