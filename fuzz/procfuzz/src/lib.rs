/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Coverage-guided fuzzing of stateful systems.
//!
//! A target implements [`model::Model`]: which ops make sense when, what
//! each sets up, and the input each sends. [`system::run`] runs a program of
//! such ops against a [`system::System`], such as a [`service::Service`],
//! and [`edit`] mutates and shrinks programs through the [`draw::Draw`]s that
//! made their ops. [`campaign`] searches for programs that reach new
//! features, and [`commands`] puts it all together as a clap subcommand:
//! replay, reduce, fuzz, and generate.

pub mod campaign;
pub mod commands;
mod coverage;
pub mod draw;
pub mod edit;
pub mod findings;
pub mod interrupt;
pub mod model;
mod process;
pub mod sanitizer;
pub mod service;
pub mod system;
