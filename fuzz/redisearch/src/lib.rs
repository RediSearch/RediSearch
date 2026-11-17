/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! RediSearch as a `procfuzz` target: its own ops ([`action`]), a generator
//! that keeps them well-typed ([`generate`], [`scope`]), the server
//! ([`redis`]), and logic rules over search results ([`logic`]).

pub mod action;
pub mod generate;
pub mod logic;
pub mod redis;
pub mod scope;

use rand::rngs::SmallRng;

use crate::action::{Action, vocabulary};
use crate::redis::{Env, Value};

pub type Program = procfuzz::program::Program<Action>;
pub type Op = procfuzz::program::Op<Action>;
pub type Execution = procfuzz::run::Execution<Value>;

pub fn execute(env: &Env, program: &Program) -> Result<Execution, String> {
    let mut execution = procfuzz::run::execute(env, program)?;
    execution
        .findings
        .extend(logic::check(program, &execution.trace));
    execution.findings.sort();
    execution.findings.dedup();
    Ok(execution)
}

pub fn mutate(program: &Program, donor: &Program, rng: &mut SmallRng) -> Program {
    let mut words = vocabulary();
    words.learn(program);
    words.learn(donor);
    let mut generate =
        |args: &[_], prefix: &[Op], rng: &mut SmallRng| generate::ops(args, prefix, &words, rng);
    procfuzz::edit::mutate(program, donor, &words, &mut generate, rng)
}
