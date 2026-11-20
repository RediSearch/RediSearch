/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! RediSearch as a procfuzz [`Model`]: its ops ([`op`]), what they set up
//! ([`state`]), which make sense when ([`generate`]), what each command does
//! ([`effect`]), the logic rules over search results ([`logic`]), the server
//! ([`server`]), programs from command scripts ([`script`]), and findings
//! as Python scripts ([`repro`]).

pub mod effect;
pub mod generate;
pub mod logic;
pub mod op;
pub mod repro;
pub mod script;
pub mod server;
pub mod state;

use procfuzz::draw::Draw;
use procfuzz::model::Model;
use procfuzz::service::Input;
use procfuzz::system::{Notes, Step};
use procfuzz_redis::resp::{Argv, Value};

use crate::op::Op;
use crate::state::State;

pub type Program = procfuzz::model::Program<Op>;

pub struct RediSearch;

impl Model for RediSearch {
    type Op = Op;
    type State = State;
    type Input = Input<Argv>;
    type Reply = Value;

    fn next(&self, state: &State, draw: &mut Draw) -> Op {
        generate::next(state, draw)
    }

    fn apply(&self, state: &mut State, op: &Op, reply: Option<&Value>) {
        state.apply(op, reply);
    }

    fn lower(&self, state: &State, op: &Op) -> Input<Argv> {
        op.lower(state)
    }

    fn check(&self, ops: &[Op], steps: &[Step<Value>], notes: &mut Notes) {
        logic::check(self, ops, steps, notes);
    }

    fn reproduction(
        &self,
        ops: &[Op],
        steps: &[Step<Value>],
        finding: &str,
    ) -> Option<(&'static str, String)> {
        Some(("repro.py", repro::script(self, ops, steps, finding)))
    }
}
