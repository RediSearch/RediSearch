/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! An in-memory key-value store with a planted wrong answer, and its model.

use std::collections::BTreeMap;
use std::io;
use std::ops::ControlFlow;

use serde::{Deserialize, Serialize};

use crate::draw::Draw;
use crate::model::Model;
use crate::system::{Notes, Run, Session, Step};

const KEYS: u64 = 4;
const VALUES: u64 = 3;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Op {
    Set { key: u8, val: u8 },
    Get { key: u8 },
    Del { key: u8 },
}

pub struct Toy;

impl Model for Toy {
    type Op = Op;
    type State = BTreeMap<u8, u8>;
    type Input = Op;
    type Reply = Option<u8>;

    fn next(&self, state: &Self::State, draw: &mut Draw) -> Op {
        let known: Vec<u8> = state.keys().copied().collect();
        let key = |draw: &mut Draw| match draw.below(2) {
            0 => small(draw.below(KEYS)),
            _ => draw.pick(&known).copied().unwrap_or(0),
        };
        match draw.below(3) {
            0 => Op::Set {
                key: key(draw),
                val: small(draw.below(VALUES)),
            },
            1 => Op::Get { key: key(draw) },
            _ => Op::Del { key: key(draw) },
        }
    }

    fn apply(&self, state: &mut Self::State, op: &Op, _: Option<&Option<u8>>) {
        match *op {
            Op::Set { key, val } => {
                state.insert(key, val);
            }
            Op::Get { .. } => {}
            Op::Del { key } => {
                state.remove(&key);
            }
        }
    }

    fn lower(&self, _: &Self::State, op: &Op) -> Op {
        op.clone()
    }

    fn check(&self, ops: &[Op], steps: &[Step<Option<u8>>], notes: &mut Notes) {
        let mut state = Self::State::default();
        for (op, step) in ops.iter().zip(steps) {
            if let (Op::Get { key }, Some(got)) = (op, &step.reply)
                && *got != state.get(key).copied()
            {
                notes.fail("logic", "get_last_set");
            }
            self.apply(&mut state, op, step.reply.as_ref());
        }
    }
}

fn small(value: u64) -> u8 {
    u8::try_from(value).expect("small")
}

/// Ignores an overwrite once it holds three keys.
#[derive(Default)]
pub struct Store(BTreeMap<u8, u8>);

impl Session for Store {
    type Input = Op;
    type Reply = Option<u8>;

    fn step(
        &mut self,
        at: usize,
        op: &Op,
        run: &mut Run<Option<u8>>,
    ) -> io::Result<ControlFlow<()>> {
        let reply = match *op {
            Op::Set { key, val } => {
                if !(self.0.len() >= 3 && self.0.contains_key(&key)) {
                    self.0.insert(key, val);
                }
                None
            }
            Op::Get { key } => self.0.get(&key).copied(),
            Op::Del { key } => self.0.remove(&key),
        };
        run.done(at, Some(reply));
        Ok(ControlFlow::Continue(()))
    }

    fn stop(self, _: &mut Run<Option<u8>>) -> io::Result<()> {
        Ok(())
    }
}

pub fn start() -> io::Result<Store> {
    Ok(Store::default())
}
