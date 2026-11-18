/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! What a target author writes, and the programs it makes.

use std::fmt::Debug;
use std::hash::Hash;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::draw::Draw;
use crate::system::{Notes, Step};

/// A system's ops: which make sense when, what each sets up, how each is
/// sent, and what must hold of a run.
///
/// [`Model::next`] must take every name an op uses, such as a key or a cursor
/// slot, from the state, never compute one. Otherwise an op made again after
/// an earlier op changed names something that is not there.
pub trait Model: Sync {
    type Op: Clone + Debug + Hash + Serialize + DeserializeOwned + Send + Sync;
    type State: Default;
    type Input;
    type Reply;

    /// An op that makes sense in `state`, from the decisions in `draw`.
    fn next(&self, state: &Self::State, draw: &mut Draw) -> Self::Op;

    /// Folds `op` into `state`. `reply` is `None` when predicting, and when
    /// the op's input has no reply.
    fn apply(&self, state: &mut Self::State, op: &Self::Op, reply: Option<&Self::Reply>);

    /// The one input `op` sends, with names resolved against the live
    /// `state`, such as the cursor id the server chose for a slot.
    fn lower(&self, state: &Self::State, op: &Self::Op) -> Self::Input;

    /// Judges a finished run: `steps[k]` belongs to `ops[k]`, and a run that
    /// ended early has fewer steps than ops.
    fn check(&self, ops: &[Self::Op], steps: &[Step<Self::Reply>], notes: &mut Notes) {
        let _ = (ops, steps, notes);
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(
    bound(serialize = "O: Serialize", deserialize = "O: DeserializeOwned"),
    deny_unknown_fields
)]
pub struct Program<O> {
    pub ops: Vec<Made<O>>,
}

/// An op and the draws that made it. A hand-written op has none and is
/// shrunk only by deletion.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(
    bound(serialize = "O: Serialize", deserialize = "O: DeserializeOwned"),
    deny_unknown_fields
)]
pub struct Made<O> {
    pub op: O,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub from: Vec<u64>,
}

impl<O> Default for Program<O> {
    fn default() -> Self {
        Program { ops: Vec::new() }
    }
}

impl<O: Clone> Program<O> {
    pub fn ops(&self) -> Vec<O> {
        self.ops.iter().map(|made| made.op.clone()).collect()
    }
}

/// A program of `len` fresh ops.
pub fn generate<M: Model>(model: &M, draw: &mut Draw, len: usize) -> Program<M::Op> {
    let mut state = M::State::default();
    Program {
        ops: (0..len)
            .map(|_| make(model, &mut state, Draw::new(draw.raw())))
            .collect(),
    }
}

/// Makes an op in `state` and predicts its effect there.
pub(crate) fn make<M: Model>(model: &M, state: &mut M::State, mut draw: Draw) -> Made<M::Op> {
    let op = model.next(state, &mut draw);
    model.apply(state, &op, None);
    Made {
        op,
        from: draw.into_taken(),
    }
}

/// The state `ops` lead to if every op succeeds.
pub(crate) fn predict<M: Model>(model: &M, ops: &[Made<M::Op>]) -> M::State {
    let mut state = M::State::default();
    for made in ops {
        model.apply(&mut state, &made.op, None);
    }
    state
}

/// The program with op `at` made again from `from`, in the state its prefix
/// predicts. The ops after it stay as they are.
pub(crate) fn remake<M: Model>(
    model: &M,
    program: &Program<M::Op>,
    at: usize,
    from: &[u64],
    seed: u64,
) -> Program<M::Op> {
    let mut state = predict(model, &program.ops[..at]);
    let mut remade = program.clone();
    remade.ops[at] = make(model, &mut state, Draw::replaying(from, seed));
    remade
}
