/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Mutating and shrinking programs through the draws that made their ops, so
//! a model needs no mutator or shrinker of its own.

use crate::draw::Draw;
use crate::model::{Model, Program, make, predict, remake};

/// Each edit with its relative weight.
const EDITS: [(Edit, u64); 4] = [
    (Edit::Insert, 5),
    (Edit::Redraw, 3),
    (Edit::Delete, 1),
    (Edit::Splice, 1),
];
const MOST_INSERTED: u64 = 3;
const MOST_SPLICED: usize = 4;
/// A program this long only shrinks, so that it stays fast to run.
const MOST_OPS: usize = 64;

#[derive(Clone, Copy)]
enum Edit {
    /// Fresh ops anywhere, made in the state the ops before them predict.
    Insert,
    /// One op made again with one of its draws replaced and the rest fresh.
    Redraw,
    Delete,
    /// The donor's ops from somewhere on, after the program's own.
    Splice,
}

/// The parent with one edit, splicing from `donor`. An edit that cannot
/// apply, such as deleting the only op, inserts instead; one that would grow
/// a program of [`MOST_OPS`] deletes instead.
pub fn mutate<M: Model>(
    model: &M,
    parent: &Program<M::Op>,
    donor: &Program<M::Op>,
    rng: &mut Draw,
) -> Program<M::Op> {
    let mut child = parent.clone();
    let len = child.ops.len();
    let edit = match pick_edit(rng) {
        Edit::Insert | Edit::Splice if len >= MOST_OPS => Edit::Delete,
        edit => edit,
    };
    match edit {
        Edit::Redraw if len > 0 => {
            let at = rng.index(len);
            let mut from = child.ops[at].from.clone();
            if from.is_empty() {
                from.push(0);
            }
            let replaced = rng.index(from.len());
            from[replaced] = rng.raw();
            from.truncate(replaced + 1);
            child = remake(model, &child, at, &from, rng.raw());
        }
        Edit::Delete if len > 1 => {
            child.ops.remove(rng.index(len));
        }
        Edit::Splice if !donor.ops.is_empty() => {
            let start = rng.index(donor.ops.len());
            child.ops.extend(
                donor.ops[start..]
                    .iter()
                    .take(MOST_SPLICED.min(MOST_OPS - len))
                    .cloned(),
            );
        }
        _ => {
            let at = rng.index(len + 1);
            let mut state = predict(model, &child.ops[..at]);
            let fresh: Vec<_> = (0..=rng.below(MOST_INSERTED))
                .map(|_| make(model, &mut state, Draw::new(rng.raw())))
                .collect();
            child.ops.splice(at..at, fresh);
        }
    }
    child
}

fn pick_edit(rng: &mut Draw) -> Edit {
    let total: u64 = EDITS.iter().map(|&(_, weight)| weight).sum();
    let mut left = rng.below(total);
    for (edit, weight) in EDITS {
        if left < weight {
            return edit;
        }
        left -= weight;
    }
    unreachable!("the weights sum to the total")
}

/// A smaller program for which `fails` still holds: first with runs of ops
/// deleted, then with draws lowered toward 0, until neither helps.
pub fn shrink<M: Model>(
    model: &M,
    program: &Program<M::Op>,
    mut fails: impl FnMut(&Program<M::Op>) -> bool,
) -> Program<M::Op> {
    let mut best = program.clone();
    loop {
        let deleted = delete_ops(&mut best, &mut fails);
        let lowered = lower_draws(model, &mut best, &mut fails);
        if !deleted && !lowered {
            return best;
        }
    }
}

/// Deletes runs of ops, halving the run length down to single ops.
fn delete_ops<O: Clone>(
    best: &mut Program<O>,
    fails: &mut impl FnMut(&Program<O>) -> bool,
) -> bool {
    let mut improved = false;
    let mut chunk = best.ops.len();
    while chunk > 0 {
        let mut at = 0;
        while at < best.ops.len() {
            let mut fewer = best.clone();
            fewer.ops.drain(at..(at + chunk).min(best.ops.len()));
            if fails(&fewer) {
                *best = fewer;
                improved = true;
            } else {
                at += chunk;
            }
        }
        chunk /= 2;
    }
    improved
}

/// Lowers each draw to 0, else to half. A candidate counts only if the op it
/// makes needs no draw beyond the record, so that no fresh draw sneaks in; it
/// may need fewer, as when an optional part is dropped. Either way its record
/// is shorter or, draw by draw, no greater and once smaller, so shrinking
/// terminates.
fn lower_draws<M: Model>(
    model: &M,
    best: &mut Program<M::Op>,
    fails: &mut impl FnMut(&Program<M::Op>) -> bool,
) -> bool {
    let mut improved = false;
    for at in 0..best.ops.len() {
        for draw in 0..best.ops[at].from.len() {
            let value = best.ops[at].from[draw];
            for lower in [0, value / 2] {
                if lower >= value {
                    continue;
                }
                let mut from = best.ops[at].from.clone();
                from[draw] = lower;
                let candidate = remake(model, best, at, &from, 0);
                if candidate.ops[at].from.len() <= from.len() && fails(&candidate) {
                    *best = candidate;
                    improved = true;
                    break;
                }
            }
        }
    }
    improved
}
