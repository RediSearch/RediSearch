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
const EDITS: [(Edit, u64); 5] = [
    (Edit::Insert, 4),
    (Edit::Redraw, 3),
    (Edit::Delete, 1),
    (Edit::Splice, 1),
    (Edit::Append, 1),
];
const MOST_INSERTED: u64 = 3;
const MOST_SPLICED: usize = 4;

#[derive(Clone, Copy)]
enum Edit {
    /// Fresh ops anywhere, made in the state the ops before them predict.
    Insert,
    /// One op made again with one of its draws replaced and the rest fresh.
    Redraw,
    Delete,
    /// The donor's ops from somewhere on, after the program's own.
    Splice,
    /// A fresh op at the end.
    Append,
}

/// The parent with one edit, splicing from `donor`. An edit that cannot
/// apply, such as deleting the only op, appends instead.
pub fn mutate<M: Model>(
    model: &M,
    parent: &Program<M::Op>,
    donor: &Program<M::Op>,
    rng: &mut Draw,
) -> Program<M::Op> {
    let mut child = parent.clone();
    let len = child.ops.len();
    match pick_edit(rng) {
        Edit::Insert => {
            let at = rng.index(len + 1);
            let mut state = predict(model, &child.ops[..at]);
            let fresh: Vec<_> = (0..=rng.below(MOST_INSERTED))
                .map(|_| make(model, &mut state, Draw::new(rng.raw())))
                .collect();
            child.ops.splice(at..at, fresh);
        }
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
            child
                .ops
                .extend(donor.ops[start..].iter().take(MOST_SPLICED).cloned());
        }
        _ => {
            let mut state = predict(model, &child.ops);
            child
                .ops
                .push(make(model, &mut state, Draw::new(rng.raw())));
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
/// makes takes exactly the lowered draws, so that no fresh draw sneaks in.
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
                if candidate.ops[at].from == from && fails(&candidate) {
                    *best = candidate;
                    improved = true;
                    break;
                }
            }
        }
    }
    improved
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Made, generate};
    use crate::system::run;
    use crate::toy::{Op, Toy, start};

    fn fails(program: &Program<Op>) -> bool {
        let run = run(&Toy, &start, program).expect("runs");
        assert_eq!(run.steps.len(), program.ops.len());
        run.notes
            .findings
            .iter()
            .any(|f| f.to_string() == "logic:get_last_set")
    }

    #[test]
    fn a_found_failure_shrinks_to_the_smallest_program_that_shows_it() {
        let mut draw = Draw::new(1);
        let failing = std::iter::repeat_with(|| generate(&Toy, &mut draw, 30))
            .find(fails)
            .expect("a random program finds the planted bug");
        let smallest = shrink(&Toy, &failing, fails);
        let ops: Vec<Op> = smallest.ops();
        assert_eq!(ops.len(), 5, "{ops:?}");
        assert!(matches!(ops[4], Op::Get { .. }), "{ops:?}");
    }

    #[test]
    fn mutants_are_valid_and_ops_remake_identically_from_their_draws() {
        let mut rng = Draw::new(2);
        let mut corpus = vec![generate(&Toy, &mut rng, 10)];
        for _ in 0..500 {
            let parent = &corpus[rng.index(corpus.len())];
            let donor = &corpus[rng.index(corpus.len())];
            let child = mutate(&Toy, parent, donor, &mut rng);
            assert!(!child.ops.is_empty());
            corpus.push(child);
        }
        let program = generate(&Toy, &mut rng, 50);
        for (at, Made { from, .. }) in program.ops.iter().enumerate() {
            assert_eq!(remake(&Toy, &program, at, from, 0), program);
        }
    }
}
