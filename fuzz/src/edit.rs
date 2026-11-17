/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! One edit set for both directions: [`mutate`] applies random edits, and
//! [`smaller`] lists the edits that strictly shrink a program.

use std::ops::RangeInclusive;

use rand::Rng;
use rand::seq::{IndexedRandom, IteratorRandom};

use crate::generate::{Pools, conn, op, related};
use crate::lower::lower;
use crate::program::{Argv, Leaf, Op, OpKind, Program, Query, ReplyMode};
use crate::scope::Scope;

const STACKED_EDITS: RangeInclusive<usize> = 1..=3;
const SPLICE_MOST: usize = 8;
const DELETE_MOST: usize = 4;
const SYNTAX: &[u8] = b"@:{}[]()|-~*%\"\\$,.; ";

/// Applies a few random edits. `donor` is another program to splice from.
pub fn mutate(program: &Program, donor: &Program, rng: &mut impl Rng) -> Program {
    let pools = Pools::of(&[program, donor]);
    let mut out = program.clone();
    for _ in 0..rng.random_range(STACKED_EDITS) {
        edit(&mut out, donor, &pools, rng);
    }
    out
}

fn edit(program: &mut Program, donor: &Program, pools: &Pools, rng: &mut impl Rng) {
    let module_args = &program.module_args;
    let scope = |ops: &[Op]| Scope::of(module_args, ops);
    let ops = &mut program.ops;
    let at = rng.random_range(0..=ops.len());
    match rng.random_range(0..11) {
        0..=2 => ops.insert(at, op(&scope(&ops[..at]), pools, rng)),
        3 => {
            let group = related(&scope(&ops[..at]), pools, rng);
            ops.splice(at..at, group);
        }
        4 if !donor.ops.is_empty() => {
            let start = rng.random_range(0..donor.ops.len());
            let end = rng.random_range(start + 1..=donor.ops.len().min(start + SPLICE_MOST));
            ops.splice(at..at, donor.ops[start..end].iter().cloned());
        }
        _ if at == ops.len() => {}
        5 => {
            let end = rng.random_range(at + 1..=ops.len().min(at + DELETE_MOST));
            ops.drain(at..end);
        }
        // Lower a typed op into the commands it sends, exposing them to token
        // edits.
        6 => {
            let op = ops.remove(at);
            let lowered = lower(&op.kind).into_iter().map(|argv| Op {
                kind: OpKind::Command { argv },
                ..op.clone()
            });
            ops.splice(at..at, lowered);
        }
        7 => {
            ops[at].conn = conn(rng);
            ops[at].reply = match ops[at].reply {
                ReplyMode::Await => ReplyMode::Defer,
                ReplyMode::Defer => ReplyMode::Await,
            };
        }
        8 => {
            let kind = &mut ops[at].kind;
            if let Some(leaf) = (0..leaf_count(kind)).choose(rng) {
                with_leaf(kind, leaf, |leaf| match leaf {
                    Leaf::Word(word) => *word = pools.word(rng),
                    Leaf::Int(n) => *n = pools.num(rng),
                });
            }
        }
        _ => {
            if let OpKind::Command { argv } = &mut ops[at].kind {
                edit_tokens(argv, pools, rng);
            }
        }
    }
}

fn edit_tokens(argv: &mut Argv, pools: &Pools, rng: &mut impl Rng) {
    let i = rng.random_range(0..argv.len());
    match rng.random_range(0..5) {
        0 if argv.len() > 1 => {
            argv.remove(i);
        }
        1 => argv.insert(i, argv[i].clone()),
        2 => {
            let j = rng.random_range(0..argv.len());
            argv.swap(i, j);
        }
        3 => argv[i] = pools.word(rng),
        _ => {
            let token = &mut argv[i].0;
            let at = rng.random_range(0..=token.len());
            match rng.random_range(0..3) {
                0 if at < token.len() => {
                    token.remove(at);
                }
                1 => {
                    token.splice(at..at, pools.word(rng).0);
                }
                _ => token.insert(at, *SYNTAX.choose(rng).expect("never empty")),
            }
        }
    }
}

/// Every program one shrinking step away, largest step first. Each is
/// strictly smaller by [`size`].
pub fn smaller(program: &Program) -> Vec<Program> {
    let mut out = Vec::new();
    let with = |f: &dyn Fn(&mut Program)| {
        let mut candidate = program.clone();
        f(&mut candidate);
        candidate
    };
    if !program.module_args.is_empty() {
        out.push(with(&|p| p.module_args.clear()));
    }
    let n = program.ops.len();
    let mut chunk = n;
    while chunk > 0 {
        for start in (0..n).step_by(chunk) {
            out.push(with(&|p| {
                p.ops.drain(start..(start + chunk).min(n));
            }));
        }
        chunk /= 2;
    }
    for (at, op) in program.ops.iter().enumerate() {
        for kind in simpler(&op.kind) {
            out.push(with(&|p| p.ops[at].kind = kind.clone()));
        }
    }
    out
}

/// The op one step simpler: a field, query node, or token fewer, half the
/// documents, or a shorter word or number.
fn simpler(kind: &OpKind) -> Vec<OpKind> {
    let mut out: Vec<OpKind> = match kind {
        OpKind::Create { fields, .. } => without_each(fields)
            .map(|fields| {
                let mut kind = kind.clone();
                if let OpKind::Create { fields: f, .. } = &mut kind {
                    *f = fields;
                }
                kind
            })
            .collect(),
        OpKind::Populate { fields, n, .. } => {
            let halved = (*n > 1).then(|| {
                let mut kind = kind.clone();
                if let OpKind::Populate { n, .. } = &mut kind {
                    *n /= 2;
                }
                kind
            });
            halved
                .into_iter()
                .chain(without_each(fields).map(|fields| {
                    let mut kind = kind.clone();
                    if let OpKind::Populate { fields: f, .. } = &mut kind {
                        *f = fields;
                    }
                    kind
                }))
                .collect()
        }
        OpKind::Query { query, .. } => hoists(query)
            .into_iter()
            .map(|query| {
                let mut kind = kind.clone();
                if let OpKind::Query { query: q, .. } = &mut kind {
                    *q = query;
                }
                kind
            })
            .collect(),
        OpKind::Command { argv } if argv.len() > 1 => without_each(argv)
            .map(|argv| OpKind::Command { argv })
            .collect(),
        _ => Vec::new(),
    };
    for leaf in 0..leaf_count(kind) {
        for to_zero in [true, false] {
            let mut shrunk = kind.clone();
            let mut changed = false;
            with_leaf(&mut shrunk, leaf, |leaf| match leaf {
                Leaf::Word(word) => {
                    let len = if to_zero { 0 } else { word.0.len() / 2 };
                    changed = len < word.0.len() && (to_zero || len > 0);
                    word.0.truncate(len);
                }
                Leaf::Int(n) => {
                    let half = if to_zero { 0 } else { *n / 2 };
                    changed = half != *n && (to_zero || half != 0);
                    *n = half;
                }
            });
            if changed {
                out.push(shrunk);
            }
        }
    }
    out
}

fn without_each<T: Clone>(items: &[T]) -> impl Iterator<Item = Vec<T>> + '_ {
    (0..items.len()).map(|i| {
        let mut rest = items.to_vec();
        rest.remove(i);
        rest
    })
}

/// The query with one node replaced by one of its children, or one child of
/// a group dropped, anywhere in the tree.
fn hoists(query: &Query) -> Vec<Query> {
    match query {
        Query::And(children) | Query::Or(children) => {
            let rebuild = |children| match query {
                Query::And(_) => Query::And(children),
                _ => Query::Or(children),
            };
            let mut out = Vec::new();
            for (i, child) in children.iter().enumerate() {
                out.push(child.clone());
                out.extend(without_each(children).nth(i).map(rebuild));
                for simpler in hoists(child) {
                    let mut children = children.clone();
                    children[i] = simpler;
                    out.push(rebuild(children));
                }
            }
            out
        }
        Query::Not(child) | Query::Knn { filter: child, .. } => {
            let mut out = vec![(**child).clone()];
            out.extend(hoists(child).into_iter().map(|simpler| {
                let mut query = query.clone();
                if let Query::Not(c) | Query::Knn { filter: c, .. } = &mut query {
                    **c = simpler;
                }
                query
            }));
            out
        }
        _ => Vec::new(),
    }
}

/// The measure [`smaller`] decreases: ops, module arguments, schema fields,
/// query nodes, populated documents, and the length of every word and number.
pub fn size(program: &Program) -> usize {
    let mut total = program.ops.len() + program.module_args.len();
    for op in &program.ops {
        total += match &op.kind {
            OpKind::Create { fields, .. } => fields.len(),
            OpKind::Populate { fields, n, .. } => fields.len() + usize::try_from(*n).expect("fits"),
            OpKind::Query { query, .. } => nodes(query),
            _ => 0,
        };
        op.kind.clone().leaves(&mut |leaf| {
            total += 1 + match leaf {
                Leaf::Word(word) => word.0.len(),
                Leaf::Int(n) => {
                    usize::try_from(64 - n.unsigned_abs().leading_zeros()).expect("at most 64")
                }
            }
        });
    }
    total
}

fn nodes(query: &Query) -> usize {
    1 + match query {
        Query::And(children) | Query::Or(children) => children.iter().map(nodes).sum(),
        Query::Not(child) | Query::Knn { filter: child, .. } => nodes(child),
        _ => 0,
    }
}

fn leaf_count(kind: &OpKind) -> usize {
    let mut count = 0;
    kind.clone().leaves(&mut |_| count += 1);
    count
}

fn with_leaf(kind: &mut OpKind, leaf: usize, mut f: impl FnMut(Leaf<'_>)) {
    let mut i = 0;
    kind.leaves(&mut |l| {
        if i == leaf {
            f(l);
        }
        i += 1;
    });
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng;
    use rand::rngs::SmallRng;

    use super::*;
    use crate::generate::generate;

    const PROGRAMS: u64 = 300;

    /// Mutation never produces a program the format cannot carry, and every
    /// shrinking step is strictly smaller, so reduction terminates.
    #[test]
    fn edits_keep_programs_serializable_and_shrinking_strict() {
        let mut rng = SmallRng::seed_from_u64(0);
        let mut program = generate(&mut rng);
        for i in 0..PROGRAMS {
            let donor = generate(&mut rng);
            program = mutate(&program, &donor, &mut rng);
            assert_eq!(Program::from_json(&program.to_json()).unwrap(), program);
            if i % 10 == 0 {
                let fresh = mutate(&generate(&mut rng), &donor, &mut rng);
                for candidate in smaller(&fresh) {
                    assert!(size(&candidate) < size(&fresh), "{candidate:?}");
                }
            }
        }
    }
}
