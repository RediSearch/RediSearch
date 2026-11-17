/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Edits in both directions: [`mutate`] makes random ones, and [`smaller`]
//! lists the ones that strictly shrink a program.

use std::ops::RangeInclusive;

use rand::Rng;
use rand::rngs::SmallRng;
use rand::seq::{IndexedRandom, IteratorRandom};

use crate::program::{Argv, Bytes, Conn, Kind, Literal, Op, Program, ReplyMode, TypedOp};
use crate::vocabulary::Vocabulary;

const EDITS: RangeInclusive<usize> = 1..=3;
const MOST_SPLICED: usize = 8;
const MOST_DELETED: usize = 4;
const SYNTAX: &[u8] = b"@:{}[]()|-~*%\"\\$,.; ";

/// Ops a target generates to follow the server arguments and ops given.
pub type Generate<'a, T> = dyn FnMut(&[Bytes], &[Op<T>], &mut SmallRng) -> Vec<Op<T>> + 'a;

/// `program` with a few random edits: generated ops inserted, ops spliced
/// from `donor`, deleted, lowered to the commands they send, moved to
/// another connection, or with a literal or command token changed.
pub fn mutate<T: TypedOp>(
    program: &Program<T>,
    donor: &Program<T>,
    vocabulary: &Vocabulary,
    generate: &mut Generate<'_, T>,
    rng: &mut SmallRng,
) -> Program<T> {
    let mut out = program.clone();
    for _ in 0..rng.random_range(EDITS) {
        let ops = &mut out.ops;
        let at = rng.random_range(0..=ops.len());
        match rng.random_range(0..11) {
            0..=3 => {
                let generated = generate(&out.args, &ops[..at], rng);
                ops.splice(at..at, generated);
            }
            4 if !donor.ops.is_empty() => {
                let start = rng.random_range(0..donor.ops.len());
                let end = rng.random_range(start + 1..=donor.ops.len().min(start + MOST_SPLICED));
                ops.splice(at..at, donor.ops[start..end].iter().cloned());
            }
            _ if at == ops.len() => {}
            5 => {
                let end = rng.random_range(at + 1..=ops.len().min(at + MOST_DELETED));
                ops.drain(at..end);
            }
            // Typed ops send well-formed commands; their lowered form can be
            // edited into malformed ones.
            6 => {
                let op = ops.remove(at);
                let commands = op.kind.lower().into_iter().map(|argv| Op {
                    kind: Kind::Command(argv),
                    ..op.clone()
                });
                ops.splice(at..at, commands);
            }
            7 => {
                ops[at].conn = Conn::random(rng);
                ops[at].reply = match ops[at].reply {
                    ReplyMode::Await => ReplyMode::Defer,
                    ReplyMode::Defer => ReplyMode::Await,
                };
            }
            8 => match ops[at].kind.literals().into_iter().choose(rng) {
                Some(Literal::Word(word)) => *word = vocabulary.word(rng),
                Some(Literal::Number(n)) => *n = vocabulary.number(rng),
                None => {}
            },
            _ => {
                if let Kind::Command(argv) = &mut ops[at].kind {
                    edit_tokens(argv, vocabulary, rng);
                }
            }
        }
    }
    out
}

fn edit_tokens(argv: &mut Argv, vocabulary: &Vocabulary, rng: &mut SmallRng) {
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
        3 => argv[i] = vocabulary.word(rng),
        _ => {
            let token = &mut argv[i].0;
            let at = rng.random_range(0..=token.len());
            match rng.random_range(0..3) {
                0 if at < token.len() => {
                    token.remove(at);
                }
                1 => {
                    token.splice(at..at, vocabulary.word(rng).0);
                }
                _ => token.insert(at, *SYNTAX.choose(rng).expect("never empty")),
            }
        }
    }
}

/// Every program one shrinking step away, largest steps first, each strictly
/// smaller by [`Program::size`].
pub fn smaller<T: TypedOp>(program: &Program<T>) -> Vec<Program<T>> {
    let edited = |edit: &dyn Fn(&mut Program<T>)| {
        let mut candidate = program.clone();
        edit(&mut candidate);
        candidate
    };
    let mut out = Vec::new();
    if !program.args.is_empty() {
        out.push(edited(&|p| p.args.clear()));
    }
    let n = program.ops.len();
    let mut chunk = n;
    while chunk > 0 {
        for start in (0..n).step_by(chunk) {
            out.push(edited(&|p| {
                p.ops.drain(start..(start + chunk).min(n));
            }));
        }
        chunk /= 2;
    }
    for (at, op) in program.ops.iter().enumerate() {
        for kind in simpler(&op.kind) {
            out.push(edited(&|p| p.ops[at].kind = kind.clone()));
        }
    }
    out
}

fn simpler<T: TypedOp>(kind: &Kind<T>) -> Vec<Kind<T>> {
    let mut out: Vec<Kind<T>> = match kind {
        Kind::Typed(typed) => typed.simpler().into_iter().map(Kind::Typed).collect(),
        Kind::Command(argv) if argv.len() > 1 => (0..argv.len())
            .map(|i| {
                let mut fewer = argv.clone();
                fewer.remove(i);
                Kind::Command(fewer)
            })
            .collect(),
        _ => Vec::new(),
    };
    for i in 0..kind.clone().literals().len() {
        for to_empty in [true, false] {
            let mut candidate = kind.clone();
            let shortened = match candidate.literals().swap_remove(i) {
                Literal::Word(word) => {
                    let len = if to_empty { 0 } else { word.0.len() / 2 };
                    let shortened = len < word.0.len() && (to_empty || len > 0);
                    word.0.truncate(len);
                    shortened
                }
                Literal::Number(n) => {
                    let shorter = if to_empty { 0 } else { *n / 2 };
                    let shortened = shorter != *n && (to_empty || shorter != 0);
                    *n = shorter;
                    shortened
                }
            };
            if shortened {
                out.push(candidate);
            }
        }
    }
    out
}

/// Shrinks `program` while `fails` holds, trying [`smaller`] candidates in
/// order. After keeping one, the search resumes at the same position in the
/// new list; it ends when a full pass keeps nothing. Each candidate runs
/// once, so a racy failure may shrink into a program that hits it less often.
pub fn reduce<T: TypedOp>(
    program: &Program<T>,
    mut fails: impl FnMut(&Program<T>) -> bool,
) -> Program<T> {
    let mut best = program.clone();
    let mut start = 0;
    loop {
        let candidates = smaller(&best);
        let start_at = start.min(candidates.len());
        match (start_at..candidates.len())
            .chain(0..start_at)
            .find(|&i| fails(&candidates[i]))
        {
            Some(i) => {
                best = candidates[i].clone();
                start = i;
            }
            None => return best,
        }
    }
}
