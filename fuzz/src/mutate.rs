/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ops::RangeInclusive;

use rand::Rng;
use rand::seq::{IndexedRandom, IteratorRandom};

use crate::generate::{conn, next_op, related_queries};
use crate::lower::lower;
use crate::program::{Argv, Literal, Op, OpKind, Program, ReplyMode};
use crate::scope::Scope;
use crate::vocabulary::Vocabulary;

const EDITS: RangeInclusive<usize> = 1..=3;
const MOST_SPLICED: usize = 8;
const MOST_DELETED: usize = 4;
const SYNTAX: &[u8] = b"@:{}[]()|-~*%\"\\$,.; ";

/// `program` with a few random edits; `donor` is a program to splice from.
pub fn mutate(program: &Program, donor: &Program, rng: &mut impl Rng) -> Program {
    let vocabulary = Vocabulary::with_literals_of(&[program, donor]);
    let mut out = program.clone();
    for _ in 0..rng.random_range(EDITS) {
        edit(&mut out, donor, &vocabulary, rng);
    }
    out
}

fn edit(program: &mut Program, donor: &Program, vocabulary: &Vocabulary, rng: &mut impl Rng) {
    let ops = &mut program.ops;
    let at = rng.random_range(0..=ops.len());
    let scope = Scope::after(&program.module_args, &ops[..at]);
    match rng.random_range(0..11) {
        0..=2 => ops.insert(at, next_op(&scope, vocabulary, rng)),
        3 => {
            let group = related_queries(&scope, vocabulary, rng);
            ops.splice(at..at, group);
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
        // Typed ops never send malformed commands; their lowered form can.
        6 => {
            let op = ops.remove(at);
            let commands = lower(&op.kind).into_iter().map(|argv| Op {
                kind: OpKind::Command { argv },
                ..op.clone()
            });
            ops.splice(at..at, commands);
        }
        7 => {
            ops[at].conn = conn(rng);
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
            if let OpKind::Command { argv } = &mut ops[at].kind {
                edit_tokens(argv, vocabulary, rng);
            }
        }
    }
}

fn edit_tokens(argv: &mut Argv, vocabulary: &Vocabulary, rng: &mut impl Rng) {
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
