/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Generates, mutates, and shrinks programs. Pure: every choice comes from the
//! caller's RNG.

use std::collections::BTreeMap;
use std::ops::RangeInclusive;

use rand::Rng;
use rand::seq::{IndexedRandom, IteratorRandom, SliceRandom};

use crate::lower::lower;
use crate::program::{
    Argv, Bound, Bytes, Conn, FORMAT_VERSION, Field, FieldKind, Op, OpKind, Program, Query,
    QueryOptions, ReplyMode, argv,
};

const CONNS: u8 = 3;
const GENERATED_OPS: RangeInclusive<usize> = 4..=40;
const STACKED_EDITS: RangeInclusive<usize> = 1..=3;
const KEYS: u32 = 16;
const QUERY_DEPTH: u32 = 2;
const INDEXES: [&str; 3] = ["i0", "i1", "i2"];
const PREFIXES: [&str; 2] = ["d0:", "d1:"];
const FIELDS: [&str; 5] = ["f0", "f1", "f2", "f3", "f4"];
const WORDS: [&str; 14] = [
    "hello",
    "world",
    "hell",
    "running",
    "runs",
    "red",
    "green",
    "blue",
    "the",
    "a",
    "x-y",
    "hello world",
    "é",
    "日本語",
];
const NUMBERS: [i64; 10] = [0, 1, -1, 7, 100, -100, 1 << 31, 1 << 53, i64::MAX, i64::MIN];
const SYNTAX: &[u8] = b"@:{}[]()|-~*%\"\\$,.; ";
const DEBUG: &str = "_FT.DEBUG";
const HOOKS: [&[&str]; 5] = [
    &[DEBUG, "GC_FORCEINVOKE", "{index}"],
    &[DEBUG, "GC_STOP_SCHEDULE", "{index}"],
    &[DEBUG, "BG_SCAN_CONTROLLER", "SET_PAUSE_BEFORE_SCAN", "true"],
    &[DEBUG, "BG_SCAN_CONTROLLER", "SET_BG_INDEX_RESUME"],
    &["DEBUG", "RELOAD"],
];

/// What the ops before some point have set up, as far as the generator models
/// it. A `Create` of a name that exists fails, so it changes nothing.
#[derive(Clone, Debug, Default)]
pub struct Scope {
    pub indexes: BTreeMap<Bytes, Schema>,
    pub workers: bool,
    pub paused: bool,
}

#[derive(Clone, Debug)]
pub struct Schema {
    pub prefix: Bytes,
    pub fields: Vec<Field>,
}

impl Scope {
    pub fn of(module_args: &[Bytes], ops: &[Op]) -> Scope {
        let mut scope = Scope {
            workers: module_args
                .windows(2)
                .any(|pair| pair[0] == "WORKERS".into() && pair[1] != "0".into()),
            ..Scope::default()
        };
        ops.iter().for_each(|op| scope.add(op));
        scope
    }

    fn add(&mut self, op: &Op) {
        match &op.kind {
            OpKind::Create {
                index,
                prefix,
                fields,
            } => {
                self.indexes.entry(index.clone()).or_insert_with(|| Schema {
                    prefix: prefix.clone(),
                    fields: fields.clone(),
                });
            }
            OpKind::Command { argv } => match argv.as_slice() {
                [command, index, ..] if *command == "FT.DROPINDEX".into() => {
                    self.indexes.remove(index);
                }
                [debug, workers, action]
                    if *debug == DEBUG.into() && *workers == "WORKERS".into() =>
                {
                    self.paused =
                        *action == "PAUSE".into() || (self.paused && *action != "RESUME".into());
                }
                _ => {}
            },
            OpKind::Query { .. } | OpKind::Barrier => {}
        }
    }
}

/// Values to draw from: a fixed dictionary plus every word and number already
/// in the programs being mutated.
struct Pools {
    words: Vec<Bytes>,
    nums: Vec<i64>,
}

impl Pools {
    fn of(programs: &[&Program]) -> Pools {
        let mut pools = Pools {
            words: WORDS.iter().map(|&w| w.into()).collect(),
            nums: NUMBERS.to_vec(),
        };
        for program in programs {
            for op in &program.ops {
                visit(&mut op.kind.clone(), &mut |leaf| match leaf {
                    Leaf::Word(word) => pools.words.push(word.clone()),
                    Leaf::Int(n) => pools.nums.push(*n),
                });
            }
        }
        pools
    }

    fn word(&self, rng: &mut impl Rng) -> Bytes {
        self.words.choose(rng).expect("never empty").clone()
    }

    fn num(&self, rng: &mut impl Rng) -> i64 {
        *self.nums.choose(rng).expect("never empty")
    }

    fn token(&self, rng: &mut impl Rng) -> Bytes {
        if rng.random_bool(0.5) {
            self.word(rng)
        } else {
            self.num(rng).to_string().into()
        }
    }
}

pub fn generate(rng: &mut impl Rng) -> Program {
    let pools = Pools::of(&[]);
    let module_args = if rng.random_bool(0.5) {
        argv(&["WORKERS", "2"])
    } else {
        Vec::new()
    };
    let mut scope = Scope::of(&module_args, &[]);
    let ops = (0..rng.random_range(GENERATED_OPS))
        .map(|_| {
            let op = op(&scope, &pools, rng);
            scope.add(&op);
            op
        })
        .collect();
    Program {
        version: FORMAT_VERSION,
        module_args,
        ops,
    }
}

fn op(scope: &Scope, pools: &Pools, rng: &mut impl Rng) -> Op {
    let index = scope.indexes.iter().choose(rng);
    let kind = match (index, rng.random_range(0..20)) {
        _ if scope.paused && rng.random_ratio(1, 3) => command(&[DEBUG, "WORKERS", "RESUME"], []),
        (None, _) | (_, 0) => create(scope, rng),
        (Some((_, schema)), 1..=7) => write(schema, pools, rng),
        (Some((_, schema)), 8..=13) => OpKind::Query {
            index: index.expect("matched Some").0.clone(),
            query: query(schema, pools, rng, QUERY_DEPTH),
            options: options(rng),
        },
        (Some((_, schema)), 14) => command(&["DEL"], [key(schema, rng)]),
        (Some(_), 15) => OpKind::Barrier,
        (Some((name, _)), 16) => {
            let mut drop = argv(&["FT.DROPINDEX"]);
            drop.push(name.clone());
            if rng.random_bool(0.5) {
                drop.push("DD".into());
            }
            OpKind::Command { argv: drop }
        }
        (Some(_), 17) if scope.workers && !scope.paused => {
            command(&[DEBUG, "WORKERS", "PAUSE"], [])
        }
        (Some((name, _)), _) => OpKind::Command {
            argv: HOOKS
                .choose(rng)
                .expect("never empty")
                .iter()
                .map(|&word| match word {
                    "{index}" => name.clone(),
                    word => word.into(),
                })
                .collect(),
        },
    };
    let reply = match kind {
        OpKind::Query { .. } if scope.paused => ReplyMode::Defer,
        OpKind::Query { .. } | OpKind::Command { .. } if rng.random_ratio(1, 8) => ReplyMode::Defer,
        _ => ReplyMode::Await,
    };
    Op {
        conn: conn(rng),
        reply,
        kind,
    }
}

fn conn(rng: &mut impl Rng) -> Conn {
    if rng.random_bool(0.5) {
        Conn::default()
    } else {
        Conn(rng.random_range(0..CONNS))
    }
}

fn command(words: &[&str], rest: impl IntoIterator<Item = Bytes>) -> OpKind {
    let mut argv = argv(words);
    argv.extend(rest);
    OpKind::Command { argv }
}

fn create(scope: &Scope, rng: &mut impl Rng) -> OpKind {
    let mut names = FIELDS;
    names.shuffle(rng);
    let fields = names[..rng.random_range(1..=4)]
        .iter()
        .map(|&name| {
            let kind = [
                FieldKind::Text,
                FieldKind::Numeric,
                FieldKind::Tag,
                FieldKind::Geo,
            ]
            .choose(rng)
            .expect("never empty")
            .clone();
            Field {
                name: name.into(),
                sortable: kind != FieldKind::Geo && rng.random_ratio(1, 4),
                kind,
            }
        })
        .collect();
    let index = INDEXES
        .iter()
        .map(|&name| Bytes::from(name))
        .filter(|name| !scope.indexes.contains_key(name))
        .choose(rng)
        .unwrap_or_else(|| (*INDEXES.choose(rng).expect("never empty")).into());
    OpKind::Create {
        index,
        prefix: (*PREFIXES.choose(rng).expect("never empty")).into(),
        fields,
    }
}

fn key(schema: &Schema, rng: &mut impl Rng) -> Bytes {
    let mut key = schema.prefix.clone();
    key.0
        .extend_from_slice(rng.random_range(0..KEYS).to_string().as_bytes());
    key
}

fn write(schema: &Schema, pools: &Pools, rng: &mut impl Rng) -> OpKind {
    let mut hset = argv(&["HSET"]);
    hset.push(key(schema, rng));
    for field in &schema.fields {
        if hset.len() > 2 && rng.random_ratio(1, 4) {
            continue;
        }
        hset.push(field.name.clone());
        hset.push(value(&field.kind, pools, rng));
    }
    OpKind::Command { argv: hset }
}

fn value(kind: &FieldKind, pools: &Pools, rng: &mut impl Rng) -> Bytes {
    match kind {
        FieldKind::Text => words(b' ', 1..=3, pools, rng),
        FieldKind::Tag => words(b',', 1..=2, pools, rng),
        FieldKind::Numeric => pools.num(rng).to_string().into(),
        FieldKind::Geo => format!(
            "{:.4},{:.4}",
            rng.random_range(-180.0..=180.0),
            rng.random_range(-85.0..=85.0)
        )
        .into(),
    }
}

fn words(separator: u8, count: RangeInclusive<usize>, pools: &Pools, rng: &mut impl Rng) -> Bytes {
    let mut out = Vec::new();
    for i in 0..rng.random_range(count) {
        if i > 0 {
            out.push(separator);
        }
        out.extend_from_slice(pools.word(rng).as_slice());
    }
    Bytes(out)
}

fn query(schema: &Schema, pools: &Pools, rng: &mut impl Rng, depth: u32) -> Query {
    let field = |kind: FieldKind, rng: &mut _| {
        schema
            .fields
            .iter()
            .filter(|f| f.kind == kind)
            .choose(rng)
            .map(|f| f.name.clone())
    };
    let leaves = if depth == 0 { 5 } else { 8 };
    match rng.random_range(0..leaves) {
        0 if depth == QUERY_DEPTH => Query::All,
        2 => match field(FieldKind::Text, rng) {
            Some(field) => Query::InField {
                field,
                term: pools.word(rng),
            },
            None => Query::Term(pools.word(rng)),
        },
        3 => match field(FieldKind::Tag, rng) {
            Some(field) => Query::Tag {
                field,
                values: (0..rng.random_range(1..=2))
                    .map(|_| pools.word(rng))
                    .collect(),
            },
            None => Query::Term(pools.word(rng)),
        },
        4 => match field(FieldKind::Numeric, rng) {
            Some(field) => Query::Range {
                field,
                min: bound(pools, rng),
                max: bound(pools, rng),
            },
            None => Query::Term(pools.word(rng)),
        },
        5 | 6 => {
            let children = (0..rng.random_range(2..=3))
                .map(|_| query(schema, pools, rng, depth - 1))
                .collect();
            if rng.random_bool(0.5) {
                Query::And(children)
            } else {
                Query::Or(children)
            }
        }
        7 => Query::Not(Box::new(query(schema, pools, rng, depth - 1))),
        _ => Query::Term(pools.word(rng)),
    }
}

fn bound(pools: &Pools, rng: &mut impl Rng) -> Bound {
    match rng.random_range(0..3) {
        0 => Bound::Unbounded,
        1 => Bound::Inclusive(pools.num(rng)),
        _ => Bound::Exclusive(pools.num(rng)),
    }
}

fn options(rng: &mut impl Rng) -> QueryOptions {
    QueryOptions {
        no_content: rng.random_bool(0.5),
        limit: rng
            .random_ratio(1, 4)
            .then(|| (rng.random_range(0..10), rng.random_range(1..20))),
        dialect: rng.random_bool(0.5).then(|| rng.random_range(1..=4)),
    }
}

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
    let ops = &mut program.ops;
    let at = rng.random_range(0..=ops.len());
    match rng.random_range(0..12) {
        0..=3 => {
            let op = op(&Scope::of(&program.module_args, &ops[..at]), pools, rng);
            ops.insert(at, op);
        }
        4 if !donor.ops.is_empty() => {
            let start = rng.random_range(0..donor.ops.len());
            let end = rng.random_range(start + 1..=donor.ops.len().min(start + 8));
            ops.splice(at..at, donor.ops[start..end].iter().cloned());
        }
        _ if at == ops.len() => {}
        5 => {
            let end = rng.random_range(at + 1..=ops.len().min(at + 4));
            ops.drain(at..end);
        }
        6 => {
            let op = ops.remove(at);
            let lowered = lower(&op.kind).into_iter().map(|argv| Op {
                kind: OpKind::Command { argv },
                ..op
            });
            ops.splice(at..at, lowered);
        }
        7 => ops[at].conn = conn(rng),
        8 => {
            ops[at].reply = match ops[at].reply {
                ReplyMode::Await => ReplyMode::Defer,
                ReplyMode::Defer => ReplyMode::Await,
            }
        }
        9 if leaf_count(&ops[at].kind) > 0 => {
            let kind = &mut ops[at].kind;
            let leaf = rng.random_range(0..leaf_count(kind));
            with_leaf(kind, leaf, |leaf| match leaf {
                Leaf::Word(word) => *word = pools.word(rng),
                Leaf::Int(n) => *n = pools.num(rng),
            });
        }
        10 | 11 => {
            if let OpKind::Command { argv } = &mut ops[at].kind {
                edit_tokens(argv, pools, rng);
            }
        }
        _ => {}
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
        3 => argv[i] = pools.token(rng),
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
        let kinds: Vec<OpKind> = match &op.kind {
            OpKind::Create {
                index,
                prefix,
                fields,
            } => (0..fields.len())
                .map(|i| {
                    let mut fields = fields.clone();
                    fields.remove(i);
                    OpKind::Create {
                        index: index.clone(),
                        prefix: prefix.clone(),
                        fields,
                    }
                })
                .collect(),
            OpKind::Query {
                index,
                query,
                options,
            } => hoists(query)
                .into_iter()
                .map(|query| OpKind::Query {
                    index: index.clone(),
                    query,
                    options: options.clone(),
                })
                .collect(),
            OpKind::Command { argv } if argv.len() > 1 => (0..argv.len())
                .map(|i| {
                    let mut argv = argv.clone();
                    argv.remove(i);
                    OpKind::Command { argv }
                })
                .collect(),
            _ => Vec::new(),
        };
        let leaves = (0..leaf_count(&op.kind)).flat_map(|leaf| {
            [true, false].into_iter().filter_map(move |to_zero| {
                let mut kind = op.kind.clone();
                let mut changed = false;
                with_leaf(&mut kind, leaf, |leaf| match leaf {
                    Leaf::Word(word) => {
                        let len = if to_zero { 0 } else { word.0.len() / 2 };
                        changed = len < word.0.len() && (to_zero || len > 0);
                        word.0.truncate(len);
                    }
                    Leaf::Int(n) => {
                        let shrunk = if to_zero { 0 } else { *n / 2 };
                        changed = shrunk != *n && (to_zero || shrunk != 0);
                        *n = shrunk;
                    }
                });
                changed.then_some(kind)
            })
        });
        for kind in kinds.into_iter().chain(leaves) {
            out.push(with(&|p| p.ops[at].kind = kind.clone()));
        }
    }
    out
}

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
                let mut rest = children.clone();
                rest.remove(i);
                out.push(rebuild(rest));
                for simpler in hoists(child) {
                    let mut children = children.clone();
                    children[i] = simpler;
                    out.push(rebuild(children));
                }
            }
            out
        }
        Query::Not(child) => {
            let mut out = vec![(**child).clone()];
            out.extend(hoists(child).into_iter().map(|q| Query::Not(Box::new(q))));
            out
        }
        _ => Vec::new(),
    }
}

/// The measure [`smaller`] decreases: ops, module arguments, schema fields,
/// query nodes, and the length of every word and number.
pub fn size(program: &Program) -> usize {
    let mut total = program.ops.len() + program.module_args.len();
    for op in &program.ops {
        match &op.kind {
            OpKind::Create { fields, .. } => total += fields.len(),
            OpKind::Query { query, .. } => total += nodes(query),
            _ => {}
        }
        visit(&mut op.kind.clone(), &mut |leaf| {
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
        Query::Not(child) => nodes(child),
        _ => 0,
    }
}

/// The values inside an op that edits may replace or shrink.
enum Leaf<'a> {
    Word(&'a mut Bytes),
    Int(&'a mut i64),
}

fn visit(kind: &mut OpKind, f: &mut dyn FnMut(Leaf<'_>)) {
    match kind {
        OpKind::Query { query, .. } => visit_query(query, f),
        OpKind::Command { argv } => argv.iter_mut().for_each(|token| f(Leaf::Word(token))),
        OpKind::Create { .. } | OpKind::Barrier => {}
    }
}

fn visit_query(query: &mut Query, f: &mut dyn FnMut(Leaf<'_>)) {
    match query {
        Query::All => {}
        Query::Term(term) | Query::InField { term, .. } => f(Leaf::Word(term)),
        Query::Tag { values, .. } => values.iter_mut().for_each(|v| f(Leaf::Word(v))),
        Query::Range { min, max, .. } => {
            for bound in [min, max] {
                if let Bound::Inclusive(n) | Bound::Exclusive(n) = bound {
                    f(Leaf::Int(n));
                }
            }
        }
        Query::And(children) | Query::Or(children) => {
            children.iter_mut().for_each(|child| visit_query(child, f))
        }
        Query::Not(child) => visit_query(child, f),
    }
}

fn leaf_count(kind: &OpKind) -> usize {
    let mut count = 0;
    visit(&mut kind.clone(), &mut |_| count += 1);
    count
}

fn with_leaf(kind: &mut OpKind, leaf: usize, mut f: impl FnMut(Leaf<'_>)) {
    let mut i = 0;
    visit(kind, &mut |l| {
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

    const PROGRAMS: u64 = 300;

    #[test]
    fn mutated_programs_round_trip() {
        let mut rng = SmallRng::seed_from_u64(0);
        let mut program = generate(&mut rng);
        for _ in 0..PROGRAMS {
            let donor = generate(&mut rng);
            program = mutate(&program, &donor, &mut rng);
            assert_eq!(Program::from_json(&program.to_json()).unwrap(), program);
        }
    }

    #[test]
    fn every_smaller_candidate_is_smaller() {
        let mut rng = SmallRng::seed_from_u64(1);
        for _ in 0..PROGRAMS / 10 {
            let program = generate(&mut rng);
            let program = mutate(&program, &program, &mut rng);
            for candidate in smaller(&program) {
                assert!(size(&candidate) < size(&program), "{candidate:?}");
            }
        }
    }
}
