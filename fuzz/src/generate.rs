/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Generates programs and ops that are well-typed against what the ops before
//! them set up. Pure: every choice comes from the caller's RNG.

use std::ops::RangeInclusive;

use rand::Rng;
use rand::seq::{IndexedRandom, IteratorRandom, SliceRandom};

use crate::lower::{self, NUMBERS, WORDS, rendered, vector};
use crate::program::{
    Algorithm, Argv, Bound, Bytes, Conn, DataType, FORMAT_VERSION, Field, FieldKind, Leaf, Metric,
    Op, OpKind, Program, Query, QueryOptions, ReplyMode, Var, argv,
};
use crate::scope::{Schema, Scope};

const CONNS: u8 = 3;
const VARS: u8 = 2;
const GENERATED_OPS: RangeInclusive<usize> = 4..=40;
const POPULATE_BITS: RangeInclusive<u32> = 0..=9;
const RELATED_EVERY: u32 = 12;
const RELATED_LIMIT: u32 = 1000;
const VECTOR_DIMS: [u16; 3] = [2, 4, 8];
const PIPELINE_STEPS: RangeInclusive<usize> = 0..=3;
const KEYS: u32 = 16;
const QUERY_DEPTH: u32 = 2;
const INDEXES: [&str; 3] = ["i0", "i1", "i2"];
const PREFIXES: [&str; 2] = ["d0:", "d1:"];
const JSON_PREFIXES: [&str; 2] = ["j0:", "j1:"];
const FIELDS: [&str; 5] = ["f0", "f1", "f2", "f3", "f4"];
const DEBUG: &str = "_FT.DEBUG";
const HOOKS: [&[&str]; 5] = [
    &[DEBUG, "GC_FORCEINVOKE", "{index}"],
    &[DEBUG, "GC_STOP_SCHEDULE", "{index}"],
    &[DEBUG, "BG_SCAN_CONTROLLER", "SET_PAUSE_BEFORE_SCAN", "true"],
    &[DEBUG, "BG_SCAN_CONTROLLER", "SET_BG_INDEX_RESUME"],
    &["DEBUG", "RELOAD"],
];

/// Values to draw from: a fixed dictionary plus every word and number in the
/// given programs, so values that reached new code are reused.
pub struct Pools {
    words: Vec<Bytes>,
    nums: Vec<i64>,
}

impl Pools {
    pub fn of(programs: &[&Program]) -> Pools {
        let mut pools = Pools {
            words: WORDS.iter().map(|&w| w.into()).collect(),
            nums: NUMBERS.to_vec(),
        };
        for op in programs.iter().flat_map(|p| &p.ops) {
            op.kind.clone().leaves(&mut |leaf| match leaf {
                Leaf::Word(word) => pools.words.push(word.clone()),
                Leaf::Int(n) => pools.nums.push(*n),
            });
        }
        pools
    }

    pub fn word(&self, rng: &mut impl Rng) -> Bytes {
        self.words.choose(rng).expect("never empty").clone()
    }

    pub fn num(&self, rng: &mut impl Rng) -> i64 {
        *self.nums.choose(rng).expect("never empty")
    }
}

pub fn generate(rng: &mut impl Rng) -> Program {
    let pools = Pools::of(&[]);
    let mut module_args = Vec::new();
    if rng.random_ratio(3, 4) {
        module_args.extend(argv(&["ON_TIMEOUT", "FAIL"]));
    }
    if rng.random_bool(0.5) {
        module_args.extend(argv(&["WORKERS", "2"]));
    }
    let mut scope = Scope::new(&module_args);
    let mut ops = Vec::new();
    for _ in 0..rng.random_range(GENERATED_OPS) {
        let group = if rng.random_ratio(1, RELATED_EVERY) {
            related(&scope, &pools, rng)
        } else {
            vec![op(&scope, &pools, rng)]
        };
        for op in group {
            scope.add(&op);
            ops.push(op);
        }
    }
    Program {
        version: FORMAT_VERSION,
        module_args,
        ops,
    }
}

/// One op that makes sense after `scope`.
pub fn op(scope: &Scope, pools: &Pools, rng: &mut impl Rng) -> Op {
    let index = scope.indexes.iter().choose(rng);
    let mut bind = None;
    let kind = match (index, rng.random_range(0..20)) {
        _ if scope.paused && rng.random_ratio(1, 3) => command(&[DEBUG, "WORKERS", "RESUME"]),
        (None, _) | (_, 0) => create(scope, rng),
        (Some((_, schema)), 1..=6) => write(schema, pools, rng),
        (Some((_, schema)), 7) => OpKind::Populate {
            on: schema.on,
            prefix: schema.prefix.clone(),
            fields: schema.fields.clone(),
            n: 1 << rng.random_range(POPULATE_BITS) | rng.random_range(0..2),
            seed: rng.random(),
        },
        (Some((name, schema)), 8..=11) => OpKind::Query {
            index: name.clone(),
            query: query(schema, pools, rng, QUERY_DEPTH),
            options: options(rng),
        },
        (Some(_), 13) if !scope.cursors.is_empty() => {
            let (var, index) = scope.cursors.iter().choose(rng).expect("not empty");
            let mut command = argv(&["FT.CURSOR"]);
            if rng.random_ratio(1, 5) {
                command.extend(["DEL".into(), index.clone(), var.token()]);
            } else {
                command.extend(["READ".into(), index.clone(), var.token()]);
                bind = Some(*var);
            }
            OpKind::Command { argv: command }
        }
        (Some((name, schema)), 12 | 13) => {
            let (text, params) = rendered(&query(schema, pools, rng, QUERY_DEPTH));
            let mut command = vec!["FT.AGGREGATE".into(), name.clone(), text];
            command.extend(pipeline(schema, pools, rng));
            if rng.random_ratio(1, 3) {
                command.extend(argv(&[
                    "WITHCURSOR",
                    "COUNT",
                    &rng.random_range(1..=10).to_string(),
                ]));
                bind = Some(Var(rng.random_range(0..VARS)));
            }
            if !params.is_empty() {
                command.extend(params);
                command.extend(argv(&["DIALECT", "2"]));
            }
            OpKind::Command { argv: command }
        }
        (Some((_, schema)), 14) => OpKind::Command {
            argv: vec!["DEL".into(), key(schema, rng)],
        },
        (Some(_), 15) => OpKind::Barrier,
        (Some((name, _)), 16) => {
            let mut argv = vec!["FT.DROPINDEX".into(), name.clone()];
            if rng.random_bool(0.5) {
                argv.push("DD".into());
            }
            OpKind::Command { argv }
        }
        (Some(_), 17) if scope.workers && !scope.paused => command(&[DEBUG, "WORKERS", "PAUSE"]),
        (Some((name, schema)), 17) => OpKind::Alter {
            index: name.clone(),
            on: schema.on,
            field: field(
                FIELDS
                    .iter()
                    .find(|&&f| schema.fields.iter().all(|s| s.name != f.into()))
                    .unwrap_or(&FIELDS[0]),
                rng,
            ),
        },
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
    let searches = matches!(&kind, OpKind::Query { .. })
        || matches!(&kind, OpKind::Command { argv } if argv[0] == "FT.AGGREGATE".into());
    let reply = if (searches && scope.paused) || rng.random_ratio(1, 8) {
        ReplyMode::Defer
    } else {
        ReplyMode::Await
    };
    Op {
        conn: conn(rng),
        reply,
        bind,
        kind,
    }
}

/// Queries the logic rules relate, behind a barrier on one connection: `q`,
/// `q p`, and `q -p`, sometimes a forced GC, `q` again, and `q` on a twin of
/// the index if there is one. Without an index, one ordinary op.
pub fn related(scope: &Scope, pools: &Pools, rng: &mut impl Rng) -> Vec<Op> {
    let Some((name, schema)) = scope.indexes.iter().choose(rng) else {
        return vec![op(scope, pools, rng)];
    };
    let q = match query(schema, pools, rng, QUERY_DEPTH) {
        Query::Knn { filter, .. } => *filter,
        q => q,
    };
    let p = query(schema, pools, rng, 0);
    let with = |part: Query| match &q {
        Query::All => part,
        q => Query::And(vec![q.clone(), part]),
    };
    let options = QueryOptions {
        no_content: true,
        limit: Some((0, RELATED_LIMIT)),
        dialect: rng.random_bool(0.5).then_some(2),
    };
    let search = |index: &Bytes, query: Query| OpKind::Query {
        index: index.clone(),
        query,
        options: options.clone(),
    };
    let mut kinds = vec![
        OpKind::Barrier,
        search(name, q.clone()),
        search(name, with(p.clone())),
        search(name, with(Query::Not(Box::new(p)))),
    ];
    if rng.random_bool(0.5) {
        let mut gc = argv(&[DEBUG, "GC_FORCEINVOKE"]);
        gc.push(name.clone());
        kinds.push(OpKind::Command { argv: gc });
    }
    kinds.push(search(name, q.clone()));
    if let Some(twin) = scope.twin(name) {
        kinds.push(search(twin, q));
    }
    kinds
        .into_iter()
        .map(|kind| Op {
            conn: Conn::default(),
            reply: ReplyMode::Await,
            bind: None,
            kind,
        })
        .collect()
}

pub fn conn(rng: &mut impl Rng) -> Conn {
    if rng.random_bool(0.5) {
        Conn::default()
    } else {
        Conn(rng.random_range(0..CONNS))
    }
}

fn command(words: &[&str]) -> OpKind {
    OpKind::Command { argv: argv(words) }
}

/// A new index, or a third of the time a twin of an existing one: the same
/// keys and fields, with `SORTABLE` drawn afresh.
fn create(scope: &Scope, rng: &mut impl Rng) -> OpKind {
    let index = INDEXES
        .iter()
        .map(|&name| Bytes::from(name))
        .filter(|name| !scope.indexes.contains_key(name))
        .choose(rng)
        .unwrap_or_else(|| (*INDEXES.choose(rng).expect("never empty")).into());
    if let Some(original) = scope.indexes.values().choose(rng)
        && rng.random_ratio(1, 3)
    {
        return OpKind::Create {
            index,
            on: original.on,
            prefix: original.prefix.clone(),
            fields: original
                .fields
                .iter()
                .map(|f| Field {
                    sortable: sortable(&f.kind, rng),
                    ..f.clone()
                })
                .collect(),
        };
    }
    let mut names = FIELDS;
    names.shuffle(rng);
    let (on, prefixes) = if rng.random_ratio(1, 3) {
        (DataType::Json, JSON_PREFIXES)
    } else {
        (DataType::Hash, PREFIXES)
    };
    OpKind::Create {
        index,
        on,
        prefix: (*prefixes.choose(rng).expect("never empty")).into(),
        fields: names[..rng.random_range(1..=4)]
            .iter()
            .map(|name| field(name, rng))
            .collect(),
    }
}

fn field(name: &str, rng: &mut impl Rng) -> Field {
    let kind = match rng.random_range(0..5) {
        0 => FieldKind::Text,
        1 => FieldKind::Numeric,
        2 => FieldKind::Tag,
        3 => FieldKind::Geo,
        _ => FieldKind::Vector {
            algorithm: *[Algorithm::Flat, Algorithm::Hnsw]
                .choose(rng)
                .expect("never empty"),
            dim: *VECTOR_DIMS.choose(rng).expect("never empty"),
            metric: *[Metric::L2, Metric::Ip, Metric::Cosine]
                .choose(rng)
                .expect("never empty"),
        },
    };
    Field {
        name: name.into(),
        sortable: sortable(&kind, rng),
        kind,
    }
}

fn sortable(kind: &FieldKind, rng: &mut impl Rng) -> bool {
    matches!(kind, FieldKind::Text | FieldKind::Numeric | FieldKind::Tag) && rng.random_ratio(1, 4)
}

/// Aggregation steps over the schema's fields. After a `GROUPBY` only its
/// key and reducer outputs remain, so later steps refer to those.
fn pipeline(schema: &Schema, pools: &Pools, rng: &mut impl Rng) -> Argv {
    let mut names: Vec<(String, bool)> = schema
        .fields
        .iter()
        .map(|f| {
            let name = String::from_utf8_lossy(f.name.as_slice()).into_owned();
            (name, f.kind == FieldKind::Numeric)
        })
        .collect();
    let mut out: Vec<String> = Vec::new();
    for _ in 0..rng.random_range(PIPELINE_STEPS) {
        let any = names.choose(rng).map(|(name, _)| format!("@{name}"));
        let numeric = names
            .iter()
            .filter(|(_, numeric)| *numeric)
            .choose(rng)
            .map(|(name, _)| format!("@{name}"));
        match (rng.random_range(0..6), any, numeric) {
            (0, Some(field), _) => out.extend(["LOAD".into(), "1".into(), field]),
            (1, Some(field), numeric) => {
                out.extend(["GROUPBY".into(), "1".into(), field.clone()]);
                out.extend(["REDUCE", "COUNT", "0", "AS", "count"].map(String::from));
                names = vec![(field[1..].to_owned(), false), ("count".into(), true)];
                if let Some(numeric) = numeric {
                    out.extend(["REDUCE".into(), "SUM".into(), "1".into(), numeric]);
                    out.extend(["AS".into(), "sum".into()]);
                    names.push(("sum".into(), true));
                }
            }
            (2, Some(field), _) => {
                let order = if rng.random_bool(0.5) { "ASC" } else { "DESC" };
                out.extend(["SORTBY".into(), "2".into(), field, order.into()]);
            }
            (3, _, Some(numeric)) => {
                out.extend(["APPLY".into(), format!("{numeric} * 2"), "AS".into()]);
                out.push("doubled".into());
                names.push(("doubled".into(), true));
            }
            (4, _, Some(numeric)) => {
                out.extend(["FILTER".into(), format!("{numeric} > {}", pools.num(rng))]);
            }
            _ => out.extend([
                "LIMIT".into(),
                "0".into(),
                rng.random_range(1..20).to_string(),
            ]),
        }
    }
    out.into_iter().map(Bytes::from).collect()
}

fn key(schema: &Schema, rng: &mut impl Rng) -> Bytes {
    let mut key = schema.prefix.clone();
    key.0
        .extend_from_slice(rng.random_range(0..KEYS).to_string().as_bytes());
    key
}

fn write(schema: &Schema, pools: &Pools, rng: &mut impl Rng) -> OpKind {
    let key = key(schema, rng);
    let mut values = Vec::new();
    for field in &schema.fields {
        if values.is_empty() || !rng.random_ratio(1, 4) {
            values.push((field, value(&field.kind, pools, rng)));
        }
    }
    OpKind::Command {
        argv: lower::write(schema.on, key, &values),
    }
}

fn value(kind: &FieldKind, pools: &Pools, rng: &mut impl Rng) -> Bytes {
    let joined = |separator: &[u8], count, rng: &mut _| {
        let words: Vec<Bytes> = (0..count).map(|_| pools.word(rng)).collect();
        Bytes(
            words
                .into_iter()
                .map(|w| w.0)
                .collect::<Vec<_>>()
                .join(separator),
        )
    };
    match kind {
        FieldKind::Text => {
            let count = rng.random_range(1..=3);
            joined(b" ", count, rng)
        }
        FieldKind::Tag => {
            let count = rng.random_range(1..=2);
            joined(b",", count, rng)
        }
        FieldKind::Numeric => pools.num(rng).to_string().into(),
        FieldKind::Geo => format!(
            "{:.4},{:.4}",
            rng.random_range(-180.0..=180.0),
            rng.random_range(-85.0..=85.0)
        )
        .into(),
        FieldKind::Vector { dim, .. } => vector(*dim, || rng.random_range(-100..=100)),
    }
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
    let vector_field = schema.fields.iter().find_map(|f| match f.kind {
        FieldKind::Vector { dim, .. } => Some((f.name.clone(), dim)),
        _ => None,
    });
    if let Some((field, dim)) = vector_field
        && depth == QUERY_DEPTH
        && rng.random_ratio(1, 3)
    {
        return Query::Knn {
            filter: Box::new(if rng.random_bool(0.5) {
                Query::All
            } else {
                query(schema, pools, rng, depth - 1)
            }),
            field,
            k: rng.random_range(1..=10),
            vector: vector(dim, || rng.random_range(-100..=100)),
        };
    }
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
