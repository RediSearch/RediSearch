/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Ops that make sense after what the ops before them set up.

use std::ops::RangeInclusive;

use rand::Rng;
use rand::seq::{IndexedRandom, IteratorRandom, SliceRandom};

use procfuzz::legacy::program::{Argv, Bytes, Conn, Kind, ReplyMode, Var, argv};
use procfuzz::legacy::vocabulary::Vocabulary;

use crate::action::{
    Action, Algorithm, Bound, DataType, Field, FieldKind, Metric, Query, QueryOptions, key,
    rendered, value, vector, vocabulary,
};
use crate::redis::DEBUG;
use crate::scope::{Schema, Scope};
use crate::{Op, Program};

const VARS: u8 = 2;
const PROGRAM_OPS: RangeInclusive<usize> = 4..=40;
const POPULATE_BITS: RangeInclusive<u32> = 0..=9;
const RELATED_QUERIES_ONE_IN: u32 = 6;
const RELATED_QUERIES_LIMIT: u32 = 1000;
const VECTOR_DIMS: [u16; 3] = [2, 4, 8];
const KEYS: u32 = 16;
const QUERY_DEPTH: u32 = 2;
const INDEXES: [&str; 3] = ["i0", "i1", "i2"];
const HASH_PREFIXES: [&str; 2] = ["d0:", "d1:"];
const JSON_PREFIXES: [&str; 2] = ["j0:", "j1:"];
const FIELDS: [&str; 5] = ["f0", "f1", "f2", "f3", "f4"];
const HOOKS: [&[&str]; 5] = [
    &[DEBUG, "GC_FORCEINVOKE", "{index}"],
    &[DEBUG, "GC_STOP_SCHEDULE", "{index}"],
    &[DEBUG, "BG_SCAN_CONTROLLER", "SET_PAUSE_BEFORE_SCAN", "true"],
    &[DEBUG, "BG_SCAN_CONTROLLER", "SET_BG_INDEX_RESUME"],
    &["DEBUG", "RELOAD"],
];

pub fn generate(rng: &mut impl Rng) -> Program {
    let vocabulary = vocabulary();
    let mut args = Vec::new();
    if rng.random_ratio(3, 4) {
        args.extend(argv(&["ON_TIMEOUT", "FAIL"]));
    }
    if rng.random_bool(0.5) {
        args.extend(argv(&["WORKERS", "2"]));
    }
    let mut scope = Scope::new(&args);
    let mut ops = Vec::new();
    for _ in 0..rng.random_range(PROGRAM_OPS) {
        for op in ops_after(&scope, &vocabulary, rng) {
            scope.add(&op);
            ops.push(op);
        }
    }
    Program { args, ops }
}

/// Ops that make sense after `prefix`: usually one, sometimes a group of
/// related queries.
pub fn ops(args: &[Bytes], prefix: &[Op], vocabulary: &Vocabulary, rng: &mut impl Rng) -> Vec<Op> {
    ops_after(&Scope::after(args, prefix), vocabulary, rng)
}

fn ops_after(scope: &Scope, vocabulary: &Vocabulary, rng: &mut impl Rng) -> Vec<Op> {
    if rng.random_ratio(1, RELATED_QUERIES_ONE_IN) {
        related_queries(scope, vocabulary, rng)
    } else {
        vec![next_op(scope, vocabulary, rng)]
    }
}

fn next_op(scope: &Scope, vocabulary: &Vocabulary, rng: &mut impl Rng) -> Op {
    let index = scope.indexes.iter().choose(rng);
    let mut bind = None;
    let kind = match (index, rng.random_range(0..20)) {
        _ if scope.paused && rng.random_ratio(1, 3) => command(&[DEBUG, "WORKERS", "RESUME"]),
        (None, _) | (_, 0) => create(scope, rng),
        (Some((_, schema)), 1..=6) => write(schema, vocabulary, rng),
        (Some((_, schema)), 7) => Kind::Typed(Action::Populate {
            on: schema.on,
            prefix: schema.prefix.clone(),
            fields: schema.fields.clone(),
            n: 1 << rng.random_range(POPULATE_BITS) | rng.random_range(0..2),
            seed: rng.random(),
        }),
        (Some((name, schema)), 8..=11) => Kind::Typed(Action::Query {
            index: name.clone(),
            query: query(schema, vocabulary, rng, QUERY_DEPTH),
            options: options(rng),
        }),
        (Some(_), 13) if !scope.cursors.is_empty() => {
            let (&var, index) = scope.cursors.iter().choose(rng).expect("not empty");
            let action = if rng.random_ratio(1, 5) {
                "DEL"
            } else {
                bind = Some(var);
                "READ"
            };
            Kind::Command(vec![
                "FT.CURSOR".into(),
                action.into(),
                index.clone(),
                var.token(),
            ])
        }
        (Some((name, schema)), 12 | 13) => {
            let (text, params) = rendered(&query(schema, vocabulary, rng, QUERY_DEPTH));
            let mut command = vec!["FT.AGGREGATE".into(), name.clone(), text];
            command.extend(pipeline(schema, vocabulary, rng));
            if rng.random_ratio(1, 3) {
                let count = rng.random_range(1..=10).to_string();
                command.extend(argv(&["WITHCURSOR", "COUNT", &count]));
                bind = Some(Var(rng.random_range(0..VARS)));
            }
            if !params.is_empty() {
                command.extend(params);
                command.extend(argv(&["DIALECT", "2"]));
            }
            Kind::Command(command)
        }
        (Some((_, schema)), 14) => Kind::Command(vec!["DEL".into(), random_key(schema, rng)]),
        (Some(_), 15) => Kind::Barrier,
        (Some((name, _)), 16) => {
            let mut argv = vec!["FT.DROPINDEX".into(), name.clone()];
            if rng.random_bool(0.5) {
                argv.push("DD".into());
            }
            Kind::Command(argv)
        }
        (Some(_), 17) if scope.workers && !scope.paused => command(&[DEBUG, "WORKERS", "PAUSE"]),
        (Some((name, schema)), 17) => {
            let unused = FIELDS
                .iter()
                .find(|&&f| schema.fields.iter().all(|s| s.name != f.into()));
            Kind::Typed(Action::Alter {
                index: name.clone(),
                on: schema.on,
                field: field(unused.unwrap_or(&FIELDS[0]), rng),
            })
        }
        (Some((name, _)), _) => Kind::Command(
            HOOKS
                .choose(rng)
                .expect("never empty")
                .iter()
                .map(|&word| match word {
                    "{index}" => name.clone(),
                    word => word.into(),
                })
                .collect(),
        ),
    };
    let searches = matches!(&kind, Kind::Typed(Action::Query { .. }))
        || matches!(&kind, Kind::Command(argv) if argv[0] == "FT.AGGREGATE".into());
    // A search waits on paused workers, so awaiting it would stall the program.
    let reply = if (searches && scope.paused) || rng.random_ratio(1, 8) {
        ReplyMode::Defer
    } else {
        ReplyMode::Await
    };
    Op {
        conn: Conn::random(rng),
        reply,
        bind,
        kind,
    }
}

/// Queries the logic rules can relate, after a barrier and on one
/// connection: `q`, `q p`, `q -p`, sometimes a forced GC, `q` again, and `q`
/// on a twin index if there is one.
fn related_queries(scope: &Scope, vocabulary: &Vocabulary, rng: &mut impl Rng) -> Vec<Op> {
    let Some((name, schema)) = scope.indexes.iter().choose(rng) else {
        return vec![next_op(scope, vocabulary, rng)];
    };
    let q = match query(schema, vocabulary, rng, QUERY_DEPTH) {
        Query::Knn { filter, .. } => *filter,
        q => q,
    };
    let p = query(schema, vocabulary, rng, 0);
    let with = |part: Query| match &q {
        Query::All => part,
        q => Query::And(vec![q.clone(), part]),
    };
    let options = QueryOptions {
        no_content: true,
        limit: Some((0, RELATED_QUERIES_LIMIT)),
        dialect: rng.random_bool(0.5).then_some(2),
    };
    let search = |index: &Bytes, query: Query| {
        Kind::Typed(Action::Query {
            index: index.clone(),
            query,
            options: options.clone(),
        })
    };
    let mut kinds = vec![
        Kind::Barrier,
        search(name, q.clone()),
        search(name, with(p.clone())),
        search(name, with(Query::Not(Box::new(p)))),
    ];
    if rng.random_bool(0.5) {
        let mut gc = argv(&[DEBUG, "GC_FORCEINVOKE"]);
        gc.push(name.clone());
        kinds.push(Kind::Command(gc));
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

fn command(words: &[&str]) -> Kind<Action> {
    Kind::Command(argv(words))
}

/// A new index, or a third of the time a twin of an existing one with
/// `SORTABLE` drawn afresh.
fn create(scope: &Scope, rng: &mut impl Rng) -> Kind<Action> {
    let index = INDEXES
        .iter()
        .map(|&name| Bytes::from(name))
        .filter(|name| !scope.indexes.contains_key(name))
        .choose(rng)
        .unwrap_or_else(|| (*INDEXES.choose(rng).expect("never empty")).into());
    if let Some(original) = scope.indexes.values().choose(rng)
        && rng.random_ratio(1, 3)
    {
        return Kind::Typed(Action::Create {
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
        });
    }
    let mut names = FIELDS;
    names.shuffle(rng);
    let (on, prefixes) = if rng.random_ratio(1, 3) {
        (DataType::Json, JSON_PREFIXES)
    } else {
        (DataType::Hash, HASH_PREFIXES)
    };
    Kind::Typed(Action::Create {
        index,
        on,
        prefix: (*prefixes.choose(rng).expect("never empty")).into(),
        fields: names[..rng.random_range(1..=4)]
            .iter()
            .map(|name| field(name, rng))
            .collect(),
    })
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

/// Aggregation steps in a fixed order. After `GROUPBY` only its key and
/// `count` remain to sort by.
fn pipeline(schema: &Schema, vocabulary: &Vocabulary, rng: &mut impl Rng) -> Argv {
    let reference = |field: &Field| format!("@{}", String::from_utf8_lossy(field.name.as_slice()));
    let any = schema.fields.choose(rng).map(reference);
    let numeric = schema
        .fields
        .iter()
        .filter(|f| f.kind == FieldKind::Numeric)
        .choose(rng)
        .map(reference);
    let mut steps = Vec::new();
    if let Some(numeric) = &numeric {
        if rng.random_ratio(1, 3) {
            steps.extend(argv(&["APPLY", &format!("{numeric} * 2"), "AS", "doubled"]));
        }
        if rng.random_ratio(1, 3) {
            let bound = vocabulary.number(rng);
            steps.extend(argv(&["FILTER", &format!("{numeric} > {bound}")]));
        }
    }
    let mut sort_key = any.clone();
    if let Some(any) = &any
        && rng.random_ratio(1, 3)
    {
        steps.extend(argv(&["GROUPBY", "1", any, "REDUCE", "COUNT", "0", "AS"]));
        steps.push("count".into());
        sort_key = Some("@count".into());
    }
    if let Some(sort_key) = sort_key
        && rng.random_ratio(1, 3)
    {
        let order = if rng.random_bool(0.5) { "ASC" } else { "DESC" };
        steps.extend(argv(&["SORTBY", "2", &sort_key, order]));
    }
    if rng.random_ratio(1, 3) {
        steps.extend(argv(&["LIMIT", "0", &rng.random_range(1..20).to_string()]));
    }
    steps
}

fn random_key(schema: &Schema, rng: &mut impl Rng) -> Bytes {
    key(&schema.prefix, rng.random_range(0..KEYS))
}

/// A write to one key that sets at least one field.
fn write(schema: &Schema, vocabulary: &Vocabulary, rng: &mut impl Rng) -> Kind<Action> {
    let key = random_key(schema, rng);
    let mut values = Vec::new();
    for field in &schema.fields {
        if values.is_empty() || !rng.random_ratio(1, 4) {
            values.push((field, value(&field.kind, vocabulary, rng)));
        }
    }
    Kind::Command(crate::action::write(schema.on, key, &values))
}

/// `All` appears only as the whole query, since `*` inside a group is a
/// syntax error.
fn query(schema: &Schema, vocabulary: &Vocabulary, rng: &mut impl Rng, depth: u32) -> Query {
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
                query(schema, vocabulary, rng, depth - 1)
            }),
            field,
            k: rng.random_range(1..=10),
            vector: vector(dim, rng),
        };
    }
    let choices = if depth == 0 { 5 } else { 8 };
    match rng.random_range(0..choices) {
        0 if depth == QUERY_DEPTH => Query::All,
        2 => match field(FieldKind::Text, rng) {
            Some(field) => Query::InField {
                field,
                term: vocabulary.word(rng),
            },
            None => Query::Term(vocabulary.word(rng)),
        },
        3 => match field(FieldKind::Tag, rng) {
            Some(field) => Query::Tag {
                field,
                values: (0..rng.random_range(1..=2))
                    .map(|_| vocabulary.word(rng))
                    .collect(),
            },
            None => Query::Term(vocabulary.word(rng)),
        },
        4 => match field(FieldKind::Numeric, rng) {
            Some(field) => Query::Range {
                field,
                min: bound(vocabulary, rng),
                max: bound(vocabulary, rng),
            },
            None => Query::Term(vocabulary.word(rng)),
        },
        5 | 6 => {
            let children = (0..rng.random_range(2..=3))
                .map(|_| query(schema, vocabulary, rng, depth - 1))
                .collect();
            if rng.random_bool(0.5) {
                Query::And(children)
            } else {
                Query::Or(children)
            }
        }
        7 => Query::Not(Box::new(query(schema, vocabulary, rng, depth - 1))),
        _ => Query::Term(vocabulary.word(rng)),
    }
}

fn bound(vocabulary: &Vocabulary, rng: &mut impl Rng) -> Bound {
    match rng.random_range(0..3) {
        0 => Bound::Unbounded,
        1 => Bound::Inclusive(vocabulary.number(rng)),
        _ => Bound::Exclusive(vocabulary.number(rng)),
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

#[cfg(test)]
mod tests {
    use rand::SeedableRng;
    use rand::rngs::SmallRng;

    use super::*;
    use crate::mutate;

    const PROGRAMS: u64 = 30;

    #[test]
    fn mutants_round_trip_and_every_shrinking_step_is_strictly_smaller() {
        let mut rng = SmallRng::seed_from_u64(0);
        for _ in 0..PROGRAMS {
            let donor = generate(&mut rng);
            let program = mutate(&generate(&mut rng), &donor, &mut rng);
            let json = serde_json::to_string(&program).unwrap();
            assert_eq!(serde_json::from_str::<Program>(&json).unwrap(), program);
            for candidate in procfuzz::legacy::edit::smaller(&program) {
                assert!(candidate.size() < program.size(), "{candidate:?}");
            }
        }
    }
}
