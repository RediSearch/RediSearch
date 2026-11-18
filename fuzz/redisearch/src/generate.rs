/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Ops that make sense after what the ops before them set up. Every name an
//! op uses, such as an index or a cursor slot, comes from the state.

use procfuzz::draw::Draw;
use procfuzz_redis::resp::{Argv, Bytes, argv};

use crate::effect::DEBUG;
use crate::op::{
    Action, Algorithm, Bound, CursorSlot, DataType, Field, FieldKind, Metric, Op, Query,
    QueryOptions, key, number, value, vector, word, write,
};
use crate::state::{Schema, Search, State};

const CONNS: u64 = 3;
const CURSOR_SLOTS: u64 = 2;
const MOST_CURSOR_COUNT: u64 = 10;
const MOST_POPULATE_BITS: u64 = 9;
/// Lists every match of a comparable search.
const COMPARABLE_LIMIT: u32 = 1000;
const VECTOR_DIMS: [u16; 3] = [2, 4, 8];
const KEYS: u64 = 16;
const QUERY_DEPTH: u32 = 2;
const INDEXES: [&str; 3] = ["i0", "i1", "i2"];
const HASH_PREFIXES: [&str; 2] = ["d0:", "d1:"];
const JSON_PREFIXES: [&str; 2] = ["j0:", "j1:"];
const FIELDS: [&str; 5] = ["f0", "f1", "f2", "f3", "f4"];
const BOOT_ARGS: [[&str; 2]; 2] = [["ON_TIMEOUT", "FAIL"], ["WORKERS", "2"]];
/// Hooks into background work; `{index}` stands for the index.
const HOOKS: [&[&str]; 5] = [
    &[DEBUG, "GC_FORCEINVOKE", "{index}"],
    &[DEBUG, "GC_STOP_SCHEDULE", "{index}"],
    &[DEBUG, "BG_SCAN_CONTROLLER", "SET_PAUSE_BEFORE_SCAN", "true"],
    &[DEBUG, "BG_SCAN_CONTROLLER", "SET_BG_INDEX_RESUME"],
    &["DEBUG", "RELOAD"],
];

pub fn next(state: &State, draw: &mut Draw) -> Op {
    if !state.started {
        // Usually a query that times out fails, as the logic rules need.
        let boot = match draw.below(4) {
            0 => None,
            1 | 2 => Some(BOOT_ARGS[0]),
            _ => Some(BOOT_ARGS[1]),
        };
        if let Some(args) = boot {
            let args = args.iter().map(|&arg| arg.to_owned()).collect();
            return Op::new(Action::Boot { args });
        }
    }
    if state.paused && draw.one_in(3) {
        return Op::new(Action::command(&[DEBUG, "WORKERS", "RESUME"]));
    }
    if let Some(comparable) = &state.comparable
        && let Some(schema) = state.indexes.get(&comparable.index)
        && draw.one_in(2)
    {
        return Op::new(related(state, comparable, schema, draw));
    }
    let indexes: Vec<(&Bytes, &Schema)> = state.indexes.iter().collect();
    let Some(&(name, schema)) = draw.pick(&indexes) else {
        return Op::new(create(state, draw));
    };
    let action = match draw.below(20) {
        0 => Action::Settle,
        1..=6 => write_key(schema, draw),
        7 => Action::Populate {
            on: schema.on,
            prefix: schema.prefix.clone(),
            fields: schema.fields.clone(),
            n: 1 << draw.below(MOST_POPULATE_BITS + 1) | u32::from(draw.one_in(2)),
            seed: draw.raw(),
        },
        8..=11 => {
            let options = if draw.one_in(2) {
                comparable_options(draw)
            } else {
                options(draw)
            };
            Action::Query {
                index: name.clone(),
                query: query(schema, draw, QUERY_DEPTH),
                options,
            }
        }
        12 => aggregate(name, schema, draw),
        13 => {
            let slots: Vec<u8> = state.cursors.keys().copied().collect();
            match draw.pick(&slots).copied() {
                Some(slot) if draw.one_in(5) => Action::CursorDel { slot },
                Some(slot) => Action::CursorRead {
                    slot,
                    count: draw
                        .one_in(2)
                        .then(|| small(draw.between(1, MOST_CURSOR_COUNT))),
                },
                None => aggregate(name, schema, draw),
            }
        }
        14 => {
            let key = key(&schema.prefix, small(draw.below(KEYS)));
            Action::Command {
                argv: vec!["DEL".into(), key].try_into().expect("not empty"),
            }
        }
        15 => create(state, draw),
        16 => {
            let mut drop = vec!["FT.DROPINDEX".into(), name.clone()];
            if draw.one_in(2) {
                drop.push("DD".into());
            }
            command(drop)
        }
        17 if state.workers && !state.paused => Action::command(&[DEBUG, "WORKERS", "PAUSE"]),
        17 => {
            let unused = FIELDS
                .iter()
                .find(|&&f| schema.fields.iter().all(|s| s.name != f.into()));
            Action::Alter {
                index: name.clone(),
                on: schema.on,
                field: field(unused.unwrap_or(&FIELDS[0]), draw),
            }
        }
        _ => {
            let hook = HOOKS[draw.index(HOOKS.len())];
            command(
                hook.iter()
                    .map(|&word| match word {
                        "{index}" => name.clone(),
                        word => word.into(),
                    })
                    .collect(),
            )
        }
    };
    let searches = matches!(action, Action::Query { .. } | Action::Aggregate { .. });
    let conn = if draw.one_in(2) {
        tiny(draw.below(CONNS))
    } else {
        0
    };
    Op {
        conn,
        // A search waits on paused workers, so awaiting it would stall the
        // program.
        defer: (searches && state.paused) || draw.one_in(8),
        action,
    }
}

/// A search the logic rules can relate to the last comparable one: the same
/// query again, on the same index or a twin, with a part added, or its
/// complement; or something that should not change its results.
fn related(state: &State, last: &Search, schema: &Schema, draw: &mut Draw) -> Action {
    let search = |index: &Bytes, query: Query| Action::Query {
        index: index.clone(),
        query,
        options: last.options.clone(),
    };
    match draw.below(6) {
        0 => search(&last.index, last.query.clone()),
        1 => search(
            state.twin(&last.index).unwrap_or(&last.index),
            last.query.clone(),
        ),
        2 if last.query != Query::All => search(&last.index, complement(&last.query)),
        2 | 3 => {
            let part = query(schema, draw, 0);
            let refined = match &last.query {
                Query::All => part,
                query => Query::And(vec![query.clone(), part]),
            };
            search(&last.index, refined)
        }
        4 => Action::Settle,
        _ => {
            let mut gc = argv(&[DEBUG, "GC_FORCEINVOKE"]);
            gc.push(last.index.clone());
            command(gc)
        }
    }
}

/// `q -p` for `q p`, and `-q` for a lone `q`: with the original, either
/// splits what is left without the part.
fn complement(query: &Query) -> Query {
    let negate = |query: &Query| match query {
        Query::Not(inner) => (**inner).clone(),
        query => Query::Not(Box::new(query.clone())),
    };
    match query {
        Query::And(children) if children.len() == 2 => {
            Query::And(vec![children[0].clone(), negate(&children[1])])
        }
        query => negate(query),
    }
}

fn command(argv: Argv) -> Action {
    Action::Command {
        argv: argv.try_into().expect("not empty"),
    }
}

/// A new index, or a third of the time a twin of an existing one with
/// `SORTABLE` drawn afresh.
fn create(state: &State, draw: &mut Draw) -> Action {
    let unused: Vec<&str> = INDEXES
        .iter()
        .copied()
        .filter(|&name| !state.indexes.contains_key(&name.into()))
        .collect();
    let index: Bytes = (*draw.pick(&unused).unwrap_or_else(|| &INDEXES[0])).into();
    let schemas: Vec<&Schema> = state.indexes.values().collect();
    if let Some(original) = draw.pick(&schemas)
        && draw.one_in(3)
    {
        return Action::Create {
            index,
            on: original.on,
            prefix: original.prefix.clone(),
            fields: original
                .fields
                .iter()
                .map(|f| Field {
                    sortable: sortable(&f.kind, draw),
                    ..f.clone()
                })
                .collect(),
        };
    }
    let (on, prefixes) = if draw.one_in(3) {
        (DataType::Json, JSON_PREFIXES)
    } else {
        (DataType::Hash, HASH_PREFIXES)
    };
    let prefix = prefixes[draw.index(prefixes.len())].into();
    let mut names = FIELDS.to_vec();
    let count = draw.between(1, 4);
    let fields = (0..count)
        .map(|_| {
            let name = names.remove(draw.index(names.len()));
            field(name, draw)
        })
        .collect();
    Action::Create {
        index,
        on,
        prefix,
        fields,
    }
}

fn field(name: &str, draw: &mut Draw) -> Field {
    let kind = match draw.below(5) {
        0 => FieldKind::Text,
        1 => FieldKind::Numeric,
        2 => FieldKind::Tag,
        3 => FieldKind::Geo,
        _ => FieldKind::Vector {
            algorithm: [Algorithm::Flat, Algorithm::Hnsw][draw.index(2)],
            dim: VECTOR_DIMS[draw.index(VECTOR_DIMS.len())],
            metric: [Metric::L2, Metric::Ip, Metric::Cosine][draw.index(3)],
        },
    };
    Field {
        name: name.into(),
        sortable: sortable(&kind, draw),
        kind,
    }
}

fn sortable(kind: &FieldKind, draw: &mut Draw) -> bool {
    matches!(kind, FieldKind::Text | FieldKind::Numeric | FieldKind::Tag) && draw.one_in(4)
}

fn aggregate(index: &Bytes, schema: &Schema, draw: &mut Draw) -> Action {
    Action::Aggregate {
        index: index.clone(),
        query: query(schema, draw, QUERY_DEPTH),
        pipeline: pipeline(schema, draw),
        cursor: draw.one_in(3).then(|| CursorSlot {
            slot: tiny(draw.below(CURSOR_SLOTS)),
            count: small(draw.between(1, MOST_CURSOR_COUNT)),
        }),
    }
}

/// Aggregation steps in a fixed order. After `GROUPBY` only its key and
/// `count` remain to sort by.
fn pipeline(schema: &Schema, draw: &mut Draw) -> Argv {
    let reference = |field: &Field| format!("@{}", String::from_utf8_lossy(field.name.as_slice()));
    let any = draw.pick(&schema.fields).map(reference);
    let numeric: Vec<&Field> = schema
        .fields
        .iter()
        .filter(|f| f.kind == FieldKind::Numeric)
        .collect();
    let numeric = draw.pick(&numeric).map(|field| reference(field));
    let mut steps = Vec::new();
    if let Some(numeric) = &numeric {
        if draw.one_in(3) {
            steps.extend(argv(&["APPLY", &format!("{numeric} * 2"), "AS", "doubled"]));
        }
        if draw.one_in(3) {
            let bound = number(draw);
            steps.extend(argv(&["FILTER", &format!("{numeric} > {bound}")]));
        }
    }
    let mut sort_key = any.clone();
    if let Some(any) = &any
        && draw.one_in(3)
    {
        steps.extend(argv(&["GROUPBY", "1", any, "REDUCE", "COUNT", "0", "AS"]));
        steps.push("count".into());
        sort_key = Some("@count".into());
    }
    if let Some(sort_key) = sort_key
        && draw.one_in(3)
    {
        let order = if draw.one_in(2) { "DESC" } else { "ASC" };
        steps.extend(argv(&["SORTBY", "2", &sort_key, order]));
    }
    if draw.one_in(3) {
        steps.extend(argv(&["LIMIT", "0", &draw.between(1, 19).to_string()]));
    }
    steps
}

/// A write to one key that sets at least one field.
fn write_key(schema: &Schema, draw: &mut Draw) -> Action {
    let key = key(&schema.prefix, small(draw.below(KEYS)));
    let mut values = Vec::new();
    for field in &schema.fields {
        if values.is_empty() || !draw.one_in(4) {
            values.push((field, value(&field.kind, draw)));
        }
    }
    command(write(schema.on, key, &values))
}

/// `All` appears only as the whole query, since `*` inside a group is a
/// syntax error.
fn query(schema: &Schema, draw: &mut Draw, depth: u32) -> Query {
    let field = |kind: FieldKind, draw: &mut Draw| {
        let fields: Vec<&Field> = schema.fields.iter().filter(|f| f.kind == kind).collect();
        draw.pick(&fields).map(|f| f.name.clone())
    };
    let vector_field = schema.fields.iter().find_map(|f| match f.kind {
        FieldKind::Vector { dim, .. } => Some((f.name.clone(), dim)),
        _ => None,
    });
    if let Some((field, dim)) = vector_field
        && depth == QUERY_DEPTH
        && draw.one_in(3)
    {
        let filter = if draw.one_in(2) {
            query(schema, draw, depth - 1)
        } else {
            Query::All
        };
        return Query::Knn {
            filter: Box::new(filter),
            field,
            k: small(draw.between(1, 10)),
            vector: vector(dim, draw),
        };
    }
    let choices = if depth == 0 { 5 } else { 8 };
    match draw.below(choices) {
        0 if depth == QUERY_DEPTH => Query::All,
        2 => match field(FieldKind::Text, draw) {
            Some(field) => Query::InField {
                field,
                term: word(draw),
            },
            None => Query::Term(word(draw)),
        },
        3 => match field(FieldKind::Tag, draw) {
            Some(field) => Query::Tag {
                field,
                values: (0..draw.between(1, 2)).map(|_| word(draw)).collect(),
            },
            None => Query::Term(word(draw)),
        },
        4 => match field(FieldKind::Numeric, draw) {
            Some(field) => Query::Range {
                field,
                min: bound(draw),
                max: bound(draw),
            },
            None => Query::Term(word(draw)),
        },
        5 | 6 => {
            let children = (0..draw.between(2, 3))
                .map(|_| query(schema, draw, depth - 1))
                .collect();
            if draw.one_in(2) {
                Query::Or(children)
            } else {
                Query::And(children)
            }
        }
        7 => Query::Not(Box::new(query(schema, draw, depth - 1))),
        _ => Query::Term(word(draw)),
    }
}

fn bound(draw: &mut Draw) -> Bound {
    match draw.below(3) {
        0 => Bound::Unbounded,
        1 => Bound::Inclusive(number(draw)),
        _ => Bound::Exclusive(number(draw)),
    }
}

fn options(draw: &mut Draw) -> QueryOptions {
    QueryOptions {
        no_content: draw.one_in(2),
        limit: draw
            .one_in(4)
            .then(|| (small(draw.below(10)), small(draw.between(1, 19)))),
        dialect: draw
            .one_in(2)
            .then(|| u8::try_from(draw.between(1, 4)).expect("small")),
    }
}

/// Options under which a search lists every match.
fn comparable_options(draw: &mut Draw) -> QueryOptions {
    QueryOptions {
        no_content: true,
        limit: Some((0, COMPARABLE_LIMIT)),
        dialect: draw.one_in(2).then_some(2),
    }
}

fn small(value: u64) -> u32 {
    u32::try_from(value).expect("small")
}

fn tiny(value: u64) -> u8 {
    u8::try_from(value).expect("tiny")
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use procfuzz::commands::load;
    use procfuzz::edit::mutate;
    use procfuzz::model::generate;

    use super::*;
    use crate::{Program, RediSearch};

    const PROGRAMS: usize = 30;
    const PROGRAM_OPS: usize = 30;

    #[test]
    fn generated_programs_and_their_mutants_round_trip() {
        let mut draw = Draw::new(0);
        for _ in 0..PROGRAMS {
            let donor = generate(&RediSearch, &mut draw, PROGRAM_OPS);
            let parent = generate(&RediSearch, &mut draw, PROGRAM_OPS);
            let program = mutate(&RediSearch, &parent, &donor, &mut draw);
            let json = serde_json::to_string(&program).unwrap();
            assert_eq!(serde_json::from_str::<Program>(&json).unwrap(), program);
        }
    }

    #[test]
    fn every_program_file_loads() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        for dir in ["seeds", "tests/programs", "planted"] {
            for entry in std::fs::read_dir(root.join(dir)).unwrap() {
                let path = entry.unwrap().path();
                if path.extension().is_some_and(|e| e == "json") {
                    load::<Op>(&path).unwrap();
                }
            }
        }
    }
}
