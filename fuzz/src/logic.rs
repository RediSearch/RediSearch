/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Logic rules: relations between replies that must hold whatever the data.
//!
//! Two replies are compared only when nothing that changes results can have
//! happened between them: both queries lie in one settled period, which starts
//! at a barrier that completed and ends at the next op that changes results,
//! and no such op was sent while either query was in flight. `DEBUG RELOAD`
//! must not change results, so it does not end a period, but it needs a
//! barrier before the next comparable query. Rules apply only when the server
//! fails a query that times out instead of returning partial results.

use std::collections::{BTreeMap, BTreeSet};

use crate::judge::{Finding, Kind};
use crate::program::{Bytes, DataType, Field, FieldKind, OpKind, Program, Query, QueryOptions};
use crate::resp::Value;
use crate::run::{Step, Trace};
use crate::target::Reply;

/// RediSearch's default stopwords; see [`vanishes`].
const STOPWORDS: [&str; 33] = [
    "a", "is", "the", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into",
    "it", "no", "not", "of", "on", "or", "such", "that", "their", "then", "there", "these", "they",
    "this", "to", "was", "will", "with",
];

/// Commands that cannot change what a query returns.
const NEUTRAL_COMMANDS: [&str; 12] = [
    "FT.INFO",
    "FT._LIST",
    "FT.EXPLAIN",
    "FT.EXPLAINCLI",
    "FT.TAGVALS",
    "FT.SEARCH",
    "FT.AGGREGATE",
    "PING",
    "EXISTS",
    "HGETALL",
    "JSON.GET",
    "DBSIZE",
];

/// `_FT.DEBUG` subcommands that change scheduling, not results.
const NEUTRAL_HOOKS: [&str; 7] = [
    "GC_FORCEINVOKE",
    "GC_FORCEBGINVOKE",
    "GC_STOP_SCHEDULE",
    "GC_CONTINUE_SCHEDULE",
    "GC_WAIT_FOR_JOBS",
    "WORKERS",
    "BG_SCAN_CONTROLLER",
];

#[derive(Clone, Copy, PartialEq, Eq)]
enum Effect {
    Neutral,
    /// Must not change results, but leaves the index rebuilding.
    Transparent,
    Changing,
}

/// A schema as the server confirmed it. `sortable` is left out: it does not
/// change which documents match, so indexes differing only in it are twins.
type Schema = (DataType, Bytes, BTreeSet<(Bytes, FieldKind)>);

struct Observation<'a> {
    period: u32,
    index: &'a Bytes,
    schema: Option<Schema>,
    query: &'a Query,
    options: &'a QueryOptions,
    total: i64,
    /// The matching keys, when the reply listed all of them.
    keys: Option<BTreeSet<Bytes>>,
}

pub fn check(program: &Program, trace: &Trace) -> Vec<Finding> {
    if !fails_on_timeout(&program.module_args) {
        return Vec::new();
    }
    let seen = observations(program, trace);
    let mut findings = Vec::new();
    for (i, a) in seen.iter().enumerate() {
        for b in &seen[i + 1..] {
            if a.period != b.period || a.query != b.query || a.options != b.options {
                continue;
            }
            let rule = if a.index == b.index {
                "equivalence"
            } else if a.schema.is_some() && a.schema == b.schema {
                "twin"
            } else {
                continue;
            };
            let keys_differ = matches!((&a.keys, &b.keys), (Some(x), Some(y)) if x != y);
            if a.total != b.total || keys_differ {
                findings.push(finding(rule, a.query));
            }
        }
    }
    for whole in &seen {
        let parts = |pick: fn(&Query) -> Option<(&Query, &Query, bool)>| {
            seen.iter().filter_map(move |o| {
                let (rest, p, negated) = pick(o.query)?;
                (o.period == whole.period
                    && o.index == whole.index
                    && o.options == whole.options
                    && rest == whole.query)
                    .then_some((p, negated, o.total))
            })
        };
        for (p, negated, total) in parts(split) {
            if negated
                || vanishes(whole.query)
                || vanishes(p)
                || !known_fields(whole.query, &whole.schema)
                || !known_fields(p, &whole.schema)
            {
                continue;
            }
            let complement = parts(split)
                .find(|(other, negated, _)| *negated && *other == p)
                .map(|(_, _, total)| total);
            if let Some(complement) = complement
                && total + complement != whole.total
            {
                findings.push(finding("partition", whole.query));
            }
        }
    }
    findings
}

/// `q p` as `(q, p, false)` and `q -p` as `(q, p, true)`; a lone `p` or `-p`
/// splits `*`.
fn split(query: &Query) -> Option<(&Query, &Query, bool)> {
    const ALL: &Query = &Query::All;
    let (rest, part) = match query {
        Query::And(children) => match children.as_slice() {
            [rest, part] => (rest, part),
            _ => return None,
        },
        part => (ALL, part),
    };
    Some(match part {
        Query::Not(p) => (rest, &**p, true),
        p => (rest, p, false),
    })
}

fn finding(rule: &str, query: &Query) -> Finding {
    Finding {
        kind: Kind::Logic,
        signature: format!("logic:{rule}:{}", shape(query)),
    }
}

/// The query's structure without its values, so one bug found through
/// different terms keeps one signature.
fn shape(query: &Query) -> String {
    let group = |name, children: &[Query]| {
        let inner: Vec<String> = children.iter().map(shape).collect();
        format!("{name}({})", inner.join(","))
    };
    match query {
        Query::All => "all".into(),
        Query::Term(_) => "term".into(),
        Query::InField { .. } => "in_field".into(),
        Query::Tag { .. } => "tag".into(),
        Query::Range { .. } => "range".into(),
        Query::And(children) => group("and", children),
        Query::Or(children) => group("or", children),
        Query::Not(child) => format!("not({})", shape(child)),
        Query::Knn { filter, .. } => format!("knn({})", shape(filter)),
    }
}

fn fails_on_timeout(module_args: &[Bytes]) -> bool {
    module_args.windows(2).any(|pair| {
        pair[0].as_slice().eq_ignore_ascii_case(b"ON_TIMEOUT")
            && pair[1].as_slice().eq_ignore_ascii_case(b"FAIL")
    })
}

/// Whether the query has a term that can become an empty node: an empty
/// term, a stopword, or one the tokenizer would split. An empty node drops
/// out of its group, so `q p` and `q -p` both become `q` and a partition
/// over it does not add up.
fn vanishes(query: &Query) -> bool {
    match query {
        Query::Term(term) | Query::InField { term, .. } => {
            let term = term.as_slice();
            term.is_empty()
                || term
                    .iter()
                    .any(|&b| b.is_ascii() && !(b.is_ascii_alphanumeric() || b == b'_'))
                || STOPWORDS
                    .iter()
                    .any(|w| term.eq_ignore_ascii_case(w.as_bytes()))
        }
        Query::And(children) | Query::Or(children) => children.iter().any(vanishes),
        Query::Not(child) | Query::Knn { filter: child, .. } => vanishes(child),
        Query::All | Query::Tag { .. } | Query::Range { .. } => false,
    }
}

/// Whether every field the query names is in the confirmed schema. Dialect 1
/// drops a clause on an unknown field instead of failing the query.
fn known_fields(query: &Query, schema: &Option<Schema>) -> bool {
    let Some((_, _, fields)) = schema else {
        return false;
    };
    let known = |field: &Bytes| fields.iter().any(|(name, _)| name == field);
    match query {
        Query::InField { field, .. } | Query::Tag { field, .. } | Query::Range { field, .. } => {
            known(field)
        }
        Query::Knn { filter, field, .. } => known(field) && known_fields(filter, schema),
        Query::And(children) | Query::Or(children) => {
            children.iter().all(|child| known_fields(child, schema))
        }
        Query::Not(child) => known_fields(child, schema),
        Query::All | Query::Term(_) => true,
    }
}

fn exact(query: &Query) -> bool {
    match query {
        Query::Knn { .. } => false,
        Query::And(children) | Query::Or(children) => children.iter().all(exact),
        Query::Not(child) => exact(child),
        _ => true,
    }
}

fn observations<'a>(program: &'a Program, trace: &'a Trace) -> Vec<Observation<'a>> {
    let mut schemas: BTreeMap<&Bytes, Schema> = BTreeMap::new();
    let mut period = 0;
    let mut settled = false;
    let mut seen = Vec::new();
    for (i, step) in trace.steps.iter().enumerate() {
        let succeeded = matches!(
            &step.reply,
            Some(Reply::Value(value)) if !matches!(value, Value::Error(_))
        );
        if marker(step) {
            if step.argv[0] == "BARRIER".into() {
                settled = succeeded;
            }
            continue;
        }
        let op = &program.ops[step.op].kind;
        match effect(op, step) {
            Effect::Changing => {
                period += 1;
                settled = false;
            }
            Effect::Transparent => settled = false,
            Effect::Neutral => {}
        }
        if succeeded {
            confirm(op, &mut schemas);
        }
        if let OpKind::Query {
            index,
            query,
            options,
        } = op
            && settled
            && exact(query)
            && quiet(program, trace, i)
            && let Some((total, keys)) = step.reply.as_ref().and_then(result)
        {
            seen.push(Observation {
                period,
                index,
                schema: schemas.get(index).cloned(),
                query,
                options,
                total,
                keys,
            });
        }
    }
    seen
}

/// Whether no op that is not neutral was sent while step `i` was in flight.
fn quiet(program: &Program, trace: &Trace, i: usize) -> bool {
    let step = &trace.steps[i];
    let Some(received) = step.received else {
        return false;
    };
    trace.steps.iter().all(|other| {
        other.sent <= step.sent
            || other.sent >= received
            || marker(other)
            || effect(&program.ops[other.op].kind, other) == Effect::Neutral
    })
}

/// A step the harness records for its own barrier or release.
fn marker(step: &Step) -> bool {
    matches!(step.argv.as_slice(), [word] if *word == "BARRIER".into() || *word == "RELEASE".into())
}

fn effect(op: &OpKind, step: &Step) -> Effect {
    match op {
        OpKind::Query { .. }
        | OpKind::Aggregate { .. }
        | OpKind::Cursor { .. }
        | OpKind::Barrier => Effect::Neutral,
        OpKind::Create { .. }
        | OpKind::Alter { .. }
        | OpKind::Populate { .. }
        | OpKind::Command { .. } => command_effect(step),
    }
}

fn command_effect(step: &Step) -> Effect {
    let word = |i: usize| {
        step.argv
            .get(i)
            .map(|w| String::from_utf8_lossy(w.as_slice()).to_ascii_uppercase())
            .unwrap_or_default()
    };
    let command = word(0);
    if NEUTRAL_COMMANDS.contains(&command.as_str())
        || (command == "_FT.DEBUG" && NEUTRAL_HOOKS.contains(&word(1).as_str()))
    {
        Effect::Neutral
    } else if command == "DEBUG" && word(1) == "RELOAD" {
        Effect::Transparent
    } else {
        Effect::Changing
    }
}

/// Folds a step the server accepted into the schemas it confirms.
fn confirm<'a>(op: &'a OpKind, schemas: &mut BTreeMap<&'a Bytes, Schema>) {
    let fields = |fields: &[Field]| -> BTreeSet<(Bytes, FieldKind)> {
        fields
            .iter()
            .map(|f| (f.name.clone(), f.kind.clone()))
            .collect()
    };
    match op {
        OpKind::Create {
            index,
            on,
            prefix,
            fields: declared,
        } => {
            schemas.insert(index, (*on, prefix.clone(), fields(declared)));
        }
        OpKind::Alter { index, field, .. } => {
            if let Some((_, _, fields)) = schemas.get_mut(index) {
                fields.insert((field.name.clone(), field.kind.clone()));
            }
        }
        OpKind::Command { argv } => {
            if let [command, index, ..] = argv.as_slice()
                && command.as_slice().eq_ignore_ascii_case(b"FT.DROPINDEX")
            {
                schemas.retain(|name, _| *name != index);
            }
        }
        _ => {}
    }
}

/// The total and, when the reply lists every match, the keys of an
/// `FT.SEARCH` reply: `[total, key, fields?, key, fields?, …]`.
fn result(reply: &Reply) -> Option<(i64, Option<BTreeSet<Bytes>>)> {
    let Reply::Value(Value::Array(items)) = reply else {
        return None;
    };
    let (Value::Int(total), rest) = items.split_first()? else {
        return None;
    };
    let keys: BTreeSet<Bytes> = rest
        .iter()
        .filter_map(|item| match item {
            Value::Bulk(key) => Some(key.clone()),
            _ => None,
        })
        .collect();
    let complete = usize::try_from(*total).is_ok_and(|total| total == keys.len());
    Some((*total, complete.then_some(keys)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coverage::Coverage;
    use crate::lower::{Cursors, lower};
    use crate::program::{FORMAT_VERSION, Op, argv};
    use crate::run::Exit;
    use crate::target::Status;

    fn create(index: &str) -> OpKind {
        OpKind::Create {
            index: index.into(),
            on: DataType::Hash,
            prefix: "d:".into(),
            fields: vec![Field {
                name: "t".into(),
                kind: FieldKind::Text,
                sortable: false,
            }],
        }
    }

    fn search(index: &str, query: Query) -> OpKind {
        OpKind::Query {
            index: index.into(),
            query,
            options: QueryOptions::default(),
        }
    }

    fn command(words: &[&str]) -> OpKind {
        OpKind::Command { argv: argv(words) }
    }

    fn term(word: &str) -> Query {
        Query::Term(word.into())
    }

    /// Runs the rules over a trace in which each op got `replies[i]`, or a
    /// count of `totals[i]` matches for a query.
    fn signatures(fail: bool, ops: &[(OpKind, i64)]) -> Vec<String> {
        let program = Program {
            version: FORMAT_VERSION,
            module_args: if fail {
                argv(&["ON_TIMEOUT", "FAIL"])
            } else {
                Vec::new()
            },
            ops: ops
                .iter()
                .map(|(kind, _)| Op {
                    conn: Default::default(),
                    reply: Default::default(),
                    kind: kind.clone(),
                })
                .collect(),
        };
        let steps = ops
            .iter()
            .enumerate()
            .map(|(at, (kind, total))| {
                let clock = u32::try_from(2 * at).unwrap();
                let (argv, value) = match kind {
                    OpKind::Barrier => (argv(&["BARRIER"]), Value::Simple("OK".into())),
                    OpKind::Query { .. } => (
                        lower(kind, &Cursors::new()).remove(0),
                        Value::Array(vec![Value::Int(*total)]),
                    ),
                    _ => (
                        lower(kind, &Cursors::new()).remove(0),
                        Value::Simple("OK".into()),
                    ),
                };
                Step {
                    op: at,
                    conn: Default::default(),
                    argv,
                    sent: clock,
                    received: Some(clock + 1),
                    reply: Some(Reply::Value(value)),
                }
            })
            .collect();
        let trace = Trace {
            steps,
            exit: Exit::Clean,
            status: Status {
                code: Some(0),
                signal: None,
            },
            log: Bytes::default(),
            coverage: Coverage::default(),
        };
        check(&program, &trace)
            .into_iter()
            .map(|f| f.signature)
            .collect()
    }

    #[test]
    fn each_rule_passes_consistent_replies_and_flags_doctored_ones() {
        let hello = || search("i", term("hello"));
        let both = || {
            search(
                "i",
                Query::And(vec![
                    term("hello"),
                    Query::InField {
                        field: "t".into(),
                        term: "world".into(),
                    },
                ]),
            )
        };
        let only = || {
            search(
                "i",
                Query::And(vec![
                    term("hello"),
                    Query::Not(Box::new(Query::InField {
                        field: "t".into(),
                        term: "world".into(),
                    })),
                ]),
            )
        };
        let partition = |rest| {
            vec![
                (create("i"), 0),
                (OpKind::Barrier, 0),
                (hello(), 5),
                (both(), 2),
                (only(), rest),
            ]
        };
        let equivalence = |between: OpKind, second| {
            vec![
                (create("i"), 0),
                (OpKind::Barrier, 0),
                (hello(), 3),
                (between, 0),
                (hello(), second),
            ]
        };
        let gc = || command(&["_FT.DEBUG", "GC_FORCEINVOKE", "i"]);
        let twin = |second| {
            vec![
                (create("i"), 0),
                (create("j"), 0),
                (OpKind::Barrier, 0),
                (hello(), 3),
                (search("j", term("hello")), second),
            ]
        };
        let none: [&str; 0] = [];
        assert_eq!(signatures(true, &partition(3)), none);
        assert_eq!(signatures(true, &partition(2)), ["logic:partition:term"]);
        assert_eq!(signatures(true, &equivalence(gc(), 3)), none);
        assert_eq!(
            signatures(true, &equivalence(gc(), 2)),
            ["logic:equivalence:term"]
        );
        assert_eq!(
            signatures(
                true,
                &equivalence(command(&["HSET", "d:1", "t", "hello"]), 2)
            ),
            none,
            "a write ends the period"
        );
        assert_eq!(
            signatures(true, &equivalence(command(&["DEBUG", "RELOAD"]), 2)),
            none,
            "a reload needs a barrier before the next comparison"
        );
        assert_eq!(signatures(true, &twin(3)), none);
        assert_eq!(signatures(true, &twin(2)), ["logic:twin:term"]);
        assert_eq!(
            signatures(false, &twin(2)),
            none,
            "partial results on timeout make every rule inconclusive"
        );
    }
}
