/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Relations between search results that must hold whatever the data:
//!
//! - equivalence: the same query on the same index returns the same result;
//! - twin: the same query on two indexes with equal [`Key`]s does too;
//! - partition: `q p` and `q -p` add up to `q`.
//!
//! Results are compared only within one comparison window, which opens at a
//! barrier that completed and closes at the next step that can change results;
//! no such step may run while either query is in flight. `DEBUG RELOAD` must
//! not change results, but the index is rebuilding afterwards, so a window
//! opens only at the next barrier. Without `ON_TIMEOUT FAIL` a slow query
//! returns partial results, so no rule applies.

use std::collections::BTreeSet;

use procfuzz::program::{Bytes, Kind};
use procfuzz::run::{Step, Trace};
use procfuzz::server::Reply;

use crate::Program;
use crate::action::{Action, Query, QueryOptions};
use crate::redis::{DEBUG, Value};
use crate::scope::{Key, Scope};

/// RediSearch's default stopwords.
const STOPWORDS: [&str; 33] = [
    "a", "is", "the", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into",
    "it", "no", "not", "of", "on", "or", "such", "that", "their", "then", "there", "these", "they",
    "this", "to", "was", "will", "with",
];
const READ_ONLY_COMMANDS: [&str; 10] = [
    "FT.INFO",
    "FT._LIST",
    "FT.EXPLAIN",
    "FT.TAGVALS",
    "FT.SEARCH",
    "FT.AGGREGATE",
    "PING",
    "EXISTS",
    "HGETALL",
    "JSON.GET",
];
/// `_FT.DEBUG` subcommands that change scheduling, not results.
const SCHEDULING_HOOKS: [&str; 5] = [
    "GC_FORCEINVOKE",
    "GC_STOP_SCHEDULE",
    "GC_CONTINUE_SCHEDULE",
    "WORKERS",
    "BG_SCAN_CONTROLLER",
];

#[derive(Clone, Copy, PartialEq, Eq)]
enum Effect {
    Harness,
    KeepsResults,
    RebuildsIndex,
    ChangesResults,
}

struct SearchResult<'a> {
    window: u32,
    index: &'a Bytes,
    key: Option<Key>,
    query: &'a Query,
    options: &'a QueryOptions,
    total: i64,
    /// Present when the reply listed every match.
    keys: Option<BTreeSet<Bytes>>,
}

pub fn check(program: &Program, trace: &Trace<Value>) -> Vec<String> {
    let fails_on_timeout = program.args.windows(2).any(|pair| {
        pair[0].as_slice().eq_ignore_ascii_case(b"ON_TIMEOUT")
            && pair[1].as_slice().eq_ignore_ascii_case(b"FAIL")
    });
    if !fails_on_timeout {
        return Vec::new();
    }
    let results = comparable_results(program, trace);
    let mut findings = Vec::new();
    for (i, a) in results.iter().enumerate() {
        for b in &results[i + 1..] {
            if a.window != b.window || a.query != b.query || a.options != b.options {
                continue;
            }
            let rule = if a.index == b.index {
                "equivalence"
            } else if a.key.is_some() && a.key == b.key {
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
    for whole in &results {
        let parts: Vec<(&Query, bool, i64)> = results
            .iter()
            .filter(|r| {
                r.window == whole.window && r.index == whole.index && r.options == whole.options
            })
            .filter_map(|r| {
                let (rest, part, negated) = split(r.query)?;
                (rest == whole.query).then_some((part, negated, r.total))
            })
            .collect();
        for &(part, _, total) in parts.iter().filter(|(_, negated, _)| !negated) {
            let checkable = !vanishes(whole.query)
                && !vanishes(part)
                && known_fields(whole.query, &whole.key)
                && known_fields(part, &whole.key);
            let complement = parts.iter().find(|(p, negated, _)| *negated && *p == part);
            if checkable
                && let Some(&(_, _, complement)) = complement
                && total + complement != whole.total
            {
                findings.push(finding("partition", whole.query));
            }
        }
    }
    findings
}

/// `q p` is `(q, p, false)` and `q -p` is `(q, p, true)`; a lone `p` or `-p`
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

/// Names the query's structure without its values, so one bug reached
/// through different terms keeps one signature.
fn finding(rule: &str, query: &Query) -> String {
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
    format!("logic:{rule}:{}", shape(query))
}

/// Whether a term can drop out of its group: an empty term, a stopword, or
/// one the tokenizer splits. Then `q p` and `q -p` are both `q`.
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

/// Dialect 1 drops a clause on an unknown field instead of failing.
fn known_fields(query: &Query, key: &Option<Key>) -> bool {
    let Some((_, _, fields)) = key else {
        return false;
    };
    let known = |field: &Bytes| fields.iter().any(|(name, _)| name == field);
    match query {
        Query::InField { field, .. } | Query::Tag { field, .. } | Query::Range { field, .. } => {
            known(field)
        }
        Query::Knn { filter, field, .. } => known(field) && known_fields(filter, key),
        Query::And(children) | Query::Or(children) => {
            children.iter().all(|child| known_fields(child, key))
        }
        Query::Not(child) => known_fields(child, key),
        Query::All | Query::Term(_) => true,
    }
}

/// Exact `FT.SEARCH` results inside a comparison window, each with the key
/// of its index as the commands the server accepted left it. KNN results
/// are approximate and left out.
fn comparable_results<'a>(program: &'a Program, trace: &'a Trace<Value>) -> Vec<SearchResult<'a>> {
    let effects: Vec<Effect> = trace.steps.iter().map(effect).collect();
    let mut scope = Scope::new(&program.args);
    let mut window = 0;
    let mut open = false;
    let mut results = Vec::new();
    for (step, &effect) in trace.steps.iter().zip(&effects) {
        let succeeded = matches!(
            &step.reply,
            Some(Reply::Value(value)) if !matches!(value, Value::Error(_))
        );
        match effect {
            Effect::Harness if step.argv[0] == "BARRIER".into() => {
                open = step.reply == Some(Reply::Done);
            }
            Effect::Harness | Effect::KeepsResults => {}
            Effect::RebuildsIndex => open = false,
            Effect::ChangesResults => {
                window += 1;
                open = false;
            }
        }
        let op = &program.ops[step.op];
        if succeeded && effect != Effect::Harness {
            scope.add(op);
        }
        let undisturbed = step.received.is_some_and(|received| {
            trace.steps.iter().zip(&effects).all(|(other, effect)| {
                other.sent <= step.sent
                    || other.sent >= received
                    || matches!(effect, Effect::Harness | Effect::KeepsResults)
            })
        });
        if let Kind::Typed(Action::Query {
            index,
            query,
            options,
        }) = &op.kind
            && open
            && undisturbed
            && !matches!(query, Query::Knn { .. })
            && let Some((total, keys)) = step.reply.as_ref().and_then(search_result)
        {
            results.push(SearchResult {
                window,
                index,
                key: scope.key(index),
                query,
                options,
                total,
                keys,
            });
        }
    }
    results
}

fn effect(step: &Step<Value>) -> Effect {
    let word = |i: usize| {
        step.argv
            .get(i)
            .map(|w| String::from_utf8_lossy(w.as_slice()).to_ascii_uppercase())
            .unwrap_or_default()
    };
    let command = word(0);
    if step.is_harness() {
        Effect::Harness
    } else if READ_ONLY_COMMANDS.contains(&command.as_str())
        || (command == DEBUG && SCHEDULING_HOOKS.contains(&word(1).as_str()))
    {
        Effect::KeepsResults
    } else if command == "DEBUG" && word(1) == "RELOAD" {
        Effect::RebuildsIndex
    } else {
        Effect::ChangesResults
    }
}

/// `FT.SEARCH` replies `[total, key, fields?, key, fields?, …]`.
fn search_result(reply: &Reply<Value>) -> Option<(i64, Option<BTreeSet<Bytes>>)> {
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
    use procfuzz::program::argv;
    use procfuzz::run::{Exit, Status};

    use super::*;
    use crate::Op;
    use crate::action::{DataType, Field, FieldKind};

    /// The findings for a run in which every op succeeded and each query
    /// matched the paired number of documents.
    fn signatures(args: &[&str], ops: &[(Kind<Action>, i64)]) -> Vec<String> {
        let program = Program {
            args: argv(args),
            ops: ops
                .iter()
                .map(|(kind, _)| Op {
                    conn: Default::default(),
                    reply: Default::default(),
                    bind: None,
                    kind: kind.clone(),
                })
                .collect(),
        };
        let steps = ops
            .iter()
            .enumerate()
            .map(|(at, (kind, total))| {
                let clock = u32::try_from(2 * at).unwrap();
                let (argv, reply) = match kind {
                    Kind::Barrier => (argv(&["BARRIER"]), Reply::Done),
                    Kind::Typed(Action::Query { .. }) => (
                        kind.lower().remove(0),
                        Reply::Value(Value::Array(vec![Value::Int(*total)])),
                    ),
                    _ => (
                        kind.lower().remove(0),
                        Reply::Value(Value::Simple("OK".into())),
                    ),
                };
                Step {
                    op: at,
                    conn: Default::default(),
                    argv,
                    sent: clock,
                    received: Some(clock + 1),
                    reply: Some(reply),
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
        };
        check(&program, &trace)
    }

    #[test]
    fn each_rule_passes_consistent_results_and_flags_inconsistent_ones() {
        let create = |index: &str| {
            Kind::Typed(Action::Create {
                index: index.into(),
                on: DataType::Hash,
                prefix: "d:".into(),
                fields: vec![Field {
                    name: "t".into(),
                    kind: FieldKind::Text,
                    sortable: false,
                }],
            })
        };
        let search = |index: &str, query| {
            Kind::Typed(Action::Query {
                index: index.into(),
                query,
                options: QueryOptions::default(),
            })
        };
        let command = |words: &[&str]| Kind::Command(argv(words));
        let hello = || Query::Term("hello".into());
        let world = || Query::InField {
            field: "t".into(),
            term: "world".into(),
        };
        let setup = |extra: Vec<(Kind<Action>, i64)>| {
            let mut ops = vec![(create("i"), 0), (create("j"), 0), (Kind::Barrier, 0)];
            ops.extend(extra);
            ops
        };
        let partition = |rest| {
            setup(vec![
                (search("i", hello()), 5),
                (search("i", Query::And(vec![hello(), world()])), 2),
                (
                    search(
                        "i",
                        Query::And(vec![hello(), Query::Not(Box::new(world()))]),
                    ),
                    rest,
                ),
            ])
        };
        let repeated = |between, second| {
            setup(vec![
                (search("i", hello()), 3),
                (between, 0),
                (search("i", hello()), second),
            ])
        };
        let twin = |second| {
            setup(vec![
                (search("i", hello()), 3),
                (search("j", hello()), second),
            ])
        };
        let gc = || command(&[DEBUG, "GC_FORCEINVOKE", "i"]);
        let fail = ["ON_TIMEOUT", "FAIL"];
        let cases = [
            (signatures(&fail, &partition(3)), vec![]),
            (
                signatures(&fail, &partition(2)),
                vec!["logic:partition:term"],
            ),
            (signatures(&fail, &repeated(gc(), 3)), vec![]),
            (
                signatures(&fail, &repeated(gc(), 2)),
                vec!["logic:equivalence:term"],
            ),
            (signatures(&fail, &twin(3)), vec![]),
            (signatures(&fail, &twin(2)), vec!["logic:twin:term"]),
            (
                signatures(&fail, &repeated(command(&["HSET", "d:1", "t", "hello"]), 2)),
                vec![],
            ),
            (
                signatures(&fail, &repeated(command(&["DEBUG", "RELOAD"]), 2)),
                vec![],
            ),
            (signatures(&[], &twin(2)), vec![]),
        ];
        for (i, (found, expected)) in cases.into_iter().enumerate() {
            assert_eq!(found, expected, "case {i}");
        }
    }
}
