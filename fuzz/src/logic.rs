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

use std::collections::BTreeSet;

use crate::judge::{Finding, Kind};
use crate::program::{Bytes, OpKind, Program, Query, QueryOptions};
use crate::resp::Value;
use crate::run::{Step, Trace};
use crate::scope::{Key, Scope};
use crate::target::Reply;

/// RediSearch's default stopwords; see [`vanishes`].
const STOPWORDS: [&str; 33] = [
    "a", "is", "the", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into",
    "it", "no", "not", "of", "on", "or", "such", "that", "their", "then", "there", "these", "they",
    "this", "to", "was", "will", "with",
];

/// Commands that cannot change what a query returns.
const NEUTRAL_COMMANDS: [&str; 10] = [
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
const NEUTRAL_HOOKS: [&str; 5] = [
    "GC_FORCEINVOKE",
    "GC_STOP_SCHEDULE",
    "GC_CONTINUE_SCHEDULE",
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

struct Observation<'a> {
    period: u32,
    index: &'a Bytes,
    key: Option<Key>,
    query: &'a Query,
    options: &'a QueryOptions,
    total: i64,
    /// The matching keys, when the reply listed all of them.
    keys: Option<BTreeSet<Bytes>>,
}

pub fn check(program: &Program, trace: &Trace) -> Vec<Finding> {
    let fails_on_timeout = program.module_args.windows(2).any(|pair| {
        pair[0].as_slice().eq_ignore_ascii_case(b"ON_TIMEOUT")
            && pair[1].as_slice().eq_ignore_ascii_case(b"FAIL")
    });
    if !fails_on_timeout {
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
    for whole in &seen {
        let comparable = |o: &&Observation| {
            o.period == whole.period && o.index == whole.index && o.options == whole.options
        };
        let parts: Vec<(&Query, bool, i64)> = seen
            .iter()
            .filter(comparable)
            .filter_map(|o| {
                let (rest, p, negated) = split(o.query)?;
                (rest == whole.query).then_some((p, negated, o.total))
            })
            .collect();
        for &(p, _, total) in parts.iter().filter(|(_, negated, _)| !negated) {
            let checkable = !vanishes(whole.query)
                && !vanishes(p)
                && known_fields(whole.query, &whole.key)
                && known_fields(p, &whole.key);
            let complement = parts.iter().find(|(q, negated, _)| *negated && *q == p);
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

/// The signature names the rule and the query's structure without its
/// values, so one bug found through different terms keeps one signature.
fn finding(rule: &str, query: &Query) -> Finding {
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
    Finding {
        kind: Kind::Logic,
        signature: format!("logic:{rule}:{}", shape(query)),
    }
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

/// The settled, exact search replies, each with its period and the schema of
/// its index at the time, from folding the ops the server accepted.
fn observations<'a>(program: &'a Program, trace: &'a Trace) -> Vec<Observation<'a>> {
    let effects: Vec<Option<Effect>> = trace.steps.iter().map(effect).collect();
    let mut scope = Scope::new(&program.module_args);
    let mut period = 0;
    let mut settled = false;
    let mut seen = Vec::new();
    for (i, step) in trace.steps.iter().enumerate() {
        let succeeded = matches!(
            &step.reply,
            Some(Reply::Value(value)) if !matches!(value, Value::Error(_))
        );
        match effects[i] {
            None if step.argv[0] == "BARRIER".into() => settled = succeeded,
            None | Some(Effect::Neutral) => {}
            Some(Effect::Transparent) => settled = false,
            Some(Effect::Changing) => {
                period += 1;
                settled = false;
            }
        }
        let op = &program.ops[step.op];
        if succeeded && effects[i].is_some() {
            scope.add(op);
        }
        let quiet = step.received.is_some_and(|received| {
            trace.steps.iter().zip(&effects).all(|(other, effect)| {
                other.sent <= step.sent
                    || other.sent >= received
                    || matches!(effect, None | Some(Effect::Neutral))
            })
        });
        if let OpKind::Query {
            index,
            query,
            options,
        } = &op.kind
            && settled
            && quiet
            && !matches!(query, Query::Knn { .. })
            && let Some((total, keys)) = step.reply.as_ref().and_then(result)
        {
            seen.push(Observation {
                period,
                index,
                key: scope.key(index),
                query,
                options,
                total,
                keys,
            });
        }
    }
    seen
}

/// How a step can change what queries return; `None` for the harness's own
/// barrier and release steps.
fn effect(step: &Step) -> Option<Effect> {
    let word = |i: usize| {
        step.argv
            .get(i)
            .map(|w| String::from_utf8_lossy(w.as_slice()).to_ascii_uppercase())
            .unwrap_or_default()
    };
    let command = word(0);
    Some(
        if step.argv.len() == 1 && (command == "BARRIER" || command == "RELEASE") {
            return None;
        } else if NEUTRAL_COMMANDS.contains(&command.as_str())
            || (command == "_FT.DEBUG" && NEUTRAL_HOOKS.contains(&word(1).as_str()))
        {
            Effect::Neutral
        } else if command == "DEBUG" && word(1) == "RELOAD" {
            Effect::Transparent
        } else {
            Effect::Changing
        },
    )
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
    use crate::lower::lower;
    use crate::program::{DataType, FORMAT_VERSION, Field, FieldKind, Op, argv};
    use crate::run::Exit;
    use crate::target::Status;

    /// The rules' verdicts on a trace in which every op succeeded and each
    /// query matched the paired number of documents.
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
                let (argv, value) = match kind {
                    OpKind::Barrier => (argv(&["BARRIER"]), Value::Simple("OK".into())),
                    OpKind::Query { .. } => (
                        lower(kind).remove(0),
                        Value::Array(vec![Value::Int(*total)]),
                    ),
                    _ => (lower(kind).remove(0), Value::Simple("OK".into())),
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
        let create = |index: &str| OpKind::Create {
            index: index.into(),
            on: DataType::Hash,
            prefix: "d:".into(),
            fields: vec![Field {
                name: "t".into(),
                kind: FieldKind::Text,
                sortable: false,
            }],
        };
        let search = |index: &str, query| OpKind::Query {
            index: index.into(),
            query,
            options: QueryOptions::default(),
        };
        let command = |words: &[&str]| OpKind::Command { argv: argv(words) };
        let hello = || Query::Term("hello".into());
        let world = || Query::InField {
            field: "t".into(),
            term: "world".into(),
        };
        let partition = |rest| {
            vec![
                (create("i"), 0),
                (OpKind::Barrier, 0),
                (search("i", hello()), 5),
                (search("i", Query::And(vec![hello(), world()])), 2),
                (
                    search(
                        "i",
                        Query::And(vec![hello(), Query::Not(Box::new(world()))]),
                    ),
                    rest,
                ),
            ]
        };
        let repeated = |between, second| {
            vec![
                (create("i"), 0),
                (OpKind::Barrier, 0),
                (search("i", hello()), 3),
                (between, 0),
                (search("i", hello()), second),
            ]
        };
        let twin = |second| {
            vec![
                (create("i"), 0),
                (create("j"), 0),
                (OpKind::Barrier, 0),
                (search("i", hello()), 3),
                (search("j", hello()), second),
            ]
        };
        let gc = || command(&["_FT.DEBUG", "GC_FORCEINVOKE", "i"]);
        let none: [&str; 0] = [];
        assert_eq!(signatures(true, &partition(3)), none);
        assert_eq!(signatures(true, &partition(2)), ["logic:partition:term"]);
        assert_eq!(signatures(true, &repeated(gc(), 3)), none);
        assert_eq!(
            signatures(true, &repeated(gc(), 2)),
            ["logic:equivalence:term"]
        );
        assert_eq!(signatures(true, &twin(3)), none);
        assert_eq!(signatures(true, &twin(2)), ["logic:twin:term"]);
        for skipped in [
            signatures(true, &repeated(command(&["HSET", "d:1", "t", "hello"]), 2)),
            signatures(true, &repeated(command(&["DEBUG", "RELOAD"]), 2)),
            signatures(false, &twin(2)),
        ] {
            assert_eq!(
                skipped, none,
                "a write, a reload without a barrier, or partial results"
            );
        }
    }
}
