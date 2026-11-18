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
//! settle that completed and closes at the next step that can change
//! results; no such step may run while either search is in flight. `DEBUG
//! RELOAD` must not change results, but the index is rebuilding afterwards,
//! so a window opens only at the next settle. Without `ON_TIMEOUT FAIL` a
//! slow search returns partial results, so no rule applies.

use std::collections::BTreeSet;

use procfuzz::system::{Notes, Step, in_completion_order};
use procfuzz_redis::resp::{Bytes, Value};

use crate::RediSearch;
use crate::effect::Effect;
use crate::op::{Action, Op, Query, QueryOptions};
use crate::state::{Key, comparable};

/// RediSearch's default stopwords.
const STOPWORDS: [&str; 33] = [
    "a", "is", "the", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into",
    "it", "no", "not", "of", "on", "or", "such", "that", "their", "then", "there", "these", "they",
    "this", "to", "was", "will", "with",
];

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

pub fn check(model: &RediSearch, ops: &[Op], steps: &[Step<Value>], notes: &mut Notes) {
    let results = comparable_results(model, ops, steps);
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
                notes.fail("logic", key(rule, a.query));
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
                notes.fail("logic", key("partition", whole.query));
            }
        }
    }
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
/// through different terms keeps one finding.
fn key(rule: &str, query: &Query) -> String {
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
    format!("{rule}:{}", shape(query))
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
/// of its index as the commands the server accepted left it. None under the
/// default timeout policy, which returns partial results.
fn comparable_results<'a>(
    model: &RediSearch,
    ops: &'a [Op],
    steps: &[Step<Value>],
) -> Vec<SearchResult<'a>> {
    // A step that never completed may still have changed results.
    let effects: Vec<Effect> = ops
        .iter()
        .zip(steps)
        .map(|(op, step)| match op.action {
            Action::Settle => Effect::Reads,
            _ if step.done.is_none() => Effect::Writes,
            ref action => action.effect(),
        })
        .collect();
    let mut window = 0;
    let mut open = false;
    let mut results = Vec::new();
    in_completion_order(model, ops, steps, |at, state| {
        let step = &steps[at];
        match (&ops[at].action, effects[at]) {
            (Action::Settle, _) => open = true,
            (_, Effect::Reads | Effect::Schedules) => {}
            (_, Effect::Rebuilds) => open = false,
            (_, Effect::Writes) => {
                window += 1;
                open = false;
            }
        }
        let done = step.done.expect("completed");
        // A write may run any time between its send and its reply.
        let undisturbed = steps.iter().zip(&effects).all(|(other, effect)| {
            matches!(effect, Effect::Reads | Effect::Schedules)
                || other.sent >= done
                || other.done.is_some_and(|other_done| other_done <= step.sent)
        });
        if let Action::Query {
            index,
            query,
            options,
        } = &ops[at].action
            && state.fails_on_timeout
            && open
            && undisturbed
            && comparable(query, options)
            && let Some((total, keys)) = step.reply.as_ref().and_then(search_result)
        {
            results.push(SearchResult {
                window,
                index,
                key: state.key(index),
                query,
                options,
                total,
                keys,
            });
        }
    });
    results
}

/// `FT.SEARCH` replies `[total, key, fields?, key, fields?, …]`.
fn search_result(reply: &Value) -> Option<(i64, Option<BTreeSet<Bytes>>)> {
    let Value::Array(items) = reply else {
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
    use procfuzz::system::Finding;

    use super::*;
    use crate::op::{DataType, Field, FieldKind};

    /// The findings for a run in which every op succeeded in turn and each
    /// search matched the paired number of documents, with the steps then
    /// changed by `edit`.
    fn findings(ops: &[(Action, i64)], edit: impl FnOnce(&mut Vec<Step<Value>>)) -> Vec<String> {
        let mut steps: Vec<Step<Value>> = ops
            .iter()
            .enumerate()
            .map(|(at, (action, total))| {
                let tick = u32::try_from(2 * at + 1).unwrap();
                let reply = match action {
                    Action::Query { .. } => Some(Value::Array(vec![Value::Int(*total)])),
                    Action::Settle | Action::Boot { .. } => None,
                    _ => Some(Value::Simple("OK".into())),
                };
                Step {
                    sent: tick,
                    done: Some(tick + 1),
                    reply,
                }
            })
            .collect();
        edit(&mut steps);
        let ops: Vec<Op> = ops
            .iter()
            .map(|(action, _)| Op::new(action.clone()))
            .collect();
        let mut notes = Notes::default();
        check(&RediSearch, &ops, &steps, &mut notes);
        notes.findings.iter().map(Finding::to_string).collect()
    }

    #[test]
    fn each_rule_flags_inconsistent_results_and_only_those() {
        let search = |index: &str, query, dialect| Action::Query {
            index: index.into(),
            query,
            options: QueryOptions {
                dialect,
                ..QueryOptions::default()
            },
        };
        let hello = || Query::Term("hello".into());
        let world = || Query::InField {
            field: "t".into(),
            term: "world".into(),
        };
        let command = |words: &[&str]| (Action::command(words), 0);
        // Two indexes on one prefix, created before `before`, then a settle,
        // then `after`.
        let program = |boot: bool, before: Vec<(Action, i64)>, after: Vec<(Action, i64)>| {
            let create = |index: &str| Action::Create {
                index: index.into(),
                on: DataType::Hash,
                prefix: "d:".into(),
                fields: vec![Field {
                    name: "t".into(),
                    kind: FieldKind::Text,
                    sortable: false,
                }],
            };
            let mut ops = Vec::new();
            if boot {
                let args = vec!["ON_TIMEOUT".into(), "FAIL".into()];
                ops.push((Action::Boot { args }, 0));
            }
            ops.extend([(create("i"), 0), (create("j"), 0)]);
            ops.extend(before);
            ops.push((Action::Settle, 0));
            ops.extend(after);
            ops
        };
        let partition = |rest| {
            let not_world = Query::And(vec![hello(), Query::Not(Box::new(world()))]);
            let after = vec![
                (search("i", hello(), None), 5),
                (search("i", Query::And(vec![hello(), world()]), None), 2),
                (search("i", not_world, None), rest),
            ];
            program(true, Vec::new(), after)
        };
        let repeated = |between, second| {
            let after = vec![
                (search("i", hello(), None), 3),
                between,
                (search("i", hello(), None), second),
            ];
            program(true, Vec::new(), after)
        };
        let twin = |boot, before, dialect| {
            let after = vec![
                (search("i", hello(), dialect), 3),
                (search("j", hello(), dialect), 2),
            ];
            program(boot, before, after)
        };
        let gc = || command(&[crate::effect::DEBUG, "GC_FORCEINVOKE", "i"]);
        let all = |ops: Vec<(Action, i64)>| findings(&ops, |_| {});
        let hset = || command(&["HSET", "d:1", "t", "hello"]);
        let cases: [(Vec<String>, &[&str]); 12] = [
            (all(partition(3)), &[]),
            (all(partition(2)), &["logic:partition:term"]),
            (all(repeated(gc(), 3)), &[]),
            (all(repeated(gc(), 2)), &["logic:equivalence:term"]),
            (all(repeated(hset(), 2)), &[]),
            // A write whose reply was read only after the second search.
            (
                findings(&repeated(hset(), 2), |steps| steps[5].done = Some(100)),
                &[],
            ),
            (all(repeated(command(&["DEBUG", "RELOAD"]), 2)), &[]),
            (all(twin(true, Vec::new(), None)), &["logic:twin:term"]),
            (all(twin(false, Vec::new(), None)), &[]),
            (all(twin(true, Vec::new(), Some(4))), &[]),
            // A raw schema change leaves its index without a twin.
            (
                all(twin(
                    true,
                    vec![command(&["FT.ALTER", "j", "SCHEMA", "ADD", "u", "TEXT"])],
                    None,
                )),
                &[],
            ),
            // The run ended before the second search.
            (findings(&repeated(gc(), 2), |steps| steps.truncate(6)), &[]),
        ];
        for (i, (found, expected)) in cases.into_iter().enumerate() {
            assert_eq!(found, expected, "case {i}");
        }
    }
}
