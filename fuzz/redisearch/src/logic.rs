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
use crate::effect::{Effect, effect};
use crate::op::{Action, Op, Query, QueryOptions};
use crate::state::{Key, State};

/// From this dialect on, a search's total is the number of results it
/// returned rather than the number that matched.
const EXACT_TOTALS_BEFORE_DIALECT: u8 = 4;
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
    let mut fails_on_timeout = false;
    in_completion_order(model, ops, steps, |_, state| {
        fails_on_timeout |= state.fails_on_timeout;
    });
    if !fails_on_timeout {
        return;
    }
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

/// What an op does to search results; a settle has no effect of its own.
fn op_effect(op: &Op, state: &State) -> Effect {
    match op.action.commands(state).first() {
        Some(command) => effect(command),
        None => Effect::Reads,
    }
}

/// Exact `FT.SEARCH` results inside a comparison window, each with the key
/// of its index as the commands the server accepted left it. KNN results
/// are approximate and left out, and so are results under dialect 4 and
/// later, whose total counts only the results returned.
fn comparable_results<'a>(
    model: &RediSearch,
    ops: &'a [Op],
    steps: &[Step<Value>],
) -> Vec<SearchResult<'a>> {
    let mut effects = vec![Effect::Reads; steps.len()];
    in_completion_order(model, ops, steps, |at, state| {
        effects[at] = op_effect(&ops[at], state);
    });
    // A step that never completed may still have changed results.
    for (at, step) in steps.iter().enumerate() {
        if step.done.is_none() && !matches!(ops[at].action, Action::Settle) {
            effects[at] = Effect::Writes;
        }
    }
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
        let undisturbed = steps.iter().zip(&effects).all(|(other, effect)| {
            other.sent <= step.sent
                || other.sent >= done
                || matches!(effect, Effect::Reads | Effect::Schedules)
        });
        if let Action::Query {
            index,
            query,
            options,
        } = &ops[at].action
            && open
            && undisturbed
            && !matches!(query, Query::Knn { .. })
            && options
                .dialect
                .is_none_or(|dialect| dialect < EXACT_TOTALS_BEFORE_DIALECT)
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

    fn run(ops: &[(Action, i64)]) -> Vec<String> {
        run_until(ops, ops.len())
    }

    /// The findings for a run in which every op succeeded in turn and each
    /// search matched the paired number of documents. The ops after
    /// `ended_after` have no steps, as when the server died.
    fn run_until(ops: &[(Action, i64)], ended_after: usize) -> Vec<String> {
        let steps: Vec<Step<Value>> = ops
            .iter()
            .take(ended_after)
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
        let ops: Vec<Op> = ops
            .iter()
            .map(|(action, _)| Op::new(action.clone()))
            .collect();
        let mut notes = Notes::default();
        check(&RediSearch, &ops, &steps, &mut notes);
        notes.findings.iter().map(Finding::to_string).collect()
    }

    #[test]
    fn each_rule_passes_consistent_results_and_flags_inconsistent_ones() {
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
        let search = |index: &str, query| Action::Query {
            index: index.into(),
            query,
            options: QueryOptions::default(),
        };
        let hello = || Query::Term("hello".into());
        let world = || Query::InField {
            field: "t".into(),
            term: "world".into(),
        };
        let fail = || Action::Boot {
            args: vec!["ON_TIMEOUT".into(), "FAIL".into()],
        };
        let setup = |boot: bool, extra: Vec<(Action, i64)>| {
            let mut ops = Vec::new();
            if boot {
                ops.push((fail(), 0));
            }
            ops.extend([(create("i"), 0), (create("j"), 0), (Action::Settle, 0)]);
            ops.extend(extra);
            ops
        };
        let partition = |rest| {
            setup(
                true,
                vec![
                    (search("i", hello()), 5),
                    (search("i", Query::And(vec![hello(), world()])), 2),
                    (
                        search(
                            "i",
                            Query::And(vec![hello(), Query::Not(Box::new(world()))]),
                        ),
                        rest,
                    ),
                ],
            )
        };
        let repeated = |between, second| {
            setup(
                true,
                vec![
                    (search("i", hello()), 3),
                    (between, 0),
                    (search("i", hello()), second),
                ],
            )
        };
        let twin = |boot, second| {
            setup(
                boot,
                vec![(search("i", hello()), 3), (search("j", hello()), second)],
            )
        };
        let dialect_4_twin = |second| {
            let search = |index: &str| Action::Query {
                index: index.into(),
                query: hello(),
                options: QueryOptions {
                    dialect: Some(4),
                    ..QueryOptions::default()
                },
            };
            setup(true, vec![(search("i"), 3), (search("j"), second)])
        };
        let gc = || Action::command(&[crate::effect::DEBUG, "GC_FORCEINVOKE", "i"]);
        let cases = [
            (run(&partition(3)), vec![]),
            (run(&partition(2)), vec!["logic:partition:term"]),
            (run(&repeated(gc(), 3)), vec![]),
            (run(&repeated(gc(), 2)), vec!["logic:equivalence:term"]),
            (run(&twin(true, 3)), vec![]),
            (run(&twin(true, 2)), vec!["logic:twin:term"]),
            (
                run(&repeated(
                    Action::command(&["HSET", "d:1", "t", "hello"]),
                    2,
                )),
                vec![],
            ),
            (
                run(&repeated(Action::command(&["DEBUG", "RELOAD"]), 2)),
                vec![],
            ),
            (
                run(&repeated(Action::command(&["FT.ALTER", "j", "SCHEMA"]), 3)),
                vec![],
            ),
            (run(&twin(false, 2)), vec![]),
            (run(&dialect_4_twin(2)), vec![]),
        ];
        for (i, (found, expected)) in cases.into_iter().enumerate() {
            assert_eq!(found, expected, "case {i}");
        }
    }

    #[test]
    fn a_raw_schema_change_leaves_its_index_without_a_twin() {
        let create = |index: &str| Action::Create {
            index: index.into(),
            on: DataType::Hash,
            prefix: "d:".into(),
            fields: Vec::new(),
        };
        let search = |index: &str| Action::Query {
            index: index.into(),
            query: Query::Term("hello".into()),
            options: QueryOptions::default(),
        };
        let ops = [
            (
                Action::Boot {
                    args: vec!["ON_TIMEOUT".into(), "FAIL".into()],
                },
                0,
            ),
            (create("i"), 0),
            (create("j"), 0),
            (
                Action::command(&["FT.ALTER", "j", "SCHEMA", "ADD", "t", "TEXT"]),
                0,
            ),
            (Action::Settle, 0),
            (search("i"), 3),
            (search("j"), 2),
        ];
        assert_eq!(run(&ops), Vec::<String>::new());
    }

    #[test]
    fn a_run_that_ended_early_is_judged_on_the_steps_it_has() {
        let ops = [
            (
                Action::Boot {
                    args: vec!["ON_TIMEOUT".into(), "FAIL".into()],
                },
                0,
            ),
            (Action::Settle, 0),
            (Action::command(&["DEBUG", "SEGFAULT"]), 0),
            (Action::Settle, 0),
        ];
        assert_eq!(run_until(&ops, 3), Vec::<String>::new());
    }
}
