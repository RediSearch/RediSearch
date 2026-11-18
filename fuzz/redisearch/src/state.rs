/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! What the ops of a program have set up on the server.

use std::collections::BTreeMap;

use procfuzz_redis::resp::{Bytes, Value};

use crate::effect::{Change, change};
use crate::op::{Action, CursorSlot, DataType, Field, FieldKind, Op, Query, QueryOptions};

#[derive(Clone, Debug, Default)]
pub struct State {
    /// Whether an op other than a boot argument ran.
    pub started: bool,
    /// Whether a query that times out fails rather than returning what it
    /// found so far.
    pub fails_on_timeout: bool,
    pub workers: bool,
    pub paused: bool,
    pub indexes: BTreeMap<Bytes, Schema>,
    pub cursors: BTreeMap<u8, Cursor>,
    /// The last search that the logic rules can compare others with.
    pub comparable: Option<Comparable>,
}

#[derive(Clone, Debug)]
pub struct Schema {
    pub on: DataType,
    pub prefix: Bytes,
    pub fields: Vec<Field>,
    /// False once a raw command changed the schema in a way the model does
    /// not follow.
    pub known: bool,
}

#[derive(Clone, Debug)]
pub struct Cursor {
    pub index: Bytes,
    /// The id the server chose, once its reply is read.
    pub id: Option<i64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Comparable {
    pub index: Bytes,
    pub query: Query,
    pub options: QueryOptions,
    /// The part a later search added to or subtracted from `query`.
    pub part: Option<Query>,
}

/// What decides which documents an index holds and how queries over it
/// read; `SORTABLE` and field order do not. Indexes with equal keys are
/// twins and must answer every query alike.
pub type Key = (DataType, Bytes, Vec<(Bytes, FieldKind)>);

impl State {
    /// Folds `op` in. `reply` is `None` when predicting, which assumes
    /// success.
    pub fn apply(&mut self, op: &Op, reply: Option<&Value>) {
        if let Action::Boot { args } = &op.action {
            if !self.started {
                self.boot(args);
            }
            return;
        }
        self.started = true;
        if matches!(reply, Some(Value::Error(_))) {
            return;
        }
        match &op.action {
            Action::Boot { .. } | Action::Settle | Action::Populate { .. } => {}
            Action::Create {
                index,
                on,
                prefix,
                fields,
            } => {
                self.indexes.entry(index.clone()).or_insert_with(|| Schema {
                    on: *on,
                    prefix: prefix.clone(),
                    fields: fields.clone(),
                    known: true,
                });
            }
            Action::Alter { index, field, .. } => {
                if let Some(schema) = self.indexes.get_mut(index)
                    && schema.fields.iter().all(|f| f.name != field.name)
                {
                    schema.fields.push(field.clone());
                }
            }
            Action::Query {
                index,
                query,
                options,
            } => self.compared(index, query, options),
            Action::Aggregate {
                index,
                cursor: Some(CursorSlot { slot, .. }),
                ..
            } => {
                let cursor = Cursor {
                    index: index.clone(),
                    id: reply.and_then(cursor_id),
                };
                self.cursors.insert(*slot, cursor);
            }
            Action::Aggregate { cursor: None, .. } => {}
            Action::CursorRead { slot, .. } => match reply.and_then(cursor_id) {
                Some(0) => {
                    self.cursors.remove(slot);
                }
                Some(id) => {
                    if let Some(cursor) = self.cursors.get_mut(slot) {
                        cursor.id = Some(id);
                    }
                }
                None => {}
            },
            Action::CursorDel { slot } => {
                self.cursors.remove(slot);
            }
            Action::Command { argv } => {
                let words = argv.words();
                let named = words.get(1);
                match change(words) {
                    Change::None => {}
                    Change::DropsIndex => {
                        if let Some(index) = named {
                            self.indexes.remove(index);
                        }
                    }
                    Change::ReshapesIndex => {
                        if let Some(index) = named {
                            self.indexes
                                .entry(index.clone())
                                .or_insert_with(|| Schema {
                                    on: DataType::Hash,
                                    prefix: Bytes::default(),
                                    fields: Vec::new(),
                                    known: false,
                                })
                                .known = false;
                        }
                    }
                    Change::PausesWorkers => self.paused = true,
                    Change::ResumesWorkers => self.paused = false,
                }
            }
        }
    }

    fn boot(&mut self, args: &[String]) {
        for pair in args.windows(2) {
            let [name, value] = pair else { continue };
            if name.eq_ignore_ascii_case("ON_TIMEOUT") {
                self.fails_on_timeout = value.eq_ignore_ascii_case("FAIL");
            }
            if name.eq_ignore_ascii_case("WORKERS") {
                self.workers = value != "0";
            }
        }
    }

    /// Remembers a search the logic rules can compare: one that lists every
    /// match, so totals and keys are exact.
    fn compared(&mut self, index: &Bytes, query: &Query, options: &QueryOptions) {
        if !options.no_content || matches!(query, Query::Knn { .. }) {
            return;
        }
        let part = self.comparable.as_ref().and_then(|last| {
            let same = last.index == *index && last.options == *options;
            let (rest, part) = match query {
                Query::And(children) => match children.as_slice() {
                    [rest, part] => (rest, part),
                    _ => return None,
                },
                part => (&Query::All, part),
            };
            let strip = |part: &Query| match part {
                Query::Not(part) => (**part).clone(),
                part => part.clone(),
            };
            (same && *rest == last.query && *part != Query::All).then(|| strip(part))
        });
        if let (Some(part), Some(last)) = (part, self.comparable.as_mut()) {
            last.part = Some(part);
            return;
        }
        let continues = self
            .comparable
            .as_ref()
            .is_some_and(|last| last.query == *query && last.options == *options);
        if !continues {
            self.comparable = Some(Comparable {
                index: index.clone(),
                query: query.clone(),
                options: options.clone(),
                part: None,
            });
        }
    }

    pub fn key(&self, index: &Bytes) -> Option<Key> {
        let schema = self.indexes.get(index).filter(|schema| schema.known)?;
        let mut fields: Vec<_> = schema
            .fields
            .iter()
            .map(|f| (f.name.clone(), f.kind.clone()))
            .collect();
        fields.sort();
        Some((schema.on, schema.prefix.clone(), fields))
    }

    pub fn twin(&self, index: &Bytes) -> Option<&Bytes> {
        let key = self.key(index)?;
        self.indexes
            .keys()
            .find(|name| *name != index && self.key(name).as_ref() == Some(&key))
    }
}

/// `FT.AGGREGATE … WITHCURSOR` and `FT.CURSOR READ` reply
/// `[results, cursor id]`.
fn cursor_id(reply: &Value) -> Option<i64> {
    match reply {
        Value::Array(items) => match items.as_slice() {
            [_, Value::Int(id)] => Some(*id),
            _ => None,
        },
        _ => None,
    }
}
