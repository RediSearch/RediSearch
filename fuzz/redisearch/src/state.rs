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

use crate::effect::{Change, describe};
use crate::op::{Action, CursorSlot, DataType, Field, FieldKind, Op, Query, QueryOptions};

/// From this dialect on, a search's total is the number of results it
/// returned rather than the number that matched.
const EXACT_TOTALS_BEFORE_DIALECT: u8 = 4;

#[derive(Clone, Debug, Default)]
pub struct State {
    /// Whether a query that times out fails rather than returning what it
    /// found so far.
    pub fails_on_timeout: bool,
    /// Whether a configuration parameter the model does not follow was set.
    pub reconfigured: bool,
    /// Workers are on by default.
    pub workers_off: bool,
    pub paused: bool,
    pub pauses_scans: bool,
    /// Whether a background scan waits to be resumed, so the server cannot
    /// become idle.
    pub scan_paused: bool,
    pub indexes: BTreeMap<Bytes, Schema>,
    pub cursors: BTreeMap<u8, Cursor>,
    /// The last search that the logic rules can compare others with.
    pub comparable: Option<Search>,
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
pub struct Search {
    pub index: Bytes,
    pub query: Query,
    pub options: QueryOptions,
}

/// What decides which documents an index holds and how queries over it
/// read; `SORTABLE` and field order do not. Indexes with equal keys are
/// twins and must answer every query alike.
pub type Key = (DataType, Bytes, Vec<(Bytes, FieldKind)>);

impl State {
    /// Folds `op` in. `reply` is `None` when predicting, which assumes
    /// success.
    pub fn apply(&mut self, op: &Op, reply: Option<&Value>) {
        if matches!(reply, Some(Value::Error(_))) {
            return;
        }
        match &op.action {
            Action::Settle | Action::Populate { .. } => {}
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
                self.scan();
            }
            Action::Alter { index, field, .. } => {
                self.scan();
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
            } => {
                if comparable(query, options) {
                    self.comparable = Some(Search {
                        index: index.clone(),
                        query: query.clone(),
                        options: options.clone(),
                    });
                }
            }
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
                let named = argv.get(1);
                match describe(argv).1 {
                    Change::None => {}
                    Change::DropsIndex => {
                        if let Some(index) = named {
                            self.indexes.remove(index);
                        }
                    }
                    Change::ReshapesIndex => {
                        self.scan();
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
                    Change::PausesScans(pauses) => self.pauses_scans = pauses,
                    Change::ResumesScan => self.scan_paused = false,
                    Change::Reloads => self.scan(),
                    Change::Configures => self.configure(argv.get(2..).unwrap_or_default()),
                }
            }
        }
    }

    fn scan(&mut self) {
        self.scan_paused |= self.pauses_scans;
    }

    /// Names as `CONFIG SET` (`search-on-timeout`) or `FT.CONFIG SET`
    /// (`ON_TIMEOUT`) takes them.
    fn configure(&mut self, settings: &[Bytes]) {
        for pair in settings.chunks(2) {
            let [name, value] = pair else { continue };
            let name = String::from_utf8_lossy(name.as_slice())
                .to_ascii_lowercase()
                .replace('_', "-");
            let value = String::from_utf8_lossy(value.as_slice()).to_ascii_lowercase();
            match name.strip_prefix("search-").unwrap_or(&name) {
                "on-timeout" => self.fails_on_timeout = value == "fail",
                "workers" => self.workers_off = value == "0",
                _ => self.reconfigured = true,
            }
        }
    }

    /// Whether searches return exactly what matched.
    pub fn exact(&self) -> bool {
        self.fails_on_timeout && !self.reconfigured
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

/// Whether the logic rules compare a search's results: they are exact. KNN
/// results are approximate, and from dialect 4 on a search's total counts
/// only the results it returned.
pub fn comparable(query: &Query, options: &QueryOptions) -> bool {
    !matches!(query, Query::Knn { .. })
        && options
            .dialect
            .is_none_or(|dialect| dialect < EXACT_TOTALS_BEFORE_DIALECT)
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
