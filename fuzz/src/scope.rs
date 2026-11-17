/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! What a sequence of ops has set up on the server.

use std::collections::BTreeMap;

use crate::program::{Bytes, DEBUG, DataType, Field, FieldKind, Op, OpKind, Var};

#[derive(Clone, Debug, Default)]
pub struct Scope {
    pub indexes: BTreeMap<Bytes, Schema>,
    pub workers: bool,
    pub paused: bool,
    /// The index each cursor variable was last bound for.
    pub cursors: BTreeMap<Var, Bytes>,
}

#[derive(Clone, Debug)]
pub struct Schema {
    pub on: DataType,
    pub prefix: Bytes,
    pub fields: Vec<Field>,
}

/// What decides which documents an index holds and how queries over it
/// read; `SORTABLE` and field order do not. Indexes with equal keys are
/// twins and must answer every query alike.
pub type Key = (DataType, Bytes, Vec<(Bytes, FieldKind)>);

impl Scope {
    pub fn new(module_args: &[Bytes]) -> Scope {
        Scope {
            workers: module_args
                .windows(2)
                .any(|pair| pair[0] == "WORKERS".into() && pair[1] != "0".into()),
            ..Scope::default()
        }
    }

    pub fn after(module_args: &[Bytes], ops: &[Op]) -> Scope {
        let mut scope = Scope::new(module_args);
        ops.iter().for_each(|op| scope.add(op));
        scope
    }

    pub fn add(&mut self, op: &Op) {
        match &op.kind {
            OpKind::Create {
                index,
                on,
                prefix,
                fields,
            } => {
                self.indexes.entry(index.clone()).or_insert_with(|| Schema {
                    on: *on,
                    prefix: prefix.clone(),
                    fields: fields.clone(),
                });
            }
            OpKind::Alter { index, field, .. } => {
                if let Some(schema) = self.indexes.get_mut(index)
                    && schema.fields.iter().all(|f| f.name != field.name)
                {
                    schema.fields.push(field.clone());
                }
            }
            OpKind::Command { argv } => {
                let word = |i: usize| argv.get(i).map(|w| w.as_slice().to_ascii_uppercase());
                match (word(0).as_deref(), word(1).as_deref()) {
                    (Some(b"FT.DROPINDEX"), Some(_)) => {
                        self.indexes.remove(&argv[1]);
                    }
                    (Some(b"FT.AGGREGATE"), Some(_)) => {
                        if let Some(var) = op.bind {
                            self.cursors.insert(var, argv[1].clone());
                        }
                    }
                    (Some(debug), Some(b"WORKERS")) if debug == DEBUG.as_bytes() => {
                        match word(2).as_deref() {
                            Some(b"PAUSE") => self.paused = true,
                            Some(b"RESUME") => self.paused = false,
                            _ => {}
                        }
                    }
                    _ => {}
                }
            }
            OpKind::Query { .. } | OpKind::Populate { .. } | OpKind::Barrier => {}
        }
    }

    pub fn key(&self, index: &Bytes) -> Option<Key> {
        let schema = self.indexes.get(index)?;
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
