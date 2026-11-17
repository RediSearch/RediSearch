/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub const FORMAT_VERSION: u32 = 1;

#[derive(Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Bytes(pub Vec<u8>);

impl Bytes {
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl From<&str> for Bytes {
    fn from(s: &str) -> Self {
        Bytes(s.as_bytes().to_vec())
    }
}

impl From<String> for Bytes {
    fn from(s: String) -> Self {
        Bytes(s.into_bytes())
    }
}

impl fmt::Debug for Bytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match std::str::from_utf8(&self.0) {
            Ok(s) => write!(f, "{s:?}"),
            Err(_) => write!(f, "hex:{}", to_hex(&self.0)),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum BytesRepr {
    Text(String),
    Hex { hex: String },
}

impl Serialize for Bytes {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match std::str::from_utf8(&self.0) {
            Ok(s) => BytesRepr::Text(s.to_owned()),
            Err(_) => BytesRepr::Hex {
                hex: to_hex(&self.0),
            },
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Bytes {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        match BytesRepr::deserialize(deserializer)? {
            BytesRepr::Text(s) => Ok(Bytes(s.into_bytes())),
            BytesRepr::Hex { hex } => (0..hex.len())
                .step_by(2)
                .map(|i| {
                    hex.get(i..i + 2)
                        .and_then(|b| u8::from_str_radix(b, 16).ok())
                })
                .collect::<Option<Vec<u8>>>()
                .map(Bytes)
                .ok_or_else(|| serde::de::Error::custom(format!("invalid hex {hex:?}"))),
        }
    }
}

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

pub type Argv = Vec<Bytes>;

pub fn argv(words: &[&str]) -> Argv {
    words.iter().map(|w| Bytes::from(*w)).collect()
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Program {
    pub version: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub module_args: Vec<Bytes>,
    pub ops: Vec<Op>,
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct Conn(pub u8);

/// A slot for a value the server chooses, such as a cursor id. An op's `bind`
/// stores the id in its reply there, and a command token `$<n>` reads slot
/// `n` (0 while it is unset).
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct Var(pub u8);

impl Var {
    pub fn token(self) -> Bytes {
        format!("${}", self.0).into()
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplyMode {
    #[default]
    Await,
    Defer,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Op {
    #[serde(default, skip_serializing_if = "is_default")]
    pub conn: Conn,
    #[serde(default, skip_serializing_if = "is_default")]
    pub reply: ReplyMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bind: Option<Var>,
    #[serde(flatten)]
    pub kind: OpKind,
}

fn is_default<T: Default + PartialEq>(value: &T) -> bool {
    *value == T::default()
}

/// The ops the fuzzer reasons about are typed; everything else is a `Command`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum OpKind {
    Create {
        index: Bytes,
        #[serde(default, skip_serializing_if = "is_default")]
        on: DataType,
        prefix: Bytes,
        fields: Vec<Field>,
    },
    Alter {
        index: Bytes,
        #[serde(default, skip_serializing_if = "is_default")]
        on: DataType,
        field: Field,
    },
    Query {
        index: Bytes,
        query: Query,
        #[serde(default)]
        options: QueryOptions,
    },
    /// `n` writes under `prefix`, with values for `fields` drawn from the
    /// fixed dictionary by a PRNG seeded with `seed`.
    Populate {
        #[serde(default, skip_serializing_if = "is_default")]
        on: DataType,
        prefix: Bytes,
        fields: Vec<Field>,
        n: u32,
        seed: u64,
    },
    Barrier,
    Command {
        argv: Argv,
    },
}

/// What an index covers: hashes, or JSON documents (with RedisJSON).
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum DataType {
    #[default]
    Hash,
    Json,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Field {
    pub name: Bytes,
    #[serde(flatten)]
    pub kind: FieldKind,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub sortable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FieldKind {
    Text,
    Numeric,
    Tag,
    Geo,
    /// FLOAT32 vectors of `dim` components.
    Vector {
        algorithm: Algorithm,
        dim: u16,
        metric: Metric,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Algorithm {
    Flat,
    Hnsw,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Metric {
    L2,
    Ip,
    Cosine,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Query {
    All,
    Term(Bytes),
    InField {
        field: Bytes,
        term: Bytes,
    },
    Tag {
        field: Bytes,
        values: Vec<Bytes>,
    },
    Range {
        field: Bytes,
        min: Bound,
        max: Bound,
    },
    And(Vec<Query>),
    Or(Vec<Query>),
    Not(Box<Query>),
    /// The `k` nearest neighbours of `vector`, a FLOAT32 blob, among the
    /// documents matching `filter`. Valid only as the whole query.
    Knn {
        filter: Box<Query>,
        field: Bytes,
        k: u32,
        vector: Bytes,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Bound {
    Unbounded,
    Inclusive(i64),
    Exclusive(i64),
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryOptions {
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub no_content: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<(u32, u32)>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dialect: Option<u8>,
}

/// A value inside an op that edits may replace or shrink.
pub enum Leaf<'a> {
    Word(&'a mut Bytes),
    Int(&'a mut i64),
}

impl OpKind {
    /// Calls `f` on every query term, tag value, range bound, and command
    /// token, in a fixed order.
    pub fn leaves(&mut self, f: &mut dyn FnMut(Leaf<'_>)) {
        match self {
            OpKind::Query { query, .. } => query.leaves(f),
            OpKind::Command { argv } => argv.iter_mut().for_each(|token| f(Leaf::Word(token))),
            OpKind::Create { .. }
            | OpKind::Alter { .. }
            | OpKind::Populate { .. }
            | OpKind::Barrier => {}
        }
    }
}

impl Query {
    fn leaves(&mut self, f: &mut dyn FnMut(Leaf<'_>)) {
        match self {
            Query::All => {}
            Query::Term(term) | Query::InField { term, .. } => f(Leaf::Word(term)),
            Query::Tag { values, .. } => values.iter_mut().for_each(|v| f(Leaf::Word(v))),
            Query::Range { min, max, .. } => {
                for bound in [min, max] {
                    if let Bound::Inclusive(n) | Bound::Exclusive(n) = bound {
                        f(Leaf::Int(n));
                    }
                }
            }
            Query::And(children) | Query::Or(children) => {
                children.iter_mut().for_each(|child| child.leaves(f))
            }
            Query::Not(child) | Query::Knn { filter: child, .. } => child.leaves(f),
        }
    }
}

impl Program {
    pub fn from_json(text: &str) -> Result<Program, String> {
        let program: Program = serde_json::from_str(text).map_err(|e| e.to_string())?;
        if program.version != FORMAT_VERSION {
            return Err(format!(
                "program format version {} is not supported (expected {FORMAT_VERSION})",
                program.version
            ));
        }
        Ok(program)
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).expect("programs always serialize")
    }
}
