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
pub const DEBUG: &str = "_FT.DEBUG";

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

/// Text when the bytes are UTF-8, `{"hex": …}` otherwise.
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

/// Holds a value only the server knows, such as a cursor id. An op with
/// `bind` stores the id from its reply in the variable, and a command token
/// `$<n>` is replaced by variable `n` (0 while unset).
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
    /// Read the reply only when the connection is next used.
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
    /// Writes keys `<prefix>0` to `<prefix><n-1>`, with values chosen by a
    /// generator seeded with `seed`.
    Populate {
        #[serde(default, skip_serializing_if = "is_default")]
        on: DataType,
        prefix: Bytes,
        fields: Vec<Field>,
        n: u32,
        seed: u64,
    },
    /// Waits until indexing and background jobs are done.
    Barrier,
    Command {
        argv: Argv,
    },
}

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
    /// Valid only as the whole query.
    Knn {
        filter: Box<Query>,
        field: Bytes,
        k: u32,
        /// FLOAT32 components, little-endian.
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

/// A value inside an op that mutation may replace and shrinking may shorten:
/// a query term, tag value, or range bound, or a command token.
pub enum Literal<'a> {
    Word(&'a mut Bytes),
    Number(&'a mut i64),
}

impl OpKind {
    pub fn literals(&mut self) -> Vec<Literal<'_>> {
        let mut out = Vec::new();
        match self {
            OpKind::Query { query, .. } => query.collect_literals(&mut out),
            OpKind::Command { argv } => out.extend(argv.iter_mut().map(Literal::Word)),
            OpKind::Create { .. }
            | OpKind::Alter { .. }
            | OpKind::Populate { .. }
            | OpKind::Barrier => {}
        }
        out
    }
}

impl Query {
    fn collect_literals<'a>(&'a mut self, out: &mut Vec<Literal<'a>>) {
        match self {
            Query::All => {}
            Query::Term(term) | Query::InField { term, .. } => out.push(Literal::Word(term)),
            Query::Tag { values, .. } => out.extend(values.iter_mut().map(Literal::Word)),
            Query::Range { min, max, .. } => {
                for bound in [min, max] {
                    if let Bound::Inclusive(n) | Bound::Exclusive(n) = bound {
                        out.push(Literal::Number(n));
                    }
                }
            }
            Query::And(children) | Query::Or(children) => children
                .iter_mut()
                .for_each(|child| child.collect_literals(out)),
            Query::Not(child) | Query::Knn { filter: child, .. } => child.collect_literals(out),
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
