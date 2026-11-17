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
        prefix: Bytes,
        fields: Vec<Field>,
    },
    Query {
        index: Bytes,
        query: Query,
        #[serde(default)]
        options: QueryOptions,
    },
    Barrier,
    Command {
        argv: Argv,
    },
    /// `n` writes under `prefix`, with values for `fields` drawn from the
    /// fixed dictionary by a PRNG seeded with `seed`.
    Populate {
        prefix: Bytes,
        fields: Vec<Field>,
        n: u32,
        seed: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Field {
    pub name: Bytes,
    #[serde(flatten)]
    pub kind: FieldKind,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub sortable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FieldKind {
    Text,
    Numeric,
    Tag,
    Geo,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes_are_text_when_valid_utf8_and_hex_otherwise() {
        assert_eq!(serde_json::to_string(&Bytes::from("é")).unwrap(), r#""é""#);
        let binary = Bytes(vec![b'a', 0xe0, 0x00, 0xff]);
        let json = serde_json::to_string(&binary).unwrap();
        assert_eq!(json, r#"{"hex":"61e000ff"}"#);
        assert_eq!(serde_json::from_str::<Bytes>(&json).unwrap(), binary);
    }

    #[test]
    fn program_round_trips() {
        let program = Program {
            version: FORMAT_VERSION,
            module_args: argv(&["WORKERS", "2"]),
            ops: vec![
                Op {
                    conn: Conn(1),
                    reply: ReplyMode::Defer,
                    kind: OpKind::Create {
                        index: "i1".into(),
                        prefix: "doc:".into(),
                        fields: vec![Field {
                            name: "t".into(),
                            kind: FieldKind::Text,
                            sortable: true,
                        }],
                    },
                },
                Op {
                    conn: Conn::default(),
                    reply: ReplyMode::Await,
                    kind: OpKind::Query {
                        index: "i1".into(),
                        query: Query::Not(Box::new(Query::Range {
                            field: "n".into(),
                            min: Bound::Exclusive(-3),
                            max: Bound::Unbounded,
                        })),
                        options: QueryOptions {
                            no_content: true,
                            limit: Some((0, 10)),
                            dialect: Some(2),
                        },
                    },
                },
                Op {
                    conn: Conn::default(),
                    reply: ReplyMode::Await,
                    kind: OpKind::Command {
                        argv: vec!["PING".into(), Bytes(vec![0xe0])],
                    },
                },
            ],
        };
        assert_eq!(Program::from_json(&program.to_json()).unwrap(), program);
    }
}
