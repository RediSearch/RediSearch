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

#[derive(Clone, Default, PartialEq, Eq, Hash)]
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

impl From<Vec<u8>> for Bytes {
    fn from(v: Vec<u8>) -> Self {
        Bytes(v)
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
            BytesRepr::Hex { hex } => from_hex(&hex).map(Bytes).map_err(serde::de::Error::custom),
        }
    }
}

pub fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn from_hex(s: &str) -> Result<Vec<u8>, String> {
    if !s.len().is_multiple_of(2) {
        return Err(format!("odd-length hex string {s:?}"));
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|e| format!("{s:?}: {e}")))
        .collect()
}

pub type Argv = Vec<Bytes>;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Program {
    pub version: u32,
    #[serde(default)]
    pub setup: Setup,
    pub ops: Vec<Op>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Setup {
    #[serde(default)]
    pub module_args: Vec<Bytes>,
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct Conn(pub u8);

impl Conn {
    pub fn is_default(&self) -> bool {
        self.0 == 0
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplyMode {
    #[default]
    Await,
    Defer,
}

impl ReplyMode {
    pub fn is_await(&self) -> bool {
        *self == ReplyMode::Await
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Op {
    #[serde(default, skip_serializing_if = "Conn::is_default")]
    pub conn: Conn,
    #[serde(default, skip_serializing_if = "ReplyMode::is_await")]
    pub reply: ReplyMode,
    #[serde(flatten)]
    pub kind: OpKind,
}

impl From<OpKind> for Op {
    fn from(kind: OpKind) -> Self {
        Op {
            conn: Conn::default(),
            reply: ReplyMode::default(),
            kind,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum OpKind {
    Create {
        index: Bytes,
        prefix: Bytes,
        fields: Vec<Field>,
    },
    Write {
        key: Bytes,
        fields: Vec<(Bytes, Bytes)>,
    },
    Delete {
        key: Bytes,
    },
    Query {
        index: Bytes,
        query: Query,
        #[serde(default)]
        options: QueryOptions,
    },
    Raw {
        argv: Argv,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Field {
    pub name: Bytes,
    #[serde(flatten)]
    pub kind: FieldKind,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub sortable: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FieldKind {
    Text,
    Numeric,
    Tag,
    Geo,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Bound {
    Unbounded,
    Inclusive(i64),
    Exclusive(i64),
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
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
    fn utf8_bytes_serialize_as_strings() {
        let json = serde_json::to_string(&Bytes::from("héllo")).unwrap();
        assert_eq!(json, r#""héllo""#);
    }

    #[test]
    fn invalid_utf8_round_trips_through_hex() {
        let original = Bytes(vec![b'a', 0xe0, 0x00, 0xff]);
        let json = serde_json::to_string(&original).unwrap();
        assert_eq!(json, r#"{"hex":"61e000ff"}"#);
        assert_eq!(serde_json::from_str::<Bytes>(&json).unwrap(), original);
    }

    #[test]
    fn malformed_hex_is_rejected() {
        assert!(serde_json::from_str::<Bytes>(r#"{"hex":"abc"}"#).is_err());
        assert!(serde_json::from_str::<Bytes>(r#"{"hex":"zz"}"#).is_err());
    }

    #[test]
    fn program_round_trips() {
        let program = Program {
            version: FORMAT_VERSION,
            setup: Setup {
                module_args: vec!["WORKERS".into(), "2".into()],
            },
            ops: vec![
                OpKind::Create {
                    index: "i1".into(),
                    prefix: "doc:".into(),
                    fields: vec![Field {
                        name: "t".into(),
                        kind: FieldKind::Text,
                        sortable: true,
                    }],
                }
                .into(),
                Op {
                    conn: Conn(1),
                    reply: ReplyMode::Defer,
                    kind: OpKind::Query {
                        index: "i1".into(),
                        query: Query::And(vec![
                            Query::Term("hello".into()),
                            Query::Not(Box::new(Query::Range {
                                field: "n".into(),
                                min: Bound::Exclusive(-3),
                                max: Bound::Unbounded,
                            })),
                        ]),
                        options: QueryOptions {
                            no_content: true,
                            limit: Some((0, 10)),
                            dialect: Some(2),
                        },
                    },
                },
                OpKind::Raw {
                    argv: vec!["PING".into(), Bytes(vec![0xe0])],
                }
                .into(),
            ],
        };
        let parsed = Program::from_json(&program.to_json()).unwrap();
        assert_eq!(parsed, program);
    }

    #[test]
    fn unsupported_version_is_rejected() {
        let err = Program::from_json(r#"{"version": 999, "ops": []}"#).unwrap_err();
        assert!(err.contains("999"), "{err}");
    }

    #[test]
    fn unknown_op_fields_are_rejected() {
        let text = r#"{"version": 1, "ops": [{"op": "delete", "key": "k", "bogus": 1}]}"#;
        assert!(Program::from_json(text).is_err());
    }
}
