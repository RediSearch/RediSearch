/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Stateful programs: ops sent over several connections to one server, each
//! awaiting its reply or leaving it for later, with server-chosen values such
//! as cursor ids carried from one op to another in variables.

use std::fmt::{self, Debug};
use std::hash::Hash;

use rand::Rng;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

const CONNS: u8 = 3;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(bound = "T: TypedOp", deny_unknown_fields)]
pub struct Program<T> {
    /// Arguments for starting the server.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<Bytes>,
    pub ops: Vec<Op<T>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(bound = "T: TypedOp")]
pub struct Op<T> {
    #[serde(default, skip_serializing_if = "is_default")]
    pub conn: Conn,
    #[serde(default, skip_serializing_if = "is_default")]
    pub reply: ReplyMode,
    /// Stores the value the reply binds, such as a cursor id, in a variable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bind: Option<Var>,
    #[serde(flatten)]
    pub kind: Kind<T>,
}

/// Serialized as `{"op": "barrier"}`, `{"op": "command", "argv": […]}`, or
/// the target's own op, which must also be tagged by `op`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(bound = "T: TypedOp", from = "KindRepr<T>", into = "KindRepr<T>")]
pub enum Kind<T> {
    /// Waits until the server has no background work left.
    Barrier,
    Command(Argv),
    Typed(T),
}

/// An op a target defines because it reasons about it: a schema it generates
/// against, a query it simplifies, a batch of writes it expands.
pub trait TypedOp:
    Clone + Debug + Eq + Hash + Serialize + DeserializeOwned + Send + Sync + 'static
{
    /// The commands it sends.
    fn lower(&self) -> Vec<Argv>;
    /// The values mutation may replace and shrinking may shorten.
    fn literals(&mut self) -> Vec<Literal<'_>>;
    /// The op one structural step simpler, each strictly smaller by
    /// [`TypedOp::size`]. Literals are shortened separately.
    fn simpler(&self) -> Vec<Self>;
    /// Structure only; literals are measured separately.
    fn size(&self) -> usize;
}

pub enum Literal<'a> {
    Word(&'a mut Bytes),
    Number(&'a mut i64),
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct Conn(pub u8);

/// A command token `$<n>` is replaced by variable `n`, or 0 while unset.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct Var(pub u8);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplyMode {
    #[default]
    Await,
    /// Read the reply only when the connection is next used.
    Defer,
}

#[derive(Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Bytes(pub Vec<u8>);

pub type Argv = Vec<Bytes>;

pub fn argv(words: &[&str]) -> Argv {
    words.iter().map(|w| Bytes::from(*w)).collect()
}

impl<T: TypedOp> Program<T> {
    /// Ops, arguments, typed-op structure, and literal lengths; every
    /// shrinking step decreases it.
    pub fn size(&self) -> usize {
        let mut total = self.ops.len() + self.args.len();
        for op in &self.ops {
            if let Kind::Typed(typed) = &op.kind {
                total += typed.size();
            }
            for literal in op.kind.clone().literals() {
                total += 1 + match literal {
                    Literal::Word(word) => word.0.len(),
                    Literal::Number(n) => {
                        usize::try_from(64 - n.unsigned_abs().leading_zeros()).expect("small")
                    }
                };
            }
        }
        total
    }
}

impl<T: TypedOp> Kind<T> {
    pub fn lower(&self) -> Vec<Argv> {
        match self {
            Kind::Barrier => Vec::new(),
            Kind::Command(argv) => vec![argv.clone()],
            Kind::Typed(typed) => typed.lower(),
        }
    }

    pub fn literals(&mut self) -> Vec<Literal<'_>> {
        match self {
            Kind::Barrier => Vec::new(),
            Kind::Command(argv) => argv.iter_mut().map(Literal::Word).collect(),
            Kind::Typed(typed) => typed.literals(),
        }
    }
}

impl Conn {
    /// Half the time the default connection, so most ops share one.
    pub fn random(rng: &mut impl Rng) -> Conn {
        if rng.random_bool(0.5) {
            Conn::default()
        } else {
            Conn(rng.random_range(0..CONNS))
        }
    }
}

impl Var {
    pub fn token(self) -> Bytes {
        format!("${}", self.0).into()
    }
}

#[derive(Serialize, Deserialize)]
#[serde(untagged, bound = "T: TypedOp")]
enum KindRepr<T> {
    BuiltIn(BuiltIn),
    Typed(T),
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
enum BuiltIn {
    Barrier,
    Command { argv: Argv },
}

impl<T> From<KindRepr<T>> for Kind<T> {
    fn from(repr: KindRepr<T>) -> Kind<T> {
        match repr {
            KindRepr::BuiltIn(BuiltIn::Barrier) => Kind::Barrier,
            KindRepr::BuiltIn(BuiltIn::Command { argv }) => Kind::Command(argv),
            KindRepr::Typed(typed) => Kind::Typed(typed),
        }
    }
}

impl<T> From<Kind<T>> for KindRepr<T> {
    fn from(kind: Kind<T>) -> KindRepr<T> {
        match kind {
            Kind::Barrier => KindRepr::BuiltIn(BuiltIn::Barrier),
            Kind::Command(argv) => KindRepr::BuiltIn(BuiltIn::Command { argv }),
            Kind::Typed(typed) => KindRepr::Typed(typed),
        }
    }
}

fn is_default<T: Default + PartialEq>(value: &T) -> bool {
    *value == T::default()
}

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
