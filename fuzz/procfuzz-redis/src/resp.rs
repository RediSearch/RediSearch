/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! RESP2, the protocol Redis speaks, as a [`Codec`].

use std::fmt;

use procfuzz::service::Codec;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A RESP2 reply.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Value {
    Nil,
    Simple(Bytes),
    Error(Bytes),
    Int(i64),
    Bulk(Bytes),
    Array(Vec<Value>),
}

/// Requests are arrays of bulk strings.
pub struct Resp2;

impl Codec for Resp2 {
    type Request = Argv;
    type Reply = Value;

    fn encode(&self, request: &Argv, out: &mut Vec<u8>) {
        encode(request, out);
    }

    fn decode(&self, buf: &[u8]) -> Result<Option<(Value, usize)>, String> {
        decode_at(buf, 0)
    }
}

#[derive(Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Bytes(pub Vec<u8>);

pub type Argv = Vec<Bytes>;

pub fn argv(words: &[&str]) -> Argv {
    words.iter().map(|w| Bytes::from(*w)).collect()
}

fn encode(argv: &Argv, out: &mut Vec<u8>) {
    out.extend_from_slice(format!("*{}\r\n", argv.len()).as_bytes());
    for arg in argv {
        out.extend_from_slice(format!("${}\r\n", arg.0.len()).as_bytes());
        out.extend_from_slice(&arg.0);
        out.extend_from_slice(b"\r\n");
    }
}

fn decode_at(buf: &[u8], at: usize) -> Result<Option<(Value, usize)>, String> {
    let Some(&kind) = buf.get(at) else {
        return Ok(None);
    };
    let Some(end) = buf[at..].windows(2).position(|w| w == b"\r\n") else {
        return Ok(None);
    };
    if end == 0 {
        return Err("empty line".to_owned());
    }
    let line = &buf[at + 1..at + end];
    let next = at + end + 2;
    let length = || {
        std::str::from_utf8(line)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .ok_or_else(|| format!("bad integer {:?}", String::from_utf8_lossy(line)))
    };
    let value = match kind {
        b'+' => Value::Simple(Bytes(line.to_vec())),
        b'-' => Value::Error(Bytes(line.to_vec())),
        b':' => Value::Int(length()?),
        b'$' | b'*' if length()? == -1 => Value::Nil,
        b'$' => {
            let end = usize::try_from(length()?)
                .ok()
                .and_then(|length| next.checked_add(length))
                .ok_or("bad bulk length")?;
            return match buf.get(end..).and_then(|rest| rest.get(..2)) {
                None => Ok(None),
                Some(b"\r\n") => Ok(Some((Value::Bulk(Bytes(buf[next..end].to_vec())), end + 2))),
                Some(_) => Err("bulk string not terminated by CRLF".to_owned()),
            };
        }
        b'*' => {
            let mut items = Vec::new();
            let mut cursor = next;
            for _ in 0..usize::try_from(length()?).map_err(|e| e.to_string())? {
                let Some((item, after)) = decode_at(buf, cursor)? else {
                    return Ok(None);
                };
                items.push(item);
                cursor = after;
            }
            return Ok(Some((Value::Array(items), cursor)));
        }
        other => return Err(format!("unknown reply type byte 0x{other:02x}")),
    };
    Ok(Some((value, next)))
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

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(buf: &[u8]) -> Result<Option<(Value, usize)>, String> {
        Resp2.decode(buf)
    }

    #[test]
    fn replies_decode_only_once_complete_and_violations_are_errors() {
        let reply = b"*4\r\n$5\r\na\r\nbc\r\n:-7\r\n*1\r\n+x\r\n$-1\r\n-ERR no\r\n";
        let first_len = reply.len() - b"-ERR no\r\n".len();
        for end in 0..first_len {
            assert_eq!(decode(&reply[..end]), Ok(None), "prefix of length {end}");
        }
        assert_eq!(
            decode(reply),
            Ok(Some((
                Value::Array(vec![
                    Value::Bulk("a\r\nbc".into()),
                    Value::Int(-7),
                    Value::Array(vec![Value::Simple("x".into())]),
                    Value::Nil,
                ]),
                first_len
            )))
        );
        for bad in [
            &b"?x\r\n"[..],
            b":abc\r\n",
            b"$2\r\nabXY",
            b"*-5\r\n",
            b"\r\n",
            b"*1\r\n\r\n",
        ] {
            assert!(decode(bad).is_err(), "{:?}", String::from_utf8_lossy(bad));
        }
    }
}
