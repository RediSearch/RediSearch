/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use serde::{Deserialize, Serialize};

use crate::program::{Argv, Bytes};

const MAX_DEPTH: usize = 64;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Value {
    Nil,
    Simple(Bytes),
    Error(Bytes),
    Int(i64),
    Bulk(Bytes),
    Array(Vec<Value>),
}

pub fn encode(argv: &Argv) -> Vec<u8> {
    let mut out = format!("*{}\r\n", argv.len()).into_bytes();
    for arg in argv {
        out.extend_from_slice(format!("${}\r\n", arg.0.len()).as_bytes());
        out.extend_from_slice(&arg.0);
        out.extend_from_slice(b"\r\n");
    }
    out
}

#[derive(Debug, PartialEq)]
pub enum Decoded {
    Complete(Value, usize),
    Incomplete,
    Invalid(String),
}

pub fn decode(buf: &[u8]) -> Decoded {
    match parse(buf, 0, 0) {
        Ok(Some((value, end))) => Decoded::Complete(value, end),
        Ok(None) => Decoded::Incomplete,
        Err(e) => Decoded::Invalid(e),
    }
}

type Parsed = Result<Option<(Value, usize)>, String>;

fn parse(buf: &[u8], at: usize, depth: usize) -> Parsed {
    if depth > MAX_DEPTH {
        return Err(format!("reply nested deeper than {MAX_DEPTH}"));
    }
    let Some((&kind, _)) = buf.get(at..).and_then(<[u8]>::split_first) else {
        return Ok(None);
    };
    let Some((line, next)) = line(buf, at + 1) else {
        return Ok(None);
    };
    match kind {
        b'+' => Ok(Some((Value::Simple(Bytes(line.to_vec())), next))),
        b'-' => Ok(Some((Value::Error(Bytes(line.to_vec())), next))),
        b':' => Ok(Some((Value::Int(integer(line)?), next))),
        b'$' => {
            let len = integer(line)?;
            if len == -1 {
                return Ok(Some((Value::Nil, next)));
            }
            let len = usize::try_from(len).map_err(|_| format!("bad bulk length {len}"))?;
            let end = next + len;
            if buf.len() < end + 2 {
                return Ok(None);
            }
            if &buf[end..end + 2] != b"\r\n" {
                return Err("bulk string not terminated by CRLF".to_owned());
            }
            Ok(Some((Value::Bulk(Bytes(buf[next..end].to_vec())), end + 2)))
        }
        b'*' => {
            let len = integer(line)?;
            if len == -1 {
                return Ok(Some((Value::Nil, next)));
            }
            let len = usize::try_from(len).map_err(|_| format!("bad array length {len}"))?;
            let mut items = Vec::new();
            let mut cursor = next;
            for _ in 0..len {
                match parse(buf, cursor, depth + 1)? {
                    Some((item, after)) => {
                        items.push(item);
                        cursor = after;
                    }
                    None => return Ok(None),
                }
            }
            Ok(Some((Value::Array(items), cursor)))
        }
        other => Err(format!("unknown reply type byte 0x{other:02x}")),
    }
}

fn line(buf: &[u8], from: usize) -> Option<(&[u8], usize)> {
    let rest = buf.get(from..)?;
    let pos = rest.windows(2).position(|w| w == b"\r\n")?;
    Some((&rest[..pos], from + pos + 2))
}

fn integer(line: &[u8]) -> Result<i64, String> {
    std::str::from_utf8(line)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| format!("bad integer {:?}", String::from_utf8_lossy(line)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn complete(input: &[u8]) -> Value {
        match decode(input) {
            Decoded::Complete(value, used) => {
                assert_eq!(used, input.len());
                value
            }
            other => panic!("expected a complete reply, got {other:?}"),
        }
    }

    #[test]
    fn encodes_binary_safe_arguments() {
        let argv = vec![Bytes::from("SET"), Bytes(vec![0, b'\r', b'\n'])];
        assert_eq!(encode(&argv), b"*2\r\n$3\r\nSET\r\n$3\r\n\0\r\n\r\n");
    }

    #[test]
    fn decodes_each_reply_type() {
        assert_eq!(complete(b"+OK\r\n"), Value::Simple("OK".into()));
        assert_eq!(complete(b"-ERR no\r\n"), Value::Error("ERR no".into()));
        assert_eq!(complete(b":-42\r\n"), Value::Int(-42));
        assert_eq!(complete(b"$-1\r\n"), Value::Nil);
        assert_eq!(
            complete(b"$5\r\na\r\nbc\r\n"),
            Value::Bulk("a\r\nbc".into())
        );
        assert_eq!(
            complete(b"*2\r\n:1\r\n*1\r\n$0\r\n\r\n"),
            Value::Array(vec![
                Value::Int(1),
                Value::Array(vec![Value::Bulk("".into())])
            ])
        );
    }

    #[test]
    fn every_strict_prefix_is_incomplete() {
        let full = b"*3\r\n$3\r\nfoo\r\n:7\r\n*1\r\n+x\r\n";
        for end in 0..full.len() {
            assert_eq!(
                decode(&full[..end]),
                Decoded::Incomplete,
                "prefix of length {end}"
            );
        }
        assert!(matches!(decode(full), Decoded::Complete(_, n) if n == full.len()));
    }

    #[test]
    fn trailing_bytes_are_left_for_the_next_reply() {
        assert_eq!(
            decode(b"+A\r\n+B\r\n"),
            Decoded::Complete(Value::Simple("A".into()), 4)
        );
    }

    #[test]
    fn protocol_violations_are_invalid() {
        assert!(matches!(decode(b"?x\r\n"), Decoded::Invalid(_)));
        assert!(matches!(decode(b":abc\r\n"), Decoded::Invalid(_)));
        assert!(matches!(decode(b"$2\r\nabXY"), Decoded::Invalid(_)));
        assert!(matches!(decode(b"*-5\r\n"), Decoded::Invalid(_)));
    }
}
