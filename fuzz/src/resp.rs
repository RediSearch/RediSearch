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

/// Decodes the first reply in `buf`: `Ok(None)` until it has fully arrived,
/// then the reply and the number of bytes it used.
pub fn decode(buf: &[u8]) -> Result<Option<(Value, usize)>, String> {
    parse(buf, 0)
}

fn parse(buf: &[u8], at: usize) -> Result<Option<(Value, usize)>, String> {
    let Some(&kind) = buf.get(at) else {
        return Ok(None);
    };
    let Some(end) = buf[at..].windows(2).position(|w| w == b"\r\n") else {
        return Ok(None);
    };
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
            let end = next + usize::try_from(length()?).map_err(|e| e.to_string())?;
            return match buf.get(end..end + 2) {
                None => Ok(None),
                Some(b"\r\n") => Ok(Some((Value::Bulk(Bytes(buf[next..end].to_vec())), end + 2))),
                Some(_) => Err("bulk string not terminated by CRLF".to_owned()),
            };
        }
        b'*' => {
            let mut items = Vec::new();
            let mut cursor = next;
            for _ in 0..usize::try_from(length()?).map_err(|e| e.to_string())? {
                let Some((item, after)) = parse(buf, cursor)? else {
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

#[cfg(test)]
mod tests {
    use super::*;

    /// An incomplete reply is not an error, so reads can resume; a malformed
    /// one is.
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
        for bad in [&b"?x\r\n"[..], b":abc\r\n", b"$2\r\nabXY", b"*-5\r\n"] {
            assert!(decode(bad).is_err(), "{:?}", String::from_utf8_lossy(bad));
        }
    }
}
