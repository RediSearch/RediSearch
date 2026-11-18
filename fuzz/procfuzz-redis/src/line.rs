/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Commands as lines of text, as `redis-cli` reads them and `MONITOR` prints
//! them.

use crate::resp::{Argv, Bytes};

/// Splits a line into words as `redis-cli` does: words are separated by
/// whitespace, a word in double quotes may hold escapes such as `\x00`, and
/// one in single quotes only `\'`.
pub fn split(line: &str) -> Result<Argv, String> {
    let line = line.as_bytes();
    let mut words = Vec::new();
    let mut at = 0;
    loop {
        while line.get(at).is_some_and(u8::is_ascii_whitespace) {
            at += 1;
        }
        let Some(&first) = line.get(at) else {
            return Ok(words);
        };
        let mut word = Vec::new();
        if first == b'"' || first == b'\'' {
            at += 1;
            loop {
                let &c = line.get(at).ok_or("unterminated quotes")?;
                at += 1;
                match c {
                    c if c == first => break,
                    b'\\' if first == b'"' => {
                        let (c, len) = unescape(&line[at..]).ok_or("unterminated quotes")?;
                        word.push(c);
                        at += len;
                    }
                    b'\\' if line.get(at) == Some(&b'\'') => {
                        word.push(b'\'');
                        at += 1;
                    }
                    c => word.push(c),
                }
            }
            if line.get(at).is_some_and(|c| !c.is_ascii_whitespace()) {
                return Err("a closing quote must be followed by a space".to_owned());
            }
        } else {
            while let Some(&c) = line.get(at).filter(|c| !c.is_ascii_whitespace()) {
                word.push(c);
                at += 1;
            }
        }
        words.push(Bytes(word));
    }
}

/// The byte an escape after a backslash stands for, and the escape's length.
fn unescape(rest: &[u8]) -> Option<(u8, usize)> {
    let hex = rest
        .get(1..3)
        .and_then(|digits| std::str::from_utf8(digits).ok())
        .and_then(|digits| u8::from_str_radix(digits, 16).ok());
    Some(match (rest.first()?, hex) {
        (b'x', Some(byte)) => (byte, 3),
        (b'n', _) => (b'\n', 1),
        (b'r', _) => (b'\r', 1),
        (b't', _) => (b'\t', 1),
        (b'b', _) => (0x08, 1),
        (b'a', _) => (0x07, 1),
        (&c, _) => (c, 1),
    })
}

/// The line [`split`] reads back as `argv`: plain words as they are, others
/// in double quotes.
pub fn join(argv: &Argv) -> String {
    let words: Vec<String> = argv.iter().map(|word| quote(&word.0)).collect();
    words.join(" ")
}

fn quote(word: &[u8]) -> String {
    let plain = |c: char| !c.is_whitespace() && !c.is_control() && !"\"'\\".contains(c);
    let inner: String = match std::str::from_utf8(word) {
        Ok(text) if !text.is_empty() && text.chars().all(plain) => return text.to_owned(),
        Ok(text) => text.chars().map(escape).collect(),
        Err(_) => word
            .iter()
            .map(|&b| match char::from(b) {
                c if c.is_ascii() => escape(c),
                _ => format!("\\x{b:02x}"),
            })
            .collect(),
    };
    format!("\"{inner}\"")
}

fn escape(c: char) -> String {
    match c {
        '"' | '\\' => format!("\\{c}"),
        '\n' => "\\n".to_owned(),
        '\r' => "\\r".to_owned(),
        '\t' => "\\t".to_owned(),
        c if c.is_ascii_control() => format!("\\x{:02x}", u32::from(c)),
        c => c.to_string(),
    }
}

/// The command of a line `MONITOR` printed for a client's request, such as
/// `1700000000.000000 [0 127.0.0.1:50000] "SET" "k" "v"`. `None` for other
/// lines, including the commands scripts and modules call.
pub fn monitored(line: &str) -> Option<&str> {
    let (time, rest) = line.split_once(" [")?;
    time.parse::<f64>().ok()?;
    let (db_and_client, command) = rest.split_once("] ")?;
    let (_, client) = db_and_client.split_once(' ')?;
    client.contains(':').then_some(command)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resp::argv;

    #[test]
    fn joined_words_split_back() {
        let words = vec![
            Bytes::from("FT.SEARCH"),
            Bytes::from("hello world"),
            Bytes::from(""),
            Bytes::from("日本語"),
            Bytes::from("say \"hi\"\n\\"),
            Bytes(vec![0, 0xff, b'a']),
        ];
        let line = join(&words);
        assert_eq!(
            line,
            r#"FT.SEARCH "hello world" "" 日本語 "say \"hi\"\n\\" "\x00\xffa""#
        );
        assert_eq!(split(&line), Ok(words));
    }

    #[test]
    fn lines_split_as_redis_cli_splits_them() {
        assert_eq!(
            split(r#"  SET 'it\'s' "\x4" x"#),
            Ok(argv(&["SET", "it's", "x4", "x"]))
        );
        assert!(split(r#"SET "k"v"#).is_err());
        assert!(split(r#"SET "k"#).is_err());
    }

    #[test]
    fn only_monitored_client_requests_are_commands() {
        let line = r#"1700000000.123456 [0 unix:/tmp/s.sock] "FT.INFO" "i""#;
        assert_eq!(monitored(line), Some(r#""FT.INFO" "i""#));
        assert_eq!(monitored(r#"1700000000.123456 [0 ] "HSET" "d""#), None);
        assert_eq!(monitored(r#"1700000000.123456 [0 lua] "GET" "k""#), None);
        assert_eq!(monitored("OK"), None);
    }
}
