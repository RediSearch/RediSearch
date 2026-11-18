/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! `redis-server` with the module and RedisJSON loaded, as a
//! [`procfuzz::server::Server`].

use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::process::Command;

use procfuzz::judge::basename;
use procfuzz::program::{Argv, Bytes, argv};
use procfuzz::server::{Harness, Server};
use serde::{Deserialize, Serialize};

pub const DEBUG: &str = "_FT.DEBUG";
/// Undo every pause and hook a program can set.
const RELEASE: [&[&str]; 4] = [
    &[DEBUG, "WORKERS", "RESUME"],
    &[
        DEBUG,
        "BG_SCAN_CONTROLLER",
        "SET_PAUSE_BEFORE_SCAN",
        "false",
    ],
    &[DEBUG, "BG_SCAN_CONTROLLER", "SET_BG_INDEX_RESUME"],
    &[DEBUG, "SYNC_POINT", "CLEAR"],
];

#[derive(Clone, Debug, Serialize)]
pub struct Env {
    pub redis_server: PathBuf,
    pub module: PathBuf,
    /// RedisJSON, loaded after the module as the Python tests do.
    pub rejson: PathBuf,
}

impl Env {
    pub fn from_env() -> Result<Env, String> {
        let var = |name: &str| {
            std::env::var_os(name).map(PathBuf::from).ok_or_else(|| {
                format!("{name} is not set; run fuzz/setup.sh and source bin/fuzz/env.sh")
            })
        };
        Ok(Env {
            redis_server: var("REDIS_SERVER")?,
            module: var("REDISEARCH_MODULE")?,
            rejson: var("REJSON_MODULE")?,
        })
    }
}

impl Server for Env {
    type Value = Value;

    fn command(&self, dir: &Path, socket: &Path, args: &[Bytes]) -> Command {
        let mut command = Command::new(&self.redis_server);
        command
            .args(["--port", "0", "--save", "", "--appendonly", "no"])
            .args(["--enable-debug-command", "yes", "--unixsocket"])
            .arg(socket)
            .arg("--dir")
            .arg(dir)
            .arg("--loadmodule")
            .arg(&self.module)
            .args(args.iter().map(|a| OsStr::from_bytes(a.as_slice())))
            .arg("--loadmodule")
            .arg(&self.rejson);
        command
    }

    fn encode(command: &Argv) -> Vec<u8> {
        encode(command)
    }

    fn decode(buf: &[u8]) -> Result<Option<(Value, usize)>, String> {
        decode_at(buf, 0)
    }

    fn ping(&self) -> Argv {
        argv(&["PING"])
    }

    fn shutdown(&self) -> Argv {
        argv(&["SHUTDOWN", "NOSAVE"])
    }

    /// `FT.AGGREGATE … WITHCURSOR` and `FT.CURSOR READ` reply
    /// `[results, cursor id]`.
    fn bound(&self, reply: &Value) -> Option<i64> {
        match reply {
            Value::Array(items) => match items.as_slice() {
                [_, Value::Int(id)] => Some(*id),
                _ => None,
            },
            _ => None,
        }
    }

    fn idle(&self, harness: &mut Harness<Self>) -> Option<bool> {
        for name in indexes(harness)? {
            let info = harness.call(&vec!["FT.INFO".into(), name])?;
            if field(&info, "indexing") != Some(0) {
                return Some(false);
            }
        }
        let stats = harness.call(&argv(&[DEBUG, "WORKERS", "STATS"]))?;
        let idle = ["totalPendingJobs", "numJobsInProgress"]
            .iter()
            .all(|name| field(&stats, name) == Some(0));
        if idle {
            harness.call(&argv(&[DEBUG, "GC_WAIT_FOR_JOBS"]))?;
        }
        Some(idle)
    }

    /// Stops at the first command the server does not answer: its main thread
    /// is blocked, so the rest would time out too.
    fn release(&self, harness: &mut Harness<Self>) -> Option<()> {
        for command in RELEASE {
            harness.call(&argv(command))?;
        }
        for name in indexes(harness)? {
            let mut command = argv(&[DEBUG, "GC_CONTINUE_SCHEDULE"]);
            command.push(name);
            harness.call(&command)?;
        }
        Some(())
    }

    /// RediSearch's assertion message, or Redis's panic message.
    fn failure(&self, line: &str) -> Option<String> {
        assertion(line).or_else(|| panic_location(line).map(|l| format!("panic:{l}")))
    }

    fn crash_frame(&self, log: &str) -> Option<String> {
        crash_frame(log)
    }
}

fn indexes(harness: &mut Harness<Env>) -> Option<Vec<Bytes>> {
    let Value::Array(names) = harness.call(&argv(&["FT._LIST"]))? else {
        return None;
    };
    Some(
        names
            .into_iter()
            .filter_map(|name| match name {
                Value::Bulk(name) | Value::Simple(name) => Some(name),
                _ => None,
            })
            .collect(),
    )
}

/// An integer in a flat `[name, value, …]` reply.
fn field(reply: &Value, name: &str) -> Option<i64> {
    let Value::Array(items) = reply else {
        return None;
    };
    items.chunks(2).find_map(|pair| match pair {
        [Value::Bulk(key) | Value::Simple(key), value] if key.as_slice() == name.as_bytes() => {
            match value {
                Value::Int(n) => Some(*n),
                Value::Bulk(text) | Value::Simple(text) => {
                    std::str::from_utf8(text.as_slice()).ok()?.parse().ok()
                }
                _ => None,
            }
        }
        _ => None,
    })
}

/// RediSearch's assertion message: `==> /src/tag_index.c:337 'x' is not true`.
fn assertion(line: &str) -> Option<String> {
    let location = line.split("==> ").nth(1)?.split_whitespace().next()?;
    Some(format!("assert:{}", basename(location)))
}

/// Redis's panic message ends with the source location after `#`.
fn panic_location(log: &str) -> Option<String> {
    let line = log
        .lines()
        .find(|line| line.contains("Guru Meditation: "))?;
    Some(basename(line.rsplit('#').next()?.trim()).to_owned())
}

/// The frame a Redis crash report blames: the first module frame, else the
/// first `redis-server` frame after the signal handler.
fn crash_frame(log: &str) -> Option<String> {
    let report = &log[log.find("------ STACK TRACE ------")?..];
    let frames: Vec<(&str, &str)> = report
        .lines()
        .skip(1)
        .take_while(|line| !line.starts_with("------ "))
        .filter_map(stack_frame)
        .collect();
    let handler = frames
        .iter()
        .position(|(_, symbol)| symbol.contains("sigtramp") || symbol.contains("sig_handler"))
        .map_or(0, |i| i + 1);
    frames
        .iter()
        .find(|(binary, _)| *binary == "redisearch.so")
        .or_else(|| {
            frames[handler..]
                .iter()
                .find(|(binary, _)| *binary == "redis-server")
        })
        .map(|(binary, symbol)| format!("{binary}:{symbol}"))
}

/// Parses a macOS (`3 redis-server 0x1 call + 12`) or Linux
/// (`/bin/redis-server *:6379(call+0xc)[0x1]`) backtrace line.
fn stack_frame(line: &str) -> Option<(&str, &str)> {
    let words: Vec<&str> = line.split_whitespace().collect();
    if let [index, binary, address, symbol, ..] = words.as_slice()
        && index.parse::<u32>().is_ok()
        && address.starts_with("0x")
    {
        return Some((basename(binary), symbol));
    }
    let (path, rest) = line.trim().split_once('(')?;
    let symbol = rest.split(['+', ')']).next().filter(|s| !s.is_empty())?;
    Some((basename(path.split_whitespace().next()?), symbol))
}

/// A RESP2 reply.
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

fn encode(argv: &Argv) -> Vec<u8> {
    let mut out = format!("*{}\r\n", argv.len()).into_bytes();
    for arg in argv {
        out.extend_from_slice(format!("${}\r\n", arg.0.len()).as_bytes());
        out.extend_from_slice(&arg.0);
        out.extend_from_slice(b"\r\n");
    }
    out
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

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(buf: &[u8]) -> Result<Option<(Value, usize)>, String> {
        <Env as Server>::decode(buf)
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

    #[test]
    fn crash_reports_become_signatures() {
        let env = Env {
            redis_server: PathBuf::new(),
            module: PathBuf::new(),
            rejson: PathBuf::new(),
        };
        assert_eq!(
            env.failure("==> /src/tag_index.c:337 'x' is not true")
                .as_deref(),
            Some("assert:tag_index.c:337")
        );
        assert_eq!(
            env.failure("# Guru Meditation: DEBUG PANIC called at Unix time 1 #debug.c:698")
                .as_deref(),
            Some("panic:debug.c:698")
        );
        let frames = [
            (
                "------ STACK TRACE ------\n\
                 0   libsystem_platform.dylib 0x1 _sigtramp + 56\n\
                 1   redis-server             0x2 call + 12\n\
                 2   redisearch.so            0x3 TagIndex_Open + 12\n\
                 ------ INFO OUTPUT ------\n\
                 3   redisearch.so            0x4 NotAFrame + 1\n",
                "redisearch.so:TagIndex_Open",
            ),
            (
                "------ STACK TRACE ------\n\
                 0   redis-server             0x1 debugCommand + 1280\n\
                 Backtrace:\n\
                 0   libsystem_platform.dylib 0x1 _sigtramp + 56\n\
                 1   libsystem_c.dylib        0x2 nanosleep + 220\n\
                 2   redis-server             0x3 debugCommand + 900\n",
                "redis-server:debugCommand",
            ),
            (
                "------ STACK TRACE ------\n\
                 /lib/x86_64-linux-gnu/libc.so.6(+0x42520)[0x7f]\n\
                 /bin/redisearch.so(IndexSpec_Free+0x1c)[0x7f]\n",
                "redisearch.so:IndexSpec_Free",
            ),
        ];
        for (log, frame) in frames {
            assert_eq!(env.crash_frame(log).as_deref(), Some(frame), "{log}");
        }
    }
}
