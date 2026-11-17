/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use serde::{Deserialize, Serialize};

use crate::logic;
use crate::lower::lower;
use crate::program::Program;
use crate::run::{Exit, Trace, run};
use crate::target::Env;
use crate::target::Reply;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    Sanitizer,
    Leak,
    Assertion,
    Crash,
    Hang,
    Protocol,
    Logic,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Finding {
    pub kind: Kind,
    pub signature: String,
}

/// Runs `program` on a fresh server. An error means the server did not start.
pub fn execute(program: &Program, env: &Env) -> Result<(Trace, Vec<Finding>), String> {
    let trace = run(program, env)?;
    let findings = judge(program, &trace);
    Ok((trace, findings))
}

fn judge(program: &Program, trace: &Trace) -> Vec<Finding> {
    let log = String::from_utf8_lossy(trace.log.as_slice());
    // Printing a crash report runs module code that can trip further
    // assertions, so only the first report counts.
    let mut findings: Vec<Finding> = log
        .lines()
        .find_map(|line| {
            if let Some(summary) = line.split("SUMMARY: AddressSanitizer: ").nth(1)
                && !summary.contains(" leaked in ")
            {
                Some(Finding {
                    kind: Kind::Sanitizer,
                    signature: format!("asan:{}", sanitizer_summary(summary)),
                })
            } else {
                let location = line.split("==> ").nth(1)?.split_whitespace().next()?;
                Some(Finding {
                    kind: Kind::Assertion,
                    signature: format!("assert:{}", basename(location)),
                })
            }
        })
        .into_iter()
        .chain(leaks(&log))
        .collect();

    let frame = crash_frame(&log);
    match trace.exit {
        Exit::Hung { at_op } => findings.push(Finding {
            kind: Kind::Hang,
            signature: format!("hang:{}", frame.unwrap_or_else(|| command(program, at_op))),
        }),
        Exit::Died { at_op } if findings.is_empty() => findings.push(Finding {
            kind: Kind::Crash,
            signature: match (panic_location(&log), frame) {
                (Some(location), _) => format!("panic:{location}"),
                (None, Some(frame)) => format!("crash:{frame}"),
                (None, None) => format!(
                    "crash:{}:signal{}:exit{}",
                    command(program, at_op),
                    trace.status.signal.unwrap_or(0),
                    trace.status.code.unwrap_or(0)
                ),
            },
        }),
        _ => {}
    }
    findings.extend(
        trace
            .steps
            .iter()
            .filter(|step| matches!(step.reply, Some(Reply::Invalid(_))))
            .map(|step| Finding {
                kind: Kind::Protocol,
                signature: format!("protocol:{}", command(program, step.op)),
            }),
    );
    findings.extend(logic::check(program, trace));
    findings.sort();
    findings.dedup();
    findings
}

/// `heap-use-after-free /src/spec.c:123:7 in IndexSpec_Free` becomes
/// `heap-use-after-free:IndexSpec_Free:spec.c:123`. Without debug
/// information the location is a binary and an offset that changes with
/// every build, so only the binary's name is kept.
fn sanitizer_summary(summary: &str) -> String {
    let mut words = summary.split_whitespace();
    let bug = words.next().unwrap_or_default();
    let location = words.next().unwrap_or_default();
    let location = match location.strip_prefix('(') {
        Some(binary) => binary.split(':').next().unwrap_or_default().to_owned(),
        None => basename(location)
            .split(':')
            .take(2)
            .collect::<Vec<_>>()
            .join(":"),
    };
    match summary.split_once(" in ") {
        Some((_, function)) => {
            let function = function.split("+0x").next().unwrap_or(function);
            format!("{bug}:{function}:{location}")
        }
        None => format!("{bug}:{location}"),
    }
}

/// Names each leak by its first frame outside an allocator.
fn leaks(log: &str) -> Vec<Finding> {
    log.split("leak of ")
        .skip(1)
        .filter_map(|report| {
            report
                .lines()
                .skip(1)
                .take_while(|line| !line.trim().is_empty())
                .filter_map(|line| line.split(" in ").nth(1)?.split_whitespace().next())
                .map(|symbol| symbol.split("+0x").next().unwrap_or(symbol))
                .find(|function| {
                    !["alloc", "strdup", "strndup"]
                        .iter()
                        .any(|allocator| function.contains(allocator))
                })
        })
        .map(|function| Finding {
            kind: Kind::Leak,
            signature: format!("leak:{function}"),
        })
        .collect()
}

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

fn command(program: &Program, op: usize) -> String {
    program
        .ops
        .get(op)
        .and_then(|op| lower(&op.kind).into_iter().next())
        .and_then(|argv| argv.into_iter().next())
        .map_or_else(
            || "end".to_owned(),
            |name| String::from_utf8_lossy(name.as_slice()).to_uppercase(),
        )
}

fn basename(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::program::{Bytes, FORMAT_VERSION};
    use crate::target::Status;

    #[test]
    fn signatures_come_from_the_report_in_the_log() {
        let program = Program {
            version: FORMAT_VERSION,
            module_args: Vec::new(),
            ops: Vec::new(),
        };
        let cases = [
            (
                Exit::Clean,
                "SUMMARY: AddressSanitizer: heap-use-after-free /src/spec.c:123:7 in IndexSpec_Free\n\
                 ==> /src/util/workers.c:180 'x' is not true\n",
                "asan:heap-use-after-free:IndexSpec_Free:spec.c:123",
            ),
            (
                Exit::Died { at_op: 0 },
                "SUMMARY: AddressSanitizer: heap-buffer-overflow (redisearch.so:arm64+0x2f5564) in QueryNode_CheckIsValid+0x1374\n",
                "asan:heap-buffer-overflow:QueryNode_CheckIsValid:redisearch.so",
            ),
            (
                Exit::Died { at_op: 0 },
                "Direct leak of 16 byte(s) in 1 object(s) allocated from:\n\
                 \x20   #0 0x10 in malloc+0x9c (libclang_rt.asan_osx_dynamic.dylib:arm64e+0x)\n\
                 \x20   #1 0x20 in rm_malloc rmalloc.h:20\n\
                 \x20   #2 0x30 in NewThing thing.c:5\n\n\
                 SUMMARY: AddressSanitizer: 16 byte(s) leaked in 1 allocation(s).\n",
                "leak:NewThing",
            ),
            (
                Exit::Died { at_op: 0 },
                "=== ASSERTION FAILED ===\n==> /src/tag_index.c:337 'x' is not true\n",
                "assert:tag_index.c:337",
            ),
            (
                Exit::Died { at_op: 0 },
                "# Guru Meditation: DEBUG PANIC called at Unix time 1794912964 #debug.c:698\n",
                "panic:debug.c:698",
            ),
            (
                Exit::Died { at_op: 0 },
                "------ STACK TRACE ------\n\
                 0   libsystem_platform.dylib 0x1 _sigtramp + 56\n\
                 1   redis-server             0x2 call + 12\n\
                 2   redisearch.so            0x3 TagIndex_Open + 12\n\
                 ------ INFO OUTPUT ------\n\
                 3   redisearch.so            0x4 NotAFrame + 1\n",
                "crash:redisearch.so:TagIndex_Open",
            ),
            (
                Exit::Hung { at_op: 0 },
                "------ STACK TRACE ------\n\
                 0   redis-server             0x1 debugCommand + 1280\n\
                 Backtrace:\n\
                 0   libsystem_platform.dylib 0x1 _sigtramp + 56\n\
                 1   libsystem_c.dylib        0x2 nanosleep + 220\n\
                 2   redis-server             0x3 debugCommand + 900\n",
                "hang:redis-server:debugCommand",
            ),
            (
                Exit::Died { at_op: 0 },
                "------ STACK TRACE ------\n\
                 /lib/x86_64-linux-gnu/libc.so.6(+0x42520)[0x7f]\n\
                 /bin/redisearch.so(IndexSpec_Free+0x1c)[0x7f]\n",
                "crash:redisearch.so:IndexSpec_Free",
            ),
        ];
        for (exit, log, expected) in cases {
            let trace = Trace {
                steps: Vec::new(),
                exit,
                status: Status {
                    code: None,
                    signal: Some(6),
                },
                log: Bytes::from(log),
                coverage: Default::default(),
            };
            let signatures: Vec<String> = judge(&program, &trace)
                .into_iter()
                .map(|f| f.signature)
                .collect();
            assert_eq!(signatures, [expected], "{log}");
        }
    }
}
