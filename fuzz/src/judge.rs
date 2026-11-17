/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use serde::{Deserialize, Serialize};

use crate::program::Program;
use crate::run::{Exit, Reply, Trace};

const MODULE_BINARY: &str = "redisearch.so";
const SERVER_BINARY: &str = "redis-server";
const ALLOCATOR_FRAMES: &[&str] = &[
    "malloc",
    "calloc",
    "realloc",
    "free",
    "strdup",
    "strndup",
    "posix_memalign",
    "zmalloc",
    "zcalloc",
    "zrealloc",
    "ztrymalloc",
    "zstrdup",
    "RM_Alloc",
    "RM_Calloc",
    "RM_Realloc",
    "RM_Strdup",
    "rm_malloc",
    "rm_calloc",
    "rm_realloc",
    "rm_strdup",
    "rm_strndup",
];

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    Sanitizer,
    Assertion,
    Crash,
    Hang,
    Leak,
    Protocol,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Finding {
    pub kind: Kind,
    pub signature: String,
}

pub fn judge(program: &Program, trace: &Trace) -> Vec<Finding> {
    let log = String::from_utf8_lossy(trace.log.as_slice());
    let mut findings = Vec::new();

    findings.extend(sanitizer_errors(&log));
    findings.extend(leaks(&log));
    findings.extend(assertions(&log));

    let report = crash_report(&log);
    match &trace.exit {
        Exit::Hung { at_op } => findings.push(Finding {
            kind: Kind::Hang,
            signature: match &report {
                Some(frame) => format!("hang:{frame}"),
                None => format!("hang:{}", command_name(program, *at_op)),
            },
        }),
        Exit::Died { at_op } if findings.is_empty() => findings.push(Finding {
            kind: Kind::Crash,
            signature: match (panic_location(&log), &report) {
                (Some(location), _) => format!("panic:{location}"),
                (None, Some(frame)) => format!("crash:{frame}"),
                (None, None) => format!(
                    "crash:{}:{}",
                    command_name(program, *at_op),
                    status_name(trace)
                ),
            },
        }),
        _ => {}
    }

    for step in &trace.steps {
        if let Reply::Invalid(_) = step.reply {
            findings.push(Finding {
                kind: Kind::Protocol,
                signature: format!("protocol:{}", command_name(program, step.op)),
            });
        }
    }

    findings.sort();
    findings.dedup();
    findings
}

fn sanitizer_errors(log: &str) -> Vec<Finding> {
    log.lines()
        .filter_map(|line| line.split("SUMMARY: AddressSanitizer: ").nth(1))
        .filter(|summary| !summary.contains(" leaked in "))
        .map(|summary| {
            let mut words = summary.split_whitespace();
            let bug = words.next().unwrap_or("unknown");
            let location = words.next().map(basename).unwrap_or_default();
            let function = summary
                .rsplit(" in ")
                .next()
                .filter(|_| summary.contains(" in "));
            Finding {
                kind: Kind::Sanitizer,
                signature: match function {
                    Some(function) => format!("asan:{bug}:{function}:{}", strip_column(&location)),
                    None => format!("asan:{bug}:{}", strip_column(&location)),
                },
            }
        })
        .collect()
}

fn leaks(log: &str) -> Vec<Finding> {
    let mut findings = Vec::new();
    let mut in_leak = false;
    let mut found_frame = false;
    for line in log.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("Direct leak of") || trimmed.starts_with("Indirect leak of") {
            in_leak = true;
            found_frame = false;
            continue;
        }
        if !in_leak || found_frame {
            continue;
        }
        if trimmed.is_empty() {
            in_leak = false;
            continue;
        }
        if let Some(function) = sanitizer_frame_function(trimmed)
            && !ALLOCATOR_FRAMES.contains(&function)
        {
            found_frame = true;
            findings.push(Finding {
                kind: Kind::Leak,
                signature: format!("leak:{function}"),
            });
        }
    }
    findings
}

fn sanitizer_frame_function(line: &str) -> Option<&str> {
    if !line.starts_with('#') {
        return None;
    }
    let symbol = line.split(" in ").nth(1)?.split_whitespace().next()?;
    symbol.split("+0x").next()
}

fn assertions(log: &str) -> Vec<Finding> {
    log.lines()
        .filter_map(|line| line.split("==> ").nth(1))
        .filter_map(|rest| rest.split_whitespace().next())
        .map(|location| Finding {
            kind: Kind::Assertion,
            signature: format!("assert:{}", basename(location)),
        })
        .collect()
}

fn panic_location(log: &str) -> Option<String> {
    let line = log
        .lines()
        .find(|line| line.contains("Guru Meditation: "))?;
    let location = line.rsplit('#').next()?.trim();
    Some(basename(location))
}

fn crash_report(log: &str) -> Option<String> {
    let start = log.find("------ STACK TRACE ------")?;
    let frames: Vec<(String, String)> = log[start..]
        .lines()
        .skip(1)
        .take_while(|line| !line.starts_with("------ ") || line.contains("STACK TRACE"))
        .filter_map(stack_frame)
        .collect();
    let after_handler: Vec<&(String, String)> = frames
        .iter()
        .skip_while(|(_, symbol)| !symbol.contains("sigtramp") && !symbol.contains("sig_handler"))
        .skip(1)
        .collect();
    frames
        .iter()
        .find(|(binary, _)| binary == MODULE_BINARY)
        .or_else(|| {
            after_handler
                .iter()
                .copied()
                .find(|(binary, _)| binary == SERVER_BINARY)
        })
        .or_else(|| after_handler.first().copied())
        .or_else(|| frames.first())
        .map(|(binary, symbol)| format!("{binary}:{symbol}"))
}

fn stack_frame(line: &str) -> Option<(String, String)> {
    let mut words = line.split_whitespace();
    let index = words.next()?;
    if index.parse::<u32>().is_ok() {
        let binary = words.next()?;
        let address = words.next()?;
        if address.starts_with("0x") {
            let symbol = words.next()?;
            return Some((basename(binary), symbol.to_owned()));
        }
        return None;
    }
    let (path, rest) = line.trim().split_once('(')?;
    let symbol = rest.split(['+', ')']).next()?;
    if symbol.is_empty() || !path.contains('/') {
        return None;
    }
    let binary = basename(path.split_whitespace().next()?);
    Some((binary, symbol.to_owned()))
}

fn command_name(program: &Program, op: usize) -> String {
    program
        .ops
        .get(op)
        .and_then(|op| crate::lower::lower(&op.kind).into_iter().next())
        .and_then(|argv| argv.into_iter().next())
        .map(|name| String::from_utf8_lossy(name.as_slice()).to_uppercase())
        .unwrap_or_else(|| "end".to_owned())
}

fn status_name(trace: &Trace) -> String {
    match trace.status {
        Some(status) => match (status.signal, status.code) {
            (Some(signal), _) => format!("signal{signal}"),
            (None, Some(code)) => format!("exit{code}"),
            (None, None) => "unknown".to_owned(),
        },
        None => "unknown".to_owned(),
    }
}

fn basename(path: &str) -> String {
    path.rsplit('/').next().unwrap_or(path).to_owned()
}

fn strip_column(location: &str) -> String {
    let parts: Vec<&str> = location.split(':').collect();
    match parts.as_slice() {
        [file, line, column] if column.parse::<u32>().is_ok() => format!("{file}:{line}"),
        _ => location.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::program::{Bytes, FORMAT_VERSION, OpKind};
    use crate::run::Step;
    use crate::target::Status;

    fn program() -> Program {
        Program {
            version: FORMAT_VERSION,
            setup: Default::default(),
            ops: vec![
                OpKind::Raw {
                    argv: vec!["FT.SEARCH".into(), "idx".into(), "x".into()],
                }
                .into(),
            ],
        }
    }

    fn trace(exit: Exit, log: &str) -> Trace {
        Trace {
            steps: Vec::new(),
            exit,
            status: Some(Status {
                code: None,
                signal: Some(6),
            }),
            log: Bytes::from(log),
        }
    }

    fn signatures(trace: &Trace) -> Vec<String> {
        judge(&program(), trace)
            .into_iter()
            .map(|f| f.signature)
            .collect()
    }

    #[test]
    fn clean_run_has_no_findings() {
        let mut clean = trace(Exit::Clean, "* Ready to accept connections\n");
        clean.status = Some(Status {
            code: Some(0),
            signal: None,
        });
        assert!(judge(&program(), &clean).is_empty());
    }

    #[test]
    fn asan_summary_gives_bug_function_and_location() {
        let log = "==1==ERROR: AddressSanitizer: heap-use-after-free on address 0x1\n\
                   SUMMARY: AddressSanitizer: heap-use-after-free /src/RediSearch/src/spec.c:123:7 in IndexSpec_Free\n";
        assert_eq!(
            signatures(&trace(Exit::Died { at_op: 0 }, log)),
            ["asan:heap-use-after-free:IndexSpec_Free:spec.c:123"]
        );
    }

    #[test]
    fn asan_report_from_a_fork_child_counts_even_if_the_parent_survives() {
        let log = "SUMMARY: AddressSanitizer: SEGV /src/gc/fork_cycle.c:80 in child_main\n";
        let findings = judge(&program(), &trace(Exit::Clean, log));
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].kind, Kind::Sanitizer);
    }

    #[test]
    fn leak_signature_skips_allocator_frames() {
        let log = "==1==ERROR: LeakSanitizer: detected memory leaks\n\n\
                   Direct leak of 16 byte(s) in 1 object(s) allocated from:\n\
                   \x20   #0 0x10 in malloc+0x9c (libclang_rt.asan_osx_dynamic.dylib:arm64e+0x)\n\
                   \x20   #1 0x20 in rm_malloc rmalloc.h:20\n\
                   \x20   #2 0x30 in NewThing thing.c:5\n\
                   \x20   #3 0x40 in Caller caller.c:9\n\n\
                   SUMMARY: AddressSanitizer: 16 byte(s) leaked in 1 allocation(s).\n";
        assert_eq!(
            signatures(&trace(Exit::Died { at_op: 1 }, log)),
            ["leak:NewThing"]
        );
    }

    #[test]
    fn assertion_signature_is_file_and_line() {
        let log =
            "=== ASSERTION FAILED ===\n==> /src/RediSearch/src/tag_index.c:337 'x' is not true\n";
        assert_eq!(
            signatures(&trace(Exit::Died { at_op: 0 }, log)),
            ["assert:tag_index.c:337"]
        );
    }

    #[test]
    fn redis_crash_report_prefers_module_frames() {
        let log = "# Redis 255.255.255 crashed by signal: 11, si_code: 0\n\
                   ------ STACK TRACE ------\n\
                   0   libsystem_platform.dylib 0x00000001 _sigtramp + 56\n\
                   1   redisearch.so            0x00000002 TagIndex_Open + 12\n\
                   2   redis-server             0x00000003 call + 1020\n\
                   ------ INFO OUTPUT ------\n";
        assert_eq!(
            signatures(&trace(Exit::Died { at_op: 0 }, log)),
            ["crash:redisearch.so:TagIndex_Open"]
        );
    }

    #[test]
    fn redis_crash_report_without_module_frames_uses_the_frame_after_the_handler() {
        let log = "------ STACK TRACE ------\n\
                   0   redis-server             0x1 debugCommand + 1280\n\
                   Backtrace:\n\
                   0   libsystem_platform.dylib 0x2 _sigtramp + 56\n\
                   1   redis-server             0x3 debugCommand + 1264\n\
                   2   redis-server             0x4 call + 1020\n";
        assert_eq!(
            signatures(&trace(Exit::Died { at_op: 0 }, log)),
            ["crash:redis-server:debugCommand"]
        );
    }

    #[test]
    fn hang_report_skips_system_frames() {
        let log = "------ STACK TRACE ------\n\
                   0   libsystem_platform.dylib 0x1 _sigtramp + 56\n\
                   1   libsystem_c.dylib        0x2 nanosleep + 220\n\
                   2   libsystem_c.dylib        0x3 usleep + 68\n\
                   3   redis-server             0x4 debugCommand + 900\n";
        assert_eq!(
            signatures(&trace(Exit::Hung { at_op: 0 }, log)),
            ["hang:redis-server:debugCommand"]
        );
    }

    #[test]
    fn linux_crash_frames_are_parsed() {
        let log = "------ STACK TRACE ------\n\
                   /lib/x86_64-linux-gnu/libc.so.6(+0x42520)[0x7f]\n\
                   /bin/redisearch.so(IndexSpec_Free+0x1c)[0x7f]\n";
        assert_eq!(
            signatures(&trace(Exit::Died { at_op: 0 }, log)),
            ["crash:redisearch.so:IndexSpec_Free"]
        );
    }

    #[test]
    fn panic_signature_drops_the_timestamp() {
        let log = "# Guru Meditation: DEBUG PANIC called at Unix time 1794912964 #debug.c:698\n";
        assert_eq!(
            signatures(&trace(Exit::Died { at_op: 0 }, log)),
            ["panic:debug.c:698"]
        );
    }

    #[test]
    fn death_without_a_report_names_the_command_and_status() {
        assert_eq!(
            signatures(&trace(Exit::Died { at_op: 0 }, "")),
            ["crash:FT.SEARCH:signal6"]
        );
    }

    #[test]
    fn hang_without_a_report_names_the_command() {
        assert_eq!(
            signatures(&trace(Exit::Hung { at_op: 0 }, "")),
            ["hang:FT.SEARCH"]
        );
    }

    #[test]
    fn stalls_are_not_findings() {
        let mut stalled = trace(Exit::Stalled { at_op: 0 }, "");
        stalled.status = Some(Status {
            code: Some(0),
            signal: None,
        });
        assert!(judge(&program(), &stalled).is_empty());
    }

    #[test]
    fn invalid_replies_are_protocol_findings() {
        let mut broken = trace(Exit::Clean, "");
        broken.steps.push(Step {
            op: 0,
            conn: Default::default(),
            argv: vec!["FT.SEARCH".into()],
            sent: 1,
            received: Some(2),
            reply: Reply::Invalid("bad".into()),
            micros: 1,
        });
        assert_eq!(signatures(&broken), ["protocol:FT.SEARCH"]);
    }
}
