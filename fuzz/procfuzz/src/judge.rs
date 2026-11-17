/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Findings any run can have: sanitizer reports, crashes, hangs, and
//! replies that break the protocol.

use crate::program::{Program, TypedOp};
use crate::run::{Exit, Trace};
use crate::server::{Reply, Server};

const ALLOCATORS: [&str; 3] = ["alloc", "strdup", "strndup"];

pub fn safety<S: Server, T: TypedOp>(
    server: &S,
    program: &Program<T>,
    trace: &Trace<S::Value>,
    log: &str,
) -> Vec<String> {
    // Printing a crash report can run code that trips further assertions,
    // so only the first report counts.
    let mut findings: Vec<String> = log
        .lines()
        .find_map(|line| error(line).or_else(|| server.failure(line)))
        .into_iter()
        .chain(leaks(log))
        .collect();
    let frame = server.crash_frame(log);
    match trace.exit {
        Exit::Hung { at_op } => findings.push(format!(
            "hang:{}",
            frame.unwrap_or_else(|| command(program, at_op))
        )),
        Exit::Died { at_op } if findings.is_empty() => findings.push(match frame {
            Some(frame) => format!("crash:{frame}"),
            None => format!(
                "crash:{}:signal{}:exit{}",
                command(program, at_op),
                trace.status.signal.unwrap_or(0),
                trace.status.code.unwrap_or(0)
            ),
        }),
        _ => {}
    }
    findings.extend(
        trace
            .steps
            .iter()
            .filter(|step| matches!(step.reply, Some(Reply::Invalid(_))))
            .map(|step| format!("protocol:{}", command(program, step.op))),
    );
    findings.sort();
    findings.dedup();
    findings
}

/// The name of the first command `op` sends, or `end` after the last op.
fn command<T: TypedOp>(program: &Program<T>, op: usize) -> String {
    program
        .ops
        .get(op)
        .and_then(|op| op.kind.lower().into_iter().next())
        .and_then(|argv| argv.into_iter().next())
        .map_or_else(
            || "end".to_owned(),
            |name| String::from_utf8_lossy(name.as_slice()).to_uppercase(),
        )
}

/// The signature of a line with an AddressSanitizer error summary.
/// `heap-use-after-free /src/spec.c:123:7 in IndexSpec_Free` becomes
/// `asan:heap-use-after-free:IndexSpec_Free:spec.c:123`. Without debug
/// information the location is a binary and an offset that changes with
/// every build, so only the binary's name is kept.
pub fn error(line: &str) -> Option<String> {
    let summary = line.split("SUMMARY: AddressSanitizer: ").nth(1)?;
    if summary.contains(" leaked in ") {
        return None;
    }
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
    Some(match summary.split_once(" in ") {
        Some((_, function)) => {
            let function = function.split("+0x").next().unwrap_or(function);
            format!("asan:{bug}:{function}:{location}")
        }
        None => format!("asan:{bug}:{location}"),
    })
}

/// A signature per leak, naming its first frame outside an allocator.
pub fn leaks(log: &str) -> Vec<String> {
    log.split("leak of ")
        .skip(1)
        .filter_map(|report| {
            report
                .lines()
                .skip(1)
                .take_while(|line| !line.trim().is_empty())
                .filter_map(|line| line.split(" in ").nth(1)?.split_whitespace().next())
                .map(|symbol| symbol.split("+0x").next().unwrap_or(symbol))
                .find(|function| !ALLOCATORS.iter().any(|a| function.contains(a)))
        })
        .map(|function| format!("leak:{function}"))
        .collect()
}

pub fn basename(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reports_become_signatures_without_addresses() {
        let errors = [
            (
                "SUMMARY: AddressSanitizer: heap-use-after-free /src/spec.c:123:7 in IndexSpec_Free",
                "asan:heap-use-after-free:IndexSpec_Free:spec.c:123",
            ),
            (
                "SUMMARY: AddressSanitizer: heap-buffer-overflow (redisearch.so:arm64+0x2f5564) in QueryNode_CheckIsValid+0x1374",
                "asan:heap-buffer-overflow:QueryNode_CheckIsValid:redisearch.so",
            ),
        ];
        for (line, signature) in errors {
            assert_eq!(error(line).unwrap(), signature);
        }
        let leak = "Direct leak of 16 byte(s) in 1 object(s) allocated from:\n\
                    \x20   #0 0x10 in malloc+0x9c (libclang_rt.asan_osx_dynamic.dylib:arm64e+0x)\n\
                    \x20   #1 0x20 in rm_malloc rmalloc.h:20\n\
                    \x20   #2 0x30 in NewThing thing.c:5\n\n\
                    SUMMARY: AddressSanitizer: 16 byte(s) leaked in 1 allocation(s).\n";
        assert_eq!(leaks(leak), ["leak:NewThing"]);
        assert_eq!(leak.lines().find_map(error), None);
    }
}
