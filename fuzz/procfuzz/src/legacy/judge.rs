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

use crate::legacy::program::{Program, TypedOp};
use crate::legacy::run::{Exit, Trace};
use crate::legacy::server::{Reply, Server};
use crate::sanitizer;

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
        .find_map(|line| {
            sanitizer::error(line)
                .map(|finding| finding.to_string())
                .or_else(|| server.failure(line))
        })
        .into_iter()
        .chain(sanitizer::leaks(log).iter().map(ToString::to_string))
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
