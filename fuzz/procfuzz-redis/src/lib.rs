/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! `redis-server` with modules loaded, as a [`procfuzz::service::Service`]
//! speaking [RESP2](resp).

pub mod line;
pub mod resp;

use std::path::{Path, PathBuf};
use std::process::Command;

use procfuzz::sanitizer::{self, basename};
use procfuzz::service::{Service, Timing};
use procfuzz::system::Finding;

use crate::resp::{Resp2, argv};

const STACK_TRACE: &str = "------ STACK TRACE ------";

/// `redis-server` at `server` with `modules` loaded in order, started with
/// the variables in `env`. It never pauses and is always idle. A crash is
/// blamed on the first module's frames first. Failures are sanitizer
/// errors, Redis assertions, and Redis panics.
pub fn service(
    server: PathBuf,
    modules: Vec<PathBuf>,
    env: Vec<(String, String)>,
) -> Service<Resp2> {
    let blamed = modules
        .first()
        .and_then(|module| module.file_name())
        .map(|name| name.to_string_lossy().into_owned());
    let launch = move |dir: &Path, socket: &Path| {
        let mut command = Command::new(&server);
        command
            .envs(env.iter().map(|(name, value)| (name, value)))
            .args(["--port", "0", "--save", "", "--appendonly", "no"])
            .args(["--enable-debug-command", "yes", "--unixsocket"])
            .arg(socket)
            .arg("--dir")
            .arg(dir);
        for module in &modules {
            command.arg("--loadmodule").arg(module);
        }
        command
    };
    Service {
        codec: Resp2,
        launch: Box::new(launch),
        ready: argv(&["PING"]),
        shutdown: argv(&["SHUTDOWN", "NOSAVE"]),
        heal: Box::new(|_| ()),
        idle: Box::new(|_| true),
        findings: Box::new(|log| sanitizer::findings(log, failure)),
        site: Box::new(move |log| crash_site(log, blamed.as_deref())),
        timing: Timing::default(),
    }
}

/// A Redis assertion, `==> /src/tag_index.c:337 'x' is not true`, or a
/// panic, whose message ends with its source location after `#`.
pub fn failure(line: &str) -> Option<Finding> {
    if let Some(location) = line
        .split("==> ")
        .nth(1)
        .and_then(|rest| rest.split_whitespace().next())
    {
        return Some(Finding {
            kind: "assert",
            key: basename(location).to_owned(),
        });
    }
    let (_, message) = line.split_once("Guru Meditation: ")?;
    Some(Finding {
        kind: "panic",
        key: basename(message.rsplit('#').next()?.trim()).to_owned(),
    })
}

/// The frame a Redis crash report blames: the first frame in `blamed`, else
/// the first `redis-server` frame after the signal handler.
pub fn crash_site(log: &str, blamed: Option<&str>) -> Option<String> {
    let report = &log[log.find(STACK_TRACE)?..];
    let frames: Vec<(&str, &str)> = report
        .lines()
        .skip(1)
        .take_while(|line| !line.starts_with("------ "))
        .filter_map(stack_frame)
        .collect();
    let handler = frames
        .iter()
        .position(|(_, symbol)| symbol.contains("sigtramp") || symbol.contains("sig_handler"))
        .map_or(0, |at| at + 1);
    frames
        .iter()
        .find(|(binary, _)| Some(*binary) == blamed)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crash_reports_become_findings() {
        let failure = |line| failure(line).map(|finding| finding.to_string());
        assert_eq!(
            failure("==> /src/tag_index.c:337 'x' is not true").as_deref(),
            Some("assert:tag_index.c:337")
        );
        assert_eq!(
            failure("# Guru Meditation: DEBUG PANIC called at Unix time 1 #debug.c:698").as_deref(),
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
            assert_eq!(
                crash_site(log, Some("redisearch.so")).as_deref(),
                Some(frame),
                "{log}"
            );
        }
    }
}
