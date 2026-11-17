/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Runs each program under `tests/programs/` against a live server built by
//! `fuzz/setup.sh`. Ignored by default; run with
//! `source bin/fuzz/env.sh && cargo test -- --ignored`.

use std::path::Path;

use redisearch_fuzz::judge::{Kind, judge};
use redisearch_fuzz::program::Program;
use redisearch_fuzz::run::{Exit, Reply, run};
use redisearch_fuzz::target::Env;

#[derive(Debug, PartialEq)]
enum Expected {
    Clean,
    Died,
    Hung,
    Stalled,
}

fn check(
    name: &str,
    expected_exit: Expected,
    expected_findings: &[Kind],
) -> redisearch_fuzz::run::Trace {
    let env = Env::from_env().expect("source bin/fuzz/env.sh first");
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/programs")
        .join(format!("{name}.json"));
    let program = Program::from_json(&std::fs::read_to_string(&path).unwrap()).unwrap();
    let trace = run(&program, &env);
    let exit = match trace.exit {
        Exit::Clean => Expected::Clean,
        Exit::Died { .. } => Expected::Died,
        Exit::Hung { .. } => Expected::Hung,
        Exit::Stalled { .. } => Expected::Stalled,
        Exit::StartupFailed { ref reason } => panic!("{name}: server failed to start:\n{reason}"),
    };
    assert_eq!(exit, expected_exit, "{name}: {:?}", trace.exit);
    let kinds: Vec<Kind> = judge(&program, &trace)
        .into_iter()
        .map(|f| f.kind)
        .collect();
    assert_eq!(kinds, expected_findings, "{name}");
    trace
}

#[test]
#[ignore = "needs bin/fuzz/env.sh"]
fn smoke_seed_runs_clean() {
    let env = Env::from_env().expect("source bin/fuzz/env.sh first");
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("seeds/smoke.json");
    let program = Program::from_json(&std::fs::read_to_string(path).unwrap()).unwrap();
    let trace = run(&program, &env);
    assert_eq!(trace.exit, Exit::Clean);
    assert!(judge(&program, &trace).is_empty());
    assert!(
        trace
            .steps
            .iter()
            .all(|s| matches!(s.reply, Reply::Value(ref v) if !matches!(v, redisearch_fuzz::resp::Value::Error(_)))),
        "every smoke command succeeds"
    );
}

#[test]
#[ignore = "needs bin/fuzz/env.sh"]
fn redis_panic_is_a_crash() {
    check("panic", Expected::Died, &[Kind::Crash]);
}

#[test]
#[ignore = "needs bin/fuzz/env.sh"]
fn segfault_is_a_crash() {
    check("segfault", Expected::Died, &[Kind::Crash]);
}

#[test]
#[ignore = "needs bin/fuzz/env.sh"]
fn a_command_that_never_returns_is_a_hang() {
    check("hang", Expected::Hung, &[Kind::Hang]);
}

#[test]
#[ignore = "needs bin/fuzz/env.sh"]
fn a_query_held_by_the_program_is_a_stall_not_a_finding() {
    check("stall", Expected::Stalled, &[]);
    check("stall-deferred", Expected::Stalled, &[]);
}

#[test]
#[ignore = "needs bin/fuzz/env.sh"]
fn a_barrier_that_cannot_complete_is_inconclusive_not_a_finding() {
    let trace = check("barrier-blocked-scan", Expected::Clean, &[]);
    assert!(trace.steps.iter().any(|s| s.reply == Reply::Inconclusive));
}

#[test]
#[ignore = "needs bin/fuzz/env.sh"]
fn a_pending_query_survives_its_index_being_dropped() {
    let trace = check("drop-under-pending-query", Expected::Clean, &[]);
    let query = trace
        .steps
        .iter()
        .find(|s| s.conn.0 == 1)
        .expect("the deferred query ran");
    let drop = trace
        .steps
        .iter()
        .find(|s| {
            s.argv
                .first()
                .is_some_and(|a| a.as_slice() == b"FT.DROPINDEX")
        })
        .expect("the drop ran");
    assert!(query.sent < drop.sent && drop.received < query.received);
}
