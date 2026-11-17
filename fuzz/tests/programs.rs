/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Runs programs against a live server built by `fuzz/setup.sh`. Ignored by
//! default; run with `source bin/fuzz/env.sh && cargo test -- --ignored`.

use std::collections::BTreeSet;
use std::path::Path;

use redisearch_fuzz::judge::{Kind, judge};
use redisearch_fuzz::program::{Op, OpKind, Program, ReplyMode, argv};
use redisearch_fuzz::resp::Value;
use redisearch_fuzz::run::{Exit, Trace, run};
use redisearch_fuzz::target::{Env, Reply};

fn load(path: &str) -> Program {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(path);
    Program::from_json(&std::fs::read_to_string(path).unwrap()).unwrap()
}

fn execute(program: &Program) -> (Trace, Vec<Kind>) {
    let env = Env::from_env().expect("source bin/fuzz/env.sh first");
    let trace = run(program, &env).unwrap();
    let kinds = judge(program, &trace).into_iter().map(|f| f.kind).collect();
    (trace, kinds)
}

#[test]
#[ignore = "needs bin/fuzz/env.sh"]
fn every_smoke_command_succeeds() {
    let (trace, kinds) = execute(&load("seeds/smoke.json"));
    assert_eq!(trace.exit, Exit::Clean);
    assert!(kinds.is_empty());
    assert!(trace.steps.iter().all(|step| matches!(
        &step.reply,
        Some(Reply::Value(value)) if !matches!(value, Value::Error(_))
    )));
}

#[test]
#[ignore = "needs bin/fuzz/env.sh"]
fn a_segfault_is_a_crash() {
    let (trace, kinds) = execute(&load("tests/programs/segfault.json"));
    assert_eq!(trace.exit, Exit::Died { at_op: 0 });
    assert_eq!(kinds, [Kind::Crash]);
}

#[test]
#[ignore = "needs bin/fuzz/env.sh"]
fn a_command_that_never_returns_is_a_hang() {
    let (trace, kinds) = execute(&load("tests/programs/hang.json"));
    assert_eq!(trace.exit, Exit::Hung { at_op: 0 });
    assert_eq!(kinds, [Kind::Hang]);
}

#[test]
#[ignore = "needs bin/fuzz/env.sh"]
fn a_reply_the_program_held_back_is_a_stall_not_a_finding() {
    let (trace, kinds) = execute(&load("tests/programs/stall.json"));
    assert_eq!(trace.exit, Exit::Stalled { at_op: 3 });
    assert!(kinds.is_empty());
}

#[test]
#[ignore = "needs bin/fuzz/env.sh"]
fn a_barrier_that_cannot_complete_times_out_without_a_finding() {
    let (trace, kinds) = execute(&load("tests/programs/barrier-blocked-scan.json"));
    assert_eq!(trace.exit, Exit::Clean);
    assert!(kinds.is_empty());
    assert_eq!(trace.steps.last().unwrap().reply, Some(Reply::Timeout));
}

#[test]
#[ignore = "needs bin/fuzz/env.sh"]
fn a_deferred_query_is_answered_after_another_connection_drops_its_index() {
    let (trace, kinds) = execute(&load("tests/programs/drop-under-pending-query.json"));
    assert_eq!(trace.exit, Exit::Clean);
    assert!(kinds.is_empty());
    let [.., query, drop, _resume, _barrier] = trace.steps.as_slice() else {
        panic!("{:?}", trace.steps);
    };
    assert!(query.sent < drop.sent && drop.received < query.received);
}

#[test]
#[ignore = "needs bin/fuzz/env.sh"]
fn a_forced_gc_reaches_edges_the_same_writes_alone_do_not() {
    let edges = |program: &Program| -> BTreeSet<u32> {
        let (trace, _) = execute(program);
        assert!(
            trace.coverage.guards > 0,
            "build the module with FUZZ_COV=1"
        );
        trace.coverage.edges.into_iter().collect()
    };
    let unforced = load("tests/programs/gc.json");
    let mut forced = unforced.clone();
    let barrier = forced
        .ops
        .iter()
        .position(|op| op.kind == OpKind::Barrier)
        .unwrap();
    forced.ops.insert(
        barrier,
        Op {
            conn: Default::default(),
            reply: ReplyMode::Await,
            kind: OpKind::Command {
                argv: argv(&["_FT.DEBUG", "GC_FORCEINVOKE", "i1"]),
            },
        },
    );
    assert!(edges(&forced).difference(&edges(&unforced)).count() > 0);
}
