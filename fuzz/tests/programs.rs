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
use std::time::Duration;

use rand::SeedableRng;
use rand::rngs::SmallRng;
use redisearch_fuzz::campaign::{Campaign, fuzz};
use redisearch_fuzz::edit::generate;
use redisearch_fuzz::judge::{Kind, judge};
use redisearch_fuzz::program::{Op, OpKind, Program, ReplyMode, argv};
use redisearch_fuzz::reduce::reduce;
use redisearch_fuzz::resp::Value;
use redisearch_fuzz::run::{Exit, Trace, run};
use redisearch_fuzz::target::{Env, Reply};

const GENERATED: usize = 50;
const CAMPAIGN_SECS: u64 = 60;

fn load(path: &str) -> Program {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(path);
    Program::from_json(&std::fs::read_to_string(path).unwrap()).unwrap()
}

fn planted() -> Env {
    Env {
        module: std::env::var_os("REDISEARCH_PLANTED_MODULE")
            .expect("rerun fuzz/setup.sh")
            .into(),
        ..env()
    }
}

fn env() -> Env {
    Env::from_env().expect("source bin/fuzz/env.sh first")
}

fn execute(program: &Program) -> (Trace, Vec<Kind>) {
    let trace = run(program, &env()).unwrap();
    let kinds = judge(program, &trace).into_iter().map(|f| f.kind).collect();
    (trace, kinds)
}

#[test]
#[ignore = "needs bin/fuzz/env.sh"]
fn every_seed_succeeds_and_reaches_edges_no_other_seed_does() {
    let seeds: Vec<(String, BTreeSet<u32>)> = std::fs::read_dir(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("seeds"),
    )
    .unwrap()
    .map(|entry| {
        let name = format!("seeds/{}", entry.unwrap().file_name().to_string_lossy());
        let (trace, kinds) = execute(&load(&name));
        assert_eq!(trace.exit, Exit::Clean, "{name}");
        assert!(kinds.is_empty(), "{name}: {kinds:?}");
        for step in &trace.steps {
            assert!(
                matches!(&step.reply, Some(Reply::Value(v)) if !matches!(v, Value::Error(_))),
                "{name}: {step:?}"
            );
        }
        (name, trace.coverage.edges.into_iter().collect())
    })
    .collect();
    for (name, edges) in &seeds {
        let others: BTreeSet<u32> = seeds
            .iter()
            .filter(|(other, _)| other != name)
            .flat_map(|(_, edges)| edges.iter().copied())
            .collect();
        assert!(
            edges.difference(&others).count() > 0,
            "{name} adds no edges"
        );
    }
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
fn a_reply_the_program_held_back_is_released_and_the_program_carries_on() {
    let (trace, kinds) = execute(&load("tests/programs/stall.json"));
    assert_eq!(trace.exit, Exit::Clean);
    assert!(kinds.is_empty());
    let [.., query, release, ping] = trace.steps.as_slice() else {
        panic!("{:?}", trace.steps);
    };
    assert_eq!(release.argv, argv(&["RELEASE"]));
    assert!(release.received < query.received && query.received < ping.received);
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

#[test]
#[ignore = "needs bin/fuzz/env.sh"]
fn most_replies_to_generated_programs_are_not_errors() {
    let mut rng = SmallRng::seed_from_u64(0);
    let (mut replies, mut errors) = (0, 0);
    for _ in 0..GENERATED {
        let (trace, _) = execute(&generate(&mut rng));
        for step in &trace.steps {
            if [argv(&["BARRIER"]), argv(&["RELEASE"])].contains(&step.argv) {
                continue;
            }
            replies += 1;
            if !matches!(&step.reply, Some(Reply::Value(v)) if !matches!(v, Value::Error(_))) {
                errors += 1;
            }
        }
    }
    assert!(
        errors * 5 <= replies,
        "{errors} of {replies} replies are errors"
    );
}

#[test]
#[ignore = "needs bin/fuzz/env.sh"]
fn a_crash_inside_a_generated_program_reduces_to_the_crashing_command() {
    let segfault = Op {
        conn: Default::default(),
        reply: ReplyMode::Await,
        kind: OpKind::Command {
            argv: argv(&["DEBUG", "SEGFAULT"]),
        },
    };
    let mut program = generate(&mut SmallRng::seed_from_u64(0));
    program.ops.insert(program.ops.len() / 2, segfault.clone());
    let finding = judge(&program, &run(&program, &env()).unwrap()).remove(0);
    let reduced = reduce(&program, &finding, &env(), |_| {});
    assert!(reduced.module_args.is_empty());
    assert_eq!(reduced.ops, [segfault]);
}

#[test]
#[ignore = "needs bin/fuzz/env.sh"]
fn planted_bugs_give_a_real_sanitizer_report_and_a_real_assertion() {
    let env = planted();
    for (seed, kind) in [
        ("planted/overflow.json", Kind::Sanitizer),
        ("planted/assert.json", Kind::Assertion),
    ] {
        let program = load(seed);
        let kinds: Vec<Kind> = judge(&program, &run(&program, &env).unwrap())
            .into_iter()
            .map(|f| f.kind)
            .collect();
        assert_eq!(kinds, [kind], "{seed}");
    }
}

#[test]
#[ignore = "needs bin/fuzz/env.sh"]
fn a_short_campaign_from_the_seeds_finds_a_planted_bug() {
    let dir = tempfile::tempdir().unwrap();
    fuzz(Campaign {
        worker: 0,
        env: planted(),
        seeds: &[Path::new(env!("CARGO_MANIFEST_DIR")).join("seeds")],
        corpus: &dir.path().join("corpus"),
        findings: &dir.path().join("findings"),
        known: None,
        duration: Some(Duration::from_secs(CAMPAIGN_SECS)),
    })
    .unwrap();
    let found: Vec<_> = std::fs::read_dir(dir.path().join("findings"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    assert!(
        found
            .iter()
            .any(|name| name.contains("QueryNode_CheckIsValid") || name.contains("module.c")),
        "{found:?}"
    );
}
