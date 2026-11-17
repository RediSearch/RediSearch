/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Needs the binaries from `fuzz/setup.sh`; run with
//! `source bin/fuzz/env.sh && cargo test -- --ignored`.

use std::collections::BTreeSet;
use std::path::Path;
use std::time::Duration;

use rand::SeedableRng;
use rand::rngs::SmallRng;
use redisearch_fuzz::campaign::{Campaign, fuzz};
use redisearch_fuzz::generate::generate;
use redisearch_fuzz::judge::{Kind, execute};
use redisearch_fuzz::program::{Op, OpKind, Program, argv};
use redisearch_fuzz::resp::Value;
use redisearch_fuzz::run::{Exit, Trace};
use redisearch_fuzz::shrink::reduce;
use redisearch_fuzz::target::{Env, Reply};

const GENERATED: usize = 50;
const CAMPAIGN_SECS: u64 = 60;

fn load(path: &str) -> Program {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(path);
    Program::from_json(&std::fs::read_to_string(path).unwrap()).unwrap()
}

fn env() -> Env {
    Env::from_env().expect("source bin/fuzz/env.sh first")
}

/// The module with the bugs from `planted/planted.patch`.
fn planted() -> Env {
    Env {
        module: std::env::var_os("REDISEARCH_PLANTED_MODULE")
            .expect("rerun fuzz/setup.sh")
            .into(),
        ..env()
    }
}

fn kinds(program: &Program, env: &Env) -> (Trace, Vec<Kind>) {
    let (trace, findings) = execute(program, env).unwrap();
    (trace, findings.into_iter().map(|f| f.kind).collect())
}

fn succeeded(reply: &Option<Reply>) -> bool {
    matches!(reply, Some(Reply::Value(v)) if !matches!(v, Value::Error(_)))
}

#[test]
#[ignore = "needs bin/fuzz/env.sh"]
fn every_seed_succeeds_and_reaches_edges_no_other_seed_does() {
    let seeds: Vec<(String, BTreeSet<u32>)> =
        std::fs::read_dir(Path::new(env!("CARGO_MANIFEST_DIR")).join("seeds"))
            .unwrap()
            .map(|entry| {
                let name = format!("seeds/{}", entry.unwrap().file_name().to_string_lossy());
                let (trace, kinds) = kinds(&load(&name), &env());
                assert_eq!((trace.exit, kinds), (Exit::Clean, vec![]), "{name}");
                for step in &trace.steps {
                    assert!(succeeded(&step.reply), "{name}: {step:?}");
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
        assert!(!edges.is_subset(&others), "{name} adds no edges");
    }
}

#[test]
#[ignore = "needs bin/fuzz/env.sh"]
fn a_command_that_never_returns_is_a_hang() {
    let (trace, kinds) = kinds(&load("tests/programs/hang.json"), &env());
    assert_eq!(
        (trace.exit, kinds),
        (Exit::Hung { at_op: 0 }, vec![Kind::Hang])
    );
}

#[test]
#[ignore = "needs bin/fuzz/env.sh"]
fn a_reply_the_program_held_back_is_released_and_the_program_carries_on() {
    let (trace, kinds) = kinds(&load("tests/programs/stall.json"), &env());
    assert_eq!((trace.exit, kinds), (Exit::Clean, vec![]));
    let [.., query, release, ping] = trace.steps.as_slice() else {
        panic!("{:?}", trace.steps);
    };
    assert_eq!(release.argv, argv(&["RELEASE"]));
    assert!(release.received < query.received && query.received < ping.received);
}

#[test]
#[ignore = "needs bin/fuzz/env.sh"]
fn a_barrier_that_cannot_complete_times_out_without_a_finding() {
    let (trace, kinds) = kinds(&load("tests/programs/barrier-blocked-scan.json"), &env());
    assert_eq!((trace.exit, kinds), (Exit::Clean, vec![]));
    assert_eq!(trace.steps.last().unwrap().reply, Some(Reply::Timeout));
}

#[test]
#[ignore = "needs bin/fuzz/env.sh"]
fn a_deferred_query_is_answered_after_another_connection_drops_its_index() {
    let (trace, kinds) = kinds(
        &load("tests/programs/drop-under-pending-query.json"),
        &env(),
    );
    assert_eq!((trace.exit, kinds), (Exit::Clean, vec![]));
    let [.., query, drop, _resume, _barrier] = trace.steps.as_slice() else {
        panic!("{:?}", trace.steps);
    };
    assert!(query.sent < drop.sent && drop.received < query.received);
}

#[test]
#[ignore = "needs bin/fuzz/env.sh"]
fn most_replies_to_generated_programs_are_not_errors() {
    let mut rng = SmallRng::seed_from_u64(0);
    let (mut replies, mut errors) = (0, 0);
    for _ in 0..GENERATED {
        let (trace, _) = kinds(&generate(&mut rng), &env());
        for step in &trace.steps {
            if ![argv(&["BARRIER"]), argv(&["RELEASE"])].contains(&step.argv) {
                replies += 1;
                errors += usize::from(!succeeded(&step.reply));
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
        reply: Default::default(),
        bind: None,
        kind: OpKind::Command {
            argv: argv(&["DEBUG", "SEGFAULT"]),
        },
    };
    let mut program = generate(&mut SmallRng::seed_from_u64(0));
    program.ops.insert(program.ops.len() / 2, segfault.clone());
    let (_, findings) = execute(&program, &env()).unwrap();
    assert_eq!(findings[0].kind, Kind::Crash);
    let reduced = reduce(&program, &findings[0], &env(), |_| {});
    assert!(reduced.module_args.is_empty());
    assert_eq!(reduced.ops, [segfault]);
}

#[test]
#[ignore = "needs bin/fuzz/env.sh"]
fn planted_bugs_give_a_real_sanitizer_report_and_a_real_assertion() {
    for (seed, kind) in [
        ("planted/overflow.json", Kind::Sanitizer),
        ("planted/assert.json", Kind::Assertion),
    ] {
        assert_eq!(kinds(&load(seed), &planted()).1, [kind], "{seed}");
    }
}

#[test]
#[ignore = "needs bin/fuzz/env.sh"]
fn a_short_campaign_from_the_seeds_finds_a_planted_bug() {
    let dir = tempfile::tempdir().unwrap();
    fuzz(Campaign {
        worker: 0,
        env: &planted(),
        seeds: &[Path::new(env!("CARGO_MANIFEST_DIR")).join("seeds")],
        corpus: &dir.path().join("corpus"),
        findings: &dir.path().join("findings"),
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
