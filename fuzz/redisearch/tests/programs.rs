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
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use procfuzz::campaign::{Campaign, fuzz};
use procfuzz::edit::reduce;
use procfuzz::program::{Kind, argv};
use procfuzz::run::Exit;
use procfuzz::server::Reply;
use rand::SeedableRng;
use rand::rngs::SmallRng;
use redisearch_fuzz::generate::generate;
use redisearch_fuzz::redis::{Env, Value};
use redisearch_fuzz::{Execution, Op, Program, execute, mutate};

const GENERATED: usize = 50;
const CAMPAIGN_SECS: u64 = 60;

fn load(path: &str) -> Program {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(path);
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

fn redisearch() -> Env {
    Env::from_env().expect("source bin/fuzz/env.sh first")
}

/// The module with the bugs from `planted/planted.patch`.
fn planted() -> Env {
    Env {
        module: std::env::var_os("REDISEARCH_PLANTED_MODULE")
            .expect("rerun fuzz/setup.sh")
            .into(),
        ..redisearch()
    }
}

/// The run and the kinds of its findings.
fn kinds(program: &Program, env: &Env) -> (Execution, Vec<String>) {
    let execution = execute(env, program).unwrap();
    let kinds = execution
        .findings
        .iter()
        .map(|f| f.split(':').next().unwrap().to_owned())
        .collect();
    (execution, kinds)
}

fn succeeded(reply: &Option<Reply<Value>>) -> bool {
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
                let (execution, kinds) = kinds(&load(&name), &redisearch());
                assert_eq!(
                    (execution.trace.exit, kinds),
                    (Exit::Clean, vec![]),
                    "{name}"
                );
                for step in &execution.trace.steps {
                    let barrier = step.is_harness() && step.reply == Some(Reply::Done);
                    assert!(barrier || succeeded(&step.reply), "{name}: {step:?}");
                }
                (name, execution.coverage.edges.into_iter().collect())
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
    let (execution, kinds) = kinds(&load("tests/programs/hang.json"), &redisearch());
    assert_eq!(
        (execution.trace.exit, kinds),
        (Exit::Hung { at_op: 0 }, vec!["hang".to_owned()])
    );
}

#[test]
#[ignore = "needs bin/fuzz/env.sh"]
fn a_reply_the_program_held_back_is_released_and_the_program_carries_on() {
    let (execution, kinds) = kinds(&load("tests/programs/stall.json"), &redisearch());
    assert_eq!((&execution.trace.exit, kinds), (&Exit::Clean, vec![]));
    let [.., query, release, ping] = execution.trace.steps.as_slice() else {
        panic!("{:?}", execution.trace.steps);
    };
    assert_eq!(release.argv, argv(&["RELEASE"]));
    assert!(release.received < query.received && query.received < ping.received);
}

#[test]
#[ignore = "needs bin/fuzz/env.sh"]
fn a_barrier_that_cannot_complete_times_out_without_a_finding() {
    let (execution, kinds) = kinds(
        &load("tests/programs/barrier-blocked-scan.json"),
        &redisearch(),
    );
    assert_eq!((&execution.trace.exit, kinds), (&Exit::Clean, vec![]));
    assert_eq!(
        execution.trace.steps.last().unwrap().reply,
        Some(Reply::Timeout)
    );
}

#[test]
#[ignore = "needs bin/fuzz/env.sh"]
fn a_deferred_query_is_answered_after_another_connection_drops_its_index() {
    let (execution, kinds) = kinds(
        &load("tests/programs/drop-under-pending-query.json"),
        &redisearch(),
    );
    assert_eq!((&execution.trace.exit, kinds), (&Exit::Clean, vec![]));
    let [.., query, drop, _resume, _barrier] = execution.trace.steps.as_slice() else {
        panic!("{:?}", execution.trace.steps);
    };
    assert!(query.sent < drop.sent && drop.received < query.received);
}

#[test]
#[ignore = "needs bin/fuzz/env.sh"]
fn most_replies_to_generated_programs_are_not_errors() {
    let env = redisearch();
    let mut rng = SmallRng::seed_from_u64(0);
    let (mut replies, mut errors) = (0, 0);
    for _ in 0..GENERATED {
        let (execution, _) = kinds(&generate(&mut rng), &env);
        for step in &execution.trace.steps {
            if !step.is_harness() {
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
    let env = redisearch();
    let segfault = Op {
        conn: Default::default(),
        reply: Default::default(),
        bind: None,
        kind: Kind::Command(argv(&["DEBUG", "SEGFAULT"])),
    };
    let mut program = generate(&mut SmallRng::seed_from_u64(0));
    program.ops.insert(program.ops.len() / 2, segfault.clone());
    let finding = execute(&env, &program).unwrap().findings.remove(0);
    assert!(finding.starts_with("crash:"), "{finding}");
    let reduced = reduce(&program, |candidate| {
        execute(&env, candidate).is_ok_and(|e| e.findings.contains(&finding))
    });
    assert!(reduced.args.is_empty());
    assert_eq!(reduced.ops, [segfault]);
}

#[test]
#[ignore = "needs bin/fuzz/env.sh"]
fn planted_bugs_give_a_real_sanitizer_report_and_a_real_assertion() {
    for (program, kind) in [
        ("planted/overflow.json", "asan"),
        ("planted/assert.json", "assert"),
    ] {
        assert_eq!(kinds(&load(program), &planted()).1, [kind], "{program}");
    }
}

#[test]
#[ignore = "needs bin/fuzz/env.sh"]
fn a_short_campaign_from_the_seeds_finds_a_planted_bug() {
    let dir = tempfile::tempdir().unwrap();
    let env = planted();
    let mut found = Vec::new();
    let campaign = Campaign {
        seeds: &[Path::new(env!("CARGO_MANIFEST_DIR")).join("seeds")],
        corpus: dir.path(),
        duration: Some(Duration::from_secs(CAMPAIGN_SECS)),
    };
    let execute = |program: &Program| {
        let execution = execute(&env, program)?;
        found.extend(execution.findings);
        Ok::<_, String>(execution.coverage.edges)
    };
    fuzz(&campaign, &AtomicBool::new(false), execute, mutate).unwrap();
    assert!(
        found
            .iter()
            .any(|f| f.contains("QueryNode_CheckIsValid") || f.contains("module.c")),
        "{found:?}"
    );
}
