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

use procfuzz::campaign::{Campaign, Exchange, fuzz};
use procfuzz::commands::{load, reduce};
use procfuzz::draw::Draw;
use procfuzz::edit::mutate;
use procfuzz::model::{Made, generate};
use procfuzz::system::{Run, run};
use procfuzz_redis::resp::Value;
use redisearch_fuzz::op::{Action, Op};
use redisearch_fuzz::server::Env;
use redisearch_fuzz::{Program, RediSearch};

const GENERATED: usize = 50;
const PROGRAM_OPS: usize = 30;
const CAMPAIGN_SECS: u64 = 60;

fn program(path: &str) -> Program {
    load(&Path::new(env!("CARGO_MANIFEST_DIR")).join(path)).unwrap()
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

fn execute(env: &Env, program: &Program) -> Run<Value> {
    run(&RediSearch, &env.service(), program).unwrap()
}

fn findings(run: &Run<Value>) -> Vec<String> {
    run.notes.findings.iter().map(ToString::to_string).collect()
}

fn kinds(run: &Run<Value>) -> Vec<&'static str> {
    run.notes
        .findings
        .iter()
        .map(|finding| finding.kind)
        .collect()
}

fn is_error(reply: &Option<Value>) -> bool {
    matches!(reply, Some(Value::Error(_)))
}

#[test]
#[ignore = "needs bin/fuzz/env.sh"]
fn every_seed_succeeds_and_reaches_edges_no_other_seed_does() {
    let seeds: Vec<(String, BTreeSet<u64>)> =
        std::fs::read_dir(Path::new(env!("CARGO_MANIFEST_DIR")).join("seeds"))
            .unwrap()
            .map(|entry| {
                let name = format!("seeds/{}", entry.unwrap().file_name().to_string_lossy());
                let run = execute(&redisearch(), &program(&name));
                assert_eq!(findings(&run), Vec::<String>::new(), "{name}");
                for step in &run.steps {
                    assert!(
                        step.done.is_some() && !is_error(&step.reply),
                        "{name}: {step:?}"
                    );
                }
                let edges = run
                    .notes
                    .features
                    .iter()
                    .filter(|feature| feature.space == "edge")
                    .map(|feature| feature.id)
                    .collect();
                (name, edges)
            })
            .collect();
    for (name, edges) in &seeds {
        let others: BTreeSet<u64> = seeds
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
    let run = execute(&redisearch(), &program("tests/programs/hang.json"));
    assert_eq!(kinds(&run), ["hang"]);
    assert_eq!(run.steps[0].done, None);
}

#[test]
#[ignore = "needs bin/fuzz/env.sh"]
fn a_reply_the_program_held_back_is_released_and_the_program_carries_on() {
    let run = execute(&redisearch(), &program("tests/programs/stall.json"));
    assert_eq!(findings(&run), Vec::<String>::new());
    let [.., query, ping] = run.steps.as_slice() else {
        panic!("{:?}", run.steps);
    };
    assert!(
        query.done.is_some() && query.done < ping.done,
        "{:?}",
        run.steps
    );
}

#[test]
#[ignore = "needs bin/fuzz/env.sh"]
fn a_settle_that_cannot_complete_times_out_without_a_finding() {
    let run = execute(
        &redisearch(),
        &program("tests/programs/barrier-blocked-scan.json"),
    );
    assert_eq!(findings(&run), Vec::<String>::new());
    assert_eq!(run.steps.last().unwrap().done, None);
}

#[test]
#[ignore = "needs bin/fuzz/env.sh"]
fn most_replies_to_generated_programs_are_not_errors() {
    let env = redisearch();
    let mut draw = Draw::new(0);
    let (mut replies, mut errors) = (0, 0);
    for _ in 0..GENERATED {
        let program = generate(&RediSearch, &mut draw, PROGRAM_OPS);
        let run = execute(&env, &program);
        for step in run.steps.iter().filter(|step| step.reply.is_some()) {
            replies += 1;
            errors += usize::from(is_error(&step.reply));
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
    let segfault = Made {
        op: Op::new(Action::command(&["DEBUG", "SEGFAULT"])),
        from: Vec::new(),
    };
    let mut program = generate(&RediSearch, &mut Draw::new(0), PROGRAM_OPS);
    program.ops.insert(program.ops.len() / 2, segfault.clone());
    let finding = findings(&execute(&env, &program)).remove(0);
    assert!(finding.starts_with("crash:"), "{finding}");
    let reduced = reduce(&RediSearch, &env.service(), &program, Some(&finding)).unwrap();
    assert_eq!(reduced.ops, [segfault]);
}

#[test]
#[ignore = "needs bin/fuzz/env.sh"]
fn planted_bugs_give_a_real_sanitizer_report_and_a_real_assertion() {
    for (path, kind) in [
        ("planted/overflow.json", "asan"),
        ("planted/assert.json", "assert"),
    ] {
        assert_eq!(
            kinds(&execute(&planted(), &program(path))),
            [kind],
            "{path}"
        );
    }
}

#[test]
#[ignore = "needs bin/fuzz/env.sh"]
fn a_short_campaign_from_the_seeds_finds_a_planted_bug() {
    let dir = tempfile::tempdir().unwrap();
    let env = planted();
    let service = env.service();
    let mut found = Vec::new();
    let campaign = Campaign {
        seeds: &[Path::new(env!("CARGO_MANIFEST_DIR")).join("seeds")],
        corpus: dir.path(),
        duration: Some(Duration::from_secs(CAMPAIGN_SECS)),
    };
    let execute = |program: &Program| {
        let run = run(&RediSearch, &service, program)?;
        found.extend(findings(&run));
        Ok::<_, std::io::Error>(run.notes.features)
    };
    let mutate =
        |parent: &Program, donor: &Program, rng: &mut Draw| mutate(&RediSearch, parent, donor, rng);
    let exchange = Exchange::new();
    fuzz(
        &campaign,
        &AtomicBool::new(false),
        exchange.peer(0),
        execute,
        mutate,
    )
    .unwrap();
    assert!(
        found
            .iter()
            .any(|f| f.contains("QueryNode_CheckIsValid") || f.contains("module.c")),
        "{found:?}"
    );
}
