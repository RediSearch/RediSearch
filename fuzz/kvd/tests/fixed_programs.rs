/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::path::PathBuf;

use kvd::{Kvd, Op, service};
use procfuzz::edit::shrink;
use procfuzz::model::{Made, Program};
use procfuzz::system::run;
use procfuzz_redis::resp::Value;

const CRASH: &str = "crash:signal 6";
const HANG: &str = "hang:no reply";
const LOGIC: &str = "logic:get_last_set";
/// Ops that change nothing the planted bugs depend on.
const PADDING: [Op; 3] = [Op::Get { key: 5 }, Op::Pop { list: 0 }, Op::Settle];

fn program(ops: &[Op]) -> Program<Op> {
    Program {
        ops: ops
            .iter()
            .map(|op| Made {
                op: op.clone(),
                from: Vec::new(),
            })
            .collect(),
    }
}

fn binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_kvd"))
}

/// The findings of one run, checking that every op got a step.
fn findings(program: &Program<Op>) -> Vec<String> {
    let run = run(&Kvd, &service(binary()), program).expect("runs");
    assert_eq!(run.steps.len(), program.ops.len());
    run.notes.findings.iter().map(ToString::to_string).collect()
}

fn crash() -> Vec<Op> {
    vec![
        Op::Set { key: 1, val: 0 },
        Op::Set { key: 3, val: 0 },
        Op::Set { key: 0, val: 0 },
        Op::Open { slot: 0 },
        Op::Del { key: 3 },
        Op::Next { slot: 0 },
        Op::Next { slot: 0 },
    ]
}

fn logic() -> Vec<Op> {
    vec![
        Op::Compact,
        Op::Set { key: 2, val: 0 },
        Op::Set { key: 1, val: 1 },
        Op::Set { key: 3, val: 0 },
        Op::Set { key: 0, val: 0 },
        Op::Set { key: 0, val: 1 },
        Op::Get { key: 0 },
    ]
}

fn hang() -> Vec<Op> {
    vec![
        Op::Push { list: 1, val: 0 },
        Op::Push { list: 1, val: 0 },
        Op::Push { list: 1, val: 0 },
        Op::DelList { list: 1 },
        Op::BPop { conn: 1, list: 1 },
    ]
}

#[test]
fn each_planted_bug_is_found_by_its_program_and_only_there() {
    let clean = vec![
        Op::Set { key: 0, val: 1 },
        Op::Get { key: 0 },
        Op::Del { key: 0 },
        Op::Get { key: 0 },
    ];
    for (ops, expected) in [
        (clean, &[][..]),
        (crash(), &[CRASH]),
        (logic(), &[LOGIC]),
        (hang(), &[HANG]),
    ] {
        assert_eq!(findings(&program(&ops)), expected, "{ops:?}");
    }
}

#[test]
fn a_program_of_launch_flags_starts_nothing() {
    let run = run(&Kvd, &service(binary()), &program(&[Op::Compact])).expect("runs");
    assert!(run.files.is_empty());
    assert!(run.steps[0].done.is_some());
}

#[test]
fn a_held_back_reply_is_released_by_healing_rather_than_reported() {
    let ops = [Op::BPop { conn: 1, list: 0 }, Op::Get { key: 0 }];
    let run = run(&Kvd, &service(binary()), &program(&ops)).expect("runs");
    assert!(run.notes.findings.is_empty(), "{:?}", run.notes.findings);
    assert_eq!(run.steps[0].reply, Some(Value::Nil));
}

#[test]
fn a_scan_reads_the_cursor_id_the_server_chose() {
    let ops = [
        Op::Set { key: 0, val: 0 },
        Op::Open { slot: 1 },
        Op::Next { slot: 1 },
        Op::Close { slot: 1 },
    ];
    let run = run(&Kvd, &service(binary()), &program(&ops)).expect("runs");
    assert!(matches!(run.steps[2].reply, Some(Value::Array(_))));
    assert_eq!(run.steps[3].reply, Some(Value::Simple("OK".into())));
}

#[test]
fn padded_programs_shrink_to_the_planted_bugs() {
    for (bug, finding) in [(crash(), CRASH), (logic(), LOGIC), (hang(), HANG)] {
        let mut padded = Vec::new();
        for (at, op) in bug.iter().enumerate() {
            padded.push(PADDING[at % PADDING.len()].clone());
            padded.push(op.clone());
        }
        let padded = program(&padded);
        let fails = |candidate: &Program<Op>| findings(candidate).iter().any(|f| f == finding);
        let shrunk = shrink(&Kvd, &padded, fails).ops();
        assert_eq!(shrunk, bug, "{finding}");
    }
}
