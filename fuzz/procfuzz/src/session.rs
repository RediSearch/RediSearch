/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! What a fuzzer does with a way to execute and mutate programs: replay one,
//! reduce one to a finding, or fuzz in parallel. Progress goes to `log`.

use std::collections::BTreeMap;

use log::{debug, info};
use rand::rngs::SmallRng;
use serde::Serialize;

use crate::campaign::{Campaign, fuzz};
use crate::edit::reduce;
use crate::findings::Findings;
use crate::program::{Program, TypedOp};
use crate::run::Execution;

/// Runs `program` `repeat` times, reports how often each finding and edge
/// recurs, and writes a bundle per finding. Returns whether anything was
/// found.
pub fn replay<T: TypedOp, V: Clone + Serialize + std::fmt::Debug>(
    execute: impl Fn(&Program<T>) -> Result<Execution<V>, String>,
    findings: &Findings,
    program: &Program<T>,
    original: Option<&Program<T>>,
    repeat: u32,
) -> Result<bool, String> {
    let mut found: BTreeMap<String, (u32, Execution<V>)> = BTreeMap::new();
    let mut runs_per_edge: BTreeMap<u32, u32> = BTreeMap::new();
    for attempt in 1..=repeat {
        let execution = execute(program)?;
        for step in &execution.trace.steps {
            debug!("[{}] {:?} -> {:?}", step.op, step.argv, step.reply);
        }
        info!(
            "run {attempt}/{repeat}: {:?}, {} steps, {}/{} edges, {} findings",
            execution.trace.exit,
            execution.trace.steps.len(),
            execution.coverage.edges.len(),
            execution.coverage.guards,
            execution.findings.len()
        );
        for &edge in &execution.coverage.edges {
            *runs_per_edge.entry(edge).or_default() += 1;
        }
        for signature in &execution.findings {
            found
                .entry(signature.clone())
                .or_insert_with(|| (0, execution.clone()))
                .0 += 1;
        }
    }
    if repeat > 1 {
        let stable = runs_per_edge
            .values()
            .filter(|&&runs| runs == repeat)
            .count();
        info!(
            "edges: {} in any run, {stable} in every run",
            runs_per_edge.len()
        );
    }
    for (signature, (reproduced, execution)) in &found {
        let dir = findings
            .write(
                signature,
                program,
                original,
                execution,
                (*reproduced, repeat),
            )
            .map_err(|e| e.to_string())?;
        info!("{reproduced}/{repeat} {signature} -> {}", dir.display());
    }
    Ok(!found.is_empty())
}

/// The smallest program found that still yields `signature`, or the first
/// finding of `program` if `signature` is `None`.
pub fn reduce_to_finding<T: TypedOp, V>(
    execute: impl Fn(&Program<T>) -> Result<Execution<V>, String>,
    program: &Program<T>,
    signature: Option<&str>,
) -> Result<Program<T>, String> {
    let finding = execute(program)?
        .findings
        .into_iter()
        .find(|finding| signature.is_none_or(|s| s == finding))
        .ok_or("the program does not yield that finding")?;
    info!("reducing {finding}");
    Ok(reduce(program, |candidate| {
        let fails = execute(candidate).is_ok_and(|e| e.findings.contains(&finding));
        if fails {
            info!(
                "kept {} ops, size {}",
                candidate.ops.len(),
                candidate.size()
            );
        }
        fails
    }))
}

/// Runs `jobs` independent campaigns in threads named `worker <n>`, each
/// keeping its corpus in `<campaign.corpus>/<n>`, and writes each finding new
/// to `findings`.
pub fn fuzz_parallel<T: TypedOp, V: Serialize>(
    execute: impl Fn(&Program<T>) -> Result<Execution<V>, String> + Sync,
    mutate: impl Fn(&Program<T>, &Program<T>, &mut SmallRng) -> Program<T> + Sync,
    findings: &Findings,
    campaign: &Campaign<'_>,
    jobs: usize,
) -> Result<(), String> {
    let (execute, mutate) = (&execute, &mutate);
    std::thread::scope(|scope| {
        let workers: Vec<_> = (0..jobs)
            .map(|worker| {
                std::thread::Builder::new()
                    .name(format!("worker {worker}"))
                    .spawn_scoped(scope, move || {
                        let corpus = campaign.corpus.join(worker.to_string());
                        let campaign = Campaign {
                            corpus: &corpus,
                            ..*campaign
                        };
                        let execute = |program: &Program<T>| {
                            let execution = execute(program)?;
                            for signature in &execution.findings {
                                if findings.claim(signature) {
                                    let dir = findings
                                        .write(signature, program, None, &execution, (1, 1))
                                        .map_err(|e| e.to_string())?;
                                    info!("new finding: {signature} -> {}", dir.display());
                                }
                            }
                            Ok::<_, String>(execution.coverage.edges)
                        };
                        fuzz(&campaign, execute, mutate)
                    })
                    .expect("threads start")
            })
            .collect();
        workers
            .into_iter()
            .map(|worker| worker.join().expect("workers do not panic"))
            .collect::<Result<Vec<()>, String>>()
    })?;
    Ok(())
}
