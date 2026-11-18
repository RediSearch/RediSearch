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
use std::sync::atomic::{AtomicBool, Ordering};

use log::{debug, error, info};
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
/// to `findings`. A worker that fails stops the others, and its error is
/// returned.
pub fn fuzz_parallel<T: TypedOp, V: Serialize>(
    execute: impl Fn(&Program<T>) -> Result<Execution<V>, String> + Sync,
    mutate: impl Fn(&Program<T>, &Program<T>, &mut SmallRng) -> Program<T> + Sync,
    findings: &Findings,
    campaign: &Campaign<'_>,
    jobs: usize,
) -> Result<(), String> {
    let (execute, mutate) = (&execute, &mutate);
    let stop = &AtomicBool::new(false);
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
                        let result = fuzz(&campaign, stop, execute, mutate);
                        if let Err(e) = &result {
                            error!("{e}");
                            stop.store(true, Ordering::Relaxed);
                        }
                        result
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::mpsc;
    use std::time::Duration;

    use serde::Deserialize;

    use super::*;
    use crate::coverage::Coverage;
    use crate::program::{Argv, Literal};
    use crate::run::{Exit, Status, Trace};

    #[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
    enum NoOp {}

    impl TypedOp for NoOp {
        fn lower(&self) -> Vec<Argv> {
            match *self {}
        }
        fn literals(&mut self) -> Vec<Literal<'_>> {
            match *self {}
        }
        fn simpler(&self) -> Vec<Self> {
            match *self {}
        }
        fn size(&self) -> usize {
            match *self {}
        }
    }

    #[test]
    fn a_failing_worker_stops_the_campaign_without_waiting_for_the_others() {
        let (done, result) = mpsc::channel();
        // Not scoped, so that a campaign that never stops fails the test
        // rather than hanging it.
        std::thread::spawn(move || done.send(campaign_with_a_failing_worker()));
        let result = result
            .recv_timeout(Duration::from_secs(60))
            .expect("the campaign stops");
        assert!(result.unwrap_err().contains("runs in a row failed"));
    }

    fn campaign_with_a_failing_worker() -> Result<(), String> {
        let dir = tempfile::tempdir().unwrap();
        let seeds = dir.path().join("seeds");
        std::fs::create_dir(&seeds).unwrap();
        std::fs::write(
            seeds.join("seed.json"),
            r#"{"ops": [{"op": "command", "argv": ["X"]}]}"#,
        )
        .unwrap();
        let findings = Findings::new(&dir.path().join("findings"), &()).unwrap();
        let corpus = dir.path().join("corpus");
        let campaign = Campaign {
            seeds: &[seeds],
            corpus: &corpus,
            duration: None,
        };
        let failing_worker_runs = AtomicUsize::new(0);
        let execute = |_: &Program<NoOp>| {
            let name = std::thread::current().name().map(str::to_owned);
            // Its seed succeeds, so the worker gets past loading the corpus.
            if name.as_deref() == Some("worker 0")
                && failing_worker_runs.fetch_add(1, Ordering::Relaxed) > 0
            {
                return Err("the server did not start".to_owned());
            }
            Ok(Execution {
                trace: Trace::<()> {
                    steps: Vec::new(),
                    exit: Exit::Clean,
                    status: Status::from(None),
                },
                log: Vec::new(),
                coverage: Coverage {
                    guards: 1,
                    edges: vec![1],
                },
                findings: Vec::new(),
            })
        };
        let mutate = |program: &Program<NoOp>, _: &Program<NoOp>, _: &mut SmallRng| program.clone();
        fuzz_parallel(execute, mutate, &findings, &campaign, 2)
    }
}
