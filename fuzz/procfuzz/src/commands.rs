/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! What a fuzzer does with a model and a system: replay a program, reduce
//! one to a finding, or fuzz. Progress goes to `log`.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use log::{debug, error, info};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::campaign::{self, Campaign, Exchange, Outcome, Setup};
use crate::draw::Draw;
use crate::edit::{mutate, shrink};
use crate::error::Error;
use crate::findings::{Findings, Rate};
use crate::model::{Model, Program, generate};
use crate::system::{Run, System, run};

/// Runs of a new finding's program that tell how often it reproduces.
pub const REPRODUCTIONS: u32 = 5;

pub fn load<O: DeserializeOwned>(path: &Path) -> Result<Program<O>, Error> {
    let text = std::fs::read_to_string(path)?;
    serde_json::from_str(&text).map_err(|source| Error::Program {
        path: path.to_owned(),
        source,
    })
}

/// A program of `len` fresh ops.
pub fn generate_from<M: Model>(model: &M, seed: u64, len: usize) -> Program<M::Op> {
    generate(model, &mut Draw::new(seed), len)
}

/// Runs `program` `repeat` times, reports how often each finding and feature
/// recurs, and writes a bundle per finding. Returns whether anything was
/// found.
pub fn replay<M, S>(
    model: &M,
    system: &S,
    findings: &Findings,
    program: &Program<M::Op>,
    original: Option<&Program<M::Op>>,
    repeat: u32,
) -> Result<bool, Error>
where
    M: Model,
    M::Reply: Clone + Serialize + Debug,
    S: System<Input = M::Input, Reply = M::Reply>,
{
    let mut found: BTreeMap<String, (u32, Run<M::Reply>)> = BTreeMap::new();
    let mut runs_per_feature = BTreeMap::new();
    for attempt in 1..=repeat {
        let run = run(model, system, program)?;
        for (made, step) in program.ops.iter().zip(&run.steps) {
            debug!("{:?} -> {:?}", made.op, step.reply);
        }
        info!(
            "run {attempt}/{repeat}: {} of {} ops, {} features, findings {:?}",
            run.steps.len(),
            program.ops.len(),
            run.notes.features.len(),
            run.notes
                .findings
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        );
        for &feature in &run.notes.features {
            *runs_per_feature.entry(feature).or_insert(0) += 1;
        }
        for finding in &run.notes.findings {
            found
                .entry(finding.to_string())
                .or_insert_with(|| (0, run.clone()))
                .0 += 1;
        }
    }
    if repeat > 1 {
        let stable = runs_per_feature
            .values()
            .filter(|&&runs| runs == repeat)
            .count();
        info!(
            "features: {} in any run, {stable} in every run",
            runs_per_feature.len()
        );
    }
    for (finding, (reproduced, run)) in &found {
        let rate = Rate {
            reproduced: *reproduced,
            runs: repeat,
        };
        let dir = findings.write(finding, program, original, run, rate)?;
        info!("{reproduced}/{repeat} {finding} -> {}", dir.display());
    }
    Ok(!found.is_empty())
}

/// The smallest program found that still yields `finding`, or the first
/// finding of `program` if `finding` is `None`.
pub fn reduce<M, S>(
    model: &M,
    system: &S,
    program: &Program<M::Op>,
    finding: Option<&str>,
) -> Result<Program<M::Op>, Error>
where
    M: Model,
    S: System<Input = M::Input, Reply = M::Reply>,
{
    let finding = run(model, system, program)?
        .notes
        .findings
        .iter()
        .map(ToString::to_string)
        .find(|found| finding.is_none_or(|wanted| wanted == found))
        .ok_or(Error::NoSuchFinding)?;
    info!("reducing {finding}");
    Ok(shrink(model, program, |candidate| {
        let fails = run(model, system, candidate).is_ok_and(|run| {
            run.notes
                .findings
                .iter()
                .any(|found| found.to_string() == finding)
        });
        if fails {
            info!("kept {} ops", candidate.ops.len());
        }
        fails
    }))
}

/// Runs `jobs` independent campaigns in threads named `worker <n>`, each
/// keeping its corpus in `<campaign.corpus>/<n>`, and writes each new finding
/// to `findings`, with how often it reproduces in [`REPRODUCTIONS`] runs.
/// With `share`, each worker takes the programs the others keep, without
/// running them. A worker that fails stops the others, and its error is
/// returned.
pub fn fuzz<M, S>(
    model: &M,
    system: &S,
    findings: &Findings,
    campaign: &Campaign<'_>,
    jobs: usize,
    share: bool,
) -> Result<Vec<Outcome>, Error>
where
    M: Model,
    M::Reply: Serialize,
    S: System<Input = M::Input, Reply = M::Reply>,
{
    let stop = &AtomicBool::new(false);
    let exchange = &Exchange::new();
    let worker = |n: usize| {
        let corpus = campaign.corpus.join(n.to_string());
        let campaign = Campaign {
            corpus: &corpus,
            ..*campaign
        };
        let execute = |program: &Program<M::Op>| {
            let run = run(model, system, program)?;
            for finding in &run.notes.findings {
                let name = finding.to_string();
                if findings.claim(&name) {
                    let rate = reproduce(model, system, program, &name);
                    let dir = findings.write(&name, program, None, &run, rate)?;
                    info!("new finding: {name} -> {}", dir.display());
                }
            }
            Ok::<_, Error>(run.notes.features)
        };
        let mutate = |parent: &Program<M::Op>, donor: &Program<M::Op>, rng: &mut Draw| {
            mutate(model, parent, donor, rng)
        };
        let peer = share.then(|| exchange.peer(n));
        let result = campaign::fuzz(&campaign, stop, peer, execute, mutate);
        if let Err(e) = &result {
            error!("{e}");
            stop.store(true, Ordering::Relaxed);
        }
        result
    };
    let worker = &worker;
    std::thread::scope(|scope| {
        let workers: Vec<_> = (0..jobs)
            .map(|n| {
                std::thread::Builder::new()
                    .name(format!("worker {n}"))
                    .spawn_scoped(scope, move || worker(n))
                    .expect("threads start")
            })
            .collect();
        workers
            .into_iter()
            .map(|worker| worker.join().expect("workers do not panic"))
            .collect()
    })
}

/// How often `finding` shows in [`REPRODUCTIONS`] runs of `program`, the
/// first of which already showed it.
fn reproduce<M, S>(model: &M, system: &S, program: &Program<M::Op>, finding: &str) -> Rate
where
    M: Model,
    S: System<Input = M::Input, Reply = M::Reply>,
{
    let again = (1..REPRODUCTIONS)
        .filter(|_| {
            run(model, system, program).is_ok_and(|run| {
                run.notes
                    .findings
                    .iter()
                    .any(|found| found.to_string() == finding)
            })
        })
        .count();
    Rate {
        reproduced: 1 + u32::try_from(again).expect("small"),
        runs: REPRODUCTIONS,
    }
}

/// How one setup did over repeated trials.
#[derive(Debug, Serialize)]
pub struct Trials {
    pub setup: Setup,
    pub outcomes: Vec<Outcome>,
    /// Per finding, the run at which each trial that showed it first did.
    pub found: BTreeMap<String, Vec<u64>>,
}

/// What to compare: `trials` campaigns of `runs` runs each per setup, from
/// `seeds`.
pub struct Comparison<'a> {
    pub setups: &'a [Setup],
    pub trials: u32,
    pub seeds: &'a [PathBuf],
    pub runs: u64,
}

/// Runs a comparison, at most `jobs` campaigns at once, and reports what
/// each setup found and kept. Each
/// finding new to `findings` is written there, without the reruns that
/// measure how often it reproduces, so every setup gets the same runs.
pub fn compare<M, S>(
    model: &M,
    system: &S,
    findings: &Findings,
    comparison: &Comparison<'_>,
    jobs: usize,
) -> Result<Vec<Trials>, Error>
where
    M: Model,
    M::Reply: Serialize,
    S: System<Input = M::Input, Reply = M::Reply>,
{
    let Comparison {
        setups,
        trials,
        seeds,
        runs,
    } = *comparison;
    let plan: Vec<(usize, u32)> = (0..setups.len())
        .flat_map(|setup| (0..trials).map(move |trial| (setup, trial)))
        .collect();
    let next = AtomicUsize::new(0);
    let stop = &AtomicBool::new(false);
    let results = Mutex::new(Vec::new());
    let trial = |setup: Setup| -> Result<(Outcome, BTreeMap<String, u64>), Error> {
        let corpus = tempfile::tempdir()?;
        let campaign = Campaign {
            seeds,
            corpus: corpus.path(),
            duration: None,
            runs: Some(runs),
            setup,
        };
        let mut found = BTreeMap::new();
        let mut done = 0;
        let execute = |program: &Program<M::Op>| {
            done += 1;
            let run = run(model, system, program)?;
            for finding in &run.notes.findings {
                let name = finding.to_string();
                if findings.claim(&name) {
                    let rate = Rate {
                        reproduced: 1,
                        runs: 1,
                    };
                    let dir = findings.write(&name, program, None, &run, rate)?;
                    info!("new finding: {name} -> {}", dir.display());
                }
                found.entry(name).or_insert(done);
            }
            Ok::<_, Error>(run.notes.features)
        };
        let mutate = |parent: &Program<M::Op>, donor: &Program<M::Op>, rng: &mut Draw| {
            mutate(model, parent, donor, rng)
        };
        let outcome = campaign::fuzz(&campaign, stop, None, execute, mutate)?;
        Ok((outcome, found))
    };
    std::thread::scope(|scope| {
        for n in 0..jobs.max(1) {
            let (next, results, plan, trial) = (&next, &results, &plan, &trial);
            std::thread::Builder::new()
                .name(format!("worker {n}"))
                .spawn_scoped(scope, move || {
                    while let Some(&(setup, number)) =
                        plan.get(next.fetch_add(1, Ordering::Relaxed))
                    {
                        let result = trial(setups[setup]);
                        match &result {
                            Ok((outcome, found)) => info!(
                                "{} trial {number}: {} runs, kept {}, found {:?}",
                                setups[setup], outcome.runs, outcome.kept, found
                            ),
                            Err(e) => {
                                error!("{e}");
                                stop.store(true, Ordering::Relaxed);
                            }
                        }
                        results.lock().expect("not poisoned").push((setup, result));
                    }
                })
                .expect("threads start");
        }
    });
    let mut report: Vec<Trials> = setups
        .iter()
        .map(|&setup| Trials {
            setup,
            outcomes: Vec::new(),
            found: BTreeMap::new(),
        })
        .collect();
    for (setup, result) in results.into_inner().expect("not poisoned") {
        let (outcome, found) = result?;
        report[setup].outcomes.push(outcome);
        for (finding, at) in found {
            report[setup].found.entry(finding).or_default().push(at);
        }
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::atomic::AtomicUsize;
    use std::sync::mpsc;
    use std::time::Duration;

    use super::*;
    use crate::campaign::Setup;
    use crate::toy::{Store, Toy};

    #[test]
    fn a_failing_worker_stops_the_campaign_without_waiting_for_the_others() {
        let (done, result) = mpsc::channel();
        // Not scoped, so that a campaign that never stops fails the test
        // rather than hanging it.
        std::thread::spawn(move || done.send(campaign_with_a_failing_worker()));
        let result = result
            .recv_timeout(Duration::from_secs(60))
            .expect("the campaign stops");
        let error = result.unwrap_err().to_string();
        assert!(error.contains("runs in a row failed"), "{error}");
    }

    fn campaign_with_a_failing_worker() -> Result<Vec<Outcome>, Error> {
        let dir = tempfile::tempdir().unwrap();
        let findings = Findings::new(&dir.path().join("findings"), &()).unwrap();
        let corpus = dir.path().join("corpus");
        let campaign = Campaign {
            seeds: &[],
            corpus: &corpus,
            duration: None,
            runs: None,
            setup: Setup::default(),
        };
        let failing_worker_runs = AtomicUsize::new(0);
        let start = || {
            let name = std::thread::current().name().map(str::to_owned);
            // Its first run succeeds, so the worker gets past loading the
            // corpus.
            if name.as_deref() == Some("worker 0")
                && failing_worker_runs.fetch_add(1, Ordering::Relaxed) > 0
            {
                return Err(io::Error::other("the system did not start"));
            }
            Ok(Store::default())
        };
        fuzz(&Toy, &start, &findings, &campaign, 2, false)
    }
}
