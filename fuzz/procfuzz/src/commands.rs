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
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

use log::{debug, error, info};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::campaign::{self, Campaign};
use crate::draw::Draw;
use crate::edit::{mutate, shrink};
use crate::error::Error;
use crate::findings::{Findings, Rate};
use crate::model::{Model, Program, generate};
use crate::system::{Run, System, run};

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
/// to `findings`. A worker that fails stops the others, and its error is
/// returned.
pub fn fuzz<M, S>(
    model: &M,
    system: &S,
    findings: &Findings,
    campaign: &Campaign<'_>,
    jobs: usize,
) -> Result<(), Error>
where
    M: Model,
    M::Reply: Serialize,
    S: System<Input = M::Input, Reply = M::Reply>,
{
    let stop = &AtomicBool::new(false);
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
                    let rate = Rate {
                        reproduced: 1,
                        runs: 1,
                    };
                    let dir = findings.write(&name, program, None, &run, rate)?;
                    info!("new finding: {name} -> {}", dir.display());
                }
            }
            Ok::<_, Error>(run.notes.features)
        };
        let mutate = |parent: &Program<M::Op>, donor: &Program<M::Op>, rng: &mut Draw| {
            mutate(model, parent, donor, rng)
        };
        let result = campaign::fuzz(&campaign, stop, execute, mutate);
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
            .collect::<Result<Vec<()>, _>>()
    })?;
    Ok(())
}
