/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! What a fuzzer does with a model and a system: replay a program, reduce
//! one to a finding, fuzz, or generate one. [`Command`] is a clap subcommand
//! for a target's own parser:
//!
//! ```ignore
//! #[derive(clap::Parser)]
//! struct Cli {
//!     #[command(subcommand)]
//!     command: procfuzz::commands::Command,
//!     #[arg(long, env = "MY_SERVER")]
//!     server: PathBuf,
//! }
//!
//! let cli = Cli::parse();
//! let found = cli.command.exec(&MyModel, &my_service(&cli.server), &cli.server)?;
//! ```
//!
//! Progress goes to `log`.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use clap::Subcommand;
use log::{debug, info};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::campaign::{self, Campaign};
use crate::draw::Draw;
use crate::edit::{mutate, shrink};
use crate::findings::{Findings, Rate};
use crate::interrupt;
use crate::model::{Model, Program, generate};
use crate::system::{Run, System, run};

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run a program on a fresh system and write a bundle per finding.
    Run {
        program: PathBuf,
        /// Run the program this many times and report how often each
        /// finding recurs.
        #[arg(long, default_value_t = 1)]
        repeat: u32,
        #[arg(long, default_value = "findings")]
        findings: PathBuf,
    },
    /// Shrink a program while it still yields a finding, then run the result
    /// like `run`.
    Reduce {
        program: PathBuf,
        /// The finding to keep; by default the program's first.
        #[arg(long)]
        finding: Option<String>,
        #[arg(long, default_value_t = 10)]
        repeat: u32,
        #[arg(long, default_value = "findings")]
        findings: PathBuf,
    },
    /// Run coverage-guided campaigns until stopped or for `--duration`.
    Fuzz {
        /// Directories of seed programs.
        #[arg(long)]
        seeds: Vec<PathBuf>,
        /// Stop after this many seconds.
        #[arg(long)]
        duration: Option<u64>,
        /// Where the programs kept are written.
        #[arg(long, default_value = "corpus")]
        corpus: PathBuf,
        #[arg(long, default_value = "findings")]
        findings: PathBuf,
        /// Campaigns to run at once; by default one per core.
        #[arg(long)]
        jobs: Option<usize>,
    },
    /// Print a generated program.
    Generate {
        #[arg(long, default_value_t = 0)]
        seed: u64,
        #[arg(long, default_value_t = 20)]
        ops: usize,
    },
}

impl Command {
    /// Runs the command and returns whether anything was found. `setup` is
    /// recorded in every finding's manifest, such as the binaries used. The
    /// first SIGINT or SIGTERM stops the command cleanly.
    pub fn exec<M, S>(self, model: &M, system: &S, setup: &impl Serialize) -> io::Result<bool>
    where
        M: Model,
        M::Reply: Clone + Serialize + Debug,
        S: System<Input = M::Input, Reply = M::Reply>,
    {
        interrupt::on_signals()?;
        match self {
            Command::Run {
                program,
                repeat,
                findings,
            } => {
                let findings = Findings::new(&findings, setup)?;
                replay(model, system, &findings, &load(&program)?, None, repeat)
            }
            Command::Reduce {
                program,
                finding,
                repeat,
                findings,
            } => {
                let findings = Findings::new(&findings, setup)?;
                let original = load(&program)?;
                let reduced = reduce(model, system, &original, finding.as_deref())?;
                replay(model, system, &findings, &reduced, Some(&original), repeat)
            }
            Command::Fuzz {
                seeds,
                duration,
                corpus,
                findings,
                jobs,
            } => {
                let findings = Findings::new(&findings, setup)?;
                let campaign = Campaign {
                    seeds: &seeds,
                    corpus: &corpus,
                    duration: duration.map(Duration::from_secs),
                };
                let jobs = jobs
                    .or_else(|| std::thread::available_parallelism().ok().map(usize::from))
                    .unwrap_or(1);
                fuzz(model, system, &findings, &campaign, jobs)?;
                Ok(false)
            }
            Command::Generate { seed, ops } => {
                let program = generate(model, &mut Draw::new(seed), ops);
                println!(
                    "{}",
                    serde_json::to_string_pretty(&program).expect("serializes")
                );
                Ok(false)
            }
        }
    }
}

/// Runs of a new finding's program that tell how often it reproduces.
const REPRODUCTIONS: u32 = 5;

pub fn load<O: DeserializeOwned>(path: &Path) -> io::Result<Program<O>> {
    let text = std::fs::read_to_string(path)?;
    serde_json::from_str(&text).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{}: {e}", path.display()),
        )
    })
}

/// Runs `program` `repeat` times, reports how often each finding recurs, and
/// writes a bundle per finding. Returns whether anything was
/// found.
pub fn replay<M, S>(
    model: &M,
    system: &S,
    findings: &Findings,
    program: &Program<M::Op>,
    original: Option<&Program<M::Op>>,
    repeat: u32,
) -> io::Result<bool>
where
    M: Model,
    M::Reply: Clone + Serialize + Debug,
    S: System<Input = M::Input, Reply = M::Reply>,
{
    let mut found: BTreeMap<String, (u32, Run<M::Reply>)> = BTreeMap::new();
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
        for finding in &run.notes.findings {
            found
                .entry(finding.to_string())
                .or_insert_with(|| (0, run.clone()))
                .0 += 1;
        }
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
) -> io::Result<Program<M::Op>>
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
        .ok_or_else(|| io::Error::other("the program does not yield that finding"))?;
    info!("reducing {finding}");
    Ok(shrink(model, program, |candidate| {
        let fails = run(model, system, candidate).is_ok_and(|run| run.notes.shows(&finding));
        if fails {
            info!("kept {} ops", candidate.ops.len());
        }
        fails
    }))
}

/// Runs a campaign in `jobs` threads, and writes each new finding to
/// `findings` with how often it reproduces in [`REPRODUCTIONS`] runs.
pub fn fuzz<M, S>(
    model: &M,
    system: &S,
    findings: &Findings,
    campaign: &Campaign<'_>,
    jobs: usize,
) -> io::Result<()>
where
    M: Model,
    M::Reply: Serialize,
    S: System<Input = M::Input, Reply = M::Reply>,
{
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
        Ok(run.notes.features)
    };
    let mutate = |parent: &Program<M::Op>, donor: &Program<M::Op>, rng: &mut Draw| {
        mutate(model, parent, donor, rng)
    };
    campaign::fuzz(campaign, jobs, execute, mutate)
}

/// How often `finding` shows in [`REPRODUCTIONS`] runs of `program`, the
/// first of which already showed it.
fn reproduce<M, S>(model: &M, system: &S, program: &Program<M::Op>, finding: &str) -> Rate
where
    M: Model,
    S: System<Input = M::Input, Reply = M::Reply>,
{
    let again = (1..REPRODUCTIONS)
        .filter(|_| run(model, system, program).is_ok_and(|run| run.notes.shows(finding)))
        .count();
    Rate {
        reproduced: 1 + u32::try_from(again).expect("small"),
        runs: REPRODUCTIONS,
    }
}
