/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The fuzzer's commands as a clap subcommand, for a target's own parser:
//!
//! ```ignore
//! #[derive(clap::Parser)]
//! struct Cli {
//!     #[command(subcommand)]
//!     command: procfuzz::cli::Command,
//!     #[arg(long, env = "MY_SERVER")]
//!     server: PathBuf,
//! }
//!
//! let cli = Cli::parse();
//! let found = cli.command.exec(&MyModel, &my_service(&cli.server), &cli.server)?;
//! ```

use std::fmt::Debug;
use std::path::PathBuf;
use std::time::Duration;

use clap::{Args, Subcommand};
use serde::Serialize;

use crate::campaign::{Campaign, Setup};
use crate::commands::{Trials, compare, fuzz, generate_from, load, reduce, replay};
use crate::error::Error;
use crate::findings::Findings;
use crate::interrupt;
use crate::model::Model;
use crate::system::System;

#[derive(Debug, Args)]
pub struct Search {
    /// Directories of seed programs.
    #[arg(long)]
    seeds: Vec<PathBuf>,
    /// Stop after this many seconds.
    #[arg(long)]
    duration: Option<u64>,
    /// Stop each worker after this many runs, counting reruns.
    #[arg(long)]
    runs: Option<u64>,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run a program on a fresh system and write a bundle per finding.
    Run {
        program: PathBuf,
        /// Run the program this many times and report how often each
        /// finding and each feature recurs.
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
    /// Run coverage-guided campaigns until stopped, for `--duration`, or for
    /// `--runs`.
    Fuzz {
        #[command(flatten)]
        search: Search,
        /// Keeps each worker's corpus under `<corpus>/<worker>`.
        #[arg(long, default_value = "corpus")]
        corpus: PathBuf,
        #[arg(long, default_value = "findings")]
        findings: PathBuf,
        /// `libafl` or `basic`, with `+confirm` to run a program again
        /// before counting a feature it shows for the first time.
        #[arg(long, default_value = "libafl+confirm")]
        setup: Setup,
        /// Campaigns to run at once; by default one per core.
        #[arg(long)]
        jobs: Option<usize>,
        /// Let each campaign take the programs the others keep, without
        /// running them.
        #[arg(long)]
        share: bool,
    },
    /// Run `--trials` campaigns of `--runs` runs per setup, and report which
    /// findings each setup found in how many trials, and how many programs
    /// it kept.
    Compare {
        /// Directories of seed programs.
        #[arg(long)]
        seeds: Vec<PathBuf>,
        #[arg(long)]
        runs: u64,
        #[arg(long, default_value_t = 8)]
        trials: u32,
        /// Setups to compare, such as `libafl` or `basic+confirm`.
        #[arg(long = "setup", required = true)]
        setups: Vec<Setup>,
        /// Trials to run at once; by default one per core.
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
    /// Runs the command and returns whether anything was found.
    /// `setup_record` is recorded in every finding's manifest, such as the
    /// binaries used. The
    /// first SIGINT or SIGTERM stops the command cleanly.
    pub fn exec<M, S>(
        self,
        model: &M,
        system: &S,
        setup_record: &impl Serialize,
    ) -> Result<bool, Error>
    where
        M: Model,
        M::Reply: Clone + Serialize + Debug,
        S: System<Input = M::Input, Reply = M::Reply>,
    {
        if let Err(e) = interrupt::on_signals() {
            log::warn!("an interrupt will leave processes behind: {e}");
        }
        match self {
            Command::Run {
                program,
                repeat,
                findings,
            } => {
                let findings = Findings::new(&findings, setup_record)?;
                replay(model, system, &findings, &load(&program)?, None, repeat)
            }
            Command::Reduce {
                program,
                finding,
                repeat,
                findings,
            } => {
                let findings = Findings::new(&findings, setup_record)?;
                let original = load(&program)?;
                let reduced = reduce(model, system, &original, finding.as_deref())?;
                replay(model, system, &findings, &reduced, Some(&original), repeat)
            }
            Command::Fuzz {
                search,
                corpus,
                findings,
                setup,
                jobs,
                share,
            } => {
                let findings = Findings::new(&findings, setup_record)?;
                let campaign = Campaign {
                    seeds: &search.seeds,
                    corpus: &corpus,
                    duration: search.duration.map(Duration::from_secs),
                    runs: search.runs,
                    setup,
                };
                let outcomes = fuzz(
                    model,
                    system,
                    &findings,
                    &campaign,
                    jobs_or_cores(jobs),
                    share,
                )?;
                for (worker, outcome) in outcomes.iter().enumerate() {
                    log::info!(
                        "worker {worker}: {} runs, kept {} programs",
                        outcome.runs,
                        outcome.kept
                    );
                }
                Ok(false)
            }
            Command::Compare {
                seeds,
                runs,
                trials,
                setups,
                jobs,
            } => {
                let report = compare(
                    model,
                    system,
                    &setups,
                    trials,
                    &seeds,
                    runs,
                    jobs_or_cores(jobs),
                )?;
                for line in summary(&report) {
                    log::info!("{line}");
                }
                Ok(false)
            }
            Command::Generate { seed, ops } => {
                let program = generate_from(model, seed, ops);
                println!(
                    "{}",
                    serde_json::to_string_pretty(&program).expect("serializes")
                );
                Ok(false)
            }
        }
    }
}

fn jobs_or_cores(jobs: Option<usize>) -> usize {
    jobs.or_else(|| std::thread::available_parallelism().ok().map(usize::from))
        .unwrap_or(1)
}

/// A line per setup: the programs it kept, and per finding the trials that
/// found it and the median run at which they did.
fn summary(report: &[Trials]) -> Vec<String> {
    report
        .iter()
        .map(|trials| {
            let count = trials.outcomes.len();
            let kept: usize = trials.outcomes.iter().map(|outcome| outcome.kept).sum();
            let mut line = format!(
                "{:<16} kept {:>6.1}",
                trials.setup.to_string(),
                f64::from(u32::try_from(kept).unwrap_or(u32::MAX))
                    / f64::from(u32::try_from(count.max(1)).unwrap_or(u32::MAX))
            );
            for (finding, found_at) in &trials.found {
                let mut found_at = found_at.clone();
                found_at.sort_unstable();
                line.push_str(&format!(
                    "  {finding} {}/{count} median {}",
                    found_at.len(),
                    found_at[found_at.len() / 2]
                ));
            }
            line
        })
        .collect()
}
