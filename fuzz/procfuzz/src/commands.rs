/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! What a fuzzer does with a model and a system: reduce a program to a
//! finding, fuzz, or generate a program. [`Command`] is a clap subcommand
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

use std::fmt::Debug;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use clap::Subcommand;
use log::info;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::campaign::{self, Campaign};
use crate::draw::Draw;
use crate::edit::{mutate, shrink};
use crate::findings::{Findings, Rate};
use crate::interrupt;
use crate::model::{Model, Program, generate};
use crate::system::{System, run};

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Shrink a program while it still yields a finding, and write the
    /// finding with a script that reproduces it.
    Reduce {
        program: PathBuf,
        /// The finding to keep; by default the program's first.
        #[arg(long)]
        finding: Option<String>,
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
            Command::Reduce {
                program,
                finding,
                findings,
            } => {
                let findings = Findings::new(&findings, setup)?;
                let program = load(&program)?;
                let finding = run(model, system, &program)?
                    .notes
                    .findings
                    .iter()
                    .map(ToString::to_string)
                    .find(|found| finding.as_deref().is_none_or(|wanted| wanted == found))
                    .ok_or_else(|| io::Error::other("the program does not yield that finding"))?;
                report(model, system, &findings, &program, &finding)?;
                Ok(true)
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

/// The smallest program found that still yields `finding`.
pub fn reduce<M, S>(
    model: &M,
    system: &S,
    program: &Program<M::Op>,
    finding: &str,
) -> Program<M::Op>
where
    M: Model,
    S: System<Input = M::Input, Reply = M::Reply>,
{
    info!("reducing {finding} from {} ops", program.ops.len());
    shrink(model, program, |candidate| {
        run(model, system, candidate).is_ok_and(|run| run.notes.shows(finding))
    })
}

/// Reduces `program` to `finding`, runs the result [`REPRODUCTIONS`] times,
/// and writes the finding with the model's reproduction script.
pub fn report<M, S>(
    model: &M,
    system: &S,
    findings: &Findings,
    program: &Program<M::Op>,
    finding: &str,
) -> io::Result<()>
where
    M: Model,
    M::Reply: Serialize,
    S: System<Input = M::Input, Reply = M::Reply>,
{
    let reduced = reduce(model, system, program, finding);
    let mut reproduced = 0;
    let mut kept = None;
    for _ in 0..REPRODUCTIONS {
        let run = run(model, system, &reduced)?;
        let shows = run.notes.shows(finding);
        reproduced += u32::from(shows);
        if reproduced == 0 || (shows && reproduced == 1) {
            kept = Some(run);
        }
    }
    let mut run = kept.expect("ran");
    if let Some((name, script)) = model.reproduction(&reduced.ops(), &run.steps, finding) {
        run.files.push((name.to_owned(), script.into_bytes()));
    }
    let rate = Rate {
        reproduced,
        runs: REPRODUCTIONS,
    };
    let dir = findings.write(finding, &reduced, Some(program), &run, rate)?;
    info!(
        "{reproduced}/{REPRODUCTIONS} {finding} -> {}",
        dir.display()
    );
    Ok(())
}

/// Runs a campaign in `jobs` threads, and [reports](report) each new
/// finding to `findings`.
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
                info!("new finding: {name}");
                report(model, system, findings, program, &name)?;
            }
        }
        Ok(run.notes.features)
    };
    let mutate = |parent: &Program<M::Op>, donor: &Program<M::Op>, rng: &mut Draw| {
        mutate(model, parent, donor, rng)
    };
    campaign::fuzz(campaign, jobs, execute, mutate)
}
