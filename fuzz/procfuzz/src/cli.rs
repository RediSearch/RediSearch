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

use clap::Subcommand;
use serde::Serialize;

use crate::campaign::Campaign;
use crate::commands::{fuzz, load, reduce, replay};
use crate::draw::Draw;
use crate::error::Error;
use crate::findings::Findings;
use crate::interrupt;
use crate::model::{Model, generate};
use crate::system::System;

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
        /// Keeps each worker's corpus under `<corpus>/<worker>`.
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
    pub fn exec<M, S>(self, model: &M, system: &S, setup: &impl Serialize) -> Result<bool, Error>
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
