/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand};
use procfuzz::legacy::campaign::Campaign;
use procfuzz::legacy::findings::Findings;
use procfuzz::legacy::session::{fuzz_parallel, reduce_to_finding, replay};
use rand::SeedableRng;
use rand::rngs::SmallRng;

use redisearch_fuzz::generate::generate;
use redisearch_fuzz::redis::Env;
use redisearch_fuzz::{Program, execute, mutate};

/// Stateful fuzzer for RediSearch. Exits with 0 without findings, 1 with
/// findings, and 2 on an error. `RUST_LOG=debug` logs every step.
#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run a program on a fresh server and write a bundle per finding.
    Run {
        program: PathBuf,
        /// Run the program this many times and report how often each
        /// finding and each edge recurs.
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
        signature: Option<String>,
        #[arg(long, default_value_t = 10)]
        repeat: u32,
        #[arg(long, default_value = "findings")]
        findings: PathBuf,
    },
    /// Run coverage-guided campaigns until stopped or for `--duration`.
    Fuzz {
        /// Directories of seed programs.
        #[arg(long, default_value = "seeds")]
        seeds: Vec<PathBuf>,
        /// Keeps each worker's corpus under `<corpus>/<worker>`.
        #[arg(long, default_value = "corpus")]
        corpus: PathBuf,
        #[arg(long, default_value = "findings")]
        findings: PathBuf,
        /// Stop after this many seconds.
        #[arg(long)]
        duration: Option<u64>,
        /// Independent campaigns to run at once; by default one per core.
        #[arg(long)]
        jobs: Option<usize>,
    },
    /// Print a generated program.
    Generate {
        #[arg(long, default_value_t = 0)]
        seed: u64,
    },
}

fn main() -> ExitCode {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format(|buf, record| {
            let thread = std::thread::current();
            writeln!(
                buf,
                "[{}] {}",
                thread.name().unwrap_or("main"),
                record.args()
            )
        })
        .init();
    if let Err(e) = procfuzz::interrupt::on_signals() {
        log::warn!("Ctrl-C will leave servers behind: {e}");
    }
    match command(Cli::parse().command) {
        Ok(false) => ExitCode::SUCCESS,
        Ok(true) => ExitCode::from(1),
        Err(e) => {
            log::error!("{e}");
            ExitCode::from(2)
        }
    }
}

/// Whether anything was found.
fn command(command: Command) -> Result<bool, String> {
    if let Command::Generate { seed } = command {
        let program = generate(&mut SmallRng::seed_from_u64(seed));
        println!(
            "{}",
            serde_json::to_string_pretty(&program).expect("serializes")
        );
        return Ok(false);
    }
    let env = Env::from_env()?;
    let execute = |program: &Program| execute(&env, program);
    let findings = |root: &Path| Findings::new(root, &env).map_err(|e| e.to_string());
    match command {
        Command::Run {
            program,
            repeat,
            findings: root,
        } => replay(execute, &findings(&root)?, &load(&program)?, None, repeat),
        Command::Reduce {
            program,
            signature,
            repeat,
            findings: root,
        } => {
            let original = load(&program)?;
            let reduced = reduce_to_finding(execute, &original, signature.as_deref())?;
            replay(
                execute,
                &findings(&root)?,
                &reduced,
                Some(&original),
                repeat,
            )
        }
        Command::Fuzz {
            seeds,
            corpus,
            findings: root,
            duration,
            jobs,
        } => {
            let campaign = Campaign {
                seeds: &seeds,
                corpus: &corpus,
                duration: duration.map(Duration::from_secs),
            };
            let jobs = jobs
                .or_else(|| std::thread::available_parallelism().ok().map(usize::from))
                .unwrap_or(1);
            fuzz_parallel(execute, mutate, &findings(&root)?, &campaign, jobs)?;
            Ok(false)
        }
        Command::Generate { .. } => unreachable!("handled above"),
    }
}

fn load(path: &Path) -> Result<Program, String> {
    std::fs::read_to_string(path)
        .map_err(|e| e.to_string())
        .and_then(|text| serde_json::from_str(&text).map_err(|e| e.to_string()))
        .map_err(|e| format!("{}: {e}", path.display()))
}
