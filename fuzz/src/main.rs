/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand};
use rand::SeedableRng;
use rand::rngs::SmallRng;

use redisearch_fuzz::bundle;
use redisearch_fuzz::campaign::{Campaign, fuzz};
use redisearch_fuzz::edit::size;
use redisearch_fuzz::generate::generate;
use redisearch_fuzz::judge::{Finding, execute};
use redisearch_fuzz::program::Program;
use redisearch_fuzz::reduce::reduce;
use redisearch_fuzz::run::Trace;
use redisearch_fuzz::target::Env;

#[derive(Parser)]
#[command(about = "Stateful fuzzer for RediSearch")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run one program against a fresh server and report its findings.
    Run {
        program: PathBuf,
        /// Run the program this many times and report how often each finding
        /// and each edge recurs.
        #[arg(long, default_value_t = 1)]
        repeat: u32,
        /// Directory that receives a bundle per distinct finding.
        #[arg(long, default_value = "findings")]
        findings: PathBuf,
        /// Print every step and its reply.
        #[arg(long)]
        verbose: bool,
    },
    /// Shrink a failing program while it still yields the same finding, then
    /// run the result repeatedly and report it like `run`.
    Reduce {
        program: PathBuf,
        /// The finding to keep; by default the first one the program yields.
        #[arg(long)]
        signature: Option<String>,
        #[arg(long, default_value_t = 10)]
        repeat: u32,
        #[arg(long, default_value = "findings")]
        findings: PathBuf,
    },
    /// Run a coverage-guided campaign until stopped or for `--duration`.
    Fuzz {
        /// Directories of seed programs.
        #[arg(long, default_value = "seeds")]
        seeds: Vec<PathBuf>,
        /// Directory that keeps the corpus across campaigns.
        #[arg(long, default_value = "corpus")]
        corpus: PathBuf,
        #[arg(long, default_value = "findings")]
        findings: PathBuf,
        /// Stop after this many seconds.
        #[arg(long)]
        duration: Option<u64>,
        /// Worker processes, each an independent campaign with its own
        /// corpus under `<corpus>/<worker>`; by default one per core.
        #[arg(long)]
        jobs: Option<usize>,
        #[arg(long, hide = true)]
        worker: Option<usize>,
    },
    /// Print a generated program.
    Generate {
        #[arg(long, default_value_t = 0)]
        seed: u64,
    },
}

fn main() -> ExitCode {
    let result = match Cli::parse().command {
        Command::Run {
            program,
            repeat,
            findings,
            verbose,
        } => load(&program)
            .and_then(|program| run_command(&program, None, repeat, &findings, verbose)),
        Command::Reduce {
            program,
            signature,
            repeat,
            findings,
        } => load(&program)
            .and_then(|program| reduce_command(&program, signature.as_deref(), repeat, &findings)),
        Command::Fuzz {
            seeds,
            corpus,
            findings,
            duration,
            jobs: _,
            worker: Some(worker),
        } => Env::from_env().and_then(|env| {
            fuzz(Campaign {
                worker,
                env,
                seeds: &seeds,
                corpus: &corpus.join(worker.to_string()),
                findings: &findings,
                duration: duration.map(Duration::from_secs),
            })
            .map(|()| false)
            .map_err(|e| e.to_string())
        }),
        Command::Fuzz { jobs, .. } => spawn_workers(jobs),
        Command::Generate { seed } => {
            println!("{}", generate(&mut SmallRng::seed_from_u64(seed)).to_json());
            Ok(false)
        }
    };
    match result {
        Ok(false) => ExitCode::SUCCESS,
        Ok(true) => ExitCode::from(1),
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::from(2)
        }
    }
}

/// Starts the workers with this process's arguments plus `--worker`, and
/// waits for all of them.
fn spawn_workers(jobs: Option<usize>) -> Result<bool, String> {
    let jobs = jobs
        .or_else(|| std::thread::available_parallelism().ok().map(usize::from))
        .unwrap_or(1);
    let exe = std::env::current_exe().map_err(|e| e.to_string())?;
    let args: Vec<String> = std::env::args().skip(1).collect();
    let workers = (0..jobs)
        .map(|worker| {
            std::process::Command::new(&exe)
                .args(&args)
                .args(["--worker", &worker.to_string()])
                .spawn()
                .map_err(|e| e.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut failed = false;
    for mut worker in workers {
        failed |= !worker.wait().map_err(|e| e.to_string())?.success();
    }
    if failed {
        Err("a worker failed".into())
    } else {
        Ok(false)
    }
}

fn load(path: &Path) -> Result<Program, String> {
    std::fs::read_to_string(path)
        .map_err(|e| e.to_string())
        .and_then(|text| Program::from_json(&text))
        .map_err(|e| format!("{}: {e}", path.display()))
}

fn reduce_command(
    program: &Program,
    signature: Option<&str>,
    repeat: u32,
    findings_dir: &Path,
) -> Result<bool, String> {
    let env = Env::from_env()?;
    let (_, findings) = execute(program, &env)?;
    let finding = findings
        .into_iter()
        .find(|finding| signature.is_none_or(|s| s == finding.signature))
        .ok_or("the program does not yield that finding")?;
    println!("reducing {:?} {}", finding.kind, finding.signature);
    let reduced = reduce(program, &finding, &env, |kept| {
        println!("  kept: {} ops, size {}", kept.ops.len(), size(kept));
    });
    run_command(&reduced, Some(program), repeat, findings_dir, false)
}

fn run_command(
    program: &Program,
    original: Option<&Program>,
    repeat: u32,
    findings_dir: &Path,
    verbose: bool,
) -> Result<bool, String> {
    let env = Env::from_env()?;

    let mut found: BTreeMap<Finding, (u32, Trace)> = BTreeMap::new();
    let mut edges: BTreeMap<u32, u32> = BTreeMap::new();
    for attempt in 1..=repeat {
        let (trace, findings) = execute(program, &env)?;
        if verbose {
            for step in &trace.steps {
                println!("  [{}] {:?} -> {:?}", step.op, step.argv, step.reply);
            }
        }
        println!(
            "run {attempt}/{repeat}: {:?}, {} steps, {}/{} edges, {} findings",
            trace.exit,
            trace.steps.len(),
            trace.coverage.edges.len(),
            trace.coverage.guards,
            findings.len()
        );
        for &edge in &trace.coverage.edges {
            *edges.entry(edge).or_default() += 1;
        }
        for finding in findings {
            found.entry(finding).or_insert_with(|| (0, trace.clone())).0 += 1;
        }
    }
    if repeat > 1 {
        let stable = edges.values().filter(|&&runs| runs == repeat).count();
        println!("edges: {} in any run, {stable} in every run", edges.len());
    }
    for (finding, (reproduced, trace)) in &found {
        let dir = bundle::write(
            findings_dir,
            program,
            original,
            trace,
            finding,
            (*reproduced, repeat),
            &env,
        )?;
        println!(
            "{reproduced}/{repeat} {:?} {} -> {}",
            finding.kind,
            finding.signature,
            dir.display()
        );
    }
    Ok(!found.is_empty())
}
