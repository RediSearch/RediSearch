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
use redisearch_fuzz::generate::generate;
use redisearch_fuzz::judge::{Finding, execute};
use redisearch_fuzz::program::Program;
use redisearch_fuzz::run::Trace;
use redisearch_fuzz::shrink::{reduce, size};
use redisearch_fuzz::target::Env;

#[derive(Parser)]
#[command(about = "Stateful fuzzer for RediSearch")]
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
        /// finding recurs.
        #[arg(long, default_value_t = 1)]
        repeat: u32,
        #[arg(long, default_value = "findings")]
        findings: PathBuf,
        /// Print every step and its reply.
        #[arg(long)]
        verbose: bool,
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
            jobs,
        } => fuzz_command(&seeds, &corpus, &findings, duration, jobs),
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

fn fuzz_command(
    seeds: &[PathBuf],
    corpus: &Path,
    findings: &Path,
    duration: Option<u64>,
    jobs: Option<usize>,
) -> Result<bool, String> {
    let env = Env::from_env()?;
    let jobs = jobs
        .or_else(|| std::thread::available_parallelism().ok().map(usize::from))
        .unwrap_or(1);
    std::thread::scope(|scope| {
        let workers: Vec<_> = (0..jobs)
            .map(|worker| {
                let env = &env;
                scope.spawn(move || {
                    fuzz(Campaign {
                        worker,
                        env,
                        seeds,
                        corpus: &corpus.join(worker.to_string()),
                        findings,
                        duration: duration.map(Duration::from_secs),
                    })
                    .map_err(|e| format!("worker {worker}: {e}"))
                })
            })
            .collect();
        workers
            .into_iter()
            .map(|worker| worker.join().expect("workers do not panic"))
            .collect::<Result<Vec<()>, String>>()
    })?;
    Ok(false)
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
    println!("reducing {}", finding.signature);
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
    let mut runs_per_edge: BTreeMap<u32, u32> = BTreeMap::new();
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
            *runs_per_edge.entry(edge).or_default() += 1;
        }
        for finding in findings {
            found.entry(finding).or_insert_with(|| (0, trace.clone())).0 += 1;
        }
    }
    if repeat > 1 {
        let stable = runs_per_edge
            .values()
            .filter(|&&runs| runs == repeat)
            .count();
        println!(
            "edges: {} in any run, {stable} in every run",
            runs_per_edge.len()
        );
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
            "{reproduced}/{repeat} {} -> {}",
            finding.signature,
            dir.display()
        );
    }
    Ok(!found.is_empty())
}
