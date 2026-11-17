/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand};

use redisearch_fuzz::judge::Finding;
use redisearch_fuzz::program::Program;
use redisearch_fuzz::target::Env;
use redisearch_fuzz::{bundle, judge, run};

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
        /// Run the program this many times and report how often each finding reproduces.
        #[arg(long, default_value_t = 1)]
        repeat: u32,
        /// Directory that receives a bundle per distinct finding.
        #[arg(long, default_value = "findings")]
        findings: PathBuf,
        /// Print every command and its reply.
        #[arg(long)]
        verbose: bool,
    },
}

fn main() -> ExitCode {
    match Cli::parse().command {
        Command::Run {
            program,
            repeat,
            findings,
            verbose,
        } => match run_command(&program, repeat, &findings, verbose) {
            Ok(found) if found => ExitCode::from(1),
            Ok(_) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("error: {e}");
                ExitCode::from(2)
            }
        },
    }
}

fn run_command(
    path: &Path,
    repeat: u32,
    findings_dir: &Path,
    verbose: bool,
) -> Result<bool, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let program = Program::from_json(&text).map_err(|e| format!("{}: {e}", path.display()))?;
    let env = Env::from_env()?;

    let mut counts: BTreeMap<Finding, u32> = BTreeMap::new();
    let mut every_run: Option<BTreeSet<u32>> = None;
    let mut any_run: BTreeSet<u32> = BTreeSet::new();
    for attempt in 1..=repeat {
        let trace = run::run(&program, &env);
        let edges: BTreeSet<u32> = trace.coverage.edges.iter().copied().collect();
        every_run = Some(match every_run {
            Some(seen) => seen.intersection(&edges).copied().collect(),
            None => edges.clone(),
        });
        any_run.extend(edges);
        if verbose {
            for step in &trace.steps {
                println!("  [{}] {:?} -> {:?}", step.op, step.argv, step.reply);
            }
        }
        let findings = judge::judge(&program, &trace);
        println!(
            "run {attempt}/{repeat}: exit {:?}, {} steps, {}/{} edges, {} findings",
            trace.exit,
            trace.steps.len(),
            trace.coverage.edges.len(),
            trace.coverage.guards,
            findings.len()
        );
        if let run::Exit::StartupFailed { reason } = &trace.exit {
            return Err(format!("server failed to start:\n{reason}"));
        }
        for finding in findings {
            if !counts.contains_key(&finding) {
                let dir = bundle::write(findings_dir, &program, &trace, &finding, &env)?;
                println!(
                    "  new {:?} {} -> {}",
                    finding.kind,
                    finding.signature,
                    dir.display()
                );
            }
            *counts.entry(finding).or_default() += 1;
        }
    }
    if repeat > 1 && !any_run.is_empty() {
        let stable = every_run.map_or(0, |edges| edges.len());
        println!(
            "edges: {} in any run, {stable} in every run, {} unstable",
            any_run.len(),
            any_run.len() - stable
        );
    }
    for (finding, count) in &counts {
        println!("{count}/{repeat} {:?} {}", finding.kind, finding.signature);
        bundle::record_rate(findings_dir, finding, *count, repeat)?;
    }
    Ok(!counts.is_empty())
}
