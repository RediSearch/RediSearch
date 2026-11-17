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

use clap::{Parser, Subcommand};

use redisearch_fuzz::bundle;
use redisearch_fuzz::judge::{Finding, judge};
use redisearch_fuzz::program::Program;
use redisearch_fuzz::run::{Trace, run};
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
}

fn main() -> ExitCode {
    let Command::Run {
        program,
        repeat,
        findings,
        verbose,
    } = Cli::parse().command;
    match run_command(&program, repeat, &findings, verbose) {
        Ok(false) => ExitCode::SUCCESS,
        Ok(true) => ExitCode::from(1),
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::from(2)
        }
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

    let mut found: BTreeMap<Finding, (u32, Trace)> = BTreeMap::new();
    let mut edges: BTreeMap<u32, u32> = BTreeMap::new();
    for attempt in 1..=repeat {
        let trace = run(&program, &env)?;
        if verbose {
            for step in &trace.steps {
                println!("  [{}] {:?} -> {:?}", step.op, step.argv, step.reply);
            }
        }
        let findings = judge(&program, &trace);
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
            &program,
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
