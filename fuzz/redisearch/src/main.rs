/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Stateful fuzzer for RediSearch. Exits with 0 without findings, 1 with
//! findings, and 2 on an error. `RUST_LOG=debug` logs every step.

use std::error::Error;
use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use redisearch_fuzz::RediSearch;
use redisearch_fuzz::script::import;
use redisearch_fuzz::server::Server;

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Command,
    #[command(flatten)]
    server: Server,
}

#[derive(Subcommand)]
enum Command {
    #[command(flatten)]
    Fuzzer(procfuzz::commands::Command),
    /// Print a program of the commands in a file, one per line as `redis-cli`
    /// reads them or `MONITOR` prints them.
    Import { commands: PathBuf },
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
    match exec(Cli::parse()) {
        Ok(false) => ExitCode::SUCCESS,
        Ok(true) => ExitCode::from(1),
        Err(e) => {
            log::error!("{e}");
            ExitCode::from(2)
        }
    }
}

/// Whether anything was found.
fn exec(cli: Cli) -> Result<bool, Box<dyn Error>> {
    match cli.command {
        Command::Fuzzer(command) => {
            cli.server.check()?;
            Ok(command.exec(&RediSearch, &cli.server.service(), &cli.server.setup())?)
        }
        Command::Import { commands } => {
            let program = import(&std::fs::read_to_string(commands)?)?;
            println!("{}", serde_json::to_string_pretty(&program)?);
            Ok(false)
        }
    }
}
