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
use procfuzz::commands::load;
use redisearch_fuzz::RediSearch;
use redisearch_fuzz::script::{export, import};
use redisearch_fuzz::server::Env;

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Command,
    #[arg(long, env = "REDIS_SERVER")]
    redis_server: Option<PathBuf>,
    #[arg(long, env = "REDISEARCH_MODULE")]
    module: Option<PathBuf>,
    /// RedisJSON.
    #[arg(long, env = "REJSON_MODULE")]
    rejson: Option<PathBuf>,
}

#[derive(Subcommand)]
enum Command {
    #[command(flatten)]
    Fuzzer(procfuzz::cli::Command),
    /// Print a program as commands for `redis-cli`.
    Export { program: PathBuf },
    /// Print a program of the commands in a file, one per line as `redis-cli`
    /// reads them or `MONITOR` prints them.
    Import { commands: PathBuf },
}

fn main() -> ExitCode {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info,libafl=warn"))
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
            let (Some(redis_server), Some(module), Some(rejson)) =
                (cli.redis_server, cli.module, cli.rejson)
            else {
                return Err(
                    "set --redis-server, --module, and --rejson, or source bin/fuzz/env.sh".into(),
                );
            };
            let env = Env {
                redis_server,
                module,
                rejson,
            };
            Ok(command.exec(&RediSearch, &env.service(), &env)?)
        }
        Command::Export { program } => {
            print!("{}", export(&load(&program)?)?);
            Ok(false)
        }
        Command::Import { commands } => {
            let program = import(&std::fs::read_to_string(commands)?)?;
            println!("{}", serde_json::to_string_pretty(&program)?);
            Ok(false)
        }
    }
}
