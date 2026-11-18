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

use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use redisearch_fuzz::RediSearch;
use redisearch_fuzz::server::Env;

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: procfuzz::cli::Command,
    #[arg(long, env = "REDIS_SERVER")]
    redis_server: PathBuf,
    #[arg(long, env = "REDISEARCH_MODULE")]
    module: PathBuf,
    /// RedisJSON.
    #[arg(long, env = "REJSON_MODULE")]
    rejson: PathBuf,
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
    let cli = Cli::parse();
    let env = Env {
        redis_server: cli.redis_server,
        module: cli.module,
        rejson: cli.rejson,
    };
    match cli.command.exec(&RediSearch, &env.service(), &env) {
        Ok(false) => ExitCode::SUCCESS,
        Ok(true) => ExitCode::from(1),
        Err(e) => {
            log::error!("{e}");
            ExitCode::from(2)
        }
    }
}
