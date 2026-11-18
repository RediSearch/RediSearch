/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Fuzzes `kvd`. Exits with 0 without findings, 1 with findings, and 2 on
//! an error.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use kvd::{Kvd, service};

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: procfuzz::cli::Command,
    /// The `kvd` binary.
    #[arg(long, env = "KVD")]
    kvd: PathBuf,
}

fn main() -> ExitCode {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let cli = Cli::parse();
    match cli.command.exec(&Kvd, &service(cli.kvd.clone()), &cli.kvd) {
        Ok(false) => ExitCode::SUCCESS,
        Ok(true) => ExitCode::from(1),
        Err(e) => {
            log::error!("{e}");
            ExitCode::from(2)
        }
    }
}
