use std::path::PathBuf;

use clap::Parser;

#[derive(Parser)]
#[command(version, about)]
pub struct Options {
    /// Path to the baseline .so file. The responses from this module will be considered the source
    /// of truth.
    #[arg(short, long)]
    pub baseline_so: PathBuf,

    /// Path to the changeset .so file. The responses from this module should match the baseline's.
    #[arg(short, long)]
    pub changeset_so: PathBuf,

    /// Command to start up RLTest which will start a Redis server for each .so file.
    #[arg(short, long)]
    pub rltest_command: String,
}
