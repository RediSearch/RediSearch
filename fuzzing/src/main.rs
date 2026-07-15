/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! RediSearch Track B fuzzer — server-based, stateful, LibAFL-driven.
//!
//! LibAFL mutates a byte buffer; [`lower::decode`] turns it into a valid-but-deep
//! `FT.*` command sequence (the grammar's AST derives `Arbitrary`, so byte
//! mutations map to local grammar changes). The sequence is replayed against a
//! live `redis-server` with `redisearch.so` loaded. A semantic state-fingerprint
//! map drives LibAFL's feedback toward new server states; crashes/hangs are the
//! objective. Findings replay exactly from their saved input bytes.

mod artifact;
mod ast;
mod driver;
mod harness;
mod lower;
mod op;
mod oracles;
mod repro;
mod server;
mod shadow;
mod swarm;

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;

use artifact::render_cmd;
use driver::{Driver, Outcome};
use harness::ServerPool;
use server::ServerConfig;

#[derive(Parser, Debug)]
#[command(about = "Server-based, LibAFL-driven stateful fuzzer for RediSearch (Track B)")]
struct Args {
    /// Path to the built redisearch.so (prefer a SAN=address build).
    #[arg(long)]
    module: PathBuf,

    /// redis-server binary to launch.
    #[arg(long, default_value = "redis-server")]
    redis_server: String,

    /// Extra module load args, space-separated (e.g. "WORKERS 0").
    #[arg(long, default_value = "WORKERS 0")]
    module_args: String,

    /// Optional RedisJSON module to load alongside RediSearch, enabling
    /// `ON JSON` indexes and `JSON.SET` document ops.
    #[arg(long)]
    rejson: Option<PathBuf>,

    /// Base RNG seed for LibAFL. Random if omitted; printed for reproduction.
    #[arg(long)]
    seed: Option<u64>,

    /// Commands per sequence (upper bound; a short input yields fewer).
    #[arg(long, default_value_t = 48)]
    seq_len: usize,

    /// Corpus directory (favored inputs cached during the run).
    #[arg(long, default_value = "corpus")]
    corpus: PathBuf,

    /// Committed root corpus directory: byte buffers loaded as initial seeds at
    /// startup (see the `seeds/` set generated from the pytest workflows).
    #[arg(long, default_value = "seeds")]
    seeds: PathBuf,

    /// Where to write finding artifacts.
    #[arg(long, default_value = "findings")]
    out_dir: PathBuf,

    /// Per-command read timeout in seconds (hang detection).
    #[arg(long, default_value_t = 4)]
    timeout_secs: u64,

    /// Restart the server every N executions to bound leaks/state bleed.
    #[arg(long, default_value_t = 200)]
    restart_every: u64,

    /// Stop after this many fuzzing rounds (each round mutates one corpus entry
    /// through a full havoc stage = many server executions). 0 = run forever.
    #[arg(long, default_value_t = 0)]
    rounds: u64,

    /// Suppress raw-binary injection into query text. Use to stop the
    /// trivially-triggered UTF-8 decoder over-read from masking deeper bugs.
    #[arg(long)]
    safe_utf8: bool,

    /// Structurally malform ~1-in-N rendered commands (bad count-prefixes,
    /// dropped/duplicated/truncated args, garbage injection) to stress the
    /// argument parsers. 0 disables. Lower N = more malformation.
    #[arg(long, default_value_t = 6)]
    malform_rate: u32,

    /// Print every command as it is executed, with its outcome.
    #[arg(short, long)]
    verbose: bool,

    /// Disable structural (FT.INFO / FT.DEBUG GC_FORCEINVOKE) feedback probing.
    /// Probing deepens exploration but costs a few commands per execution.
    #[arg(long)]
    no_structure_feedback: bool,

    /// Disable inline auto-reduction of findings. By default each distinct
    /// finding is delta-debugged to a minimal reproducing sequence before it is
    /// saved; this pauses fuzzing briefly (once per distinct root cause).
    #[arg(long)]
    no_auto_reduce: bool,

    /// Disable config swarm. By default each server (re)start loads the module
    /// under a different randomized config regime (worker pool, GC policy,
    /// numeric/vector encodings, ...) to explore far more of the server; the
    /// regime is recorded per finding so crashes still replay.
    #[arg(long)]
    no_swarm: bool,

    /// Decode the input bytes for --seed and print the command sequence (no server).
    #[arg(long)]
    dump: bool,

    /// Decode a corpus/seed file and print the command sequence it lowers to (no
    /// server). Handy for inspecting any committed seed or corpus entry.
    #[arg(long)]
    dump_file: Option<PathBuf>,

    /// Preload an ASan runtime into redis-server (path to libclang_rt.asan_*).
    #[arg(long)]
    preload: Option<PathBuf>,

    /// ASAN_OPTIONS for the server process.
    #[arg(
        long,
        default_value = "abort_on_error=1:halt_on_error=1:detect_odr_violation=0"
    )]
    asan_options: String,
}

/// Everything the harness needs, assembled from CLI args.
pub struct Cfg {
    pub server: ServerConfig,
    pub seq_len: usize,
    pub timeout: Duration,
    pub grace: Duration,
    pub restart_every: u64,
    pub rounds: u64,
    pub json_available: bool,
    pub safe_utf8: bool,
    pub malform_rate: u32,
    pub verbose: bool,
    pub structure_feedback: bool,
    pub auto_reduce: bool,
    pub swarm: bool,
    pub out_dir: PathBuf,
    pub corpus: PathBuf,
    pub seeds: PathBuf,
    pub seed: u64,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let timeout = Duration::from_secs(args.timeout_secs);
    let grace = Duration::from_secs(12);

    let mut env = vec![("ASAN_OPTIONS".to_string(), args.asan_options.clone())];
    if let Some(p) = &args.preload {
        let var = if cfg!(target_os = "macos") {
            "DYLD_INSERT_LIBRARIES"
        } else {
            "LD_PRELOAD"
        };
        env.push((var.to_string(), p.display().to_string()));
    }
    let mut extra_modules = Vec::new();
    if let Some(rejson) = &args.rejson {
        extra_modules.push((rejson.clone(), Vec::new()));
    }
    let server = ServerConfig {
        redis_server: args.redis_server.clone(),
        module: args.module.clone(),
        module_args: args
            .module_args
            .split_whitespace()
            .map(str::to_string)
            .collect(),
        extra_modules,
        env,
    };

    // --dump-file needs no server: decode a committed seed/corpus file.
    if let Some(path) = &args.dump_file {
        let bytes = std::fs::read(path)?;
        let cmds = lower::decode(
            &bytes,
            args.seq_len,
            args.rejson.is_some(),
            args.safe_utf8,
            args.malform_rate,
        );
        for (j, cmd) in cmds.iter().enumerate() {
            println!("[{j}] {}", render_cmd(cmd));
        }
        return Ok(());
    }

    // --dump needs no server.
    if args.dump {
        let seed = args.seed.unwrap_or(0);
        let bytes = harness::seed_bytes(seed);
        let cmds = lower::decode(
            &bytes,
            args.seq_len,
            args.rejson.is_some(),
            args.safe_utf8,
            args.malform_rate,
        );
        for (j, cmd) in cmds.iter().enumerate() {
            println!("[{j}] {}", render_cmd(cmd));
        }
        return Ok(());
    }

    // Probe RedisJSON availability once.
    let json_available = if args.rejson.is_some() {
        let mut pool = ServerPool::new(&server, timeout, grace)?;
        let has = pool.driver().has_json();
        pool.stop();
        has
    } else {
        false
    };

    let seed = args.seed.unwrap_or(0xC0FFEE);
    let cfg = Cfg {
        server,
        seq_len: args.seq_len,
        timeout,
        grace,
        restart_every: args.restart_every,
        rounds: args.rounds,
        json_available,
        safe_utf8: args.safe_utf8,
        malform_rate: args.malform_rate,
        verbose: args.verbose,
        structure_feedback: !args.no_structure_feedback,
        auto_reduce: !args.no_auto_reduce,
        swarm: !args.no_swarm,
        out_dir: args.out_dir.clone(),
        corpus: args.corpus.clone(),
        seeds: args.seeds.clone(),
        seed,
    };
    std::fs::create_dir_all(&cfg.out_dir)?;
    std::fs::create_dir_all(&cfg.corpus)?;
    println!(
        "Track B (LibAFL): seed={seed} seq_len={} json={json_available} safe_utf8={}",
        cfg.seq_len, cfg.safe_utf8
    );

    harness::run(cfg)
}

/// Verbose per-command execution used by --verbose runs.
pub fn exec_verbose(drv: &mut Driver, cmd: &op::Cmd) -> Outcome {
    let outcome = drv.exec(cmd);
    let tag = match &outcome {
        Outcome::Reply(_) => "OK",
        Outcome::ServerError(_) => "ERR",
        Outcome::Timeout(_) => "TIMEOUT",
        Outcome::Disconnected(_) => "DISCONNECT",
    };
    println!("  {} => {tag}", render_cmd(cmd));
    outcome
}
