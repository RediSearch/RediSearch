/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The LibAFL harness: a custom `Executor` that lowers a mutated byte buffer to
//! a command sequence, replays it against a live server, records a semantic
//! state fingerprint (for feedback that rewards reaching new server states), and
//! classifies crashes/hangs as objectives. LibAFL owns mutation, corpus, and
//! scheduling; this module owns everything server- and finding-specific.
//!
//! It also hosts the engine-independent pieces reused by `--replay`/`--minimize`:
//! [`ServerPool`] and [`replay_detect`].

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::time::Duration;

use anyhow::Result;
use libafl::corpus::{InMemoryOnDiskCorpus, OnDiskCorpus};
use libafl::events::SimpleEventManager;
use libafl::executors::{Executor, ExitKind, HasObservers};
use libafl::feedback_or_fast;
use libafl::feedbacks::{CrashFeedback, MaxMapFeedback, TimeoutFeedback};
use libafl::inputs::{BytesInput, HasTargetBytes};
use libafl::monitors::SimpleMonitor;
use libafl::mutators::{havoc_mutations, HavocScheduledMutator};
use libafl::observers::{CanTrack, ExplicitTracking, StdMapObserver};
use libafl::schedulers::{IndexesLenTimeMinimizerScheduler, QueueScheduler};
use libafl::stages::StdMutationalStage;
use libafl::state::{HasExecutions, StdState};
use libafl::{Evaluator, Fuzzer, StdFuzzer};
use libafl_bolts::ownedref::OwnedMutSlice;
use libafl_bolts::rands::StdRand;
use libafl_bolts::tuples::{tuple_list, RefIndexable};
use libafl_bolts::AsSlice;

use crate::artifact::{render_cmd, Artifact};
use crate::driver::{cursor_id, Driver, Outcome};
use crate::op::Cmd;
use crate::oracles::Finding;
use crate::server::{Server, ServerConfig};
use crate::Cfg;

/// Size of the semantic state-fingerprint map. Each reached "state feature" sets
/// one byte; `MaxMapFeedback` keeps any input that lights a new one.
const MAP_SIZE: usize = 1 << 16;

/// Force a GC cycle in the structural probe only every Nth execution. Forcing a
/// synchronous fork-based GC every execution is the dominant per-exec cost, and
/// the grammar already exercises GC through its own `FT.DEBUG` commands.
const GC_PROBE_EVERY: u64 = 16;

/// The state-map observer with index + novelty tracking, as required by the
/// minimizer scheduler and novelty feedback.
type StateObserver = ExplicitTracking<StdMapObserver<'static, u8, false>, true, true>;

/// Derive a stable seed byte buffer for --dump.
pub fn seed_bytes(seed: u64) -> Vec<u8> {
    // A spread of bytes so --dump / initial seeds produce non-trivial sequences.
    let mut v = Vec::with_capacity(1024);
    let mut x = seed | 1;
    for _ in 0..1024 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        v.push((x & 0xff) as u8);
    }
    v
}

/// Config params the grammar can `FT.CONFIG SET`. Their startup defaults are
/// snapshotted and restored on every reset, so a `SET` in one sequence never
/// bleeds into the next — keeping findings self-contained and reproducible.
// Modern (`search-*`) config names for snapshot/restore, so our own machinery
// uses the non-deprecated `CONFIG GET/SET` (no "FT.CONFIG is deprecated" log
// spam). Same backing store as the grammar's `FT.CONFIG SET`, so restore still
// undoes a fuzzed change.
const CONFIG_PARAMS: &[&str] = &[
    "search-timeout",
    "search-min-prefix",
    "search-max-prefix-expansions",
    "search-default-dialect",
    "search-fork-gc-run-interval",
    "search-fork-gc-clean-threshold",
    "search-max-aggregate-results",
    "search-max-doctablesize",
    "search-on-timeout",
];

/// A managed server + connected driver that recycles itself on crash/restart.
pub struct ServerPool {
    cfg: ServerConfig,
    timeout: Duration,
    grace: Duration,
    /// When set, each (re)start applies a fresh swarm config derived from
    /// `swarm_seed ^ restart_count`. When `None`, a fixed config is applied
    /// (`pinned_config`) — used to reproduce a finding under its exact regime.
    swarm_seed: Option<u64>,
    /// A fixed config applied on every (re)start when not swarming (replay/reduce).
    pinned_config: Vec<(String, String)>,
    restart_count: u64,
    current_config: Vec<(String, String)>,
    server: Server,
    driver: Driver,
    config_defaults: Vec<(String, Vec<u8>)>,
    /// Set when a sequence issues `FT.CONFIG SET`; drives the next reset's
    /// config restore. Almost no sequence touches config, so gating on this
    /// avoids 10 `FT.CONFIG SET` commands on every reset.
    config_dirty: bool,
}

impl ServerPool {
    /// A pool applying a fixed startup config on each (re)start (default: none,
    /// i.e. the server's own defaults). Used for replay/reduce with a pinned regime.
    pub fn new(cfg: &ServerConfig, timeout: Duration, grace: Duration) -> Result<ServerPool> {
        Self::build(cfg, timeout, grace, None, Vec::new())
    }

    /// A pool with a fixed startup config applied on each (re)start.
    pub fn with_config(
        cfg: &ServerConfig,
        timeout: Duration,
        grace: Duration,
        config: Vec<(String, String)>,
    ) -> Result<ServerPool> {
        Self::build(cfg, timeout, grace, None, config)
    }

    /// A pool that applies a fresh swarm config on each (re)start from `seed`.
    pub fn with_swarm(
        cfg: &ServerConfig,
        timeout: Duration,
        grace: Duration,
        seed: u64,
    ) -> Result<ServerPool> {
        Self::build(cfg, timeout, grace, Some(seed), Vec::new())
    }

    fn build(
        cfg: &ServerConfig,
        timeout: Duration,
        grace: Duration,
        swarm_seed: Option<u64>,
        pinned_config: Vec<(String, String)>,
    ) -> Result<ServerPool> {
        let server = Server::start(cfg)?;
        let driver = Driver::connect(&server.url(), timeout)?;
        let mut pool = ServerPool {
            cfg: cfg.clone(),
            timeout,
            grace,
            swarm_seed,
            pinned_config,
            restart_count: 0,
            current_config: Vec::new(),
            server,
            driver,
            config_defaults: Vec::new(),
            config_dirty: false,
        };
        pool.boot()?;
        Ok(pool)
    }

    /// (Re)connect to the current server, apply the startup config for this
    /// instance, then snapshot config defaults (post-config, so restore returns
    /// to the swarmed regime, not factory).
    fn boot(&mut self) -> Result<()> {
        self.driver = Driver::connect(&self.server.url(), self.timeout)?;
        self.current_config = match self.swarm_seed {
            Some(seed) => {
                crate::swarm::swarm_config(seed ^ self.restart_count.wrapping_mul(0x100_0001))
            }
            None => self.pinned_config.clone(),
        };
        for (k, v) in &self.current_config {
            self.driver.config_set(k, v); // best-effort; immutable/invalid ignored
        }
        self.config_defaults = self.driver.config_snapshot(CONFIG_PARAMS);
        Ok(())
    }

    pub fn driver(&mut self) -> &mut Driver {
        &mut self.driver
    }

    /// The `search-*` config values applied to the current server, for recording
    /// alongside a finding so it replays under the same regime.
    pub fn current_config(&self) -> &[(String, String)] {
        &self.current_config
    }

    /// Note that the just-replayed sequence changed config, so the next reset
    /// restores defaults.
    pub fn mark_config_dirty(&mut self) {
        self.config_dirty = true;
    }

    /// Reset per-sequence state: drop indexes, flush keys, and — only if the
    /// previous sequence changed config — restore config defaults.
    pub fn reset(&mut self) {
        self.driver.reset();
        if self.config_dirty {
            self.driver.config_restore(&self.config_defaults);
            self.config_dirty = false;
        }
    }

    pub fn restart(&mut self) -> Result<()> {
        self.server.stop();
        self.restart_count += 1;
        self.server = Server::start(&self.cfg)?;
        self.boot()
    }

    pub fn stop(&mut self) {
        self.server.stop();
    }

    /// Execute one command with crash/hang classification. `Err(finding)` on a
    /// crash or hang; `Ok(outcome)` otherwise (reconnecting on a recoverable
    /// drop). Shared by the fuzz executor, introspection probes, and the reducer,
    /// so a crash is caught identically everywhere.
    fn step(&mut self, cmd: &Cmd, grace: Duration, verbose: bool) -> Result<Outcome, Finding> {
        let outcome = if verbose {
            crate::exec_verbose(&mut self.driver, cmd)
        } else {
            self.driver.exec(cmd)
        };
        match &outcome {
            Outcome::Reply(_) | Outcome::ServerError(_) => Ok(outcome),
            Outcome::Timeout(_) => Err(if self.server.wait_exit(grace) {
                Finding::Crash {
                    log: self.server.read_full_log(),
                }
            } else {
                Finding::Hang {
                    log: self.server.read_full_log(),
                }
            }),
            Outcome::Disconnected(_) => {
                // Dead, exiting, or unrecoverable → crash; otherwise reconnect.
                let dead = !self.server.is_alive() || self.server.wait_exit(grace);
                if dead || self.restart().is_err() {
                    Err(Finding::Crash {
                        log: self.server.read_full_log(),
                    })
                } else {
                    Ok(outcome)
                }
            }
        }
    }

    /// Reset, replay a fixed concrete sequence, and return any finding. Restores
    /// the server after a crash so the pool is reusable for the next candidate —
    /// this is what lets the reducer run many candidates against one server.
    fn replay_seq(&mut self, cmds: &[Cmd], grace: Duration) -> Option<Finding> {
        self.reset();
        let mut live_cursors: Vec<i64> = Vec::new();
        let mut crashed = false;
        let mut result = None;
        for cmd in cmds {
            let cmd = resolve_cursor(cmd, &live_cursors);
            match self.step(&cmd, grace, false) {
                Ok(Outcome::Reply(v)) => {
                    if produces_cursor(&cmd) {
                        if let Some(id) = cursor_id(&v) {
                            live_cursors.push(id);
                        }
                    }
                }
                Ok(_) => {}
                Err(f) => {
                    result = Some(f);
                    crashed = true;
                    break;
                }
            }
        }
        if result.is_none() {
            if !self.server.is_alive() {
                result = Some(Finding::Crash {
                    log: self.server.read_full_log(),
                });
                crashed = true;
            } else if !self.driver.ping() {
                match recheck(self) {
                    Verdict::Recovered => {}
                    Verdict::Crashed => {
                        result = Some(Finding::Crash {
                            log: self.server.read_full_log(),
                        });
                        crashed = true;
                    }
                    Verdict::Hung => {
                        result = Some(Finding::Hang {
                            log: self.server.read_full_log(),
                        });
                    }
                }
            }
        }
        // A crash leaves the server dead; bring it back for the next candidate.
        if crashed {
            let _ = self.restart();
        }
        result
    }
}

/// Compute a semantic state fingerprint hash for a feature and fold it into map.
fn mark(map: &mut [u8], feature: impl Hash) {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    feature.hash(&mut h);
    let idx = (h.finish() as usize) % map.len();
    map[idx] = map[idx].saturating_add(1);
}

/// The custom executor. Owns the state-map observer (LibAFL reads it for
/// feedback) and the server pool (it replays each input).
struct RsExecutor {
    observers: (StateObserver, ()),
    map_ptr: *mut u8,
    pool: ServerPool,
    cfg_seq_len: usize,
    json: bool,
    safe_utf8: bool,
    malform_rate: u32,
    verbose: bool,
    structure_feedback: bool,
    grace: Duration,
    timeout: Duration,
    execs: u64,
    restart_every: u64,
    // finding bookkeeping
    server_cfg: ServerConfig,
    auto_reduce: bool,
    out_dir: std::path::PathBuf,
    seen: HashMap<String, u64>,
    findings: u64,
    last_report: std::time::Instant,
}

impl RsExecutor {
    /// Execute one command with crash/hang classification, recording it verbosely
    /// when enabled. Delegates the classification to [`ServerPool::step`].
    fn step(&mut self, cmd: &Cmd) -> Result<Outcome, Finding> {
        self.pool.step(cmd, self.grace, self.verbose)
    }

    fn replay(&mut self, cmds: &[Cmd]) -> (Option<Finding>, Vec<Cmd>, Vec<Behavior>) {
        let mut live_cursors: Vec<i64> = Vec::new();
        let mut concrete: Vec<Cmd> = Vec::with_capacity(cmds.len());
        let mut behaviors: Vec<Behavior> = Vec::with_capacity(cmds.len());
        for cmd in cmds {
            let cmd = resolve_cursor(cmd, &live_cursors);
            concrete.push(cmd.clone());
            match self.step(&cmd) {
                Ok(outcome) => {
                    behaviors.push(behavior(&cmd, &outcome));
                    if let Outcome::Reply(v) = &outcome {
                        if produces_cursor(&cmd) {
                            if let Some(id) = cursor_id(v) {
                                if !live_cursors.contains(&id) {
                                    live_cursors.push(id);
                                }
                            }
                        }
                    }
                }
                Err(f) => return (Some(f), concrete, behaviors),
            }
        }
        // Post-sequence health check.
        if !self.pool.server.is_alive() {
            return (
                Some(Finding::Crash {
                    log: self.pool.server.read_full_log(),
                }),
                concrete,
                behaviors,
            );
        }
        if !self.pool.driver().ping() {
            let f = match recheck(&mut self.pool) {
                Verdict::Recovered => None,
                Verdict::Crashed => Some(Finding::Crash {
                    log: self.pool.server.read_full_log(),
                }),
                Verdict::Hung => Some(Finding::Hang {
                    log: self.pool.server.read_full_log(),
                }),
            };
            return (f, concrete, behaviors);
        }
        (None, concrete, behaviors)
    }

    /// Introspect live-index internal state via crash-checked `FT.INFO` (always)
    /// and `FT.DEBUG GC_FORCEINVOKE` (only when `force_gc`). A crash/hang here is
    /// a finding; otherwise it yields structural counters and per-probe behaviors
    /// that feed the reward.
    fn probe(&mut self, force_gc: bool) -> (Option<Finding>, Vec<Behavior>, Vec<(String, u64)>) {
        let mut behaviors = Vec::new();
        let mut feats = Vec::new();

        let list_cmd: Cmd = vec![b"FT._LIST".to_vec()];
        let names = match self.step(&list_cmd) {
            Ok(o) => {
                behaviors.push(behavior(&list_cmd, &o));
                match o {
                    Outcome::Reply(v) => reply_strings(&v),
                    _ => Vec::new(),
                }
            }
            Err(f) => return (Some(f), behaviors, feats),
        };

        for name in names.iter().take(4) {
            let info_cmd: Cmd = vec![b"FT.INFO".to_vec(), name.clone().into_bytes()];
            match self.step(&info_cmd) {
                Ok(o) => {
                    behaviors.push(behavior(&info_cmd, &o));
                    if let Outcome::Reply(v) = &o {
                        feats.extend(info_fields_from(v, INFO_FIELDS));
                    }
                }
                Err(f) => return (Some(f), behaviors, feats),
            }
            // Periodically force a GC cycle: exercises the deferred-GC path and
            // catches introspection-triggered corruption. A crash here is a
            // finding. Skipped most executions — the fork is the dominant cost.
            if force_gc {
                let gc_cmd: Cmd = vec![
                    b"FT.DEBUG".to_vec(),
                    b"GC_FORCEINVOKE".to_vec(),
                    name.clone().into_bytes(),
                ];
                match self.step(&gc_cmd) {
                    Ok(o) => behaviors.push(behavior(&gc_cmd, &o)),
                    Err(f) => return (Some(f), behaviors, feats),
                }
            }
        }
        (None, behaviors, feats)
    }

    fn record(&mut self, finding: &Finding, concrete: &[Cmd]) {
        let sig = finding.signature();
        let count = self.seen.entry(sig.clone()).or_insert(0);
        *count += 1;
        if *count != 1 {
            return; // already reported this root cause
        }
        self.findings += 1;

        // The crash happened under this (possibly swarmed) config; pin reduction
        // and validation to it.
        let config = self.pool.current_config().to_vec();
        let repro = |cmds: &[Cmd]| {
            replay_detect(
                &self.server_cfg,
                cmds,
                config.clone(),
                self.timeout,
                self.grace,
            )
            .map(|f| f.signature() == sig)
            .unwrap_or(false)
        };

        // Auto-reduce on first sighting of a distinct signature (isolated server,
        // fast: FLUSHALL between candidates). This search can over-reduce for
        // state-dependent bugs, so the result is validated below.
        let reduced: Vec<Cmd> = if self.auto_reduce {
            match ServerPool::with_config(
                &self.server_cfg,
                self.timeout,
                self.grace,
                config.clone(),
            ) {
                Ok(mut rp) => {
                    let r = reduce(&mut rp, concrete.to_vec(), &sig, self.grace, false);
                    rp.stop();
                    r
                }
                Err(_) => concrete.to_vec(),
            }
        } else {
            concrete.to_vec()
        };

        // Validation gate: a saved artifact must reproduce on a *fresh* server
        // (the true standalone condition). Prefer the reduced form; fall back to
        // the original if reduction was unsound; flag if neither reproduces
        // standalone (the bug needs cross-sequence campaign state we don't yet
        // capture).
        let (cmds, standalone) = if repro(&reduced) {
            (reduced, true)
        } else if reduced.len() != concrete.len() && repro(concrete) {
            (concrete.to_vec(), true)
        } else {
            (concrete.to_vec(), false)
        };

        // The artifact is a self-contained, runnable Python reproduction script.
        let artifact = Artifact::from_commands(&cmds, finding.kind(), &config, standalone);
        let key_log = crate::repro::key_log_lines(finding.log());
        let script =
            crate::repro::render_script(&artifact, &sig, &key_log, &self.server_cfg.module_args);
        let stem = format!("{}-{}", finding.kind(), self.findings);
        let path = self.out_dir.join(format!("{stem}.py"));
        let _ = std::fs::write(&path, script);
        // Save the full server log (with the ASan/assert report) alongside the
        // script. The finding already captured it at crash time.
        if !finding.log().is_empty() {
            let _ = std::fs::write(self.out_dir.join(format!("{stem}.log")), finding.log());
        }
        let flag = if standalone {
            ""
        } else {
            " (NOT standalone-reproducible)"
        };
        eprintln!(
            "[FINDING #{}] kind={} sig=[{sig}] {} commands{flag} -> {}",
            self.findings,
            finding.kind(),
            cmds.len(),
            path.display()
        );
    }
}

// SAFETY: the map buffer outlives the observer; only the executor thread touches it.
impl<EM, S, Z> Executor<EM, BytesInput, S, Z> for RsExecutor
where
    S: HasExecutions,
{
    fn run_target(
        &mut self,
        _fuzzer: &mut Z,
        state: &mut S,
        _mgr: &mut EM,
        input: &BytesInput,
    ) -> Result<ExitKind, libafl::Error> {
        self.execs += 1;
        *state.executions_mut() += 1;
        if self.restart_every > 0 && self.execs.is_multiple_of(self.restart_every) {
            let _ = self.pool.restart();
        }

        // Reset the state map for this run.
        // SAFETY: map_ptr/len match the observer's owned buffer for the run.
        let map = unsafe { std::slice::from_raw_parts_mut(self.map_ptr, MAP_SIZE) };
        for b in map.iter_mut() {
            *b = 0;
        }

        let bytes = input.target_bytes();
        let cmds = crate::lower::decode(
            bytes.as_slice(),
            self.cfg_seq_len,
            self.json,
            self.safe_utf8,
            self.malform_rate,
        );

        self.pool.reset();
        let (mut finding, concrete, mut behaviors) = self.replay(&cmds);
        if concrete.iter().any(is_config_set) {
            self.pool.mark_config_dirty();
        }

        // Introspect internal index state, crash-checked. A crash/hang during
        // FT.INFO / FT.DEBUG GC_FORCEINVOKE is itself a finding; otherwise the
        // structural counters (bucketed) and per-probe outcomes feed the reward,
        // rewarding reaching new index shapes — deeper than reply shape alone.
        // FT.INFO runs every execution (read-only, cheap); the synchronous
        // fork-based GC (GC_FORCEINVOKE) runs only periodically — it is the
        // dominant cost, and the grammar also exercises GC via its own commands.
        let mut structural: Vec<(String, u64)> = Vec::new();
        if finding.is_none() && self.structure_feedback {
            let force_gc = self.execs.is_multiple_of(GC_PROBE_EVERY);
            let (probe_finding, probe_behaviors, feats) = self.probe(force_gc);
            behaviors.extend(probe_behaviors);
            structural = feats;
            finding = probe_finding;
        }

        fingerprint(map, &behaviors);
        for (key, val) in &structural {
            mark(map, ("info", key, val.next_power_of_two()));
        }

        let exit = match &finding {
            Some(Finding::Crash { .. }) => ExitKind::Crash,
            Some(Finding::Hang { .. }) => ExitKind::Timeout,
            _ => ExitKind::Ok,
        };
        if let Some(f) = &finding {
            self.record(f, &concrete);
            // Recover the server so fuzzing continues.
            let _ = self.pool.restart();
        }

        if self.last_report.elapsed() >= Duration::from_secs(10) {
            self.last_report = std::time::Instant::now();
            eprintln!(
                "  [harness] execs={} distinct_findings={} corpus-state-map active",
                self.execs, self.findings
            );
        }
        Ok(exit)
    }
}

impl HasObservers for RsExecutor {
    type Observers = (StateObserver, ());
    fn observers(&self) -> RefIndexable<&Self::Observers, Self::Observers> {
        RefIndexable::from(&self.observers)
    }
    fn observers_mut(&mut self) -> RefIndexable<&mut Self::Observers, Self::Observers> {
        RefIndexable::from(&mut self.observers)
    }
}

/// One command's observed behavior: the verb, an outcome tag, and a detail hash
/// (error class, or result-count bucket for a successful search/aggregate).
struct Behavior {
    verb: Vec<u8>,
    tag: u8,
    detail: u64,
}

const OUT_OK: u8 = 0;
const OUT_ERR: u8 = 1;
const OUT_TIMEOUT: u8 = 2;
const OUT_DISCONNECT: u8 = 3;

/// Classify a command's outcome into a behavior. This is the *depth* signal:
/// distinct error classes and result-count regimes are distinct behaviors, so
/// the feedback rewards exploring error paths, not only the happy path.
fn behavior(cmd: &Cmd, outcome: &Outcome) -> Behavior {
    let verb = cmd.first().cloned().unwrap_or_default();
    let (tag, detail) = match outcome {
        Outcome::Reply(v) => (OUT_OK, reply_detail(cmd, v)),
        Outcome::ServerError(e) => (OUT_ERR, error_class(e)),
        Outcome::Timeout(_) => (OUT_TIMEOUT, 0),
        Outcome::Disconnected(_) => (OUT_DISCONNECT, 0),
    };
    Behavior { verb, tag, detail }
}

/// A hash of the error's leading tokens (its class, e.g. `Unknown argument`),
/// so different error kinds are different behaviors but instance detail is ignored.
fn error_class(e: &str) -> u64 {
    let class: String = e.split_whitespace().take(3).collect::<Vec<_>>().join(" ");
    let mut h = std::collections::hash_map::DefaultHasher::new();
    class.hash(&mut h);
    h.finish()
}

/// For a successful `FT.SEARCH`/`FT.AGGREGATE`, the result-count bucket (the
/// first array element is the total). Other replies contribute a constant.
fn reply_detail(cmd: &Cmd, v: &redis::Value) -> u64 {
    let verb = cmd.first().map(|x| x.as_slice()).unwrap_or(b"");
    if verb == b"FT.SEARCH" || verb == b"FT.AGGREGATE" {
        if let redis::Value::Array(items) = v {
            if let Some(redis::Value::Int(n)) = items.first() {
                return (*n as u64).next_power_of_two();
            }
        }
    }
    0
}

/// Fold the per-command behavior trace into the state map. Features that recur
/// collapse to the same byte; reaching a *new* (verb, outcome, detail) is what
/// `MaxMapFeedback` rewards.
fn fingerprint(map: &mut [u8], behaviors: &[Behavior]) {
    mark(map, ("ncmds", behaviors.len().next_power_of_two()));
    for b in behaviors {
        mark(map, ("verb", &b.verb));
        mark(map, ("outcome", &b.verb, b.tag, b.detail));
    }
}

/// Structural `FT.INFO` counters worth rewarding new regimes of. Restricted to
/// *stable, deterministic* fields — no memory addresses, byte sizes, or timings,
/// which background GC/fork would make noisy and saturate the map.
const INFO_FIELDS: &[&str] = &[
    "num_docs",
    "num_terms",
    "num_records",
    "max_doc_id",
    "total_inverted_index_blocks",
    "number_of_uses",
    "cleaning",
    "num_fields",
];

/// Parse a RESP array reply (e.g. `FT._LIST`) into its bulk-string elements.
fn reply_strings(v: &redis::Value) -> Vec<String> {
    match v {
        redis::Value::Array(items) => items
            .iter()
            .filter_map(|it| match it {
                redis::Value::BulkString(b) => std::str::from_utf8(b).ok().map(str::to_string),
                redis::Value::SimpleString(s) => Some(s.clone()),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// Extract the wanted `FT.INFO` fields from its flat `[k, v, ...]` reply, keeping
/// only values that parse as non-negative integers.
fn info_fields_from(v: &redis::Value, wanted: &[&str]) -> Vec<(String, u64)> {
    let redis::Value::Array(items) = v else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut i = 0;
    while i + 1 < items.len() {
        if let redis::Value::BulkString(k) = &items[i] {
            if let Ok(key) = std::str::from_utf8(k) {
                if wanted.contains(&key) {
                    if let Some(n) = crate::driver::value_as_u64(&items[i + 1]) {
                        out.push((key.to_string(), n));
                    }
                }
            }
        }
        i += 2;
    }
    out
}

pub fn run(cfg: Cfg) -> Result<()> {
    let mut map_buf = vec![0u8; MAP_SIZE];
    let map_ptr = map_buf.as_mut_ptr();
    // The observer owns the map; the executor writes through map_ptr.
    let observer = unsafe {
        StdMapObserver::from_ownedref(
            "state",
            OwnedMutSlice::from_raw_parts_mut(map_ptr, MAP_SIZE),
        )
    }
    .track_indices()
    .track_novelties();

    let mut feedback = MaxMapFeedback::new(&observer);
    let mut objective = feedback_or_fast!(CrashFeedback::new(), TimeoutFeedback::new());

    let corpus = InMemoryOnDiskCorpus::new(cfg.corpus.clone())
        .map_err(|e| anyhow::anyhow!("corpus: {e}"))?;
    let solutions = OnDiskCorpus::new(cfg.out_dir.join("solutions"))
        .map_err(|e| anyhow::anyhow!("sol: {e}"))?;

    let mut state = StdState::new(
        StdRand::with_seed(cfg.seed),
        corpus,
        solutions,
        &mut feedback,
        &mut objective,
    )
    .map_err(|e| anyhow::anyhow!("state: {e}"))?;

    let monitor = SimpleMonitor::new(|s| println!("{s}"));
    let mut mgr = SimpleEventManager::new(monitor);

    let scheduler = IndexesLenTimeMinimizerScheduler::new(&observer, QueueScheduler::new());
    let mut fuzzer = StdFuzzer::new(scheduler, feedback, objective);

    let pool = if cfg.swarm {
        ServerPool::with_swarm(&cfg.server, cfg.timeout, cfg.grace, cfg.seed)?
    } else {
        ServerPool::new(&cfg.server, cfg.timeout, cfg.grace)?
    };
    let mut executor = RsExecutor {
        observers: (observer, ()),
        map_ptr,
        pool,
        cfg_seq_len: cfg.seq_len,
        json: cfg.json_available,
        safe_utf8: cfg.safe_utf8,
        malform_rate: cfg.malform_rate,
        verbose: cfg.verbose,
        structure_feedback: cfg.structure_feedback,
        grace: cfg.grace,
        timeout: cfg.timeout,
        execs: 0,
        restart_every: cfg.restart_every,
        server_cfg: cfg.server.clone(),
        auto_reduce: cfg.auto_reduce,
        out_dir: cfg.out_dir.clone(),
        seen: HashMap::new(),
        findings: 0,
        last_report: std::time::Instant::now(),
    };

    // Seed the corpus from the committed root set (`--seeds`, default `seeds/`):
    // byte buffers that decode to realistic, deep command sequences (index create
    // + docs + search/aggregate/vector/cursor/GC/…), so mutation explores outward
    // from valid deep states instead of first rediscovering that a query needs an
    // index. These were generated once from the pytest workflows and committed;
    // inspect any of them with `--dump-file`.
    let mut n_seeds = 0usize;
    if let Ok(rd) = std::fs::read_dir(&cfg.seeds) {
        let mut paths: Vec<_> = rd
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.is_file())
            .collect();
        paths.sort();
        for p in paths {
            let Ok(bytes) = std::fs::read(&p) else {
                continue;
            };
            let input = BytesInput::new(bytes);
            if fuzzer
                .add_input(&mut state, &mut executor, &mut mgr, input)
                .is_ok()
            {
                n_seeds += 1;
            }
        }
    }
    // Plus a couple of trivial random buffers, for byte-level diversity and so the
    // corpus is non-empty even if the root seed directory is missing.
    for s in [1u64, 2, 3, 5, 8] {
        let input = BytesInput::new(seed_bytes(cfg.seed ^ s));
        fuzzer
            .add_input(&mut state, &mut executor, &mut mgr, input)
            .map_err(|e| anyhow::anyhow!("seed input: {e}"))?;
    }
    println!(
        "seeded corpus: {n_seeds} root seeds (from {}) + 5 random buffers",
        cfg.seeds.display()
    );

    let mut stages = tuple_list!(StdMutationalStage::new(HavocScheduledMutator::new(
        havoc_mutations()
    )));

    if cfg.rounds > 0 {
        for _ in 0..cfg.rounds {
            fuzzer
                .fuzz_one(&mut stages, &mut executor, &mut state, &mut mgr)
                .map_err(|e| anyhow::anyhow!("fuzz: {e}"))?;
        }
    } else {
        fuzzer
            .fuzz_loop(&mut stages, &mut executor, &mut state, &mut mgr)
            .map_err(|e| anyhow::anyhow!("fuzz_loop: {e}"))?;
    }

    executor.pool.stop();
    println!("done: {} distinct findings", executor.findings);
    Ok(())
}

// ---- shared helpers (also used by replay/minimize) -------------------------

/// True for a `CONFIG SET ...` command (so the pool knows to restore config).
fn is_config_set(cmd: &Cmd) -> bool {
    cmd.first()
        .map(|v| v.eq_ignore_ascii_case(b"CONFIG"))
        .unwrap_or(false)
        && cmd
            .get(1)
            .map(|v| v.eq_ignore_ascii_case(b"SET"))
            .unwrap_or(false)
}

fn produces_cursor(cmd: &Cmd) -> bool {
    let head = cmd.first().map(|v| v.as_slice()).unwrap_or(b"");
    if head == b"FT.AGGREGATE" {
        cmd.iter().any(|a| a.eq_ignore_ascii_case(b"WITHCURSOR"))
    } else {
        head == b"FT.CURSOR"
    }
}

/// Rewrite an `FT.CURSOR READ/DEL` id to a live cursor when one is available.
fn resolve_cursor(cmd: &Cmd, live: &[i64]) -> Cmd {
    if cmd.first().map(|v| v.as_slice()) == Some(b"FT.CURSOR") && cmd.len() == 4 && !live.is_empty()
    {
        let mut out = cmd.clone();
        // Interpret the existing id byte-string as a slot selector.
        let slot = std::str::from_utf8(&out[3])
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0)
            .unsigned_abs() as usize;
        out[3] = live[slot % live.len()].to_string().into_bytes();
        out
    } else {
        cmd.clone()
    }
}

enum Verdict {
    Recovered,
    Crashed,
    Hung,
}

fn recheck(pool: &mut ServerPool) -> Verdict {
    let deadline = std::time::Instant::now() + pool.grace;
    loop {
        if !pool.server.is_alive() {
            return Verdict::Crashed;
        }
        if let Ok(mut d) = Driver::connect(&pool.server.url(), pool.timeout) {
            if d.ping() {
                pool.driver = d;
                return Verdict::Recovered;
            }
        }
        if std::time::Instant::now() >= deadline {
            return if pool.server.wait_exit(Duration::from_secs(1)) {
                Verdict::Crashed
            } else {
                Verdict::Hung
            };
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Replay a fixed concrete command sequence against a fresh server and return the
/// finding it produces, if any. Used by --replay.
pub fn replay_detect(
    cfg: &ServerConfig,
    cmds: &[Cmd],
    config: Vec<(String, String)>,
    timeout: Duration,
    grace: Duration,
) -> Option<Finding> {
    let mut pool = ServerPool::with_config(cfg, timeout, grace, config).ok()?;
    let f = pool.replay_seq(cmds, grace);
    pool.stop();
    f
}

/// Hybrid crash-preserving reducer: command-level delta-debugging (drop whole
/// commands) followed by an intra-command shrink pass (drop trailing args, then
/// shorten each surviving arg). Every candidate must reproduce the *same crash
/// signature* — so reduction never drifts to a different (easier) bug. All
/// candidates run against one reused server via `pool.replay_seq`.
pub fn reduce(
    pool: &mut ServerPool,
    mut cmds: Vec<Cmd>,
    target: &str,
    grace: Duration,
    log: bool,
) -> Vec<Cmd> {
    let repro = |pool: &mut ServerPool, cand: &[Cmd]| {
        !cand.is_empty()
            && pool
                .replay_seq(cand, grace)
                .map(|f| f.signature() == target)
                .unwrap_or(false)
    };

    // Pass 1: drop whole commands until no single removal preserves the crash.
    let mut changed = true;
    while changed {
        changed = false;
        let mut i = 0;
        while i < cmds.len() {
            let mut cand = cmds.clone();
            cand.remove(i);
            if repro(pool, &cand) {
                cmds = cand;
                changed = true;
                if log {
                    println!("  dropped command {i}; {} remain", cmds.len());
                }
            } else {
                i += 1;
            }
        }
    }

    // Pass 2: intra-command shrink — for each surviving command, greedily drop
    // trailing args, then shorten each remaining arg (truncate long/binary args,
    // collapse the rest to a single byte). Keeps only signature-preserving edits.
    for i in 0..cmds.len() {
        // Never shrink below the command verb (index 0).
        while cmds[i].len() > 1 {
            let mut cand = cmds.clone();
            cand[i].pop();
            if repro(pool, &cand) {
                cmds = cand;
            } else {
                break;
            }
        }
        for a in 1..cmds[i].len() {
            for shrunk in shrink_arg(&cmds[i][a]) {
                let mut cand = cmds.clone();
                cand[i][a] = shrunk;
                if repro(pool, &cand) {
                    cmds = cand;
                    break;
                }
            }
        }
        if log {
            println!("  shrank command {i} -> {}", render_cmd(&cmds[i]));
        }
    }
    cmds
}

/// Candidate simplifications of one argument, smallest first: empty, a single
/// byte, then a halved-length prefix (useful for long vector blobs).
fn shrink_arg(arg: &[u8]) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    if arg.len() > 1 {
        out.push(Vec::new());
        out.push(vec![b'0']);
        out.push(arg[..arg.len() / 2].to_vec());
    }
    out
}
