/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! A search for programs that reach new features. An engine sees a program
//! only through one function from program to features, so engines can be
//! swapped and compared: LibAFL's, and a plain loop.

mod basic;
mod libafl;

use std::collections::HashSet;
use std::fmt::{self, Debug, Display};
use std::hash::Hash;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::draw::Draw;
use crate::error::Error;
use crate::interrupt::interrupted;
use crate::model::Program;
use crate::system::Feature;

/// Runs that may fail in a row, as when the machine is briefly out of
/// resources, before the campaign gives up.
const FAILURES_IN_A_ROW: u32 = 3;

pub struct Campaign<'a> {
    /// Directories of programs to start from.
    pub seeds: &'a [PathBuf],
    /// Where the programs found are kept; a campaign starts from them too.
    pub corpus: &'a Path,
    /// Until stopped, if `None`.
    pub duration: Option<Duration>,
    /// At most this many runs, counting reruns.
    pub runs: Option<u64>,
    pub setup: Setup,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum Engine {
    /// LibAFL's corpus, feedback, and length-and-time minimizing scheduler.
    Libafl,
    /// Keeps every program that shows a new feature and mutates one picked
    /// uniformly.
    Basic,
}

/// An engine, and whether it confirms new features, written as `libafl`,
/// `basic+confirm`, and so on.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct Setup {
    pub engine: Engine,
    /// Before counting a feature not seen before, run the program again,
    /// count only the features both runs showed, and never count the others.
    pub confirm: bool,
}

impl Default for Setup {
    fn default() -> Setup {
        Setup {
            engine: Engine::Libafl,
            confirm: true,
        }
    }
}

impl FromStr for Setup {
    type Err = String;

    fn from_str(text: &str) -> Result<Setup, String> {
        let (engine, confirm) = match text.strip_suffix("+confirm") {
            Some(engine) => (engine, true),
            None => (text, false),
        };
        let engine = match engine {
            "libafl" => Engine::Libafl,
            "basic" => Engine::Basic,
            other => return Err(format!("unknown engine {other:?}")),
        };
        Ok(Setup { engine, confirm })
    }
}

impl fmt::Display for Setup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let engine = match self.engine {
            Engine::Libafl => "libafl",
            Engine::Basic => "basic",
        };
        let confirm = if self.confirm { "+confirm" } else { "" };
        write!(f, "{engine}{confirm}")
    }
}

/// What a campaign did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Outcome {
    /// Counting reruns.
    pub runs: u64,
    /// Programs kept for showing new features, seeds included.
    pub kept: usize,
}

/// What a campaign's programs are made of.
pub trait Op: Clone + Debug + Hash + Serialize + DeserializeOwned {}

impl<O: Clone + Debug + Hash + Serialize + DeserializeOwned> Op for O {}

/// `execute` runs a program and returns the features it reached; anything
/// else a run shows, such as findings, is for `execute` to handle. `mutate`
/// edits a program, splicing from a donor program. With a `peer`, the
/// campaign shares the programs it keeps and takes those of other campaigns
/// without running them. Without seeds the search
/// starts from the empty program. It ends without an error once its
/// duration or runs are spent, `stop` is set, or the process is
/// [`interrupted`], after the run in progress.
pub fn fuzz<O: Op, E: Display>(
    campaign: &Campaign<'_>,
    stop: &AtomicBool,
    peer: Option<Peer<'_, O>>,
    execute: impl FnMut(&Program<O>) -> Result<Vec<Feature>, E>,
    mutate: impl FnMut(&Program<O>, &Program<O>, &mut Draw) -> Program<O>,
) -> Result<Outcome, Error> {
    let runner = Runner {
        execute,
        stop,
        peer,
        deadline: campaign.duration.map(|d| Instant::now() + d),
        budget: campaign.runs,
        confirm: campaign.setup.confirm,
        runs: 0,
        failures: 0,
        seen: HashSet::new(),
        unstable: HashSet::new(),
    };
    match campaign.setup.engine {
        Engine::Libafl => libafl::search(campaign, runner, mutate),
        Engine::Basic => basic::search(campaign, runner, mutate),
    }
}

/// Why a campaign ends.
enum Halt {
    /// Its time or runs are spent, or it was stopped.
    Done,
    Failed(String),
}

/// Runs programs for an engine: counts runs, tolerates a few failures in a
/// row, confirms new features, and tells the engine when to end.
struct Runner<'a, F, O> {
    execute: F,
    stop: &'a AtomicBool,
    peer: Option<Peer<'a, O>>,
    deadline: Option<Instant>,
    budget: Option<u64>,
    confirm: bool,
    runs: u64,
    failures: u32,
    seen: HashSet<Feature>,
    /// Shown by one run of a program and not by another.
    unstable: HashSet<Feature>,
}

/// The features of a run that count.
struct Observed {
    features: Vec<Feature>,
    /// Whether any of them was not seen before.
    new: bool,
}

impl<F, O: Clone> Runner<'_, F, O> {
    fn over(&self) -> bool {
        self.stop.load(Ordering::Relaxed)
            || interrupted()
            || self
                .deadline
                .is_some_and(|deadline| Instant::now() >= deadline)
            || self.budget.is_some_and(|budget| self.runs >= budget)
    }

    /// `None` if the run failed but the campaign can go on.
    fn observe<E: Display>(&mut self, program: &Program<O>) -> Result<Option<Observed>, Halt>
    where
        F: FnMut(&Program<O>) -> Result<Vec<Feature>, E>,
    {
        let Some(mut features) = self.run(program)? else {
            return Ok(None);
        };
        features.retain(|feature| !self.unstable.contains(feature));
        if self.confirm && features.iter().any(|feature| !self.seen.contains(feature)) {
            let Some(again) = self.run(program)? else {
                return Ok(None);
            };
            let again: HashSet<Feature> = again.into_iter().collect();
            let first: HashSet<Feature> = features.iter().copied().collect();
            self.unstable.extend(first.symmetric_difference(&again));
            features.retain(|feature| again.contains(feature));
        }
        let new = features.iter().any(|feature| !self.seen.contains(feature));
        self.seen.extend(features.iter().copied());
        if new && let Some(peer) = &self.peer {
            peer.publish(program, &features);
        }
        Ok(Some(Observed { features, new }))
    }

    /// The programs other campaigns kept that show a feature this one has
    /// not seen, with the features that count, which are now seen.
    fn imports(&mut self) -> Vec<(Program<O>, Vec<Feature>)> {
        let Some(peer) = &mut self.peer else {
            return Vec::new();
        };
        let mut imports = Vec::new();
        for (program, mut features) in peer.receive() {
            features.retain(|feature| !self.unstable.contains(feature));
            if features.iter().any(|feature| !self.seen.contains(feature)) {
                self.seen.extend(features.iter().copied());
                imports.push((program, features));
            }
        }
        imports
    }

    fn run<E: Display>(&mut self, program: &Program<O>) -> Result<Option<Vec<Feature>>, Halt>
    where
        F: FnMut(&Program<O>) -> Result<Vec<Feature>, E>,
    {
        if self.over() {
            return Err(Halt::Done);
        }
        self.runs += 1;
        match (self.execute)(program) {
            Ok(features) => {
                self.failures = 0;
                Ok(Some(features))
            }
            Err(e) => {
                self.failures += 1;
                if self.failures == FAILURES_IN_A_ROW {
                    return Err(Halt::Failed(format!(
                        "{FAILURES_IN_A_ROW} runs in a row failed, the last with: {e}"
                    )));
                }
                log::warn!("run failed: {e}");
                Ok(None)
            }
        }
    }
}

/// Programs campaigns kept, with the features that made them worth keeping.
#[derive(Default)]
pub struct Exchange<O> {
    shared: Mutex<Vec<Shared<O>>>,
}

struct Shared<O> {
    from: usize,
    program: Program<O>,
    features: Vec<Feature>,
}

/// One campaign's end of an [`Exchange`].
pub struct Peer<'a, O> {
    exchange: &'a Exchange<O>,
    me: usize,
    /// How many shared programs this campaign has looked at.
    read: usize,
}

impl<O: Clone> Exchange<O> {
    pub fn new() -> Exchange<O> {
        Exchange {
            shared: Mutex::new(Vec::new()),
        }
    }

    /// The end of campaign `me`.
    pub fn peer(&self, me: usize) -> Peer<'_, O> {
        Peer {
            exchange: self,
            me,
            read: 0,
        }
    }
}

impl<O: Clone> Peer<'_, O> {
    fn publish(&self, program: &Program<O>, features: &[Feature]) {
        let shared = Shared {
            from: self.me,
            program: program.clone(),
            features: features.to_vec(),
        };
        self.exchange
            .shared
            .lock()
            .expect("not poisoned")
            .push(shared);
    }

    /// What other campaigns shared since the last call.
    fn receive(&mut self) -> Vec<(Program<O>, Vec<Feature>)> {
        let shared = self.exchange.shared.lock().expect("not poisoned");
        let new = shared[self.read..]
            .iter()
            .filter(|shared| shared.from != self.me)
            .map(|shared| (shared.program.clone(), shared.features.clone()))
            .collect();
        self.read = shared.len();
        new
    }
}

/// The programs in `dirs`, skipping hidden files and files that do not
/// parse.
fn load_programs<O: Op>(dirs: &[PathBuf]) -> Vec<Program<O>> {
    let mut programs = Vec::new();
    for dir in dirs {
        let Ok(entries) = std::fs::read_dir(dir) else {
            continue;
        };
        let mut paths: Vec<PathBuf> = entries.flatten().map(|entry| entry.path()).collect();
        paths.sort();
        for path in paths {
            let hidden = path
                .file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with('.'));
            if hidden || !path.is_file() {
                continue;
            }
            match crate::commands::load(&path) {
                Ok(program) => programs.push(program),
                Err(e) => log::warn!("skipping {e}"),
            }
        }
    }
    programs
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use super::*;

    #[test]
    fn a_feature_shown_by_only_one_of_two_runs_never_counts() {
        let stop = AtomicBool::new(false);
        let mut calls = 0;
        let feature = |id| Feature { space: "edge", id };
        let execute = |_: &Program<()>| {
            calls += 1;
            let mut features = vec![feature(1)];
            if calls % 2 == 1 {
                features.push(feature(2));
            }
            Ok::<_, String>(features)
        };
        let mut runner = Runner {
            execute,
            stop: &stop,
            peer: None,
            deadline: None,
            budget: Some(10),
            confirm: true,
            runs: 0,
            failures: 0,
            seen: HashSet::new(),
            unstable: HashSet::new(),
        };
        let first = runner.observe(&Program::default()).ok().flatten().unwrap();
        assert_eq!((first.features, first.new), (vec![feature(1)], true));
        assert_eq!(runner.runs, 2);
        let again = runner.observe(&Program::default()).ok().flatten().unwrap();
        assert_eq!((again.features, again.new), (vec![feature(1)], false));
        assert_eq!(runner.runs, 3);
    }

    #[test]
    fn a_program_another_campaign_kept_is_kept_without_running_it() {
        let imported = Program {
            ops: vec![crate::model::Made {
                op: 99u8,
                from: Vec::new(),
            }],
        };
        for engine in [Engine::Libafl, Engine::Basic] {
            let exchange = Exchange::new();
            let feature = |id| Feature { space: "edge", id };
            exchange.peer(1).publish(&imported, &[feature(7)]);
            let corpus = tempfile::tempdir().unwrap();
            let campaign = Campaign {
                seeds: &[],
                corpus: corpus.path(),
                duration: None,
                runs: Some(20),
                setup: Setup {
                    engine,
                    confirm: true,
                },
            };
            let mut ran = Vec::new();
            let execute = |program: &Program<u8>| {
                ran.push(program.clone());
                Ok::<_, String>(vec![feature(0)])
            };
            let mutate = |parent: &Program<u8>, _: &Program<u8>, _: &mut Draw| {
                let mut child = parent.clone();
                child.ops.push(crate::model::Made {
                    op: 1,
                    from: Vec::new(),
                });
                child
            };
            let stop = AtomicBool::new(false);
            let outcome = fuzz(&campaign, &stop, Some(exchange.peer(0)), execute, mutate).unwrap();
            assert!(!ran.contains(&imported), "{engine:?}");
            assert_eq!(
                outcome.kept, 2,
                "{engine:?}: the empty program and the import"
            );
        }
    }
}
