/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! A search for programs that reach new features. It keeps a program that
//! reaches a feature no kept program reaches, and mostly picks, to mutate,
//! a program that is the shortest to reach some feature. The search sees a
//! program only through one function from program to features.

use std::collections::{HashMap, HashSet};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};

use log::{info, warn};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::commands::load;
use crate::draw::Draw;
use crate::interrupt::interrupted;
use crate::model::Program;
use crate::system::Feature;

const MUTATIONS_PER_PICK: usize = 8;
/// One pick in this many is of any kept program rather than of a shortest.
const ANY_PROGRAM_ONE_IN: u64 = 20;
const STATUS_EVERY: Duration = Duration::from_secs(30);
/// Runs that may fail in a row, as when the machine is briefly out of
/// resources, before the campaign gives up.
const FAILURES_IN_A_ROW: u32 = 3;
/// Runs after which a campaign that has seen no feature says so.
const RUNS_BEFORE_WARNING: u64 = 20;

pub struct Campaign<'a> {
    /// Directories of programs to start from.
    pub seeds: &'a [PathBuf],
    /// Where the programs kept are written; a campaign starts from them too.
    pub corpus: &'a Path,
    /// Until stopped, if `None`.
    pub duration: Option<Duration>,
}

/// Searches in `jobs` threads named `worker <n>`, sharing what they keep,
/// until the duration is spent, a worker fails, or the process is
/// [`interrupted`], after the runs in progress.
///
/// `execute` runs a program and returns the features it reached; anything
/// else a run shows, such as findings, is for `execute` to handle. A feature
/// not seen before counts only if a second run of the program shows it too;
/// one that shows in only one of the runs never counts. `mutate` edits a
/// program, splicing from a donor. Without seeds the search starts from the
/// empty program.
pub fn fuzz<O>(
    campaign: &Campaign<'_>,
    jobs: usize,
    execute: impl Fn(&Program<O>) -> io::Result<Vec<Feature>> + Sync,
    mutate: impl Fn(&Program<O>, &Program<O>, &mut Draw) -> Program<O> + Sync,
) -> io::Result<()>
where
    O: Clone + Serialize + DeserializeOwned + Send,
{
    std::fs::create_dir_all(campaign.corpus)?;
    let search = Search {
        execute,
        corpus: campaign.corpus,
        deadline: campaign.duration.map(|d| Instant::now() + d),
        stop: AtomicBool::new(false),
        runs: AtomicU64::new(0),
        kept: Mutex::new(Kept {
            programs: Vec::new(),
            shortest: HashMap::new(),
            unstable: HashSet::new(),
            status: Instant::now(),
        }),
    };
    let mut failures = 0;
    for dir in campaign
        .seeds
        .iter()
        .map(PathBuf::as_path)
        .chain([campaign.corpus])
    {
        for entry in std::fs::read_dir(dir)? {
            let path = entry?.path();
            if path.extension().is_some_and(|e| e == "json") && !search.stopping() {
                search.observe(&load(&path)?, &mut failures)?;
            }
        }
    }
    let worker = |n: u64| {
        let mut seeds = Draw::new(n ^ now());
        let mut failures = 0;
        while !search.stopping() {
            let mut rng = Draw::new(seeds.raw());
            let (parent, donor) = search.kept.lock().expect("not poisoned").pick(&mut rng);
            for _ in 0..MUTATIONS_PER_PICK {
                if search.stopping() {
                    break;
                }
                let child = mutate(&parent, &donor, &mut rng);
                if let Err(e) = search.observe(&child, &mut failures) {
                    search.stop.store(true, Ordering::Relaxed);
                    return Err(e);
                }
            }
        }
        Ok(())
    };
    let worker = &worker;
    std::thread::scope(|scope| {
        let workers: Vec<_> = (0..jobs)
            .map(|n| {
                std::thread::Builder::new()
                    .name(format!("worker {n}"))
                    .spawn_scoped(scope, move || worker(u64::try_from(n).expect("fits")))
                    .expect("threads start")
            })
            .collect();
        workers
            .into_iter()
            .try_for_each(|worker| worker.join().expect("workers do not panic"))
    })
}

struct Search<'a, O, F> {
    execute: F,
    corpus: &'a Path,
    deadline: Option<Instant>,
    /// Set by a worker that failed.
    stop: AtomicBool,
    runs: AtomicU64,
    kept: Mutex<Kept<O>>,
}

struct Kept<O> {
    programs: Vec<Program<O>>,
    /// For each feature reached, the shortest program kept that reaches it.
    shortest: HashMap<Feature, usize>,
    /// Shown by one run of a program and not by another.
    unstable: HashSet<Feature>,
    status: Instant,
}

impl<O, F> Search<'_, O, F>
where
    O: Clone + Serialize,
    F: Fn(&Program<O>) -> io::Result<Vec<Feature>>,
{
    fn stopping(&self) -> bool {
        self.stop.load(Ordering::Relaxed)
            || interrupted()
            || self
                .deadline
                .is_some_and(|deadline| Instant::now() >= deadline)
    }

    /// Runs `program` and keeps it if it reaches a new feature, as a second
    /// run confirms. A run that fails counts against `failures`, the runs
    /// failed in a row.
    fn observe(&self, program: &Program<O>, failures: &mut u32) -> io::Result<()> {
        let Some(first) = self.run(program, failures)? else {
            return Ok(());
        };
        if !self.lock().is_new(&first) {
            return Ok(());
        }
        let Some(again) = self.run(program, failures)? else {
            return Ok(());
        };
        let again: HashSet<Feature> = again.into_iter().collect();
        let mut kept = self.lock();
        let first: HashSet<Feature> = first.into_iter().collect();
        kept.unstable.extend(first.symmetric_difference(&again));
        let stable: Vec<Feature> = first.intersection(&again).copied().collect();
        if kept.is_new(&stable) {
            kept.keep(program, &stable, self.corpus)?;
        }
        Ok(())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Kept<O>> {
        self.kept.lock().expect("not poisoned")
    }

    /// The features of a run; `None` if it failed but the campaign can go on.
    fn run(&self, program: &Program<O>, failures: &mut u32) -> io::Result<Option<Vec<Feature>>> {
        let runs = self.runs.fetch_add(1, Ordering::Relaxed) + 1;
        let result = (self.execute)(program);
        let mut kept = self.lock();
        if runs == RUNS_BEFORE_WARNING && kept.shortest.is_empty() {
            warn!("{runs} runs showed no feature; is the target built with coverage?");
        }
        if kept.status.elapsed() >= STATUS_EVERY {
            kept.status = Instant::now();
            info!(
                "{runs} runs, {} programs kept, {} features",
                kept.programs.len(),
                kept.shortest.len()
            );
        }
        match result {
            Ok(features) => {
                *failures = 0;
                Ok(Some(features))
            }
            Err(_) if self.stopping() => Ok(None),
            Err(e) => {
                *failures += 1;
                if *failures == FAILURES_IN_A_ROW {
                    return Err(io::Error::other(format!(
                        "{FAILURES_IN_A_ROW} runs in a row failed, the last with: {e}"
                    )));
                }
                warn!("run failed: {e}");
                Ok(None)
            }
        }
    }
}

impl<O: Clone + Serialize> Kept<O> {
    fn is_new(&self, features: &[Feature]) -> bool {
        features
            .iter()
            .any(|f| !self.unstable.contains(f) && !self.shortest.contains_key(f))
    }

    fn keep(
        &mut self,
        program: &Program<O>,
        features: &[Feature],
        corpus: &Path,
    ) -> io::Result<()> {
        let json = serde_json::to_vec_pretty(program).map_err(io::Error::other)?;
        let mut hasher = DefaultHasher::new();
        json.hash(&mut hasher);
        std::fs::write(corpus.join(format!("{:016x}.json", hasher.finish())), json)?;
        let at = self.programs.len();
        self.programs.push(program.clone());
        for &feature in features {
            let shortest = self.shortest.entry(feature).or_insert(at);
            if self.programs[*shortest].ops.len() > program.ops.len() {
                *shortest = at;
            }
        }
        Ok(())
    }

    /// A parent and a donor; both the empty program before anything is kept.
    fn pick(&self, rng: &mut Draw) -> (Program<O>, Program<O>) {
        if self.programs.is_empty() {
            return (Program::default(), Program::default());
        }
        let any = |rng: &mut Draw| rng.index(self.programs.len());
        let parent = if rng.one_in(ANY_PROGRAM_ONE_IN) {
            any(rng)
        } else {
            let shortest: Vec<usize> = self.shortest.values().copied().collect();
            shortest[rng.index(shortest.len())]
        };
        let donor = any(rng);
        (self.programs[parent].clone(), self.programs[donor].clone())
    }
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |since| since.as_secs())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;
    use crate::model::Made;

    fn edge(id: u64) -> Feature {
        Feature { space: "edge", id }
    }

    /// A program shows edge 1 every run and edge 2 every other run, so only
    /// the first program to show edge 1 is kept.
    #[test]
    fn only_features_two_runs_agree_on_count() {
        let corpus = tempfile::tempdir().unwrap();
        let campaign = Campaign {
            seeds: &[],
            corpus: corpus.path(),
            duration: Some(Duration::from_millis(200)),
        };
        let runs = AtomicUsize::new(0);
        let execute = |_: &Program<u8>| {
            let mut features = vec![edge(1)];
            if runs.fetch_add(1, Ordering::Relaxed).is_multiple_of(2) {
                features.push(edge(2));
            }
            Ok(features)
        };
        let mutate = |parent: &Program<u8>, _: &Program<u8>, _: &mut Draw| {
            let mut child = parent.clone();
            child.ops.push(Made {
                op: 1,
                from: Vec::new(),
            });
            child
        };
        fuzz(&campaign, 1, execute, mutate).unwrap();
        assert_eq!(std::fs::read_dir(corpus.path()).unwrap().count(), 1);
    }
}
