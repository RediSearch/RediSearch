/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! A search for programs that reach new features, on LibAFL: its on-disk
//! corpus, max-map feedback over hashed features, and a scheduler that
//! prefers short and fast programs. The search sees a program only through
//! one function from program to features.

use std::borrow::Cow;
use std::collections::HashSet;
use std::fmt::{Debug, Display};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use libafl::corpus::{Corpus, CorpusId, InMemoryCorpus, InMemoryOnDiskCorpus};
use libafl::events::SimpleEventManager;
use libafl::executors::{Executor, ExitKind, HasObservers};
use libafl::feedbacks::MaxMapFeedback;
use libafl::fuzzer::{Evaluator, ExecutionProcessor, Fuzzer, StdFuzzer};
use libafl::inputs::Input;
use libafl::monitors::SimpleMonitor;
use libafl::mutators::{MutationResult, Mutator};
use libafl::observers::{CanTrack, ExplicitTracking, OwnedMapObserver};
use libafl::schedulers::{IndexesLenTimeMinimizerScheduler, QueueScheduler};
use libafl::stages::StdMutationalStage;
use libafl::state::{HasCorpus, HasExecutions, HasRand, StdState};
use libafl::{Error, random_corpus_id};
use libafl_bolts::rands::{Rand, StdRand};
use libafl_bolts::tuples::{RefIndexable, tuple_list, tuple_list_type};
use libafl_bolts::{HasLen, Named, current_nanos};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::draw::Draw;
use crate::interrupt::interrupted;
use crate::model::Program;
use crate::system::Feature;

/// Features are hashed into a map of this many entries.
const MAP: usize = 1 << 20;
const MUTATIONS_PER_PICK: usize = 8;
const STATUS_EVERY: Duration = Duration::from_secs(30);
/// Runs that may fail in a row, as when the machine is briefly out of
/// resources, before the campaign gives up.
const FAILURES_IN_A_ROW: u32 = 3;
/// Runs after which a campaign that has seen no feature says so.
const RUNS_BEFORE_WARNING: u64 = 20;

pub struct Campaign<'a> {
    /// Directories of programs to start from.
    pub seeds: &'a [PathBuf],
    /// Where the programs found are kept; a campaign starts from them too.
    pub corpus: &'a Path,
    /// Until stopped, if `None`.
    pub duration: Option<Duration>,
}

/// What a campaign's programs are made of.
pub trait Op: Clone + Debug + Hash + Serialize + DeserializeOwned {}

impl<O: Clone + Debug + Hash + Serialize + DeserializeOwned> Op for O {}

/// Searches until the duration is spent, `stop` is set, or the process is
/// [`interrupted`], after the run in progress.
///
/// `execute` runs a program and returns the features it reached; anything
/// else a run shows, such as findings, is for `execute` to handle. A feature
/// not seen before counts only if a second run of the program shows it too;
/// one that shows in only one of the runs never counts. `mutate` edits a
/// program, splicing from a donor. Programs that show a new feature are
/// shared through `peer`, and those other campaigns shared are taken without
/// running them. Without seeds the search starts from the empty program.
pub fn fuzz<O: Op, E: Display>(
    campaign: &Campaign<'_>,
    stop: &AtomicBool,
    peer: Peer<'_, O>,
    execute: impl FnMut(&Program<O>) -> Result<Vec<Feature>, E>,
    mutate: impl FnMut(&Program<O>, &Program<O>, &mut Draw) -> Program<O>,
) -> Result<(), Error> {
    let features = OwnedMapObserver::new("features", vec![0u8; MAP]).track_indices();
    let mut feedback = MaxMapFeedback::new(&features);
    let mut state = StdState::new(
        StdRand::with_seed(current_nanos()),
        InMemoryOnDiskCorpus::<Entry<O>>::with_meta_format(campaign.corpus, None)?,
        InMemoryCorpus::new(),
        &mut feedback,
        &mut (),
    )?;
    let scheduler = IndexesLenTimeMinimizerScheduler::new(&features, QueueScheduler::new());
    let mut fuzzer = StdFuzzer::new(scheduler, feedback, ());
    let mut executor = RunExecutor {
        execute,
        stop,
        deadline: campaign.duration.map(|d| Instant::now() + d),
        peer,
        runs: 0,
        failures: 0,
        seen: HashSet::new(),
        unstable: HashSet::new(),
        observers: tuple_list!(features),
    };
    let mut last_status = Instant::now();
    let mut manager = SimpleEventManager::new(SimpleMonitor::new(move |line: &str| {
        if line.starts_with("[UserStats") && last_status.elapsed() >= STATUS_EVERY {
            last_status = Instant::now();
            log::info!("{line}");
        }
    }));
    let mut stages = tuple_list!(StdMutationalStage::with_max_iterations(
        ClosureMutator { mutate },
        NonZeroUsize::new(MUTATIONS_PER_PICK).expect("non-zero"),
    ));

    let mut dirs = campaign.seeds.to_vec();
    dirs.push(campaign.corpus.to_owned());
    let result: Result<(), Error> = (|| {
        state.load_initial_inputs(&mut fuzzer, &mut executor, &mut manager, &dirs)?;
        if state.corpus().count() == 0 {
            let empty = Entry(Program::default());
            fuzzer.add_input(&mut state, &mut executor, &mut manager, empty)?;
        }
        loop {
            for (program, features) in executor.imports() {
                executor.mark(&features);
                let entry = Entry(program);
                let observers = &executor.observers;
                fuzzer.evaluate_execution(
                    &mut state,
                    &mut manager,
                    &entry,
                    observers,
                    &ExitKind::Ok,
                    false,
                )?;
            }
            fuzzer.fuzz_one(&mut stages, &mut executor, &mut state, &mut manager)?;
        }
    })();
    match result {
        Err(Error::ShuttingDown) => Ok(()),
        result => result,
    }
}

/// Programs campaigns kept, with the features that made them worth keeping.
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

impl<O: Clone> Default for Exchange<O> {
    fn default() -> Self {
        Exchange::new()
    }
}

impl<O: Clone> Peer<'_, O> {
    fn publish(&self, program: &Program<O>, features: &[Feature]) {
        let shared = Shared {
            from: self.me,
            program: program.clone(),
            features: features.to_vec(),
        };
        let mut all = self.exchange.shared.lock().expect("not poisoned");
        all.push(shared);
    }

    /// What other campaigns shared since the last call.
    fn receive(&mut self) -> Vec<(Program<O>, Vec<Feature>)> {
        let all = self.exchange.shared.lock().expect("not poisoned");
        let new = all[self.read..]
            .iter()
            .filter(|shared| shared.from != self.me)
            .map(|shared| (shared.program.clone(), shared.features.clone()))
            .collect();
        self.read = all.len();
        new
    }
}

/// LibAFL's input traits are foreign, so they are implemented on a wrapper.
/// Programs are kept as JSON files in the corpus directory.
#[derive(Clone, Debug, Hash, Serialize, Deserialize)]
#[serde(transparent, bound = "O: Op")]
struct Entry<O>(Program<O>);

impl<O: Op> Input for Entry<O> {
    fn to_file<Q: AsRef<Path>>(&self, path: Q) -> Result<(), Error> {
        let json =
            serde_json::to_string_pretty(&self.0).map_err(|e| Error::serialize(e.to_string()))?;
        Ok(std::fs::write(path, json)?)
    }

    fn from_file<Q: AsRef<Path>>(path: Q) -> Result<Self, Error> {
        let json = std::fs::read_to_string(path)?;
        serde_json::from_str(&json)
            .map(Entry)
            .map_err(|e| Error::illegal_argument(e.to_string()))
    }
}

impl<O> HasLen for Entry<O> {
    fn len(&self) -> usize {
        self.0.ops.len()
    }
}

type Features = ExplicitTracking<OwnedMapObserver<u8>, true, false>;

/// Runs programs for LibAFL: confirms new features, tolerates a few
/// failures in a row, and ends the search when it should.
struct RunExecutor<'a, F, O> {
    execute: F,
    stop: &'a AtomicBool,
    deadline: Option<Instant>,
    peer: Peer<'a, O>,
    runs: u64,
    /// In a row.
    failures: u32,
    seen: HashSet<Feature>,
    /// Shown by one run of a program and not by another.
    unstable: HashSet<Feature>,
    observers: tuple_list_type!(Features),
}

impl<F, O: Clone> RunExecutor<'_, F, O> {
    /// The features of a run that count; `None` if the run failed but the
    /// campaign can go on.
    fn observe<E: Display>(&mut self, program: &Program<O>) -> Result<Option<Vec<Feature>>, Error>
    where
        F: FnMut(&Program<O>) -> Result<Vec<Feature>, E>,
    {
        let Some(mut features) = self.run(program)? else {
            return Ok(None);
        };
        features.retain(|feature| !self.unstable.contains(feature));
        if features.iter().any(|feature| !self.seen.contains(feature)) {
            let Some(again) = self.run(program)? else {
                return Ok(None);
            };
            let again: HashSet<Feature> = again.into_iter().collect();
            let first: HashSet<Feature> = features.iter().copied().collect();
            self.unstable.extend(first.symmetric_difference(&again));
            features.retain(|feature| again.contains(feature));
            if features.iter().any(|feature| !self.seen.contains(feature)) {
                self.peer.publish(program, &features);
            }
        }
        self.seen.extend(features.iter().copied());
        if self.runs == RUNS_BEFORE_WARNING && self.seen.is_empty() {
            log::warn!(
                "{RUNS_BEFORE_WARNING} runs showed no feature; is the target built with coverage?"
            );
        }
        Ok(Some(features))
    }

    fn run<E: Display>(&mut self, program: &Program<O>) -> Result<Option<Vec<Feature>>, Error>
    where
        F: FnMut(&Program<O>) -> Result<Vec<Feature>, E>,
    {
        let late = self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline);
        if late || self.stop.load(Ordering::Relaxed) || interrupted() {
            return Err(Error::shutting_down());
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
                    return Err(Error::unknown(format!(
                        "{FAILURES_IN_A_ROW} runs in a row failed, the last with: {e}"
                    )));
                }
                log::warn!("run failed: {e}");
                Ok(None)
            }
        }
    }

    /// The programs other campaigns shared that show a feature this one has
    /// not seen, with the features that count, which are now seen.
    fn imports(&mut self) -> Vec<(Program<O>, Vec<Feature>)> {
        let mut imports = Vec::new();
        for (program, mut features) in self.peer.receive() {
            features.retain(|feature| !self.unstable.contains(feature));
            if features.iter().any(|feature| !self.seen.contains(feature)) {
                self.seen.extend(features.iter().copied());
                imports.push((program, features));
            }
        }
        imports
    }

    /// Sets the map to show exactly `features`.
    fn mark(&mut self, features: &[Feature]) {
        let map = self.observers.0.as_mut();
        map.fill(0);
        for &feature in features {
            map[slot(feature)] = 1;
        }
    }
}

impl<F, O> HasObservers for RunExecutor<'_, F, O> {
    type Observers = tuple_list_type!(Features);

    fn observers(&self) -> RefIndexable<&Self::Observers, Self::Observers> {
        RefIndexable::from(&self.observers)
    }

    fn observers_mut(&mut self) -> RefIndexable<&mut Self::Observers, Self::Observers> {
        RefIndexable::from(&mut self.observers)
    }
}

impl<EM, S, Z, O, E, F> Executor<EM, Entry<O>, S, Z> for RunExecutor<'_, F, O>
where
    O: Clone,
    S: HasExecutions,
    E: Display,
    F: FnMut(&Program<O>) -> Result<Vec<Feature>, E>,
{
    fn run_target(
        &mut self,
        _fuzzer: &mut Z,
        state: &mut S,
        _manager: &mut EM,
        entry: &Entry<O>,
    ) -> Result<ExitKind, Error> {
        let before = self.runs;
        let features = self.observe(&entry.0);
        *state.executions_mut() += self.runs - before;
        if let Some(features) = features? {
            self.mark(&features);
        }
        Ok(ExitKind::Ok)
    }
}

fn slot(feature: Feature) -> usize {
    let mut hasher = DefaultHasher::new();
    feature.hash(&mut hasher);
    let map = u64::try_from(MAP).expect("fits");
    usize::try_from(hasher.finish() % map).expect("below the map size")
}

struct ClosureMutator<F> {
    mutate: F,
}

impl<F> Named for ClosureMutator<F> {
    fn name(&self) -> &Cow<'static, str> {
        static NAME: Cow<'static, str> = Cow::Borrowed("mutate");
        &NAME
    }
}

impl<S, O, F> Mutator<Entry<O>, S> for ClosureMutator<F>
where
    S: HasRand + HasCorpus<Entry<O>>,
    O: Op,
    F: FnMut(&Program<O>, &Program<O>, &mut Draw) -> Program<O>,
{
    fn mutate(&mut self, state: &mut S, entry: &mut Entry<O>) -> Result<MutationResult, Error> {
        let mut rng = Draw::new(state.rand_mut().next());
        let id = random_corpus_id!(state.corpus(), state.rand_mut());
        let donor = state.corpus().cloned_input_for_id(id)?;
        entry.0 = (self.mutate)(&entry.0, &donor.0, &mut rng);
        Ok(MutationResult::Mutated)
    }

    fn post_exec(&mut self, _state: &mut S, _new: Option<CorpusId>) -> Result<(), Error> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Made;

    fn edge(id: u64) -> Feature {
        Feature { space: "edge", id }
    }

    fn program(op: u8) -> Program<u8> {
        Program {
            ops: vec![Made {
                op,
                from: Vec::new(),
            }],
        }
    }

    /// A program shows edge 1 every run and edge 2 every other run. Another
    /// campaign shared a program showing edge 7.
    #[test]
    fn only_features_two_runs_agree_on_count_and_shared_programs_are_not_run() {
        let exchange = Exchange::new();
        exchange.peer(1).publish(&program(99), &[edge(7)]);
        let corpus = tempfile::tempdir().unwrap();
        let campaign = Campaign {
            seeds: &[],
            corpus: corpus.path(),
            duration: Some(Duration::from_millis(200)),
        };
        let mut ran = Vec::new();
        let execute = |program: &Program<u8>| {
            ran.push(program.clone());
            let mut features = vec![edge(1)];
            if ran.len() % 2 == 1 {
                features.push(edge(2));
            }
            Ok::<_, String>(features)
        };
        let mutate = |parent: &Program<u8>, _: &Program<u8>, _: &mut Draw| {
            let mut child = parent.clone();
            child.ops.extend(program(1).ops);
            child
        };
        let stop = AtomicBool::new(false);
        fuzz(&campaign, &stop, exchange.peer(0), execute, mutate).unwrap();
        assert!(!ran.contains(&program(99)));
        let kept = std::fs::read_dir(corpus.path())
            .unwrap()
            .filter(|entry| {
                !entry
                    .as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with('.')
            })
            .count();
        assert_eq!(kept, 2, "the empty program and the shared one");
    }
}
