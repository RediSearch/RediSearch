/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! A search for programs that reach new features, on LibAFL. The search
//! keeps each such program and mutates the ones it keeps, preferring short
//! and fast ones.

use std::borrow::Cow;
use std::fmt::{Debug, Display};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use libafl::corpus::{Corpus, CorpusId, InMemoryCorpus, InMemoryOnDiskCorpus};
use libafl::events::SimpleEventManager;
use libafl::executors::{Executor, ExitKind, HasObservers};
use libafl::feedbacks::MaxMapFeedback;
use libafl::fuzzer::{Evaluator, Fuzzer, StdFuzzer};
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

/// `execute` runs a program and returns the features it reached; anything
/// else a run shows, such as findings, is for `execute` to handle. `mutate`
/// edits a program, splicing from a donor program. Without seeds the search
/// starts from the empty program. It returns without an error once `stop` is
/// set or the process is [`interrupted`], after the run in progress.
pub fn fuzz<O: Op, E: Display>(
    campaign: &Campaign<'_>,
    stop: &AtomicBool,
    execute: impl FnMut(&Program<O>) -> Result<Vec<Feature>, E>,
    mutate: impl FnMut(&Program<O>, &Program<O>, &mut Draw) -> Program<O>,
) -> Result<(), Error> {
    match search(campaign, stop, execute, mutate) {
        Err(Error::ShuttingDown) => Ok(()),
        result => result,
    }
}

fn search<O: Op, E: Display>(
    campaign: &Campaign<'_>,
    stop: &AtomicBool,
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
    let mut executor = ClosureExecutor {
        execute,
        stop,
        failures: 0,
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
    state.load_initial_inputs(&mut fuzzer, &mut executor, &mut manager, &dirs)?;
    if state.corpus().count() == 0 {
        fuzzer.add_input(
            &mut state,
            &mut executor,
            &mut manager,
            Entry(Program::default()),
        )?;
    }
    let deadline = campaign.duration.map(|d| Instant::now() + d);
    while deadline.is_none_or(|deadline| Instant::now() < deadline) {
        fuzzer.fuzz_one(&mut stages, &mut executor, &mut state, &mut manager)?;
    }
    Ok(())
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

struct ClosureExecutor<'a, F> {
    execute: F,
    stop: &'a AtomicBool,
    /// In a row.
    failures: u32,
    observers: tuple_list_type!(Features),
}

impl<F> HasObservers for ClosureExecutor<'_, F> {
    type Observers = tuple_list_type!(Features);

    fn observers(&self) -> RefIndexable<&Self::Observers, Self::Observers> {
        RefIndexable::from(&self.observers)
    }

    fn observers_mut(&mut self) -> RefIndexable<&mut Self::Observers, Self::Observers> {
        RefIndexable::from(&mut self.observers)
    }
}

impl<EM, S, Z, O, E, F> Executor<EM, Entry<O>, S, Z> for ClosureExecutor<'_, F>
where
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
        if self.stop.load(Ordering::Relaxed) || interrupted() {
            return Err(Error::shutting_down());
        }
        *state.executions_mut() += 1;
        match (self.execute)(&entry.0) {
            Ok(features) => {
                self.failures = 0;
                let map = self.observers.0.as_mut();
                for feature in features {
                    map[slot(feature)] = 1;
                }
            }
            Err(e) => {
                self.failures += 1;
                if self.failures == FAILURES_IN_A_ROW {
                    return Err(Error::unknown(format!(
                        "{FAILURES_IN_A_ROW} runs in a row failed, the last with: {e}"
                    )));
                }
                log::warn!("run failed: {e}");
            }
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
