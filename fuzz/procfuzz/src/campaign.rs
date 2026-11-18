/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! A search for programs that reach new edges, on LibAFL. The search keeps
//! each such program and mutates the ones it keeps, preferring small and fast
//! ones.

use std::borrow::Cow;
use std::fmt::Display;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use libafl::corpus::{Corpus, CorpusId, InMemoryCorpus, InMemoryOnDiskCorpus};
use libafl::events::SimpleEventManager;
use libafl::executors::{Executor, ExitKind, HasObservers};
use libafl::feedbacks::MaxMapFeedback;
use libafl::fuzzer::{Fuzzer, StdFuzzer};
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
use rand::SeedableRng;
use rand::rngs::SmallRng;
use serde::{Deserialize, Serialize};

use crate::coverage::CAPACITY;
use crate::interrupt::interrupted;
use crate::program::{Program, TypedOp};

const MUTATIONS_PER_PICK: usize = 8;
/// Runs that may fail in a row, as when the machine is briefly out of
/// resources, before the campaign gives up.
const FAILURES_IN_A_ROW: u32 = 3;
const STATUS_EVERY: Duration = Duration::from_secs(30);

pub struct Campaign<'a> {
    /// Directories of programs to start from.
    pub seeds: &'a [PathBuf],
    /// Where the programs found are kept; a campaign starts from them too.
    pub corpus: &'a Path,
    /// Until stopped, if `None`.
    pub duration: Option<Duration>,
}

/// `execute` runs a program and returns the edges it reached; anything else a
/// run produces, such as findings, is for `execute` to handle. `mutate`
/// edits a program, splicing from a donor program. The campaign returns
/// without an error once `stop` is set or the process is
/// [`interrupted`], after the run in progress.
pub fn fuzz<T: TypedOp, E: Display>(
    campaign: &Campaign<'_>,
    stop: &AtomicBool,
    execute: impl FnMut(&Program<T>) -> Result<Vec<u32>, E>,
    mutate: impl FnMut(&Program<T>, &Program<T>, &mut SmallRng) -> Program<T>,
) -> Result<(), String> {
    match run(campaign, stop, execute, mutate) {
        Ok(()) | Err(Error::ShuttingDown) => Ok(()),
        Err(e) => Err(e.to_string()),
    }
}

fn run<T: TypedOp, E: Display>(
    campaign: &Campaign<'_>,
    stop: &AtomicBool,
    execute: impl FnMut(&Program<T>) -> Result<Vec<u32>, E>,
    mutate: impl FnMut(&Program<T>, &Program<T>, &mut SmallRng) -> Program<T>,
) -> Result<(), Error> {
    let edges = OwnedMapObserver::new("edges", vec![0u8; CAPACITY]).track_indices();
    let mut feedback = MaxMapFeedback::new(&edges);
    let mut state = StdState::new(
        StdRand::with_seed(current_nanos()),
        InMemoryOnDiskCorpus::<Entry<T>>::with_meta_format(campaign.corpus, None)?,
        InMemoryCorpus::new(),
        &mut feedback,
        &mut (),
    )?;
    let scheduler = IndexesLenTimeMinimizerScheduler::new(&edges, QueueScheduler::new());
    let mut fuzzer = StdFuzzer::new(scheduler, feedback, ());
    let mut executor = ClosureExecutor {
        execute,
        stop,
        failures: 0,
        observers: tuple_list!(edges),
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
        return Err(Error::empty("no seed reached any edge"));
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
#[serde(transparent, bound = "T: TypedOp")]
struct Entry<T>(Program<T>);

impl<T: TypedOp> Input for Entry<T> {
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

impl<T: TypedOp> HasLen for Entry<T> {
    fn len(&self) -> usize {
        self.0.size()
    }
}

type Edges = ExplicitTracking<OwnedMapObserver<u8>, true, false>;

struct ClosureExecutor<'a, F> {
    execute: F,
    stop: &'a AtomicBool,
    /// In a row.
    failures: u32,
    observers: tuple_list_type!(Edges),
}

impl<F> HasObservers for ClosureExecutor<'_, F> {
    type Observers = tuple_list_type!(Edges);

    fn observers(&self) -> RefIndexable<&Self::Observers, Self::Observers> {
        RefIndexable::from(&self.observers)
    }

    fn observers_mut(&mut self) -> RefIndexable<&mut Self::Observers, Self::Observers> {
        RefIndexable::from(&mut self.observers)
    }
}

impl<EM, S, Z, T, E, F> Executor<EM, Entry<T>, S, Z> for ClosureExecutor<'_, F>
where
    S: HasExecutions,
    E: Display,
    F: FnMut(&Program<T>) -> Result<Vec<u32>, E>,
{
    fn run_target(
        &mut self,
        _fuzzer: &mut Z,
        state: &mut S,
        _manager: &mut EM,
        entry: &Entry<T>,
    ) -> Result<ExitKind, Error> {
        if self.stop.load(Ordering::Relaxed) || interrupted() {
            return Err(Error::shutting_down());
        }
        *state.executions_mut() += 1;
        match (self.execute)(&entry.0) {
            Ok(edges) => {
                self.failures = 0;
                let map = self.observers.0.as_mut();
                for edge in edges {
                    map[usize::try_from(edge).expect("fits")] = 1;
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

struct ClosureMutator<F> {
    mutate: F,
}

impl<F> Named for ClosureMutator<F> {
    fn name(&self) -> &Cow<'static, str> {
        static NAME: Cow<'static, str> = Cow::Borrowed("mutate");
        &NAME
    }
}

impl<S, T, F> Mutator<Entry<T>, S> for ClosureMutator<F>
where
    S: HasRand + HasCorpus<Entry<T>>,
    T: TypedOp,
    F: FnMut(&Program<T>, &Program<T>, &mut SmallRng) -> Program<T>,
{
    fn mutate(&mut self, state: &mut S, entry: &mut Entry<T>) -> Result<MutationResult, Error> {
        let mut rng = SmallRng::seed_from_u64(state.rand_mut().next());
        let id = random_corpus_id!(state.corpus(), state.rand_mut());
        let donor = state.corpus().cloned_input_for_id(id)?;
        entry.0 = (self.mutate)(&entry.0, &donor.0, &mut rng);
        Ok(MutationResult::Mutated)
    }

    fn post_exec(&mut self, _state: &mut S, _new: Option<CorpusId>) -> Result<(), Error> {
        Ok(())
    }
}
