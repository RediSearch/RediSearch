/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The LibAFL engine: its corpus, max-map feedback over hashed features,
//! and the length-and-time minimizing scheduler.

use std::borrow::Cow;
use std::fmt::Display;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::num::NonZeroUsize;
use std::path::Path;
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
use serde::{Deserialize, Serialize};

use super::{Campaign, Halt, Op, Outcome, Runner};
use crate::draw::Draw;
use crate::model::Program;
use crate::system::Feature;

/// Features are hashed into a map of this many entries.
const MAP: usize = 1 << 20;
const MUTATIONS_PER_PICK: usize = 8;
const STATUS_EVERY: Duration = Duration::from_secs(30);

pub(super) fn search<O: Op, E: Display, F>(
    campaign: &Campaign<'_>,
    runner: Runner<'_, F, O>,
    mutate: impl FnMut(&Program<O>, &Program<O>, &mut Draw) -> Program<O>,
) -> Result<Outcome, crate::error::Error>
where
    F: FnMut(&Program<O>) -> Result<Vec<Feature>, E>,
{
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
    let mut executor = RunnerExecutor {
        runner,
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
            fuzzer.add_input(
                &mut state,
                &mut executor,
                &mut manager,
                Entry(Program::default()),
            )?;
        }
        loop {
            for (program, features) in executor.runner.imports() {
                let map = executor.observers.0.as_mut();
                map.fill(0);
                for feature in features {
                    map[slot(feature)] = 1;
                }
                fuzzer.evaluate_execution(
                    &mut state,
                    &mut manager,
                    &Entry(program),
                    &executor.observers,
                    &ExitKind::Ok,
                    false,
                )?;
            }
            fuzzer.fuzz_one(&mut stages, &mut executor, &mut state, &mut manager)?;
        }
    })();
    match result {
        Ok(()) | Err(Error::ShuttingDown) => Ok(Outcome {
            runs: executor.runner.runs,
            kept: state.corpus().count(),
        }),
        Err(e) => Err(e.into()),
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

struct RunnerExecutor<'a, F, O> {
    runner: Runner<'a, F, O>,
    observers: tuple_list_type!(Features),
}

impl<F, O> HasObservers for RunnerExecutor<'_, F, O> {
    type Observers = tuple_list_type!(Features);

    fn observers(&self) -> RefIndexable<&Self::Observers, Self::Observers> {
        RefIndexable::from(&self.observers)
    }

    fn observers_mut(&mut self) -> RefIndexable<&mut Self::Observers, Self::Observers> {
        RefIndexable::from(&mut self.observers)
    }
}

impl<EM, S, Z, O, E, F> Executor<EM, Entry<O>, S, Z> for RunnerExecutor<'_, F, O>
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
        let before = self.runner.runs;
        let observed = self.runner.observe(&entry.0);
        *state.executions_mut() += self.runner.runs - before;
        match observed {
            Ok(Some(observed)) => {
                let map = self.observers.0.as_mut();
                for feature in observed.features {
                    map[slot(feature)] = 1;
                }
                Ok(ExitKind::Ok)
            }
            Ok(None) => Ok(ExitKind::Ok),
            Err(Halt::Done) => Err(Error::shutting_down()),
            Err(Halt::Failed(e)) => Err(Error::unknown(e)),
        }
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
