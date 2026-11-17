/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The coverage-guided loop. LibAFL keeps the corpus of programs that reached
//! new edges and picks which to mutate next; findings never enter it.

use std::borrow::Cow;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
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

use crate::bundle;
use crate::judge::execute;
use crate::mutate::mutate;
use crate::program::Program;
use crate::shrink::size;
use crate::target::Env;

/// Larger than the number of edges in the module.
const MAP: usize = 1 << 20;
const MUTATIONS_PER_PICK: usize = 8;
const STATUS_EVERY: Duration = Duration::from_secs(30);

/// Workers share `findings`: the first to create a finding's directory
/// reports it, and a signature that already has one is not reported again.
pub struct Campaign<'a> {
    pub worker: usize,
    pub env: &'a Env,
    pub seeds: &'a [PathBuf],
    pub corpus: &'a Path,
    pub findings: &'a Path,
    pub duration: Option<Duration>,
}

impl Input for Program {
    fn to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), Error> {
        Ok(std::fs::write(path, self.to_json())?)
    }

    fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        Program::from_json(&std::fs::read_to_string(path)?).map_err(Error::illegal_argument)
    }
}

impl HasLen for Program {
    fn len(&self) -> usize {
        size(self)
    }
}

type Edges = ExplicitTracking<OwnedMapObserver<u8>, true, false>;

pub fn fuzz(campaign: Campaign<'_>) -> Result<(), Error> {
    let edges = OwnedMapObserver::new("edges", vec![0u8; MAP]).track_indices();
    let mut feedback = MaxMapFeedback::new(&edges);
    std::fs::create_dir_all(campaign.findings)?;
    let mut state = StdState::new(
        StdRand::with_seed(current_nanos()),
        InMemoryOnDiskCorpus::with_meta_format(campaign.corpus, None)?,
        InMemoryCorpus::new(),
        &mut feedback,
        &mut (),
    )?;
    let scheduler = IndexesLenTimeMinimizerScheduler::new(&edges, QueueScheduler::new());
    let mut fuzzer = StdFuzzer::new(scheduler, feedback, ());
    let mut executor = ServerExecutor {
        env: campaign.env,
        findings: campaign.findings,
        observers: tuple_list!(edges),
    };
    let worker = campaign.worker;
    let mut last_status = Instant::now();
    let mut manager = SimpleEventManager::new(SimpleMonitor::new(move |line: &str| {
        if line.starts_with("[UserStats") && last_status.elapsed() >= STATUS_EVERY {
            last_status = Instant::now();
            println!("worker {worker}: {line}");
        }
    }));
    let mut stages = tuple_list!(StdMutationalStage::with_max_iterations(
        ProgramMutator,
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

struct ServerExecutor<'a> {
    env: &'a Env,
    findings: &'a Path,
    observers: tuple_list_type!(Edges),
}

impl HasObservers for ServerExecutor<'_> {
    type Observers = tuple_list_type!(Edges);

    fn observers(&self) -> RefIndexable<&Self::Observers, Self::Observers> {
        RefIndexable::from(&self.observers)
    }

    fn observers_mut(&mut self) -> RefIndexable<&mut Self::Observers, Self::Observers> {
        RefIndexable::from(&mut self.observers)
    }
}

impl<EM, S: HasExecutions, Z> Executor<EM, Program, S, Z> for ServerExecutor<'_> {
    fn run_target(
        &mut self,
        _fuzzer: &mut Z,
        state: &mut S,
        _manager: &mut EM,
        program: &Program,
    ) -> Result<ExitKind, Error> {
        *state.executions_mut() += 1;
        let (trace, findings) = execute(program, self.env).map_err(Error::unknown)?;
        let map = self.observers.0.as_mut();
        for &edge in &trace.coverage.edges {
            map[usize::try_from(edge).expect("fits") % MAP] = 1;
        }
        for finding in findings {
            let claimed = self.findings.join(bundle::dir_name(&finding.signature));
            if std::fs::create_dir(claimed).is_ok() {
                let dir = bundle::write(
                    self.findings,
                    program,
                    None,
                    &trace,
                    &finding,
                    (1, 1),
                    self.env,
                )
                .map_err(Error::unknown)?;
                println!("new finding: {} -> {}", finding.signature, dir.display());
            }
        }
        Ok(ExitKind::Ok)
    }
}

/// [`mutate`], splicing from a random corpus entry.
struct ProgramMutator;

impl Named for ProgramMutator {
    fn name(&self) -> &Cow<'static, str> {
        static NAME: Cow<'static, str> = Cow::Borrowed("program_mutator");
        &NAME
    }
}

impl<S: HasRand + HasCorpus<Program>> Mutator<Program, S> for ProgramMutator {
    fn mutate(&mut self, state: &mut S, program: &mut Program) -> Result<MutationResult, Error> {
        let mut rng = SmallRng::seed_from_u64(state.rand_mut().next());
        let id = random_corpus_id!(state.corpus(), state.rand_mut());
        let donor = state.corpus().cloned_input_for_id(id)?;
        *program = mutate(program, &donor, &mut rng);
        Ok(MutationResult::Mutated)
    }

    fn post_exec(&mut self, _state: &mut S, _new: Option<CorpusId>) -> Result<(), Error> {
        Ok(())
    }
}
