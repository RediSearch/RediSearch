/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The coverage-guided loop. LibAFL supplies the corpus, edge novelty, and
//! scheduling; everything it runs goes through [`run`] and [`judge`].

use std::borrow::Cow;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use libafl::corpus::{Corpus, CorpusId, InMemoryCorpus, InMemoryOnDiskCorpus};
use libafl::events::SimpleEventManager;
use libafl::executors::{Executor, ExitKind, HasObservers};
use libafl::feedbacks::{Feedback, MaxMapFeedback, StateInitializer};
use libafl::fuzzer::{Fuzzer, StdFuzzer};
use libafl::inputs::Input;
use libafl::monitors::SimpleMonitor;
use libafl::mutators::{MutationResult, Mutator};
use libafl::observers::{CanTrack, ExplicitTracking, Observer, OwnedMapObserver};
use libafl::schedulers::{IndexesLenTimeMinimizerScheduler, QueueScheduler};
use libafl::stages::StdMutationalStage;
use libafl::state::{HasCorpus, HasExecutions, HasRand, StdState};
use libafl::{Error, random_corpus_id};
use libafl_bolts::rands::{Rand, StdRand};
use libafl_bolts::tuples::{Handle, Handled, MatchName, MatchNameRef, RefIndexable, tuple_list};
use libafl_bolts::{HasLen, Named, current_nanos};
use rand::SeedableRng;
use rand::rngs::SmallRng;
use serde::{Deserialize, Serialize};

use crate::bundle;
use crate::edit::{mutate, size};
use crate::judge::{Finding, execute};
use crate::program::Program;
use crate::run::{Exit, Trace};
use crate::target::Env;

/// Edge ids are guard numbers, fewer than this in the module.
const MAP: usize = 1 << 20;
const MUTATIONS_PER_PICK: usize = 8;
const STATUS_EVERY: Duration = Duration::from_secs(30);

/// One worker's campaign. A signature that already has a directory under
/// `findings` is known and not reported again; workers share `findings`, so a
/// new finding belongs to the worker that creates its directory first.
pub struct Campaign<'a> {
    pub worker: usize,
    pub env: Env,
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

type Observers = (
    ExplicitTracking<OwnedMapObserver<u8>, true, false>,
    (Verdict, ()),
);

pub fn fuzz(campaign: Campaign<'_>) -> Result<(), Error> {
    let edges = OwnedMapObserver::new("edges", vec![0u8; MAP]).track_indices();
    let verdict = Verdict::default();
    let mut feedback = MaxMapFeedback::new(&edges);
    std::fs::create_dir_all(campaign.findings)?;
    let mut objective = NewFinding {
        verdict: verdict.handle(),
        findings: campaign.findings.to_owned(),
        env: campaign.env.clone(),
    };
    let mut state = StdState::new(
        StdRand::with_seed(current_nanos()),
        InMemoryOnDiskCorpus::with_meta_format(campaign.corpus, None)?,
        InMemoryCorpus::new(),
        &mut feedback,
        &mut objective,
    )?;
    let scheduler = IndexesLenTimeMinimizerScheduler::new(&edges, QueueScheduler::new());
    let mut fuzzer = StdFuzzer::new(scheduler, feedback, objective);
    let mut executor = Target {
        env: campaign.env,
        observers: tuple_list!(edges, verdict),
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
        Edits,
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

/// The last run's trace and findings, for [`NewFinding`].
#[derive(Debug, Default, Serialize, Deserialize)]
struct Verdict {
    last: Option<(Trace, Vec<Finding>)>,
}

impl Named for Verdict {
    fn name(&self) -> &Cow<'static, str> {
        static NAME: Cow<'static, str> = Cow::Borrowed("verdict");
        &NAME
    }
}

impl<I, S> Observer<I, S> for Verdict {
    fn pre_exec(&mut self, _state: &mut S, _input: &I) -> Result<(), Error> {
        self.last = None;
        Ok(())
    }
}

struct Target {
    env: Env,
    observers: Observers,
}

impl HasObservers for Target {
    type Observers = Observers;

    fn observers(&self) -> RefIndexable<&Observers, Observers> {
        RefIndexable::from(&self.observers)
    }

    fn observers_mut(&mut self) -> RefIndexable<&mut Observers, Observers> {
        RefIndexable::from(&mut self.observers)
    }
}

impl<EM, S: HasExecutions, Z> Executor<EM, Program, S, Z> for Target {
    fn run_target(
        &mut self,
        _fuzzer: &mut Z,
        state: &mut S,
        _manager: &mut EM,
        program: &Program,
    ) -> Result<ExitKind, Error> {
        *state.executions_mut() += 1;
        let (trace, findings) = execute(program, &self.env).map_err(Error::unknown)?;
        let map = self.observers.0.as_mut();
        for &edge in &trace.coverage.edges {
            if let Ok(edge) = usize::try_from(edge) {
                map[edge % MAP] = 1;
            }
        }
        let exit = match trace.exit {
            Exit::Clean => ExitKind::Ok,
            Exit::Died { .. } => ExitKind::Crash,
            Exit::Hung { .. } => ExitKind::Timeout,
        };
        self.observers.1.0.last = Some((trace, findings));
        Ok(exit)
    }
}

/// The objective: a finding whose directory this worker could create.
struct NewFinding {
    verdict: Handle<Verdict>,
    findings: PathBuf,
    env: Env,
}

impl Named for NewFinding {
    fn name(&self) -> &Cow<'static, str> {
        static NAME: Cow<'static, str> = Cow::Borrowed("new_finding");
        &NAME
    }
}

impl<S> StateInitializer<S> for NewFinding {}

impl<EM, OT: MatchName, S> Feedback<EM, Program, OT, S> for NewFinding {
    fn is_interesting(
        &mut self,
        _state: &mut S,
        _manager: &mut EM,
        program: &Program,
        observers: &OT,
        _exit: &ExitKind,
    ) -> Result<bool, Error> {
        let verdict = observers
            .get(&self.verdict)
            .ok_or_else(|| Error::illegal_state("the verdict observer is missing"))?;
        let Some((trace, findings)) = &verdict.last else {
            return Ok(false);
        };
        let mut new = false;
        for finding in findings {
            let claimed = self.findings.join(bundle::dir_name(&finding.signature));
            if std::fs::create_dir(claimed).is_ok() {
                let dir = bundle::write(
                    &self.findings,
                    program,
                    None,
                    trace,
                    finding,
                    (1, 1),
                    &self.env,
                )
                .map_err(Error::unknown)?;
                println!(
                    "new finding: {:?} {} -> {}",
                    finding.kind,
                    finding.signature,
                    dir.display()
                );
                new = true;
            }
        }
        Ok(new)
    }
}

/// [`mutate`], with a donor for splicing drawn from the corpus.
struct Edits;

impl Named for Edits {
    fn name(&self) -> &Cow<'static, str> {
        static NAME: Cow<'static, str> = Cow::Borrowed("edits");
        &NAME
    }
}

impl<S: HasRand + HasCorpus<Program>> Mutator<Program, S> for Edits {
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
