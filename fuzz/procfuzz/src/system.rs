/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The system under test, and running a program against it.

use std::fmt;
use std::io;
use std::ops::ControlFlow;

use serde::Serialize;

use crate::interrupt::interrupted;
use crate::model::{Model, Program};

/// What can start sessions. Any `Fn() -> io::Result<impl Session>` is one.
pub trait System: Sync {
    type Input;
    type Reply;
    type Session<'a>: Session<Input = Self::Input, Reply = Self::Reply>
    where
        Self: 'a;

    fn start(&self) -> io::Result<Self::Session<'_>>;
}

/// One started system.
pub trait Session {
    type Input;
    type Reply;

    /// Performs input `at`, completing it in `run` now or in a later step.
    /// `Err` means the harness broke and the run is void; `Break` means the
    /// system ended.
    fn step(
        &mut self,
        at: usize,
        input: &Self::Input,
        run: &mut Run<Self::Reply>,
    ) -> io::Result<ControlFlow<()>>;

    /// Stops the system and records what shows only at the end.
    fn stop(self, run: &mut Run<Self::Reply>) -> io::Result<()>;
}

impl<F, S> System for F
where
    F: Fn() -> io::Result<S> + Sync,
    S: Session,
{
    type Input = S::Input;
    type Reply = S::Reply;
    type Session<'a>
        = S
    where
        F: 'a;

    fn start(&self) -> io::Result<S> {
        self()
    }
}

/// What a run did and what it showed.
#[derive(Clone, Debug)]
pub struct Run<R> {
    /// One per op performed, in program order.
    pub steps: Vec<Step<R>>,
    pub notes: Notes,
    /// Named files worth keeping with a finding, such as the system's log.
    pub files: Vec<(String, Vec<u8>)>,
    clock: u32,
}

/// `sent` and `done` are ticks of one clock, so the steps between them ran
/// while the input was outstanding. `done` is `None` if the input never
/// completed.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Step<R> {
    pub sent: u32,
    pub done: Option<u32>,
    pub reply: Option<R>,
}

#[derive(Clone, Debug, Default)]
pub struct Notes {
    pub features: Vec<Feature>,
    pub findings: Vec<Finding>,
}

/// Something worth reaching: an edge, a kind of reply, a shape of state.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Feature {
    pub space: &'static str,
    pub id: u64,
}

/// A failure, identified so that it is the same across runs and builds.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Finding {
    pub kind: &'static str,
    pub key: String,
}

impl fmt::Display for Finding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.kind, self.key)
    }
}

impl Notes {
    pub fn saw(&mut self, space: &'static str, id: u64) {
        self.features.push(Feature { space, id });
    }

    /// Whether the run showed the finding named `name`.
    pub fn shows(&self, name: &str) -> bool {
        self.findings
            .iter()
            .any(|finding| finding.to_string() == name)
    }

    pub fn fail(&mut self, kind: &'static str, key: impl Into<String>) {
        let finding = Finding {
            kind,
            key: key.into(),
        };
        if !self.findings.contains(&finding) {
            self.findings.push(finding);
        }
    }
}

impl<R> Run<R> {
    fn new() -> Run<R> {
        Run {
            steps: Vec::new(),
            notes: Notes::default(),
            files: Vec::new(),
            clock: 0,
        }
    }

    fn tick(&mut self) -> u32 {
        self.clock += 1;
        self.clock
    }

    /// Completes input `at`, with its reply if it has one.
    pub fn done(&mut self, at: usize, reply: Option<R>) {
        let tick = self.tick();
        let step = &mut self.steps[at];
        step.done = Some(tick);
        step.reply = reply;
    }
}

/// Runs `program` on a fresh session and judges it. Each op is lowered
/// against the state its completed predecessors left, folded in the order
/// they completed.
pub fn run<M, S>(model: &M, system: &S, program: &Program<M::Op>) -> io::Result<Run<M::Reply>>
where
    M: Model,
    S: System<Input = M::Input, Reply = M::Reply>,
{
    if interrupted() {
        return Err(io::Error::new(io::ErrorKind::Interrupted, "interrupted"));
    }
    let mut session = system.start()?;
    let mut run = Run::new();
    let mut live = M::State::default();
    let mut folded = 0;
    for (at, made) in program.ops.iter().enumerate() {
        let input = model.lower(&live, &made.op);
        let sent = run.tick();
        run.steps.push(Step {
            sent,
            done: None,
            reply: None,
        });
        let flow = session.step(at, &input, &mut run)?;
        folded = fold_completed(model, program, &run, &mut live, folded);
        if flow.is_break() {
            break;
        }
    }
    session.stop(&mut run)?;
    model.check(&program.ops(), &run.steps, &mut run.notes);
    Ok(run)
}

/// Calls `visit` with each completed step's index and the state the steps
/// completed before it left, in the order they completed. This is the state
/// a run's ops were lowered against, for [`Model::check`] to judge replies
/// by.
pub fn in_completion_order<M: Model>(
    model: &M,
    ops: &[M::Op],
    steps: &[Step<M::Reply>],
    mut visit: impl FnMut(usize, &M::State),
) {
    let mut state = M::State::default();
    for at in completed_since(steps, 0) {
        visit(at, &state);
        model.apply(&mut state, &ops[at], steps[at].reply.as_ref());
    }
}

/// Folds the steps completed after the tick `since` into `live` and returns
/// the latest tick folded.
fn fold_completed<M: Model>(
    model: &M,
    program: &Program<M::Op>,
    run: &Run<M::Reply>,
    live: &mut M::State,
    since: u32,
) -> u32 {
    let mut latest = since;
    for at in completed_since(&run.steps, since) {
        let step = &run.steps[at];
        model.apply(live, &program.ops[at].op, step.reply.as_ref());
        latest = latest.max(step.done.expect("completed"));
    }
    latest
}

/// The indexes of the steps completed after the tick `since`, in the order
/// they completed.
fn completed_since<R>(steps: &[Step<R>], since: u32) -> Vec<usize> {
    let mut completed: Vec<(u32, usize)> = steps
        .iter()
        .enumerate()
        .filter_map(|(at, step)| {
            step.done
                .filter(|&done| done > since)
                .map(|done| (done, at))
        })
        .collect();
    completed.sort_unstable();
    completed.into_iter().map(|(_, at)| at).collect()
}
