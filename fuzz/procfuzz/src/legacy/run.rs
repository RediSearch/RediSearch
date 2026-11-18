/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Runs a program against a fresh server and records what happened.

use std::collections::{BTreeMap, VecDeque};
use std::os::unix::process::ExitStatusExt;
use std::process::ExitStatus;
use std::thread::sleep;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::coverage::{Coverage, CoverageMap};
use crate::interrupt::interrupted;
use crate::legacy::judge;
use crate::legacy::program::{Argv, Bytes, Conn, Kind, Op, Program, ReplyMode, TypedOp, Var, argv};
use crate::legacy::server::{Connection, Harness, Reply, Server};
use crate::process::Process;

const STARTUP_DEADLINE: Duration = Duration::from_secs(30);
/// How long a reply may take before the harness releases the pauses and
/// hooks the program set, which may be what holds it back.
const RELEASE_AFTER: Duration = Duration::from_secs(2);
const REPLY_DEADLINE: Duration = Duration::from_secs(10);
const EXIT_DEADLINE: Duration = Duration::from_secs(30);
const POLL: Duration = Duration::from_millis(20);

#[derive(Clone, Debug)]
pub struct Execution<V> {
    pub trace: Trace<V>,
    pub log: Vec<u8>,
    pub coverage: Coverage,
    /// Signatures, `<kind>:<detail>`, that stay the same across runs and
    /// builds.
    pub findings: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Trace<V> {
    pub steps: Vec<Step<V>>,
    pub exit: Exit,
    pub status: Status,
}

/// A command a program sent, or a `BARRIER` or `RELEASE` the harness ran.
/// `sent` and `received` are ticks of one clock, so the steps between them
/// ran while the reply was pending. `reply` is `None` if it was never read.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Step<V> {
    pub op: usize,
    pub conn: Conn,
    pub argv: Argv,
    pub sent: u32,
    pub received: Option<u32>,
    pub reply: Option<Reply<V>>,
}

impl<V> Step<V> {
    /// Whether the harness ran this step rather than the program.
    pub fn is_harness(&self) -> bool {
        self.argv.len() == 1 && [&argv(&["BARRIER"]), &argv(&["RELEASE"])].contains(&&self.argv)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Exit {
    Clean,
    Died { at_op: usize },
    Hung { at_op: usize },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Status {
    pub code: Option<i32>,
    pub signal: Option<i32>,
}

impl From<Option<ExitStatus>> for Status {
    fn from(status: Option<ExitStatus>) -> Status {
        Status {
            code: status.and_then(|s| s.code()),
            signal: status.and_then(|s| s.signal()),
        }
    }
}

/// Runs `program` on a fresh server and judges the run for sanitizer
/// reports, crashes, hangs, and protocol violations. An error means the
/// server did not start, or the process was [`interrupted`] before.
pub fn execute<S: Server, T: TypedOp>(
    server: &S,
    program: &Program<T>,
) -> Result<Execution<S::Value>, String> {
    if interrupted() {
        return Err("interrupted".to_owned());
    }
    let dir = tempfile::tempdir().map_err(|e| e.to_string())?;
    let socket = dir.path().join("server.sock");
    let log = dir.path().join("server.log");
    let coverage = CoverageMap::new().map_err(|e| format!("coverage map: {e}"))?;
    let mut command = server.command(dir.path(), &socket, &program.args);
    coverage.attach(&mut command);
    let process = Process::spawn(command, &log).map_err(|e| format!("starting the server: {e}"))?;
    let mut interpreter = Interpreter {
        server,
        process,
        harness: Harness::new(&socket),
        socket,
        conns: BTreeMap::new(),
        pending: BTreeMap::new(),
        vars: BTreeMap::new(),
        steps: Vec::new(),
        clock: 0,
    };
    if !interpreter.started() {
        let log = std::fs::read(&log).unwrap_or_default();
        return Err(format!(
            "server did not start:\n{}",
            String::from_utf8_lossy(&log)
        ));
    }
    let end = program.ops.len();
    let exit = program
        .ops
        .iter()
        .enumerate()
        .find_map(|(at, op)| interpreter.exec(at, op))
        .or_else(|| interpreter.finish(end));
    if matches!(exit, Some(Exit::Hung { .. })) {
        interpreter.process.abort();
    }
    let status = Status::from(interpreter.process.reap(EXIT_DEADLINE));
    let exit = exit.unwrap_or(if status.code == Some(0) {
        Exit::Clean
    } else {
        Exit::Died { at_op: end }
    });
    let trace = Trace {
        steps: interpreter.steps,
        exit,
        status,
    };
    let log = std::fs::read(&log).unwrap_or_default();
    let findings = judge::safety(server, program, &trace, &String::from_utf8_lossy(&log));
    Ok(Execution {
        trace,
        log,
        coverage: coverage.read(),
        findings,
    })
}

struct Interpreter<'a, S: Server> {
    server: &'a S,
    process: Process,
    harness: Harness<S>,
    socket: std::path::PathBuf,
    conns: BTreeMap<Conn, Connection<S>>,
    /// Per connection, the steps awaiting a reply and the variable each
    /// reply binds.
    pending: BTreeMap<Conn, VecDeque<(usize, Option<Var>)>>,
    vars: BTreeMap<Var, i64>,
    steps: Vec<Step<S::Value>>,
    clock: u32,
}

/// Each method returning `Option<Exit>` returns `Some` when the program
/// cannot go on.
impl<S: Server> Interpreter<'_, S> {
    fn started(&mut self) -> bool {
        let start = Instant::now();
        while start.elapsed() < STARTUP_DEADLINE && self.process.exited().is_none() {
            if let Ok(mut conn) = Connection::<S>::open(&self.socket)
                && conn.call(&self.server.ping(), STARTUP_DEADLINE).is_some()
            {
                return true;
            }
            sleep(POLL);
        }
        false
    }

    fn exec<T: TypedOp>(&mut self, at: usize, op: &Op<T>) -> Option<Exit> {
        if op.kind == Kind::Barrier {
            return self.barrier(at);
        }
        op.kind.lower().into_iter().find_map(|command| {
            let command = command
                .into_iter()
                .map(|token| self.substitute(token))
                .collect();
            self.read_replies(op.conn, at)
                .or_else(|| self.send(op.conn, at, command, op.bind))
                .or_else(|| match op.reply {
                    ReplyMode::Await => self.read_replies(op.conn, at),
                    ReplyMode::Defer => None,
                })
        })
    }

    fn send(&mut self, conn: Conn, at: usize, command: Argv, bind: Option<Var>) -> Option<Exit> {
        let sent = self.tick();
        let result = match self.conns.get_mut(&conn) {
            Some(connection) => connection.send(&command),
            None => Connection::open(&self.socket).and_then(|mut connection| {
                connection.send(&command)?;
                self.conns.insert(conn, connection);
                Ok(())
            }),
        };
        self.steps.push(Step {
            op: at,
            conn,
            argv: command,
            sent,
            received: None,
            reply: None,
        });
        let step = self.steps.len() - 1;
        match result {
            Ok(()) => {
                self.pending
                    .entry(conn)
                    .or_default()
                    .push_back((step, bind));
                None
            }
            Err(e) => self.forget_connection(conn, step, Reply::Closed(e), at),
        }
    }

    /// A reply that does not arrive within [`RELEASE_AFTER`] gets
    /// [`REPLY_DEADLINE`] more after a release; one that still does not
    /// arrive is a hang.
    fn read_replies(&mut self, conn: Conn, at: usize) -> Option<Exit> {
        while let Some((step, bind)) = self.pending.get_mut(&conn).and_then(VecDeque::pop_front) {
            let mut reply = self.receive(conn, RELEASE_AFTER);
            if reply == Reply::Timeout {
                let sent = self.tick();
                let released = self.server.release(&mut self.harness).is_some();
                self.record_harness_step(at, "RELEASE", sent, released);
                reply = self.receive(conn, REPLY_DEADLINE);
            }
            match reply {
                Reply::Value(ref value) => {
                    if let (Some(var), Some(bound)) = (bind, self.server.bound(value)) {
                        self.vars.insert(var, bound);
                    }
                    self.record_reply(step, reply);
                }
                Reply::Timeout => {
                    self.record_reply(step, reply);
                    return Some(Exit::Hung { at_op: at });
                }
                Reply::Closed(_) | Reply::Invalid(_) | Reply::Done => {
                    if let Some(exit) = self.forget_connection(conn, step, reply, at) {
                        return Some(exit);
                    }
                }
            }
        }
        None
    }

    fn read_all_replies(&mut self, at: usize) -> Option<Exit> {
        let conns: Vec<Conn> = self.pending.keys().copied().collect();
        conns
            .into_iter()
            .find_map(|conn| self.read_replies(conn, at))
    }

    fn receive(&mut self, conn: Conn, timeout: Duration) -> Reply<S::Value> {
        self.conns
            .get_mut(&conn)
            .expect("a connection with pending replies is open")
            .receive(timeout)
    }

    fn record_reply(&mut self, step: usize, reply: Reply<S::Value>) {
        let received = self.tick();
        self.steps[step].received = Some(received);
        self.steps[step].reply = Some(reply);
    }

    /// The connection's next use opens a new one. The program ends here only
    /// if the server is exiting.
    fn forget_connection(
        &mut self,
        conn: Conn,
        step: usize,
        reply: Reply<S::Value>,
        at: usize,
    ) -> Option<Exit> {
        self.record_reply(step, reply);
        self.conns.remove(&conn);
        self.pending.remove(&conn);
        self.process
            .wait_exit(RELEASE_AFTER)
            .map(|_| Exit::Died { at_op: at })
    }

    /// A barrier that times out is recorded, not a finding: the program may
    /// have paused what it waits for.
    fn barrier(&mut self, at: usize) -> Option<Exit> {
        if let Some(exit) = self.read_all_replies(at) {
            return Some(exit);
        }
        let sent = self.tick();
        let deadline = Instant::now() + RELEASE_AFTER;
        let idle = loop {
            match self.server.idle(&mut self.harness) {
                Some(true) => break true,
                Some(false) if Instant::now() < deadline => sleep(POLL),
                _ => break false,
            }
        };
        self.record_harness_step(at, "BARRIER", sent, idle);
        self.process.exited().map(|_| Exit::Died { at_op: at })
    }

    fn record_harness_step(&mut self, at: usize, name: &str, sent: u32, completed: bool) {
        let received = self.tick();
        self.steps.push(Step {
            op: at,
            conn: Conn::default(),
            argv: argv(&[name]),
            sent,
            received: Some(received),
            reply: Some(if completed {
                Reply::Done
            } else {
                Reply::Timeout
            }),
        });
    }

    fn finish(&mut self, end: usize) -> Option<Exit> {
        let _ = self.server.release(&mut self.harness);
        if let Some(exit) = self.read_all_replies(end) {
            return Some(exit);
        }
        self.harness.send(&self.server.shutdown());
        match self.process.wait_exit(EXIT_DEADLINE) {
            Some(_) => None,
            None => Some(Exit::Hung { at_op: end }),
        }
    }

    fn substitute(&self, token: Bytes) -> Bytes {
        let var = std::str::from_utf8(token.as_slice())
            .ok()
            .and_then(|t| t.strip_prefix('$')?.parse().ok())
            .map(Var);
        match var {
            Some(var) => self.vars.get(&var).copied().unwrap_or(0).to_string().into(),
            None => token,
        }
    }

    fn tick(&mut self) -> u32 {
        self.clock += 1;
        self.clock
    }
}
