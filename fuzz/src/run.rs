/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::collections::{BTreeMap, VecDeque};
use std::thread::sleep;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::coverage::Coverage;
use crate::lower::lower;
use crate::program::{Argv, Bytes, Conn, DEBUG, Op, OpKind, Program, ReplyMode, Var, argv};
use crate::resp::Value;
use crate::target::{Connection, Env, Reply, Server, Status};

/// How long a reply may take before the harness lifts the pauses and hooks
/// the program set, which may be what holds it back.
const RELEASE_AFTER: Duration = Duration::from_secs(2);
const REPLY_DEADLINE: Duration = Duration::from_secs(10);
const HARNESS_DEADLINE: Duration = Duration::from_secs(5);
const SHUTDOWN_DEADLINE: Duration = Duration::from_secs(30);
const POLL: Duration = Duration::from_millis(20);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Trace {
    pub steps: Vec<Step>,
    pub exit: Exit,
    pub status: Status,
    pub log: Bytes,
    pub coverage: Coverage,
}

/// A command the program sent, or a `BARRIER` or `RELEASE` the harness ran.
/// `sent` and `received` are ticks of one clock, so the steps between them
/// ran while the reply was pending. `reply` is `None` if it was never read.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Step {
    pub op: usize,
    pub conn: Conn,
    pub argv: Argv,
    pub sent: u32,
    pub received: Option<u32>,
    pub reply: Option<Reply>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Exit {
    Clean,
    Died { at_op: usize },
    Hung { at_op: usize },
}

pub fn run(program: &Program, env: &Env) -> Result<Trace, String> {
    let mut interpreter = Interpreter {
        server: Server::spawn(env, &program.module_args)?,
        conns: BTreeMap::new(),
        pending: BTreeMap::new(),
        vars: BTreeMap::new(),
        harness: None,
        steps: Vec::new(),
        clock: 0,
    };
    let end = program.ops.len();
    let exit = program
        .ops
        .iter()
        .enumerate()
        .find_map(|(at, op)| interpreter.exec(at, op))
        .or_else(|| interpreter.finish(end));
    if matches!(exit, Some(Exit::Hung { .. })) {
        interpreter.server.abort();
    }
    let reaped = interpreter.server.reap();
    let exit = exit.unwrap_or(if reaped.status.code == Some(0) {
        Exit::Clean
    } else {
        Exit::Died { at_op: end }
    });
    Ok(Trace {
        steps: interpreter.steps,
        exit,
        status: reaped.status,
        log: Bytes(reaped.log),
        coverage: reaped.coverage,
    })
}

struct Interpreter {
    server: Server,
    conns: BTreeMap<Conn, Connection>,
    /// Per connection, the steps awaiting a reply and the variable each
    /// reply binds.
    pending: BTreeMap<Conn, VecDeque<(usize, Option<Var>)>>,
    vars: BTreeMap<Var, i64>,
    /// The harness's own connection, outside the program's steps.
    harness: Option<Connection>,
    steps: Vec<Step>,
    clock: u32,
}

/// Each method returns `Some` when the program cannot go on.
impl Interpreter {
    fn exec(&mut self, at: usize, op: &Op) -> Option<Exit> {
        if op.kind == OpKind::Barrier {
            return self.barrier(at);
        }
        lower(&op.kind).into_iter().find_map(|command| {
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
            None => self.server.connect().and_then(|mut connection| {
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
                let released = self.release().is_some();
                self.record_harness_step(at, "RELEASE", sent, released);
                reply = self.receive(conn, REPLY_DEADLINE);
            }
            match reply {
                Reply::Value(ref value) => {
                    if let (Some(var), Some(id)) = (bind, cursor_id(value)) {
                        self.vars.insert(var, id);
                    }
                    self.record_reply(step, reply);
                }
                Reply::Timeout => {
                    self.record_reply(step, reply);
                    return Some(Exit::Hung { at_op: at });
                }
                Reply::Closed(_) | Reply::Invalid(_) => {
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

    fn receive(&mut self, conn: Conn, deadline: Duration) -> Reply {
        self.conns
            .get_mut(&conn)
            .expect("a connection with pending replies is open")
            .recv(deadline)
    }

    fn record_reply(&mut self, step: usize, reply: Reply) {
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
        reply: Reply,
        at: usize,
    ) -> Option<Exit> {
        self.record_reply(step, reply);
        self.conns.remove(&conn);
        self.pending.remove(&conn);
        self.server
            .wait_exit(HARNESS_DEADLINE)
            .map(|_| Exit::Died { at_op: at })
    }

    /// Reads every pending reply, then waits up to [`RELEASE_AFTER`] for the
    /// server to be idle. A barrier that times out is recorded, not a finding:
    /// the program may have paused what it waits for.
    fn barrier(&mut self, at: usize) -> Option<Exit> {
        if let Some(exit) = self.read_all_replies(at) {
            return Some(exit);
        }
        let sent = self.tick();
        let deadline = Instant::now() + RELEASE_AFTER;
        let idle = loop {
            match self.is_idle() {
                Some(true) => break true,
                Some(false) if Instant::now() < deadline => sleep(POLL),
                _ => break false,
            }
        };
        if idle {
            self.harness_call(argv(&[DEBUG, "GC_WAIT_FOR_JOBS"]));
        }
        self.record_harness_step(at, "BARRIER", sent, idle);
        self.server.exited().map(|_| Exit::Died { at_op: at })
    }

    /// Records `OK` if the harness action completed, `Timeout` if not.
    fn record_harness_step(&mut self, at: usize, name: &str, sent: u32, completed: bool) {
        let received = self.tick();
        self.steps.push(Step {
            op: at,
            conn: Conn::default(),
            argv: argv(&[name]),
            sent,
            received: Some(received),
            reply: Some(if completed {
                Reply::Value(Value::Simple("OK".into()))
            } else {
                Reply::Timeout
            }),
        });
    }

    fn is_idle(&mut self) -> Option<bool> {
        for name in self.indexes()? {
            let info = vec!["FT.INFO".into(), name];
            if field(&self.harness_call(info)?, "indexing") != Some(0) {
                return Some(false);
            }
        }
        let stats = self.harness_call(argv(&[DEBUG, "WORKERS", "STATS"]))?;
        Some(
            ["totalPendingJobs", "numJobsInProgress"]
                .iter()
                .all(|name| field(&stats, name) == Some(0)),
        )
    }

    fn finish(&mut self, end: usize) -> Option<Exit> {
        self.release();
        if let Some(exit) = self.read_all_replies(end) {
            return Some(exit);
        }
        if let Some(harness) = self.harness.as_mut() {
            let _ = harness.send(&argv(&["SHUTDOWN", "NOSAVE"]));
        }
        match self.server.wait_exit(SHUTDOWN_DEADLINE) {
            Some(_) => None,
            None => Some(Exit::Hung { at_op: end }),
        }
    }

    /// Lifts every pause and hook a program can set. Stops at the first
    /// command the server does not answer: its main thread is blocked, so
    /// the rest would time out too.
    fn release(&mut self) -> Option<()> {
        for command in [
            &[DEBUG, "WORKERS", "RESUME"][..],
            &[
                DEBUG,
                "BG_SCAN_CONTROLLER",
                "SET_PAUSE_BEFORE_SCAN",
                "false",
            ],
            &[DEBUG, "BG_SCAN_CONTROLLER", "SET_BG_INDEX_RESUME"],
            &[DEBUG, "SYNC_POINT", "CLEAR"],
        ] {
            self.harness_call(argv(command))?;
        }
        for name in self.indexes()? {
            let mut command = argv(&[DEBUG, "GC_CONTINUE_SCHEDULE"]);
            command.push(name);
            self.harness_call(command)?;
        }
        Some(())
    }

    fn indexes(&mut self) -> Option<Vec<Bytes>> {
        let Value::Array(names) = self.harness_call(argv(&["FT._LIST"]))? else {
            return None;
        };
        Some(
            names
                .into_iter()
                .filter_map(|name| match name {
                    Value::Bulk(name) | Value::Simple(name) => Some(name),
                    _ => None,
                })
                .collect(),
        )
    }

    fn harness_call(&mut self, command: Argv) -> Option<Value> {
        if self.harness.is_none() {
            self.harness = self.server.connect().ok();
        }
        let reply = self.harness.as_mut()?.call(&command, HARNESS_DEADLINE);
        if reply.is_err() {
            self.harness = None;
        }
        reply.ok()
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

/// `FT.AGGREGATE … WITHCURSOR` and `FT.CURSOR READ` reply `[results, id]`.
fn cursor_id(reply: &Value) -> Option<i64> {
    match reply {
        Value::Array(items) => match items.as_slice() {
            [_, Value::Int(id)] => Some(*id),
            _ => None,
        },
        _ => None,
    }
}

/// An integer in a flat `[name, value, …]` reply.
fn field(reply: &Value, name: &str) -> Option<i64> {
    let Value::Array(items) = reply else {
        return None;
    };
    items.chunks(2).find_map(|pair| match pair {
        [Value::Bulk(key) | Value::Simple(key), value] if key.as_slice() == name.as_bytes() => {
            match value {
                Value::Int(n) => Some(*n),
                Value::Bulk(text) | Value::Simple(text) => {
                    std::str::from_utf8(text.as_slice()).ok()?.parse().ok()
                }
                _ => None,
            }
        }
        _ => None,
    })
}
