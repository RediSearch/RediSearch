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
use crate::program::{Argv, Bytes, Conn, Op, OpKind, Program, ReplyMode, Var, argv};
use crate::resp::Value;
use crate::target::{Connection, Env, Reply, Server, Status};

const SETTLE: Duration = Duration::from_secs(2);
const REPLY_DEADLINE: Duration = Duration::from_secs(10);
const GRACE: Duration = Duration::from_secs(5);
const SHUTDOWN_DEADLINE: Duration = Duration::from_secs(30);
const POLL: Duration = Duration::from_millis(20);
const DEBUG: &str = "_FT.DEBUG";

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Trace {
    pub steps: Vec<Step>,
    pub exit: Exit,
    pub status: Status,
    pub log: Bytes,
    pub coverage: Coverage,
}

/// One command, or one barrier. `sent` and `received` are positions on a
/// single clock, so a deferred reply shows which steps ran while it was
/// pending. `reply` is `None` when the reply was never read.
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
    let mut it = Interpreter {
        server: Server::spawn(env, &program.module_args)?,
        conns: BTreeMap::new(),
        pending: BTreeMap::new(),
        vars: BTreeMap::new(),
        control: None,
        steps: Vec::new(),
        clock: 0,
    };
    let end = program.ops.len();
    let mut exit = program
        .ops
        .iter()
        .enumerate()
        .find_map(|(at, op)| it.exec(at, op));
    if !matches!(exit, Some(Exit::Died { .. } | Exit::Hung { .. })) {
        exit = it.finish(end).or(exit);
    }
    if matches!(exit, Some(Exit::Hung { .. })) {
        it.server.abort();
    }
    let reaped = it.server.reap();
    let exit = exit.unwrap_or(if reaped.status.code == Some(0) {
        Exit::Clean
    } else {
        Exit::Died { at_op: end }
    });
    Ok(Trace {
        steps: it.steps,
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
    control: Option<Connection>,
    steps: Vec<Step>,
    clock: u32,
}

impl Interpreter {
    fn exec(&mut self, at: usize, op: &Op) -> Option<Exit> {
        if op.kind == OpKind::Barrier {
            return self.barrier(at);
        }
        if let Some(exit) = self.drain(op.conn, at) {
            return Some(exit);
        }
        lower(&op.kind).into_iter().find_map(|command| {
            let command = command
                .into_iter()
                .map(|token| self.resolve(token))
                .collect();
            self.drain(op.conn, at)
                .or_else(|| self.send(op.conn, at, command, op.bind))
                .or_else(|| match op.reply {
                    ReplyMode::Await => self.drain(op.conn, at),
                    ReplyMode::Defer => None,
                })
        })
    }

    fn send(&mut self, conn: Conn, at: usize, command: Argv, binds: Option<Var>) -> Option<Exit> {
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
                    .push_back((step, binds));
                None
            }
            Err(e) => self.dropped(conn, step, Reply::Closed(e), at),
        }
    }

    /// Reads every pending reply on `conn`. A reply that does not arrive in
    /// time gets one more chance after the harness releases the scheduling
    /// controls the program holds, and the program then carries on.
    fn drain(&mut self, conn: Conn, at: usize) -> Option<Exit> {
        while let Some((step, binds)) = self.pending.get_mut(&conn).and_then(VecDeque::pop_front) {
            let mut reply = self.read(conn, SETTLE);
            if reply == Reply::Timeout {
                let sent = self.tick();
                let released = self.release().is_some();
                self.marker(at, "RELEASE", sent, released);
                reply = self.read(conn, REPLY_DEADLINE);
            }
            match reply {
                Reply::Value(ref value) => {
                    if let (Some(var), Some(id)) = (binds, cursor_id(value)) {
                        self.vars.insert(var, id);
                    }
                    self.record(step, reply);
                }
                Reply::Timeout => {
                    self.record(step, reply);
                    return Some(Exit::Hung { at_op: at });
                }
                Reply::Closed(_) | Reply::Invalid(_) => {
                    if let Some(exit) = self.dropped(conn, step, reply, at) {
                        return Some(exit);
                    }
                }
            }
        }
        None
    }

    fn drain_all(&mut self, at: usize) -> Option<Exit> {
        let conns: Vec<Conn> = self.pending.keys().copied().collect();
        conns.into_iter().find_map(|conn| self.drain(conn, at))
    }

    fn read(&mut self, conn: Conn, deadline: Duration) -> Reply {
        self.conns
            .get_mut(&conn)
            .expect("a connection with pending replies is open")
            .recv(deadline)
    }

    fn record(&mut self, step: usize, reply: Reply) {
        let received = self.tick();
        self.steps[step].received = Some(received);
        self.steps[step].reply = Some(reply);
    }

    /// Records why `conn` stopped working and forgets it; its next use opens a
    /// new one. The program ends here only if the server is exiting.
    fn dropped(&mut self, conn: Conn, step: usize, reply: Reply, at: usize) -> Option<Exit> {
        self.record(step, reply);
        self.conns.remove(&conn);
        self.pending.remove(&conn);
        self.server
            .wait_exit(GRACE)
            .map(|_| Exit::Died { at_op: at })
    }

    fn barrier(&mut self, at: usize) -> Option<Exit> {
        if let Some(exit) = self.drain_all(at) {
            return Some(exit);
        }
        let sent = self.tick();
        let deadline = Instant::now() + SETTLE;
        let quiet = loop {
            match self.quiescent() {
                Some(true) => break true,
                Some(false) if Instant::now() < deadline => sleep(POLL),
                _ => break false,
            }
        };
        if quiet {
            self.control(argv(&[DEBUG, "GC_WAIT_FOR_JOBS"]));
        }
        self.marker(at, "BARRIER", sent, quiet);
        self.server.exited().map(|_| Exit::Died { at_op: at })
    }

    /// Records an action of the harness itself as a step: `OK` if it
    /// completed, `Timeout` if it did not.
    fn marker(&mut self, at: usize, name: &str, sent: u32, completed: bool) {
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

    fn quiescent(&mut self) -> Option<bool> {
        for name in self.indexes()? {
            let mut info = argv(&["FT.INFO"]);
            info.push(name);
            if field(&self.control(info)?, "indexing") != Some(0) {
                return Some(false);
            }
        }
        let stats = self.control(argv(&[DEBUG, "WORKERS", "STATS"]))?;
        Some(
            ["totalPendingJobs", "numJobsInProgress"]
                .iter()
                .all(|name| field(&stats, name) == Some(0)),
        )
    }

    /// Ends a program that ran to completion: releases held controls, reads
    /// the replies still pending, and shuts the server down.
    fn finish(&mut self, end: usize) -> Option<Exit> {
        self.release();
        if let Some(exit @ (Exit::Died { .. } | Exit::Hung { .. })) = self.drain_all(end) {
            return Some(exit);
        }
        if let Some(control) = self.control.as_mut() {
            let _ = control.send(&argv(&["SHUTDOWN", "NOSAVE"]));
        }
        match self.server.wait_exit(SHUTDOWN_DEADLINE) {
            Some(_) => None,
            None => Some(Exit::Hung { at_op: end }),
        }
    }

    /// Undoes every scheduling control a program can hold. Stops at the first
    /// command the server does not answer: its main thread is blocked, and the
    /// rest would time out too.
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
            self.control(argv(command))?;
        }
        for name in self.indexes()? {
            let mut command = argv(&[DEBUG, "GC_CONTINUE_SCHEDULE"]);
            command.push(name);
            self.control(command)?;
        }
        Some(())
    }

    fn indexes(&mut self) -> Option<Vec<Bytes>> {
        match self.control(argv(&["FT._LIST"]))? {
            Value::Array(names) => Some(
                names
                    .into_iter()
                    .filter_map(|name| match name {
                        Value::Bulk(name) | Value::Simple(name) => Some(name),
                        _ => None,
                    })
                    .collect(),
            ),
            _ => None,
        }
    }

    /// Sends a harness command on a connection of its own, outside the
    /// program's steps.
    fn control(&mut self, command: Argv) -> Option<Value> {
        if self.control.is_none() {
            self.control = self.server.connect().ok();
        }
        let reply = self.control.as_mut()?.call(&command, GRACE);
        if reply.is_err() {
            self.control = None;
        }
        reply.ok()
    }

    /// A `$<n>` token becomes the value bound to variable `n`, or 0.
    fn resolve(&self, token: Bytes) -> Bytes {
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

/// The cursor id in a reply to `FT.AGGREGATE … WITHCURSOR` or
/// `FT.CURSOR READ`: `[results, id]`.
fn cursor_id(reply: &Value) -> Option<i64> {
    match reply {
        Value::Array(items) => match items.as_slice() {
            [_, Value::Int(id)] => Some(*id),
            _ => None,
        },
        _ => None,
    }
}

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
