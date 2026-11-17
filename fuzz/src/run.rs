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
use crate::lower::{DEBUG_COMMAND, lower, words};
use crate::program::{Argv, Bytes, Conn, Op, OpKind, Program, ReplyMode};
use crate::resp::Value;
use crate::target::{Connection, Env, Received, Server, Status};

const CONTROL_DEADLINE: Duration = Duration::from_secs(5);
const DEATH_GRACE: Duration = Duration::from_secs(2);
const REAP_DEADLINE: Duration = Duration::from_secs(10);
const BARRIER_POLL: Duration = Duration::from_millis(20);
const BARRIER_ARGV: &str = "BARRIER";

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Trace {
    pub steps: Vec<Step>,
    pub exit: Exit,
    pub status: Option<Status>,
    pub log: Bytes,
    pub coverage: Coverage,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Step {
    pub op: usize,
    pub conn: Conn,
    pub argv: Argv,
    pub sent: u32,
    pub received: Option<u32>,
    pub reply: Reply,
    pub micros: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Reply {
    Value(Value),
    Timeout,
    Closed(String),
    Invalid(String),
    Inconclusive,
    NotRead,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Exit {
    Clean,
    Died { at_op: usize },
    Hung { at_op: usize },
    Stalled { at_op: usize },
    StartupFailed { reason: String },
}

#[derive(Clone, Copy)]
enum Interrupt {
    Died,
    Hung,
    Stalled,
    Broken,
}

#[derive(Clone, Copy, PartialEq)]
enum Released {
    No,
    Yes,
}

pub fn run(program: &Program, env: &Env) -> Trace {
    let mut server = match Server::spawn(env, &program.setup) {
        Ok(server) => server,
        Err(reason) => {
            return Trace {
                steps: Vec::new(),
                exit: Exit::StartupFailed { reason },
                status: None,
                log: Bytes::default(),
                coverage: Coverage::default(),
            };
        }
    };
    let mut interpreter = Interpreter {
        env,
        conns: BTreeMap::new(),
        pending: BTreeMap::new(),
        control: None,
        steps: Vec::new(),
        sent_at: Vec::new(),
        clock: 0,
        started: Instant::now(),
    };

    let interrupted = program
        .ops
        .iter()
        .enumerate()
        .find_map(|(index, op)| interpreter.exec(&mut server, index, op).map(|i| (index, i)));

    let end = program.ops.len();
    let exit = match interrupted {
        Some((at_op, Interrupt::Died)) => {
            server.wait_exit(env.shutdown_deadline);
            Exit::Died { at_op }
        }
        Some((at_op, Interrupt::Hung)) => {
            server.abort();
            server.wait_exit(env.shutdown_deadline);
            Exit::Hung { at_op }
        }
        other => match interpreter.finish(&mut server) {
            Some(Interrupt::Died) => Exit::Died { at_op: end },
            Some(_) => {
                server.abort();
                server.wait_exit(env.shutdown_deadline);
                Exit::Hung { at_op: end }
            }
            None => match other {
                Some((at_op, Interrupt::Stalled)) => Exit::Stalled { at_op },
                _ => Exit::Clean,
            },
        },
    };
    let reaped = server.reap(REAP_DEADLINE);
    let exit = match exit {
        Exit::Clean if !reaped.status.is_some_and(|s| s.success()) => Exit::Died { at_op: end },
        exit => exit,
    };
    Trace {
        steps: interpreter.steps,
        exit,
        status: reaped.status,
        log: Bytes(reaped.log),
        coverage: reaped.coverage,
    }
}

struct Interpreter<'a> {
    env: &'a Env,
    conns: BTreeMap<Conn, Connection>,
    pending: BTreeMap<Conn, VecDeque<usize>>,
    control: Option<Connection>,
    steps: Vec<Step>,
    sent_at: Vec<u64>,
    clock: u32,
    started: Instant,
}

impl Interpreter<'_> {
    fn tick(&mut self) -> u32 {
        self.clock += 1;
        self.clock
    }

    fn exec(&mut self, server: &mut Server, index: usize, op: &Op) -> Option<Interrupt> {
        if op.kind == OpKind::Barrier {
            return self.barrier(server, index);
        }
        for argv in lower(&op.kind) {
            if let Some(interrupt) = self.collect_conn(server, op.conn, Released::No) {
                return Some(interrupt);
            }
            let step = self.send(server, index, op.conn, argv);
            if self.steps[step].reply != Reply::NotRead {
                if server.wait_exit(DEATH_GRACE).is_some() {
                    return Some(Interrupt::Died);
                }
                continue;
            }
            self.pending.entry(op.conn).or_default().push_back(step);
            if op.reply == ReplyMode::Await
                && let Some(interrupt) = self.collect_conn(server, op.conn, Released::No)
            {
                return Some(interrupt);
            }
        }
        None
    }

    fn send(&mut self, server: &Server, op: usize, conn: Conn, argv: Argv) -> usize {
        let sent = self.tick();
        let reply = match self.connection(server, conn) {
            Ok(connection) => match connection.send(&argv) {
                Ok(()) => Reply::NotRead,
                Err(e) => Reply::Closed(e),
            },
            Err(e) => Reply::Closed(e),
        };
        if reply != Reply::NotRead {
            self.conns.remove(&conn);
        }
        self.steps.push(Step {
            op,
            conn,
            argv,
            sent,
            received: None,
            reply,
            micros: 0,
        });
        self.sent_at.push(self.elapsed_micros());
        self.steps.len() - 1
    }

    fn collect_conn(
        &mut self,
        server: &mut Server,
        conn: Conn,
        released: Released,
    ) -> Option<Interrupt> {
        while let Some(&step) = self.pending.get(&conn).and_then(VecDeque::front) {
            if let Some(interrupt) = self.collect(server, conn, step, released) {
                return Some(interrupt);
            }
        }
        None
    }

    fn collect_all(&mut self, server: &mut Server, released: Released) -> Option<Interrupt> {
        let conns: Vec<Conn> = self.pending.keys().copied().collect();
        conns
            .into_iter()
            .find_map(|conn| self.collect_conn(server, conn, released))
    }

    fn collect(
        &mut self,
        server: &mut Server,
        conn: Conn,
        step: usize,
        released: Released,
    ) -> Option<Interrupt> {
        let deadline = self.env.reply_deadline;
        let mut received = self.read(conn, deadline);
        let mut interrupt = None;
        if matches!(received, Received::Timeout) && released == Released::No {
            self.release(server);
            received = self.read(conn, self.env.release_grace);
            interrupt = Some(Interrupt::Stalled);
        }
        let (reply, interrupt) = match received {
            Received::Value(value) => (Reply::Value(value), interrupt),
            Received::Invalid(e) => (Reply::Invalid(e), Some(Interrupt::Broken)),
            Received::Timeout => (Reply::Timeout, Some(Interrupt::Hung)),
            Received::Closed(e) => {
                self.conns.remove(&conn);
                let unread: Vec<usize> = self.pending.remove(&conn).into_iter().flatten().collect();
                for other in unread {
                    self.steps[other].reply = Reply::Closed(e.clone());
                }
                let died = server.wait_exit(DEATH_GRACE).is_some();
                (Reply::Closed(e), died.then_some(Interrupt::Died))
            }
        };
        if let Some(queue) = self.pending.get_mut(&conn) {
            queue.retain(|&s| s != step);
        }
        let received_at = self.tick();
        let micros = self.elapsed_micros().saturating_sub(self.sent_at[step]);
        let entry = &mut self.steps[step];
        entry.reply = reply;
        entry.received = Some(received_at);
        entry.micros = micros;
        interrupt
    }

    fn read(&mut self, conn: Conn, deadline: Duration) -> Received {
        match self.conns.get_mut(&conn) {
            Some(connection) => connection.recv(deadline),
            None => Received::Closed("connection is gone".to_owned()),
        }
    }

    fn barrier(&mut self, server: &mut Server, index: usize) -> Option<Interrupt> {
        if let Some(interrupt) = self.collect_all(server, Released::No) {
            return Some(interrupt);
        }
        let sent = self.tick();
        let start = self.elapsed_micros();
        let deadline = Instant::now() + self.env.reply_deadline;
        let quiet = loop {
            match self.quiescent(server) {
                Some(true) => break true,
                Some(false) if Instant::now() < deadline => sleep(BARRIER_POLL),
                _ => break false,
            }
        };
        if quiet && let Some(control) = self.control(server) {
            let _ = control.call(
                &words(&[DEBUG_COMMAND, "GC_WAIT_FOR_JOBS"]),
                CONTROL_DEADLINE,
            );
        }
        let received = self.tick();
        let micros = self.elapsed_micros().saturating_sub(start);
        self.sent_at.push(start);
        self.steps.push(Step {
            op: index,
            conn: Conn::default(),
            argv: words(&[BARRIER_ARGV]),
            sent,
            received: Some(received),
            reply: if quiet {
                Reply::Value(Value::Simple("OK".into()))
            } else {
                Reply::Inconclusive
            },
            micros,
        });
        if server.try_exit().is_some() {
            return Some(Interrupt::Died);
        }
        None
    }

    fn quiescent(&mut self, server: &Server) -> Option<bool> {
        let control = self.control(server)?;
        let Ok(Value::Array(names)) = control.call(&words(&["FT._LIST"]), CONTROL_DEADLINE) else {
            return None;
        };
        for name in names {
            let (Value::Bulk(name) | Value::Simple(name)) = name else {
                continue;
            };
            let mut info = words(&["FT.INFO"]);
            info.push(name);
            let Ok(reply) = control.call(&info, CONTROL_DEADLINE) else {
                return None;
            };
            if field(&reply, "indexing").is_some_and(|v| v != 0) {
                return Some(false);
            }
        }
        let stats = control
            .call(
                &words(&[DEBUG_COMMAND, "WORKERS", "STATS"]),
                CONTROL_DEADLINE,
            )
            .ok()?;
        let busy = ["totalPendingJobs", "numJobsInProgress"]
            .iter()
            .any(|name| field(&stats, name).is_some_and(|v| v != 0));
        Some(!busy)
    }

    fn finish(&mut self, server: &mut Server) -> Option<Interrupt> {
        self.release(server);
        if let Some(interrupt) = self.collect_all(server, Released::Yes) {
            return Some(interrupt);
        }
        if let Some(control) = self.control(server) {
            let _ = control.send(&words(&["SHUTDOWN", "NOSAVE"]));
        }
        match server.wait_exit(self.env.shutdown_deadline) {
            Some(_) => None,
            None => Some(Interrupt::Hung),
        }
    }

    fn connection(&mut self, server: &Server, conn: Conn) -> Result<&mut Connection, String> {
        if !self.conns.contains_key(&conn) {
            let connection = server.connect(self.env.reply_deadline)?;
            self.conns.insert(conn, connection);
        }
        Ok(self.conns.get_mut(&conn).expect("inserted above"))
    }

    fn control(&mut self, server: &Server) -> Option<&mut Connection> {
        if self.control.is_none() {
            self.control = server.connect(CONTROL_DEADLINE).ok();
        }
        self.control.as_mut()
    }

    fn release(&mut self, server: &Server) {
        let Some(control) = self.control(server) else {
            return;
        };
        for command in [
            &[DEBUG_COMMAND, "WORKERS", "RESUME"][..],
            &[
                DEBUG_COMMAND,
                "BG_SCAN_CONTROLLER",
                "SET_PAUSE_BEFORE_SCAN",
                "false",
            ],
            &[DEBUG_COMMAND, "BG_SCAN_CONTROLLER", "SET_BG_INDEX_RESUME"],
            &[DEBUG_COMMAND, "SYNC_POINT", "CLEAR"],
        ] {
            let _ = control.call(&words(command), CONTROL_DEADLINE);
        }
        let Ok(Value::Array(names)) = control.call(&words(&["FT._LIST"]), CONTROL_DEADLINE) else {
            return;
        };
        for name in names {
            if let Value::Bulk(name) | Value::Simple(name) = name {
                let mut command = words(&[DEBUG_COMMAND, "GC_CONTINUE_SCHEDULE"]);
                command.push(name);
                let _ = control.call(&command, CONTROL_DEADLINE);
            }
        }
    }

    fn elapsed_micros(&self) -> u64 {
        u64::try_from(self.started.elapsed().as_micros()).unwrap_or(u64::MAX)
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
                Value::Bulk(text) | Value::Simple(text) => std::str::from_utf8(text.as_slice())
                    .ok()?
                    .trim()
                    .parse()
                    .ok(),
                _ => None,
            }
        }
        _ => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pairs(items: &[(&str, Value)]) -> Value {
        Value::Array(
            items
                .iter()
                .flat_map(|(k, v)| [Value::Bulk((*k).into()), v.clone()])
                .collect(),
        )
    }

    #[test]
    fn field_reads_integers_and_numeric_strings() {
        let reply = pairs(&[
            ("indexing", Value::Int(0)),
            ("numJobsInProgress", Value::Bulk("3".into())),
            ("other", Value::Simple("x".into())),
        ]);
        assert_eq!(field(&reply, "indexing"), Some(0));
        assert_eq!(field(&reply, "numJobsInProgress"), Some(3));
        assert_eq!(field(&reply, "other"), None);
        assert_eq!(field(&reply, "missing"), None);
        assert_eq!(field(&Value::Nil, "indexing"), None);
    }
}
