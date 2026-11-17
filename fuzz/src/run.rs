/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::lower::lower;
use crate::program::{Argv, Bytes, Conn, Program, ReplyMode};
use crate::resp::Value;
use crate::target::{Connection, Env, Received, Server, Status};

const CONTROL_DEADLINE: Duration = Duration::from_secs(5);
const DEATH_GRACE: Duration = Duration::from_secs(2);
const REAP_DEADLINE: Duration = Duration::from_secs(10);
const DEBUG_COMMAND: &str = "_FT.DEBUG";

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Trace {
    pub steps: Vec<Step>,
    pub exit: Exit,
    pub status: Option<Status>,
    pub log: Bytes,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Step {
    pub op: usize,
    pub argv: Argv,
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

pub fn run(program: &Program, env: &Env) -> Trace {
    let mut server = match Server::spawn(env, &program.setup) {
        Ok(server) => server,
        Err(reason) => {
            return Trace {
                steps: Vec::new(),
                exit: Exit::StartupFailed { reason },
                status: None,
                log: Bytes::default(),
            };
        }
    };
    let mut interpreter = Interpreter {
        env,
        conns: BTreeMap::new(),
        control: None,
        steps: Vec::new(),
    };

    let mut interrupted = None;
    'ops: for (index, op) in program.ops.iter().enumerate() {
        assert_eq!(
            op.reply,
            ReplyMode::Await,
            "deferred replies are not implemented yet"
        );
        for argv in lower(&op.kind) {
            if let Some(interrupt) = interpreter.step(&mut server, index, op.conn, argv) {
                interrupted = Some((index, interrupt));
                break 'ops;
            }
        }
    }

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
        other => {
            if interpreter.shutdown(&mut server) {
                match other {
                    Some((at_op, Interrupt::Stalled)) => Exit::Stalled { at_op },
                    _ => Exit::Clean,
                }
            } else {
                server.abort();
                server.wait_exit(env.shutdown_deadline);
                Exit::Hung { at_op: end }
            }
        }
    };
    let (status, log) = server.reap(REAP_DEADLINE);
    let exit = match exit {
        Exit::Clean if !status.is_some_and(|s| s.success()) => Exit::Died { at_op: end },
        exit => exit,
    };
    Trace {
        steps: interpreter.steps,
        exit,
        status,
        log: Bytes(log),
    }
}

struct Interpreter<'a> {
    env: &'a Env,
    conns: BTreeMap<Conn, Connection>,
    control: Option<Connection>,
    steps: Vec<Step>,
}

impl Interpreter<'_> {
    fn step(
        &mut self,
        server: &mut Server,
        op: usize,
        conn: Conn,
        argv: Argv,
    ) -> Option<Interrupt> {
        let start = Instant::now();
        let deadline = self.env.reply_deadline;
        let received = match self.connection(server, conn) {
            Ok(connection) => match connection.send(&argv) {
                Ok(()) => connection.recv(deadline),
                Err(e) => Received::Closed(e),
            },
            Err(e) => Received::Closed(e),
        };
        let (reply, interrupt) = match received {
            Received::Value(value) => (Reply::Value(value), None),
            Received::Invalid(e) => (Reply::Invalid(e), Some(Interrupt::Broken)),
            Received::Closed(e) => {
                self.conns.remove(&conn);
                let died = server.wait_exit(DEATH_GRACE).is_some();
                (Reply::Closed(e), died.then_some(Interrupt::Died))
            }
            Received::Timeout => {
                self.release(server);
                match self
                    .conns
                    .get_mut(&conn)
                    .map(|c| c.recv(self.env.release_grace))
                {
                    Some(Received::Value(value)) => (Reply::Value(value), Some(Interrupt::Stalled)),
                    Some(Received::Invalid(e)) => (Reply::Invalid(e), Some(Interrupt::Broken)),
                    Some(Received::Closed(e)) => (Reply::Closed(e), Some(Interrupt::Died)),
                    _ => (Reply::Timeout, Some(Interrupt::Hung)),
                }
            }
        };
        self.steps.push(Step {
            op,
            argv,
            reply,
            micros: u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX),
        });
        interrupt
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
        let _ = control.call(
            &argv(&[DEBUG_COMMAND, "WORKERS", "RESUME"]),
            CONTROL_DEADLINE,
        );
        let Ok(Value::Array(names)) = control.call(&argv(&["FT._LIST"]), CONTROL_DEADLINE) else {
            return;
        };
        for name in names {
            if let Value::Bulk(name) | Value::Simple(name) = name {
                let mut command = argv(&[DEBUG_COMMAND, "GC_CONTINUE_SCHEDULE"]);
                command.push(name);
                let _ = control.call(&command, CONTROL_DEADLINE);
            }
        }
    }

    fn shutdown(&mut self, server: &mut Server) -> bool {
        self.release(server);
        let deadline = self.env.shutdown_deadline;
        if let Some(control) = self.control(server) {
            let _ = control.send(&argv(&["SHUTDOWN", "NOSAVE"]));
        }
        server.wait_exit(deadline).is_some()
    }
}

fn argv(words: &[&str]) -> Argv {
    words.iter().map(|w| Bytes::from(*w)).collect()
}
