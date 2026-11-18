/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! A [`System`] for a process that answers requests on a Unix socket, with
//! edge coverage from `shim/coverage.c` and findings from its log.

use std::collections::{BTreeMap, VecDeque};
use std::io::{self, ErrorKind, Read, Write};
use std::ops::ControlFlow;
use std::os::unix::net::UnixStream;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread::sleep;
use std::time::{Duration, Instant};

use serde::Serialize;
use tempfile::TempDir;

use crate::coverage::CoverageMap;
use crate::process::Process;
use crate::system::{Finding, Run, Session, System};

const READY_POLL: Duration = Duration::from_millis(1);
const IDLE_POLL: Duration = Duration::from_millis(10);
const READ_CHUNK: usize = 16 * 1024;
const LOG: &str = "server.log";
const SOCKET: &str = "server.sock";

/// How requests and replies look on the wire.
pub trait Codec: Sync {
    type Request: Sync;
    type Reply;

    fn encode(&self, request: &Self::Request, out: &mut Vec<u8>);

    /// The first reply in `buf` and its length, or `None` until it is
    /// complete. An error is a protocol violation.
    fn decode(&self, buf: &[u8]) -> Result<Option<(Self::Reply, usize)>, String>;
}

/// What an op of a model of a [`Service`] lowers to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Input<Q> {
    /// Requests sent one after another on a connection, each awaiting the
    /// reply to the one before; the last reply is the input's. `wait: false`
    /// leaves that reply to be read when the connection is next used.
    Send {
        conn: u8,
        wait: bool,
        requests: Vec<Q>,
    },
    /// Reads every outstanding reply, then waits until the process is
    /// [idle](Service::idle). It completes only if the process became idle
    /// within [`Timing::patience`].
    Settle,
}

/// Assembled by a target from its parts. Requests the harness itself sends
/// go over a connection of its own.
pub struct Service<C: Codec> {
    pub codec: C,
    /// The command that starts the process in a directory of its own,
    /// listening on the socket.
    pub launch: Launch,
    /// Answered once the process is ready.
    pub ready: C::Request,
    pub shutdown: C::Request,
    /// Undoes every pause a program can set, such as one that holds a reply
    /// back.
    pub heal: Hook<C, ()>,
    /// Whether the process has no background work left.
    pub idle: Hook<C, bool>,
    /// The failures the log reports.
    pub findings: LogProbe<Vec<Finding>>,
    /// Where the log says a crash or hang happened, to name it by.
    pub site: LogProbe<Option<String>>,
    pub timing: Timing,
}

pub type Launch = Box<dyn Fn(&Path, &Path) -> Command + Send + Sync>;
pub type Hook<C, T> = Box<dyn Fn(&mut Admin<'_, C>) -> T + Send + Sync>;
pub type LogProbe<T> = Box<dyn Fn(&str) -> T + Send + Sync>;

#[derive(Clone, Copy, Debug, Serialize)]
pub struct Timing {
    /// For the process to become ready.
    pub startup: Duration,
    /// For a reply, before healing; also how long [`Input::Settle`] waits to
    /// become idle.
    pub patience: Duration,
    /// For a reply after healing, and for a request to be written, before
    /// either is a hang.
    pub deadline: Duration,
    /// For the process to exit after shutdown, and to write its reports
    /// after a hang.
    pub exit: Duration,
}

impl Default for Timing {
    fn default() -> Timing {
        Timing {
            startup: Duration::from_secs(30),
            patience: Duration::from_secs(2),
            deadline: Duration::from_secs(10),
            exit: Duration::from_secs(30),
        }
    }
}

impl<C: Codec> System for Service<C> {
    type Input = Input<C::Request>;
    type Reply = C::Reply;
    type Session<'a>
        = Live<'a, C>
    where
        C: 'a;

    /// Starts the process and waits until it answers [`Service::ready`].
    fn start(&self) -> io::Result<Live<'_, C>> {
        let dir = tempfile::tempdir()?;
        let coverage = CoverageMap::new()?;
        let mut command = (self.launch)(dir.path(), &dir.path().join(SOCKET));
        coverage.attach(&mut command);
        let mut live = Live {
            service: self,
            process: Process::spawn(command, &dir.path().join(LOG))?,
            dir,
            coverage,
            admin: None,
            conns: BTreeMap::new(),
            hung: false,
        };
        let start = Instant::now();
        while start.elapsed() < self.timing.startup && live.process.exited().is_none() {
            if live.admin().call(&self.ready).is_some() {
                return Ok(live);
            }
            sleep(READY_POLL);
        }
        let log = std::fs::read(live.dir.path().join(LOG)).unwrap_or_default();
        Err(io::Error::other(format!(
            "the process did not start:\n{}",
            String::from_utf8_lossy(&log)
        )))
    }
}

/// The harness's own connection, for [`Service::heal`] and
/// [`Service::idle`].
pub struct Admin<'a, C: Codec> {
    conn: &'a mut Option<Conn>,
    socket: PathBuf,
    codec: &'a C,
    deadline: Duration,
}

impl<C: Codec> Admin<'_, C> {
    /// `None` if the process did not answer in time. The connection is then
    /// dropped, so a late reply never answers a later call.
    pub fn call(&mut self, request: &C::Request) -> Option<C::Reply> {
        let conn = match self.conn {
            Some(conn) => conn,
            None => self
                .conn
                .insert(Conn::open(&self.socket, self.deadline).ok()?),
        };
        let reply = conn
            .send(self.codec, request)
            .and_then(|()| conn.receive(self.codec, self.deadline))
            .ok();
        if reply.is_none() {
            *self.conn = None;
        }
        reply
    }
}

pub struct Live<'a, C: Codec> {
    service: &'a Service<C>,
    dir: TempDir,
    process: Process,
    coverage: CoverageMap,
    admin: Option<Conn>,
    conns: BTreeMap<u8, Conn>,
    hung: bool,
}

struct Conn {
    stream: UnixStream,
    buf: Vec<u8>,
    /// For each request awaiting a reply, oldest first, the input its reply
    /// completes, if it is the input's last request.
    pending: VecDeque<Option<usize>>,
}

enum Failure {
    Timeout,
    Closed,
    Invalid(String),
}

impl Conn {
    fn open(socket: &Path, write_timeout: Duration) -> io::Result<Conn> {
        let stream = UnixStream::connect(socket)?;
        stream.set_write_timeout(Some(write_timeout))?;
        Ok(Conn {
            stream,
            buf: Vec::new(),
            pending: VecDeque::new(),
        })
    }

    fn send<C: Codec>(&mut self, codec: &C, request: &C::Request) -> Result<(), Failure> {
        let mut out = Vec::new();
        codec.encode(request, &mut out);
        self.stream.write_all(&out).map_err(|e| failure(&e))
    }

    /// A timeout keeps the bytes read so far, so a later call still returns
    /// the reply.
    fn receive<C: Codec>(&mut self, codec: &C, timeout: Duration) -> Result<C::Reply, Failure> {
        let start = Instant::now();
        let mut chunk = [0u8; READ_CHUNK];
        loop {
            if let Some((reply, used)) = codec.decode(&self.buf).map_err(Failure::Invalid)? {
                self.buf.drain(..used);
                return Ok(reply);
            }
            let left = timeout
                .checked_sub(start.elapsed())
                .filter(|left| !left.is_zero())
                .ok_or(Failure::Timeout)?;
            self.stream
                .set_read_timeout(Some(left))
                .map_err(|_| Failure::Closed)?;
            match self.stream.read(&mut chunk) {
                Ok(0) => return Err(Failure::Closed),
                Ok(n) => self.buf.extend_from_slice(&chunk[..n]),
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) => return Err(failure(&e)),
            }
        }
    }
}

fn failure(e: &io::Error) -> Failure {
    match e.kind() {
        ErrorKind::WouldBlock | ErrorKind::TimedOut => Failure::Timeout,
        _ => Failure::Closed,
    }
}

impl<C: Codec> Live<'_, C> {
    fn socket(&self) -> PathBuf {
        self.dir.path().join(SOCKET)
    }

    fn admin(&mut self) -> Admin<'_, C> {
        Admin {
            socket: self.socket(),
            conn: &mut self.admin,
            codec: &self.service.codec,
            deadline: self.service.timing.deadline,
        }
    }

    fn heal(&mut self) {
        let heal = &self.service.heal;
        heal(&mut self.admin());
    }

    /// Reads the replies outstanding on `conn`, oldest first.
    fn drain(&mut self, conn: u8, run: &mut Run<C::Reply>) -> ControlFlow<()> {
        let Timing {
            patience, deadline, ..
        } = self.service.timing;
        while self.conns.get(&conn).is_some_and(|c| !c.pending.is_empty()) {
            let mut got = self.receive(conn, patience);
            if let Err(Failure::Timeout) = got {
                self.heal();
                got = self.receive(conn, deadline);
            }
            match got {
                Ok(reply) => {
                    let pending = &mut self.conns.get_mut(&conn).expect("open").pending;
                    if let Some(at) = pending.pop_front().expect("pending") {
                        run.done(at, Some(reply));
                    }
                }
                Err(Failure::Timeout) => {
                    self.hung = true;
                    return ControlFlow::Break(());
                }
                Err(Failure::Closed) => self.forget(conn)?,
                Err(Failure::Invalid(violation)) => {
                    run.notes.fail("protocol", violation);
                    self.forget(conn)?;
                }
            }
        }
        ControlFlow::Continue(())
    }

    fn drain_all(&mut self, run: &mut Run<C::Reply>) -> ControlFlow<()> {
        let conns: Vec<u8> = self.conns.keys().copied().collect();
        for conn in conns {
            self.drain(conn, run)?;
        }
        ControlFlow::Continue(())
    }

    fn receive(&mut self, conn: u8, timeout: Duration) -> Result<C::Reply, Failure> {
        let codec = &self.service.codec;
        self.conns
            .get_mut(&conn)
            .expect("a connection with pending replies is open")
            .receive(codec, timeout)
    }

    /// Drops a connection the process closed, with its outstanding replies.
    /// The run ends only if the process is exiting.
    fn forget(&mut self, conn: u8) -> ControlFlow<()> {
        self.conns.remove(&conn);
        match self.process.wait_exit(self.service.timing.patience) {
            Some(_) => ControlFlow::Break(()),
            None => ControlFlow::Continue(()),
        }
    }

    /// Sends `request` on `conn`, opening it if need be; its reply completes
    /// input `completes`, if any.
    fn send(
        &mut self,
        conn: u8,
        request: &C::Request,
        completes: Option<usize>,
    ) -> ControlFlow<()> {
        if !self.conns.contains_key(&conn) {
            match Conn::open(&self.socket(), self.service.timing.deadline) {
                Ok(opened) => {
                    self.conns.insert(conn, opened);
                }
                Err(_) => return self.forget(conn),
            }
        }
        let open = self.conns.get_mut(&conn).expect("open");
        match open.send(&self.service.codec, request) {
            Ok(()) => {
                open.pending.push_back(completes);
                ControlFlow::Continue(())
            }
            Err(Failure::Timeout) => {
                self.hung = true;
                ControlFlow::Break(())
            }
            Err(_) => self.forget(conn),
        }
    }

    /// Whether the process became idle in time.
    fn settle(&mut self) -> bool {
        let start = Instant::now();
        while start.elapsed() < self.service.timing.patience {
            let idle = &self.service.idle;
            if idle(&mut self.admin()) {
                return true;
            }
            sleep(IDLE_POLL);
        }
        false
    }
}

impl<C: Codec> Session for Live<'_, C> {
    type Input = Input<C::Request>;
    type Reply = C::Reply;

    fn step(&mut self, at: usize, input: &Self::Input, run: &mut Run<C::Reply>) -> ControlFlow<()> {
        match input {
            Input::Send {
                conn,
                wait,
                requests,
            } => {
                for (i, request) in requests.iter().enumerate() {
                    let completes = (i + 1 == requests.len()).then_some(at);
                    self.drain(*conn, run)?;
                    self.send(*conn, request, completes)?;
                }
                if *wait {
                    self.drain(*conn, run)?;
                }
                ControlFlow::Continue(())
            }
            Input::Settle => {
                self.drain_all(run)?;
                if self.settle() {
                    run.done(at, None);
                }
                ControlFlow::Continue(())
            }
        }
    }

    /// Heals, reads what is outstanding, and shuts the process down, then
    /// records its edges and the findings its exit and log show.
    fn stop(mut self, run: &mut Run<C::Reply>) {
        let service = self.service;
        let timing = service.timing;
        if !self.hung {
            self.heal();
            let _ = self.drain_all(run);
        }
        if !self.hung {
            if let Ok(mut conn) = Conn::open(&self.socket(), timing.deadline) {
                let _ = conn.send(&service.codec, &service.shutdown);
            }
            self.hung = self.process.wait_exit(timing.exit).is_none();
        }
        let Live {
            dir,
            process,
            coverage,
            hung,
            ..
        } = self;
        if hung {
            process.abort();
        }
        let status = process.reap(timing.exit);
        for edge in coverage.edges() {
            run.notes.saw("edge", u64::from(edge));
        }
        let log = std::fs::read(dir.path().join(LOG)).unwrap_or_default();
        let text = String::from_utf8_lossy(&log);
        for finding in (service.findings)(&text) {
            run.notes.fail(finding.kind, finding.key);
        }
        let site = (service.site)(&text);
        if hung {
            run.notes
                .fail("hang", site.unwrap_or_else(|| "no reply".to_owned()));
        } else if run.notes.findings.is_empty()
            && let Some(status) = status.filter(|status| !status.success())
        {
            let cause = match (status.signal(), status.code()) {
                (Some(signal), _) => format!("signal {signal}"),
                (None, code) => format!("exit {}", code.unwrap_or(-1)),
            };
            run.notes.fail("crash", site.unwrap_or(cause));
        }
        run.files.push((LOG.to_owned(), log));
    }
}
