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

use tempfile::TempDir;

use crate::coverage::CoverageMap;
use crate::process::Process;
use crate::sanitizer;
use crate::system::{Finding, Run, Session, System};

const READY_POLL: Duration = Duration::from_millis(1);
const IDLE_POLL: Duration = Duration::from_millis(10);
const READ_CHUNK: usize = 16 * 1024;
const LOG: &str = "server.log";

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
    /// Launch arguments. They count only before the first other input,
    /// which launches the process.
    Boot(Vec<String>),
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
    /// listening on a socket, with the boot arguments.
    pub launch: Launch,
    /// Answered once the process is ready.
    pub ready: C::Request,
    pub shutdown: C::Request,
    /// Undoes every pause a program can set, such as one that holds a reply
    /// back.
    pub heal: Hook<C, ()>,
    /// Whether the process has no background work left.
    pub idle: Hook<C, bool>,
    /// The failure a line of the log reports. Only the first line that
    /// reports one counts: reporting a failure can cause others.
    pub failure: LogProbe<Finding>,
    /// Where the log says a crash or hang happened, to name it by.
    pub site: LogProbe<String>,
    pub timing: Timing,
}

pub type Launch = Box<dyn Fn(&Path, &Path, &[String]) -> Command + Send + Sync>;
pub type Hook<C, T> = Box<dyn Fn(&mut Admin<'_, C>) -> T + Send + Sync>;
pub type LogProbe<T> = Box<dyn Fn(&str) -> Option<T> + Send + Sync>;

#[derive(Clone, Copy, Debug)]
pub struct Timing {
    /// For the process to become ready.
    pub startup: Duration,
    /// For a reply, before healing; also how long [`Input::Settle`] waits to
    /// become idle.
    pub patience: Duration,
    /// For a reply after healing, before it is a hang.
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

impl<C: Codec> Service<C> {
    /// A service that never pauses, is always idle, and reports
    /// AddressSanitizer errors.
    pub fn new(
        codec: C,
        launch: impl Fn(&Path, &Path, &[String]) -> Command + Send + Sync + 'static,
        ready: C::Request,
        shutdown: C::Request,
    ) -> Service<C> {
        Service {
            codec,
            launch: Box::new(launch),
            ready,
            shutdown,
            heal: Box::new(|_| ()),
            idle: Box::new(|_| true),
            failure: Box::new(sanitizer::error),
            site: Box::new(|_| None),
            timing: Timing::default(),
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

    fn start(&self) -> io::Result<Live<'_, C>> {
        let dir = tempfile::tempdir()?;
        let socket = dir.path().join("server.sock");
        Ok(Live {
            service: self,
            dir,
            socket,
            boot: Vec::new(),
            up: None,
            conns: BTreeMap::new(),
            pending: BTreeMap::new(),
            hung: false,
        })
    }
}

/// The harness's own connection, for [`Service::heal`] and
/// [`Service::idle`].
pub struct Admin<'a, C: Codec> {
    conn: &'a mut Conn,
    codec: &'a C,
    deadline: Duration,
}

impl<C: Codec> Admin<'_, C> {
    /// `None` if the process did not answer in time.
    pub fn call(&mut self, request: &C::Request) -> Option<C::Reply> {
        self.conn.send(self.codec, request).ok()?;
        match self.conn.receive(self.codec, self.deadline) {
            Got::Reply(reply) => Some(reply),
            Got::Timeout | Got::Closed | Got::Invalid(_) => None,
        }
    }
}

pub struct Live<'a, C: Codec> {
    service: &'a Service<C>,
    dir: TempDir,
    socket: PathBuf,
    boot: Vec<String>,
    up: Option<Up>,
    conns: BTreeMap<u8, Conn>,
    /// Per connection, the inputs awaiting a reply, oldest first.
    pending: BTreeMap<u8, VecDeque<usize>>,
    hung: bool,
}

struct Up {
    process: Process,
    coverage: CoverageMap,
    admin: Conn,
}

struct Conn {
    stream: UnixStream,
    buf: Vec<u8>,
}

enum Got<R> {
    Reply(R),
    Timeout,
    Closed,
    Invalid(String),
}

impl Conn {
    fn open(socket: &Path) -> io::Result<Conn> {
        Ok(Conn {
            stream: UnixStream::connect(socket)?,
            buf: Vec::new(),
        })
    }

    fn send<C: Codec>(&mut self, codec: &C, request: &C::Request) -> io::Result<()> {
        let mut out = Vec::new();
        codec.encode(request, &mut out);
        self.stream.write_all(&out)
    }

    /// A timeout keeps the bytes read so far, so a later call still returns
    /// the reply.
    fn receive<C: Codec>(&mut self, codec: &C, timeout: Duration) -> Got<C::Reply> {
        let start = Instant::now();
        let mut chunk = [0u8; READ_CHUNK];
        loop {
            match codec.decode(&self.buf) {
                Ok(Some((reply, used))) => {
                    self.buf.drain(..used);
                    return Got::Reply(reply);
                }
                Ok(None) => {}
                Err(e) => return Got::Invalid(e),
            }
            let Some(left) = timeout
                .checked_sub(start.elapsed())
                .filter(|left| !left.is_zero())
            else {
                return Got::Timeout;
            };
            if self.stream.set_read_timeout(Some(left)).is_err() {
                return Got::Closed;
            }
            match self.stream.read(&mut chunk) {
                Ok(0) => return Got::Closed,
                Ok(n) => self.buf.extend_from_slice(&chunk[..n]),
                Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {
                    return Got::Timeout;
                }
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(_) => return Got::Closed,
            }
        }
    }
}

impl<C: Codec> Live<'_, C> {
    /// Starts the process unless it is running, with the boot arguments
    /// collected so far.
    fn launch(&mut self) -> io::Result<()> {
        if self.up.is_none() {
            self.up = Some(self.spawn()?);
        }
        Ok(())
    }

    fn spawn(&self) -> io::Result<Up> {
        let service = self.service;
        let coverage = CoverageMap::new()?;
        let mut command = (service.launch)(self.dir.path(), &self.socket, &self.boot);
        coverage.attach(&mut command);
        let log = self.dir.path().join(LOG);
        let mut process = Process::spawn(command, &log)?;
        let start = Instant::now();
        while start.elapsed() < service.timing.startup && process.exited().is_none() {
            if let Ok(mut admin) = Conn::open(&self.socket)
                && admin.send(&service.codec, &service.ready).is_ok()
                && let Got::Reply(_) = admin.receive(&service.codec, service.timing.startup)
            {
                return Ok(Up {
                    process,
                    coverage,
                    admin,
                });
            }
            sleep(READY_POLL);
        }
        let log = std::fs::read(&log).unwrap_or_default();
        Err(io::Error::other(format!(
            "the process did not start:\n{}",
            String::from_utf8_lossy(&log)
        )))
    }

    fn admin(&mut self) -> Option<Admin<'_, C>> {
        let up = self.up.as_mut()?;
        Some(Admin {
            conn: &mut up.admin,
            codec: &self.service.codec,
            deadline: self.service.timing.deadline,
        })
    }

    fn heal(&mut self) {
        let service = self.service;
        if let Some(mut admin) = self.admin() {
            (service.heal)(&mut admin);
        }
    }

    /// Reads the replies outstanding on `conn`, oldest first.
    fn drain(&mut self, conn: u8, run: &mut Run<C::Reply>) -> ControlFlow<()> {
        let timing = self.service.timing;
        while let Some(&at) = self.pending.get(&conn).and_then(VecDeque::front) {
            let mut got = self.receive(conn, timing.patience);
            if let Got::Timeout = got {
                self.heal();
                got = self.receive(conn, timing.deadline);
            }
            match got {
                Got::Reply(reply) => {
                    self.pending.get_mut(&conn).expect("pending").pop_front();
                    run.done(at, Some(reply));
                }
                Got::Timeout => {
                    self.hung = true;
                    return ControlFlow::Break(());
                }
                Got::Closed => self.forget(conn)?,
                Got::Invalid(violation) => {
                    run.notes.fail("protocol", violation);
                    self.forget(conn)?;
                }
            }
        }
        ControlFlow::Continue(())
    }

    fn drain_all(&mut self, run: &mut Run<C::Reply>) -> ControlFlow<()> {
        let conns: Vec<u8> = self.pending.keys().copied().collect();
        for conn in conns {
            self.drain(conn, run)?;
        }
        ControlFlow::Continue(())
    }

    fn receive(&mut self, conn: u8, timeout: Duration) -> Got<C::Reply> {
        self.conns
            .get_mut(&conn)
            .expect("a connection with pending replies is open")
            .receive(&self.service.codec, timeout)
    }

    /// Drops a connection the process closed, with its outstanding replies.
    /// The run ends only if the process is exiting.
    fn forget(&mut self, conn: u8) -> ControlFlow<()> {
        self.conns.remove(&conn);
        self.pending.remove(&conn);
        let patience = self.service.timing.patience;
        let exited = self
            .up
            .as_mut()
            .is_some_and(|up| up.process.wait_exit(patience).is_some());
        if exited {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    }

    fn send(&mut self, at: usize, conn: u8, request: &C::Request) -> io::Result<ControlFlow<()>> {
        if !self.conns.contains_key(&conn) {
            match Conn::open(&self.socket) {
                Ok(opened) => {
                    self.conns.insert(conn, opened);
                }
                Err(_) => return Ok(self.forget(conn)),
            }
        }
        let codec = &self.service.codec;
        let sent = self
            .conns
            .get_mut(&conn)
            .expect("open")
            .send(codec, request);
        Ok(match sent {
            Ok(()) => {
                self.pending.entry(conn).or_default().push_back(at);
                ControlFlow::Continue(())
            }
            Err(_) => self.forget(conn),
        })
    }

    /// Whether the process became idle in time.
    fn settle(&mut self) -> bool {
        let service = self.service;
        let start = Instant::now();
        while start.elapsed() < service.timing.patience {
            let Some(mut admin) = self.admin() else {
                return false;
            };
            if (service.idle)(&mut admin) {
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

    fn step(
        &mut self,
        at: usize,
        input: &Self::Input,
        run: &mut Run<C::Reply>,
    ) -> io::Result<ControlFlow<()>> {
        match input {
            Input::Boot(args) => {
                if self.up.is_none() {
                    self.boot.extend(args.iter().cloned());
                }
                run.done(at, None);
                Ok(ControlFlow::Continue(()))
            }
            Input::Send {
                conn,
                wait,
                requests,
            } => {
                self.launch()?;
                for request in requests {
                    if self.drain(*conn, run).is_break()
                        || self.send(at, *conn, request)?.is_break()
                    {
                        return Ok(ControlFlow::Break(()));
                    }
                }
                Ok(if *wait {
                    self.drain(*conn, run)
                } else {
                    ControlFlow::Continue(())
                })
            }
            Input::Settle => {
                self.launch()?;
                let flow = self.drain_all(run);
                if flow.is_continue() && self.settle() {
                    run.done(at, None);
                }
                Ok(flow)
            }
        }
    }

    /// Heals, reads what is outstanding, and shuts the process down, then
    /// records its edges and the findings its exit and log show.
    fn stop(mut self, run: &mut Run<C::Reply>) -> io::Result<()> {
        if self.up.is_none() {
            return Ok(());
        }
        if !self.hung {
            self.heal();
            let _ = self.drain_all(run);
        }
        let service = self.service;
        let mut up = self.up.take().expect("launched");
        if !self.hung {
            let _ = up.admin.send(&service.codec, &service.shutdown);
            self.hung = up.process.wait_exit(service.timing.exit).is_none();
        }
        if self.hung {
            up.process.abort();
        }
        let status = up.process.reap(service.timing.exit);
        for edge in up.coverage.edges() {
            run.notes.saw("edge", u64::from(edge));
        }
        let log = std::fs::read(self.dir.path().join(LOG)).unwrap_or_default();
        let text = String::from_utf8_lossy(&log);
        if let Some(finding) = text.lines().find_map(|line| (service.failure)(line)) {
            run.notes.fail(finding.kind, finding.key);
        }
        for finding in sanitizer::leaks(&text) {
            run.notes.fail(finding.kind, finding.key);
        }
        let site = (service.site)(&text);
        if self.hung {
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
        Ok(())
    }
}
