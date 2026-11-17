/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ffi::OsStr;
use std::fs::File;
use std::io::{ErrorKind, Read, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::net::UnixStream;
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::thread::sleep;
use std::time::{Duration, Instant};

use nix::errno::Errno;
use nix::sys::signal::{Signal, kill, killpg};
use nix::unistd::Pid;
use serde::{Deserialize, Serialize};

use crate::coverage::{self, Coverage, Segment};
use crate::program::{Argv, Bytes, argv};
use crate::resp::{self, Value};

const STARTUP_DEADLINE: Duration = Duration::from_secs(30);
const EXIT_DEADLINE: Duration = Duration::from_secs(30);
const POLL: Duration = Duration::from_millis(20);

#[derive(Clone, Debug)]
pub struct Env {
    pub redis_server: PathBuf,
    pub module: PathBuf,
    /// RedisJSON, loaded after the module as the Python tests do.
    pub rejson: PathBuf,
}

impl Env {
    pub fn from_env() -> Result<Env, String> {
        let var = |name: &str| {
            std::env::var_os(name).map(PathBuf::from).ok_or_else(|| {
                format!("{name} is not set; run fuzz/setup.sh and source bin/fuzz/env.sh")
            })
        };
        Ok(Env {
            redis_server: var("REDIS_SERVER")?,
            module: var("REDISEARCH_MODULE")?,
            rejson: var("REJSON_MODULE")?,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Status {
    pub code: Option<i32>,
    pub signal: Option<i32>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Reply {
    Value(Value),
    Timeout,
    Closed(String),
    Invalid(String),
}

pub struct Server {
    child: Child,
    pgid: Pid,
    exited: Option<Status>,
    socket: PathBuf,
    log: PathBuf,
    coverage: Segment,
    _dir: tempfile::TempDir,
}

pub struct Reaped {
    pub status: Status,
    pub log: Vec<u8>,
    pub coverage: Coverage,
}

impl Server {
    pub fn spawn(env: &Env, module_args: &[Bytes]) -> Result<Server, String> {
        let dir = tempfile::Builder::new()
            .prefix("rsfz")
            .tempdir()
            .map_err(|e| e.to_string())?;
        let socket = dir.path().join("redis.sock");
        let log = dir.path().join("server.log");
        let out = File::create(&log).map_err(|e| e.to_string())?;
        let err = out.try_clone().map_err(|e| e.to_string())?;
        let coverage = Segment::create()?;
        let child = Command::new(&env.redis_server)
            .args(["--port", "0", "--save", "", "--appendonly", "no"])
            .args(["--enable-debug-command", "yes", "--unixsocket"])
            .arg(&socket)
            .arg("--dir")
            .arg(dir.path())
            .arg("--loadmodule")
            .arg(&env.module)
            .args(module_args.iter().map(|a| OsStr::from_bytes(a.as_slice())))
            .arg("--loadmodule")
            .arg(&env.rejson)
            .env(coverage::SHM_ENV, coverage.name())
            .stdin(Stdio::null())
            .stdout(out)
            .stderr(err)
            .process_group(0)
            .spawn()
            .map_err(|e| format!("spawning {}: {e}", env.redis_server.display()))?;
        let mut server = Server {
            pgid: Pid::from_raw(i32::try_from(child.id()).map_err(|e| e.to_string())?),
            child,
            exited: None,
            socket,
            log,
            coverage,
            _dir: dir,
        };
        let start = Instant::now();
        while start.elapsed() < STARTUP_DEADLINE && server.exited().is_none() {
            if let Ok(mut conn) = server.connect()
                && conn.call(&argv(&["PING"]), STARTUP_DEADLINE).is_ok()
            {
                return Ok(server);
            }
            sleep(POLL);
        }
        let log = String::from_utf8_lossy(&server.read_log()).into_owned();
        Err(format!("server did not start:\n{log}"))
    }

    pub fn connect(&self) -> Result<Connection, String> {
        UnixStream::connect(&self.socket)
            .map(|stream| Connection {
                stream,
                buf: Vec::new(),
            })
            .map_err(|e| e.to_string())
    }

    pub fn exited(&mut self) -> Option<Status> {
        if self.exited.is_none() {
            self.exited = self.child.try_wait().ok().flatten().map(|s| Status {
                code: s.code(),
                signal: s.signal(),
            });
        }
        self.exited
    }

    pub fn wait_exit(&mut self, deadline: Duration) -> Option<Status> {
        let start = Instant::now();
        while self.exited().is_none() && start.elapsed() < deadline {
            sleep(POLL);
        }
        self.exited()
    }

    pub fn abort(&self) {
        let _ = kill(self.pgid, Signal::SIGABRT);
    }

    /// Lets the server finish exiting, so a sanitizer or crash report is
    /// complete, then kills whatever is left of its process group.
    pub fn reap(mut self) -> Reaped {
        self.wait_exit(EXIT_DEADLINE);
        let start = Instant::now();
        while killpg(self.pgid, Signal::SIGKILL) != Err(Errno::ESRCH)
            && start.elapsed() < EXIT_DEADLINE
        {
            self.exited();
            sleep(POLL);
        }
        Reaped {
            status: self.exited().unwrap_or(Status {
                code: None,
                signal: None,
            }),
            log: self.read_log(),
            coverage: self.coverage.read(),
        }
    }

    fn read_log(&self) -> Vec<u8> {
        std::fs::read(&self.log).unwrap_or_default()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = killpg(self.pgid, Signal::SIGKILL);
        let _ = self.child.wait();
    }
}

pub struct Connection {
    stream: UnixStream,
    buf: Vec<u8>,
}

impl Connection {
    pub fn send(&mut self, argv: &Argv) -> Result<(), String> {
        self.stream
            .write_all(&resp::encode(argv))
            .map_err(|e| e.to_string())
    }

    /// Reads one reply. A timeout keeps the bytes read so far, so a later
    /// call still returns this reply.
    pub fn recv(&mut self, deadline: Duration) -> Reply {
        let start = Instant::now();
        let mut chunk = [0u8; 64 * 1024];
        loop {
            match resp::decode(&self.buf) {
                Ok(Some((value, used))) => {
                    self.buf.drain(..used);
                    return Reply::Value(value);
                }
                Err(e) => return Reply::Invalid(e),
                Ok(None) => {}
            }
            let Some(left) = deadline
                .checked_sub(start.elapsed())
                .filter(|d| !d.is_zero())
            else {
                return Reply::Timeout;
            };
            let _ = self.stream.set_read_timeout(Some(left));
            match self.stream.read(&mut chunk) {
                Ok(0) => return Reply::Closed("connection closed by server".to_owned()),
                Ok(n) => self.buf.extend_from_slice(&chunk[..n]),
                Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {
                    return Reply::Timeout;
                }
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) => return Reply::Closed(e.to_string()),
            }
        }
    }

    pub fn call(&mut self, argv: &Argv, deadline: Duration) -> Result<Value, String> {
        self.send(argv)?;
        match self.recv(deadline) {
            Reply::Value(value) => Ok(value),
            other => Err(format!("{other:?}")),
        }
    }
}
