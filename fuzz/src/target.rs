/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::fs::File;
use std::io::{ErrorKind, Read, Write};
use std::os::unix::net::UnixStream;
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::path::PathBuf;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread::sleep;
use std::time::{Duration, Instant};

use nix::errno::Errno;
use nix::sys::signal::{Signal, kill, killpg};
use nix::unistd::Pid;
use serde::{Deserialize, Serialize};

use crate::program::{Argv, Setup};
use crate::resp::{self, Decoded, Value};

const POLL_INTERVAL: Duration = Duration::from_millis(20);
const SOCKET_NAME: &str = "redis.sock";
const LOG_NAME: &str = "server.log";

#[derive(Clone, Debug)]
pub struct Env {
    pub redis_server: PathBuf,
    pub module: PathBuf,
    pub startup_deadline: Duration,
    pub reply_deadline: Duration,
    pub release_grace: Duration,
    pub shutdown_deadline: Duration,
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
            startup_deadline: Duration::from_secs(30),
            reply_deadline: Duration::from_secs(10),
            release_grace: Duration::from_secs(5),
            shutdown_deadline: Duration::from_secs(30),
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Status {
    pub code: Option<i32>,
    pub signal: Option<i32>,
}

impl From<ExitStatus> for Status {
    fn from(status: ExitStatus) -> Self {
        Status {
            code: status.code(),
            signal: status.signal(),
        }
    }
}

impl Status {
    pub fn success(&self) -> bool {
        self.code == Some(0)
    }
}

pub struct Server {
    child: Child,
    pgid: Pid,
    status: Option<ExitStatus>,
    socket: PathBuf,
    log: PathBuf,
    _dir: tempfile::TempDir,
}

impl Server {
    pub fn spawn(env: &Env, setup: &Setup) -> Result<Server, String> {
        let dir = tempfile::Builder::new()
            .prefix("rsfz")
            .tempdir()
            .map_err(|e| format!("creating server directory: {e}"))?;
        let socket = dir.path().join(SOCKET_NAME);
        let log = dir.path().join(LOG_NAME);
        let out = File::create(&log).map_err(|e| format!("creating {}: {e}", log.display()))?;
        let err = out.try_clone().map_err(|e| e.to_string())?;

        let mut command = Command::new(&env.redis_server);
        command
            .arg("--port")
            .arg("0")
            .arg("--unixsocket")
            .arg(&socket)
            .arg("--dir")
            .arg(dir.path())
            .args([
                "--save",
                "",
                "--appendonly",
                "no",
                "--enable-debug-command",
                "yes",
            ])
            .arg("--loadmodule")
            .arg(&env.module)
            .args(setup.module_args.iter().map(|a| os_arg(a.as_slice())))
            .stdin(Stdio::null())
            .stdout(out)
            .stderr(err)
            .process_group(0);
        let child = command
            .spawn()
            .map_err(|e| format!("spawning {}: {e}", env.redis_server.display()))?;
        let pgid = Pid::from_raw(i32::try_from(child.id()).map_err(|e| e.to_string())?);
        let mut server = Server {
            child,
            pgid,
            status: None,
            socket,
            log,
            _dir: dir,
        };
        server.wait_ready(env.startup_deadline)?;
        Ok(server)
    }

    fn wait_ready(&mut self, deadline: Duration) -> Result<(), String> {
        let start = Instant::now();
        while start.elapsed() < deadline {
            if let Some(status) = self.try_exit() {
                return Err(format!(
                    "server exited during startup ({status:?}):\n{}",
                    tail(&self.read_log(), 40)
                ));
            }
            if self.socket.exists()
                && let Ok(mut conn) = self.connect(Duration::from_secs(1))
                && conn
                    .call(&vec!["PING".into()], Duration::from_secs(1))
                    .is_ok()
            {
                return Ok(());
            }
            sleep(POLL_INTERVAL);
        }
        Err(format!(
            "server not ready after {deadline:?}:\n{}",
            tail(&self.read_log(), 40)
        ))
    }

    pub fn connect(&self, write_deadline: Duration) -> Result<Connection, String> {
        let stream = UnixStream::connect(&self.socket).map_err(|e| e.to_string())?;
        stream
            .set_write_timeout(Some(write_deadline))
            .map_err(|e| e.to_string())?;
        Ok(Connection {
            stream,
            buf: Vec::new(),
        })
    }

    pub fn try_exit(&mut self) -> Option<Status> {
        if self.status.is_none() {
            self.status = self.child.try_wait().ok().flatten();
        }
        self.status.map(Status::from)
    }

    pub fn wait_exit(&mut self, deadline: Duration) -> Option<Status> {
        let start = Instant::now();
        loop {
            if let Some(status) = self.try_exit() {
                return Some(status);
            }
            if start.elapsed() >= deadline {
                return None;
            }
            sleep(POLL_INTERVAL);
        }
    }

    pub fn abort(&self) {
        let _ = kill(self.pgid, Signal::SIGABRT);
    }

    pub fn reap(mut self, deadline: Duration) -> (Option<Status>, Vec<u8>) {
        let _ = killpg(self.pgid, Signal::SIGKILL);
        if self.status.is_none() {
            self.status = self.child.wait().ok();
        }
        let start = Instant::now();
        while killpg(self.pgid, None) != Err(Errno::ESRCH) && start.elapsed() < deadline {
            let _ = killpg(self.pgid, Signal::SIGKILL);
            sleep(POLL_INTERVAL);
        }
        (self.status.map(Status::from), self.read_log())
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

#[derive(Debug)]
pub enum Received {
    Value(Value),
    Timeout,
    Closed(String),
    Invalid(String),
}

impl Connection {
    pub fn send(&mut self, argv: &Argv) -> Result<(), String> {
        self.stream
            .write_all(&resp::encode(argv))
            .map_err(|e| e.to_string())
    }

    pub fn recv(&mut self, deadline: Duration) -> Received {
        let start = Instant::now();
        let mut chunk = [0u8; 64 * 1024];
        loop {
            match resp::decode(&self.buf) {
                Decoded::Complete(value, used) => {
                    self.buf.drain(..used);
                    return Received::Value(value);
                }
                Decoded::Invalid(e) => return Received::Invalid(e),
                Decoded::Incomplete => {}
            }
            let Some(left) = deadline
                .checked_sub(start.elapsed())
                .filter(|d| !d.is_zero())
            else {
                return Received::Timeout;
            };
            if let Err(e) = self.stream.set_read_timeout(Some(left)) {
                return Received::Closed(e.to_string());
            }
            match self.stream.read(&mut chunk) {
                Ok(0) => return Received::Closed("connection closed by server".to_owned()),
                Ok(n) => self.buf.extend_from_slice(&chunk[..n]),
                Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {
                    return Received::Timeout;
                }
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) => return Received::Closed(e.to_string()),
            }
        }
    }

    pub fn call(&mut self, argv: &Argv, deadline: Duration) -> Result<Value, String> {
        self.send(argv)?;
        match self.recv(deadline) {
            Received::Value(value) => Ok(value),
            other => Err(format!("{other:?}")),
        }
    }
}

fn os_arg(bytes: &[u8]) -> &std::ffi::OsStr {
    use std::os::unix::ffi::OsStrExt;
    std::ffi::OsStr::from_bytes(bytes)
}

fn tail(log: &[u8], lines: usize) -> String {
    let text = String::from_utf8_lossy(log);
    let all: Vec<&str> = text.lines().collect();
    all[all.len().saturating_sub(lines)..].join("\n")
}
