/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The server under test, as programs and the interpreter see it.

use std::fmt::Debug;
use std::io::{ErrorKind, Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::legacy::program::{Argv, Bytes};

const HARNESS_DEADLINE: Duration = Duration::from_secs(5);

/// A server that listens on a Unix socket and answers each command with one
/// reply.
pub trait Server: Sync {
    type Value: Clone + Debug + PartialEq + Serialize + DeserializeOwned + Send + Sync;

    /// Starts the server listening on `socket`, keeping its files in `dir`,
    /// with a program's arguments.
    fn command(&self, dir: &Path, socket: &Path, args: &[Bytes]) -> Command;
    fn encode(command: &Argv) -> Vec<u8>;
    /// `Ok(None)` until the first reply in `buf` is complete, then the reply
    /// and the number of bytes it used.
    fn decode(buf: &[u8]) -> Result<Option<(Self::Value, usize)>, String>;
    /// A command a ready server answers.
    fn ping(&self) -> Argv;
    /// A command that makes the server exit.
    fn shutdown(&self) -> Argv;
    /// The value an op's `bind` stores, if the reply has one.
    fn bound(&self, reply: &Self::Value) -> Option<i64>;
    /// Whether the server has finished its background work; polled until it
    /// has or a barrier times out. `None` if it does not answer.
    fn idle(&self, harness: &mut Harness<Self>) -> Option<bool>;
    /// Lifts every pause and hook a program can set, since one may be what
    /// holds a reply back. `None` if the server does not answer.
    fn release(&self, harness: &mut Harness<Self>) -> Option<()>;
    /// The signature of a failure the server reports on one log line in a
    /// format of its own, such as an assertion message.
    fn failure(&self, _line: &str) -> Option<String> {
        None
    }
    /// The frame a crash report in the log blames.
    fn crash_frame(&self, _log: &str) -> Option<String> {
        None
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Reply<V> {
    Value(V),
    /// A harness step completed.
    Done,
    Timeout,
    Closed(String),
    Invalid(String),
}

pub struct Connection<S: ?Sized> {
    stream: UnixStream,
    buf: Vec<u8>,
    _server: std::marker::PhantomData<fn() -> S>,
}

impl<S: Server + ?Sized> Connection<S> {
    pub fn open(socket: &Path) -> Result<Connection<S>, String> {
        UnixStream::connect(socket)
            .map(|stream| Connection {
                stream,
                buf: Vec::new(),
                _server: std::marker::PhantomData,
            })
            .map_err(|e| e.to_string())
    }

    pub fn send(&mut self, command: &Argv) -> Result<(), String> {
        self.stream
            .write_all(&S::encode(command))
            .map_err(|e| e.to_string())
    }

    /// A timeout keeps the bytes read so far, so a later call still returns
    /// this reply.
    pub fn receive(&mut self, timeout: Duration) -> Reply<S::Value> {
        let start = Instant::now();
        let mut chunk = [0u8; 64 * 1024];
        loop {
            match S::decode(&self.buf) {
                Ok(Some((value, used))) => {
                    self.buf.drain(..used);
                    return Reply::Value(value);
                }
                Err(e) => return Reply::Invalid(e),
                Ok(None) => {}
            }
            let Some(left) = timeout
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

    pub fn call(&mut self, command: &Argv, timeout: Duration) -> Option<S::Value> {
        self.send(command).ok()?;
        match self.receive(timeout) {
            Reply::Value(value) => Some(value),
            _ => None,
        }
    }
}

/// The harness's own connection, outside a program's steps. It reconnects
/// after a failed call.
pub struct Harness<S: ?Sized> {
    socket: PathBuf,
    connection: Option<Connection<S>>,
}

impl<S: Server + ?Sized> Harness<S> {
    pub(crate) fn new(socket: &Path) -> Harness<S> {
        Harness {
            socket: socket.to_owned(),
            connection: None,
        }
    }

    pub fn call(&mut self, command: &Argv) -> Option<S::Value> {
        if self.connection.is_none() {
            self.connection = Connection::open(&self.socket).ok();
        }
        let reply = self.connection.as_mut()?.call(command, HARNESS_DEADLINE);
        if reply.is_none() {
            self.connection = None;
        }
        reply
    }

    pub(crate) fn send(&mut self, command: &Argv) {
        if let Some(connection) = self.connection.as_mut() {
            let _ = connection.send(command);
        }
    }
}
