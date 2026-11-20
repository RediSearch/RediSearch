/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::fs::File;
use std::io;
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread::sleep;
use std::time::{Duration, Instant};

use nix::errno::Errno;
use nix::sys::signal::{Signal, kill, killpg};
use nix::unistd::Pid;

const POLL: Duration = Duration::from_millis(1);

/// A process under test, in a process group of its own so that its children
/// die with it, with stdout and stderr in one log. Dropping it kills the
/// group.
pub struct Process {
    child: Child,
    group: Pid,
    status: Option<ExitStatus>,
}

impl Process {
    pub fn spawn(mut command: Command, log: &Path) -> io::Result<Process> {
        let out = File::create(log)?;
        let child = command
            .stdin(Stdio::null())
            .stdout(out.try_clone()?)
            .stderr(out)
            .process_group(0)
            .spawn()?;
        let group = Pid::from_raw(i32::try_from(child.id()).map_err(io::Error::other)?);
        Ok(Process {
            child,
            group,
            status: None,
        })
    }

    pub fn exited(&mut self) -> Option<ExitStatus> {
        if self.status.is_none() {
            self.status = self.child.try_wait().ok().flatten();
        }
        self.status
    }

    pub fn wait_exit(&mut self, timeout: Duration) -> Option<ExitStatus> {
        let start = Instant::now();
        while self.exited().is_none() && start.elapsed() < timeout {
            sleep(POLL);
        }
        self.exited()
    }

    /// Makes a hung process write its crash report.
    pub fn abort(&self) {
        let _ = kill(self.group, Signal::SIGABRT);
    }

    /// Gives the process `timeout` to exit, so a sanitizer or crash report is
    /// complete, then kills what is left of its group. Its children's edges
    /// are in a [`CoverageMap`](crate::coverage::CoverageMap) only after this.
    pub fn reap(mut self, timeout: Duration) -> Option<ExitStatus> {
        self.wait_exit(timeout);
        let start = Instant::now();
        while killpg(self.group, Signal::SIGKILL) != Err(Errno::ESRCH) && start.elapsed() < timeout
        {
            self.exited();
            sleep(POLL);
        }
        self.exited()
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        let _ = killpg(self.group, Signal::SIGKILL);
        let _ = self.child.wait();
    }
}
