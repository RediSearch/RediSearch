/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Stopping on SIGINT or SIGTERM. The run in progress finishes and every
//! loop returns, so each server's process group, coverage segment, and
//! directory are cleaned up by their owners rather than left behind.

use std::io;
use std::sync::atomic::{AtomicBool, Ordering};

use nix::libc::c_int;
use nix::sys::signal::{SaFlags, SigAction, SigHandler, SigSet, Signal, sigaction};

static INTERRUPTED: AtomicBool = AtomicBool::new(false);

/// Makes the first SIGINT or SIGTERM set [`interrupted`]. A second one
/// terminates the process as usual.
pub fn on_signals() -> io::Result<()> {
    let action = SigAction::new(
        SigHandler::Handler(record),
        SaFlags::SA_RESETHAND,
        SigSet::empty(),
    );
    for signal in [Signal::SIGINT, Signal::SIGTERM] {
        // SAFETY: `record` only stores to an atomic, which is
        // async-signal-safe.
        unsafe { sigaction(signal, &action) }?;
    }
    Ok(())
}

pub fn interrupted() -> bool {
    INTERRUPTED.load(Ordering::Relaxed)
}

extern "C" fn record(_: c_int) {
    INTERRUPTED.store(true, Ordering::Relaxed);
}
