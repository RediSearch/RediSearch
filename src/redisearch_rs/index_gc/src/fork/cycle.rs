/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Coordination of the five fork-GC scanners.
//!
//! The child writes each scanner's stream in a fixed order. The parent must
//! consume those streams in exactly the same order before verifying the child
//! completed its final heartbeat.

use std::io::Read as _;

use index_spec::IndexSpecReadGuard;
use inverted_index::DocLiveness;

use super::{frame::TERMINATOR, io_result_ext::IoResultExt};
use crate::scanners::{
    existing_docs::{collect_existing_docs, handle_existing_docs},
    missing_docs::{collect_missing_docs, handle_missing_docs},
    numeric::{collect_numeric, handle_numeric},
    sparse_vectors::{collect_sparse_vectors, handle_sparse_vectors},
    tags::{collect_tags, handle_tags},
    terms::{collect_terms, handle_terms},
};
use crate::{ForkPipe, Frame, GcError, HandleOutcome};

unsafe extern "C" {
    static mut RedisModule_SendChildHeartbeat: Option<unsafe extern "C" fn(f64)>;
}

/// Run all child-side scanners in wire-protocol order.
///
/// Each scanner writes its own terminator. Once all scanner streams are sent,
/// this function reports the child's final heartbeat and writes a completion
/// marker before returning to the C caller. `docs` answers every scanner's
/// existence checks.
pub fn collect_scanners(
    fgc: &mut ForkPipe,
    spec: &IndexSpecReadGuard<'_>,
    docs: impl DocLiveness + Copy,
) {
    let index_name = spec
        .display_name(global_config::hide_user_data_from_log())
        .to_string_lossy();
    tracing::debug!(index = %index_name, "ForkPipe child scanning indexes start");

    {
        let mut writer = fgc.writer();
        collect_terms(&mut writer, spec, docs).unwrap_or_exit();
        collect_numeric(&mut writer, spec, docs).unwrap_or_exit();
        collect_tags(&mut writer, spec, docs).unwrap_or_exit();
        collect_sparse_vectors(&mut writer, spec, docs).unwrap_or_exit();
        collect_missing_docs(&mut writer, spec, docs).unwrap_or_exit();
        collect_existing_docs(&mut writer, spec, docs).unwrap_or_exit();
    }

    // SAFETY: this Redis API function pointer is initialized by the module
    // loader before Fork GC can run, and is not mutated afterwards.
    let send_child_heartbeat = unsafe {
        RedisModule_SendChildHeartbeat.expect("RedisModule_SendChildHeartbeat must be initialized")
    };
    // SAFETY: reporting full progress from the child is what the Redis fork
    // API expects at the end of a child's work.
    unsafe { send_child_heartbeat(1.0) };

    tracing::debug!(index = %index_name, "ForkPipe child scanning indexes end");
    Frame::Terminator.encode(&mut fgc.writer()).unwrap_or_exit();
}

fn handle_individual_scanner(
    fgc: &mut ForkPipe,
    scanner: &'static str,
    mut handle: impl FnMut(&mut ForkPipe) -> Result<HandleOutcome, GcError>,
) -> Result<(), (&'static str, GcError)> {
    loop {
        match handle(fgc) {
            Ok(HandleOutcome::Collected) => continue,
            Ok(HandleOutcome::Done) => return Ok(()),
            Err(error) => return Err((scanner, error)),
        }
    }
}

/// Consume and apply all scanner streams, then require the child's completion marker.
pub fn handle_scanners(fgc: &mut ForkPipe) -> Result<HandleOutcome, (&'static str, GcError)> {
    tracing::debug!("ForkPipe parent starts applying changes");

    handle_individual_scanner(fgc, "terms", handle_terms)?;
    handle_individual_scanner(fgc, "numeric", handle_numeric)?;
    handle_individual_scanner(fgc, "tags", handle_tags)?;
    handle_individual_scanner(fgc, "sparse vectors", handle_sparse_vectors)?;
    handle_individual_scanner(fgc, "missing docs", handle_missing_docs)?;
    handle_individual_scanner(fgc, "existing docs", handle_existing_docs)?;

    let mut completion = [0; size_of::<usize>()];
    fgc.reader().read_exact(&mut completion).map_err(|error| {
        (
            "parent",
            GcError::codec("reading the fork-GC completion marker", error),
        )
    })?;
    if usize::from_ne_bytes(completion) != TERMINATOR {
        return Err((
            "parent",
            GcError::codec(
                "checking the fork-GC completion marker",
                "expected a terminator after the child's final heartbeat",
            ),
        ));
    }

    tracing::debug!("ForkPipe parent ends applying changes");
    Ok(HandleOutcome::Done)
}
