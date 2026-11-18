/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! What each command does: to search results, to what the model tracks, and
//! which pauses it can set that healing undoes.

use procfuzz_redis::resp::{Argv, Bytes, argv};

pub const DEBUG: &str = "_FT.DEBUG";

/// What a command can do to the results of a search.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Effect {
    Reads,
    /// Changes when background work runs, not what searches return.
    Schedules,
    /// Rebuilds indexes, which return the same results once rebuilt.
    Rebuilds,
    /// May change results. Every command not named here does.
    Writes,
}

/// What a command does to the state the model tracks, when it succeeds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Change {
    None,
    /// Drops the index named by the second word.
    DropsIndex,
    /// Changes the schema of the index named by the second word in a way the
    /// model does not follow.
    ReshapesIndex,
    PausesWorkers,
    ResumesWorkers,
}

/// What `command` does, judged by its first three words.
pub fn describe(command: &[Bytes]) -> (Effect, Change) {
    let word = |at: usize| {
        command
            .get(at)
            .map(|word| String::from_utf8_lossy(word.as_slice()).to_ascii_uppercase())
            .unwrap_or_default()
    };
    let (name, sub, arg) = (word(0), word(1), word(2));
    match (name.as_str(), sub.as_str(), arg.as_str()) {
        (DEBUG, "WORKERS", "PAUSE") => (Effect::Schedules, Change::PausesWorkers),
        (DEBUG, "WORKERS", "RESUME") => (Effect::Schedules, Change::ResumesWorkers),
        (DEBUG, "GC_FORCEINVOKE" | "GC_STOP_SCHEDULE" | "GC_CONTINUE_SCHEDULE", _)
        | (DEBUG, "WORKERS" | "BG_SCAN_CONTROLLER" | "SYNC_POINT", _) => {
            (Effect::Schedules, Change::None)
        }
        ("DEBUG", "RELOAD", _) => (Effect::Rebuilds, Change::None),
        ("FT.DROPINDEX", ..) => (Effect::Writes, Change::DropsIndex),
        ("FT.CREATE" | "FT.ALTER", ..) => (Effect::Writes, Change::ReshapesIndex),
        (
            "FT.SEARCH" | "FT.AGGREGATE" | "FT.CURSOR" | "FT.INFO" | "FT._LIST" | "FT.EXPLAIN"
            | "FT.TAGVALS" | "PING" | "EXISTS" | "HGETALL" | "JSON.GET",
            ..,
        ) => (Effect::Reads, Change::None),
        _ => (Effect::Writes, Change::None),
    }
}

/// The commands that undo every pause a program can set: paused workers, a
/// paused background scan, sync points, and, for each of `indexes`, a
/// stopped GC schedule.
pub fn undo_all(indexes: &[Bytes]) -> Vec<Argv> {
    let mut commands = vec![
        argv(&[DEBUG, "WORKERS", "RESUME"]),
        argv(&[
            DEBUG,
            "BG_SCAN_CONTROLLER",
            "SET_PAUSE_BEFORE_SCAN",
            "false",
        ]),
        argv(&[DEBUG, "BG_SCAN_CONTROLLER", "SET_BG_INDEX_RESUME"]),
        argv(&[DEBUG, "SYNC_POINT", "CLEAR"]),
    ];
    for index in indexes {
        let mut resume = argv(&[DEBUG, "GC_CONTINUE_SCHEDULE"]);
        resume.push(index.clone());
        commands.push(resume);
    }
    commands
}
