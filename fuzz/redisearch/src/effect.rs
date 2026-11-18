/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! What each command does, in one table: to search results, to what the
//! model tracks, and how to undo a pause it sets.

use procfuzz_redis::resp::{Argv, Bytes};

pub const DEBUG: &str = "_FT.DEBUG";
/// Stands for each index in an undo command.
const EACH_INDEX: &str = "{index}";

/// What a command can do to the results of a search.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Effect {
    Reads,
    /// Changes when background work runs, not what searches return.
    Schedules,
    /// Rebuilds indexes, which return the same results once rebuilt.
    Rebuilds,
    /// May change results. Every command not in the table does.
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

struct Entry {
    /// The first words, matched without regard to case.
    words: &'static [&'static str],
    effect: Effect,
    change: Change,
    /// Undoes what the command pauses.
    undo: &'static [&'static [&'static str]],
}

const fn entry(words: &'static [&'static str], effect: Effect) -> Entry {
    Entry {
        words,
        effect,
        change: Change::None,
        undo: &[],
    }
}

/// The first entry whose words begin a command describes it.
const COMMANDS: [Entry; 24] = [
    entry(&["FT.SEARCH"], Effect::Reads),
    entry(&["FT.AGGREGATE"], Effect::Reads),
    entry(&["FT.CURSOR"], Effect::Reads),
    entry(&["FT.INFO"], Effect::Reads),
    entry(&["FT._LIST"], Effect::Reads),
    entry(&["FT.EXPLAIN"], Effect::Reads),
    entry(&["FT.TAGVALS"], Effect::Reads),
    entry(&["PING"], Effect::Reads),
    entry(&["EXISTS"], Effect::Reads),
    entry(&["HGETALL"], Effect::Reads),
    entry(&["JSON.GET"], Effect::Reads),
    entry(&[DEBUG, "GC_FORCEINVOKE"], Effect::Schedules),
    Entry {
        undo: &[&[DEBUG, "GC_CONTINUE_SCHEDULE", EACH_INDEX]],
        ..entry(&[DEBUG, "GC_STOP_SCHEDULE"], Effect::Schedules)
    },
    entry(&[DEBUG, "GC_CONTINUE_SCHEDULE"], Effect::Schedules),
    Entry {
        change: Change::PausesWorkers,
        undo: &[&[DEBUG, "WORKERS", "RESUME"]],
        ..entry(&[DEBUG, "WORKERS", "PAUSE"], Effect::Schedules)
    },
    Entry {
        change: Change::ResumesWorkers,
        ..entry(&[DEBUG, "WORKERS", "RESUME"], Effect::Schedules)
    },
    entry(&[DEBUG, "WORKERS"], Effect::Schedules),
    Entry {
        undo: &[
            &[
                DEBUG,
                "BG_SCAN_CONTROLLER",
                "SET_PAUSE_BEFORE_SCAN",
                "false",
            ],
            &[DEBUG, "BG_SCAN_CONTROLLER", "SET_BG_INDEX_RESUME"],
        ],
        ..entry(&[DEBUG, "BG_SCAN_CONTROLLER"], Effect::Schedules)
    },
    Entry {
        undo: &[&[DEBUG, "SYNC_POINT", "CLEAR"]],
        ..entry(&[DEBUG, "SYNC_POINT"], Effect::Schedules)
    },
    entry(&["DEBUG", "RELOAD"], Effect::Rebuilds),
    Entry {
        change: Change::DropsIndex,
        ..entry(&["FT.DROPINDEX"], Effect::Writes)
    },
    Entry {
        change: Change::ReshapesIndex,
        ..entry(&["FT.CREATE"], Effect::Writes)
    },
    Entry {
        change: Change::ReshapesIndex,
        ..entry(&["FT.ALTER"], Effect::Writes)
    },
    entry(&["FT.DROP"], Effect::Writes),
];

fn lookup(command: &[Bytes]) -> Option<&'static Entry> {
    COMMANDS.iter().find(|entry| {
        entry.words.len() <= command.len()
            && entry
                .words
                .iter()
                .zip(command)
                .all(|(word, given)| given.as_slice().eq_ignore_ascii_case(word.as_bytes()))
    })
}

pub fn effect(command: &[Bytes]) -> Effect {
    lookup(command).map_or(Effect::Writes, |entry| entry.effect)
}

pub fn change(command: &[Bytes]) -> Change {
    lookup(command).map_or(Change::None, |entry| entry.change)
}

/// The commands that undo every pause a command in the table can set, for
/// the given indexes.
pub fn undo_all(indexes: &[Bytes]) -> Vec<Argv> {
    let mut commands = Vec::new();
    for undo in COMMANDS.iter().flat_map(|entry| entry.undo) {
        if undo.contains(&EACH_INDEX) {
            for index in indexes {
                commands.push(
                    undo.iter()
                        .map(|&word| match word {
                            EACH_INDEX => index.clone(),
                            word => word.into(),
                        })
                        .collect(),
                );
            }
        } else {
            commands.push(undo.iter().map(|&word| word.into()).collect());
        }
    }
    commands
}

#[cfg(test)]
mod tests {
    use procfuzz_redis::resp::argv;

    use super::*;

    #[test]
    fn the_most_specific_entry_describes_a_command() {
        assert_eq!(
            change(&argv(&["_ft.debug", "workers", "pause"])),
            Change::PausesWorkers
        );
        assert_eq!(
            effect(&argv(&["_FT.DEBUG", "WORKERS", "STATS"])),
            Effect::Schedules
        );
        assert_eq!(effect(&argv(&["HSET", "k", "f", "v"])), Effect::Writes);
        assert_eq!(
            change(&argv(&["FT.ALTER", "i", "SCHEMA"])),
            Change::ReshapesIndex
        );
        assert!(undo_all(&["i0".into()]).contains(&argv(&[DEBUG, "GC_CONTINUE_SCHEDULE", "i0"])));
    }
}
