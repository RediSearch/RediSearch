/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Programs from command scripts, such as a `MONITOR` recording of a Python
//! test.

use procfuzz::model::Made;
use procfuzz_redis::line::{monitored, split};

use crate::Program;
use crate::op::{Action, Op};

/// A program of one command per line, all on one connection. If any line is
/// one `MONITOR` printed, only those are read; otherwise every line but blank
/// ones and `#` comments.
pub fn import(text: &str) -> Result<Program, String> {
    let lines = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'));
    let mut commands: Vec<&str> = lines.clone().filter_map(monitored).collect();
    if commands.is_empty() {
        commands = lines.collect();
    }
    let ops = commands
        .into_iter()
        .map(|line| {
            let argv = split(line).map_err(|e| format!("{line}: {e}"))?;
            let op = Op {
                conn: 0,
                defer: false,
                action: Action::Command { argv },
            };
            Ok(Made {
                op,
                from: Vec::new(),
            })
        })
        .collect::<Result<_, String>>()?;
    Ok(Program { ops })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_monitor_recording_imports_as_the_commands_clients_sent() {
        let recording = "OK\n\
            1.0 [0 127.0.0.1:1] \"FT.ADD\" \"i\" \"d\" \"1\" \"FIELDS\" \"t\" \"a b\"\n\
            1.1 [0 ] \"HSET\" \"d\" \"t\" \"a b\"\n\
            1.2 [0 127.0.0.1:2] \"FT.SEARCH\" \"i\" \"*\"\n";
        let commands: Vec<Vec<String>> = import(recording)
            .unwrap()
            .ops()
            .iter()
            .map(|op| match &op.action {
                Action::Command { argv } => argv
                    .iter()
                    .map(|word| String::from_utf8_lossy(word.as_slice()).into_owned())
                    .collect(),
                action => panic!("{action:?}"),
            })
            .collect();
        assert_eq!(
            commands,
            [
                vec!["FT.ADD", "i", "d", "1", "FIELDS", "t", "a b"],
                vec!["FT.SEARCH", "i", "*"],
            ]
        );
    }
}
