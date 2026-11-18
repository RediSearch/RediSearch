/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Programs as command scripts: [`import`] reads commands, such as a
//! `MONITOR` recording of a Python test, and [`export`] writes a program to
//! run without the fuzzer.

use procfuzz::model::Made;
use procfuzz::service::Input;
use procfuzz_redis::line::{join, monitored, split};

use crate::Program;
use crate::op::{Action, Op};
use crate::state::State;

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

/// The program as `redis-cli` reads it, with settles as `#` notes. Only a program on one connection that awaits every reply and
/// keeps no cursor has one.
pub fn export(program: &Program) -> Result<String, String> {
    let mut state = State::default();
    let mut script = String::new();
    for (at, made) in program.ops.iter().enumerate() {
        match made.op.lower(&state) {
            Input::Settle => script += "# wait until the server is idle\n",
            Input::Send {
                conn: 0,
                wait: true,
                requests,
            } if !made.op.action.uses_a_cursor() => {
                for request in &requests {
                    script += &join(request);
                    script.push('\n');
                }
            }
            _ => {
                return Err(format!(
                    "op {at} uses a second connection, a deferred reply, or a cursor"
                ));
            }
        }
        state.apply(&made.op, None);
    }
    Ok(script)
}

#[cfg(test)]
mod tests {
    use procfuzz::draw::Draw;
    use procfuzz::model::generate;

    use super::*;
    use crate::RediSearch;

    #[test]
    fn exported_programs_import_as_the_same_commands() {
        let mut program = generate(&RediSearch, &mut Draw::new(0), 50);
        program.ops.retain_mut(|made| {
            made.op.conn = 0;
            made.op.defer = false;
            !made.op.action.uses_a_cursor()
        });
        let script = export(&program).unwrap();
        let commands = |script: &str| -> Vec<String> {
            script
                .lines()
                .filter(|line| !line.starts_with('#'))
                .map(str::to_owned)
                .collect()
        };
        assert_eq!(
            commands(&export(&import(&script).unwrap()).unwrap()),
            commands(&script)
        );
    }

    #[test]
    fn a_monitor_recording_imports_as_the_commands_clients_sent() {
        let recording = "OK\n\
            1.0 [0 127.0.0.1:1] \"FT.ADD\" \"i\" \"d\" \"1\" \"FIELDS\" \"t\" \"a b\"\n\
            1.1 [0 ] \"HSET\" \"d\" \"t\" \"a b\"\n\
            1.2 [0 127.0.0.1:2] \"FT.SEARCH\" \"i\" \"*\"\n";
        assert_eq!(
            export(&import(recording).unwrap()).unwrap(),
            "FT.ADD i d 1 FIELDS t \"a b\"\nFT.SEARCH i *\n"
        );
    }
}
