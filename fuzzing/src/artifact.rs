/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Finding artifacts: a readable, replayable JSON record of a crashing/hanging
//! command sequence. Binary args are hex-encoded so the file stays valid JSON
//! and replays losslessly.

use crate::op::Cmd;

/// A single command argument: a plain string when printable UTF-8, otherwise
/// hex-encoded (for embedding in the Python `COMMANDS` literal).
#[derive(Clone)]
pub enum ArgJson {
    Text(String),
    Bin { hex: String },
}

impl ArgJson {
    pub fn encode(bytes: &[u8]) -> ArgJson {
        match std::str::from_utf8(bytes) {
            Ok(s) if !s.chars().any(|c| c.is_control()) => ArgJson::Text(s.to_string()),
            _ => ArgJson::Bin { hex: to_hex(bytes) },
        }
    }
}

pub fn to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

pub fn encode_cmd(cmd: &Cmd) -> Vec<ArgJson> {
    cmd.iter().map(|a| ArgJson::encode(a)).collect()
}

/// Human-readable one-line rendering of a command for logs / --dump.
pub fn render_cmd(cmd: &Cmd) -> String {
    cmd.iter()
        .map(|a| match std::str::from_utf8(a) {
            Ok(s) if !s.chars().any(|c| c.is_control()) => s.to_string(),
            _ => format!("<hex:{}>", to_hex(a)),
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// A finding artifact: the crash kind, the swarm config (`search-*` settings)
/// the crash occurred under, whether the sequence reproduces standalone, and the
/// concrete command sequence. Rendered by `repro.rs` into a runnable Python
/// script; the full server log is saved as a sibling `.log`.
pub struct Artifact {
    pub finding_kind: String,
    pub config: Vec<(String, String)>,
    /// Whether this exact sequence reproduced the finding on a freshly-started
    /// server. `false` means the bug needed accumulated campaign state and this
    /// sequence alone may not crash on replay.
    pub standalone: bool,
    pub commands: Vec<Vec<ArgJson>>,
}

impl Artifact {
    pub fn from_commands(
        commands: &[Cmd],
        finding_kind: &str,
        config: &[(String, String)],
        standalone: bool,
    ) -> Artifact {
        Artifact {
            finding_kind: finding_kind.to_string(),
            config: config.to_vec(),
            standalone,
            commands: commands.iter().map(encode_cmd).collect(),
        }
    }
}
