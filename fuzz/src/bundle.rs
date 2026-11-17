/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::judge::Finding;
use crate::program::Program;
use crate::run::Trace;
use crate::target::Env;

const SANITIZER_VARS: &[&str] = &[
    "ASAN_OPTIONS",
    "LSAN_OPTIONS",
    "RS_GLOBAL_DTORS",
    "LD_PRELOAD",
];

#[derive(Serialize)]
struct Manifest<'a> {
    finding: &'a Finding,
    reproduced: u32,
    runs: u32,
    redis_server: &'a Path,
    module: &'a Path,
    environment: Vec<(&'a str, String)>,
}

/// Writes `<root>/<signature>/`: the program, the trace of its first failing
/// run, the server log, and a manifest of the binaries and sanitizer settings.
/// A reduced program is written with the program it was reduced from.
pub fn write(
    root: &Path,
    program: &Program,
    original: Option<&Program>,
    trace: &Trace,
    finding: &Finding,
    (reproduced, runs): (u32, u32),
    env: &Env,
) -> Result<PathBuf, String> {
    let name: String = finding
        .signature
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || "._-".contains(c) {
                c
            } else {
                '_'
            }
        })
        .collect();
    let dir = root.join(name);
    let manifest = Manifest {
        finding,
        reproduced,
        runs,
        redis_server: &env.redis_server,
        module: &env.module,
        environment: SANITIZER_VARS
            .iter()
            .filter_map(|&name| Some((name, std::env::var(name).ok()?)))
            .collect(),
    };
    std::fs::create_dir_all(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    for (file, contents) in [
        ("program.json", program.to_json().into_bytes()),
        (
            "trace.json",
            serde_json::to_vec_pretty(trace).expect("traces serialize"),
        ),
        (
            "manifest.json",
            serde_json::to_vec_pretty(&manifest).expect("manifests serialize"),
        ),
        ("server.log", trace.log.0.clone()),
    ]
    .into_iter()
    .chain(original.map(|original| ("original.json", original.to_json().into_bytes())))
    {
        let path = dir.join(file);
        std::fs::write(&path, contents).map_err(|e| format!("{}: {e}", path.display()))?;
    }
    Ok(dir)
}
