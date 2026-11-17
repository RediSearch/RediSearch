/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::judge::Finding;
use crate::program::Program;
use crate::run::Trace;
use crate::target::Env;

const MAX_DIR_NAME: usize = 120;
const SANITIZER_VARS: &[&str] = &[
    "ASAN_OPTIONS",
    "LSAN_OPTIONS",
    "RS_GLOBAL_DTORS",
    "LD_PRELOAD",
];

#[derive(Serialize, Deserialize)]
struct Manifest {
    finding: Finding,
    redis_server: PathBuf,
    module: PathBuf,
    environment: Vec<(String, String)>,
    reproduced: Option<(u32, u32)>,
}

pub fn dir_for(root: &Path, finding: &Finding) -> PathBuf {
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
        .take(MAX_DIR_NAME)
        .collect();
    root.join(name)
}

pub fn write(
    root: &Path,
    program: &Program,
    trace: &Trace,
    finding: &Finding,
    env: &Env,
) -> Result<PathBuf, String> {
    let dir = dir_for(root, finding);
    std::fs::create_dir_all(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    let manifest = Manifest {
        finding: finding.clone(),
        redis_server: env.redis_server.clone(),
        module: env.module.clone(),
        environment: SANITIZER_VARS
            .iter()
            .filter_map(|name| {
                std::env::var(name)
                    .ok()
                    .map(|value| (name.to_string(), value))
            })
            .collect(),
        reproduced: None,
    };
    let trace_json = serde_json::to_string_pretty(trace).map_err(|e| e.to_string())?;
    let manifest_json = serde_json::to_string_pretty(&manifest).map_err(|e| e.to_string())?;
    for (name, contents) in [
        ("program.json", program.to_json().into_bytes()),
        ("trace.json", trace_json.into_bytes()),
        ("server.log", trace.log.0.clone()),
        ("manifest.json", manifest_json.into_bytes()),
    ] {
        let path = dir.join(name);
        std::fs::write(&path, contents).map_err(|e| format!("{}: {e}", path.display()))?;
    }
    Ok(dir)
}

pub fn record_rate(
    root: &Path,
    finding: &Finding,
    reproduced: u32,
    attempts: u32,
) -> Result<(), String> {
    let path = dir_for(root, finding).join("manifest.json");
    let text = std::fs::read_to_string(&path).map_err(|e| format!("{}: {e}", path.display()))?;
    let mut manifest: Manifest = serde_json::from_str(&text).map_err(|e| e.to_string())?;
    manifest.reproduced = Some((reproduced, attempts));
    let json = serde_json::to_string_pretty(&manifest).map_err(|e| e.to_string())?;
    std::fs::write(&path, json).map_err(|e| format!("{}: {e}", path.display()))
}
