/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::io;
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::legacy::program::{Program, TypedOp};
use crate::legacy::run::Execution;

const SANITIZER_VARS: [&str; 3] = ["ASAN_OPTIONS", "LSAN_OPTIONS", "LD_PRELOAD"];

/// One directory per finding signature, holding what reproduces it. A
/// directory's existence marks its finding as known, so creating an empty one
/// silences a known bug, and workers sharing the root report each finding
/// once.
pub struct Findings {
    root: PathBuf,
    /// What every manifest records about the setup, such as the binaries.
    setup: serde_json::Value,
}

#[derive(Serialize)]
struct Manifest<'a> {
    signature: &'a str,
    reproduced: u32,
    runs: u32,
    setup: &'a serde_json::Value,
    environment: Vec<(&'a str, String)>,
}

impl Findings {
    pub fn new(root: &Path, setup: &impl Serialize) -> io::Result<Findings> {
        std::fs::create_dir_all(root)?;
        Ok(Findings {
            root: root.to_owned(),
            setup: serde_json::to_value(setup).map_err(io::Error::other)?,
        })
    }

    pub fn dir(&self, signature: &str) -> PathBuf {
        let name: String = signature
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || "._-".contains(c) {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        self.root.join(name)
    }

    /// Whether this call is the first to see the finding.
    pub fn claim(&self, signature: &str) -> bool {
        std::fs::create_dir(self.dir(signature)).is_ok()
    }

    /// Writes the program, a reduced program's `original`, the trace and log
    /// of a run that yielded the finding, and a manifest.
    pub fn write<T: TypedOp, V: Serialize>(
        &self,
        signature: &str,
        program: &Program<T>,
        original: Option<&Program<T>>,
        execution: &Execution<V>,
        (reproduced, runs): (u32, u32),
    ) -> io::Result<PathBuf> {
        let dir = self.dir(signature);
        let manifest = Manifest {
            signature,
            reproduced,
            runs,
            setup: &self.setup,
            environment: SANITIZER_VARS
                .iter()
                .filter_map(|&name| Some((name, std::env::var(name).ok()?)))
                .collect(),
        };
        std::fs::create_dir_all(&dir)?;
        std::fs::write(dir.join("program.json"), to_json(program)?)?;
        std::fs::write(dir.join("trace.json"), to_json(&execution.trace)?)?;
        std::fs::write(dir.join("manifest.json"), to_json(&manifest)?)?;
        std::fs::write(dir.join("server.log"), &execution.log)?;
        if let Some(original) = original {
            std::fs::write(dir.join("original.json"), to_json(original)?)?;
        }
        Ok(dir)
    }
}

pub fn to_json(value: &impl Serialize) -> io::Result<Vec<u8>> {
    serde_json::to_vec_pretty(value).map_err(io::Error::other)
}
