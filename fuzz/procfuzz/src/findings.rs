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

use crate::model::Program;
use crate::system::{Run, Step};

/// One directory per finding, holding what reproduces it. A directory's
/// existence marks its finding as known, so creating an empty one silences a
/// known bug, and workers sharing the root report each finding once.
pub struct Findings {
    root: PathBuf,
    /// What every manifest records about the setup, such as the binaries.
    setup: serde_json::Value,
}

/// How often a finding showed when its program ran repeatedly.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct Rate {
    pub reproduced: u32,
    pub runs: u32,
}

#[derive(Serialize)]
struct Manifest<'a> {
    finding: &'a str,
    #[serde(flatten)]
    rate: Rate,
    setup: &'a serde_json::Value,
}

#[derive(Serialize)]
struct Trace<'a, R> {
    steps: &'a [Step<R>],
    findings: Vec<String>,
}

impl Findings {
    pub fn new(root: &Path, setup: &impl Serialize) -> io::Result<Findings> {
        std::fs::create_dir_all(root)?;
        Ok(Findings {
            root: root.to_owned(),
            setup: serde_json::to_value(setup).map_err(io::Error::other)?,
        })
    }

    pub fn dir(&self, finding: &str) -> PathBuf {
        let name: String = finding
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
    pub fn claim(&self, finding: &str) -> bool {
        std::fs::create_dir(self.dir(finding)).is_ok()
    }

    /// Writes the program, a reduced program's `original`, the steps and
    /// files of a run that showed the finding, and a manifest.
    pub fn write<O: Serialize, R: Serialize>(
        &self,
        finding: &str,
        program: &Program<O>,
        original: Option<&Program<O>>,
        run: &Run<R>,
        rate: Rate,
    ) -> io::Result<PathBuf> {
        let dir = self.dir(finding);
        let manifest = Manifest {
            finding,
            rate,
            setup: &self.setup,
        };
        let trace = Trace {
            steps: &run.steps,
            findings: run.notes.findings.iter().map(ToString::to_string).collect(),
        };
        std::fs::create_dir_all(&dir)?;
        std::fs::write(dir.join("program.json"), to_json(program)?)?;
        std::fs::write(dir.join("trace.json"), to_json(&trace)?)?;
        std::fs::write(dir.join("manifest.json"), to_json(&manifest)?)?;
        for (name, contents) in &run.files {
            std::fs::write(dir.join(name), contents)?;
        }
        if let Some(original) = original {
            std::fs::write(dir.join("original.json"), to_json(original)?)?;
        }
        Ok(dir)
    }
}

pub fn to_json(value: &impl Serialize) -> io::Result<Vec<u8>> {
    serde_json::to_vec_pretty(value).map_err(io::Error::other)
}
