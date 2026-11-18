/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::fmt;
use std::io;
use std::path::PathBuf;

#[derive(Debug)]
pub enum Error {
    /// Running a program, or reading or writing a file.
    Io(io::Error),
    /// A program file that does not parse.
    Program {
        path: PathBuf,
        source: serde_json::Error,
    },
    /// The program to reduce does not yield the finding to keep.
    NoSuchFinding,
    /// Runs kept failing.
    Failing(String),
    /// LibAFL failed.
    Campaign(libafl::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "{e}"),
            Error::Program { path, source } => write!(f, "{}: {source}", path.display()),
            Error::NoSuchFinding => write!(f, "the program does not yield that finding"),
            Error::Failing(e) => write!(f, "{e}"),
            Error::Campaign(e) => write!(f, "campaign: {e}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            Error::Program { source, .. } => Some(source),
            Error::NoSuchFinding | Error::Failing(_) | Error::Campaign(_) => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Error {
        Error::Io(e)
    }
}

impl From<libafl::Error> for Error {
    fn from(e: libafl::Error) -> Error {
        Error::Campaign(e)
    }
}
