/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! A rendered command and a fluent builder for one. Commands are binary-safe
//! (each argument is raw bytes) so vector blobs and `$PARAMS` values survive.

/// A single rendered command: argv where each argument is raw bytes.
pub type Cmd = Vec<Vec<u8>>;

/// Fluent builder for an argument list, so lowering reads as the command it
/// emits instead of a wall of `push(b"...".to_vec())`.
#[derive(Default)]
pub struct Args(Vec<Vec<u8>>);

impl Args {
    pub fn cmd(name: &str) -> Args {
        Args(vec![name.as_bytes().to_vec()])
    }
    /// A keyword / literal string argument.
    pub fn kw(mut self, s: &str) -> Args {
        self.0.push(s.as_bytes().to_vec());
        self
    }
    /// A raw (possibly binary) argument.
    pub fn raw(mut self, v: Vec<u8>) -> Args {
        self.0.push(v);
        self
    }
    /// A value rendered via `Display` (numbers, etc.).
    pub fn val(mut self, v: impl std::fmt::Display) -> Args {
        self.0.push(v.to_string().into_bytes());
        self
    }
    /// Apply `f` only when `cond` holds — for optional clauses.
    pub fn when(self, cond: bool, f: impl FnOnce(Args) -> Args) -> Args {
        if cond {
            f(self)
        } else {
            self
        }
    }
    pub fn extend(mut self, more: Vec<Vec<u8>>) -> Args {
        self.0.extend(more);
        self
    }
    pub fn build(self) -> Cmd {
        self.0
    }
}
