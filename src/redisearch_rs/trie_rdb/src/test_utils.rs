/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Test-only in-memory implementation of [`RdbIO`].
//!
//! [`MockRdbIO`] records every write as a typed [`Op`] and replays the ops
//! on read, standing in for a real `RedisModuleIO` handle. Both the
//! byte-keyed and the str-keyed serializer test suites use it, so it lives
//! here rather than in either suite: sharing one [`Op`] type is what lets
//! the str-flavor suite assert its save traces equal the byte-flavor's.
//!
//! Gated behind the `test-utils` feature, which the crate's own
//! dev-dependency on itself turns on; nothing in a production build compiles
//! it.

use super::*;

/// One typed call against [`RdbIO`]. The wire-shape tests assert against
/// `Vec<Op>` traces directly.
#[derive(Debug, Clone, PartialEq)]
pub enum Op {
    U64(u64),
    F64(f64),
    Bytes(Vec<u8>),
}

/// Round-trip [`RdbIO`] mock: `save_*` append to `ops`; `load_*` replay
/// them in order from an internal read cursor. One buffer, so saving into
/// the mock and then loading it back reproduces the production save→load
/// path against a single endpoint — the shape the real `RedisModuleIO`
/// handle has. `ops` is public so wire-shape tests can assert the exact
/// recorded trace.
#[derive(Default)]
pub struct MockRdbIO {
    pub ops: Vec<Op>,
    read_pos: usize,
    fail_after: Option<usize>,
    read_calls: usize,
}

impl MockRdbIO {
    /// Preload the mock with a known op stream, for load-only tests that
    /// feed a hand-built (possibly malformed) trace rather than saving one.
    pub fn from_ops(ops: Vec<Op>) -> Self {
        Self {
            ops,
            ..Self::default()
        }
    }

    /// Short-circuit `load_*` with an [`std::io::Error`] after `n`
    /// successful reads, to exercise mid-stream IO failure paths.
    pub fn fail_after(mut self, n: usize) -> Self {
        self.fail_after = Some(n);
        self
    }

    fn next_read(&mut self) -> io::Result<Op> {
        if let Some(n) = self.fail_after
            && self.read_calls >= n
        {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "mock: injected io failure",
            ));
        }
        self.read_calls += 1;
        let op = self.ops.get(self.read_pos).cloned().ok_or_else(|| {
            io::Error::new(io::ErrorKind::UnexpectedEof, "mock: op stream exhausted")
        })?;
        self.read_pos += 1;
        Ok(op)
    }
}

// The trie RDB wire format only ever uses u64/f64/buffer; the `i64`/`f32`
// methods of the shared `RdbIO` trait exist for other consumers (e.g. RSE's
// vecsim) and are never reached by `trie_rdb`'s serializers, so the mock
// asserts that invariant rather than modeling them.
impl RdbIO for MockRdbIO {
    fn write_u64(&mut self, v: u64) {
        self.ops.push(Op::U64(v));
    }
    fn write_f64(&mut self, v: f64) {
        self.ops.push(Op::F64(v));
    }
    fn write_buffer(&mut self, b: &[u8]) {
        self.ops.push(Op::Bytes(b.to_vec()));
    }
    fn write_i64(&mut self, _v: i64) {
        unreachable!("trie_rdb never serializes i64");
    }
    fn write_f32(&mut self, _v: f32) {
        unreachable!("trie_rdb never serializes f32");
    }

    fn read_u64(&mut self) -> io::Result<u64> {
        match self.next_read()? {
            Op::U64(v) => Ok(v),
            op => panic!("mock: expected U64, got {op:?}"),
        }
    }
    fn read_f64(&mut self) -> io::Result<f64> {
        match self.next_read()? {
            Op::F64(v) => Ok(v),
            op => panic!("mock: expected F64, got {op:?}"),
        }
    }
    fn read_buffer(&mut self) -> io::Result<Vec<u8>> {
        match self.next_read()? {
            Op::Bytes(v) => Ok(v),
            op => panic!("mock: expected Bytes, got {op:?}"),
        }
    }
    fn read_i64(&mut self) -> io::Result<i64> {
        unreachable!("trie_rdb never deserializes i64");
    }
    fn read_f32(&mut self) -> io::Result<f32> {
        unreachable!("trie_rdb never deserializes f32");
    }
}
