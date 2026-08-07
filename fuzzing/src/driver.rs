/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Thin redis-rs wrapper that classifies each reply into an [`Outcome`] the
//! oracles can act on. A server-side `-ERR` is a normal outcome, not a finding.

use std::time::Duration;

use anyhow::Result;

pub enum Outcome {
    /// A well-formed reply (any RESP value).
    Reply(redis::Value),
    /// A normal server error reply (e.g. `-ERR ...`). NOT a finding.
    ServerError(String),
    /// I/O or connection error — the server may have crashed. Crash candidate.
    Disconnected(String),
    /// No reply within the read timeout. Hang candidate.
    Timeout(String),
}

/// Interpret a RESP value as a non-negative integer, whether returned as an
/// integer or a numeric bulk string (FT.INFO mixes both).
pub fn value_as_u64(v: &redis::Value) -> Option<u64> {
    match v {
        redis::Value::Int(n) if *n >= 0 => Some(*n as u64),
        redis::Value::BulkString(b) => std::str::from_utf8(b)
            .ok()
            .and_then(|s| s.trim().parse::<f64>().ok())
            .filter(|f| f.is_finite() && *f >= 0.0)
            .map(|f| f as u64),
        redis::Value::SimpleString(s) => s.trim().parse::<u64>().ok(),
        _ => None,
    }
}

/// Extract a cursor id from an `FT.AGGREGATE ... WITHCURSOR` / `FT.CURSOR READ`
/// reply (`[results, cursor_id]`). Returns `None` for a zero id (exhausted).
pub fn cursor_id(v: &redis::Value) -> Option<i64> {
    if let redis::Value::Array(items) = v {
        if let Some(redis::Value::Int(id)) = items.get(1) {
            if *id != 0 {
                return Some(*id);
            }
        }
    }
    None
}

pub struct Driver {
    con: redis::Connection,
}

impl Driver {
    pub fn connect(url: &str, read_timeout: Duration) -> Result<Driver> {
        let client = redis::Client::open(url)?;
        let con = client.get_connection()?;
        con.set_read_timeout(Some(read_timeout))?;
        Ok(Driver { con })
    }

    pub fn exec(&mut self, args: &[Vec<u8>]) -> Outcome {
        if args.is_empty() {
            return Outcome::ServerError("empty command".to_string());
        }
        let mut cmd = redis::Cmd::new();
        for a in args {
            cmd.arg(a.as_slice());
        }
        match cmd.query::<redis::Value>(&mut self.con) {
            Ok(v) => Outcome::Reply(v),
            Err(e) => {
                if e.is_timeout() {
                    Outcome::Timeout(e.to_string())
                } else if e.is_io_error() || e.is_connection_dropped() || e.is_connection_refusal()
                {
                    Outcome::Disconnected(e.to_string())
                } else {
                    // Response/extension errors: the server answered with an error,
                    // which is legitimate behavior for a bad command.
                    Outcome::ServerError(e.to_string())
                }
            }
        }
    }

    pub fn ping(&mut self) -> bool {
        redis::cmd("PING")
            .query::<String>(&mut self.con)
            .map(|s| s == "PONG")
            .unwrap_or(false)
    }

    /// True if RedisJSON is loaded (so `ON JSON` indexes and `JSON.SET` work).
    pub fn has_json(&mut self) -> bool {
        redis::cmd("MODULE")
            .arg("LIST")
            .query::<redis::Value>(&mut self.con)
            .map(|v| format!("{v:?}").to_lowercase().contains("json"))
            .unwrap_or(false)
    }

    /// Snapshot current values of the given (modern `search-*`) config params via
    /// the non-deprecated `CONFIG GET`, so they can be restored between sequences.
    pub fn config_snapshot(&mut self, params: &[&str]) -> Vec<(String, Vec<u8>)> {
        let mut out = Vec::new();
        for p in params {
            let reply = redis::cmd("CONFIG")
                .arg("GET")
                .arg(p)
                .query::<redis::Value>(&mut self.con);
            // Reply is [param, value] (flat map); pull the value.
            if let Ok(redis::Value::Array(kv)) = reply {
                if let Some(redis::Value::BulkString(v)) = kv.get(1) {
                    out.push((p.to_string(), v.clone()));
                }
            }
        }
        out
    }

    /// Best-effort `CONFIG SET <param> <value>`; errors (immutable/invalid) are
    /// ignored so an unsupported swarm knob never disrupts the run.
    pub fn config_set(&mut self, param: &str, value: &str) {
        let _ = redis::cmd("CONFIG")
            .arg("SET")
            .arg(param)
            .arg(value)
            .query::<redis::Value>(&mut self.con);
    }

    /// Restore config params to snapshotted values via `CONFIG SET` (best-effort).
    pub fn config_restore(&mut self, defaults: &[(String, Vec<u8>)]) {
        for (p, v) in defaults {
            let _ = redis::cmd("CONFIG")
                .arg("SET")
                .arg(p)
                .arg(v.as_slice())
                .query::<redis::Value>(&mut self.con);
        }
    }

    /// Reset server state between sequences: drop any indexes, then flush keys.
    pub fn reset(&mut self) {
        if let Ok(names) = redis::cmd("FT._LIST").query::<Vec<String>>(&mut self.con) {
            for n in names {
                let _ = redis::cmd("FT.DROPINDEX")
                    .arg(&n)
                    .query::<redis::Value>(&mut self.con);
            }
        }
        let _ = redis::cmd("FLUSHALL").query::<redis::Value>(&mut self.con);
    }
}
