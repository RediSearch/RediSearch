/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! `redis-server` with the module and RedisJSON loaded.

use std::path::PathBuf;

use procfuzz::service::{Admin, Service};
use procfuzz_redis::resp::{Bytes, Resp2, Value, argv};
use serde::Serialize;

use crate::effect::{DEBUG, undo_all};

/// The binaries, from the variables `bin/fuzz/env.sh` sets.
#[derive(Clone, Debug, Serialize)]
pub struct Env {
    pub redis_server: PathBuf,
    pub module: PathBuf,
    /// RedisJSON, loaded after the module as the Python tests do.
    pub rejson: PathBuf,
}

impl Env {
    pub fn from_env() -> Result<Env, String> {
        let var = |name: &str| {
            std::env::var_os(name).map(PathBuf::from).ok_or_else(|| {
                format!("{name} is not set; run fuzz/setup.sh and source bin/fuzz/env.sh")
            })
        };
        Ok(Env {
            redis_server: var("REDIS_SERVER")?,
            module: var("REDISEARCH_MODULE")?,
            rejson: var("REJSON_MODULE")?,
        })
    }

    /// Heals by undoing every pause a command can set, and is idle once no
    /// index is indexing and the workers and the GC have nothing left to do.
    pub fn service(&self) -> Service<Resp2> {
        let modules = vec![self.module.clone(), self.rejson.clone()];
        let mut service = procfuzz_redis::service(self.redis_server.clone(), modules);
        service.heal = Box::new(heal);
        service.idle = Box::new(|admin| idle(admin).unwrap_or(false));
        service
    }
}

/// Stops at the first command the server does not answer: its main thread
/// is blocked, so the rest would time out too.
fn heal(admin: &mut Admin<'_, Resp2>) {
    let indexes = indexes(admin).unwrap_or_default();
    for command in undo_all(&indexes) {
        if admin.call(&command).is_none() {
            return;
        }
    }
}

fn idle(admin: &mut Admin<'_, Resp2>) -> Option<bool> {
    for name in indexes(admin)? {
        let info = admin.call(&vec!["FT.INFO".into(), name])?;
        if field(&info, "indexing") != Some(0) {
            return Some(false);
        }
    }
    let stats = admin.call(&argv(&[DEBUG, "WORKERS", "STATS"]))?;
    let idle = ["totalPendingJobs", "numJobsInProgress"]
        .iter()
        .all(|name| field(&stats, name) == Some(0));
    if idle {
        admin.call(&argv(&[DEBUG, "GC_WAIT_FOR_JOBS"]))?;
    }
    Some(idle)
}

fn indexes(admin: &mut Admin<'_, Resp2>) -> Option<Vec<Bytes>> {
    let Value::Array(names) = admin.call(&argv(&["FT._LIST"]))? else {
        return None;
    };
    Some(
        names
            .into_iter()
            .filter_map(|name| match name {
                Value::Bulk(name) | Value::Simple(name) => Some(name),
                _ => None,
            })
            .collect(),
    )
}

/// An integer in a flat `[name, value, …]` reply.
fn field(reply: &Value, name: &str) -> Option<i64> {
    let Value::Array(items) = reply else {
        return None;
    };
    items.chunks(2).find_map(|pair| match pair {
        [Value::Bulk(key) | Value::Simple(key), value] if key.as_slice() == name.as_bytes() => {
            match value {
                Value::Int(n) => Some(*n),
                Value::Bulk(text) | Value::Simple(text) => {
                    std::str::from_utf8(text.as_slice()).ok()?.parse().ok()
                }
                _ => None,
            }
        }
        _ => None,
    })
}
