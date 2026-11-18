/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! `redis-server` with the module and RedisJSON loaded.

use std::path::{Path, PathBuf};

use procfuzz::service::{Admin, Service};
use procfuzz_redis::resp::{Bytes, Resp2, Value, argv};
use serde::Serialize;

use crate::effect::{DEBUG, undo_all};

const ROOT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");

/// The binaries; by default where `fuzz/setup.sh` puts them.
#[derive(Clone, Debug, Serialize, clap::Args)]
pub struct Server {
    #[arg(long, default_value_os_t = built("redis-server"))]
    pub redis_server: PathBuf,
    /// `bin/fuzz/planted/redisearch.so` has known bugs, for testing the
    /// fuzzer.
    #[arg(long, default_value_os_t = built("redisearch.so"))]
    pub module: PathBuf,
    /// RedisJSON, loaded after the module as the Python tests do.
    #[arg(long, default_value_os_t = built("rejson.so"))]
    pub rejson: PathBuf,
}

/// A file `fuzz/setup.sh` writes, such as `planted/redisearch.so`.
pub fn built(name: &str) -> PathBuf {
    Path::new(ROOT).join("bin/fuzz").join(name)
}

impl Default for Server {
    fn default() -> Server {
        Server {
            redis_server: built("redis-server"),
            module: built("redisearch.so"),
            rejson: built("rejson.so"),
        }
    }
}

impl Server {
    pub fn check(&self) -> Result<(), String> {
        for path in [&self.redis_server, &self.module, &self.rejson] {
            if !path.exists() {
                return Err(format!(
                    "{} does not exist; run fuzz/setup.sh",
                    path.display()
                ));
            }
        }
        Ok(())
    }

    /// Heals by undoing every pause a command can set, and is idle once no
    /// index is indexing and the workers and the GC have nothing left to do.
    pub fn service(&self) -> Service<Resp2> {
        let modules = vec![self.module.clone(), self.rejson.clone()];
        // Halting makes each run yield one report. Without RS_GLOBAL_DTORS
        // the module skips its teardown and every global allocation reports
        // as a leak.
        let env = [
            (
                "ASAN_OPTIONS",
                "halt_on_error=1:detect_odr_violation=0:detect_leaks=1".to_owned(),
            ),
            (
                "LSAN_OPTIONS",
                format!("suppressions={ROOT}/tests/memcheck/asan.supp:print_suppressions=0"),
            ),
            ("RS_GLOBAL_DTORS", "1".to_owned()),
        ];
        let env = env
            .into_iter()
            .map(|(name, value)| (name.to_owned(), value))
            .collect();
        let mut service = procfuzz_redis::service(self.redis_server.clone(), modules, env);
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
