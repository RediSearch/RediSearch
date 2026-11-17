/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The shared-memory edge map that `fuzz/shim/coverage.c` writes:
//! `{ u64 capacity; u64 guards; u8 edges[capacity]; }`.

use std::fs::File;
use std::sync::atomic::{AtomicU32, Ordering};

use memmap2::MmapMut;
use nix::fcntl::OFlag;
use nix::sys::mman::{shm_open, shm_unlink};
use nix::sys::stat::Mode;
use serde::{Deserialize, Serialize};

pub const SHM_ENV: &str = "REDISEARCH_COV_SHM";
const CAPACITY: usize = 1 << 24;
const HEADER: usize = 16;

static NEXT_SEGMENT: AtomicU32 = AtomicU32::new(0);

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Coverage {
    pub guards: u64,
    pub edges: Vec<u32>,
}

pub struct Segment {
    name: String,
    map: MmapMut,
}

impl Segment {
    pub fn create() -> Result<Segment, String> {
        let name = format!(
            "/rsfz{}.{}",
            std::process::id(),
            NEXT_SEGMENT.fetch_add(1, Ordering::Relaxed)
        );
        let fd = shm_open(
            name.as_str(),
            OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_RDWR,
            Mode::S_IRUSR | Mode::S_IWUSR,
        )
        .map_err(|e| format!("shm_open {name}: {e}"))?;
        let segment = File::from(fd);
        let map = segment
            .set_len(u64::try_from(HEADER + CAPACITY).expect("fits"))
            // SAFETY: the segment was just created with O_EXCL, so only this
            // process and the server it spawns map it, and neither truncates it.
            .and_then(|()| unsafe { MmapMut::map_mut(&segment) });
        let mut map = map.map_err(|e| {
            let _ = shm_unlink(name.as_str());
            format!("mapping {name}: {e}")
        })?;
        map[..8].copy_from_slice(&u64::try_from(CAPACITY).expect("fits").to_ne_bytes());
        Ok(Segment { name, map })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn read(&self) -> Coverage {
        Coverage {
            guards: u64::from_ne_bytes(self.map[8..HEADER].try_into().expect("8 bytes")),
            edges: (0..CAPACITY)
                .filter(|&id| self.map[HEADER + id] != 0)
                .filter_map(|id| u32::try_from(id).ok())
                .collect(),
        }
    }
}

impl Drop for Segment {
    fn drop(&mut self) {
        let _ = shm_unlink(self.name.as_str());
    }
}
