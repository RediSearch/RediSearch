/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Edge coverage from SanitizerCoverage's `trace-pc-guard`. Build the binary
//! under test with `-fsanitize-coverage=trace-pc-guard` and link
//! `shim/coverage.c` into it uninstrumented. The shim writes
//! `{ u64 capacity; u64 guards; u8 edges[capacity]; }` to the shared memory
//! named in [`ENV`], so edges reached by forked children or just before a
//! crash are kept.

use std::fs::File;
use std::io;
use std::process::Command;
use std::sync::atomic::{AtomicU32, Ordering};

use memmap2::MmapMut;
use nix::fcntl::OFlag;
use nix::sys::mman::{shm_open, shm_unlink};
use nix::sys::stat::Mode;

pub const ENV: &str = "PROCFUZZ_COVERAGE_SHM";
/// Edge ids at or above this are dropped by the shim.
pub const CAPACITY: usize = 1 << 20;
const HEADER: usize = 16;

static NEXT_MAP: AtomicU32 = AtomicU32::new(0);

pub struct CoverageMap {
    name: String,
    map: MmapMut,
}

impl CoverageMap {
    pub fn new() -> io::Result<CoverageMap> {
        // macOS limits shared-memory names to 31 bytes.
        let name = format!(
            "/procfuzz{}.{}",
            std::process::id(),
            NEXT_MAP.fetch_add(1, Ordering::Relaxed)
        );
        let fd = shm_open(
            name.as_str(),
            OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_RDWR,
            Mode::S_IRUSR | Mode::S_IWUSR,
        )?;
        let segment = File::from(fd);
        let map = segment
            .set_len(u64::try_from(HEADER + CAPACITY).expect("fits"))
            // SAFETY: the segment was just created with O_EXCL, so only this
            // process and the ones it starts map it, and none truncates it.
            .and_then(|()| unsafe { MmapMut::map_mut(&segment) });
        let mut map = map.inspect_err(|_| {
            let _ = shm_unlink(name.as_str());
        })?;
        map[..8].copy_from_slice(&u64::try_from(CAPACITY).expect("fits").to_ne_bytes());
        Ok(CoverageMap { name, map })
    }

    /// Makes the process `command` starts write its edges here.
    pub fn attach(&self, command: &mut Command) {
        command.env(ENV, &self.name);
    }

    /// How many edges the binary has.
    pub fn guards(&self) -> u64 {
        u64::from_ne_bytes(self.map[8..HEADER].try_into().expect("8 bytes"))
    }

    /// The ids of the edges reached. Complete only once the process and its
    /// children have exited.
    pub fn edges(&self) -> impl Iterator<Item = u32> + '_ {
        // Guard ids start at 1, so the last one is `guards` itself.
        let end = usize::try_from(self.guards())
            .map_or(CAPACITY, |guards| guards.saturating_add(1).min(CAPACITY));
        (1..end)
            .filter(|&id| self.map[HEADER + id] != 0)
            .filter_map(|id| u32::try_from(id).ok())
    }
}

impl Drop for CoverageMap {
    fn drop(&mut self) {
        let _ = shm_unlink(self.name.as_str());
    }
}
