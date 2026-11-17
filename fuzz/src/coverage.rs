/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The shared-memory edge map written by `fuzz/shim/coverage.c`.

use std::fs::File;
use std::sync::atomic::{AtomicU32, Ordering};

use memmap2::MmapMut;
use nix::fcntl::OFlag;
use nix::sys::mman::{shm_open, shm_unlink};
use nix::sys::stat::Mode;
use serde::{Deserialize, Serialize};

pub const SHM_ENV: &str = "REDISEARCH_COV_SHM";
const MAGIC: u64 = 0x5253_4656_434f_5631;
const MAGIC_AT: usize = 0;
const CAPACITY_AT: usize = 8;
const GUARDS_AT: usize = 16;
const EDGES_AT: usize = 24;

static NEXT_SEGMENT: AtomicU32 = AtomicU32::new(0);

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Coverage {
    pub guards: u64,
    pub edges: Vec<u32>,
}

pub struct Segment {
    name: String,
    map: MmapMut,
    capacity: usize,
}

impl Segment {
    pub fn create(capacity: usize) -> Result<Segment, String> {
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
        let file = File::from(fd);
        let mapped = file
            .set_len(u64::try_from(EDGES_AT + capacity).map_err(|e| e.to_string())?)
            // SAFETY: the segment was just created with O_EXCL, so only this
            // process and the server it spawns can map it, and neither ever
            // truncates it while the mapping is alive.
            .and_then(|()| unsafe { MmapMut::map_mut(&file) });
        let mut map = match mapped {
            Ok(map) => map,
            Err(e) => {
                let _ = shm_unlink(name.as_str());
                return Err(format!("mapping {name}: {e}"));
            }
        };
        map[MAGIC_AT..CAPACITY_AT].copy_from_slice(&MAGIC.to_ne_bytes());
        map[CAPACITY_AT..GUARDS_AT].copy_from_slice(
            &u64::try_from(capacity)
                .map_err(|e| e.to_string())?
                .to_ne_bytes(),
        );
        Ok(Segment {
            name,
            map,
            capacity,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn guards(&self) -> u64 {
        let mut word = [0u8; 8];
        word.copy_from_slice(&self.map[GUARDS_AT..EDGES_AT]);
        u64::from_ne_bytes(word)
    }

    pub fn edges(&self) -> Vec<u32> {
        self.map[EDGES_AT..EDGES_AT + self.capacity]
            .iter()
            .enumerate()
            .filter(|(_, hit)| **hit != 0)
            .filter_map(|(id, _)| u32::try_from(id).ok())
            .collect()
    }
}

impl Drop for Segment {
    fn drop(&mut self) {
        let _ = shm_unlink(self.name.as_str());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_new_segment_has_a_header_and_no_edges() {
        let segment = Segment::create(64).unwrap();
        assert_eq!(&segment.map[MAGIC_AT..CAPACITY_AT], &MAGIC.to_ne_bytes());
        assert_eq!(segment.guards(), 0);
        assert!(segment.edges().is_empty());
    }

    #[test]
    fn edges_are_the_ids_written_by_the_shim() {
        let mut segment = Segment::create(64).unwrap();
        segment.map[EDGES_AT + 3] = 1;
        segment.map[EDGES_AT + 63] = 1;
        segment.map[GUARDS_AT..EDGES_AT].copy_from_slice(&40u64.to_ne_bytes());
        assert_eq!(segment.edges(), [3, 63]);
        assert_eq!(segment.guards(), 40);
    }

    #[test]
    fn segments_are_removed_when_dropped() {
        let name = {
            let segment = Segment::create(8).unwrap();
            segment.name().to_owned()
        };
        assert!(shm_open(name.as_str(), OFlag::O_RDWR, Mode::empty()).is_err());
    }
}
