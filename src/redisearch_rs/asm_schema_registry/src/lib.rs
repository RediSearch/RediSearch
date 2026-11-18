/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use rdb_io::RdbIO;
use std::collections::BTreeSet;
use std::io;

/// Names dropped while an ASM import may still deliver an older schema.
///
/// Core serializes destination imports. Local creation does not clear records:
/// they suppress imported schemas, never delete the replacement index.
#[derive(Default)]
pub struct AsmSchemaRegistry {
    active: Option<Import>,
}

struct Import {
    task_id: Vec<u8>,
    dropped: BTreeSet<Vec<u8>>,
}

impl AsmSchemaRegistry {
    /// Starts an import, preserving records on duplicate notifications.
    ///
    /// # Panics
    /// Panics if switching tasks would discard deletion records. Core serializes
    /// imports; full sync clears pre-snapshot records with [`Self::begin_loading`].
    pub fn start(&mut self, task_id: &[u8]) {
        if self
            .active
            .as_ref()
            .is_some_and(|active| active.task_id == task_id)
        {
            return;
        }
        assert!(
            self.active
                .as_ref()
                .is_none_or(|active| active.dropped.is_empty()),
            "ASM task changed without ending the previous import or beginning full sync"
        );
        self.active = Some(Import {
            task_id: task_id.to_vec(),
            dropped: BTreeSet::new(),
        });
    }

    /// Ends the matching import; stale terminal notifications are ignored.
    pub fn end(&mut self, task_id: &[u8]) {
        if self
            .active
            .as_ref()
            .is_some_and(|active| active.task_id == task_id)
        {
            self.active = None;
        }
    }

    /// Records a drop, returning whether a new record was inserted.
    pub fn record_drop(&mut self, name: &[u8]) -> bool {
        self.active
            .as_mut()
            .is_some_and(|active| active.dropped.insert(name.to_vec()))
    }

    /// Whether schema replay must ignore this name.
    pub fn was_dropped(&self, name: &[u8]) -> bool {
        self.active
            .as_ref()
            .is_some_and(|active| active.dropped.contains(name))
    }

    /// Whether an RDB must carry deletion records even without any indexes.
    pub fn has_drops(&self) -> bool {
        self.active
            .as_ref()
            .is_some_and(|active| !active.dropped.is_empty())
    }

    /// Clears pre-snapshot records while retaining the active task.
    ///
    /// Core may suppress a duplicate start notification during full sync.
    pub fn begin_loading(&mut self) {
        if let Some(active) = &mut self.active {
            active.dropped.clear();
        }
    }

    /// Saves the active import's deletion records in the module auxiliary payload.
    pub fn save(&self, io: &mut impl RdbIO) {
        let active = self
            .active
            .as_ref()
            .filter(|active| !active.dropped.is_empty());
        io.write_u64(u64::from(active.is_some()));
        if let Some(active) = active {
            io.write_buffer(&active.task_id);
            io.write_u64(active.dropped.len() as u64);
            for name in &active.dropped {
                io.write_buffer(name);
            }
        }
    }

    /// Replaces records only for the task restored by Core before module auxiliary loading.
    ///
    /// Core's `cluster-asm-task` RDB field precedes module auxiliary data. It restores
    /// the serialized task ID and notifies Search before this call; duplicate start
    /// notifications may be suppressed. Cold recovery and snapshots from another
    /// task consume but discard the records.
    ///
    /// # Errors
    /// Returns an error on malformed framing or failed IO, without changing the tracker.
    pub fn load(&mut self, io: &mut impl RdbIO) -> io::Result<()> {
        let saved = match io.read_u64()? {
            0 => None,
            1 => {
                let task_id = io.read_buffer()?;
                let count = io.read_u64()?;
                let mut dropped = BTreeSet::new();
                for _ in 0..count {
                    dropped.insert(io.read_buffer()?);
                }
                Some(Import { task_id, dropped })
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid import marker",
                ));
            }
        };
        self.begin_loading();
        if let (Some(active), Some(saved)) = (&mut self.active, saved)
            && active.task_id == saved.task_id
        {
            active.dropped = saved.dropped;
        }
        Ok(())
    }
}
