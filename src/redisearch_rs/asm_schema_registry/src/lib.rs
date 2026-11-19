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
/// Core serializes imports and delivers their lifecycle events synchronously.
/// Local creation does not clear records: they suppress imported schemas,
/// never delete the replacement index.
#[derive(Default)]
pub struct AsmSchemaRegistry {
    dropped: Option<BTreeSet<Vec<u8>>>,
}

impl AsmSchemaRegistry {
    /// Starts tracking an import, preserving records on duplicate notifications.
    pub fn start(&mut self) {
        self.dropped.get_or_insert_default();
    }

    /// Releases deletion records on import completion or failure.
    pub fn end(&mut self) {
        self.dropped = None;
    }

    /// Records a drop, returning whether a new record was inserted.
    pub fn record_drop(&mut self, name: &[u8]) -> bool {
        self.dropped
            .as_mut()
            .is_some_and(|names| names.insert(name.to_vec()))
    }

    /// Whether schema replay must ignore this name.
    pub fn was_dropped(&self, name: &[u8]) -> bool {
        self.dropped
            .as_ref()
            .is_some_and(|names| names.contains(name))
    }

    /// Whether an RDB must carry deletion records even without any indexes.
    pub fn has_drops(&self) -> bool {
        self.dropped.as_ref().is_some_and(|names| !names.is_empty())
    }

    /// Clears pre-snapshot records while retaining active-import state.
    ///
    /// Core may suppress a duplicate start notification during full sync.
    pub fn begin_loading(&mut self) {
        if let Some(names) = &mut self.dropped {
            names.clear();
        }
    }

    /// Saves deletion records in the module auxiliary payload.
    pub fn save(&self, io: &mut impl RdbIO) {
        let saved = self.dropped.as_ref().filter(|names| !names.is_empty());
        io.write_u64(u64::from(saved.is_some()));
        if let Some(names) = saved {
            io.write_u64(names.len() as u64);
            for name in names {
                io.write_buffer(name);
            }
        }
    }

    /// Replaces records when Core has established an active import.
    ///
    /// Core restores its import state before module auxiliary loading. Together
    /// with [`Self::begin_loading`], this also handles suppressed duplicate start
    /// notifications. Cold recovery consumes but discards the saved records.
    ///
    /// # Errors
    /// Returns an error on malformed framing or failed IO, without changing the registry.
    pub fn load(&mut self, io: &mut impl RdbIO) -> io::Result<()> {
        let saved = match io.read_u64()? {
            0 => BTreeSet::new(),
            1 => {
                let count = io.read_u64()?;
                let mut names = BTreeSet::new();
                for _ in 0..count {
                    names.insert(io.read_buffer()?);
                }
                names
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid import marker",
                ));
            }
        };
        if let Some(names) = &mut self.dropped {
            *names = saved;
        }
        Ok(())
    }
}
