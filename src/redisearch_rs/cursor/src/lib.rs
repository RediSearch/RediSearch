/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Cursor ownership between execution cycles. Callers serialize registry access;
//! payloads remain registered while executing, but only idle payloads are accessible.

#![warn(
    clippy::unwrap_used,
    clippy::dbg_macro,
    clippy::todo,
    clippy::unimplemented,
    clippy::print_stdout,
    clippy::panic,
    clippy::unnecessary_safety_comment,
    clippy::ptr_as_ptr,
    clippy::borrow_as_ptr,
    clippy::ref_as_ptr,
    clippy::indexing_slicing
)]

use std::{collections::HashMap, hash::BuildHasherDefault};

/// Cursor IDs encode which registry owns them through their parity.
#[derive(Clone, Copy, Debug)]
pub enum Kind {
    /// Even IDs exposed to clients.
    User,
    /// Odd IDs used for coordinator queries.
    Coordinator,
}

/// Whether collection must respect the interval between sweeps.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Collection {
    /// Explicit collection bypasses throttling.
    Forced,
    /// Opportunistic collection runs at most once per second.
    Throttled,
}

enum State {
    Executing,
    DeleteOnPause,
    Idle(usize),
}

struct Entry<T> {
    value: T,
    interval_ms: u32,
    state: State,
}

/// Owns payloads until exhaustion, deletion or idle expiry.
///
/// Destruction runs synchronously inside the registry operation. Executing
/// payloads must be returned or removed by their owner before dropping the registry.
pub struct Registry<T> {
    entries: HashMap<u64, Entry<T>, BuildHasherDefault<fnv::Fnv64>>,
    idle: Vec<(u64, u64)>,
    kind: Kind,
    counter: u32,
    last_collect: u64,
    next_timeout: Option<u64>,
}

impl<T> Registry<T> {
    /// Creates a registry with the ID parity selected by [`Kind`].
    pub fn new(kind: Kind) -> Self {
        Self {
            entries: HashMap::default(),
            idle: Vec::new(),
            kind,
            counter: 0,
            last_collect: 0,
            next_timeout: None,
        }
    }

    /// Registers an executing payload, retrying the supplied random source on collisions.
    /// The source must eventually produce an unused ID in this registry.
    pub fn reserve(&mut self, value: T, interval_ms: u32, mut random: impl FnMut() -> u32) -> u64 {
        let id = loop {
            let bits = random() & 0x7fff_fffe;
            let id = u64::from(bits)
                + match self.kind {
                    Kind::User => 2,
                    Kind::Coordinator => 1,
                };
            if !self.entries.contains_key(&id) {
                break id;
            }
        };
        self.entries.insert(
            id,
            Entry {
                value,
                interval_ms,
                state: State::Executing,
            },
        );
        id
    }

    /// Takes an idle payload for execution. Executing and unknown IDs return [`None`].
    pub fn take(&mut self, id: u64) -> Option<&T> {
        let State::Idle(pos) = self.entries.get(&id)?.state else {
            return None;
        };
        self.remove_idle(pos);
        let entry = self
            .entries
            .get_mut(&id)
            .expect("idle cursor remains registered");
        entry.state = State::Executing;
        Some(&entry.value)
    }

    /// Parks an executing payload, or destroys it if deletion was requested.
    /// Returns whether the caller must reschedule its idle timer.
    /// Idle and unknown IDs leave the registry unchanged and return `false`.
    pub fn pause(&mut self, id: u64, now_ns: u64) -> bool {
        let Some(entry) = self.entries.get_mut(&id) else {
            return false;
        };
        match entry.state {
            State::DeleteOnPause => {
                self.entries.remove(&id);
                false
            }
            State::Executing => {
                let deadline = now_ns.wrapping_add(u64::from(entry.interval_ms) * 1_000_000);
                entry.state = State::Idle(self.idle.len());
                self.idle.push((id, deadline));
                match self.next_timeout {
                    Some(next) if deadline < next => {
                        self.next_timeout = Some(deadline);
                        true
                    }
                    None => true,
                    _ => false,
                }
            }
            State::Idle(_) => false,
        }
    }

    fn remove_idle(&mut self, pos: usize) {
        let (_, deadline) = self.idle.swap_remove(pos);
        if let Some(&(id, _)) = self.idle.get(pos) {
            self.entries
                .get_mut(&id)
                .expect("idle cursor is registered")
                .state = State::Idle(pos);
        }
        if self.next_timeout == Some(deadline) {
            self.next_timeout = None;
        }
    }

    /// Destroys a payload once its owner has finished execution.
    /// Idle and unknown IDs leave the registry unchanged.
    pub fn remove(&mut self, id: u64) {
        if self
            .entries
            .get(&id)
            .is_some_and(|entry| !matches!(entry.state, State::Idle(_)))
        {
            self.entries.remove(&id);
        }
    }

    /// Deletes an idle payload or marks an executing one for deletion on [`Self::pause`].
    /// Returns whether the ID was found.
    pub fn purge(&mut self, id: u64) -> bool {
        let Some(entry) = self.entries.get_mut(&id) else {
            return false;
        };
        if let State::Idle(pos) = entry.state {
            self.remove_idle(pos);
            self.entries.remove(&id);
        } else {
            entry.state = State::DeleteOnPause;
        }
        true
    }

    /// Deletes all idle payloads and marks executing ones for deletion.
    pub fn clear(&mut self) {
        for (id, _) in self.idle.drain(..) {
            self.entries.remove(&id);
        }
        // Process-lifetime registries have no destructor at shutdown.
        self.entries.shrink_to_fit();
        self.idle.shrink_to_fit();
        self.next_timeout = None;
        for entry in self.entries.values_mut() {
            entry.state = State::DeleteOnPause;
        }
    }

    /// Returns the earliest idle deadline, recomputing an invalidated minimum.
    pub fn next_deadline(&mut self) -> Option<u64> {
        if self.next_timeout.is_none() {
            self.next_timeout = self.idle.iter().map(|&(_, deadline)| deadline).min();
        }
        self.next_timeout
    }

    /// Collects deadlines at or before `now_ns`; [`None`] means the sweep was skipped.
    /// Forced collection bypasses the one-second throttle, but not a cached future minimum.
    pub fn collect(&mut self, now_ns: u64, mode: Collection) -> Option<usize> {
        if self.next_timeout.is_some_and(|next| next > now_ns)
            || (mode == Collection::Throttled
                && now_ns.wrapping_sub(self.last_collect) < 1_000_000_000)
        {
            return None;
        }
        self.last_collect = now_ns;
        let before = self.idle.len();
        let mut pos = 0;
        for _ in 0..before {
            let &(id, deadline) = self
                .idle
                .get(pos)
                .expect("each initial idle entry is visited once");
            if deadline <= now_ns {
                self.remove_idle(pos);
                self.entries.remove(&id);
            } else {
                pos += 1;
            }
        }
        Some(before - self.idle.len())
    }

    /// Counts an operation, attempting throttled collection every 500 operations.
    /// Call before [`Self::reserve`], [`Self::take`], [`Self::pause`], [`Self::purge`]
    /// and [`Self::remove`], while holding the registry lock.
    /// The clock is read only when a sweep is due.
    pub fn tick(&mut self, now_ns: impl FnOnce() -> u64) {
        self.counter = self.counter.wrapping_add(1);
        if self.counter.is_multiple_of(500) {
            self.collect(now_ns(), Collection::Throttled);
        }
    }

    /// Number of registered payloads, including those executing or marked for deletion.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether no payloads remain registered.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Number of idle payloads.
    pub const fn idle_len(&self) -> usize {
        self.idle.len()
    }

    /// Visits only parked payloads, which execution threads no longer access.
    pub fn idle_values(&self) -> impl Iterator<Item = &T> {
        self.idle.iter().map(|(id, _)| {
            &self
                .entries
                .get(id)
                .expect("idle cursor is registered")
                .value
        })
    }
}
