/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::sync::atomic::{AtomicUsize, Ordering};

const HOT_POOL_PERCENT: usize = 80;
const HOT_TTL_MS: u64 = 60 * 1000;

struct WriteBufferManager {
    budget: AtomicUsize,
    index_count: AtomicUsize,
    hot_index_count: AtomicUsize,
    epoch: AtomicUsize,
}

impl WriteBufferManager {
    const fn new() -> Self {
        Self {
            budget: AtomicUsize::new(0),
            index_count: AtomicUsize::new(0),
            hot_index_count: AtomicUsize::new(0),
            epoch: AtomicUsize::new(0),
        }
    }

    fn snapshot(&self) -> WriteBufferSnapshot {
        WriteBufferSnapshot {
            budget: self.budget.load(Ordering::Relaxed),
            index_count: self.index_count.load(Ordering::Relaxed),
            hot_index_count: self.hot_index_count.load(Ordering::Relaxed),
            epoch: self.epoch.load(Ordering::Relaxed),
        }
    }

    fn bump_epoch(&self) {
        self.epoch.fetch_add(1, Ordering::Relaxed);
    }

    fn decrement_hot_index_count(&self) {
        let mut current = self.hot_index_count.load(Ordering::Relaxed);
        while current != 0 {
            match self.hot_index_count.compare_exchange_weak(
                current,
                current - 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(actual) => current = actual,
            }
        }
    }

    #[cfg(test)]
    fn reset(&self) {
        self.budget.store(0, Ordering::Relaxed);
        self.index_count.store(0, Ordering::Relaxed);
        self.hot_index_count.store(0, Ordering::Relaxed);
        self.epoch.store(0, Ordering::Relaxed);
    }
}

#[derive(Clone, Copy)]
struct WriteBufferSnapshot {
    budget: usize,
    index_count: usize,
    hot_index_count: usize,
    epoch: usize,
}

/// Per-index disk write-buffer state owned by the Rust budget manager.
///
/// C embeds this in `IndexSpec` and zero-initializes it with the rest of the spec.
#[repr(C)]
#[cheadergen::config(export)]
#[derive(Clone, Copy, Debug, Default)]
pub struct DiskWriteBufferIndexState {
    pub hot: bool,
    pub hot_until_ms: u64,
    pub budget_epoch: usize,
    pub applied_budget: usize,
}

static MANAGER: WriteBufferManager = WriteBufferManager::new();

const fn per_index_budget(budget: usize, index_count: usize) -> usize {
    if budget == 0 || index_count == 0 {
        return 0;
    }
    budget / index_count
}

const fn percent_of_budget(budget: usize, percentage: usize) -> usize {
    (budget / 100) * percentage + ((budget % 100) * percentage) / 100
}

const fn index_budget(hot: bool, snapshot: WriteBufferSnapshot) -> usize {
    let hot_budget = percent_of_budget(snapshot.budget, HOT_POOL_PERCENT);
    if hot {
        return per_index_budget(hot_budget, snapshot.hot_index_count);
    }

    let cold_index_count = snapshot
        .index_count
        .saturating_sub(snapshot.hot_index_count);
    per_index_budget(snapshot.budget - hot_budget, cold_index_count)
}

fn mark_index_hot(state: &mut DiskWriteBufferIndexState, now_ms: u64) {
    if !state.hot {
        state.hot = true;
        MANAGER.hot_index_count.fetch_add(1, Ordering::Relaxed);
        MANAGER.bump_epoch();
    }
    state.hot_until_ms = now_ms + HOT_TTL_MS;
}

const fn budget_update(
    state: &mut DiskWriteBufferIndexState,
    snapshot: WriteBufferSnapshot,
) -> usize {
    let budget = index_budget(state.hot, snapshot);
    if budget == 0 || (state.applied_budget == budget && state.budget_epoch == snapshot.epoch) {
        return 0;
    }

    state.applied_budget = budget;
    state.budget_epoch = snapshot.epoch;
    budget
}

/// Set the total Search disk write-buffer budget.
#[unsafe(no_mangle)]
pub extern "C" fn DiskWriteBuffer_SetModuleBudget(budget: usize) {
    if budget == 0 {
        return;
    }

    MANAGER.budget.store(budget, Ordering::Relaxed);
    MANAGER.bump_epoch();
}

/// Return whether Redis has provided a non-zero Search disk write-buffer budget.
#[unsafe(no_mangle)]
pub extern "C" fn DiskWriteBuffer_HasModuleBudget() -> bool {
    MANAGER.budget.load(Ordering::Relaxed) != 0
}

/// Register a newly opened disk index with the write-buffer budget manager.
///
/// # Safety
///
/// `state` must be null or a valid, uniquely borrowed [`DiskWriteBufferIndexState`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn DiskWriteBuffer_RegisterIndexOpen(
    state: *mut DiskWriteBufferIndexState,
    now_ms: u64,
) {
    // SAFETY: the caller owns the `IndexSpec` write path and must pass a unique state pointer.
    let Some(state) = (unsafe { state.as_mut() }) else {
        return;
    };

    MANAGER.index_count.fetch_add(1, Ordering::Relaxed);
    mark_index_hot(state, now_ms);
}

/// Unregister a disk index from the write-buffer budget manager.
///
/// # Safety
///
/// `state` must be null or a valid, uniquely borrowed [`DiskWriteBufferIndexState`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn DiskWriteBuffer_RegisterIndexClose(state: *mut DiskWriteBufferIndexState) {
    // SAFETY: the caller owns the `IndexSpec` close path and must pass a unique state pointer.
    let Some(state) = (unsafe { state.as_mut() }) else {
        return;
    };

    if state.hot {
        state.hot = false;
        MANAGER.decrement_hot_index_count();
        MANAGER.bump_epoch();
    }

    let current = MANAGER.index_count.load(Ordering::Relaxed);
    if current != 0 {
        MANAGER.index_count.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Mark a disk index as actively receiving writes and return its new budget, if changed.
///
/// # Safety
///
/// `state` must be null or a valid, uniquely borrowed [`DiskWriteBufferIndexState`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn DiskWriteBuffer_MarkIndexWriteActive(
    state: *mut DiskWriteBufferIndexState,
    now_ms: u64,
) -> usize {
    if !DiskWriteBuffer_HasModuleBudget() {
        return 0;
    }

    // SAFETY: the caller owns the `IndexSpec` write path and must pass a unique state pointer.
    let Some(state) = (unsafe { state.as_mut() }) else {
        return 0;
    };

    mark_index_hot(state, now_ms);
    budget_update(state, MANAGER.snapshot())
}

/// Return the index's new budget after refreshing hot/cold state, if changed.
///
/// # Safety
///
/// `state` must be null or a valid, uniquely borrowed [`DiskWriteBufferIndexState`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn DiskWriteBuffer_MaintainIndex(
    state: *mut DiskWriteBufferIndexState,
    now_ms: u64,
) -> usize {
    let mut snapshot = MANAGER.snapshot();
    if snapshot.budget == 0 || snapshot.index_count == 0 {
        return 0;
    }

    // SAFETY: the caller owns the `IndexSpec` write path and must pass a unique state pointer.
    let Some(state) = (unsafe { state.as_mut() }) else {
        return 0;
    };

    if state.hot && state.hot_until_ms <= now_ms {
        state.hot = false;
        MANAGER.decrement_hot_index_count();
        MANAGER.bump_epoch();
        snapshot = MANAGER.snapshot();
    }

    budget_update(state, snapshot)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn register_open(state: &mut DiskWriteBufferIndexState, now_ms: u64) {
        // SAFETY: tests pass a unique mutable state owned by this stack frame.
        unsafe { DiskWriteBuffer_RegisterIndexOpen(state, now_ms) };
    }

    fn register_open_null(now_ms: u64) {
        // SAFETY: null is explicitly accepted as a no-op by this FFI function.
        unsafe { DiskWriteBuffer_RegisterIndexOpen(std::ptr::null_mut(), now_ms) };
    }

    fn register_close_null() {
        // SAFETY: null is explicitly accepted as a no-op by this FFI function.
        unsafe { DiskWriteBuffer_RegisterIndexClose(std::ptr::null_mut()) };
    }

    fn maintain(state: &mut DiskWriteBufferIndexState, now_ms: u64) -> usize {
        // SAFETY: tests pass a unique mutable state owned by this stack frame.
        unsafe { DiskWriteBuffer_MaintainIndex(state, now_ms) }
    }

    #[test]
    fn hot_indexes_share_the_hot_pool_and_cold_indexes_share_the_rest() {
        let snapshot = WriteBufferSnapshot {
            budget: 1000,
            index_count: 10,
            hot_index_count: 2,
            epoch: 1,
        };

        assert_eq!(index_budget(true, snapshot), 400);
        assert_eq!(index_budget(false, snapshot), 25);
    }

    #[test]
    fn unchanged_budget_returns_zero_and_updates_state_only_once() {
        let snapshot = WriteBufferSnapshot {
            budget: 1000,
            index_count: 10,
            hot_index_count: 2,
            epoch: 1,
        };
        let mut state = DiskWriteBufferIndexState {
            hot: true,
            hot_until_ms: 0,
            budget_epoch: 0,
            applied_budget: 0,
        };

        assert_eq!(budget_update(&mut state, snapshot), 400);
        assert_eq!(budget_update(&mut state, snapshot), 0);
        assert_eq!(state.applied_budget, 400);
        assert_eq!(state.budget_epoch, 1);
    }

    #[test]
    fn new_index_is_hot_and_receives_hot_budget() {
        MANAGER.reset();
        DiskWriteBuffer_SetModuleBudget(1000);
        let mut first = DiskWriteBufferIndexState::default();
        let mut second = DiskWriteBufferIndexState::default();

        register_open(&mut first, 100);
        register_open(&mut second, 100);

        assert_eq!(maintain(&mut first, 100), 400);
        assert_eq!(maintain(&mut second, 100), 400);
    }

    #[test]
    fn hot_index_becomes_cold_after_ttl() {
        MANAGER.reset();
        DiskWriteBuffer_SetModuleBudget(1000);
        let mut hot = DiskWriteBufferIndexState::default();
        let mut cold = DiskWriteBufferIndexState::default();

        register_open(&mut hot, 100);
        register_open(&mut cold, 100);

        assert_eq!(maintain(&mut hot, 100 + HOT_TTL_MS + 1), 200);
    }

    #[test]
    fn null_state_registration_is_a_noop() {
        MANAGER.reset();

        register_open_null(100);
        register_close_null();

        assert_eq!(MANAGER.snapshot().index_count, 0);
    }
}
