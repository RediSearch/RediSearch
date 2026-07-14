/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Benchmark-grade safe-memory-reclamation (SMR) epoch for the lock-free TTL
//! table reads. This is the Rust analogue of the C `src/util/lockfree_reclaim.c`
//! used by the lock-free doc table.
//!
//! A retired object is destroyed only once no reader can still hold a pointer to
//! it. Correctness rests on one caller-side invariant:
//!
//! > **RETIRE-AFTER-UNLINK**: an object is retired only *after* it has been made
//! > unreachable to readers that begin a new read section — i.e. the bucket slot
//! > has been overwritten (copy-on-write), or the containing bucket array has
//! > been replaced by a freshly published one.
//!
//! Given that, any reader that could still reach the object is inside a read
//! section, so it is safe to destroy once the active-reader count returns to
//! zero. This is a global quiescent point, not a per-object grace period, so a
//! continuous stream of readers can delay reclamation — but TTL read sections
//! are a single predicate call, so the pending set stays bounded.
//!
//! When no reader is active (the default, where reads still run under the spec
//! read lock) retirement degrades to an immediate free, so there is no added
//! latency or retention on the product path.

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Threads currently inside a lock-free read section.
static ACTIVE: AtomicUsize = AtomicUsize::new(0);

/// Pending retirements as `(pointer-as-usize, destructor)`. `usize` (not a raw
/// pointer) so the list is `Send`; the destructor reconstitutes the box.
static RETIRED: Mutex<Vec<(usize, unsafe fn(usize))>> = Mutex::new(Vec::new());

/// Enter a lock-free read section.
fn read_begin() {
    ACTIVE.fetch_add(1, Ordering::SeqCst);
}

/// Leave a lock-free read section, draining the pending set if we were the last
/// reader so it does not accumulate between writes.
fn read_end() {
    if ACTIVE.fetch_sub(1, Ordering::SeqCst) == 1 {
        try_reclaim();
    }
}

/// Retire `ptr` for deferred destruction via `dtor`. The caller must already
/// satisfy RETIRE-AFTER-UNLINK (see module docs). Frees immediately when no
/// reader is active.
///
/// # Safety
/// `ptr` must be a valid argument for `dtor`, and the `(ptr, dtor)` pair must be
/// retired at most once.
pub unsafe fn retire(ptr: usize, dtor: unsafe fn(usize)) {
    if ACTIVE.load(Ordering::SeqCst) == 0 {
        // SAFETY: object is already unlinked (caller contract) and no reader is
        // active, so nothing can dereference it.
        unsafe { dtor(ptr) };
        return;
    }
    RETIRED.lock().unwrap().push((ptr, dtor));
}

/// Free the pending set iff no reader is active. Every retired object was
/// unlinked before retirement, so if the count is zero now, every reader that
/// could have reached it has executed its seq-cst decrement (observed by this
/// seq-cst load) and finished dereferencing it.
pub fn try_reclaim() {
    if ACTIVE.load(Ordering::SeqCst) != 0 {
        return;
    }
    let batch = {
        let mut guard = RETIRED.lock().unwrap();
        if guard.is_empty() {
            return;
        }
        std::mem::take(&mut *guard)
    };
    for (ptr, dtor) in batch {
        // SAFETY: no reader is active and the object was unlinked before being
        // retired, so no live reference to it remains.
        unsafe { dtor(ptr) };
    }
}

/// RAII guard for a lock-free read section. Hold it for the duration of a read
/// that dereferences published buckets.
pub struct ReadGuard;

/// Enter a read section, returning a guard that leaves it on drop.
pub fn pin() -> ReadGuard {
    read_begin();
    ReadGuard
}

impl Drop for ReadGuard {
    fn drop(&mut self) {
        read_end();
    }
}
