/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::cell::RefCell;

use triomphe::Arc;

use crate::RsValue;

/// Maximum number of `Arc<RsValue>` allocations to keep in the thread-local pool.
/// Matches the C `mempool_t` capacity used for RSValue recycling.
const MAX_POOL_SIZE: usize = 1000;

/// Thread-local pool of recycled `Arc<RsValue>` allocations.
///
/// When a [`super::SharedRsValue`] is the last reference to its `Arc<RsValue>`,
/// the Arc is returned to this pool instead of being deallocated. New
/// [`super::SharedRsValue`] allocations pop from the pool first, avoiding
/// malloc/free churn in hot loops (e.g. sort pipelines clearing thousands of
/// search results).
///
/// # Concurrency
///
/// The pool is accessed exclusively through `thread_local!` storage, so no
/// cross-thread contention is possible. A `SharedRsValue` created on thread A
/// and dropped on thread B will be recycled into thread B's pool — this is
/// safe because the `Arc` allocation is globally valid regardless of which
/// thread holds it.
///
/// Arcs stored in the pool always have `strong_count == 1` (the pool itself is
/// the sole owner). No other thread can hold a reference to a pooled Arc, so
/// `Arc::get_mut` on a pooled entry is guaranteed to succeed.
struct Pool(Vec<Arc<RsValue>>);

thread_local! {
    static POOL: RefCell<Pool> = const { RefCell::new(Pool(Vec::new())) };
}

/// Get a recycled `Arc<RsValue>` with the given value, or allocate a new one.
pub(crate) fn pool_get(value: RsValue) -> Arc<RsValue> {
    // Use `try_with` to be thread-local destruction safe in the rare case
    // that `pool_get` is called during thread-local destruction.
    if let Some(mut arc) = POOL
        .try_with(|pool| pool.borrow_mut().0.pop())
        .ok()
        .flatten()
    {
        // `strong_count == 1` (pool held the only reference), so `get_mut`
        // always succeeds.
        *Arc::get_mut(&mut arc).unwrap() = value;
        arc
    } else {
        Arc::new(value)
    }
}

/// Return an `Arc<RsValue>` to the pool for recycling, or drop it if pool is full.
///
/// # Panics
///
/// Panics if `strong_count > 1` (i.e. the caller is not the sole owner).
pub(crate) fn pool_release(mut arc: Arc<RsValue>) {
    // Clear the value to release any owned resources (strings, arrays, etc.)
    // `get_mut` succeeds because the caller is the sole owner (see `Pool` docs).
    *Arc::get_mut(&mut arc).unwrap() = RsValue::Undefined;

    // This function is called from `SharedRsValue::drop`. During thread shutdown,
    // thread-local destruction order is unspecified, so the `POOL` TLS may already
    // be destroyed when a `SharedRsValue` held (directly or transitively) in another
    // thread-local is dropped. `LocalKey::with` (used by `with_borrow_mut`) would panic
    // in that case, and a panic inside `Drop` during thread shutdown aborts the process.
    //
    // We therefore use `try_with` + `borrow_mut` instead: if the pool is already
    // destroyed, `try_with` returns `Err` and we silently fall through, letting the
    // `Arc` deallocate normally rather than aborting.
    let _ = POOL.try_with(|pool| {
        let mut pool = pool.borrow_mut();
        if pool.0.len() < MAX_POOL_SIZE {
            pool.0.push(arc);
        }
        // else: drop the Arc, deallocating it
    });
}
