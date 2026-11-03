/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! C ownership boundary for [`QuantStream`].

pub use quantile::QuantStream;
use std::num::NonZeroUsize;

/// Creates a stream, copying the targets. Returns null for zero buffer capacity.
/// Release the returned stream with [`QS_Free`].
///
/// # Safety
/// `quantiles` must be aligned and [valid] for reading `num_quantiles` initialized
/// doubles during this call; the total size must not exceed [`isize::MAX`].
/// It may be null when `num_quantiles` is zero.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NewQuantileStream(
    quantiles: *const f64,
    num_quantiles: usize,
    buffer_length: usize,
) -> *mut QuantStream {
    let Some(capacity) = NonZeroUsize::new(buffer_length) else {
        return std::ptr::null_mut();
    };
    let targets = if num_quantiles == 0 {
        &[]
    } else {
        debug_assert!(!quantiles.is_null());
        // SAFETY: the nonempty target range is readable for this call and copied by new.
        unsafe { std::slice::from_raw_parts(quantiles, num_quantiles) }
    };
    Box::into_raw(Box::new(QuantStream::new(targets, capacity)))
}

/// Adds an observation; NaNs are ignored and signed zeros normalized to positive zero.
///
/// # Safety
/// `stream` must be a non-null, aligned, initialized, [valid] stream with exclusive
/// access for this call.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn QS_Insert(stream: *mut QuantStream, value: f64) {
    debug_assert!(!stream.is_null());
    // SAFETY: the live stream is exclusively accessible for this mutation.
    unsafe { (&mut *stream).insert(value) };
}

/// Flushes pending values and returns the quantile estimate, or NaN if empty.
///
/// # Safety
/// `stream` must be a non-null, aligned, initialized, [valid] stream with exclusive
/// access for this call, because querying flushes pending observations.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn QS_Query(stream: *mut QuantStream, q: f64) -> f64 {
    debug_assert!(!stream.is_null());
    // SAFETY: the live stream is exclusively accessible for the query's flush.
    unsafe { (&mut *stream).query(q) }
}

/// Returns the count of flushed observations, excluding the pending buffer.
///
/// # Safety
/// `stream` must be non-null, aligned, initialized and [valid] for shared access
/// during this call, without concurrent mutation or destruction.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn QS_GetCount(stream: *const QuantStream) -> usize {
    debug_assert!(!stream.is_null());
    // SAFETY: shared access to the live stream excludes concurrent writes.
    unsafe { (&*stream).count() }
}

/// Consumes a stream and releases all its allocations.
///
/// # Safety
/// `stream` must be a non-null pointer returned by [`NewQuantileStream`], not yet
/// freed, with exclusive ownership and no outstanding references. Do not use it again.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn QS_Free(stream: *mut QuantStream) {
    debug_assert!(!stream.is_null());
    // SAFETY: this uniquely owned allocation originated from Box::into_raw above.
    unsafe { drop(Box::from_raw(stream)) };
}
