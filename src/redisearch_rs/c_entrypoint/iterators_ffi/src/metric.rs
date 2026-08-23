/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use ffi::{QueryIterator, RLookupKey};
use rlookup::RLookupKeyHandle;
use rqe_core::DocId;
use rqe_iterators::interop::RQEIteratorWrapper;
use rqe_iterators::{
    metric::{self, Metric, MetricType},
    utils::OwnedSlice,
};

#[unsafe(no_mangle)]
/// Creates a new metric iterator sorted by ID.
///
/// # Safety
///
/// 1. `ids` must be a valid pointer to an array of `DocId` with at least `num` elements.
///    The array must be sorted in ascending order.
/// 2. `metric_list` must be a valid pointer to an array of `f64` with at least `num` elements.
/// 3. The caller must ensure that `ids` and `metric_list` are not null unless `num` is zero.
/// 4. The memory pointed to by `ids` and `metric_list` will be freed using `RedisModule_Free`,
///    so the caller must ensure that these pointers were allocated in a compatible manner.
pub unsafe extern "C" fn NewMetricIteratorSortedById(
    ids: *mut DocId,
    metric_list: *mut f64,
    num: usize,
    type_: MetricType,
) -> *mut QueryIterator {
    // SAFETY: All safety preconditions are guaranteed by the caller.
    unsafe { new_metric_iterator::<true>(ids, metric_list, num, type_) }
}

#[unsafe(no_mangle)]
/// Creates a new metric iterator sorted by score.
///
/// # Safety
///
/// 1. `ids` must be a valid pointer to an array of `DocId` with at least `num` elements.
/// 2. `metric_list` must be a valid pointer to an array of `f64` with at least `num` elements.
/// 3. The caller must ensure that `ids` and `metric_list` are not null unless `num` is zero.
/// 4. The memory pointed to by `ids` and `metric_list` will be freed using `RedisModule_Free`,
///    so the caller must ensure that these pointers were allocated in a compatible manner.
pub unsafe extern "C" fn NewMetricIteratorSortedByScore(
    ids: *mut DocId,
    metric_list: *mut f64,
    num: usize,
    type_: MetricType,
) -> *mut QueryIterator {
    // SAFETY: All safety preconditions are guaranteed by the caller.
    unsafe { new_metric_iterator::<false>(ids, metric_list, num, type_) }
}

/// # Safety
///
/// 1. `ids` must be a valid pointer to an array of `DocId` with at least `num` elements.
/// 2. `metric_list` must be a valid pointer to an array of `f64` with at least `num` elements.
/// 3. The caller must ensure that `ids` and `metric_list` are not null unless `num` is zero.
/// 4. The memory pointed to by `ids` and `metric_list` will be freed using `RedisModule_Free`,
///    so the caller must ensure that these pointers were allocated in a compatible manner.
unsafe fn new_metric_iterator<const SORTED_BY_ID: bool>(
    ids: *mut DocId,
    metrics: *mut f64,
    num: usize,
    _type: MetricType,
) -> *mut QueryIterator {
    let (ids_list, metrics_list) = if ids.is_null() {
        // SAFETY: Safe thanks to 3.
        debug_assert_eq!(
            num, 0,
            "The pointer to the array of IDs is null, but the number of IDs is non-zero."
        );

        (OwnedSlice::default(), OwnedSlice::default())
    } else {
        debug_assert!(
            !metrics.is_null(),
            "The pointer to the array of metric data is null, but the pointer to the array of IDs is not null."
        );

        let ids = NonNull::new(ids).expect("`ids` must not be null");
        let metrics = NonNull::new(metrics).expect("`metrics` must not be null");
        // SAFETY: Safe thanks to 1.
        let ids_list = unsafe { OwnedSlice::from_c(ids, num) };
        // SAFETY: Safe thanks to 2.
        let metrics_list = unsafe { OwnedSlice::from_c(metrics, num) };

        (ids_list, metrics_list)
    };

    RQEIteratorWrapper::boxed_new(Metric::<SORTED_BY_ID>::new(ids_list, metrics_list)).as_ptr()
}

/// Sets the [`RLookupKeyHandle`] for this metric iterator.
///
/// # Safety
///
/// 1. `header` is a valid non-null pointer to a [`QueryIterator`].
/// 2. `header` was built via [`NewMetricIteratorSortedByScore`] or [`NewMetricIteratorSortedById`].
/// 3. The caller has exclusive access to that iterator for the duration of the call.
/// 4. `key_handle` is either a null pointer, or a valid non-null pointer to a [`RLookupKeyHandle`]
///    that stays live until the iterator is freed — not merely for this call. The iterator clears
///    the handle's validity flag when it is dropped, so releasing the handle while the iterator is
///    still alive is a use-after-free at that later point.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SetMetricRLookupHandle(
    header: *mut QueryIterator,
    key_handle: *mut RLookupKeyHandle<'_>,
) {
    let header = NonNull::new(header).expect("header must not be null");

    // SAFETY: 1 + 2 give the callee its live iterator, 3 its exclusive access,
    // and 4 the handle that outlasts the iterator writing through it.
    unsafe { metric::set_key_handle(header, NonNull::new(key_handle)) };
}

/// Get a pointer to the [`RLookupKey`] slot inside this metric iterator.
///
/// # Safety
///
/// 1. `header` is a valid non-null pointer to a [`QueryIterator`].
/// 2. `header` was built via [`NewMetricIteratorSortedByScore`] or [`NewMetricIteratorSortedById`].
/// 3. The caller has exclusive access to that iterator for the duration of the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GetMetricOwnKeyRef(header: *mut QueryIterator) -> *mut *mut RLookupKey {
    let header = NonNull::new(header).expect("header must not be null");

    // SAFETY: Safe thanks to 1 + 2 + 3. The borrow parameter is discharged by
    // discarding it: it is inferred to a lifetime local to this function and
    // erased by the cast below, so no key typed with it escapes to C — which
    // has none to offer and reads no borrowed string through this pointer.
    let slot = unsafe { metric::own_key_ref(header) };

    slot.as_ptr().cast::<*mut RLookupKey>()
}
