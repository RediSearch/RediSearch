/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::wrapper::RQEIteratorWrapper;
use ffi::{
    IteratorType_METRIC_ITERATOR, QueryIterator, RLookupKey, RLookupKeyHandle, RedisModule_Free,
    t_docId,
};
use rqe_iterators::metric::{MetricIterator, MetricIteratorSortedById, MetricType};

#[unsafe(no_mangle)]
/// Creates a new metric iterator sorted by ID.
///
/// # Safety
///
/// 1. `ids` must be a valid pointer to an array of `t_docId` with at least `num` elements.
///    The array must be sorted in ascending order.
/// 2. `metric_list` must be a valid pointer to an array of `f64` with at least `num` elements.
/// 3. The caller must ensure that `ids` and `metric_list` are not null unless `num` is zero.
/// 4. The memory pointed to by `ids` and `metric_list` will be freed using `RedisModule_Free`,
///    so the caller must ensure that these pointers were allocated in a compatible manner.
pub unsafe extern "C" fn NewMetricIteratorSortedById(
    ids: *const t_docId,
    metric_list: *const f64,
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
/// 1. `ids` must be a valid pointer to an array of `t_docId` with at least `num` elements.
/// 2. `metric_list` must be a valid pointer to an array of `f64` with at least `num` elements.
/// 3. The caller must ensure that `ids` and `metric_list` are not null unless `num` is zero.
/// 4. The memory pointed to by `ids` and `metric_list` will be freed using `RedisModule_Free`,
///    so the caller must ensure that these pointers were allocated in a compatible manner.
pub unsafe extern "C" fn NewMetricIteratorSortedByScore(
    ids: *const t_docId,
    metric_list: *const f64,
    num: usize,
    type_: MetricType,
) -> *mut QueryIterator {
    // SAFETY: All safety preconditions are guaranteed by the caller.
    unsafe { new_metric_iterator::<false>(ids, metric_list, num, type_) }
}

/// # Safety
///
/// 1. `ids` must be a valid pointer to an array of `t_docId` with at least `num` elements.
/// 2. `metric_list` must be a valid pointer to an array of `f64` with at least `num` elements.
/// 3. The caller must ensure that `ids` and `metric_list` are not null unless `num` is zero.
/// 4. The memory pointed to by `ids` and `metric_list` will be freed using `RedisModule_Free`,
///    so the caller must ensure that these pointers were allocated in a compatible manner.
unsafe fn new_metric_iterator<const SORTED_BY_ID: bool>(
    ids: *const t_docId,
    metric_list: *const f64,
    num: usize,
    _type: MetricType,
) -> *mut QueryIterator {
    // TODO: Figure out a mechanism to avoid re-allocating ids and metrics on the Rust side.
    let mut vec_ids = Vec::with_capacity(num);
    let mut vec_metrics = Vec::with_capacity(num);
    if !ids.is_null() {
        debug_assert!(
            !metric_list.is_null(),
            "The pointer to the array of metric data is null, but the pointer to the array of IDs is not null."
        );
        // SAFETY: The free function has been initialized at this stage.
        let free_fn = unsafe { RedisModule_Free.unwrap() };
        // SAFETY: Safe thanks to 1 + 3.
        let slice = unsafe { std::slice::from_raw_parts(ids, num) };
        vec_ids.extend_from_slice(slice);
        // SAFETY: Safe thanks to 4.
        unsafe { free_fn(ids as *mut std::os::raw::c_void) };

        // SAFETY: Safe thanks to 2 + 3.
        let slice = unsafe { std::slice::from_raw_parts(metric_list, num) };
        vec_metrics.extend_from_slice(slice);
        // SAFETY: Safe thanks to 4.
        unsafe { free_fn(metric_list as *mut std::os::raw::c_void) };
    } else {
        debug_assert_eq!(
            num, 0,
            "The pointer to the array of IDs is null, but the number of IDs is non-zero."
        );
    }
    RQEIteratorWrapper::boxed_new(
        IteratorType_METRIC_ITERATOR,
        MetricIterator::<SORTED_BY_ID>::new(vec_ids, vec_metrics),
    )
}

/// Sets the [`RLookupKeyHandle`] for this metric iterator.
///
/// # Safety
///
/// 1. `header` is a valid non-null pointer to a [`QueryIterator`].
/// 2. `header` was built via [`NewMetricIteratorSortedByScore`] or [`NewMetricIteratorSortedById`].
/// 3. `key_handle` is either a null pointer or a valid non-null pointer to a [`RLookupKeyHandle`] instance.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SetMetricRLookupHandle(
    header: *mut QueryIterator,
    key_handle: *mut RLookupKeyHandle,
) {
    debug_assert!(!header.is_null());
    debug_assert_eq!(
        // SAFETY: Safe thanks to 1.
        unsafe { *header }.type_,
        IteratorType_METRIC_ITERATOR,
        "Expected a metric iterator"
    );
    // SAFETY: Safe thanks to 1 + 2.
    let wrapper = unsafe { RQEIteratorWrapper::<MetricIteratorSortedById>::from_header(header) };
    // SAFETY: Safe thanks to 3.
    unsafe { wrapper.inner.set_handle(key_handle) };
}

/// Get a mutable reference to the [`RLookupKey`] stored inside this metric iterator.
///
/// # Safety
///
/// 1. `header` is a valid non-null pointer to a [`QueryIterator`].
/// 2. `header` was built via [`NewMetricIteratorSortedByScore`] or [`NewMetricIteratorSortedById`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GetMetricOwnKeyRef(header: *mut QueryIterator) -> *mut *mut RLookupKey {
    debug_assert!(!header.is_null());
    debug_assert_eq!(
        // SAFETY: Safe thanks to 1.
        unsafe { *header }.type_,
        IteratorType_METRIC_ITERATOR,
        "Expected a metric iterator"
    );
    // SAFETY: Safe thanks to 1 + 2.
    let wrapper = unsafe { RQEIteratorWrapper::<MetricIteratorSortedById>::from_header(header) };
    wrapper.inner.key_mut_ref() as *mut _
}

/// Get the metric type used by this metric iterator.
///
/// # Safety
///
/// 1. `header` is a valid non-null pointer to a [`QueryIterator`].
/// 2. `header` was built via [`NewMetricIteratorSortedByScore`] or [`NewMetricIteratorSortedById`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GetMetricType(header: *mut QueryIterator) -> MetricType {
    debug_assert!(!header.is_null());
    debug_assert_eq!(
        // SAFETY: Safe thanks to 1.
        unsafe { *header }.type_,
        IteratorType_METRIC_ITERATOR,
        "Expected a metric iterator"
    );
    // SAFETY: Safe thanks to 1 + 2.
    let wrapper = unsafe { RQEIteratorWrapper::<MetricIteratorSortedById>::from_header(header) };
    wrapper.inner.metric_type()
}
