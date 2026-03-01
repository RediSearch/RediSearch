/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#![allow(non_snake_case, non_upper_case_globals)]

use ffi::{
    IteratorType_METRIC_SORTED_BY_ID_ITERATOR, IteratorType_METRIC_SORTED_BY_SCORE_ITERATOR,
    QueryIterator, RLookupKey, RLookupKeyHandle, t_docId,
};
use rqe_iterators::{
    metric::{Metric, MetricSortedById, MetricSortedByScore, MetricType},
    utils::OwnedSlice,
};
use rqe_iterators_interop::RQEIteratorWrapper;

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
    ids: *mut t_docId,
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
/// 1. `ids` must be a valid pointer to an array of `t_docId` with at least `num` elements.
/// 2. `metric_list` must be a valid pointer to an array of `f64` with at least `num` elements.
/// 3. The caller must ensure that `ids` and `metric_list` are not null unless `num` is zero.
/// 4. The memory pointed to by `ids` and `metric_list` will be freed using `RedisModule_Free`,
///    so the caller must ensure that these pointers were allocated in a compatible manner.
pub unsafe extern "C" fn NewMetricIteratorSortedByScore(
    ids: *mut t_docId,
    metric_list: *mut f64,
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
    ids: *mut t_docId,
    metrics: *mut f64,
    num: usize,
    _type: MetricType,
) -> *mut QueryIterator {
    let (ids_list, metrics_list) = if ids.is_null() {
        // Safety: Safe thanks to 3
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

        // Safety: Safe thanks to 1
        let ids_list = unsafe { OwnedSlice::from_c(ids, num) };
        // Safety: Safe thanks to 2
        let metrics_list = unsafe { OwnedSlice::from_c(metrics, num) };

        (ids_list, metrics_list)
    };

    RQEIteratorWrapper::boxed_new(
        if SORTED_BY_ID {
            IteratorType_METRIC_SORTED_BY_ID_ITERATOR
        } else {
            IteratorType_METRIC_SORTED_BY_SCORE_ITERATOR
        },
        Metric::<SORTED_BY_ID>::new(ids_list, metrics_list),
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

    // SAFETY: Safe thanks to 1.
    let iterator_type = unsafe { *header }.type_;

    match iterator_type {
        IteratorType_METRIC_SORTED_BY_ID_ITERATOR => {
            // SAFETY: Safe thanks to 1 + 2.
            let wrapper =
                unsafe { RQEIteratorWrapper::<MetricSortedById>::mut_ref_from_header_ptr(header) };
            // SAFETY: Safe thanks to 3.
            unsafe { wrapper.inner.set_handle(key_handle) };
        }
        IteratorType_METRIC_SORTED_BY_SCORE_ITERATOR => {
            // SAFETY: Safe thanks to 1 + 2.
            let wrapper = unsafe {
                RQEIteratorWrapper::<MetricSortedByScore>::mut_ref_from_header_ptr(header)
            };
            // SAFETY: Safe thanks to 3.
            unsafe { wrapper.inner.set_handle(key_handle) };
        }
        _ => unreachable!(
            "expected a metric iterator, either sorted by ID or Score (metric value): unexpected type: {iterator_type}"
        ),
    }
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

    // SAFETY: Safe thanks to 1.
    let iterator_type = unsafe { *header }.type_;

    match iterator_type {
        IteratorType_METRIC_SORTED_BY_ID_ITERATOR => {
            // SAFETY: Safe thanks to 1 + 2.
            let wrapper =
                unsafe { RQEIteratorWrapper::<MetricSortedById>::mut_ref_from_header_ptr(header) };
            wrapper.inner.key_mut_ref() as *mut _
        }
        IteratorType_METRIC_SORTED_BY_SCORE_ITERATOR => {
            // SAFETY: Safe thanks to 1 + 2.
            let wrapper = unsafe {
                RQEIteratorWrapper::<MetricSortedByScore>::mut_ref_from_header_ptr(header)
            };
            wrapper.inner.key_mut_ref() as *mut _
        }
        _ => unreachable!(
            "expected a metric iterator, either sorted by ID or Score (metric value): unexpected type: {iterator_type}"
        ),
    }
}

/// Get the metric type used by this metric iterator.
///
/// # Safety
///
/// 1. `header` is a valid non-null pointer to a [`QueryIterator`].
/// 2. `header` was built via [`NewMetricIteratorSortedByScore`] or [`NewMetricIteratorSortedById`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GetMetricType(header: *const QueryIterator) -> MetricType {
    debug_assert!(!header.is_null());

    // SAFETY: Safe thanks to 1.
    let iterator_type = unsafe { *header }.type_;

    match iterator_type {
        IteratorType_METRIC_SORTED_BY_ID_ITERATOR => {
            // SAFETY: Safe thanks to 1 + 2.
            let wrapper =
                unsafe { RQEIteratorWrapper::<MetricSortedById>::ref_from_header_ptr(header) };
            wrapper.inner.metric_type()
        }
        IteratorType_METRIC_SORTED_BY_SCORE_ITERATOR => {
            // SAFETY: Safe thanks to 1 + 2.
            let wrapper =
                unsafe { RQEIteratorWrapper::<MetricSortedByScore>::ref_from_header_ptr(header) };
            wrapper.inner.metric_type()
        }
        _ => unreachable!(
            "expected a metric iterator, either sorted by ID or Score (metric value): unexpected type: {iterator_type}"
        ),
    }
}
