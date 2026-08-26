/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::QueryIterator;
use rqe_core::DocId;
use rqe_iterators::interop::RQEIteratorWrapper;
use rqe_iterators::{
    metric::{Metric, MetricType},
    utils::OwnedSlice,
};

#[unsafe(no_mangle)]
/// Creates a new metric iterator sorted by ID.
///
/// # Safety
///
/// 1. `ids` must be a [valid] pointer to an array of `DocId` with at least `num` elements.
///    The array must be sorted in ascending order.
/// 2. `metric_list` must be a [valid] pointer to an array of `f64` with at least `num` elements.
/// 3. The caller must ensure that `ids` and `metric_list` are not null unless `num` is zero.
/// 4. The memory pointed to by `ids` and `metric_list` will be freed using `RedisModule_Free`,
///    so the caller must ensure that these pointers were allocated in a compatible manner.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
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
/// 1. `ids` must be a [valid] pointer to an array of `DocId` with at least `num` elements.
/// 2. `metric_list` must be a [valid] pointer to an array of `f64` with at least `num` elements.
/// 3. The caller must ensure that `ids` and `metric_list` are not null unless `num` is zero.
/// 4. The memory pointed to by `ids` and `metric_list` will be freed using `RedisModule_Free`,
///    so the caller must ensure that these pointers were allocated in a compatible manner.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
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
/// 1. `ids` must be a [valid] pointer to an array of `DocId` with at least `num` elements.
/// 2. `metric_list` must be a [valid] pointer to an array of `f64` with at least `num` elements.
/// 3. The caller must ensure that `ids` and `metric_list` are not null unless `num` is zero.
/// 4. The memory pointed to by `ids` and `metric_list` will be freed using `RedisModule_Free`,
///    so the caller must ensure that these pointers were allocated in a compatible manner.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
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

        // SAFETY: Safe thanks to 1.
        let ids_list = unsafe { OwnedSlice::from_c(ids, num) };
        // SAFETY: Safe thanks to 2.
        let metrics_list = unsafe { OwnedSlice::from_c(metrics, num) };

        (ids_list, metrics_list)
    };

    RQEIteratorWrapper::boxed_new(Metric::<SORTED_BY_ID>::new(ids_list, metrics_list))
}
