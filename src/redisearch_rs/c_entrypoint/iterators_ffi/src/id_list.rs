/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::{IteratorType_ID_LIST_ITERATOR, QueryIterator, RedisModule_Free, t_docId};
use inverted_index::RSIndexResult;
use rqe_iterators::id_list::IdList;

use crate::wrapper::RQEIteratorWrapper;

#[unsafe(no_mangle)]
/// Creates a new iterator over a list of sorted document IDs.
///
/// # Safety
///
/// 1. `ids` must be a valid pointer to an array of `t_docId` with at least `num` elements.
///    The array must be sorted in ascending order.
/// 2. The caller must ensure that `ids` is not null unless `num` is zero.
/// 3. The memory pointed to by `ids` will be freed using `RedisModule_Free`,
///    so the caller must ensure that the pointer was allocated in a compatible manner.
pub unsafe extern "C" fn NewSortedIdListIterator(
    ids: *const t_docId,
    num: u64,
    weight: f64,
) -> *mut QueryIterator {
    // SAFETY: All safety preconditions are guaranteed by the caller.
    unsafe { new_id_list_iterator::<true>(ids, num, weight) }
}

#[unsafe(no_mangle)]
/// Creates a new iterator over a list of unsorted document IDs.
///
/// # Safety
///
/// 1. `ids` must be a valid pointer to an array of `t_docId` with at least `num` elements.
/// 2. The caller must ensure that `ids` is not null unless `num` is zero.
/// 3. The memory pointed to by `ids` will be freed using `RedisModule_Free`,
///    so the caller must ensure that the pointer was allocated in a compatible manner.
pub unsafe extern "C" fn NewUnsortedIdListIterator(
    ids: *const t_docId,
    num: u64,
    weight: f64,
) -> *mut QueryIterator {
    // SAFETY: All safety preconditions are guaranteed by the caller.
    unsafe { new_id_list_iterator::<false>(ids, num, weight) }
}

/// # Safety
///
/// 1. `ids` must be a valid pointer to an array of `t_docId` with at least `num` elements.
/// 2. The caller must ensure that `ids` is not null unless `num` is zero.
/// 3. The memory pointed to by `ids` will be freed using `RedisModule_Free`,
///    so the caller must ensure that the pointer was allocated in a compatible manner.
unsafe fn new_id_list_iterator<const SORTED: bool>(
    ids: *const t_docId,
    num: u64,
    weight: f64,
) -> *mut QueryIterator {
    // TODO: Figure out a mechanism to avoid re-allocating ids on the Rust side.
    let mut vec = Vec::with_capacity(num as usize);
    // We assume that `num` is the number of elements in `ids`.
    if !ids.is_null() {
        // SAFETY: Safe thanks to 1. + 2.
        let slice = unsafe { std::slice::from_raw_parts(ids, num as usize) };
        vec.extend_from_slice(slice);
        // SAFETY: The free function has been initialized at this stage.
        let free_fn = unsafe { RedisModule_Free.unwrap() };
        // SAFETY: Safe thanks to 3.
        unsafe { free_fn(ids as *mut std::os::raw::c_void) };
    } else {
        debug_assert_eq!(
            num, 0,
            "The pointer to the array of IDs is null, but the number of IDs is non-zero."
        );
    }
    RQEIteratorWrapper::boxed_new(
        IteratorType_ID_LIST_ITERATOR,
        IdList::<SORTED>::with_result(vec, RSIndexResult::virt().weight(weight)),
    )
}
