/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! FFI layer for the [`Accumulator`] reducers: `COUNT`, `SUM`, `AVG`, `MIN`,
//! `MAX`, `STDDEV`, `FIRST_VALUE` and the exact `COUNT_DISTINCT`.
//!
//! C parses the reducer arguments and calls one of the constructors below; the
//! rest of the vtable is one set of callbacks, generic over the accumulator.

use std::ffi::{c_int, c_void};
use std::ptr::{self, NonNull};

use reducers::accumulator::{Accumulator, AccumulatorReducer};
use reducers::count::Count;
use reducers::count_distinct::CountDistinct;
use reducers::first_value::{FirstValue, SortBy};
use reducers::min_max::{Extreme, MinMax};
use reducers::std_dev::StdDev;
use reducers::sum::Sum;
use rlookup::{RLookupKey, RLookupRow};

/// Creates a `COUNT` reducer and returns its base [`ffi::Reducer`], which the
/// caller frees through its `Free` callback.
#[unsafe(no_mangle)]
pub extern "C" fn CountReducer_Create() -> *mut ffi::Reducer {
    into_c_reducer(Count)
}

/// Creates a `SUM` reducer of `srckey`, or an `AVG` one if `average`, and returns
/// its base [`ffi::Reducer`], which the caller frees through its `Free` callback.
///
/// # Safety
///
/// 1. `srckey` must be a [valid] pointer to an [`RLookupKey`][ffi::RLookupKey] that
///    remains valid, and is not mutated, for the lifetime of the returned reducer.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SumReducer_Create(
    srckey: *const ffi::RLookupKey,
    average: bool,
) -> *mut ffi::Reducer {
    // SAFETY: ensured by caller (1.)
    let key = unsafe { srckey.cast::<RLookupKey>().as_ref() }.expect("srckey must not be null");
    into_c_reducer(Sum::new(key, average))
}

/// Creates a `MIN` reducer of `srckey`, or a `MAX` one if `max`, and returns its
/// base [`ffi::Reducer`], which the caller frees through its `Free` callback.
///
/// # Safety
///
/// 1. `srckey` must be a [valid] pointer to an [`RLookupKey`][ffi::RLookupKey] that
///    remains valid, and is not mutated, for the lifetime of the returned reducer.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn MinMaxReducer_Create(
    srckey: *const ffi::RLookupKey,
    max: bool,
) -> *mut ffi::Reducer {
    // SAFETY: ensured by caller (1.)
    let key = unsafe { srckey.cast::<RLookupKey>().as_ref() }.expect("srckey must not be null");
    let extreme = if max { Extreme::Max } else { Extreme::Min };
    into_c_reducer(MinMax::new(key, extreme))
}

/// Creates a `STDDEV` reducer of `srckey` and returns its base [`ffi::Reducer`],
/// which the caller frees through its `Free` callback.
///
/// # Safety
///
/// 1. `srckey` must be a [valid] pointer to an [`RLookupKey`][ffi::RLookupKey] that
///    remains valid, and is not mutated, for the lifetime of the returned reducer.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn StdDevReducer_Create(srckey: *const ffi::RLookupKey) -> *mut ffi::Reducer {
    // SAFETY: ensured by caller (1.)
    let key = unsafe { srckey.cast::<RLookupKey>().as_ref() }.expect("srckey must not be null");
    into_c_reducer(StdDev::new(key))
}

/// Creates a `FIRST_VALUE` reducer of `retkey`, sorted by `sortkey` in ascending
/// order if `ascending`, or unsorted if `sortkey` is null, and returns its base
/// [`ffi::Reducer`], which the caller frees through its `Free` callback.
///
/// # Safety
///
/// 1. `retkey` must be a [valid] pointer to an [`RLookupKey`][ffi::RLookupKey], and
///    `sortkey` null or one, that remain valid, and are not mutated, for the
///    lifetime of the returned reducer.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn FirstValueReducer_Create(
    retkey: *const ffi::RLookupKey,
    sortkey: *const ffi::RLookupKey,
    ascending: bool,
) -> *mut ffi::Reducer {
    // SAFETY: ensured by caller (1.)
    let key = unsafe { retkey.cast::<RLookupKey>().as_ref() }.expect("retkey must not be null");
    // SAFETY: ensured by caller (1.)
    let sort_key = unsafe { sortkey.cast::<RLookupKey>().as_ref() };
    let sort_by = sort_key.map(|key| SortBy { key, ascending });
    into_c_reducer(FirstValue::new(key, sort_by))
}

/// Creates an exact `COUNT_DISTINCT` reducer of `srckey` and returns its base
/// [`ffi::Reducer`], which the caller frees through its `Free` callback.
///
/// # Safety
///
/// 1. `srckey` must be a [valid] pointer to an [`RLookupKey`][ffi::RLookupKey] that
///    remains valid, and is not mutated, for the lifetime of the returned reducer.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn CountDistinctReducer_Create(
    srckey: *const ffi::RLookupKey,
) -> *mut ffi::Reducer {
    // SAFETY: ensured by caller (1.)
    let key = unsafe { srckey.cast::<RLookupKey>().as_ref() }.expect("srckey must not be null");
    into_c_reducer(CountDistinct::new(key))
}

/// Boxes an [`AccumulatorReducer`] running `accumulator` and wires its vtable.
fn into_c_reducer<A: Accumulator>(accumulator: A) -> *mut ffi::Reducer {
    let mut reducer = Box::new(AccumulatorReducer::new(accumulator));
    let vtable = reducer
        .reducer_mut()
        .set_new_instance(new_instance::<A>)
        .set_add(add::<A>)
        .set_finalize(finalize::<A>)
        .set_free(free::<A>);
    if std::mem::needs_drop::<A::State>() {
        vtable.set_free_instance(free_instance::<A>);
    }
    Box::into_raw(reducer).cast()
}

/// # Safety
///
/// 1. `r` must point to a [valid] [`AccumulatorReducer<A>`] created by
///    [`into_c_reducer`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
unsafe extern "C" fn new_instance<A: Accumulator>(r: *mut ffi::Reducer) -> *mut c_void {
    // SAFETY: ensured by caller (1.)
    let r = unsafe { r.cast::<AccumulatorReducer<A>>().as_ref() }.unwrap();
    ptr::from_mut(r.new_state()).cast()
}

/// # Safety
///
/// 1. `r` must point to a [valid] [`AccumulatorReducer<A>`] created by
///    [`into_c_reducer`].
/// 2. `state` must be a group state returned by [`new_instance`] for `r`, not
///    accessed through any other pointer during the call.
/// 3. `row` must point to a [valid] [`ffi::RLookupRow`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
unsafe extern "C" fn add<A: Accumulator>(
    r: *mut ffi::Reducer,
    state: *mut c_void,
    row: *const ffi::RLookupRow,
) -> c_int {
    // SAFETY: ensured by caller (1.)
    let r = unsafe { r.cast::<AccumulatorReducer<A>>().as_ref() }.unwrap();
    // SAFETY: ensured by caller (2.)
    let state = unsafe { state.cast::<A::State>().as_mut() }.unwrap();
    // SAFETY: ensured by caller (3.)
    let row = unsafe { row.cast::<RLookupRow>().as_ref() }.unwrap();

    r.accumulator().add(state, row);
    1 // C reducer->Add convention: always returns 1
}

/// # Safety
///
/// 1. `r` must point to a [valid] [`AccumulatorReducer<A>`] created by
///    [`into_c_reducer`].
/// 2. `state` must be a group state returned by [`new_instance`] for `r`, not
///    mutated through any other pointer during the call.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
unsafe extern "C" fn finalize<A: Accumulator>(
    r: *mut ffi::Reducer,
    state: *mut c_void,
) -> *mut ffi::RSValue {
    // SAFETY: ensured by caller (1.)
    let r = unsafe { r.cast::<AccumulatorReducer<A>>().as_ref() }.unwrap();
    // SAFETY: ensured by caller (2.)
    let state = unsafe { state.cast::<A::State>().as_ref() }.unwrap();

    r.accumulator().finalize(state).into_raw() as *mut ffi::RSValue
}

/// # Safety
///
/// 1. `r` must point to a [valid] [`AccumulatorReducer<A>`] created by
///    [`into_c_reducer`].
/// 2. `state` must be a group state returned by [`new_instance`] for `r`, not
///    freed yet, and not used afterwards.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
unsafe extern "C" fn free_instance<A: Accumulator>(r: *mut ffi::Reducer, state: *mut c_void) {
    // SAFETY: ensured by caller (1.)
    let r = unsafe { r.cast::<AccumulatorReducer<A>>().as_ref() }.unwrap();
    let state = NonNull::new(state.cast::<A::State>()).unwrap();
    // SAFETY: ensured by caller (2.)
    unsafe { r.drop_state(state.as_ptr()) };
}

/// # Safety
///
/// 1. `r` must point to a [valid] [`AccumulatorReducer<A>`] created by
///    [`into_c_reducer`] and not freed yet. Neither `r` nor any group state it
///    returned may be used afterwards.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
unsafe extern "C" fn free<A: Accumulator>(r: *mut ffi::Reducer) {
    // SAFETY: ensured by caller (1.)
    drop(unsafe { Box::from_raw(r.cast::<AccumulatorReducer<A>>()) });
}
