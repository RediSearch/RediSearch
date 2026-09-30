/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The `RedisModuleSlotRangeArray` helpers, driven through their C ABI.
//!
//! This binary does not link the Redis allocator, so arrays returned by the
//! helpers are released with the Rust global allocator they come from.

use std::alloc::{Layout, dealloc};
use std::ptr;

use slots_tracker::{SlotRange, SlotRangeArray};
use slots_tracker_ffi::{
    SlotRangeArray_Clone, SlotRangeArray_ContainsSlot, SlotRangeArray_SizeOf,
    SlotRangesArray_Deserialize, SlotRangesArray_Serialize,
};

/// A `RedisModuleSlotRangeArray` with its flexible array inline, as C lays it out.
#[repr(C)]
struct Fixed<const N: usize> {
    num_ranges: i32,
    ranges: [SlotRange; N],
}

impl<const N: usize> Fixed<N> {
    fn new(bounds: [(u16, u16); N]) -> Self {
        Self {
            num_ranges: N as i32,
            ranges: bounds.map(|(start, end)| SlotRange { start, end }),
        }
    }

    const fn as_ptr(&self) -> *const SlotRangeArray {
        ptr::from_ref(self).cast()
    }
}

/// Copies the ranges out of `array` and frees it.
///
/// # Safety
///
/// `array` must have been returned by [`SlotRangeArray_Clone`] or
/// [`SlotRangesArray_Deserialize`] and not freed yet.
unsafe fn take(array: *mut SlotRangeArray) -> Vec<SlotRange> {
    assert!(!array.is_null());
    // SAFETY: `array` is a live array (see above).
    let len = unsafe { (*array).num_ranges } as usize;
    // SAFETY: the flexible array holds `len` ranges; `&raw const` only computes its
    // address.
    let first = unsafe { &raw const (*array).ranges }.cast::<SlotRange>();
    // SAFETY: as above.
    let ranges = unsafe { std::slice::from_raw_parts(first, len) }.to_vec();
    let (layout, _) = Layout::new::<SlotRangeArray>()
        .extend(Layout::array::<SlotRange>(len).unwrap())
        .unwrap();
    // SAFETY: the array was allocated by the global allocator with this layout.
    unsafe { dealloc(array.cast(), layout.pad_to_align()) };
    ranges
}

#[test]
fn size_counts_the_header_and_every_range() {
    assert_eq!(SlotRangeArray_SizeOf(0), 4);
    assert_eq!(SlotRangeArray_SizeOf(100), 404);
}

#[test]
fn contains_slot_checks_every_range() {
    let array = Fixed::new([(0, 100), (500, 600), (1000, 1500)]);
    for (slot, expected) in [
        (0, true),
        (100, true),
        (101, false),
        (499, false),
        (550, true),
        (999, false),
        (1500, true),
        (1501, false),
    ] {
        // SAFETY: `array` is a valid array of 3 ranges.
        let found = unsafe { SlotRangeArray_ContainsSlot(array.as_ptr(), slot) };
        assert_eq!(found, expected, "slot {slot}");
    }

    let empty = Fixed::new([]);
    // SAFETY: `empty` is a valid array of no ranges.
    assert!(!unsafe { SlotRangeArray_ContainsSlot(empty.as_ptr(), 0) });
}

#[test]
fn clone_copies_every_range() {
    let array = Fixed::new([(1, 2), (3, 4)]);
    // SAFETY: `array` is a valid array; the clone is freed by `take`.
    let clone = unsafe { take(SlotRangeArray_Clone(array.as_ptr())) };
    assert_eq!(clone, array.ranges);

    let empty = Fixed::new([]);
    // SAFETY: as above.
    assert!(unsafe { take(SlotRangeArray_Clone(empty.as_ptr())) }.is_empty());
}

#[test]
fn serialized_arrays_round_trip() {
    let array = Fixed::new([(0, 16383), (7, 7)]);
    // SAFETY: `array` is a valid array.
    let buf = unsafe { SlotRangesArray_Serialize(array.as_ptr()) };
    let len = SlotRangeArray_SizeOf(2);

    // SAFETY: `buf` holds `len` initialized bytes; the result is freed by `take`.
    let decoded = unsafe { take(SlotRangesArray_Deserialize(buf, len)) };
    assert_eq!(decoded, array.ranges);

    // SAFETY: as above; one byte short of the declared count.
    assert!(unsafe { SlotRangesArray_Deserialize(buf, len - 1) }.is_null());
    // SAFETY: `buf` was allocated by the global allocator as `len` bytes.
    unsafe { dealloc(buf.cast(), Layout::array::<u8>(len).unwrap()) };
}

#[test]
fn deserialize_rejects_a_null_buffer() {
    // SAFETY: a null buffer is allowed.
    assert!(unsafe { SlotRangesArray_Deserialize(ptr::null(), 4) }.is_null());
}
