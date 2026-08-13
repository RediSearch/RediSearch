/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! A [`GlobalAlloc`] that routes Rust allocations to the Redis module allocator and
//! satisfies alignments that allocator does not provide on its own.
//!
//! The `RedisModule_*` API exposes no aligned-allocation primitive, so
//! [`redis_module::alloc::RedisAlloc`] can do no better than the alignment the
//! underlying allocator happens to hand out — [`MIN_ALIGN`]. A [`Layout`] asking for
//! more than that is silently under-served, which is undefined behaviour the moment
//! the memory is used as the type it was allocated for. [`AlignedRedisAlloc`] wraps
//! it, over-allocating and adjusting the pointer whenever a layout asks for more.

use std::alloc::{GlobalAlloc, Layout};
use std::ptr;

use redis_module::alloc::RedisAlloc;

/// The alignment the Redis module allocator satisfies without help.
///
/// `RedisModule_Alloc` is Redis' `zmalloc`, which on both platforms RediSearch
/// supports forwards to an allocator that returns `max_align_t`-aligned addresses:
/// jemalloc on Linux, libc `malloc` on macOS.
///
/// Understating this costs an unnecessary fixup; overstating it hands out
/// under-aligned memory, so it is deliberately the weakest guarantee both
/// allocators make rather than the strongest either happens to provide.
const MIN_ALIGN: usize = 2 * size_of::<usize>();

/// Size of the header stored directly below an over-aligned block, holding the
/// pointer that has to be given back to the underlying allocator.
const HEADER: usize = size_of::<*mut u8>();

/// A [`GlobalAlloc`] backed by the Redis module allocator that honours layouts
/// requiring more than [`MIN_ALIGN`] bytes of alignment.
///
/// Layouts within [`MIN_ALIGN`] are forwarded to [`RedisAlloc`] untouched. Anything
/// stricter is carved out of a larger block, with the underlying allocation's own
/// pointer stashed in the [`HEADER`] word below the address returned to the caller.
#[derive(Default, Debug, Copy, Clone)]
pub struct AlignedRedisAlloc;

// SAFETY: allocated blocks are live, of at least the requested size, and aligned as
// requested; `dealloc` returns to `RedisAlloc` exactly the pointer it handed out.
unsafe impl GlobalAlloc for AlignedRedisAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: `RedisAlloc::alloc` has no precondition beyond a non-zero-sized
        // layout, which `alloc_with` only ever hands it if it was given one itself.
        let alloc = |layout| unsafe { RedisAlloc.alloc(layout) };
        // SAFETY: `RedisAlloc` is a sound `GlobalAlloc`, so it upholds the contract
        // `alloc_with` requires of its closure.
        unsafe { alloc_with(layout, alloc) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `dealloc_with` only invokes this with a pointer and layout that
        // came from the matching `alloc` above, which allocated through `RedisAlloc`.
        let dealloc = |ptr, layout| unsafe { RedisAlloc.dealloc(ptr, layout) };
        // SAFETY: the caller guarantees `ptr` and `layout` come from a matching
        // `Self::alloc`.
        unsafe { dealloc_with(ptr, layout, dealloc) }
    }
}

/// Allocate `layout` through `alloc`, adjusting the result when `layout` needs more
/// alignment than [`MIN_ALIGN`].
///
/// Returns a null pointer if `alloc` does, or if the padded request cannot be
/// expressed as a [`Layout`].
///
/// Taking the allocator as a closure keeps the pointer arithmetic testable without
/// a loaded Redis server behind `RedisModule_Alloc`.
///
/// # Safety
///
/// `alloc` must behave like [`GlobalAlloc::alloc`], and additionally return blocks
/// aligned to at least [`MIN_ALIGN`] regardless of the alignment its layout asks
/// for — which is what makes over-allocating here sufficient.
unsafe fn alloc_with(layout: Layout, alloc: impl FnOnce(Layout) -> *mut u8) -> *mut u8 {
    if layout.align() <= MIN_ALIGN {
        return alloc(layout);
    }

    // Worst case the block starts one byte past an aligned address, so reaching the
    // next one from just above the header costs `align - 1` further bytes.
    let Some(size) = layout.size().checked_add(layout.align() + HEADER) else {
        return ptr::null_mut();
    };
    // Alignment 1 keeps the underlying allocator out of the alignment business; it
    // still owes `MIN_ALIGN`, and everything beyond that is provided here.
    let Ok(padded) = Layout::from_size_align(size, 1) else {
        return ptr::null_mut();
    };

    let base = alloc(padded);
    if base.is_null() {
        return base;
    }

    let offset = aligned_offset(base.addr(), layout.align());
    // SAFETY: `offset <= HEADER + align - 1`, so `offset + layout.size() < size` and
    // both the returned pointer and the block behind it stay within the allocation.
    let ptr = unsafe { base.add(offset) };
    // SAFETY: `offset >= HEADER` puts the header word inside the allocation.
    let header = unsafe { ptr.cast::<*mut u8>().sub(1) };
    // SAFETY: `layout.align() > MIN_ALIGN > HEADER` makes `ptr` — hence `header` — a
    // multiple of `HEADER`, so the write is aligned, and the word it covers is live.
    unsafe { header.write(base) };
    ptr
}

/// Free a pointer produced by [`alloc_with`] through the matching `dealloc`.
///
/// # Safety
///
/// `ptr` must have come from [`alloc_with`] with this same `layout`, and `dealloc`
/// must free blocks allocated by the closure that call was given.
unsafe fn dealloc_with(ptr: *mut u8, layout: Layout, dealloc: impl FnOnce(*mut u8, Layout)) {
    if layout.align() <= MIN_ALIGN {
        return dealloc(ptr, layout);
    }

    // SAFETY: `alloc_with` took the same branch for this layout, so the word below
    // `ptr` is inside the allocation and holds the underlying allocator's pointer.
    let header = unsafe { ptr.cast::<*mut u8>().sub(1) };
    // SAFETY: `alloc_with` wrote that pointer through an equally aligned reference.
    let base = unsafe { header.read() };
    let size = layout.size() + layout.align() + HEADER;
    // SAFETY: `alloc_with` built this exact layout and it was accepted, otherwise
    // there would be no allocation to free.
    let padded = unsafe { Layout::from_size_align_unchecked(size, 1) };
    dealloc(base, padded);
}

/// Distance from `base` to the lowest address at or above `base + HEADER` that is a
/// multiple of `align`, which must be a power of two.
fn aligned_offset(base: usize, align: usize) -> usize {
    debug_assert!(align.is_power_of_two());
    ((base + HEADER).next_multiple_of(align)) - base
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::alloc::System;
    use std::cell::Cell;

    /// Alignments above [`MIN_ALIGN`] worth covering: the 32 that
    /// `memchr`'s AVX2 searcher needs and provoked this allocator, plus a
    /// cache-line and a page.
    const OVER_ALIGNED: [usize; 3] = [32, 64, 4096];

    #[test]
    fn layouts_within_min_align_are_forwarded_untouched() {
        for align in [1, 2, 4, 8, MIN_ALIGN] {
            let layout = Layout::from_size_align(24, align).unwrap();
            let seen = Cell::new(None);

            // SAFETY: `System` is a sound `GlobalAlloc` returning `MIN_ALIGN`-aligned blocks.
            let ptr = unsafe {
                alloc_with(layout, |l| {
                    seen.set(Some(l));
                    System.alloc(l)
                })
            };

            assert_eq!(seen.get(), Some(layout), "align {align} was padded");
            assert!(!ptr.is_null());

            // SAFETY: `ptr` comes from the `alloc_with` call directly above.
            unsafe { dealloc_with(ptr, layout, |p, l| System.dealloc(p, l)) };
        }
    }

    #[test]
    fn over_aligned_blocks_are_aligned_and_usable() {
        for align in OVER_ALIGNED {
            for size in [align, 3 * align, 7 * align] {
                let layout = Layout::from_size_align(size, align).unwrap();

                // SAFETY: as above.
                let ptr = unsafe { alloc_with(layout, |l| System.alloc(l)) };
                assert!(!ptr.is_null());
                assert_eq!(ptr.addr() % align, 0, "size {size} align {align}");

                // Touching every byte proves the block really extends that far;
                // Miri and the sanitizers fail the test if it does not.
                // SAFETY: `alloc_with` guarantees `size` writable bytes at `ptr`.
                unsafe { ptr.write_bytes(0xAB, size) };

                // SAFETY: `ptr` comes from the `alloc_with` call above.
                unsafe { dealloc_with(ptr, layout, |p, l| System.dealloc(p, l)) };
            }
        }
    }

    #[test]
    fn over_aligned_blocks_are_freed_at_the_allocators_own_pointer() {
        let layout = Layout::from_size_align(100, 64).unwrap();
        let allocated = Cell::new(ptr::null_mut());

        // SAFETY: as above.
        let ptr = unsafe {
            alloc_with(layout, |l| {
                let p = System.alloc(l);
                allocated.set(p);
                p
            })
        };
        assert_ne!(
            ptr,
            allocated.get(),
            "test needs a layout that gets adjusted"
        );

        let freed = Cell::new(ptr::null_mut());
        // SAFETY: `ptr` comes from the `alloc_with` call above.
        unsafe {
            dealloc_with(ptr, layout, |p, l| {
                freed.set(p);
                System.dealloc(p, l)
            })
        };

        assert_eq!(freed.get(), allocated.get());
    }

    #[test]
    fn a_size_that_cannot_be_padded_fails_instead_of_wrapping() {
        // The largest size `Layout` accepts for this alignment, so padding it
        // pushes the request past what `Layout` can express.
        let layout = Layout::from_size_align(isize::MAX as usize - 4095, 4096).unwrap();

        // SAFETY: the closure is never reached; a null return is a valid outcome.
        let ptr = unsafe {
            alloc_with(layout, |_| {
                unreachable!("padding must fail before allocating")
            })
        };

        assert!(ptr.is_null());
    }
}
