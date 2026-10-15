/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! A [`GlobalAlloc`] adapter that serves alignments its inner allocator cannot.
//!
//! The Redis module allocator has no aligned-allocation primitive: it returns
//! whatever its backing `malloc` does, which guarantees only `max_align_t`.
//! Rust types can demand more — an AVX2 searcher needs 32 bytes — and handing
//! them under-aligned memory is undefined behavior. [`OverAligned`] closes that
//! gap without touching small-alignment allocations, which are forwarded as-is.

use std::alloc::{GlobalAlloc, Layout};
use std::ptr;

/// The default inner alignment of an [`OverAligned`].
///
/// This is `max_align_t` on every platform the module ships on, which is all
/// `malloc` promises. Layouts asking for more are served by over-allocating.
pub const MAX_INNER_ALIGN: usize = 16;

/// Size of the back-pointer stored just below every over-aligned block.
const HEADER_SIZE: usize = size_of::<*mut u8>();

/// Abort on a broken allocator invariant.
///
/// A global allocator must not unwind, and a panic would also re-enter the
/// allocator to format its message, so neither `panic!` nor `debug_assert!` is
/// safe here.
#[cold]
fn invariant_violated() -> ! {
    std::process::abort()
}

/// Wraps an allocator that honors alignments up to `INNER_ALIGN`, so that
/// every alignment is honored.
///
/// A layout whose alignment is at most `INNER_ALIGN` goes straight to the
/// inner allocator. A larger one is served from an inner block padded by the
/// alignment: the returned pointer is the first suitably aligned address with
/// room below it to store the inner block's address, which deallocation reads
/// back.
///
/// ```text
/// inner block, allocated with size `layout.size() + layout.align()`
/// and alignment `INNER_ALIGN`:
///
/// base                            block (aligned to `layout.align()`)
///  │                               │
///  ▼                               ▼
///  ┌─────────────────┬─────────────┬───────────────────────┬─────────┐
///  │     padding     │   header    │  caller's `size` bytes│  slack  │
///  └─────────────────┴─────────────┴───────────────────────┴─────────┘
///                     ╰─ holds `base`, ends exactly at `block`
///  ╰─ offset ∈ [HEADER_SIZE, layout.align()] ─╯
///     (padding is empty when offset == HEADER_SIZE)
/// ```
///
/// `dealloc` reads the header just below `block` to recover `base` and frees
/// the whole inner block.
///
/// The inner allocator must return memory aligned to
/// `min(layout.align(), INNER_ALIGN)`; nothing else is assumed of it.
///
/// `INNER_ALIGN` must be a power of two no smaller than the size of a pointer,
/// so the header fits in the padding; this is checked at compile time.
pub struct OverAligned<A, const INNER_ALIGN: usize = MAX_INNER_ALIGN>(pub A);

impl<A, const INNER_ALIGN: usize> OverAligned<A, INNER_ALIGN> {
    /// Compile-time check of the `INNER_ALIGN` contract, forced by every
    /// over-aligned code path.
    const VALID: () = {
        assert!(
            INNER_ALIGN.is_power_of_two(),
            "INNER_ALIGN must be a power of two"
        );
        assert!(
            HEADER_SIZE <= INNER_ALIGN,
            "the header must fit in the padding before an over-aligned block"
        );
    };

    /// The layout requested from the inner allocator to serve the over-aligned
    /// `layout`, or `None` if its size overflows.
    fn padded_layout(layout: Layout) -> Option<Layout> {
        let () = Self::VALID;
        let size = layout.size().checked_add(layout.align())?;
        Layout::from_size_align(size, INNER_ALIGN).ok()
    }

    /// Carve an aligned block out of `base`, a block allocated with
    /// [`Self::padded_layout`] of `layout`, and record `base` below it.
    ///
    /// # Safety
    ///
    /// `base` must be non-null, aligned to [`INNER_ALIGN`], and valid for
    /// writes of `Self::padded_layout(layout).size()` bytes; `layout.align()`
    /// must exceed [`INNER_ALIGN`].
    unsafe fn place(base: *mut u8, layout: Layout) -> *mut u8 {
        // `base` is a multiple of INNER_ALIGN and so is the rounded address,
        // so the offset is a multiple of INNER_ALIGN in
        // [HEADER_SIZE, layout.align()]: the header fits below the block, and the
        // block ends within the `size + align` bytes of `base`.
        if cfg!(debug_assertions) && !base.addr().is_multiple_of(INNER_ALIGN) {
            invariant_violated();
        }
        let aligned = (base.addr() + HEADER_SIZE).next_multiple_of(layout.align());
        let offset = aligned - base.addr();
        if cfg!(debug_assertions) && !(HEADER_SIZE..=layout.align()).contains(&offset) {
            invariant_violated();
        }
        // SAFETY: `offset <= layout.align()`, so the pointer stays inside the
        // inner block, per the bound above.
        let block = unsafe { base.add(offset) };
        // SAFETY: `block` is the over-aligned block being placed.
        let header = unsafe { Self::header(block) };
        // SAFETY: the header slot lies in `[base, block)` because
        // `offset >= HEADER_SIZE`, and it is pointer-aligned because `block` is
        // aligned to `layout.align() > INNER_ALIGN >= HEADER_SIZE`.
        unsafe { header.write(base) };
        block
    }

    /// The slot holding the inner block's address for the over-aligned `block`.
    ///
    /// # Safety
    ///
    /// `block` must have been returned by [`Self::place`].
    const unsafe fn header(block: *mut u8) -> *mut *mut u8 {
        // SAFETY: `place` reserved at least HEADER_SIZE bytes below `block`
        // within the same inner allocation.
        unsafe { block.cast::<*mut u8>().sub(1) }
    }
}

// SAFETY: every layout gets memory of at least its size and alignment, from the
// inner allocator directly or from a padded inner block via `place`, and every
// pointer is returned to the inner allocator with the layout it was allocated
// with, so the inner allocator's guarantees carry over.
unsafe impl<A: GlobalAlloc, const INNER_ALIGN: usize> GlobalAlloc for OverAligned<A, INNER_ALIGN> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if layout.align() <= INNER_ALIGN {
            // SAFETY: the caller's contract for `layout` is forwarded unchanged,
            // and the inner allocator honors this alignment by the type contract.
            return unsafe { self.0.alloc(layout) };
        }
        let Some(padded) = Self::padded_layout(layout) else {
            return ptr::null_mut();
        };
        // SAFETY: `padded` has a non-zero size, since it is at least `layout.align()`.
        let base = unsafe { self.0.alloc(padded) };
        if base.is_null() {
            return base;
        }
        // SAFETY: `base` came from the inner allocator for `padded`, so it is
        // aligned to INNER_ALIGN and valid for `padded.size()` bytes.
        unsafe { Self::place(base, layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if layout.align() <= INNER_ALIGN {
            // SAFETY: as in `alloc`.
            return unsafe { self.0.alloc_zeroed(layout) };
        }
        // SAFETY: the caller's contract for `layout` is forwarded unchanged.
        let block = unsafe { self.alloc(layout) };
        if !block.is_null() {
            // SAFETY: `alloc` returned a block valid for `layout.size()` bytes.
            unsafe { block.write_bytes(0, layout.size()) };
        }
        block
    }

    unsafe fn dealloc(&self, block: *mut u8, layout: Layout) {
        if layout.align() <= INNER_ALIGN {
            // SAFETY: `block` was allocated by the inner allocator with `layout`,
            // since `alloc` forwards exactly these layouts.
            return unsafe { self.0.dealloc(block, layout) };
        }
        // `alloc` only hands out an over-aligned block after computing this
        // padded layout for the same `layout`, so it cannot fail here.
        let Some(padded) = Self::padded_layout(layout) else {
            invariant_violated();
        };
        // SAFETY: `block` was returned by `alloc` for this over-aligned `layout`,
        // so it was produced by `place`.
        let header = unsafe { Self::header(block) };
        // SAFETY: `place` stored the inner block's address in this slot.
        let base = unsafe { header.read() };
        // SAFETY: `base` was allocated by the inner allocator with `padded`.
        unsafe { self.0.dealloc(base, padded) }
    }

    unsafe fn realloc(&self, block: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if layout.align() <= INNER_ALIGN {
            // SAFETY: the caller's contract is forwarded unchanged; the new
            // layout keeps the same, inner-supported alignment.
            return unsafe { self.0.realloc(block, layout, new_size) };
        }
        // The inner allocator cannot move an over-aligned block while keeping
        // its offset, so copy into a fresh one.
        let Ok(new_layout) = Layout::from_size_align(new_size, layout.align()) else {
            return ptr::null_mut();
        };
        // SAFETY: `new_size` is non-zero by the caller's contract.
        let new_block = unsafe { self.alloc(new_layout) };
        if !new_block.is_null() {
            // SAFETY: both blocks are valid for the smaller of the two sizes, and
            // they are distinct live allocations, so they do not overlap.
            unsafe { ptr::copy_nonoverlapping(block, new_block, layout.size().min(new_size)) };
            // SAFETY: `block` is live with `layout`, per the caller's contract.
            unsafe { self.dealloc(block, layout) };
        }
        new_block
    }
}
