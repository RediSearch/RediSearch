/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use over_aligned_alloc::{MAX_INNER_ALIGN, OverAligned};

/// Offset at which [`Misaligning`] places blocks inside a [`MISALIGN_BASE`]-aligned
/// system block.
const MISALIGN_OFFSET: usize = MAX_INNER_ALIGN;
/// Alignment of the system block behind every [`Misaligning`] allocation.
const MISALIGN_BASE: usize = 2 * MAX_INNER_ALIGN;

/// A call that reached [`Misaligning`], with its arguments and result, as addresses.
#[derive(Debug, PartialEq)]
enum Call {
    Alloc {
        layout: Layout,
        result: usize,
    },
    AllocZeroed {
        layout: Layout,
        result: usize,
    },
    Realloc {
        block: usize,
        layout: Layout,
        new_size: usize,
        result: usize,
    },
    Dealloc {
        block: usize,
        layout: Layout,
    },
}

/// An inner allocator that honors [`MAX_INNER_ALIGN`] and nothing more: every
/// address it returns is a multiple of [`MAX_INNER_ALIGN`] but of no larger power of
/// two, like a `malloc` that happens to be maximally unlucky.
///
/// It records every live allocation with its layout, so a test can check that
/// each block comes back with the layout it was allocated with, and logs every
/// call, so a test can check exactly what the adapter forwarded.
#[derive(Default)]
struct Misaligning {
    live: Mutex<HashMap<usize, Layout>>,
    calls: Mutex<Vec<Call>>,
    /// Fail every allocation, to model the inner allocator running out of memory.
    exhausted: AtomicBool,
}

impl Misaligning {
    fn exhausted() -> Self {
        let inner = Self::default();
        inner.set_exhausted(true);
        inner
    }

    fn set_exhausted(&self, exhausted: bool) {
        self.exhausted.store(exhausted, Ordering::Relaxed);
    }

    fn system_layout(layout: Layout) -> Layout {
        Layout::from_size_align(layout.size() + MISALIGN_BASE, MISALIGN_BASE).unwrap()
    }

    fn live_count(&self) -> usize {
        self.live.lock().unwrap().len()
    }

    /// The calls logged since the last time this was called.
    fn take_calls(&self) -> Vec<Call> {
        std::mem::take(&mut self.calls.lock().unwrap())
    }

    fn log(&self, call: Call) {
        self.calls.lock().unwrap().push(call);
    }

    /// Allocate a misaligned block for `layout`, or null when exhausted.
    fn allocate(&self, layout: Layout) -> *mut u8 {
        assert!(
            layout.align() <= MAX_INNER_ALIGN,
            "OverAligned asked the inner allocator for {layout:?}"
        );
        if self.exhausted.load(Ordering::Relaxed) {
            return std::ptr::null_mut();
        }
        // SAFETY: the system layout has a non-zero size.
        let base = unsafe { System.alloc(Self::system_layout(layout)) };
        assert!(!base.is_null());
        // SAFETY: the system block has MISALIGN_BASE bytes of slack before `layout.size()`.
        let block = unsafe { base.add(MISALIGN_OFFSET) };
        self.live.lock().unwrap().insert(block.addr(), layout);
        block
    }

    /// Free a block from [`Self::allocate`], checking it comes back with its layout.
    ///
    /// # Safety
    ///
    /// `block` must be a live block from [`Self::allocate`].
    unsafe fn free(&self, block: *mut u8, layout: Layout) {
        let recorded = self.live.lock().unwrap().remove(&block.addr());
        assert_eq!(recorded, Some(layout), "freed with a different layout");
        // SAFETY: `block` was placed MISALIGN_OFFSET bytes into a system block.
        let base = unsafe { block.sub(MISALIGN_OFFSET) };
        // SAFETY: that system block was allocated with this layout.
        unsafe { System.dealloc(base, Self::system_layout(layout)) };
    }
}

// SAFETY: blocks come from `System` with enough room past the offset for
// `layout.size()` bytes, and are returned to it with the same layout.
unsafe impl GlobalAlloc for Misaligning {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let block = self.allocate(layout);
        self.log(Call::Alloc {
            layout,
            result: block.addr(),
        });
        block
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let block = self.allocate(layout);
        if !block.is_null() {
            // SAFETY: the block is valid for `layout.size()` bytes.
            unsafe { block.write_bytes(0, layout.size()) };
        }
        self.log(Call::AllocZeroed {
            layout,
            result: block.addr(),
        });
        block
    }

    unsafe fn realloc(&self, block: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_block = self.allocate(Layout::from_size_align(new_size, layout.align()).unwrap());
        if !new_block.is_null() {
            // SAFETY: both blocks are live and valid for the smaller size.
            unsafe { std::ptr::copy_nonoverlapping(block, new_block, layout.size().min(new_size)) };
            // SAFETY: `block` is live with `layout`, per the caller's contract.
            unsafe { self.free(block, layout) };
        }
        self.log(Call::Realloc {
            block: block.addr(),
            layout,
            new_size,
            result: new_block.addr(),
        });
        new_block
    }

    unsafe fn dealloc(&self, block: *mut u8, layout: Layout) {
        // SAFETY: per the caller's contract.
        unsafe { self.free(block, layout) };
        self.log(Call::Dealloc {
            block: block.addr(),
            layout,
        });
    }
}

/// Wrap `inner` with the default inner alignment, which a bare `OverAligned(..)`
/// expression cannot infer.
fn over_aligned(inner: Misaligning) -> OverAligned<Misaligning> {
    OverAligned(inner)
}

fn layout(size: usize, align: usize) -> Layout {
    Layout::from_size_align(size, align).unwrap()
}

/// Sizes and alignments that cover blocks smaller than, equal to, and larger
/// than their alignment, for every alignment past what the inner allocator honors.
fn over_aligned_layouts() -> impl Iterator<Item = Layout> {
    [32, 64, 4096]
        .into_iter()
        .flat_map(|align| [1, 31, 32, 33, 100, 5000].map(move |size| layout(size, align)))
}

/// Fill a block with a pattern derived from its size, to catch overlapping or
/// truncated blocks when it is read back.
///
/// # Safety
///
/// `block` must be valid for writes of `size` bytes.
unsafe fn fill(block: *mut u8, size: usize) {
    // SAFETY: per the caller's contract.
    let bytes = unsafe { std::slice::from_raw_parts_mut(block, size) };
    for (i, byte) in bytes.iter_mut().enumerate() {
        *byte = (i % 251) as u8;
    }
}

/// # Safety
///
/// `block` must be valid for reads of `size` bytes.
unsafe fn assert_filled(block: *const u8, size: usize) {
    // SAFETY: per the caller's contract.
    let bytes = unsafe { std::slice::from_raw_parts(block, size) };
    for (i, &byte) in bytes.iter().enumerate() {
        assert_eq!(byte, (i % 251) as u8, "byte {i}");
    }
}

#[test]
fn inner_allocator_never_over_aligns() {
    let inner = Misaligning::default();
    let small = layout(8, MAX_INNER_ALIGN);
    // SAFETY: non-zero size; freed below with the same layout.
    let block = unsafe { inner.alloc(small) };
    assert_eq!(block.addr() % MAX_INNER_ALIGN, 0);
    assert_ne!(block.addr() % MISALIGN_BASE, 0);
    // SAFETY: allocated above with `small`.
    unsafe { inner.dealloc(block, small) };
}

#[test]
fn over_aligned_layouts_get_aligned_usable_blocks() {
    let alloc = over_aligned(Misaligning::default());
    // Keep every block live at once, so the fills would collide if blocks overlapped.
    let blocks: Vec<_> = over_aligned_layouts()
        .map(|layout| {
            // SAFETY: non-zero size; freed below with the same layout.
            let block = unsafe { alloc.alloc(layout) };
            assert!(!block.is_null());
            assert_eq!(block.addr() % layout.align(), 0, "{layout:?}");
            // SAFETY: the block is valid for `layout.size()` bytes.
            unsafe { fill(block, layout.size()) };
            (block, layout)
        })
        .collect();
    for (block, layout) in blocks {
        // SAFETY: the block is valid for `layout.size()` bytes.
        unsafe { assert_filled(block, layout.size()) };
        // SAFETY: the block was allocated with `layout`.
        unsafe { alloc.dealloc(block, layout) };
    }
    assert_eq!(alloc.0.live_count(), 0);
}

/// Layouts the inner allocator honors must reach it exactly as the caller made
/// them, through every entry point, with the inner allocator's pointers handed
/// back untouched: no padding, no header, no extra calls.
#[test]
fn inner_alignments_are_forwarded_unchanged() {
    let alloc = over_aligned(Misaligning::default());
    for align in [1, 8, MAX_INNER_ALIGN] {
        let small = layout(24, align);
        let grown = layout(100, align);

        // SAFETY: non-zero size; reallocated and freed below.
        let block = unsafe { alloc.alloc(small) };
        assert_eq!(
            alloc.0.take_calls(),
            [Call::Alloc {
                layout: small,
                result: block.addr()
            }]
        );

        // SAFETY: non-zero size; freed below with the same layout.
        let zeroed = unsafe { alloc.alloc_zeroed(small) };
        assert_eq!(
            alloc.0.take_calls(),
            [Call::AllocZeroed {
                layout: small,
                result: zeroed.addr()
            }]
        );

        // SAFETY: `block` is live with `small`, and the new size is non-zero.
        let regrown = unsafe { alloc.realloc(block, small, grown.size()) };
        assert_eq!(
            alloc.0.take_calls(),
            [Call::Realloc {
                block: block.addr(),
                layout: small,
                new_size: grown.size(),
                result: regrown.addr(),
            }]
        );

        // SAFETY: `regrown` is live with `grown`.
        unsafe { alloc.dealloc(regrown, grown) };
        // SAFETY: `zeroed` is live with `small`.
        unsafe { alloc.dealloc(zeroed, small) };
        assert_eq!(
            alloc.0.take_calls(),
            [
                Call::Dealloc {
                    block: regrown.addr(),
                    layout: grown
                },
                Call::Dealloc {
                    block: zeroed.addr(),
                    layout: small
                },
            ]
        );
    }
    assert_eq!(alloc.0.live_count(), 0);
}

#[test]
fn alloc_zeroed_zeroes_over_aligned_blocks() {
    let alloc = over_aligned(Misaligning::default());
    for layout in over_aligned_layouts() {
        // SAFETY: non-zero size; freed below with the same layout.
        let block = unsafe { alloc.alloc_zeroed(layout) };
        assert_eq!(block.addr() % layout.align(), 0, "{layout:?}");
        // SAFETY: the block is valid for `layout.size()` bytes.
        let bytes = unsafe { std::slice::from_raw_parts(block, layout.size()) };
        assert!(bytes.iter().all(|&b| b == 0), "{layout:?}");
        // SAFETY: allocated above with `layout`.
        unsafe { alloc.dealloc(block, layout) };
    }
    assert_eq!(alloc.0.live_count(), 0);
}

#[test]
fn realloc_keeps_contents_and_alignment() {
    let alloc = over_aligned(Misaligning::default());
    for align in [MAX_INNER_ALIGN, 32, 4096] {
        let mut current = layout(40, align);
        // SAFETY: non-zero size.
        let mut block = unsafe { alloc.alloc(current) };
        // SAFETY: the block is valid for `current.size()` bytes.
        unsafe { fill(block, current.size()) };
        // Grow, then shrink below the original size.
        for new_size in [3000, 10] {
            // SAFETY: `block` is live with `current`, and `new_size` is non-zero.
            block = unsafe { alloc.realloc(block, current, new_size) };
            assert!(!block.is_null());
            assert_eq!(block.addr() % align, 0, "align {align}, size {new_size}");
            // SAFETY: the block holds the old contents up to the smaller size.
            unsafe { assert_filled(block, current.size().min(new_size)) };
            current = layout(new_size, align);
            // SAFETY: the block is valid for `current.size()` bytes.
            unsafe { fill(block, current.size()) };
        }
        // SAFETY: `block` is live with `current`.
        unsafe { alloc.dealloc(block, current) };
    }
    assert_eq!(alloc.0.live_count(), 0);
}

#[test]
fn inner_exhaustion_is_reported_as_null() {
    let alloc = over_aligned(Misaligning::exhausted());
    for layout in over_aligned_layouts() {
        // SAFETY: non-zero size; nothing to free when the result is null.
        assert!(unsafe { alloc.alloc(layout) }.is_null(), "{layout:?}");
        // SAFETY: as above.
        let zeroed = unsafe { alloc.alloc_zeroed(layout) };
        assert!(zeroed.is_null(), "{layout:?}");
    }
}

#[test]
fn failed_over_aligned_realloc_keeps_the_original_block() {
    let alloc = over_aligned(Misaligning::default());
    let original = layout(40, 64);
    // SAFETY: non-zero size; freed below with the same layout.
    let block = unsafe { alloc.alloc(original) };
    // SAFETY: the block is valid for `original.size()` bytes.
    unsafe { fill(block, original.size()) };

    alloc.0.set_exhausted(true);
    // SAFETY: `block` is live with `original`, and the new size is non-zero.
    let moved = unsafe { alloc.realloc(block, original, 4000) };
    assert!(moved.is_null());
    alloc.0.set_exhausted(false);

    // SAFETY: a failed realloc leaves `block` live with its contents.
    unsafe { assert_filled(block, original.size()) };
    // SAFETY: `block` is still live with `original`.
    unsafe { alloc.dealloc(block, original) };
    assert_eq!(alloc.0.live_count(), 0);
}

#[test]
fn oversized_padding_is_reported_as_null() {
    let alloc = over_aligned(Misaligning::default());
    // Valid on its own, but adding the alignment as padding overflows `isize::MAX`.
    let huge = layout(isize::MAX as usize - 31, 32);
    // SAFETY: non-zero size; nothing to free when the result is null.
    assert!(unsafe { alloc.alloc(huge) }.is_null());
    assert_eq!(alloc.0.live_count(), 0);
}

/// The case behind the bug: a value whose type demands 32-byte alignment, the
/// way an AVX2 substring searcher does, boxed through the adapter.
#[test]
fn boxed_over_aligned_value_is_aligned() {
    #[repr(align(32))]
    struct Avx2Sized([u8; 32]);

    let alloc = over_aligned(Misaligning::default());
    let layout = Layout::new::<Avx2Sized>();
    // SAFETY: non-zero size; freed below with the same layout.
    let block = unsafe { alloc.alloc(layout) }.cast::<Avx2Sized>();
    assert!(block.is_aligned());
    // SAFETY: `block` is aligned and valid for an `Avx2Sized`.
    unsafe { block.write(Avx2Sized([7; 32])) };
    // SAFETY: written above.
    assert_eq!(unsafe { &*block }.0, [7; 32]);
    // SAFETY: allocated above with `layout`.
    unsafe { alloc.dealloc(block.cast(), layout) };
    assert_eq!(alloc.0.live_count(), 0);
}
