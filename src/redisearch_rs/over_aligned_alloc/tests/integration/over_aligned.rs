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

use over_aligned_alloc::{MAX_INNER_ALIGN, OverAligned};

/// Offset at which [`Misaligning`] places blocks inside a [`MISALIGN_BASE`]-aligned
/// system block.
const MISALIGN_OFFSET: usize = MAX_INNER_ALIGN;
/// Alignment of the system block behind every [`Misaligning`] allocation.
const MISALIGN_BASE: usize = 2 * MAX_INNER_ALIGN;

/// An inner allocator that honors [`MAX_INNER_ALIGN`] and nothing more: every
/// address it returns is a multiple of [`MAX_INNER_ALIGN`] but of no larger power of
/// two, like a `malloc` that happens to be maximally unlucky.
///
/// It records every live allocation with its layout, so a test can check that
/// each block comes back with the layout it was allocated with.
#[derive(Default)]
struct Misaligning {
    live: Mutex<HashMap<usize, Layout>>,
    /// Fail every allocation, to model the inner allocator running out of memory.
    exhausted: bool,
}

impl Misaligning {
    fn exhausted() -> Self {
        Self {
            exhausted: true,
            ..Self::default()
        }
    }

    fn system_layout(layout: Layout) -> Layout {
        Layout::from_size_align(layout.size() + MISALIGN_BASE, MISALIGN_BASE).unwrap()
    }

    fn live_count(&self) -> usize {
        self.live.lock().unwrap().len()
    }
}

// SAFETY: blocks come from `System` with enough room past the offset for
// `layout.size()` bytes, and are returned to it with the same layout.
unsafe impl GlobalAlloc for Misaligning {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        assert!(
            layout.align() <= MAX_INNER_ALIGN,
            "OverAligned asked the inner allocator for {layout:?}"
        );
        if self.exhausted {
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

    unsafe fn dealloc(&self, block: *mut u8, layout: Layout) {
        let recorded = self.live.lock().unwrap().remove(&block.addr());
        assert_eq!(recorded, Some(layout), "freed with a different layout");
        // SAFETY: `block` was placed MISALIGN_OFFSET bytes into a system block.
        let base = unsafe { block.sub(MISALIGN_OFFSET) };
        // SAFETY: that system block was allocated with this layout.
        unsafe { System.dealloc(base, Self::system_layout(layout)) };
    }
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
    let alloc = OverAligned(Misaligning::default());
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

#[test]
fn inner_alignments_are_forwarded_unchanged() {
    let alloc = OverAligned(Misaligning::default());
    for align in [1, 8, MAX_INNER_ALIGN] {
        let layout = layout(24, align);
        // SAFETY: non-zero size; freed below with the same layout.
        let block = unsafe { alloc.alloc(layout) };
        // A forwarded block is exactly what the inner allocator recorded, with
        // no padding added.
        assert_eq!(
            alloc.0.live.lock().unwrap().get(&block.addr()),
            Some(&layout)
        );
        // SAFETY: allocated above with `layout`.
        unsafe { alloc.dealloc(block, layout) };
    }
    assert_eq!(alloc.0.live_count(), 0);
}

#[test]
fn alloc_zeroed_zeroes_over_aligned_blocks() {
    let alloc = OverAligned(Misaligning::default());
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
    let alloc = OverAligned(Misaligning::default());
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
    let alloc = OverAligned(Misaligning::exhausted());
    for layout in over_aligned_layouts() {
        // SAFETY: non-zero size; nothing to free when the result is null.
        assert!(unsafe { alloc.alloc(layout) }.is_null(), "{layout:?}");
        // SAFETY: as above.
        let zeroed = unsafe { alloc.alloc_zeroed(layout) };
        assert!(zeroed.is_null(), "{layout:?}");
    }
}

#[test]
fn oversized_padding_is_reported_as_null() {
    let alloc = OverAligned(Misaligning::default());
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

    let alloc = OverAligned(Misaligning::default());
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
