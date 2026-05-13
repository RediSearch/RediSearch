/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{alloc::Layout, borrow::Borrow, fmt::Debug, ptr};

use ref_mode::{Active, Ref, SharedPtr, Suspended};

/// Borrowed view of the encoded offsets of a term in a document. You can read the offsets by
/// iterating over it with RSIndexResult_IterateOffsets.
///
/// This is a borrowed, `Copy` type — it does not own the data and will not free it on drop.
/// Use [`RSOffsetVector`] for owned offset data.
///
/// The `R: Ref` parameter selects between [`Active<'index>`] mode (the data
/// pointer is a valid `&'index [u8]`) and [`ref_mode::Suspended`] mode (the
/// data pointer may be stale).
///
/// `data` is `Option<SharedPtr<R, u8>>` so the empty slice can be represented
/// with a null pointer. Thanks to `NonNull`'s niche, the in-memory layout
/// is still a bare `*const u8` followed by a `u32`.
#[cheadergen::config(rename = "RSOffsetVector")]
#[repr(C)]
pub struct RawOffsetSlice<R: Ref> {
    /// Pointer to the borrowed offset data, or `None` for the empty slice.
    pub data: Option<SharedPtr<R, u8>>,
    pub len: u32,
}

/// The [`Active`] instantiation of [`RawOffsetSlice`]: a borrowed view whose data
/// pointer is a live `&'a [u8]`.
#[cheadergen::config(export)]
pub type RSOffsetSlice<'a> = RawOffsetSlice<Active<'a>>;

impl<R: Ref> Copy for RawOffsetSlice<R> {}

impl<R: Ref> Clone for RawOffsetSlice<R> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a> PartialEq for RSOffsetSlice<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl<'a> Eq for RSOffsetSlice<'a> {}

impl<R: Ref> Debug for RawOffsetSlice<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Some(data) = self.data else {
            return write!(f, "RSOffsetSlice(null)");
        };
        // SAFETY: `len` is guaranteed to be a valid length for the data pointer
        // when the slice was constructed from a valid `&[u8]` source. Debug
        // output for `Suspended` instances may be stale; callers must ensure
        // the underlying memory is still valid before using this impl.
        let offsets = unsafe { std::slice::from_raw_parts(data.as_raw(), self.len as usize) };

        write!(f, "RSOffsetSlice {offsets:?}")
    }
}

impl<'a> AsRef<[u8]> for RSOffsetSlice<'a> {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl<'a> Borrow<[u8]> for RSOffsetSlice<'a> {
    fn borrow(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl<'a> RSOffsetSlice<'a> {
    /// Create an offset slice borrowing from the given byte slice.
    ///
    /// # Panics
    ///
    /// Panics if `bytes.len() > u32::MAX as usize`.
    pub fn from_slice(bytes: &'a [u8]) -> Self {
        assert!(
            bytes.len() <= u32::MAX as usize,
            "offset slice length exceeds u32::MAX"
        );
        // Match the C ABI of the historic `RSOffsetVector`: empty slices
        // are represented as `data = null, len = 0`. Non-empty slices
        // carry a real pointer.
        let data = if bytes.is_empty() {
            None
        } else {
            Some(SharedPtr::from_ref(&bytes[0]))
        };
        Self {
            data,
            len: bytes.len() as u32,
        }
    }

    /// Return the offset data as a byte slice.
    pub const fn as_bytes(&self) -> &'a [u8] {
        match self.data {
            None => &[],
            Some(data) => {
                // SAFETY: By the `Active<'a>` invariant on the wrapping
                // `SharedPtr<Active<'a>, u8>`, it points to memory valid for
                // reads of `len` bytes for the lifetime `'a`.
                unsafe { std::slice::from_raw_parts(data.as_raw(), self.len as usize) }
            }
        }
    }

    /// Create an owned copy of this offset slice, allocating new memory for the data.
    pub fn to_owned(&self) -> RSOffsetVector {
        let data = if self.len > 0 {
            let src = self.data.expect("non-empty slice must have a data pointer");
            let layout = Layout::array::<u8>(self.len as usize).unwrap();
            // SAFETY: we just checked that len > 0
            let dst = unsafe { std::alloc::alloc(layout) };
            if dst.is_null() {
                std::alloc::handle_alloc_error(layout)
            };
            // SAFETY:
            // - The source buffer and the destination buffer don't overlap because
            //   they belong to distinct non-overlapping allocations.
            // - The destination buffer is valid for writes of `src.len` elements
            //   since it was just allocated with capacity `src.len`.
            // - The source buffer is valid for reads of `src.len` elements as a call invariant.
            unsafe { std::ptr::copy_nonoverlapping(src.as_raw(), dst, self.len as usize) };

            dst
        } else {
            ptr::null_mut()
        };

        RSOffsetVector {
            data,
            len: self.len,
        }
    }
}

impl<R: Ref> RawOffsetSlice<R> {
    /// Create a new, empty offset slice.
    pub const fn empty() -> Self {
        Self { data: None, len: 0 }
    }
}

/// Owned encoded offsets of a term in a document.
///
/// This type owns the data and will free it on drop. Use [`RSOffsetSlice`] for borrowed offset
/// data.
///
/// The `#[repr(C)]` layout is identical to [`RSOffsetSlice`] — both store a
/// nullable `*const u8` followed by a `u32`. The borrowed slice's
/// `Option<SharedPtr<R, u8>>` field collapses to a bare pointer thanks to
/// `NonNull`'s niche.
#[repr(C)]
#[cheadergen::config(skip)]
pub struct RSOffsetVector {
    /// Pointer to the owned offset data, allocated via the global allocator.
    pub data: *mut u8,
    pub len: u32,
}

impl PartialEq for RSOffsetVector {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for RSOffsetVector {}

impl Debug for RSOffsetVector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.data.is_null() {
            return write!(f, "RSOffsetVector(null)");
        }
        // SAFETY: `len` is guaranteed to be a valid length for the data pointer.
        let offsets =
            unsafe { std::slice::from_raw_parts(self.data.cast_const(), self.len as usize) };

        write!(f, "RSOffsetVector {offsets:?}")
    }
}

impl AsRef<[u8]> for RSOffsetVector {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl Borrow<[u8]> for RSOffsetVector {
    fn borrow(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl RSOffsetVector {
    /// Create a new, empty offset vector.
    pub const fn empty() -> Self {
        Self {
            data: ptr::null_mut(),
            len: 0,
        }
    }

    /// Return a borrowed view of this owned offset vector.
    pub const fn as_slice<'a>(&'a self) -> RSOffsetSlice<'a> {
        let data = if self.data.is_null() {
            None
        } else {
            // SAFETY: `self.data` is non-null (we just checked above).
            let nn = unsafe { std::ptr::NonNull::new_unchecked(self.data) };
            // SAFETY: `self.data` lives for `'a` (tied to the `&'a self`
            // borrow) and we hold a shared `&'a self`, so the pointer is
            // valid for reads and unaliased for `'a`.
            Some(unsafe { SharedPtr::<Suspended, _>::from_non_null(nn).into_active() })
        };
        RSOffsetSlice {
            data,
            len: self.len,
        }
    }

    /// Return the offset data as a byte slice.
    pub const fn as_bytes(&self) -> &[u8] {
        if self.data.is_null() {
            &[]
        } else {
            // SAFETY: We checked that data is not NULL and `len` is guaranteed to be a valid
            // length for the data pointer.
            unsafe { std::slice::from_raw_parts(self.data, self.len as usize) }
        }
    }
}

impl Drop for RSOffsetVector {
    fn drop(&mut self) {
        if !self.data.is_null() {
            let layout = Layout::array::<u8>(self.len as usize).unwrap();
            // SAFETY: Data was allocated via the global allocator with the matching layout.
            unsafe { std::alloc::dealloc(self.data, layout) };
        }
    }
}
