/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{alloc::Layout, borrow::Borrow, fmt::Debug, io::Cursor, marker::PhantomData, ptr};

/// Borrowed view of the encoded offsets of a term in a document. You can read the offsets by
/// iterating over it with RSIndexResult_IterateOffsets.
///
/// This is a borrowed, `Copy` type — it does not own the data and will not free it on drop.
/// Use [`RSOffsetVector`] for owned offset data.
#[repr(C)]
#[derive(Copy, Clone)]
pub struct RSOffsetSlice<'index> {
    /// Pointer to the borrowed offset data.
    pub data: *const u8,
    pub len: u32,
    /// The data pointer does not carry a lifetime, so use a `PhantomData` to track it instead.
    _phantom: PhantomData<&'index ()>,
}

impl PartialEq for RSOffsetSlice<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl Eq for RSOffsetSlice<'_> {}

impl Debug for RSOffsetSlice<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.data.is_null() {
            return write!(f, "RSOffsetSlice(null)");
        }
        // SAFETY: `len` is guaranteed to be a valid length for the data pointer.
        let offsets = unsafe { std::slice::from_raw_parts(self.data, self.len as usize) };

        write!(f, "RSOffsetSlice {offsets:?}")
    }
}

impl AsRef<[u8]> for RSOffsetSlice<'_> {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl Borrow<[u8]> for RSOffsetSlice<'_> {
    fn borrow(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl<'index> RSOffsetSlice<'index> {
    /// Create an offset slice borrowing from the given byte slice.
    ///
    /// # Panics
    ///
    /// Panics if `bytes.len() > u32::MAX as usize`.
    pub fn from_slice(bytes: &'index [u8]) -> Self {
        assert!(
            bytes.len() <= u32::MAX as usize,
            "offset slice length exceeds u32::MAX"
        );
        Self {
            data: bytes.as_ptr(),
            len: bytes.len() as u32,
            _phantom: PhantomData,
        }
    }
}

impl RSOffsetSlice<'_> {
    /// Create a new, empty offset slice.
    pub const fn empty() -> Self {
        Self {
            data: ptr::null(),
            len: 0,
            _phantom: PhantomData,
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

    /// Create an owned copy of this offset slice, allocating new memory for the data.
    pub fn to_owned(&self) -> RSOffsetVector {
        let data = if self.len > 0 {
            debug_assert!(!self.data.is_null(), "data must not be null");
            let layout = Layout::array::<u8>(self.len as usize).unwrap();
            // SAFETY: we just checked that len > 0
            let data = unsafe { std::alloc::alloc(layout) };
            if data.is_null() {
                std::alloc::handle_alloc_error(layout)
            };
            // SAFETY:
            // - The source buffer and the destination buffer don't overlap because
            //   they belong to distinct non-overlapping allocations.
            // - The destination buffer is valid for writes of `src.len` elements
            //   since it was just allocated with capacity `src.len`.
            // - The source buffer is valid for reads of `src.len` elements as a call invariant.
            unsafe { std::ptr::copy_nonoverlapping(self.data, data, self.len as usize) };

            data
        } else {
            ptr::null_mut()
        };

        RSOffsetVector {
            data,
            len: self.len,
        }
    }
}

/// Owned encoded offsets of a term in a document.
///
/// This type owns the data and will free it on drop. Use [`RSOffsetSlice`] for borrowed offset
/// data.
///
/// The `#[repr(C)]` layout is identical to [`RSOffsetSlice`] (minus the zero-sized `PhantomData`),
/// so a `&RSOffsetVector` can be safely cast to `&RSOffsetSlice<'_>`.
#[repr(C)]
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
        RSOffsetSlice {
            data: self.data,
            len: self.len,
            _phantom: PhantomData,
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

/// Sentinel value indicating end of offset stream.
///
/// Matches the C constant `RS_OFFSETVECTOR_EOF` (`UINT32_MAX`).
pub const RS_OFFSETVECTOR_EOF: u32 = u32::MAX;

/// Iterates over delta-varint encoded term positions in an offset byte slice,
/// yielding absolute positions one at a time.
///
/// Offsets are stored as delta-encoded varints: each encoded value represents the
/// difference from the previous position. This iterator decodes them back to
/// absolute positions.
///
/// Returns [`RS_OFFSETVECTOR_EOF`] when all positions have been consumed.
pub struct OffsetPositionIterator<'a> {
    cursor: Cursor<&'a [u8]>,
    last_value: u32,
}

impl<'a> OffsetPositionIterator<'a> {
    /// Create a new iterator over the given delta-varint encoded offset data.
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            cursor: Cursor::new(data),
            last_value: 0,
        }
    }

    /// Return the next absolute position, or [`RS_OFFSETVECTOR_EOF`] if exhausted.
    pub fn next_position(&mut self) -> u32 {
        match varint::read::<u32, _>(&mut self.cursor) {
            Ok(delta) => {
                self.last_value += delta;
                self.last_value
            }
            Err(_) => RS_OFFSETVECTOR_EOF,
        }
    }
}
