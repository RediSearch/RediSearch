/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! A reference-counted byte buffer that [`String`]s borrow slices of instead of copying them.

use crate::String;
use std::{
    ffi::c_char,
    ptr::NonNull,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering, fence},
    },
};

/// Releases a buffer given to [`SharedBuffer::from_raw`].
///
/// # Safety
///
/// Called once per buffer, with its pointer and length, after the last reference is gone.
pub type Dealloc = unsafe fn(NonNull<u8>, usize);

/// The furthest into its buffer a shared [`String`] may start: it finds the buffer by subtracting its offset, which
/// must fit the three spare bytes of its layout.
pub const MAX_SHARED_OFFSET: usize = (1 << 24) - 1;

/// The bytes at a buffer's start [`SharedBuffer::from_raw`] overwrites with a pointer to its bookkeeping.
pub const RESERVED_PREFIX: usize = size_of::<*const ()>();

/// A handle on a buffer, freed (on any thread) once the handle and every [`String`] made by [`SharedBuffer::share`] are
/// gone. One surviving string keeps the whole buffer allocated.
#[derive(Debug)]
pub struct SharedBuffer {
    header: NonNull<Header>,
}

#[derive(Debug)]
struct Header {
    /// The handle plus every live shared [`String`].
    refs: AtomicUsize,
    base: NonNull<u8>,
    len: usize,
    dealloc: Dealloc,
    /// Counts [`Header::len`] while this buffer lives.
    live_bytes: Option<Arc<AtomicUsize>>,
}

// SAFETY: the buffer is only read after creation, and released once, by the last reference.
unsafe impl Send for SharedBuffer {}
// SAFETY: as above; `&SharedBuffer` only permits reads and reference count increments.
unsafe impl Sync for SharedBuffer {}

impl SharedBuffer {
    /// Takes ownership of the `len` bytes at `base`, overwriting the first [`RESERVED_PREFIX`]. Returns `None`, taking
    /// nothing, if the buffer is shorter than that. `live_bytes` counts `len` until the buffer is freed.
    ///
    /// # Safety
    ///
    /// 1. `base` must be [valid] for reads and writes of `len` bytes, and accessed only through the returned handle and
    ///    its strings.
    /// 2. `dealloc(base, len)` must be the buffer's only release.
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    pub unsafe fn from_raw(
        base: NonNull<u8>,
        len: usize,
        dealloc: Dealloc,
        live_bytes: Option<Arc<AtomicUsize>>,
    ) -> Option<Self> {
        const { assert!(RESERVED_PREFIX == size_of::<*const Header>()) };
        if len < RESERVED_PREFIX {
            return None;
        }
        if let Some(live) = &live_bytes {
            live.fetch_add(len, Ordering::Relaxed);
        }
        let header = NonNull::from(Box::leak(Box::new(Header {
            refs: AtomicUsize::new(1),
            base,
            len,
            dealloc,
            live_bytes,
        })));
        // SAFETY: the prefix is in bounds and writable per (1.), and unreferenced.
        unsafe {
            base.cast::<*const Header>()
                .write_unaligned(header.as_ptr())
        };
        Some(Self { header })
    }

    /// Includes the [`RESERVED_PREFIX`].
    pub const fn as_bytes(&self) -> &[u8] {
        let header = self.header();
        // SAFETY: valid for `len` bytes while the handle lives, and never written after creation.
        unsafe { std::slice::from_raw_parts(header.base.as_ptr(), header.len) }
    }

    /// A [`String`] of the `len` bytes at `offset`, keeping the buffer alive. `None` if `offset` is inside the
    /// [`RESERVED_PREFIX`] or past [`MAX_SHARED_OFFSET`], or the bytes are not followed by a NUL within the buffer.
    pub fn share(&self, offset: usize, len: u32) -> Option<String> {
        let header = self.header();
        let end = offset.checked_add(len as usize)?;
        if !(RESERVED_PREFIX..=MAX_SHARED_OFFSET).contains(&offset)
            || end >= header.len
            || self.as_bytes()[end] != 0
        {
            return None;
        }

        // Relaxed, as in `Arc::clone`: made from a live reference.
        header.refs.fetch_add(1, Ordering::Relaxed);
        // SAFETY: in bounds, checked above; derived from `base` so the string can reach the prefix.
        let ptr = unsafe { header.base.as_ptr().add(offset) }.cast::<c_char>();
        // SAFETY: the bytes and NUL are in bounds and unmodified while the reference just taken lives.
        Some(unsafe { String::shared(ptr, len, offset) })
    }

    const fn header(&self) -> &Header {
        // SAFETY: the header lives while this handle's reference does.
        unsafe { self.header.as_ref() }
    }

    /// # Safety
    ///
    /// 1. `ptr` and `offset` must be those of a [`String`] made by [`SharedBuffer::share`], giving up its reference.
    pub(crate) unsafe fn release_shared(ptr: *const c_char, offset: usize) {
        // SAFETY: `offset` bytes into its buffer, with `base`'s provenance, per (1.).
        let base = unsafe { ptr.sub(offset) }.cast::<*const Header>();
        // SAFETY: written by `from_raw` before any string existed; the buffer is alive per (1.).
        let header = unsafe { base.read_unaligned() };
        // SAFETY: `from_raw` wrote a non-null header pointer.
        let header = unsafe { NonNull::new_unchecked(header.cast_mut()) };
        // SAFETY: the string's reference is given up here, per (1.).
        unsafe { release(header) };
    }
}

impl Drop for SharedBuffer {
    fn drop(&mut self) {
        // SAFETY: this handle holds one reference, given up here.
        unsafe { release(self.header) };
    }
}

/// # Safety
///
/// 1. The caller must hold one of `header`'s references and not use it afterwards.
unsafe fn release(header: NonNull<Header>) {
    // SAFETY: the header is alive while the caller's reference is, per (1.).
    let refs = &unsafe { header.as_ref() }.refs;
    // Release and acquire as in `Arc::drop`, so all reads happen before the free.
    if refs.fetch_sub(1, Ordering::Release) != 1 {
        return;
    }
    fence(Ordering::Acquire);

    // SAFETY: the last reference is gone, so nothing else can reach the header.
    let header = unsafe { Box::from_raw(header.as_ptr()) };
    if let Some(live) = &header.live_bytes {
        live.fetch_sub(header.len, Ordering::Relaxed);
    }
    // SAFETY: called once, with what `from_raw` was given, after the last reference.
    unsafe { (header.dealloc)(header.base, header.len) };
}
