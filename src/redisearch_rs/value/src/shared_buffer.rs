/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! A reference-counted byte buffer that [`String`]s can borrow slices of without copying
//! them, for decoders whose input buffer already holds every string they produce.

use crate::String;
use std::{
    ffi::c_char,
    ptr::NonNull,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering, fence},
    },
};

/// Releases a buffer handed to [`SharedBuffer::from_raw`], given its start and length.
///
/// # Safety
///
/// Called exactly once per buffer, with the pointer and length it was created with, after
/// every reference to the buffer is gone.
pub type Dealloc = unsafe fn(NonNull<u8>, usize);

/// The furthest from its buffer's start a shared string may begin.
///
/// A shared [`String`] finds its buffer by subtracting its offset from its own pointer, and
/// the offset has to fit the spare bytes of [`String`]'s layout, which is three. A string past
/// this offset is not shareable; see [`SharedBuffer::share`].
pub const MAX_SHARED_OFFSET: usize = (1 << 24) - 1;

/// The bytes at a buffer's start that [`SharedBuffer::from_raw`] claims: they hold a pointer
/// to the buffer's bookkeeping, through which a [`String`] borrowing from the buffer releases
/// it.
pub const RESERVED_PREFIX: usize = size_of::<*const ()>();

/// A handle on a buffer that [`String`]s borrow from, keeping it alive.
///
/// The buffer is freed when the handle and every [`String`] made by [`SharedBuffer::share`]
/// are gone, whichever is last — on whatever thread that happens.
///
/// A single surviving string keeps the whole buffer allocated. [`SharedBuffer::from_raw`]
/// therefore takes an optional counter of the bytes its buffers hold, so a caller can see how
/// much its strings pin and stop sharing past a budget.
#[derive(Debug)]
pub struct SharedBuffer {
    header: NonNull<Header>,
}

/// The buffer's bookkeeping, allocated separately so the buffer stays exactly the caller's.
#[derive(Debug)]
struct Header {
    /// The handle plus every live shared [`String`].
    refs: AtomicUsize,
    base: NonNull<u8>,
    len: usize,
    dealloc: Dealloc,
    /// Bytes held by live buffers created with the same counter; this one adds [`Header::len`]
    /// while it lives.
    live_bytes: Option<Arc<AtomicUsize>>,
}

// SAFETY: the buffer is only read through a shared handle — `share` writes nothing — and
// released exactly once, by whichever owner drops the last reference.
unsafe impl Send for SharedBuffer {}
// SAFETY: as above; `&SharedBuffer` only permits reads and reference count increments.
unsafe impl Sync for SharedBuffer {}

impl SharedBuffer {
    /// Takes ownership of the `len` bytes at `base`, to be released with `dealloc`.
    ///
    /// The first [`RESERVED_PREFIX`] bytes are overwritten, so the caller must be done
    /// reading them. Returns `None`, having done nothing and taken nothing, if the buffer is
    /// not even that long.
    ///
    /// `live_bytes`, if given, counts this buffer's `len` until the buffer is freed.
    ///
    /// # Safety
    ///
    /// 1. `base` must be [valid] for reads and writes of `len` bytes, and must not be
    ///    accessed through any other pointer once this returns `Some` — except by reading
    ///    [`SharedBuffer::as_bytes`] and the strings [`SharedBuffer::share`] makes.
    /// 2. `dealloc(base, len)` must release the buffer, and the buffer must need no other
    ///    release.
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
        // SAFETY: the prefix is in bounds and writable per (1.), and no reference to it
        // exists; a string reads it back with the same unaligned access.
        unsafe {
            base.cast::<*const Header>()
                .write_unaligned(header.as_ptr())
        };
        Some(Self { header })
    }

    /// The buffer's bytes, its [`RESERVED_PREFIX`] included.
    pub const fn as_bytes(&self) -> &[u8] {
        let header = self.header();
        // SAFETY: the buffer is valid for `len` bytes while this handle lives, and is only
        // ever written before the handle is handed out.
        unsafe { std::slice::from_raw_parts(header.base.as_ptr(), header.len) }
    }

    /// A [`String`] of the `len` bytes at `offset` that keeps this buffer alive instead of
    /// copying them, or `None` if no such string can be made.
    ///
    /// `None` when `offset` is past [`MAX_SHARED_OFFSET`] or inside the
    /// [`RESERVED_PREFIX`], or when the string does not fit the buffer with a NUL right after
    /// it, as every [`String`] requires.
    pub fn share(&self, offset: usize, len: u32) -> Option<String> {
        let header = self.header();
        let end = offset.checked_add(len as usize)?;
        if !(RESERVED_PREFIX..=MAX_SHARED_OFFSET).contains(&offset)
            || end >= header.len
            || self.as_bytes()[end] != 0
        {
            return None;
        }

        // A relaxed increment suffices, as for `Arc::clone`: the new reference is made from
        // one that is already live, so the buffer cannot be freed in between.
        header.refs.fetch_add(1, Ordering::Relaxed);
        // SAFETY: `offset` is in bounds, checked above; deriving the pointer from `base`
        // keeps the provenance the string needs to reach the prefix again.
        let ptr = unsafe { header.base.as_ptr().add(offset) }.cast::<c_char>();
        // SAFETY: the bytes up to and including the NUL are in bounds and stay unmodified for
        // as long as the reference just taken lives; the string releases it on drop.
        Some(unsafe { String::shared(ptr, len, offset) })
    }

    const fn header(&self) -> &Header {
        // SAFETY: the header lives until the last reference is released, and this handle
        // holds one.
        unsafe { self.header.as_ref() }
    }

    /// Releases one reference to the buffer whose shared string starts `offset` bytes into
    /// it, at `ptr`.
    ///
    /// # Safety
    ///
    /// 1. `ptr` and `offset` must be those of a [`String`] made by [`SharedBuffer::share`],
    ///    which gives up its reference with this call.
    pub(crate) unsafe fn release_shared(ptr: *const c_char, offset: usize) {
        // SAFETY: the string lies `offset` bytes into its buffer, with `base`'s provenance,
        // per (1.).
        let base = unsafe { ptr.sub(offset) }.cast::<*const Header>();
        // SAFETY: `from_raw` wrote the header pointer there before any string existed, and the
        // buffer is still alive since the string's reference is not yet released.
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

/// Gives up one reference, freeing the buffer and `header` with the last one.
///
/// # Safety
///
/// 1. The caller must hold one of `header`'s references and not use it afterwards.
unsafe fn release(header: NonNull<Header>) {
    // SAFETY: the header is alive while the caller's reference is, per (1.).
    let refs = &unsafe { header.as_ref() }.refs;
    // Release and acquire as in `Arc::drop`: every other owner's reads of the buffer happen
    // before it is freed.
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
