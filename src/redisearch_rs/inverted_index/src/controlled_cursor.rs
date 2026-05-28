//! Custom implementation of [`std::io::Cursor`] which controls the growth strategy used when
//! writing to the underlying vec. The default [`std::io::Cursor`] calls [`Vec::reserve`] directly,
//! which uses an amortised doubling strategy. Earlier revisions of this file replaced that with
//! [`Vec::reserve_exact`] to minimise per-block memory overhead for rare-term inverted-index
//! leaves, but that turned out to be a hot-path regression during bulk indexing because every
//! write triggers a fresh reallocation.
//!
//! The current implementation calls [`Vec::reserve`] on the write path so that the underlying
//! allocator can amortise growth. The initial buffer capacity of [`crate::IndexBlock`] is
//! pre-sized to absorb a handful of typical entries before the first reallocation, which keeps
//! the overhead for rare-term blocks bounded while still benefiting from amortised growth for
//! common-term blocks. There is a `CHANGED` comment to mark this section.
//!
//! The rest of this code is a verbatim copy of the [`std::io::Cursor`] implementation.
//!
//! ## License
//!
//! Portions of this code are derived from the Rust standard library
//! ([`std::io::Cursor`](https://github.com/rust-lang/rust)), which is dual-licensed under:
//!
//! - [Apache License 2.0](./LICENSE-APACHE)
//! - [MIT License](./LICENSE-MIT)
//!
//! We have kept the same license(s) for this codebase.

use std::io::{IoSlice, Seek, SeekFrom, Write};

pub struct ControlledCursor<'buf> {
    inner: &'buf mut Vec<u8>,
    pos: u64,
}

impl<'buf> ControlledCursor<'buf> {
    pub const fn new(inner: &'buf mut Vec<u8>) -> Self {
        Self {
            pos: inner.len() as u64,
            inner,
        }
    }
}

/// Writes the slice to the vec without allocating.
///
/// # Safety
///
/// `vec` must have `buf.len()` spare capacity.
unsafe fn vec_write_all_unchecked(pos: usize, vec: &mut Vec<u8>, buf: &[u8]) -> usize {
    debug_assert!(vec.capacity() >= pos + buf.len());

    // SAFETY: we just checked the position is within the capacity
    let vec = unsafe { vec.as_mut_ptr().add(pos) };

    // SAFETY: we are meeting the following safety conditions
    // - vec is valid for writes of buf.len() bytes because of the capacity check above
    // - vec is properly aligned because it comes from a Vec<u8>
    // - buf.as_ptr() is valid for reads of buf.len() bytes because buf is a valid slice
    // - buf.as_ptr() is properly aligned because it comes from a &[u8]
    unsafe { vec.copy_from(buf.as_ptr(), buf.len()) };

    pos + buf.len()
}

/// Resizing `write_all` implementation for [`ControlledCursor`].
///
/// Cursor is allowed to have a pre-allocated and initialised
/// vector body, but with a position of 0. This means the [`Write`]
/// will overwrite the contents of the vec.
///
/// This also allows for the vec body to be empty, but with a position of N.
/// This means that [`Write`] will pad the vec with 0 initially,
/// before writing anything from that point
fn vec_write_all(pos_mut: &mut u64, vec: &mut Vec<u8>, buf: &[u8]) -> std::io::Result<usize> {
    let buf_len = buf.len();
    let mut pos = reserve_and_pad(pos_mut, vec, buf_len)?;

    // Write the buf then progress the vec forward if necessary
    // Safety: we have ensured that the capacity is available
    // and that all bytes get written up to pos
    unsafe {
        pos = vec_write_all_unchecked(pos, vec, buf);
    }

    if pos > vec.len() {
        // SAFETY: we meet the following safety conditions
        // - `new_len` is equal or less than `capacity()` because of the `reserve_and_pad()` call
        // - All the elements from `old_len..new_len` was initialized in the `vec_write_all_unchecked()` call
        unsafe {
            vec.set_len(pos);
        }
    }

    // Bump us forward
    *pos_mut += buf_len as u64;
    Ok(buf_len)
}

/// Resizing `write_all_vectored` implementation for [`ControlledCursor`].
///
/// Cursor is allowed to have a pre-allocated and initialised
/// vector body, but with a position of 0. This means the [`Write`]
/// will overwrite the contents of the vec.
///
/// This also allows for the vec body to be empty, but with a position of N.
/// This means that [`Write`] will pad the vec with 0 initially,
/// before writing anything from that point
fn vec_write_all_vectored(
    pos_mut: &mut u64,
    vec: &mut Vec<u8>,
    bufs: &[IoSlice<'_>],
) -> std::io::Result<usize> {
    // For safety reasons, we don't want this sum to overflow ever.
    // If this saturates, the reserve should panic to avoid any unsound writing.
    let buf_len = bufs.iter().fold(0usize, |a, b| a.saturating_add(b.len()));
    let mut pos = reserve_and_pad(pos_mut, vec, buf_len)?;

    for buf in bufs {
        // Write the buf then progress the vec forward if necessary
        // Safety: we have ensured that the capacity is available
        // and that all bytes get written up to the last pos
        unsafe {
            pos = vec_write_all_unchecked(pos, vec, buf);
        }
    }
    if pos > vec.len() {
        // SAFETY: we meet the following safety conditions
        // - `new_len` is equal or less than `capacity()` because of the `reserve_and_pad()` call
        // - All the elements from `old_len..new_len` was initialized in the `vec_write_all_unchecked()` call
        unsafe {
            vec.set_len(pos);
        }
    }

    // Bump us forward
    *pos_mut += buf_len as u64;
    Ok(buf_len)
}

/// Reserves the required space, and pads the vec with 0s if necessary.
fn reserve_and_pad(pos_mut: &mut u64, vec: &mut Vec<u8>, buf_len: usize) -> std::io::Result<usize> {
    let pos: usize = (*pos_mut).try_into().map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "cursor position exceeds maximum possible vector length",
        )
    })?;

    // For safety reasons, we don't want these numbers to overflow
    // otherwise our allocation won't be enough
    let desired_cap = pos.saturating_add(buf_len);
    if desired_cap > vec.capacity() {
        // CHANGED: the standard library calls `reserve` directly with the additional element
        // count needed from the current length. We compute the same delta here. `reserve` uses
        // amortised-doubling growth, which keeps per-entry write costs O(1) for blocks that
        // receive many entries. Rare-term blocks are kept bounded by pre-sizing the block buffer
        // at construction (see `IndexBlock::INITIAL_BUFFER_CAPACITY`).
        vec.reserve(desired_cap - vec.len());
    }

    // Pad if pos is above the current len.
    if pos > vec.len() {
        let diff = pos - vec.len();
        // Unfortunately, `resize()` would suffice but the optimiser does not
        // realise the `reserve` it does can be eliminated. So we do it manually
        // to eliminate that extra branch
        let spare = vec.spare_capacity_mut();
        debug_assert!(spare.len() >= diff);
        // Safety: we have allocated enough capacity for this.
        // And we are only writing, not reading
        unsafe {
            spare
                .get_unchecked_mut(..diff)
                .fill(core::mem::MaybeUninit::new(0));
        }

        // Safety: we meet the following safety conditions
        // - `new_len` is equal or less than `capacity()` because of the `reserve` code block
        // - All the elements from `old_len..new_len` was just initialized with 0s
        unsafe {
            vec.set_len(pos);
        }
    }

    Ok(pos)
}

impl<'buf> Write for ControlledCursor<'buf> {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        vec_write_all(&mut self.pos, self.inner, buf)
    }

    #[inline]
    fn write_vectored(&mut self, bufs: &[std::io::IoSlice<'_>]) -> std::io::Result<usize> {
        vec_write_all_vectored(&mut self.pos, self.inner, bufs)
    }

    // The other methods are not used by the inverted index implementation, so there is no need to
    // implement them.

    #[inline]
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'buf> Seek for ControlledCursor<'buf> {
    fn seek(&mut self, style: SeekFrom) -> std::io::Result<u64> {
        let (base_pos, offset) = match style {
            SeekFrom::Start(n) => {
                self.pos = n;
                return Ok(n);
            }
            SeekFrom::End(n) => (self.inner.len() as u64, n),
            SeekFrom::Current(n) => (self.pos, n),
        };
        match base_pos.checked_add_signed(offset) {
            Some(n) => {
                self.pos = n;
                Ok(self.pos)
            }
            None => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid seek to a negative or overflowing position",
            )),
        }
    }
}
