//! Custom implementation of [`std::io::Cursor`] which uses a conservative growth strategy
//! when writing to the underlying vec. This is needed because the default Cursor uses a
//! [`Vec::reserve`] call which uses a doubling strategy. This can lead to excessive memory usage
//! for inverted index leafs with a small number of documents and therefore a small buffer inside
//! [`crate::IndexBlock`]s. This is common for text indexes where many terms are rare and only appear in a
//! few documents.
//!
//! This implementation writes into a [`crate::BlockBuffer`] and uses a
//! [`thin_vec::ThinVec::reserve_exact`] call to only allocate the exact amount of memory needed to
//! write the data. This can lead to more frequent allocations, but avoids the excessive memory
//! usage caused by the doubling strategy. There is a `CHANGED` comment to mark this section.
//!
//! The rest of this code follows the [`std::io::Cursor`] implementation, with its raw writes into
//! spare capacity replaced by safe slice copies.
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

use crate::BlockBuffer;

pub struct ControlledCursor<'buf> {
    inner: &'buf mut BlockBuffer,
    pos: u64,
}

impl<'buf> ControlledCursor<'buf> {
    pub fn new(inner: &'buf mut BlockBuffer) -> Self {
        Self {
            pos: inner.len() as u64,
            inner,
        }
    }
}

/// Write `buf` at `pos`, overwriting the bytes already there and appending the rest. The caller
/// must have reserved the capacity and padded the vec up to `pos`.
fn vec_write_at(pos: usize, vec: &mut BlockBuffer, buf: &[u8]) -> usize {
    debug_assert!(vec.len() >= pos && vec.capacity() >= pos + buf.len());

    let overlap = (vec.len() - pos).min(buf.len());
    vec[pos..pos + overlap].copy_from_slice(&buf[..overlap]);
    vec.extend_from_slice(&buf[overlap..]);

    pos + buf.len()
}

fn vec_write_all(pos_mut: &mut u64, vec: &mut BlockBuffer, buf: &[u8]) -> std::io::Result<usize> {
    let buf_len = buf.len();
    let pos = reserve_and_pad(pos_mut, vec, buf_len)?;
    vec_write_at(pos, vec, buf);

    // Bump us forward
    *pos_mut += buf_len as u64;
    Ok(buf_len)
}

fn vec_write_all_vectored(
    pos_mut: &mut u64,
    vec: &mut BlockBuffer,
    bufs: &[IoSlice<'_>],
) -> std::io::Result<usize> {
    // For safety reasons, we don't want this sum to overflow ever.
    // If this saturates, the reserve should panic to avoid any unsound writing.
    let buf_len = bufs.iter().fold(0usize, |a, b| a.saturating_add(b.len()));
    let mut pos = reserve_and_pad(pos_mut, vec, buf_len)?;

    for buf in bufs {
        pos = vec_write_at(pos, vec, buf);
    }

    // Bump us forward
    *pos_mut += buf_len as u64;
    Ok(buf_len)
}

fn reserve_and_pad(
    pos_mut: &mut u64,
    vec: &mut BlockBuffer,
    buf_len: usize,
) -> std::io::Result<usize> {
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
        // CHANGED: only the code in this code branch is different from the standard library
        // implementation.
        let mut new_cap = vec.capacity();

        while new_cap < desired_cap {
            new_cap += (1 + new_cap / 5).min(1_024 * 1_024);
        }

        // We want our vec's total capacity
        // to have room for (pos+buf_len) bytes. Reserve exact allocates
        // based on additional elements from the length, so we need to
        // reserve the difference
        vec.reserve_exact(new_cap - vec.len());
    }

    // Pad if pos is above the current len. The capacity reserved above covers it, so this
    // does not reallocate.
    if pos > vec.len() {
        vec.resize(pos, 0);
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
