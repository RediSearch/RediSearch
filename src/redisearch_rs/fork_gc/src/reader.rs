use std::ffi::c_void;
use std::io::{self, Read};
use std::ptr;

/// Reader over a Fork GC pipe endpoint.
///
/// Mirror of [`PipeWriter`] on the parent side. Constructed via
/// [`ForkGC::pipe_read`](crate::ForkGC::pipe_read) in production, or
/// directly via [`from_reader`](Self::from_reader) in tests. Exposes
/// [`recv_fixed`](Self::recv_fixed) as an inherent method; callers go
/// through that rather than through [`Read`] directly.
pub struct Reader<R: Read> {
    reader: R,
}

impl<R: Read> Reader<R> {
    /// Wrap any [`Read`] impl as a `PipeReader`.
    pub fn from_reader(reader: R) -> Self {
        Self { reader }
    }

    /// Read exactly `buf.len()` bytes from the pipe into `buf`.
    ///
    /// Wraps [`Read::read_exact`]. The production reader polls the fd
    /// with a 3-minute timeout and retries on `EINTR`; a timeout is
    /// surfaced as [`io::ErrorKind::TimedOut`] and a short stream as
    /// [`io::ErrorKind::UnexpectedEof`]. The FFI trampoline maps either
    /// to `REDISMODULE_ERR`.
    pub fn recv_fixed(&mut self, buf: &mut [u8]) -> io::Result<()> {
        self.reader.read_exact(buf)
    }

    /// Read a length-prefixed buffer frame.
    ///
    /// Counterpart of [`PipeWriter::send_buffer`] and
    /// [`PipeWriter::send_terminator`]. Reads a native-endian `size_t`
    /// prefix, then:
    ///
    /// - `usize::MAX` → [`RecvFrame::Terminator`] (end-of-stream
    ///   sentinel; no payload follows).
    /// - `0` → [`RecvFrame::Empty`] (no payload).
    /// - otherwise → [`RecvFrame::Data`] containing exactly that many
    ///   payload bytes.
    pub fn recv_buffer(&mut self) -> io::Result<RecvFrame> {
        let mut len_bytes = [0u8; size_of::<usize>()];
        self.recv_fixed(&mut len_bytes)?;
        let len = usize::from_ne_bytes(len_bytes);

        if len == usize::MAX {
            return Ok(RecvFrame::Terminator);
        }
        if len == 0 {
            return Ok(RecvFrame::Empty);
        }

        let mut data = vec![0u8; len];
        self.recv_fixed(&mut data)?;
        Ok(RecvFrame::Data(data))
    }
}

/// A frame decoded by [`PipeReader::recv_buffer`].
///
/// The three variants correspond to the three possible length prefixes
/// of the Fork GC buffer protocol: `usize::MAX` (end of stream), `0`
/// (empty), or a positive payload length.
#[derive(Debug)]
pub enum RecvFrame {
    /// End-of-stream sentinel — the writer called
    /// [`PipeWriter::send_terminator`].
    Terminator,
    /// Zero-length frame — the writer called
    /// [`PipeWriter::send_buffer`] with an empty slice.
    Empty,
    /// A frame carrying `data.len()` payload bytes.
    Data(Vec<u8>),
}

impl RecvFrame {
    /// Consume this frame, producing the `(buf, len)` pair that the C
    /// `FGC_recvBuffer` API exposes through its out-parameters.
    ///
    /// - [`RecvFrame::Terminator`] → `(terminator, usize::MAX)`. The
    ///   caller supplies the sentinel pointer that C-side callers
    ///   (e.g. `recvFieldHeader`) compare by identity to detect
    ///   end-of-stream.
    /// - [`RecvFrame::Empty`] → `(null, 0)`.
    /// - [`RecvFrame::Data`] → the payload is copied into a fresh
    ///   `len + 1`-byte, NUL-terminated allocation via the global
    ///   allocator (which routes through `RedisModule_Alloc`). Ownership
    ///   of the pointer is transferred to the caller, which is expected
    ///   to release it with the Redis module allocator's `rm_free`.
    pub fn into_c_buffer(self, terminator: *mut c_void) -> (*mut c_void, usize) {
        match self {
            RecvFrame::Terminator => (terminator, usize::MAX),
            RecvFrame::Empty => (ptr::null_mut(), 0),
            RecvFrame::Data(data) => {
                let len = data.len();
                // Allocate `len + 1` with a trailing NUL so C callers
                // can treat the result as a C string, matching the
                // original `rm_malloc(len + 1); buf[len] = 0;` shape.
                let mut owned = Vec::<u8>::with_capacity(len + 1);
                owned.extend_from_slice(&data);
                owned.push(0);
                let leaked: &'static mut [u8] = Box::leak(owned.into_boxed_slice());
                (leaked.as_mut_ptr().cast(), len)
            }
        }
    }
}
