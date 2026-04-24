use std::io::{self, Read};

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
}
