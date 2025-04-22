use super::write;

#[derive(Debug)]
pub(crate) struct VectorWriter {
    buffer: Vec<u8>,
    n_members: usize,
    last_value: u32,
}

impl VectorWriter {
    /// Create a new `VectorWriter` with the given capacity.
    pub fn new(cap: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(cap),
            last_value: 0,
            n_members: 0,
        }
    }

    /// Write an integer to the vector.
    ///
    /// # Return Value
    ///
    /// The number of bytes written to the vector.
    pub fn write(&mut self, value: u32) -> std::io::Result<usize> {
        // Assumes `value` is greater than `last_value` (copied over from the C code).
        let bytes_written = write(value - self.last_value, &mut self.buffer)?;
        if bytes_written != 0 {
            self.n_members += 1;
            self.last_value = value;
        }

        Ok(bytes_written)
    }

    /// reference to the internal byte buffer.
    #[inline(always)]
    pub fn bytes(&self) -> &[u8] {
        &self.buffer
    }

    /// reference to the internal byte buffer as mutable.
    #[inline(always)]
    pub fn bytes_mut(&mut self) -> &mut Vec<u8> {
        &mut self.buffer
    }

    /// Reset the vector writer (dropping all bytes).
    pub fn reset(&mut self) {
        self.buffer.clear();
        self.last_value = 0;
        self.n_members = 0;
    }

    /// The number of bytes written to the vector.
    #[inline(always)]
    pub fn bytes_len(&self) -> usize {
        self.buffer.len()
    }

    /// The number of members written to the vector.
    #[inline(always)]
    pub fn count(&self) -> usize {
        self.n_members
    }

    /// Truncate the vector.
    pub fn truncate(&mut self) -> usize {
        self.buffer.shrink_to_fit();

        self.buffer.capacity()
    }
}
