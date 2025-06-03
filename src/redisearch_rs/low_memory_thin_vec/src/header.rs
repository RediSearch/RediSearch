/// The header of a LowMemoryThinVec.
#[repr(C)]
pub(crate) struct Header {
    len: SizeType,
    cap: SizeType,
}

/// The type used to represent the capacity of a `LowMemoryThinVec`.
pub(crate) type SizeType = u16;

/// The maximum capacity of a `LowMemoryThinVec`.
pub(crate) const MAX_CAP: usize = SizeType::MAX as usize;

#[inline(always)]
/// Convert a `usize` to a [`SizeType`], panicking if the value is too large to fit.
pub(crate) const fn assert_size(x: usize) -> SizeType {
    if x > MAX_CAP {
        panic!("LowMemoryThinVec size may not exceed the capacity of a 16-bit sized int");
    }
    x as SizeType
}

impl Header {
    /// Creates a new header with the given length and capacity.
    ///
    /// # Panics
    ///
    /// Panics if the length is greater than the capacity.
    pub(crate) const fn new(len: usize, cap: usize) -> Self {
        assert!(len <= cap, "Length must be less than or equal to capacity");
        Self {
            len: assert_size(len),
            cap: assert_size(cap),
        }
    }

    #[inline]
    pub(crate) const fn len(&self) -> usize {
        self.len as usize
    }

    #[inline]
    pub(crate) const fn capacity(&self) -> usize {
        self.cap as usize
    }

    #[inline]
    pub(crate) const fn set_capacity(&mut self, cap: usize) {
        assert!(
            cap >= self.len as usize,
            "Capacity must be greater than or equal to the current length"
        );
        self.cap = assert_size(cap);
    }

    #[inline]
    pub(crate) const fn set_len(&mut self, len: usize) {
        assert!(
            len <= self.cap as usize,
            "New length must be less than or equal to current capacity"
        );
        self.len = assert_size(len);
    }
}

/// Singleton used by all empty collections to avoid allocating a header with
/// an empty array of elements on the heap.
pub(crate) static EMPTY_HEADER: Header = Header::new(0, 0);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_max_cap() {
        Header::new(0, u16::MAX as usize);
    }

    #[test]
    #[should_panic(
        expected = "LowMemoryThinVec size may not exceed the capacity of a 16-bit sized int"
    )]
    fn test_over_max_cap() {
        Header::new(0, (u16::MAX as usize) + 1);
    }
    #[test]
    #[should_panic(expected = "Capacity must be greater than or equal to the current length")]
    fn test_small_capacity() {
        let mut header = Header::new(10, 20);
        header.set_capacity(5);
    }

    #[test]
    #[should_panic(expected = "New length must be less than or equal to current capacity")]
    fn test_large_length() {
        let mut header = Header::new(10, 20);
        header.set_len(30);
    }
}
