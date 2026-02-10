use crate::{capacity::VecCapacity, layout::header_field_padding};

/// The header of a [`ThinVec`](crate::ThinVec).
#[repr(C)]
pub struct Header<S: VecCapacity> {
    len: S,
    cap: S,
}

impl<S: VecCapacity> Header<S> {
    /// Creates a new header with the given capacity.
    ///
    /// Length is set to zero.
    pub(crate) const fn for_capacity(cap: S) -> Self {
        Self { len: S::ZERO, cap }
    }

    #[inline]
    pub(crate) const fn len(&self) -> S {
        self.len
    }

    #[inline]
    pub(crate) const fn capacity(&self) -> S {
        self.cap
    }

    #[inline]
    pub(crate) fn set_capacity(&mut self, cap: S) {
        assert!(
            cap >= self.len,
            "Capacity must be greater than or equal to the current length"
        );
        self.cap = cap;
    }

    #[inline]
    pub(crate) fn set_len(&mut self, len: usize) {
        assert!(
            len <= self.cap.to_usize(),
            "New length must be less than or equal to current capacity"
        );
        self.len = S::from_usize(len);
    }

    /// Returns the size of the header, including the padding required for alignment
    /// when the vector is storing elements of type `T`.
    pub const fn size_with_padding<T>() -> usize {
        let header_size = std::mem::size_of::<Self>();
        header_size + header_field_padding::<T, S>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_max_cap_u8() {
        Header::<u8>::for_capacity(u8::MAX);
    }

    #[test]
    fn test_max_cap_u16() {
        Header::<u16>::for_capacity(u16::MAX);
    }

    #[test]
    fn test_max_cap_u32() {
        Header::<u32>::for_capacity(u32::MAX);
    }

    #[test]
    #[should_panic(expected = "Capacity must be greater than or equal to the current length")]
    fn test_small_capacity() {
        let mut header = Header::<u16>::for_capacity(20);
        header.set_len(10);
        header.set_capacity(5);
    }

    #[test]
    #[should_panic(expected = "New length must be less than or equal to current capacity")]
    fn test_large_length() {
        let mut header = Header::<u16>::for_capacity(20);
        header.set_len(30);
    }

    #[test]
    fn test_header_sizes() {
        use std::mem::size_of;

        assert_eq!(size_of::<Header<u8>>(), 2); // 1 + 1
        assert_eq!(size_of::<Header<u16>>(), 4); // 2 + 2
        assert_eq!(size_of::<Header<u32>>(), 8); // 4 + 4
        assert_eq!(size_of::<Header<u64>>(), 16); // 8 + 8
    }
}
