/// Private module to seal the `SizeType` trait.
mod private {
    pub trait Sealed {}
    impl Sealed for u8 {}
    impl Sealed for u16 {}
    impl Sealed for u32 {}
    impl Sealed for u64 {}
}

/// Trait for types that can be used as the size/capacity type for `LowMemoryThinVec`.
///
/// This trait is sealed and can only be implemented for u8, u16, u32, and u64.
pub trait SizeType: private::Sealed + Copy + Default + 'static {
    /// The maximum value representable by this type.
    const MAX: usize;

    /// A human-readable name for this type, used in panic messages.
    const TYPE_NAME: &'static str;

    /// Convert from usize, panicking if out of range.
    fn from_usize(val: usize) -> Self;

    /// Convert to usize.
    fn to_usize(self) -> usize;

    /// Reference to a static empty header for this size type.
    fn empty_header() -> &'static Header<Self>;
}

impl SizeType for u8 {
    const MAX: usize = u8::MAX as usize;
    const TYPE_NAME: &'static str = "8-bit";

    #[inline]
    fn from_usize(val: usize) -> Self {
        if val > <Self as SizeType>::MAX {
            panic!(
                "LowMemoryThinVec size may not exceed the capacity of an {} sized int",
                Self::TYPE_NAME
            );
        }
        val as u8
    }

    #[inline]
    fn to_usize(self) -> usize {
        self as usize
    }

    fn empty_header() -> &'static Header<Self> {
        static EMPTY: Header<u8> = Header::from_size_type(0, 0);
        &EMPTY
    }
}

impl SizeType for u16 {
    const MAX: usize = u16::MAX as usize;
    const TYPE_NAME: &'static str = "16-bit";

    #[inline]
    fn from_usize(val: usize) -> Self {
        if val > <Self as SizeType>::MAX {
            panic!(
                "LowMemoryThinVec size may not exceed the capacity of a {} sized int",
                Self::TYPE_NAME
            );
        }
        val as u16
    }

    #[inline]
    fn to_usize(self) -> usize {
        self as usize
    }

    fn empty_header() -> &'static Header<Self> {
        static EMPTY: Header<u16> = Header::from_size_type(0, 0);
        &EMPTY
    }
}

impl SizeType for u32 {
    const MAX: usize = u32::MAX as usize;
    const TYPE_NAME: &'static str = "32-bit";

    #[inline]
    fn from_usize(val: usize) -> Self {
        if val > <Self as SizeType>::MAX {
            panic!(
                "LowMemoryThinVec size may not exceed the capacity of a {} sized int",
                Self::TYPE_NAME
            );
        }
        val as u32
    }

    #[inline]
    fn to_usize(self) -> usize {
        self as usize
    }

    fn empty_header() -> &'static Header<Self> {
        static EMPTY: Header<u32> = Header::from_size_type(0, 0);
        &EMPTY
    }
}

impl SizeType for u64 {
    const MAX: usize = u64::MAX as usize;
    const TYPE_NAME: &'static str = "64-bit";

    #[inline]
    fn from_usize(val: usize) -> Self {
        // On 64-bit platforms, usize::MAX == u64::MAX, so no overflow check needed.
        // On 32-bit platforms, usize fits in u64.
        val as u64
    }

    #[inline]
    fn to_usize(self) -> usize {
        // On 32-bit platforms, this could truncate, but in practice
        // we never store more than usize::MAX elements.
        self as usize
    }

    fn empty_header() -> &'static Header<Self> {
        static EMPTY: Header<u64> = Header::from_size_type(0, 0);
        &EMPTY
    }
}

/// The header of a `LowMemoryThinVec`.
#[repr(C)]
pub struct Header<S: SizeType> {
    len: S,
    cap: S,
}

impl<S: SizeType> Header<S> {
    /// Creates a new header directly from size type values.
    ///
    /// This is a const fn that takes validated size type values directly,
    /// bypassing the trait method calls for use in static initialization and
    /// after explicit validation. Since the inputs are already of type S,
    /// no bounds checking is needed.
    ///
    /// The caller must ensure len <= cap.
    pub(crate) const fn from_size_type(len: S, cap: S) -> Self {
        Self { len, cap }
    }

    /// Creates a new header with the given length and capacity.
    ///
    /// # Panics
    ///
    /// Panics if the length is greater than the capacity, or if either value
    /// exceeds the maximum for the size type.
    pub(crate) fn new(len: usize, cap: usize) -> Self {
        assert!(len <= cap, "Length must be less than or equal to capacity");
        Self {
            len: S::from_usize(len),
            cap: S::from_usize(cap),
        }
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.len.to_usize()
    }

    #[inline]
    pub(crate) fn capacity(&self) -> usize {
        self.cap.to_usize()
    }

    #[inline]
    pub(crate) fn set_capacity(&mut self, cap: usize) {
        assert!(
            cap >= self.len.to_usize(),
            "Capacity must be greater than or equal to the current length"
        );
        self.cap = S::from_usize(cap);
    }

    #[inline]
    pub(crate) fn set_len(&mut self, len: usize) {
        assert!(
            len <= self.cap.to_usize(),
            "New length must be less than or equal to current capacity"
        );
        self.len = S::from_usize(len);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_max_cap_u8() {
        Header::<u8>::new(0, u8::MAX as usize);
    }

    #[test]
    #[should_panic(
        expected = "LowMemoryThinVec size may not exceed the capacity of an 8-bit sized int"
    )]
    fn test_over_max_cap_u8() {
        Header::<u8>::new(0, (u8::MAX as usize) + 1);
    }

    #[test]
    fn test_max_cap_u16() {
        Header::<u16>::new(0, u16::MAX as usize);
    }

    #[test]
    #[should_panic(
        expected = "LowMemoryThinVec size may not exceed the capacity of a 16-bit sized int"
    )]
    fn test_over_max_cap_u16() {
        Header::<u16>::new(0, (u16::MAX as usize) + 1);
    }

    #[test]
    fn test_max_cap_u32() {
        Header::<u32>::new(0, u32::MAX as usize);
    }

    #[test]
    #[cfg(target_pointer_width = "64")]
    #[should_panic(
        expected = "LowMemoryThinVec size may not exceed the capacity of a 32-bit sized int"
    )]
    fn test_over_max_cap_u32() {
        Header::<u32>::new(0, (u32::MAX as usize) + 1);
    }

    #[test]
    #[should_panic(expected = "Capacity must be greater than or equal to the current length")]
    fn test_small_capacity() {
        let mut header = Header::<u16>::new(10, 20);
        header.set_capacity(5);
    }

    #[test]
    #[should_panic(expected = "New length must be less than or equal to current capacity")]
    fn test_large_length() {
        let mut header = Header::<u16>::new(10, 20);
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
