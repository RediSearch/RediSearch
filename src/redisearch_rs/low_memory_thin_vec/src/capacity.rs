use crate::Header;

/// Private module to seal the [`VecCapacity`] trait.
mod private {
    pub trait Sealed {}
    impl Sealed for u8 {}
    impl Sealed for u16 {}
    impl Sealed for u32 {}
    impl Sealed for u64 {}
}

/// Trait for types that can be used as the size/capacity type for [`LowMemoryThinVec`](crate::LowMemoryThinVec).
///
/// This trait is sealed and can only be implemented for u8, u16, u32, and u64.
pub trait VecCapacity:
    private::Sealed + Copy + PartialEq + PartialOrd + Eq + Ord + 'static
{
    /// The maximum value representable by this type.
    const MAX: Self;

    /// The zero value for this type.
    const ZERO: Self;

    /// A human-readable name for this type, used in panic messages.
    const TYPE_NAME: &'static str;

    /// Convert from usize, panicking if out of range.
    fn from_usize(val: usize) -> Self;

    /// Convert to usize.
    fn to_usize(self) -> usize;

    /// Reference to a static empty header for this size type.
    fn empty_header() -> &'static Header<Self>;
}

impl VecCapacity for u8 {
    const MAX: u8 = u8::MAX;
    const ZERO: Self = 0;
    const TYPE_NAME: &'static str = "8-bit";

    #[inline]
    fn from_usize(val: usize) -> Self {
        if val > <Self as VecCapacity>::MAX as usize {
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
        static EMPTY: Header<u8> = Header::for_capacity(0);
        &EMPTY
    }
}

impl VecCapacity for u16 {
    const MAX: u16 = u16::MAX;
    const ZERO: Self = 0;
    const TYPE_NAME: &'static str = "16-bit";

    #[inline]
    fn from_usize(val: usize) -> Self {
        if val > <Self as VecCapacity>::MAX as usize {
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
        static EMPTY: Header<u16> = Header::for_capacity(0);
        &EMPTY
    }
}

impl VecCapacity for u32 {
    const MAX: u32 = u32::MAX;
    const ZERO: Self = 0;
    const TYPE_NAME: &'static str = "32-bit";

    #[inline]
    fn from_usize(val: usize) -> Self {
        if val > <Self as VecCapacity>::MAX as usize {
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
        static EMPTY: Header<u32> = Header::for_capacity(0);
        &EMPTY
    }
}

impl VecCapacity for u64 {
    const MAX: u64 = u64::MAX;
    const ZERO: Self = 0;
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
        static EMPTY: Header<u64> = Header::for_capacity(0);
        &EMPTY
    }
}
