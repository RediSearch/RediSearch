use crate::Header;

/// Private module to seal the [`VecCapacity`] trait.
mod private {
    pub trait Sealed {}
    impl Sealed for u8 {}
    impl Sealed for u16 {}
    impl Sealed for u32 {}
    impl Sealed for u64 {}
}

/// Trait for types that can be used as the size/capacity type for [`ThinVec`](crate::ThinVec).
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

    /// Reference to a static empty header for this size type.
    const EMPTY_HEADER: &'static Header<Self>;

    /// Convert from usize, panicking if out of range.
    fn from_usize(val: usize) -> Self;

    /// Convert to usize.
    fn to_usize(self) -> usize;
}

static EMPTY_U8: Header<u8> = Header::for_capacity(0);
static EMPTY_U16: Header<u16> = Header::for_capacity(0);
static EMPTY_U32: Header<u32> = Header::for_capacity(0);
static EMPTY_U64: Header<u64> = Header::for_capacity(0);

impl VecCapacity for u8 {
    const MAX: Self = u8::MAX;
    const ZERO: Self = 0;
    const TYPE_NAME: &'static str = "8-bit";
    const EMPTY_HEADER: &'static Header<Self> = &EMPTY_U8;

    #[inline]
    fn from_usize(val: usize) -> Self {
        if val > <Self as VecCapacity>::MAX as usize {
            panic!(
                "TinyThinVec size may not exceed the capacity of an {} sized int",
                Self::TYPE_NAME
            );
        }
        val as u8
    }

    #[inline]
    fn to_usize(self) -> usize {
        self as usize
    }
}

impl VecCapacity for u16 {
    const MAX: Self = u16::MAX;
    const ZERO: Self = 0;
    const TYPE_NAME: &'static str = "16-bit";
    const EMPTY_HEADER: &'static Header<Self> = &EMPTY_U16;

    #[inline]
    fn from_usize(val: usize) -> Self {
        if val > <Self as VecCapacity>::MAX as usize {
            panic!(
                "SmallThinVec size may not exceed the capacity of a {} sized int",
                Self::TYPE_NAME
            );
        }
        val as u16
    }

    #[inline]
    fn to_usize(self) -> usize {
        self as usize
    }
}

impl VecCapacity for u32 {
    const MAX: Self = u32::MAX;
    const ZERO: Self = 0;
    const TYPE_NAME: &'static str = "32-bit";
    const EMPTY_HEADER: &'static Header<Self> = &EMPTY_U32;

    #[inline]
    fn from_usize(val: usize) -> Self {
        if val > <Self as VecCapacity>::MAX as usize {
            panic!(
                "MediumThinVec size may not exceed the capacity of a {} sized int",
                Self::TYPE_NAME
            );
        }
        val as u32
    }

    #[inline]
    fn to_usize(self) -> usize {
        self as usize
    }
}

impl VecCapacity for u64 {
    const MAX: Self = u64::MAX;
    const ZERO: Self = 0;
    const TYPE_NAME: &'static str = "64-bit";
    const EMPTY_HEADER: &'static Header<Self> = &EMPTY_U64;

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
}
