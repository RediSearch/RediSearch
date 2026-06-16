use crate::Header;

/// Private module to seal the [`VecCapacity`] trait.
mod private {
    use super::{AlignedU8, AlignedU16, AlignedU32, AlignedU64};

    pub trait Sealed {}
    impl Sealed for u8 {}
    impl Sealed for u16 {}
    impl Sealed for u32 {}
    impl Sealed for u64 {}
    impl Sealed for AlignedU8 {}
    impl Sealed for AlignedU16 {}
    impl Sealed for AlignedU32 {}
    impl Sealed for AlignedU64 {}
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

    /// The alignment of the [`Self::EMPTY_HEADER`] singleton's storage.
    const SINGLETON_ALIGN: usize;

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
    const SINGLETON_ALIGN: usize = std::mem::align_of::<Header<Self>>();
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
    const SINGLETON_ALIGN: usize = std::mem::align_of::<Header<Self>>();
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
    const SINGLETON_ALIGN: usize = std::mem::align_of::<Header<Self>>();
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
    const SINGLETON_ALIGN: usize = std::mem::align_of::<Header<Self>>();
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

// Aligned capacity types.
//
// Each `Header<AlignedU*>` is byte-identical to `Header<u*>`.
// The `EMPTY_ALIGNED_*` singletons are aligned to the full header size (rather than
// just the header's natural alignment), reported via [`SINGLETON_ALIGN`](VecCapacity::SINGLETON_ALIGN),
// so [`ThinVec::data_raw`](crate::ThinVec) elides its empty-singleton alignment
// guard for more element types.

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct AlignedU8(u8);

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct AlignedU16(u16);

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct AlignedU32(u32);

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct AlignedU64(u64);

#[repr(C, align(2))]
struct AlignedEmptyU8(Header<AlignedU8>);
#[repr(C, align(4))]
struct AlignedEmptyU16(Header<AlignedU16>);
#[repr(C, align(8))]
struct AlignedEmptyU32(Header<AlignedU32>);
#[repr(C, align(16))]
struct AlignedEmptyU64(Header<AlignedU64>);

static EMPTY_ALIGNED_U8: AlignedEmptyU8 = AlignedEmptyU8(Header::for_capacity(AlignedU8(0)));
static EMPTY_ALIGNED_U16: AlignedEmptyU16 = AlignedEmptyU16(Header::for_capacity(AlignedU16(0)));
static EMPTY_ALIGNED_U32: AlignedEmptyU32 = AlignedEmptyU32(Header::for_capacity(AlignedU32(0)));
static EMPTY_ALIGNED_U64: AlignedEmptyU64 = AlignedEmptyU64(Header::for_capacity(AlignedU64(0)));

impl VecCapacity for AlignedU8 {
    const MAX: Self = AlignedU8(u8::MAX);
    const ZERO: Self = AlignedU8(0);
    const TYPE_NAME: &'static str = "8-bit (aligned)";
    const SINGLETON_ALIGN: usize = std::mem::size_of::<Header<Self>>();
    const EMPTY_HEADER: &'static Header<Self> = &EMPTY_ALIGNED_U8.0;

    #[inline]
    fn from_usize(val: usize) -> Self {
        if val > u8::MAX as usize {
            panic!(
                "ThinVec size may not exceed the capacity of an {} sized int",
                Self::TYPE_NAME
            );
        }
        AlignedU8(val as u8)
    }

    #[inline]
    fn to_usize(self) -> usize {
        self.0 as usize
    }
}

impl VecCapacity for AlignedU16 {
    const MAX: Self = AlignedU16(u16::MAX);
    const ZERO: Self = AlignedU16(0);
    const TYPE_NAME: &'static str = "16-bit (aligned)";
    const SINGLETON_ALIGN: usize = std::mem::size_of::<Header<Self>>();
    const EMPTY_HEADER: &'static Header<Self> = &EMPTY_ALIGNED_U16.0;

    #[inline]
    fn from_usize(val: usize) -> Self {
        if val > u16::MAX as usize {
            panic!(
                "ThinVec size may not exceed the capacity of a {} sized int",
                Self::TYPE_NAME
            );
        }
        AlignedU16(val as u16)
    }

    #[inline]
    fn to_usize(self) -> usize {
        self.0 as usize
    }
}

impl VecCapacity for AlignedU32 {
    const MAX: Self = AlignedU32(u32::MAX);
    const ZERO: Self = AlignedU32(0);
    const TYPE_NAME: &'static str = "32-bit (aligned)";
    const SINGLETON_ALIGN: usize = std::mem::size_of::<Header<Self>>();
    const EMPTY_HEADER: &'static Header<Self> = &EMPTY_ALIGNED_U32.0;

    #[inline]
    fn from_usize(val: usize) -> Self {
        if val > u32::MAX as usize {
            panic!(
                "ThinVec size may not exceed the capacity of a {} sized int",
                Self::TYPE_NAME
            );
        }
        AlignedU32(val as u32)
    }

    #[inline]
    fn to_usize(self) -> usize {
        self.0 as usize
    }
}

impl VecCapacity for AlignedU64 {
    const MAX: Self = AlignedU64(u64::MAX);
    const ZERO: Self = AlignedU64(0);
    const TYPE_NAME: &'static str = "64-bit (aligned)";
    const SINGLETON_ALIGN: usize = std::mem::size_of::<Header<Self>>();
    const EMPTY_HEADER: &'static Header<Self> = &EMPTY_ALIGNED_U64.0;

    #[inline]
    fn from_usize(val: usize) -> Self {
        // On 64-bit platforms, usize::MAX == u64::MAX, so no overflow check needed.
        // On 32-bit platforms, usize fits in u64.
        AlignedU64(val as u64)
    }

    #[inline]
    fn to_usize(self) -> usize {
        self.0 as usize
    }
}
