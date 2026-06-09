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

    /// The guaranteed alignment of the storage backing [`Self::EMPTY_HEADER`].
    ///
    /// One-past-the-end of the empty-header singleton is a validly-aligned data
    /// pointer for any element type `T` whose alignment does not exceed
    /// `SINGLETON_ALIGN` (provided no header padding is needed, i.e.
    /// `header_field_padding::<T, Self>() == 0`). This lets
    /// [`ThinVec::data_raw`](crate::ThinVec) elide its per-access empty-singleton
    /// alignment guard for such types (see
    /// [`data_ptr_guard_elided`](crate::data_ptr_guard_elided)).
    ///
    /// The primitive capacity types (`u8`/`u16`/`u32`/`u64`) use their *natural*
    /// header alignment (`align_of::<Header<Self>>()`), so they are unaligned by
    /// default. The opt-in [`AlignedU8`]/[`AlignedU16`]/[`AlignedU32`]/[`AlignedU64`]
    /// newtypes instead over-align the singleton to the full header size
    /// (`size_of::<Header<Self>>()`), widening the set of `T` for which the guard
    /// can be elided — useful for e.g. a `u32`-capacity vector of 8-byte-aligned
    /// records, whose natural header alignment is only 4.
    const SINGLETON_ALIGN: usize;

    /// Reference to a static empty header for this size type.
    const EMPTY_HEADER: &'static Header<Self>;

    /// Convert from usize, panicking if out of range.
    fn from_usize(val: usize) -> Self;

    /// Convert to usize.
    fn to_usize(self) -> usize;
}

// The empty-header singletons for the primitive capacity types use their
// natural alignment (`align_of::<Header<S>>()`), matching the unaligned-by-default
// behaviour. The opt-in `AlignedU*` newtypes (defined further down) instead back
// their singletons with over-aligned wrapper structs.
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

// Opt-in, over-aligned capacity types.
//
// Each is a `#[repr(transparent)]` newtype over the matching primitive, so
// `Header<AlignedU*>` is byte-identical to `Header<u*>` and switching a vector
// between them is ABI-compatible. The difference is that their empty-header
// singleton is backed by an over-aligned wrapper struct (aligned to the full
// header size, not just the natural header alignment), and `SINGLETON_ALIGN`
// reports that larger alignment. This lets [`ThinVec::data_raw`](crate::ThinVec)
// elide its empty-singleton alignment guard for more element types — see
// [`data_ptr_guard_elided`](crate::data_ptr_guard_elided).
//
// The `align` literal on each wrapper must equal `size_of::<Header<S>>()`
// (`2 * size_of::<S>`); the `aligned_singleton_alignment_matches_header_size`
// test enforces this.

/// A `u8` capacity type whose empty-header singleton is over-aligned to the full
/// header size, so [`data_ptr_guard_elided`](crate::data_ptr_guard_elided) holds
/// for more element types than plain `u8`.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct AlignedU8(u8);

/// A `u16` capacity type whose empty-header singleton is over-aligned to the full
/// header size, so [`data_ptr_guard_elided`](crate::data_ptr_guard_elided) holds
/// for more element types than plain `u16`.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct AlignedU16(u16);

/// A `u32` capacity type whose empty-header singleton is over-aligned to the full
/// header size, so [`data_ptr_guard_elided`](crate::data_ptr_guard_elided) holds
/// for more element types than plain `u32` — notably 8-byte-aligned records,
/// whose natural `u32` header alignment is only 4.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct AlignedU32(u32);

/// A `u64` capacity type whose empty-header singleton is over-aligned to the full
/// header size, so [`data_ptr_guard_elided`](crate::data_ptr_guard_elided) holds
/// for more element types than plain `u64` (e.g. 16-byte-aligned records).
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::{align_of, size_of};

    /// The runtime address of the empty-header singleton must honour
    /// `SINGLETON_ALIGN`, so that one-past-the-end (`addr + header_size`) is a
    /// validly-aligned data pointer for the element types whose guard
    /// `data_ptr_guard_elided` elides. If this drifts, the guard could be
    /// wrongly elided, producing a misaligned data pointer. (`align_of_val`
    /// can't be used here: it reports the *pointee type's* declared alignment,
    /// erasing any `#[repr(align(N))]` wrapper.)
    fn check_runtime_alignment<S: VecCapacity>() {
        let addr = S::EMPTY_HEADER as *const Header<S> as usize;
        assert!(
            addr.is_multiple_of(S::SINGLETON_ALIGN),
            "empty-header singleton for {} is not aligned to SINGLETON_ALIGN ({}) at runtime",
            S::TYPE_NAME,
            S::SINGLETON_ALIGN,
        );
    }

    /// The primitive capacity types are unaligned by default: their singleton
    /// uses the *natural* header alignment.
    fn check_natural<S: VecCapacity>() {
        assert_eq!(
            S::SINGLETON_ALIGN,
            align_of::<Header<S>>(),
            "SINGLETON_ALIGN ({}) must equal align_of::<Header<{}>>() ({})",
            S::SINGLETON_ALIGN,
            S::TYPE_NAME,
            align_of::<Header<S>>(),
        );
        check_runtime_alignment::<S>();
    }

    /// The opt-in `AlignedU*` types over-align their singleton to the full
    /// header size.
    fn check_aligned<S: VecCapacity>() {
        assert_eq!(
            S::SINGLETON_ALIGN,
            size_of::<Header<S>>(),
            "SINGLETON_ALIGN ({}) must equal size_of::<Header<{}>>() ({})",
            S::SINGLETON_ALIGN,
            S::TYPE_NAME,
            size_of::<Header<S>>(),
        );
        check_runtime_alignment::<S>();
    }

    #[test]
    fn primitive_singleton_uses_natural_alignment() {
        check_natural::<u8>();
        check_natural::<u16>();
        check_natural::<u32>();
        check_natural::<u64>();
    }

    #[test]
    fn aligned_singleton_alignment_matches_header_size() {
        check_aligned::<AlignedU8>();
        check_aligned::<AlignedU16>();
        check_aligned::<AlignedU32>();
        check_aligned::<AlignedU64>();
    }

    /// Pins the opt-in semantics: for an 8-byte-aligned element, plain `u32`
    /// keeps the empty-singleton guard (natural header alignment is only 4),
    /// while `AlignedU32` over-aligns the singleton and elides it.
    #[test]
    fn aligned_variant_widens_guard_elision() {
        use crate::data_ptr_guard_elided;

        assert!(!data_ptr_guard_elided::<[u64; 1], u32>());
        assert!(data_ptr_guard_elided::<[u64; 1], AlignedU32>());
    }
}
