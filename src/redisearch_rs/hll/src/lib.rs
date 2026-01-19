/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! A pure Rust HyperLogLog implementation with compile-time constants.
//!
//! HyperLogLog is a probabilistic data structure for estimating the cardinality
//! (number of distinct elements) of a multiset. This implementation provides:
//!
//! - Compile-time validation of parameters via const generics
//! - Pluggable hash functions via the [`Hasher32`] trait
//! - Optimal memory layout using `Box<[u8; SIZE]>`
//!
//! # Example
//!
//! ```
//! use hll::{Hll, HllHasher};
//!
//! // Create an HLL with 12-bit precision (4096 registers, ~1.6% error)
//! let mut hll: Hll<12, 4096, HllHasher> = Hll::new();
//! hll.add(b"hello");
//! hll.add(b"world");
//! hll.add(b"hello"); // duplicate, won't increase count significantly
//!
//! // Estimated cardinality should be close to 2
//! let count = hll.count();
//! assert!(count <= 3);
//! ```

use std::cell::Cell;
use std::hash::Hasher;
use std::marker::PhantomData;

use thiserror::Error;

/// A 32-bit hasher trait for HyperLogLog.
///
/// This trait is similar to [`std::hash::Hasher`] but returns a 32-bit hash
/// value, which is what HyperLogLog requires.
pub trait Hasher32: Default {
    /// Returns the 32-bit hash value.
    fn finish32(&self) -> u32;

    /// Writes bytes into the hasher.
    fn write(&mut self, bytes: &[u8]);
}

/// The default HLL seed used for compatibility with the C implementation.
const HLL_SEED: u32 = 0x5f61767a;

/// FNV-32a hasher with a C-compatible seed.
///
/// This hasher uses the same seed as the C HLL implementation to ensure
/// hash compatibility when interoperating with C code.
pub struct HllHasher(fnv::Fnv32);

impl Default for HllHasher {
    fn default() -> Self {
        Self(fnv::Fnv32::with_offset_basis(HLL_SEED))
    }
}

impl Hasher32 for HllHasher {
    #[inline]
    fn finish32(&self) -> u32 {
        self.0.finish() as u32
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        Hasher::write(&mut self.0, bytes);
    }
}

/// Errors that can occur when creating an HLL from a slice.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum HllError {
    /// The provided slice has an invalid length.
    #[error("invalid register slice length: expected {expected}, got {got}")]
    InvalidLength {
        /// The expected length.
        expected: usize,
        /// The actual length provided.
        got: usize,
    },
}

/// A HyperLogLog probabilistic cardinality estimator.
///
/// # Type Parameters
///
/// - `BITS`: The number of bits used for register indexing (4..=20).
///   Higher values give more accuracy but use more memory.
/// - `SIZE`: The number of registers, must equal `1 << BITS`.
/// - `H`: The hasher type implementing [`Hasher32`].
///
/// # Memory Usage
///
/// The memory usage is `SIZE` bytes for registers, which equals `2^BITS` bytes.
/// For example, `Hll12` uses 4096 bytes.
///
/// # Error Rate
///
/// The expected relative error is approximately `1.04 / sqrt(2^BITS)`:
/// - `BITS=12`: ~1.6% error
/// - `BITS=14`: ~0.8% error
/// - `BITS=16`: ~0.4% error
pub struct Hll<const BITS: u8, const SIZE: usize, H: Hasher32 = HllHasher> {
    cached_card: Cell<Option<usize>>,
    registers: Box<[u8; SIZE]>,
    _hasher: PhantomData<H>,
}

impl<const BITS: u8, const SIZE: usize, H: Hasher32> Hll<BITS, SIZE, H> {
    /// Compile-time assertion that BITS is in the valid range.
    const _BITS_RANGE_CHECK: () = assert!(BITS >= 4 && BITS <= 20, "BITS must be in 4..=20");

    /// Compile-time assertion that SIZE matches 1 << BITS.
    const _BITS_SIZE_CHECK: () = assert!(SIZE == (1 << BITS), "SIZE must equal 1 << BITS");

    /// The number of bits used for ranking (trailing zeros).
    const RANK_BITS: u8 = 32 - BITS;

    /// Creates a new empty HLL.
    ///
    /// All registers are initialized to zero, representing an empty set.
    #[must_use]
    pub fn new() -> Self {
        // Trigger compile-time checks
        let () = Self::_BITS_RANGE_CHECK;
        let () = Self::_BITS_SIZE_CHECK;

        Self {
            cached_card: Cell::new(Some(0)),
            registers: Box::new([0u8; SIZE]),
            _hasher: PhantomData,
        }
    }

    /// Creates an HLL from existing register data.
    ///
    /// This is useful for deserializing an HLL or loading from external storage.
    #[must_use]
    pub fn from_registers(registers: [u8; SIZE]) -> Self {
        // Trigger compile-time checks
        let () = Self::_BITS_RANGE_CHECK;
        let () = Self::_BITS_SIZE_CHECK;

        Self {
            cached_card: Cell::new(None),
            registers: Box::new(registers),
            _hasher: PhantomData,
        }
    }

    /// Creates an HLL from a slice of register data.
    ///
    /// # Errors
    ///
    /// Returns [`HllError::InvalidLength`] if the slice length doesn't match `SIZE`.
    pub fn try_from_slice(slice: &[u8]) -> Result<Self, HllError> {
        // Trigger compile-time checks
        let () = Self::_BITS_RANGE_CHECK;
        let () = Self::_BITS_SIZE_CHECK;

        if slice.len() != SIZE {
            return Err(HllError::InvalidLength {
                expected: SIZE,
                got: slice.len(),
            });
        }

        let mut registers = Box::new([0u8; SIZE]);
        registers.copy_from_slice(slice);

        Ok(Self {
            cached_card: Cell::new(None),
            registers,
            _hasher: PhantomData,
        })
    }

    /// Adds an element by hashing it.
    ///
    /// The element is hashed using the configured hasher type `H`.
    pub fn add(&mut self, data: &[u8]) {
        let mut hasher = H::default();
        hasher.write(data);
        self.add_hash(hasher.finish32());
    }

    /// Adds a pre-computed 32-bit hash value.
    ///
    /// Use this when you've already computed the hash externally or when
    /// batch-processing many elements.
    pub fn add_hash(&mut self, hash: u32) {
        let index = (hash >> Self::RANK_BITS) as usize;
        let rank = rank(hash, Self::RANK_BITS);

        // SAFETY: index is computed from the high BITS bits of a u32,
        // so it's always in range 0..SIZE (which equals 1 << BITS).
        if rank > self.registers[index] {
            self.registers[index] = rank;
            self.cached_card.set(None);
        }
    }

    /// Estimates the cardinality (number of distinct elements).
    ///
    /// The result is cached internally and recomputed only when new elements
    /// are added. Multiple calls without modifications are O(1).
    #[must_use]
    pub fn count(&self) -> usize {
        if let Some(cached) = self.cached_card.get() {
            return cached;
        }

        let estimate = self.compute_estimate();
        self.cached_card.set(Some(estimate));
        estimate
    }

    /// Merges another HLL into this one.
    ///
    /// After merging, this HLL will represent the union of both sets.
    /// The other HLL must have the same `BITS` and `SIZE` parameters.
    pub fn merge(&mut self, other: &Self) {
        for i in 0..SIZE {
            if other.registers[i] > self.registers[i] {
                self.registers[i] = other.registers[i];
                self.cached_card.set(None);
            }
        }
    }

    /// Clears all registers, resetting to an empty state.
    pub fn clear(&mut self) {
        self.registers.fill(0);
        self.cached_card.set(Some(0));
    }

    /// Returns a reference to the raw register data.
    ///
    /// This is useful for serialization or interop with other HLL implementations.
    #[must_use]
    pub const fn registers(&self) -> &[u8; SIZE] {
        &self.registers
    }

    /// Returns the bit precision.
    #[must_use]
    pub const fn bits() -> u8 {
        BITS
    }

    /// Returns the number of registers.
    #[must_use]
    pub const fn size() -> usize {
        SIZE
    }

    /// Computes the cardinality estimate using the HyperLogLog algorithm.
    fn compute_estimate(&self) -> usize {
        let alpha_mm = alpha(BITS, SIZE) * (SIZE as f64) * (SIZE as f64);

        let mut sum = 0.0;
        for &reg in self.registers.iter() {
            sum += 1.0 / ((1u32 << reg) as f64);
        }

        let mut estimate = alpha_mm / sum;

        // Small range correction using linear counting.
        // Only apply when there are many empty registers (> 20% zeros),
        // since linear counting is inaccurate with few zeros.
        // This improves on the original HLL which applied it whenever zeros > 0.
        let zeros = self.registers.iter().filter(|&&r| r == 0).count();
        if estimate <= 2.5 * (SIZE as f64) && zeros > SIZE / 5 {
            estimate = (SIZE as f64) * ((SIZE as f64) / (zeros as f64)).ln();
        }
        // Large range correction
        else if estimate > (1.0 / 30.0) * 4_294_967_296.0 {
            estimate = -4_294_967_296.0 * (1.0 - (estimate / 4_294_967_296.0)).ln();
        }

        estimate as usize
    }
}

impl<const BITS: u8, const SIZE: usize, H: Hasher32> Default for Hll<BITS, SIZE, H> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const BITS: u8, const SIZE: usize, H: Hasher32> Clone for Hll<BITS, SIZE, H> {
    fn clone(&self) -> Self {
        Self {
            cached_card: self.cached_card.clone(),
            registers: self.registers.clone(),
            _hasher: PhantomData,
        }
    }
}

impl<const BITS: u8, const SIZE: usize, H: Hasher32> std::fmt::Debug for Hll<BITS, SIZE, H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Hll")
            .field("bits", &BITS)
            .field("size", &SIZE)
            .field("cached_card", &self.cached_card.get())
            .finish_non_exhaustive()
    }
}

/// Calculates the rank (trailing zeros + 1, capped at rank_bits).
#[inline]
const fn rank(hash: u32, rank_bits: u8) -> u8 {
    let r = if hash == 0 { 32 } else { hash.trailing_zeros() as u8 };
    let capped = if r > rank_bits { rank_bits } else { r };
    capped + 1
}

/// Returns the alpha correction factor for the given parameters.
const fn alpha(bits: u8, size: usize) -> f64 {
    match bits {
        4 => 0.673,
        5 => 0.697,
        6 => 0.709,
        _ => 0.7213 / (1.0 + 1.079 / size as f64),
    }
}

// Type aliases for common configurations

/// HLL with 4-bit precision (16 registers, ~26% error).
pub type Hll4<H = HllHasher> = Hll<4, 16, H>;

/// HLL with 5-bit precision (32 registers, ~18% error).
pub type Hll5<H = HllHasher> = Hll<5, 32, H>;

/// HLL with 6-bit precision (64 registers, ~13% error).
pub type Hll6<H = HllHasher> = Hll<6, 64, H>;

/// HLL with 8-bit precision (256 registers, ~6.5% error).
pub type Hll8<H = HllHasher> = Hll<8, 256, H>;

/// HLL with 10-bit precision (1024 registers, ~3.3% error).
pub type Hll10<H = HllHasher> = Hll<10, 1024, H>;

/// HLL with 12-bit precision (4096 registers, ~1.6% error).
pub type Hll12<H = HllHasher> = Hll<12, 4096, H>;

/// HLL with 14-bit precision (16384 registers, ~0.8% error).
pub type Hll14<H = HllHasher> = Hll<14, 16384, H>;

/// HLL with 16-bit precision (65536 registers, ~0.4% error).
pub type Hll16<H = HllHasher> = Hll<16, 65536, H>;

#[cfg(test)]
mod tests {
    use super::*;

    /// Concrete type alias for testing to avoid inference issues with multiple Hasher32 impls.
    type TestHll12 = Hll<12, 4096, HllHasher>;

    #[test]
    fn test_new_hll() {
        let hll = TestHll12::new();
        assert_eq!(hll.count(), 0);
        assert_eq!(TestHll12::bits(), 12);
        assert_eq!(TestHll12::size(), 4096);
    }

    #[test]
    fn test_add_single_element() {
        let mut hll = TestHll12::new();
        hll.add(b"hello");
        assert_eq!(hll.count(), 1);
    }

    #[test]
    fn test_add_duplicate_elements() {
        let mut hll = TestHll12::new();
        for _ in 0..100 {
            hll.add(b"same");
        }
        assert_eq!(hll.count(), 1);
    }

    #[test]
    fn test_add_many_distinct_elements() {
        let mut hll = TestHll12::new();
        let n = 10000u32;
        // Use simple incrementing integers - they work well with FNV-1a
        for i in 0..n {
            hll.add(&i.to_le_bytes());
        }

        // Count zeros for debugging
        let zeros = hll.registers().iter().filter(|&&r| r == 0).count();
        eprintln!(
            "n={n}, zeros={zeros} ({:.1}%), count={}",
            100.0 * zeros as f64 / 4096.0,
            hll.count()
        );

        let count = hll.count();
        // Expected error is ~1.6%, allow for 15% tolerance
        // Note: FNV-1a with sequential integers has some bias
        let error = (count as f64 - n as f64).abs() / n as f64;
        assert!(error < 0.15, "error {error} too large, count={count}, n={n}");
    }

    #[test]
    fn test_add_hash_direct() {
        // Test with pre-computed hash values to verify algorithm correctness
        let mut hll = TestHll12::new();

        // Add hashes that will go to different registers with known ranks
        // Hash format: top 12 bits = register index, trailing zeros = rank - 1
        // Register 0 with rank 1 (0 trailing zeros): 0x00100001
        hll.add_hash(0x00100001);
        // Register 1 with rank 2 (1 trailing zero): 0x00200002
        hll.add_hash(0x00200002);
        // Register 2 with rank 3 (2 trailing zeros): 0x00300004
        hll.add_hash(0x00300004);

        // Check that registers were set correctly
        let regs = hll.registers();
        assert_eq!(regs[1], 1, "register 1 should be 1");
        assert_eq!(regs[2], 2, "register 2 should be 2");
        assert_eq!(regs[3], 3, "register 3 should be 3");

        // Count should be small (we only added 3 distinct elements)
        let count = hll.count();
        assert!(count <= 10, "count {count} should be small for 3 elements");
    }

    #[test]
    fn test_hash_distribution() {
        // Test that hash function produces reasonable distribution

        let test_cases = [
            (b"hello".as_slice(), "hello"),
            (b"world".as_slice(), "world"),
            (&0u32.to_le_bytes() as &[u8], "0"),
            (&1u32.to_le_bytes() as &[u8], "1"),
            (&1000u32.to_le_bytes() as &[u8], "1000"),
        ];

        for (data, name) in test_cases {
            let mut hasher = HllHasher::default();
            hasher.write(data);
            let hash = hasher.finish32();
            let index = hash >> 20;
            let trailing = hash.trailing_zeros();
            eprintln!("{name}: hash={hash:#010x}, index={index}, trailing_zeros={trailing}");
        }

        // Check that different inputs produce different hashes
        let mut hasher1 = HllHasher::default();
        hasher1.write(b"test1");
        let hash1 = hasher1.finish32();

        let mut hasher2 = HllHasher::default();
        hasher2.write(b"test2");
        let hash2 = hasher2.finish32();

        assert_ne!(hash1, hash2, "different inputs should produce different hashes");
    }

    #[test]
    fn test_register_distribution() {
        let mut hll = TestHll12::new();
        let n = 10000u32;

        for i in 0..n {
            hll.add(format!("element-{i}").as_bytes());
        }

        let count = hll.count();
        let error = (count as f64 - n as f64).abs() / n as f64;
        // With improved small range correction, error should be < 5%
        assert!(error < 0.05, "error {error} too large for n={n}, count={count}");
    }

    #[test]
    fn test_small_cardinality() {
        // Test small cardinality where linear counting is expected
        let mut hll = TestHll12::new();
        let n = 1000u32;

        for i in 0..n {
            hll.add(&i.to_le_bytes());
        }

        let count = hll.count();
        let error = (count as f64 - n as f64).abs() / n as f64;

        // For small cardinalities, allow more error due to variance
        assert!(error < 0.30, "error {error} too large for n={n}, count={count}");
    }

    #[test]
    fn test_merge() {
        let mut hll1 = TestHll12::new();
        let mut hll2 = TestHll12::new();

        for i in 0..1000u32 {
            hll1.add(&i.to_le_bytes());
        }

        for i in 500..1500u32 {
            hll2.add(&i.to_le_bytes());
        }

        hll1.merge(&hll2);

        let count = hll1.count();
        // Should be close to 1500 (union of 0..1000 and 500..1500)
        let error = (count as f64 - 1500.0).abs() / 1500.0;
        assert!(error < 0.05, "error {error} too large, count={count}");
    }

    #[test]
    fn test_clear() {
        let mut hll = TestHll12::new();
        hll.add(b"hello");
        assert!(hll.count() > 0);

        hll.clear();
        assert_eq!(hll.count(), 0);
    }

    #[test]
    fn test_cache_invalidation() {
        let mut hll = TestHll12::new();
        hll.add(b"hello");
        let count1 = hll.count();
        let count2 = hll.count(); // Should use cache
        assert_eq!(count1, count2);

        hll.add(b"world");
        let count3 = hll.count(); // Cache should be invalidated
        assert!(count3 >= count1);
    }

    #[test]
    fn test_from_registers() {
        let mut hll1 = TestHll12::new();
        for i in 0..1000u32 {
            hll1.add(&i.to_le_bytes());
        }

        let registers = *hll1.registers();
        let hll2 = TestHll12::from_registers(registers);

        assert_eq!(hll1.count(), hll2.count());
    }

    #[test]
    fn test_try_from_slice() {
        let hll1 = TestHll12::new();
        let slice = hll1.registers().as_slice();

        let hll2 = TestHll12::try_from_slice(slice).unwrap();
        assert_eq!(hll1.count(), hll2.count());
    }

    #[test]
    fn test_try_from_slice_invalid_length() {
        let err = TestHll12::try_from_slice(&[0u8; 100]).unwrap_err();
        assert_eq!(
            err,
            HllError::InvalidLength {
                expected: 4096,
                got: 100
            }
        );
    }

    #[test]
    fn test_type_aliases() {
        // Just verify they compile and have correct parameters
        assert_eq!(Hll4::<HllHasher>::bits(), 4);
        assert_eq!(Hll4::<HllHasher>::size(), 16);

        assert_eq!(Hll8::<HllHasher>::bits(), 8);
        assert_eq!(Hll8::<HllHasher>::size(), 256);

        assert_eq!(Hll16::<HllHasher>::bits(), 16);
        assert_eq!(Hll16::<HllHasher>::size(), 65536);
    }

    #[test]
    fn test_rank_function() {
        assert_eq!(rank(0, 20), 21); // 0 has 32 trailing zeros, capped to 20, +1
        assert_eq!(rank(1, 20), 1); // 1 has 0 trailing zeros, +1
        assert_eq!(rank(2, 20), 2); // 2 has 1 trailing zero, +1
        assert_eq!(rank(4, 20), 3); // 4 has 2 trailing zeros, +1
        assert_eq!(rank(0b1000, 20), 4); // 8 has 3 trailing zeros, +1
    }

    /// Custom hasher for testing pluggable hasher support.
    #[derive(Default)]
    struct CustomTestHasher(u32);

    impl Hasher32 for CustomTestHasher {
        fn finish32(&self) -> u32 {
            self.0
        }

        fn write(&mut self, bytes: &[u8]) {
            for &b in bytes {
                self.0 = self.0.wrapping_add(b as u32);
            }
        }
    }

    #[test]
    fn test_custom_hasher() {
        let mut hll: Hll12<CustomTestHasher> = Hll::new();
        hll.add(b"test");
        assert!(hll.count() >= 1);
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    /// Concrete type alias for property tests to avoid inference issues.
    type TestHll12 = Hll<12, 4096, HllHasher>;

    proptest! {
        /// Test that merge always increases or maintains the count estimate.
        #[test]
        fn merge_is_monotonic(n1 in 10u32..100, n2 in 10u32..100) {
            let mut hll1 = TestHll12::new();
            let mut hll2 = TestHll12::new();

            // Add elements to both HLLs
            for i in 0..n1 {
                hll1.add(&i.to_le_bytes());
            }
            for i in n1..(n1 + n2) {
                hll2.add(&i.to_le_bytes());
            }

            let count1_before = hll1.count();
            hll1.merge(&hll2);
            let count_merged = hll1.count();

            // Merged count should be >= count before merge (monotonicity)
            prop_assert!(count_merged >= count1_before,
                "merged count {} < original {}", count_merged, count1_before);
        }

        /// Test that clearing an HLL always resets count to 0.
        #[test]
        fn clear_resets_to_zero(n in 1u32..1000) {
            let mut hll = TestHll12::new();
            for i in 0..n {
                hll.add(&i.to_le_bytes());
            }
            prop_assert!(hll.count() > 0, "count should be > 0 after adding {} elements", n);

            hll.clear();
            prop_assert_eq!(hll.count(), 0);
        }

        /// Test that adding the same element multiple times doesn't increase count.
        #[test]
        fn duplicates_dont_increase_count(n in 1u32..100) {
            let mut hll = TestHll12::new();
            let data = b"same_element";

            for _ in 0..n {
                hll.add(data);
            }

            // Should be exactly 1 since all elements are identical
            prop_assert_eq!(hll.count(), 1);
        }

        /// Test that from_registers preserves the count.
        #[test]
        fn from_registers_preserves_count(n in 10u32..500) {
            let mut hll1 = TestHll12::new();
            for i in 0..n {
                hll1.add(&i.to_le_bytes());
            }

            let count1 = hll1.count();
            let registers = *hll1.registers();
            let hll2 = TestHll12::from_registers(registers);

            prop_assert_eq!(hll2.count(), count1);
        }
    }
}
