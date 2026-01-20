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
//! - Pluggable hash functions via the [`hash32::Hasher`] trait
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

impl Hasher for HllHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0.finish()
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        self.0.write(bytes);
    }
}

impl hash32::Hasher for HllHasher {
    #[inline]
    fn finish32(&self) -> u32 {
        self.0.finish() as u32
    }
}

/// Murmur3 hasher with good hash distribution.
///
/// This hasher provides better avalanche properties than FNV-1a,
/// especially for sequential or structured data like integers.
pub type Murmur3Hasher = hash32::Murmur3Hasher;

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
/// - `H`: The hasher type implementing [`hash32::Hasher`].
///
/// # Why Two Const Parameters?
///
/// Ideally, `SIZE` would be computed as `1 << BITS` automatically. However,
/// Rust's const generics on stable do not yet support "generic const expressions"
/// (the `generic_const_exprs` feature). This means we cannot write:
///
/// ```ignore
/// pub struct Hll<const BITS: u8, H: hash32::Hasher = HllHasher> {
///     registers: Box<[u8; 1 << BITS]>,  // ERROR: not allowed on stable
/// }
/// ```
///
/// Until this feature stabilizes, we require both `BITS` and `SIZE` as separate
/// parameters, with a compile-time assertion ensuring `SIZE == 1 << BITS`.
/// The type aliases (e.g., [`Hll12`]) hide this complexity for common configurations.
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
pub struct Hll<const BITS: u8, const SIZE: usize, H: hash32::Hasher + Default = HllHasher> {
    cached_card: Cell<Option<usize>>,
    registers: Box<[u8; SIZE]>,
    _hasher: PhantomData<H>,
}

impl<const BITS: u8, const SIZE: usize, H: hash32::Hasher + Default> Hll<BITS, SIZE, H> {
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

impl<const BITS: u8, const SIZE: usize, H: hash32::Hasher + Default> Default for Hll<BITS, SIZE, H> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const BITS: u8, const SIZE: usize, H: hash32::Hasher + Default> Clone for Hll<BITS, SIZE, H> {
    fn clone(&self) -> Self {
        Self {
            cached_card: self.cached_card.clone(),
            registers: self.registers.clone(),
            _hasher: PhantomData,
        }
    }
}

impl<const BITS: u8, const SIZE: usize, H: hash32::Hasher + Default> std::fmt::Debug
    for Hll<BITS, SIZE, H>
{
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

    #[test]
    fn test_rank_function() {
        assert_eq!(rank(0, 20), 21); // 0 has 32 trailing zeros, capped to 20, +1
        assert_eq!(rank(1, 20), 1); // 1 has 0 trailing zeros, +1
        assert_eq!(rank(2, 20), 2); // 2 has 1 trailing zero, +1
        assert_eq!(rank(4, 20), 3); // 4 has 2 trailing zeros, +1
        assert_eq!(rank(0b1000, 20), 4); // 8 has 3 trailing zeros, +1
    }
}
