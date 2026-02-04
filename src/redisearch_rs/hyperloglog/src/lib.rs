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
//! - Optimal memory layout using `[u8; SIZE]`
//!
//! # Example
//!
//! ```
//! use hyperloglog::{HyperLogLog10, CFnvHasher};
//!
//! // Create an instance with 10-bit precision (1024 registers, ~3.2% error)
//! let mut hll: HyperLogLog10<CFnvHasher> = HyperLogLog10::new();
//! hll.add(b"hello");
//! hll.add(b"world");
//! let count = hll.count();
//! hll.add(b"hello"); // duplicate, won't increase count
//! assert_eq!(count, hll.count());
//!
//! // Estimated cardinality should be close to 2
//! let count = hll.count();
//! assert!(count <= 3);
//! ```
//!
//! # Implementation details
//!
//! The registers are stored on the stack, with an upper bound of 1KB (i.e. 10-bit precision).

use std::cell::Cell;
use std::marker::PhantomData;
use thiserror::Error;

mod fnv;

pub use fnv::CFnvHasher;

/// Murmur3 hasher with good hash distribution.
///
/// This hasher provides better avalanche properties than FNV-1a,
/// especially for sequential or structured data like integers.
pub type Murmur3Hasher = hash32::Murmur3Hasher;

/// Errors that can occur when creating an [`HyperLogLog`] instance from a slice.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("Invalid register slice length: expected {EXPECTED}, got {got}")]
pub struct InvalidBufferLength<const EXPECTED: usize> {
    /// The actual length provided.
    pub got: usize,
}

/// A HyperLogLog probabilistic cardinality estimator.
///
/// # Type Parameters
///
/// - `BITS`: The number of bits used for register indexing (4..=10).
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
/// pub struct HyperLogLog<const BITS: u8, H: hash32::Hasher = HllHasher> {
///     registers: Box<[u8; 1 << BITS]>,  // ERROR: not allowed on stable
/// }
/// ```
///
/// Until this feature stabilizes, we require both `BITS` and `SIZE` as separate
/// parameters, with a compile-time assertion ensuring `SIZE == 1 << BITS`.
/// The type aliases (e.g., [`HyperLogLog10`]) hide this complexity for common configurations.
///
/// # Memory Usage
///
/// The memory usage is `SIZE` bytes for registers, which equals `2^BITS` bytes.
/// For example, `HyperLogLog10` uses 1024 bytes.
///
/// # Error Rate
///
/// The expected relative error is approximately `1.04 / sqrt(2^BITS)`:
/// - `BITS=6`: ~13% error
/// - `BITS=10`: ~3.3% error
pub struct HyperLogLog<const BITS: u8, const SIZE: usize, H: hash32::Hasher + Default = CFnvHasher>
{
    cached_card: Cell<Option<usize>>,
    registers: [u8; SIZE],
    _hasher: PhantomData<H>,
}

impl<const BITS: u8, const SIZE: usize, H: hash32::Hasher + Default> HyperLogLog<BITS, SIZE, H> {
    /// Compile-time assertion that BITS is in the valid range.
    /// We use 10 as the upper bound since it'd be counter-productive to have a struct on the stack
    /// that's bigger than 1KB.
    const _BITS_RANGE_CHECK: () = assert!(BITS >= 4 && BITS <= 10, "BITS must be in 4..=10");

    /// Compile-time assertion that SIZE matches 1 << BITS.
    const _BITS_SIZE_CHECK: () = assert!(SIZE == (1 << BITS), "SIZE must equal 1 << BITS");

    /// The number of bits used for ranking (trailing zeros).
    const RANK_BITS: u8 = 32 - BITS;

    /// Creates a new empty HLL.
    ///
    /// All registers are initialized to zero, representing an empty set.
    pub const fn new() -> Self {
        // Trigger compile-time checks
        let () = Self::_BITS_RANGE_CHECK;
        let () = Self::_BITS_SIZE_CHECK;

        Self {
            cached_card: Cell::new(Some(0)),
            registers: [0u8; SIZE],
            _hasher: PhantomData,
        }
    }

    /// Creates an HLL from existing register data.
    ///
    /// This is useful for deserializing an HLL or loading from external storage.
    pub fn from_registers(registers: [u8; SIZE]) -> Self {
        // Trigger compile-time checks
        let () = Self::_BITS_RANGE_CHECK;
        let () = Self::_BITS_SIZE_CHECK;

        #[cfg(debug_assertions)]
        Self::validate_register_values(registers.as_slice());

        Self {
            cached_card: Cell::new(None),
            registers,
            _hasher: PhantomData,
        }
    }

    /// Adds an element by hashing it.
    ///
    /// The element is hashed using the configured hasher type `H`.
    pub fn add<E>(&mut self, data: E)
    where
        E: std::hash::Hash,
    {
        let mut hasher = H::default();
        data.hash(&mut hasher);
        self.add_precomputed_hash(hasher.finish32());
    }

    /// Adds a pre-computed 32-bit hash value.
    ///
    /// Use this when you've already computed the hash externally or when
    /// batch-processing many elements.
    ///
    /// # Be Careful!
    ///
    /// To ensure the correctness of the HyperLogLog algorithm,
    /// the precomputed hash must have been produced by the same
    /// hashing function (`H`) used for all the other values.
    /// Using a different hashing function undermines the uniformity
    /// of the value distribution in the hashing space.
    pub fn add_precomputed_hash(&mut self, hash: u32) {
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
        let mut changed = false;
        for i in 0..SIZE {
            if other.registers[i] > self.registers[i] {
                self.registers[i] = other.registers[i];
                changed = true;
            }
        }
        if changed {
            self.cached_card.set(None);
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
    pub const fn registers(&self) -> &[u8; SIZE] {
        &self.registers
    }

    /// Sets the register data, replacing existing values.
    ///
    /// This invalidates any cached cardinality estimate.
    pub fn set_registers(&mut self, registers: [u8; SIZE]) {
        #[cfg(debug_assertions)]
        Self::validate_register_values(registers.as_slice());

        self.registers = registers;
        self.cached_card.set(None);
    }

    /// Try to set the register data from a slice whose length is not
    /// known at compile-time, replacing existing values.
    ///
    /// This invalidates any cached cardinality estimate.
    pub fn try_set_registers(&mut self, registers: &[u8]) -> Result<(), InvalidBufferLength<SIZE>> {
        if registers.len() != SIZE {
            return Err(InvalidBufferLength {
                got: registers.len(),
            });
        }

        #[cfg(debug_assertions)]
        Self::validate_register_values(registers);

        self.registers.copy_from_slice(registers);
        self.cached_card.set(None);
        Ok(())
    }

    /// Returns the bit precision.
    pub const fn bits() -> u8 {
        BITS
    }

    /// Returns the number of registers.
    pub const fn size() -> usize {
        SIZE
    }

    /// Computes the cardinality estimate using the HyperLogLog algorithm.
    fn compute_estimate(&self) -> usize {
        let alpha_mm = alpha(BITS, SIZE) * (SIZE as f64) * (SIZE as f64);

        let sum: f64 = self
            .registers
            .iter()
            .map(|&reg| 1.0 / ((1u32 << reg) as f64))
            .sum();

        let mut estimate = alpha_mm / sum;

        // Small range correction using linear counting.
        if estimate <= 2.5 * (SIZE as f64) {
            #[cfg(not(miri))]
            let zeros = bytecount::count(self.registers.as_slice(), 0);
            #[cfg(miri)]
            let zeros = self.registers.iter().filter(|&&reg| reg == 0).count();

            if zeros > 0 {
                estimate = (SIZE as f64) * ((SIZE as f64) / (zeros as f64)).ln();
            }
        }
        // Large range correction
        else if estimate > (1.0 / 30.0) * 4_294_967_296.0 {
            estimate = -4_294_967_296.0 * (1.0 - (estimate / 4_294_967_296.0)).ln();
        }

        estimate as usize
    }

    /// Panics if any of the register values exceeds the expected cap.
    fn validate_register_values(registers: &[u8]) {
        let max_value = Self::RANK_BITS + 1;
        for (i, entry) in registers.iter().enumerate() {
            assert!(
                *entry <= max_value,
                "The {i}th register values exceeds the expected cap. Got {entry}, expected at most {max_value}.",
            )
        }
    }
}

impl<const BITS: u8, const SIZE: usize, H: hash32::Hasher + Default> TryFrom<&[u8]>
    for HyperLogLog<BITS, SIZE, H>
{
    type Error = InvalidBufferLength<SIZE>;

    fn try_from(slice: &[u8]) -> Result<Self, Self::Error> {
        let mut self_ = Self::new();
        self_.try_set_registers(slice)?;
        Ok(self_)
    }
}

impl<const BITS: u8, const SIZE: usize, H: hash32::Hasher + Default> Default
    for HyperLogLog<BITS, SIZE, H>
{
    fn default() -> Self {
        Self::new()
    }
}

// Manual `Clone` implementation to avoid an unnecessary `Clone` bound on the
// generic hasher type parameter, `H`.
impl<const BITS: u8, const SIZE: usize, H: hash32::Hasher + Default> Clone
    for HyperLogLog<BITS, SIZE, H>
{
    fn clone(&self) -> Self {
        Self {
            cached_card: self.cached_card.clone(),
            registers: self.registers,
            _hasher: PhantomData,
        }
    }
}

impl<const BITS: u8, const SIZE: usize, H: hash32::Hasher + Default> std::fmt::Debug
    for HyperLogLog<BITS, SIZE, H>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HyperLogLog")
            .field("bits", &BITS)
            .field("size", &SIZE)
            .field("cached_cardinality", &self.cached_card.get())
            .finish_non_exhaustive()
    }
}

/// Calculates the rank (trailing zeros + 1, capped at rank_bits).
const fn rank(hash: u32, rank_bits: u8) -> u8 {
    let r = hash.trailing_zeros() as u8;
    // `std::cmp::min` is not yet `const` on the stable toolchain
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

/// HyperLogLog with 4-bit precision (16 registers, ~26% error).
pub type HyperLogLog4<H = CFnvHasher> = HyperLogLog<4, 16, H>;

/// HyperLogLog with 5-bit precision (32 registers, ~18% error).
pub type HyperLogLog5<H = CFnvHasher> = HyperLogLog<5, 32, H>;

/// HyperLogLog with 6-bit precision (64 registers, ~13% error).
pub type HyperLogLog6<H = CFnvHasher> = HyperLogLog<6, 64, H>;

/// HyperLogLog with 7-bit precision (128 registers, ~9.2% error).
pub type HyperLogLog7<H = CFnvHasher> = HyperLogLog<7, 128, H>;

/// HyperLogLog with 8-bit precision (256 registers, ~6.5% error).
pub type HyperLogLog8<H = CFnvHasher> = HyperLogLog<8, 256, H>;

/// HyperLogLog with 9-bit precision (512 registers, ~4.6% error).
pub type HyperLogLog9<H = CFnvHasher> = HyperLogLog<9, 512, H>;

/// HyperLogLog with 10-bit precision (1024 registers, ~3.3% error).
pub type HyperLogLog10<H = CFnvHasher> = HyperLogLog<10, 1024, H>;

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
