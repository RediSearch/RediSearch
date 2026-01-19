/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! HyperLogLog cardinality estimation implementation.
//!
//! HyperLogLog is a probabilistic data structure used to estimate the cardinality
//! (number of distinct elements) of a multiset. The expected error rate is
//! approximately `1.04 / sqrt(2^bits)` where `bits` is the precision parameter.
//!
//! # Example
//!
//! ```
//! use hll::HyperLogLog;
//!
//! let mut hll = HyperLogLog::new(6).unwrap(); // 6 bits = ~13% error rate
//! hll.add(&1u64.to_le_bytes());
//! hll.add(&2u64.to_le_bytes());
//! hll.add(&3u64.to_le_bytes());
//! // Duplicates don't affect the count
//! hll.add(&1u64.to_le_bytes());
//!
//! let count = hll.count();
//! // count is approximately 3
//! ```

use std::hash::Hasher;

use fnv::Fnv32;

/// Marker value indicating the cached cardinality needs to be recomputed.
const INVALID_CACHE_CARDINALITY: usize = usize::MAX;

/// The default offset basis for the FNV-1a hash function used by the C implementation.
/// This is the ASCII representation of "_avz" (0x5f = '_', 0x61 = 'a', 0x76 = 'v', 0x7a = 'z').
const HLL_FNV_OFFSET_BASIS: u32 = 0x5f61767a;

/// HyperLogLog probabilistic cardinality estimator.
///
/// This implementation uses FNV-1a hashing and supports precision bits
/// from 4 to 20 (inclusive), giving register counts from 16 to 1,048,576.
#[derive(Clone)]
pub struct HyperLogLog {
    /// Number of bits used for the register index. Valid range: 4-20.
    bits: u8,
    /// Number of bits used for the rank (32 - bits). Also the maximum rank value.
    rank_bits: u8,
    /// Number of registers (2^bits).
    size: u32,
    /// Cached cardinality from the last count operation.
    /// Set to [`INVALID_CACHE_CARDINALITY`] when registers are modified.
    cached_card: usize,
    /// Register array storing the maximum observed rank for each bucket.
    registers: Box<[u8]>,
}

impl HyperLogLog {
    /// Creates a new HyperLogLog with the specified precision.
    ///
    /// The `bits` parameter determines the number of registers (2^bits) and
    /// affects the accuracy of cardinality estimates. The expected error rate
    /// is approximately `1.04 / sqrt(2^bits)`.
    ///
    /// | bits | registers | error rate |
    /// |------|-----------|------------|
    /// | 4    | 16        | ~26%       |
    /// | 6    | 64        | ~13%       |
    /// | 10   | 1024      | ~3.25%     |
    /// | 14   | 16384     | ~0.81%     |
    ///
    /// # Errors
    ///
    /// Returns `None` if `bits` is not in the range 4..=20.
    #[must_use]
    pub fn new(bits: u8) -> Option<Self> {
        if !(4..=20).contains(&bits) {
            return None;
        }

        let size = 1u32 << bits;
        Some(Self {
            bits,
            rank_bits: 32 - bits,
            size,
            cached_card: 0, // Initially the cardinality is 0
            registers: vec![0u8; size as usize].into_boxed_slice(),
        })
    }

    /// Creates a HyperLogLog from existing register data.
    ///
    /// The size of the register slice must be a power of 2 and correspond
    /// to a valid bits value (4-20).
    ///
    /// # Errors
    ///
    /// Returns `None` if:
    /// - `registers.len()` is not a power of 2
    /// - The implied bits value is not in the range 4..=20
    #[must_use]
    pub fn from_registers(registers: &[u8]) -> Option<Self> {
        let size = registers.len();
        if !size.is_power_of_two() {
            return None;
        }

        let bits = size.trailing_zeros() as u8;
        if !(4..=20).contains(&bits) {
            return None;
        }

        Some(Self {
            bits,
            rank_bits: 32 - bits,
            size: size as u32,
            cached_card: INVALID_CACHE_CARDINALITY,
            registers: registers.to_vec().into_boxed_slice(),
        })
    }

    /// Sets registers from a buffer, replacing existing registers.
    ///
    /// The size of the register slice must be a power of 2 and match the
    /// current register count, or correspond to a valid bits value (4-20)
    /// if resizing.
    ///
    /// # Errors
    ///
    /// Returns `false` if:
    /// - `registers.len()` is not a power of 2
    /// - The implied bits value is not in the range 4..=20
    pub fn set_registers(&mut self, registers: &[u8]) -> bool {
        let size = registers.len();
        if !size.is_power_of_two() {
            return false;
        }

        let bits = size.trailing_zeros() as u8;
        if !(4..=20).contains(&bits) {
            return false;
        }

        if self.size != size as u32 {
            // Reinitialize with new size
            self.bits = bits;
            self.rank_bits = 32 - bits;
            self.size = size as u32;
            self.registers = vec![0u8; size].into_boxed_slice();
        }

        self.registers.copy_from_slice(registers);
        self.cached_card = INVALID_CACHE_CARDINALITY;
        true
    }

    /// Clears all registers and resets the cardinality to 0.
    pub fn clear(&mut self) {
        self.registers.fill(0);
        self.cached_card = 0;
    }

    /// Returns the number of registers.
    #[must_use]
    pub const fn size(&self) -> u32 {
        self.size
    }

    /// Returns the precision (number of bits).
    #[must_use]
    pub const fn bits(&self) -> u8 {
        self.bits
    }

    /// Returns a reference to the register array.
    #[must_use]
    pub fn registers(&self) -> &[u8] {
        &self.registers
    }

    /// Adds a value to the HyperLogLog.
    ///
    /// The value is hashed using FNV-1a and added to the appropriate register.
    pub fn add<T: AsRef<[u8]> + ?Sized>(&mut self, value: &T) {
        let hash = self.hash_value(value.as_ref());
        self.add_hash(hash);
    }

    /// Adds a pre-computed 32-bit hash to the HyperLogLog.
    pub fn add_hash(&mut self, hash: u32) {
        let index = (hash >> self.rank_bits) as usize;
        let rank = Self::compute_rank(hash, self.rank_bits);

        if rank > self.registers[index] {
            self.registers[index] = rank;
            // New max rank, invalidate the cached cardinality
            self.cached_card = INVALID_CACHE_CARDINALITY;
        }
    }

    /// Estimates the cardinality (number of distinct elements).
    ///
    /// The result is cached until a new element is added or registers are modified.
    pub fn count(&mut self) -> usize {
        // Return the cached cardinality if it's available
        if self.cached_card != INVALID_CACHE_CARDINALITY {
            return self.cached_card;
        }

        let estimate = self.compute_count();
        self.cached_card = estimate;
        estimate
    }

    /// Estimates the cardinality without caching (const-friendly version).
    #[must_use]
    pub fn count_uncached(&self) -> usize {
        if self.cached_card != INVALID_CACHE_CARDINALITY {
            return self.cached_card;
        }
        self.compute_count()
    }

    /// Merges another HyperLogLog into this one.
    ///
    /// After merging, this HyperLogLog will estimate the cardinality of the
    /// union of both sets.
    ///
    /// # Errors
    ///
    /// Returns `false` if the two HyperLogLogs have different sizes.
    pub fn merge(&mut self, other: &HyperLogLog) -> bool {
        if self.size != other.size {
            return false;
        }

        for (dst, &src) in self.registers.iter_mut().zip(other.registers.iter()) {
            if *dst < src {
                *dst = src;
                // New max rank, invalidate the cached cardinality
                self.cached_card = INVALID_CACHE_CARDINALITY;
            }
        }
        true
    }

    /// Computes the FNV-1a hash of a byte slice.
    fn hash_value(&self, buf: &[u8]) -> u32 {
        let mut hasher = Fnv32::with_offset_basis(HLL_FNV_OFFSET_BASIS);
        hasher.write(buf);
        hasher.finish() as u32
    }

    /// Computes the rank (position of first set bit + 1) for a hash value.
    ///
    /// The rank is clamped to `max_rank` to prevent overflow.
    #[inline]
    fn compute_rank(hash: u32, max_rank: u8) -> u8 {
        // Index of first set bit (from LSB), or 32 if hash is 0
        let rank = if hash == 0 {
            32
        } else {
            hash.trailing_zeros() as u8
        };
        // Clamp to max_rank and add 1
        rank.min(max_rank) + 1
    }

    /// Computes the cardinality estimate using the HyperLogLog algorithm.
    fn compute_count(&self) -> usize {
        // Alpha adjustment factor based on register count
        let alpha_mm = match self.bits {
            4 => 0.673,
            5 => 0.697,
            6 => 0.709,
            _ => 0.7213 / (1.0 + 1.079 / self.size as f64),
        };

        let alpha_mm = alpha_mm * (self.size as f64) * (self.size as f64);

        // Compute harmonic mean of 2^(-register[i])
        let sum: f64 = self
            .registers
            .iter()
            .map(|&r| 1.0 / ((1u64 << r) as f64))
            .sum();

        let mut estimate = alpha_mm / sum;

        // Small range correction (linear counting)
        if estimate <= 2.5 * self.size as f64 {
            let zeros = self.registers.iter().filter(|&&r| r == 0).count();
            if zeros > 0 {
                estimate = (self.size as f64) * (self.size as f64 / zeros as f64).ln();
            }
        }
        // Large range correction
        else if estimate > (1.0 / 30.0) * 4_294_967_296.0 {
            estimate = -4_294_967_296.0 * (1.0 - estimate / 4_294_967_296.0).ln();
        }

        estimate as usize
    }
}

impl std::fmt::Debug for HyperLogLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HyperLogLog")
            .field("bits", &self.bits)
            .field("size", &self.size)
            .field("cached_card", &self.cached_card)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // This test needs access to the private compute_rank function
    #[test]
    fn test_compute_rank() {
        // Test rank computation
        assert_eq!(HyperLogLog::compute_rank(0, 26), 27); // 0 -> rank 32, clamped to 26+1
        assert_eq!(HyperLogLog::compute_rank(1, 26), 1); // 0b001 -> trailing zeros = 0, rank = 1
        assert_eq!(HyperLogLog::compute_rank(2, 26), 2); // 0b010 -> trailing zeros = 1, rank = 2
        assert_eq!(HyperLogLog::compute_rank(4, 26), 3); // 0b100 -> trailing zeros = 2, rank = 3
        assert_eq!(HyperLogLog::compute_rank(8, 26), 4); // 0b1000 -> trailing zeros = 3, rank = 4
    }
}
