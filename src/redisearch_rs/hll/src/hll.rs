/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::cell::Cell;
use std::hash::Hasher;
use std::marker::PhantomData;
use fnv::Fnv32;

/// Trait for types that can be converted to a byte slice for hashing
pub trait AsHashBytes {
    /// Get a reference to the bytes representation of this value
    fn as_hash_bytes(&self) -> &[u8];
}

// Implement for fixed-size numeric types using their memory representation
impl AsHashBytes for f64 {
    fn as_hash_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self as *const f64 as *const u8, 8) }
    }
}

impl AsHashBytes for f32 {
    fn as_hash_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self as *const f32 as *const u8, 4) }
    }
}

impl AsHashBytes for i64 {
    fn as_hash_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self as *const i64 as *const u8, 8) }
    }
}

impl AsHashBytes for i32 {
    fn as_hash_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self as *const i32 as *const u8, 4) }
    }
}

impl AsHashBytes for u64 {
    fn as_hash_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self as *const u64 as *const u8, 8) }
    }
}

impl AsHashBytes for u32 {
    fn as_hash_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self as *const u32 as *const u8, 4) }
    }
}

impl AsHashBytes for str {
    fn as_hash_bytes(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl AsHashBytes for [u8] {
    fn as_hash_bytes(&self) -> &[u8] {
        self
    }
}

/// HyperLogLog probabilistic cardinality estimator
///
/// The BITS parameter determines the number of registers (2^BITS).
/// Valid range is 4-20 bits, giving 16 to 1,048,576 registers.
/// Expected error rate is approximately 1.04 / sqrt(2^BITS)
///
/// The T parameter is the type of values being added to the HLL.
pub struct HLL<T, const BITS: usize>
where
    T: AsHashBytes + ?Sized,
    [(); 1 << BITS]: Sized,
{
    /// Cached cardinality from the last count operation
    /// None when invalid (after modifications)
    cached_card: Cell<Option<usize>>,
    /// Register array stored inline
    registers: [u8; 1 << BITS],
    /// Phantom data to track the value type
    _phantom: PhantomData<T>,
}

impl<T, const BITS: usize> HLL<T, BITS>
where
    T: AsHashBytes + ?Sized,
    [(); 1 << BITS]: Sized,
{
    /// Number of bits used for rank calculation (32 - BITS)
    const RANK_BITS: u8 = (32 - BITS) as u8;

    /// Number of registers (2^BITS)
    const SIZE: usize = 1 << BITS;

    /// Create a new HLL with the given number of bits
    ///
    /// # Panics
    /// Panics if BITS is not in the valid range [4, 20]
    pub fn new() -> Self {
        assert!(BITS >= 4 && BITS <= 20, "BITS must be between 4 and 20");

        Self {
            cached_card: Cell::new(Some(0)), // Initially cardinality is 0
            registers: [0; 1 << BITS],
            _phantom: PhantomData,
        }
    }

    /// Add an element to the HLL
    ///
    /// Hashes the provided data and updates the appropriate register.
    pub fn add(&mut self, data: T)
    where
        T: Sized,
    {
        let bytes = data.as_hash_bytes();
        // let mut hasher = Fnv32::default();
        let mut hasher = Fnv32::with_offset_basis(0x5f61767a);
        hasher.write(bytes);
        let hash = hasher.finish() as u32;
        self.add_hash(hash);
    }

    /// Add an element to the HLL by reference (for unsized types like str and [u8])
    ///
    /// Hashes the provided data and updates the appropriate register.
    pub fn add_ref(&mut self, data: &T) {
        let bytes = data.as_hash_bytes();
        let mut hasher = Fnv32::with_offset_basis(0x5f61767a);
        hasher.write(bytes);
        let hash = hasher.finish() as u32;
        self.add_hash(hash);
    }

    /// Add a precomputed hash to the HLL
    ///
    /// Updates the appropriate register based on the hash value.
    pub fn add_hash(&mut self, hash: u32) {
        let index = (hash >> Self::RANK_BITS) as usize;
        let rank = Self::rank(hash);

        if rank > self.registers[index] {
            self.registers[index] = rank;
            // New max rank, invalidate the cached cardinality
            self.cached_card.set(None);
        }
    }

    /// Constant used in cardinality estimation
    const ALFA_MM: f64 = match BITS {
        4 => 0.673,
        5 => 0.697,
        6 => 0.709,
        _ => 0.7213 / (1.0 + 1.079 / Self::SIZE as f64),
    } * (Self::SIZE as f64) * (Self::SIZE as f64);

    /// Estimate the cardinality of the HLL
    ///
    /// Returns the estimated number of unique elements added to the HLL.
    /// Uses cached value if available.
    pub fn count(&self) -> usize {
        // Return the cached cardinality if it's available
        if let Some(cached) = self.cached_card.get() {
            return cached;
        }

        let sum: f64 = self.registers
            .iter()
            .map(|&reg| 1.0 / (1_u64 << reg) as f64)
            .sum();

        let mut estimate = Self::ALFA_MM / sum;

        // Small range correction
        if estimate <= 5.0 / 2.0 * Self::SIZE as f64 {
            let zeros = self.registers.iter().filter(|&&reg| reg == 0).count();
            if zeros > 0 {
                estimate = (Self::SIZE as f64) * (Self::SIZE as f64 / zeros as f64).ln();
            }
        }
        // Large range correction
        else if estimate > (1.0 / 30.0) * 4294967296.0 {
            estimate = -4294967296.0 * (1.0 - (estimate / 4294967296.0)).ln();
        }

        let result = estimate as usize;
        // Cache the current estimate
        self.cached_card.set(Some(result));
        result
    }

    /// Merge another HLL into this one
    ///
    /// Takes the maximum value for each register between the two HLLs.
    /// Both HLLs must have the same SIZE parameter (enforced by type system).
    pub fn merge(&mut self, other: &Self) {
        let modified = self.registers
            .iter_mut()
            .zip(&other.registers)
            .map(|(this, &other)| {
                if *this < other {
                    *this = other;
                    true
                } else {
                    false
                }
            })
            .any(|changed| changed);

        // Invalidate the cached cardinality if any register was updated
        if modified {
            self.cached_card.set(None);
        }
    }

    /// Clear all registers and reset cardinality to 0
    pub fn clear(&mut self) {
        self.registers.fill(0);
        self.cached_card.set(Some(0)); // No elements, so the cardinality is 0
    }

    /// Calculate the rank of a hash value
    fn rank(hash: u32) -> u8 {
        let rank = hash.trailing_zeros() as u8;
        Self::RANK_BITS.min(rank) + 1
    }
}
