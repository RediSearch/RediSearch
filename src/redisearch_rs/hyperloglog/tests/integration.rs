/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Integration tests for the hyperloglog crate.
//!
//! These tests use only the public API and verify the behavior
//! of the HyperLogLog implementation from an external perspective.

use std::hash::Hasher;

use hyperloglog::{
    CFnvHasher, HyperLogLog, HyperLogLog4, HyperLogLog8, HyperLogLog9, HyperLogLog10,
    InvalidBufferLength, Murmur3Hasher,
};

/// Concrete type alias for testing to avoid inference issues with multiple Hasher32 impls.
type TestHll10 = HyperLogLog10<[u8; 4], CFnvHasher>;

#[test]
fn test_new_hll() {
    let hll = TestHll10::new();
    assert_eq!(hll.count(), 0);
    assert_eq!(TestHll10::bits(), 10);
    assert_eq!(TestHll10::size(), 1024);
}

#[test]
fn test_add_single_element() {
    let mut hll = HyperLogLog10::<[u8; 5], CFnvHasher>::new();
    hll.add(b"hello");
    assert_eq!(hll.count(), 1);
}

#[test]
fn test_add_duplicate_elements() {
    let mut hll = HyperLogLog10::<[u8; 4], CFnvHasher>::new();
    for _ in 0..100 {
        hll.add(b"same");
    }
    assert_eq!(hll.count(), 1);
}

#[test]
#[cfg_attr(miri, ignore = "Too slow to be run under miri.")]
fn test_add_many_distinct_elements() {
    let mut hll = TestHll10::new();
    let n = 10000u32;
    // Use simple incrementing integers - they work well with FNV-1a
    for i in 0..n {
        hll.add(&i.to_le_bytes());
    }

    // Count zeros for debugging
    let zeros = hll.registers().iter().filter(|&&r| r == 0).count();
    eprintln!(
        "n={n}, zeros={zeros} ({:.1}%), count={}",
        100.0 * zeros as f64 / TestHll10::size() as f64,
        hll.count()
    );

    let count = hll.count();
    // Expected error is ~1.6%, allow for 15% tolerance
    // Note: FNV-1a with sequential integers has some bias
    let error = (count as f64 - n as f64).abs() / n as f64;
    assert!(
        error < 0.15,
        "error {error} too large, count={count}, n={n}"
    );
}

#[test]
fn test_add_hash_direct() {
    // Test with pre-computed hash values to verify algorithm correctness
    let mut hll = TestHll10::new();

    // Add hashes that will go to different registers with known ranks
    // Hash format: top 10 bits = register index, trailing zeros = rank - 1
    // Register 0 with rank 1 (0 trailing zeros)
    hll.add_precomputed_hash(0b100000000000000000001);
    // Register 1 with rank 2 (1 trailing zero)
    hll.add_precomputed_hash(0b10000000000000000000010);
    // Register 2 with rank 3 (2 trailing zeros)
    hll.add_precomputed_hash(0b100000000000000000001100);

    // Check that registers were set correctly
    let regs = hll.registers();
    assert_eq!(regs[0], 1, "register 0 should be 1");
    assert_eq!(regs[1], 2, "register 1 should be 2");
    assert_eq!(regs[2], 3, "register 2 should be 3");

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
        let mut hasher = CFnvHasher::default();
        hasher.write(data);
        let hash = hash32::Hasher::finish32(&hasher);
        let index = hash >> 20;
        let trailing = hash.trailing_zeros();
        eprintln!("{name}: hash={hash:#010x}, index={index}, trailing_zeros={trailing}");
    }

    // Check that different inputs produce different hashes
    let mut hasher1 = CFnvHasher::default();
    hasher1.write(b"test1");
    let hash1 = hash32::Hasher::finish32(&hasher1);

    let mut hasher2 = CFnvHasher::default();
    hasher2.write(b"test2");
    let hash2 = hash32::Hasher::finish32(&hasher2);

    assert_ne!(
        hash1, hash2,
        "different inputs should produce different hashes"
    );
}

#[test]
#[cfg_attr(miri, ignore = "Too slow to be run under miri.")]
fn test_register_distribution() {
    let mut hll = HyperLogLog10::<String, Murmur3Hasher>::new();
    let n = 10000u32;

    for i in 0..n {
        hll.add(&format!("element-{i}"));
    }

    let count = hll.count();
    let error = (count as f64 - n as f64).abs() / n as f64;
    // Allow up to 30% error to match C implementation behavior
    assert!(
        error < 0.30,
        "error {error} too large for n={n}, count={count}"
    );
}

#[test]
fn test_small_cardinality() {
    // Test small cardinality where linear counting is expected
    let mut hll = TestHll10::new();
    let n = 1000u32;

    for i in 0..n {
        hll.add(&i.to_le_bytes());
    }

    let count = hll.count();
    let error = (count as f64 - n as f64).abs() / n as f64;

    // For small cardinalities, allow more error due to variance
    assert!(
        error < 0.30,
        "error {error} too large for n={n}, count={count}"
    );
}

#[test]
fn test_merge() {
    let mut hll1 = TestHll10::new();
    let mut hll2 = TestHll10::new();

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
    let mut hll = HyperLogLog10::<[u8; 5], CFnvHasher>::new();
    hll.add(b"hello");
    assert!(hll.count() > 0);

    hll.clear();
    assert_eq!(hll.count(), 0);
}

#[test]
fn test_cache_invalidation() {
    let mut hll = HyperLogLog10::<[u8; 5], CFnvHasher>::new();
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
    let mut hll1 = TestHll10::new();
    for i in 0..1000u32 {
        hll1.add(&i.to_le_bytes());
    }

    let registers = *hll1.registers();
    let hll2 = TestHll10::from_registers(registers);

    assert_eq!(hll1.count(), hll2.count());
}

#[test]
fn test_try_from_slice() {
    let hll1 = TestHll10::new();
    let slice = hll1.registers().as_slice();

    let hll2 = TestHll10::try_from(slice).unwrap();
    assert_eq!(hll1.count(), hll2.count());
}

#[test]
fn test_try_from_slice_invalid_length() {
    let err = TestHll10::try_from([0u8; 100].as_slice()).unwrap_err();
    assert_eq!(err, InvalidBufferLength::<1024> { got: 100 });
}

#[test]
fn test_type_aliases() {
    // Just verify they compile and have correct parameters
    assert_eq!(HyperLogLog4::<u8, CFnvHasher>::bits(), 4);
    assert_eq!(HyperLogLog4::<u8, CFnvHasher>::size(), 16);

    assert_eq!(HyperLogLog8::<u8, CFnvHasher>::bits(), 8);
    assert_eq!(HyperLogLog8::<u8, CFnvHasher>::size(), 256);

    assert_eq!(HyperLogLog9::<u8, CFnvHasher>::bits(), 9);
    assert_eq!(HyperLogLog9::<u8, CFnvHasher>::size(), 512);
}

#[test]
#[cfg_attr(miri, ignore = "Too slow to be run under miri.")]
fn test_murmur3_accuracy() {
    type Murmur3HyperLogLog10 = HyperLogLog10<[u8; 4], Murmur3Hasher>;

    let mut hll = Murmur3HyperLogLog10::new();
    let n = 10000u32;

    for i in 0..n {
        hll.add(&i.to_le_bytes());
    }

    let count = hll.count();
    let error = (count as f64 - n as f64).abs() / n as f64;

    // Murmur3 should achieve < 10% error with sequential integers
    assert!(
        error < 0.10,
        "error {error} too large, count={count}, n={n}"
    );
}

/// Custom hasher for testing pluggable hasher support.
#[derive(Default)]
struct CustomTestHasher(u32);

impl Hasher for CustomTestHasher {
    fn finish(&self) -> u64 {
        self.0 as u64
    }

    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = self.0.wrapping_add(b as u32);
        }
    }
}

impl hash32::Hasher for CustomTestHasher {
    fn finish32(&self) -> u32 {
        self.0
    }
}

#[test]
fn test_custom_hasher() {
    let mut hll: HyperLogLog10<[u8; 4], CustomTestHasher> = HyperLogLog::new();
    hll.add(b"test");
    assert!(hll.count() >= 1);
}

#[test]
fn test_large_range_correction() {
    // Use HyperLogLog10 with high register values to trigger large range correction
    // Large correction applies when estimate > (1/30) * 2^32 ≈ 143 million
    let mut registers = [0u8; 1024];
    registers.fill(19); // High values -> small sum -> large raw estimate

    let hll = HyperLogLog10::<u8, Murmur3Hasher>::from_registers(registers);
    let count = hll.count();

    // Should produce a reasonable estimate (not overflow or panic)
    assert!(count > 0, "count should be positive");
}

#[test]
fn test_hyperloglog4_small_precision() {
    let mut hll = HyperLogLog4::<[u8; 4], Murmur3Hasher>::new();
    let n = 1000u32;

    for i in 0..n {
        hll.add(&i.to_le_bytes());
    }

    let count = hll.count();
    let error = (count as f64 - n as f64).abs() / n as f64;
    // HyperLogLog4 theoretical error ~26%, allow up to 35%
    assert!(
        error < 0.35,
        "error {:.1}% exceeds 35% for HyperLogLog4",
        error * 100.0
    );
}

#[test]
fn test_hyperloglog5_small_precision() {
    let mut hll = HyperLogLog::<[u8; 4], 5, 32, Murmur3Hasher>::new();
    let n = 1000u32;

    for i in 0..n {
        hll.add(&i.to_le_bytes());
    }

    let count = hll.count();
    let error = (count as f64 - n as f64).abs() / n as f64;
    // HyperLogLog5 theoretical error ~18%, allow up to 25%
    assert!(
        error < 0.25,
        "error {:.1}% exceeds 25% for HyperLogLog5",
        error * 100.0
    );
}

#[test]
fn test_hyperloglog6_small_precision() {
    let mut hll = HyperLogLog::<[u8; 4], 6, 64, Murmur3Hasher>::new();
    let n = 1000u32;

    for i in 0..n {
        hll.add(&i.to_le_bytes());
    }

    let count = hll.count();
    let error = (count as f64 - n as f64).abs() / n as f64;
    // HyperLogLog6 theoretical error ~13%, allow up to 20%
    assert!(
        error < 0.20,
        "error {:.1}% exceeds 20% for HyperLogLog6",
        error * 100.0
    );
}

#[cfg(not(miri))]
#[test]
fn test_debug_repr() {
    let mut hll = HyperLogLog10::<u32, Murmur3Hasher>::default();
    for i in 0..10u32 {
        hll.add(&i);
    }
    // Debug representation doesn't include registers,
    // but it tracks compile-time constants
    insta::assert_debug_snapshot!(hll, @r###"
    HyperLogLog {
        bits: 10,
        size: 1024,
        cached_cardinality: None,
        ..
    }
    "###);
}

#[test]
fn test_clone() {
    let mut hll = HyperLogLog10::<u32, Murmur3Hasher>::default();
    for i in 0..10u32 {
        hll.add(&i);
    }
    let cloned = hll.clone();
    assert_eq!(cloned.count(), hll.count());
}

#[cfg(debug_assertions)]
#[test]
#[should_panic(
    expected = "The 0th register values exceeds the expected cap. Got 35, expected at most 29."
)]
fn test_register_validation() {
    let mut hll = HyperLogLog4::<u8, Murmur3Hasher>::default();
    hll.set_registers([35; 16]);
}

#[cfg(not(miri))]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    /// Concrete type alias for property tests to avoid inference issues.
    type TestHll10 = HyperLogLog10<[u8; 4], CFnvHasher>;

    proptest! {
        /// Test that merge always increases or maintains the count estimate.
        #[test]
        fn merge_is_monotonic(n1 in 10u32..100, n2 in 10u32..100) {
            let mut hll1 = TestHll10::new();
            let mut hll2 = TestHll10::new();

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
            let mut hll = TestHll10::new();
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
            let mut hll = HyperLogLog10::<[u8; 12], CFnvHasher>::new();
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
            let mut hll1 = TestHll10::new();
            for i in 0..n {
                hll1.add(&i.to_le_bytes());
            }

            let count1 = hll1.count();
            let registers = *hll1.registers();
            let hll2 = TestHll10::from_registers(registers);

            prop_assert_eq!(hll2.count(), count1);
        }
    }
}
