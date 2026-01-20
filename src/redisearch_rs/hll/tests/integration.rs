/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Integration tests for the HLL crate.
//!
//! These tests use only the public API and verify the behavior
//! of the HyperLogLog implementation from an external perspective.

use std::hash::Hasher;

use hll::{Hll, Hll4, Hll8, Hll12, Hll16, HllError, HllHasher, Murmur3Hasher};

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
    assert!(
        error < 0.15,
        "error {error} too large, count={count}, n={n}"
    );
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
        let hash = hash32::Hasher::finish32(&hasher);
        let index = hash >> 20;
        let trailing = hash.trailing_zeros();
        eprintln!("{name}: hash={hash:#010x}, index={index}, trailing_zeros={trailing}");
    }

    // Check that different inputs produce different hashes
    let mut hasher1 = HllHasher::default();
    hasher1.write(b"test1");
    let hash1 = hash32::Hasher::finish32(&hasher1);

    let mut hasher2 = HllHasher::default();
    hasher2.write(b"test2");
    let hash2 = hash32::Hasher::finish32(&hasher2);

    assert_ne!(
        hash1, hash2,
        "different inputs should produce different hashes"
    );
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
    assert!(
        error < 0.05,
        "error {error} too large for n={n}, count={count}"
    );
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
    assert!(
        error < 0.30,
        "error {error} too large for n={n}, count={count}"
    );
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
fn test_murmur3_accuracy() {
    type Murmur3Hll12 = Hll<12, 4096, Murmur3Hasher>;

    let mut hll = Murmur3Hll12::new();
    let n = 10000u32;

    for i in 0..n {
        hll.add(&i.to_le_bytes());
    }

    let count = hll.count();
    let error = (count as f64 - n as f64).abs() / n as f64;

    // Murmur3 should achieve < 5% error with sequential integers
    assert!(
        error < 0.05,
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
    let mut hll: Hll12<CustomTestHasher> = Hll::new();
    hll.add(b"test");
    assert!(hll.count() >= 1);
}

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
