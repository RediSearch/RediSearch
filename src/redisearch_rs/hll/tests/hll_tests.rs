/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use hll::HyperLogLog;

#[test]
fn test_new_valid_bits() {
    for bits in 4..=20 {
        let hll = HyperLogLog::new(bits);
        assert!(hll.is_some(), "bits={} should be valid", bits);
        let hll = hll.unwrap();
        assert_eq!(hll.bits(), bits);
        assert_eq!(hll.size(), 1 << bits);
    }
}

#[test]
fn test_new_invalid_bits() {
    assert!(HyperLogLog::new(3).is_none());
    assert!(HyperLogLog::new(21).is_none());
    assert!(HyperLogLog::new(0).is_none());
}

#[test]
fn test_empty_count() {
    let mut hll = HyperLogLog::new(6).unwrap();
    assert_eq!(hll.count(), 0);
}

#[test]
fn test_single_element() {
    let mut hll = HyperLogLog::new(6).unwrap();
    hll.add(&42u64.to_le_bytes());
    let count = hll.count();
    // Should be approximately 1, allowing for HLL error
    assert!(count >= 1 && count <= 3, "count={}", count);
}

#[test]
fn test_duplicates_ignored() {
    let mut hll = HyperLogLog::new(6).unwrap();
    for _ in 0..1000 {
        hll.add(&42u64.to_le_bytes());
    }
    let count = hll.count();
    // Should still be approximately 1
    assert!(count >= 1 && count <= 3, "count={}", count);
}

#[test]
fn test_cardinality_estimation() {
    let mut hll = HyperLogLog::new(10).unwrap(); // ~3.25% error
    let n: u64 = 10_000;

    for i in 0..n {
        hll.add(&i.to_le_bytes());
    }

    let count = hll.count();
    // Allow 10% error margin
    let lower = (n as f64 * 0.9) as usize;
    let upper = (n as f64 * 1.1) as usize;
    assert!(
        count >= lower && count <= upper,
        "count={}, expected ~{}",
        count,
        n
    );
}

#[test]
fn test_merge() {
    let mut hll1 = HyperLogLog::new(6).unwrap();
    let mut hll2 = HyperLogLog::new(6).unwrap();

    // Add 0-99 to hll1
    for i in 0u64..100 {
        hll1.add(&i.to_le_bytes());
    }

    // Add 50-149 to hll2
    for i in 50u64..150 {
        hll2.add(&i.to_le_bytes());
    }

    // Merge hll2 into hll1
    assert!(hll1.merge(&hll2));

    let count = hll1.count();
    // Should be approximately 150 (0-149 union)
    // With 13% error rate and low register count, allow 50% margin
    assert!(count >= 75 && count <= 225, "count={}", count);
}

#[test]
fn test_merge_different_sizes_fails() {
    let mut hll1 = HyperLogLog::new(6).unwrap();
    let hll2 = HyperLogLog::new(8).unwrap();
    assert!(!hll1.merge(&hll2));
}

#[test]
fn test_from_registers() {
    let mut hll = HyperLogLog::new(4).unwrap(); // 16 registers
    hll.add(&1u64.to_le_bytes());
    hll.add(&2u64.to_le_bytes());

    let registers = hll.registers().to_vec();
    let hll2 = HyperLogLog::from_registers(&registers).unwrap();

    assert_eq!(hll.size(), hll2.size());
    assert_eq!(hll.registers(), hll2.registers());
}

#[test]
fn test_set_registers() {
    let mut hll = HyperLogLog::new(4).unwrap();
    let registers = vec![1u8; 16]; // 16 registers
    assert!(hll.set_registers(&registers));
    assert_eq!(hll.registers(), &registers[..]);
}

#[test]
fn test_clear() {
    let mut hll = HyperLogLog::new(6).unwrap();
    hll.add(&1u64.to_le_bytes());
    hll.add(&2u64.to_le_bytes());
    assert!(hll.count() > 0);

    hll.clear();
    assert_eq!(hll.count(), 0);
}

#[test]
fn test_cached_count() {
    let mut hll = HyperLogLog::new(6).unwrap();
    hll.add(&1u64.to_le_bytes());

    let count1 = hll.count();
    let count2 = hll.count();
    assert_eq!(count1, count2);

    // Adding a new element should invalidate cache
    hll.add(&2u64.to_le_bytes());
    // Count may change
    let _ = hll.count();
}
