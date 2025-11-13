/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use hll::HLL;

fn main() {
    // // Test with String type
    // println!("=== Testing with String ===");
    // let mut hll_string: HLL<_, 12> = HLL::new();

    // for i in 0..10000 {
    //     let s = format!("element_{}", i);
    //     hll_string.add_ref(s.as_str());
    // }

    // let count = hll_string.count();
    // println!("Added 10,000 unique string elements");
    // println!("Estimated cardinality: {}", count);
    // println!("Error: {:.2}%", (count as f64 - 10000.0).abs() / 10000.0 * 100.0);

    // Test with f64 type
    println!("\n=== Testing with f64 ===");
    const BITS: usize = 8;
    let mut hll_f64: HLL<f64, BITS> = HLL::new();

    for i in 0..10000 {
        let val = i as f64;
        hll_f64.add(val);
    }

    let count = hll_f64.count();
    println!("Added 10,000 unique f64 elements");
    println!("Estimated cardinality: {}", count);
    println!("Error: {:.2}%", (count as f64 - 10000.0).abs() / 10000.0 * 100.0);

    // Test merge with f64
    let mut hll2: HLL<f64, BITS> = HLL::new();
    for i in 5000..15000 {
        let val = i as f64;
        hll2.add(val);
    }

    println!("\nCreated second HLL with f64 elements 5000-14999");
    println!("Second HLL cardinality: {}", hll2.count());

    hll_f64.merge(&hll2);
    println!("\nAfter merge:");
    println!("Estimated cardinality: {}", hll_f64.count());
    println!("Expected: ~15000");

    // Test with i32 type
    // println!("\n=== Testing with i32 ===");
    // let mut hll_i32: HLL<i32, 12> = HLL::new();

    // for i in 0..10000_i32 {
    //     hll_i32.add(i);
    // }

    // let count = hll_i32.count();
    // println!("Added 10,000 unique i32 elements");
    // println!("Estimated cardinality: {}", count);
    // println!("Error: {:.2}%", (count as f64 - 10000.0).abs() / 10000.0 * 100.0);

    // // Test clear
    // hll_f64.clear();
    // println!("\nAfter clear:");
    // println!("Cardinality: {}", hll_f64.count());
}
