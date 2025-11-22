/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv3); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use hll::HLL;
use hyperloglogplus::{HyperLogLog, HyperLogLogPlus};
use std::collections::hash_map::RandomState;
use std::time::Instant;

fn main() {
    println!("=== HyperLogLog Implementation Comparison ===\n");

    // Test configurations
    let test_sizes = vec![1_000, 10_000, 100_000, 1_000_000];
    let test_configs = vec![
        (6, "6 bits (64 registers)"),
        (14, "14 bits (16,384 registers)"),
    ];

    for &(bits, desc) in &test_configs {
        println!("\n{}", "=".repeat(70));
        println!("Testing with {}", desc);
        println!("{}", "=".repeat(70));

        for &size in &test_sizes {
            println!("\n{} unique elements:", size);
            println!("{}", "-".repeat(50));

            // Our implementation with u64
            if bits == 6 {
                println!("\n📊 Our HLL<u64, {}>:", bits);
                let start = Instant::now();
                let mut our_hll: HLL<u64, 6> = HLL::new();
                for i in 0..size {
                    our_hll.add(i as u64);
                }
                let our_time = start.elapsed();
                let our_count = our_hll.count();
                let our_error = ((our_count as f64 - size as f64).abs() / size as f64) * 100.0;
                let our_size = std::mem::size_of_val(&our_hll);

                println!("  Time:     {:.2} µs", our_time.as_micros());
                println!("  Estimate: {}", our_count);
                println!("  Error:    {:.2}%", our_error);
                println!("  Memory:   {} bytes", our_size);

                // hyperloglogplus implementation
                println!("\n📊 hyperloglogplus HyperLogLogPlus<u64>:");
                let start = Instant::now();
                let mut hllpp: HyperLogLogPlus<u64, _> = HyperLogLogPlus::new(6, RandomState::new()).unwrap();
                for i in 0..size {
                    hllpp.insert(&(i as u64));
                }
                let hllpp_time = start.elapsed();
                let hllpp_count = hllpp.count();
                let hllpp_error = ((hllpp_count - size as f64).abs() / size as f64) * 100.0;
                let hllpp_size = std::mem::size_of_val(&hllpp);

                println!("  Time:     {:.2} µs", hllpp_time.as_micros());
                println!("  Estimate: {:.0}", hllpp_count);
                println!("  Error:    {:.2}%", hllpp_error);
                println!("  Memory:   {} bytes", hllpp_size);

                // Comparison summary
                println!("\n📈 Comparison:");
                println!("  Speed ratio (theirs/ours): {:.2}x", hllpp_time.as_secs_f64() / our_time.as_secs_f64());
                println!("  Memory ratio (theirs/ours): {:.2}x", hllpp_size as u64 / our_size as u64);
            } else {
                println!("\n📊 Our HLL<u64, {}>:", bits);
                let start = Instant::now();
                let mut our_hll: HLL<u64, 14> = HLL::new();
                for i in 0..size {
                    our_hll.add(i as u64);
                }
                let our_time = start.elapsed();
                let our_count = our_hll.count();
                let our_error = ((our_count as f64 - size as f64).abs() / size as f64) * 100.0;
                let our_size = std::mem::size_of_val(&our_hll);

                println!("  Time:     {:.2} µs", our_time.as_micros());
                println!("  Estimate: {}", our_count);
                println!("  Error:    {:.2}%", our_error);
                println!("  Memory:   {} bytes", our_size);

                // hyperloglogplus implementation
                println!("\n📊 hyperloglogplus HyperLogLogPlus<u64>:");
                let start = Instant::now();
                let mut hllpp: HyperLogLogPlus<u64, _> = HyperLogLogPlus::new(14, RandomState::new()).unwrap();
                for i in 0..size {
                    hllpp.insert(&(i as u64));
                }
                let hllpp_time = start.elapsed();
                let hllpp_count = hllpp.count();
                let hllpp_error = ((hllpp_count - size as f64).abs() / size as f64) * 100.0;
                let hllpp_size = std::mem::size_of_val(&hllpp);

                println!("  Time:     {:.2} µs", hllpp_time.as_micros());
                println!("  Estimate: {:.0}", hllpp_count);
                println!("  Error:    {:.2}%", hllpp_error);
                println!("  Memory:   {} bytes", hllpp_size);

                // Comparison summary
                println!("\n📈 Comparison:");
                println!("  Speed ratio (theirs/ours): {:.2}x", hllpp_time.as_secs_f64() / our_time.as_secs_f64());
                println!("  Memory ratio (theirs/ours): {:.2}x", hllpp_size as u64 / our_size as u64);
            }
        }
    }

    // Test merge functionality
    println!("\n{}", "=".repeat(70));
    println!("Testing Merge Functionality (14 bits)");
    println!("{}", "=".repeat(70));

    let merge_size = 10_000;

    // Our implementation with u64
    println!("\n📊 Our HLL<u64, 14> - Merge Test:");
    let start = Instant::now();
    let mut our_hll1: HLL<u64, 14> = HLL::new();
    let mut our_hll2: HLL<u64, 14> = HLL::new();

    for i in 0..merge_size {
        our_hll1.add(i as u64);
    }
    for i in (merge_size / 2)..(merge_size + merge_size / 2) {
        our_hll2.add(i as u64);
    }

    let before_merge = our_hll1.count();
    our_hll1.merge(&our_hll2);
    let after_merge = our_hll1.count();
    let our_merge_time = start.elapsed();

    let expected = merge_size + merge_size / 2;
    let merge_error = ((after_merge as f64 - expected as f64).abs() / expected as f64) * 100.0;

    println!("  HLL1 before merge: {}", before_merge);
    println!("  HLL2 count:        {}", our_hll2.count());
    println!("  After merge:       {}", after_merge);
    println!("  Expected:          {}", expected);
    println!("  Error:             {:.2}%", merge_error);
    println!("  Time:              {:.2} µs", our_merge_time.as_micros());

    // hyperloglogplus merge
    println!("\n📊 hyperloglogplus HyperLogLogPlus<u64, 14> - Merge Test:");
    let start = Instant::now();
    let mut hllpp1: HyperLogLogPlus<u64, _> = HyperLogLogPlus::new(14, RandomState::new()).unwrap();
    let mut hllpp2: HyperLogLogPlus<u64, _> = HyperLogLogPlus::new(14, RandomState::new()).unwrap();

    for i in 0..merge_size {
        hllpp1.insert(&(i as u64));
    }
    for i in (merge_size / 2)..(merge_size + merge_size / 2) {
        hllpp2.insert(&(i as u64));
    }

    let before_merge_pp = hllpp1.count();
    hllpp1.merge(&hllpp2).unwrap();
    let after_merge_pp = hllpp1.count();
    let hllpp_merge_time = start.elapsed();

    let merge_error_pp = ((after_merge_pp as f64 - expected as f64).abs() / expected as f64) * 100.0;

    println!("  HLL1 before merge: {:.0}", before_merge_pp);
    println!("  HLL2 count:        {:.0}", hllpp2.count());
    println!("  After merge:       {:.0}", after_merge_pp);
    println!("  Expected:          {}", expected);
    println!("  Error:             {:.2}%", merge_error_pp);
    println!("  Time:              {:.2} µs", hllpp_merge_time.as_micros());

    println!("\n📈 Merge Performance:");
    println!("  Speed ratio (theirs/ours): {:.2}x", hllpp_merge_time.as_secs_f64() / our_merge_time.as_secs_f64());
}
