/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::hint::black_box;
use varint::*;

fn main() {
    compute_and_report_memory_usage();
}

/// Generate test data and build varint encodings using the Rust implementation.
/// Report memory usage and encoding efficiency.
fn compute_and_report_memory_usage() {
    let test_data = generate_comprehensive_test_data();
    let raw_size = test_data.len() * 4; // u32 = 4 bytes each.

    // Encode with Rust implementation.
    let mut rust_writer = VectorWriter::new(test_data.len());
    for &value in &test_data {
        let _ = rust_writer.write(black_box(value));
    }
    let rust_encoded_size = rust_writer.bytes_len();

    // Field mask encoding.
    let field_masks: Vec<u128> = test_data.iter().map(|&v| v as u128).collect();

    let mut rust_field_size = 0;
    for &mask in &field_masks {
        let mut buf = Vec::new();
        mask.write_as_varint(&mut buf).unwrap();
        rust_field_size += buf.len();
    }

    // Report statistics.
    println!(
        r#"Varint Encoding Analysis:
- Raw data size: {:.3} KB ({} u32 values)
- Varint encoded size: {:.3} KB ({:.2}x compression)
- Field mask encoded size: {:.3} KB ({:.2}x compression)
- Space savings: {:.1}% (varint), {:.1}% (field mask)"#,
        raw_size as f64 / 1024.0,
        test_data.len(),
        rust_encoded_size as f64 / 1024.0,
        raw_size as f64 / rust_encoded_size as f64,
        rust_field_size as f64 / 1024.0,
        raw_size as f64 / rust_field_size as f64,
        (1.0 - rust_encoded_size as f64 / raw_size as f64) * 100.0,
        (1.0 - rust_field_size as f64 / raw_size as f64) * 100.0,
    );

    // Encoding efficiency breakdown.
    analyze_encoding_efficiency(&test_data);

    println!("\nRun `cargo bench` for detailed performance benchmarks.");
}

/// Analyze encoding efficiency by value ranges.
fn analyze_encoding_efficiency(test_data: &[u32]) {
    let mut single_byte = 0;
    let mut two_byte = 0;
    let mut three_byte = 0;
    let mut four_byte = 0;
    let mut five_byte = 0;

    for &value in test_data {
        let mut buf = Vec::new();
        value.write_as_varint(&mut buf).unwrap();
        match buf.len() {
            1 => single_byte += 1,
            2 => two_byte += 1,
            3 => three_byte += 1,
            4 => four_byte += 1,
            5 => five_byte += 1,
            _ => {}
        }
    }

    let total = test_data.len();
    println!(
        r#"
Encoding Efficiency Breakdown:
- 1-byte encodings: {} ({:.1}%) - values 0-127
- 2-byte encodings: {} ({:.1}%) - values 128-16,383
- 3-byte encodings: {} ({:.1}%) - values 16,384-2,097,151
- 4-byte encodings: {} ({:.1}%) - values 2,097,152-268,435,455
- 5-byte encodings: {} ({:.1}%) - values 268,435,456+
- Average bytes per value: {:.2}"#,
        single_byte,
        single_byte as f64 / total as f64 * 100.0,
        two_byte,
        two_byte as f64 / total as f64 * 100.0,
        three_byte,
        three_byte as f64 / total as f64 * 100.0,
        four_byte,
        four_byte as f64 / total as f64 * 100.0,
        five_byte,
        five_byte as f64 / total as f64 * 100.0,
        (single_byte + two_byte * 2 + three_byte * 3 + four_byte * 4 + five_byte * 5) as f64
            / total as f64,
    );
}

fn generate_comprehensive_test_data() -> Vec<u32> {
    let mut values = Vec::new();

    // Edge cases.
    values.extend([0, 1, u32::MAX]);

    // Single byte values (0-127).
    for i in 0..50 {
        values.push(i);
    }

    // Two byte values (128-16383).
    for i in 0..100 {
        values.push(128 + i * 100);
    }

    // Three byte values (16384-2097151).
    for i in 0..200 {
        values.push(16384 + i * 1000);
    }

    // Four byte values (2097152-268435455).
    for i in 0..100 {
        values.push(2097152 + i * 100000);
    }

    // Five byte values (268435456-u32::MAX).
    for i in 0..50 {
        values.push(268435456 + i * 10000000);
    }

    // Sequential pattern.
    for i in 0..1000 {
        values.push(i * 10);
    }

    // Pseudo-random pattern.
    for i in 0u32..500 {
        values.push(i.wrapping_mul(1103515245).wrapping_add(12345));
    }

    values
}
