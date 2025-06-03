/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use encode_decode::varint;
use std::hint::black_box;
use varint_bencher::{FieldMask, c_varint::c_varint_ops};

// Force linking of Redis mock symbols by referencing the lib.
extern crate varint_bencher;

fn main() {
    compute_and_report_memory_usage();
}

/// Generate test data and build varint encodings using both Rust and C implementations.
/// Report memory usage and encoding efficiency.
fn compute_and_report_memory_usage() {
    let test_data = generate_comprehensive_test_data();
    let raw_size = test_data.len() * 4; // u32 = 4 bytes each.

    // Encode with Rust implementation.
    let mut rust_writer = varint::VectorWriter::new(test_data.len());
    for &value in &test_data {
        let _ = rust_writer.write(black_box(value));
    }
    let rust_encoded_size = rust_writer.bytes_len();

    // Encode with C implementation.
    let mut c_writer = varint_bencher::c_varint::CVarintVectorWriter::new(test_data.len());
    for &value in &test_data {
        c_writer.write(black_box(value));
    }
    let c_encoded_size = c_writer.bytes_len();

    // Field mask encoding comparison.
    let field_masks: Vec<FieldMask> = test_data.iter().map(|&v| v as FieldMask).collect();

    let mut rust_field_size = 0;
    for &mask in &field_masks {
        let mut buf = Vec::new();
        varint::write_field_mask(black_box(mask), &mut buf).unwrap();
        rust_field_size += buf.len();
    }

    let mut c_field_size = 0;
    for &mask in &field_masks {
        let encoded = c_varint_ops::write_field_mask_to_vec(black_box(mask));
        c_field_size += encoded.len();
    }

    // Report statistics.
    println!(
        r#"Varint Encoding Statistics:
- Raw data size: {:.3} KB ({} u32 values)
- Rust varint encoding: {:.3} KB ({:.2}x compression)
- C varint encoding: {:.3} KB ({:.2}x compression)
- Rust field mask encoding: {:.3} KB
- C field mask encoding: {:.3} KB"#,
        raw_size as f64 / 1024.0,
        test_data.len(),
        rust_encoded_size as f64 / 1024.0,
        raw_size as f64 / rust_encoded_size as f64,
        c_encoded_size as f64 / 1024.0,
        raw_size as f64 / c_encoded_size as f64,
        rust_field_size as f64 / 1024.0,
        c_field_size as f64 / 1024.0,
    );

    // Efficiency comparison.
    if rust_encoded_size == c_encoded_size {
        println!("- Varint implementations produce identical output sizes");
    } else {
        let diff_pct =
            ((rust_encoded_size as f64 - c_encoded_size as f64) / c_encoded_size as f64) * 100.0;
        if rust_encoded_size < c_encoded_size {
            println!(
                "- Rust varint encoding is {:.1}% more space efficient",
                -diff_pct
            );
        } else {
            println!(
                "- C varint encoding is {:.1}% more space efficient",
                diff_pct
            );
        }
    }

    if rust_field_size == c_field_size {
        println!("- Field mask implementations produce identical output sizes");
    } else {
        let field_diff_pct =
            ((rust_field_size as f64 - c_field_size as f64) / c_field_size as f64) * 100.0;
        if rust_field_size < c_field_size {
            println!(
                "- Rust field mask encoding is {:.1}% more space efficient",
                -field_diff_pct
            );
        } else {
            println!(
                "- C field mask encoding is {:.1}% more space efficient",
                field_diff_pct
            );
        }
    }

    println!("\nRun `cargo bench` for detailed performance comparisons.");
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
