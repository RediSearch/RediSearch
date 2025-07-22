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

/// Show detailed differences between two byte arrays.
fn show_byte_differences(rust_bytes: &[u8], c_bytes: &[u8], description: &str) {
    println!("  {} byte-by-byte comparison:", description);
    println!(
        "    Rust length: {}, C length: {}",
        rust_bytes.len(),
        c_bytes.len()
    );

    if rust_bytes.len() != c_bytes.len() {
        println!("    Different lengths detected");
    }

    let min_len = rust_bytes.len().min(c_bytes.len());
    let mut differences = 0;

    for i in 0..min_len {
        if rust_bytes[i] != c_bytes[i] {
            if differences < 5 {
                // Show first 5 differences.
                println!(
                    "    Byte {}: Rust=0x{:02x}, C=0x{:02x}",
                    i, rust_bytes[i], c_bytes[i]
                );
            }
            differences += 1;
        }
    }

    if differences > 5 {
        println!("    ... and {} more differences", differences - 5);
    } else if differences == 0 && rust_bytes.len() != c_bytes.len() {
        println!("    All common bytes identical, but different lengths");
    } else if differences == 0 {
        println!("    No differences found (this shouldn't happen)");
    } else {
        println!("    Total differences: {}", differences);
    }

    // Show first few bytes of each for context.
    let show_bytes = 16.min(rust_bytes.len()).min(c_bytes.len());
    if show_bytes > 0 {
        print!("    First {} bytes - Rust: ", show_bytes);
        for i in 0..show_bytes {
            print!("{:02x} ", rust_bytes[i]);
        }
        println!();
        print!("    First {} bytes - C:    ", show_bytes);
        for i in 0..show_bytes {
            print!("{:02x} ", c_bytes[i]);
        }
        println!();
    }
}

fn main() {
    verify_implementations();
    compute_and_report_memory_usage();
}

/// Verify that Rust and C implementations produce identical results for basic test cases.
fn verify_implementations() {
    let test_values = [0, 1, 127, 128, 16383, 16384, u32::MAX];

    for &value in &test_values {
        // Test varint encoding.
        let mut rust_buf = Vec::new();
        varint::write(value, &mut rust_buf).unwrap();

        let c_encoded = varint_bencher::c_varint::c_varint_ops::write_to_vec(value);

        if rust_buf != c_encoded {
            panic!(
                "Varint encoding mismatch for value {}: Rust={:?}, C={:?}",
                value, rust_buf, c_encoded
            );
        }

        // Test field mask encoding.
        let field_mask = value as FieldMask;
        let mut rust_field_buf = Vec::new();
        varint::write_field_mask(field_mask, &mut rust_field_buf).unwrap();

        let c_field_encoded =
            varint_bencher::c_varint::c_varint_ops::write_field_mask_to_vec(field_mask);

        if rust_field_buf != c_field_encoded {
            panic!(
                "Field mask encoding mismatch for value {}: Rust={:?}, C={:?}",
                field_mask, rust_field_buf, c_field_encoded
            );
        }
    }

    println!(
        "✓ Implementation verification passed for {} test values",
        test_values.len()
    );
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
    let rust_encoded_bytes = rust_writer.bytes();

    // Encode with C implementation.
    let mut c_writer = varint_bencher::c_varint::CVarintVectorWriter::new(test_data.len());
    for &value in &test_data {
        c_writer.write(black_box(value));
    }
    let c_encoded_size = c_writer.bytes_len();
    let c_encoded_bytes = c_writer.bytes();

    // Field mask encoding comparison.
    let field_masks: Vec<FieldMask> = test_data.iter().map(|&v| v as FieldMask).collect();

    let mut rust_field_bytes = Vec::new();
    let mut rust_field_size = 0;
    for &mask in &field_masks {
        let mut buf = Vec::new();
        varint::write_field_mask(black_box(mask), &mut buf).unwrap();
        rust_field_size += buf.len();
        rust_field_bytes.extend_from_slice(&buf);
    }

    let mut c_field_bytes = Vec::new();
    let mut c_field_size = 0;
    for &mask in &field_masks {
        let encoded = c_varint_ops::write_field_mask_to_vec(black_box(mask));
        c_field_size += encoded.len();
        c_field_bytes.extend_from_slice(&encoded);
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

    // Correctness verification.
    let varint_identical = rust_encoded_bytes == c_encoded_bytes;
    let field_mask_identical = rust_field_bytes == c_field_bytes;

    if varint_identical {
        println!("- Varint implementations produce identical encoded output");
    } else {
        show_byte_differences(rust_encoded_bytes, c_encoded_bytes, "Varint");
    }

    if field_mask_identical {
        println!("- Field mask implementations produce identical encoded output");
    } else {
        show_byte_differences(&rust_field_bytes, &c_field_bytes, "Field mask");
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
