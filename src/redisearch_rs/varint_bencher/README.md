# Varint Benchmarks

A benchmarking suite for analyzing varint (variable-length integer) encoding
performance and memory efficiency in Rust, focusing on space usage optimization
and encoding characteristics.

## Overview

This crate provides:
- **Memory usage analysis**: Analyze space efficiency of Rust varint encoding
- **Compression ratios**: Analyze how well varint encoding compresses different
  value ranges
- **Performance benchmarks**: Use `cargo bench` for detailed timing analysis

## Quick Start

1. **Run memory analysis**:
   ```bash
   cd src/redisearch_rs/varint_bencher
   cargo run --release
   ```

2. **Run performance benchmarks**:
   ```bash
   cargo bench
   ```

## Memory Usage Analysis

The main binary analyzes space efficiency of varint encoding:

```bash
cargo run --release
```

**Expected output:**
```text
Varint Encoding Analysis:
- Raw data size: 7.824 KB (2003 u32 values)
- Varint encoded size: 4.451 KB (1.76x compression)
- Field mask encoded size: 5.817 KB (1.35x compression)
- Space savings: 43.1% (varint), 25.6% (field mask)

Encoding Efficiency Breakdown:
- 1-byte encodings: 100 (5.0%) - values 0-127
- 2-byte encodings: 200 (10.0%) - values 128-16,383
- 3-byte encodings: 400 (20.0%) - values 16,384-2,097,151
- 4-byte encodings: 200 (10.0%) - values 2,097,152-268,435,455
- 5-byte encodings: 100 (5.0%) - values 268,435,456+
- Average bytes per value: 2.22

Run `cargo bench` for detailed performance benchmarks.
```

## What It Tests

### Varint Encoding
- **1-byte values** (0-127): Optimal compression case
- **2-byte values** (128-16,383): Common medium integers
- **3-byte values** (16,384-2,097,151): Larger integers
- **4-byte values** (2,097,152-268,435,455): Very large integers
- **5-byte values** (268,435,456+): Maximum varint size
- **Edge cases**: 0, 1, u32::MAX
- **Patterns**: Sequential and pseudo-random data

### Field Mask Encoding
- FieldMask values converted from test integers
- Analysis of field mask encoding efficiency

## Performance Benchmarks

For detailed timing analysis, use criterion benchmarks:

```bash
# All benchmarks
cargo bench

# Specific benchmark groups
cargo bench encode
cargo bench decode
cargo bench "vector writer"

# View HTML reports
open target/criterion/report/index.html
```

### Benchmark Groups

- **Encode**: Single varint encoding performance
- **Encode FieldMask**: Field mask encoding performance
- **Decode**: Single varint decoding performance
- **Decode FieldMask**: Field mask decoding performance
- **Vector Writer**: Batch encoding using VectorWriter

## Analysis Features

### Space Efficiency
- **Compression ratios**: How much space is saved compared to raw u32 storage
- **Encoding breakdown**: Distribution of values across different varint sizes
- **Average bytes per value**: Overall encoding efficiency metric

### Performance Characteristics
- **Encoding speed**: How fast values can be encoded
- **Decoding speed**: How fast values can be decoded
- **Memory allocation**: Vector writer performance for batch operations

## Project Structure

```
varint_bencher/
├── src/
│   ├── lib.rs           # Public API
│   ├── main.rs          # Memory usage analysis binary
│   └── bencher.rs       # Performance benchmarking utilities
├── benches/             # Criterion performance benchmarks
└── Cargo.toml
```

## Development

The tool separates concerns:
- **Memory analysis**: Handled by the main binary (space efficiency, compression
  ratios, encoding distribution)
- **Performance analysis**: Handled by `cargo bench` (statistical timing with
  criterion)

## Understanding Varint Encoding

Varint (variable-length integer) encoding uses a compact representation where:
- Small values (0-127) use only 1 byte
- Larger values use additional bytes as needed
- Maximum encoding is 5 bytes for u32 values

This makes varint particularly effective for data sets with many small integer
values, which is common in search indexes and data compression scenarios.
