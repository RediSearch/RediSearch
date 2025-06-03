# Varint Benchmarks

A benchmarking suite for comparing varint (variable-length integer) implementations
between Rust and C, focusing on memory efficiency and space usage.

## Overview

This crate provides:
- **Memory usage analysis**: Compare space efficiency between Rust and C varint
  implementations
- **Compression ratios**: Analyze how well varint encoding compresses different
  value ranges
- **Implementation correctness**: Verify that Rust and C implementations produce
  identical results
- **Performance benchmarks**: Use `cargo bench` for detailed timing analysis

## Quick Start

1. **Build C libraries** (from repository root):
   ```bash
   make build
   ```

2. **Run memory analysis**:
   ```bash
   cd src/redisearch_rs/varint_bencher
   cargo run --release
   ```

3. **Run performance benchmarks**:
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
Varint Encoding Statistics:
- Raw data size: 7.824 KB (2003 u32 values)
- Rust varint encoding: 4.451 KB (1.76x compression)
- C varint encoding: 4.451 KB (1.76x compression)
- Rust field mask encoding: 5.817 KB
- C field mask encoding: 5.817 KB
- Varint implementations produce identical output sizes
- Field mask implementations produce identical output sizes

Run `cargo bench` for detailed performance comparisons.
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
- u128 field mask values converted from test integers
- Comparison between Rust and C field mask encoding implementations

## Performance Benchmarks

For detailed timing analysis, use criterion benchmarks:

```bash
# All benchmarks
cargo bench

# View HTML reports
open target/criterion/report/index.html
```

## C Integration

The tool requires C libraries built with `make build`. If C integration fails:

- **Build fails**: Missing C libraries - run `make build` first
- **Runtime panic**: C function calls fail - indicates FFI linking issues

The memory analysis focuses on correctness and space efficiency rather than
trying to replicate criterion's statistical timing analysis.

## Project Structure

```
varint_bencher/
├── src/
│   ├── lib.rs           # Public API and FFI integration
│   ├── main.rs          # Memory usage analysis binary
│   └── c_varint/        # C implementation wrappers
├── benches/             # Criterion performance benchmarks
├── build.rs             # C library detection and FFI generation
└── Cargo.toml
```

## Development

The tool separates concerns:
- **Memory analysis**: Handled by the main binary (space efficiency, compression
  ratios)
- **Performance analysis**: Handled by `cargo bench` (statistical timing with
  criterion)
- **Correctness**: Verified by comparing Rust and C output for identical results

This approach avoids duplicating criterion's sophisticated timing infrastructure
while providing valuable space usage insights.