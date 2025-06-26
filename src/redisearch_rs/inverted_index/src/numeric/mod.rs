/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Numeric encoding for [`RSIndexResult`] records.
//!
//! This module implements a compact binary encoding for numeric values that optimizes
//! for common cases while supporting the full range of floating-point values.
//!
//! # Encoding Format
//!
//! Each encoded record consists of three sections that may be empty:
//!
//! ```text
//! ┌─────────────┬─────────────┬─────────────┐
//! │ Header      │ Delta       │ Value       │
//! │ (1 byte)    │ (0-7 bytes) │ (0-8 bytes) │
//! └─────────────┴─────────────┴─────────────┘
//! ```
//!
//! 1. **Header byte** (1 byte) - always present, encodes type information and metadata
//! 2. **Delta bytes** (0-7 bytes) - document ID delta from previous record, may be empty
//! 3. **Value bytes** (0-8 bytes) - the numeric value if not encoded in header, may be empty
//!
//! ## Header Byte Layout {#header-layout}
//! The header, which is the first section, always takes up one byte and is laid out as follows:
//!
//! ```text
//! Bit:    7    6    5      4    3      2    1    0
//!       ┌────┬────┬────┐ ┌────┬────┐ ┌────┬────┬────┐
//!       │ Type-specific│ │ Type    │ │ Delta bytes  │
//!       │    (5-7)     │ │  (3-4)  │ │    (0-2)     │
//!       └────┴────┴────┘ └────┴────┘ └────┴────┴────┘
//! ```
//!
//! ### Delta bytes
//! The delta bytes, bits 0-2 of the header, indicate how many bytes follow the header to represent
//! the delta. Value from 0-7 are allowed, meaning 0-7 bytes can be used for the delta.
//!
//! ### Type Encoding (bits 3-4)
//! The type encoding, middle 2 bits of header, can only ever be one of these four values:
//!
//! | Bits | Type     | Description           |
//! |------|----------|-----------------------|
//! | `00` | TINY     | Small integers 0-7    |
//! | `01` | FLOAT    | Floating-point values |
//! | `10` | INT_POS  | Positive integers > 7 |
//! | `11` | INT_NEG  | Negative integers     |
//!
//! #### TINY Type (00) - Bits 5-7 {#tiny-type}
//!
//! ```text
//! Bit:   7   6   5
//!      ┌───┬───┬───┐
//!      │ V │ V │ V │  Value (0-7)
//!      └───┴───┴───┘
//! ```
//! The value is encoded directly in bits 5-7 of the header. No additional value bytes
//! will appear after the delta.
//!
//! #### FLOAT Type (01) - Bits 5-7 {#float-type}
//!
//! ```text
//! Bit:   7   6   5
//!      ┌───┬───┬───┐
//!      │ F │ N │ I │
//!      └───┴───┴───┘
//!        │   │   └─── Infinite flag
//!        │   └─────── Negative flag
//!        └─────────── F64 flag (0=f32, 1=f64)
//! ```
//!
//! These flags allows for the following combinations:
//!
//! | F64 | Neg | Inf | Value Bytes | Description  |
//! |-----|-----|-----|-------------|--------------|
//! | 0   | 0   | 0   | 4           | Positive f32 |
//! | 0   | 1   | 0   | 4           | Negative f32 |
//! | 1   | 0   | 0   | 8           | Positive f64 |
//! | 1   | 1   | 0   | 8           | Negative f64 |
//! | 0   | 0   | 1   | 0           | +∞           |
//! | 0   | 1   | 1   | 0           | -∞           |
//! | 1   | 0   | 1   | 0           | *Unused*     |
//! | 1   | 1   | 1   | 0           | *Unused*     |
//!
//! Here value bytes indicate how many bytes follow the delta to represent the value.
//! This means for f32 values, 4 bytes will follow the delta, and for f64 values, 8 bytes will
//! follow. While the infinite flag indicates no value will follow the delta.
//!
//! #### INT_POS Type (10) - Bits 5-7 {#pos-int-type}
//!
//! ```text
//! Bit:   7   6   5
//!      ┌───┬───┬───┐
//!      │ L │ L │ L │  Length - 1 (0-7)
//!      └───┴───┴───┘
//! ```
//! Encodes `(value_bytes - 1)` where value_bytes is 1-8. This determines how many bytes follow the
//! delta to represent the actual positive integer value.
//!
//! #### INT_NEG Type (11) - Bits 5-7 {#neg-int-type}
//!
//! ```text
//! Bit:   7   6   5
//!      ┌───┬───┬───┐
//!      │ L │ L │ L │  Length - 1 (0-7)
//!      └───┴───┴───┘
//! ```
//! Encodes `(value_bytes - 1)` where value_bytes is 1-8. This determines how many bytes follow the
//! delta to represent the actual negative integer value (stored as positive magnitude).
//!
//! ## Examples
//!
//! ```text
//! Value: 5.0, Delta: 2
//! ┌─────────────┬─────────────┐
//! │ Header      │ Delta       │ (no Value section)
//! │ 0b101_00_001│ 0b00000010  │
//! └─────────────┴─────────────┘
//!      │  │   │   └─ Delta: 2 (taking up 1 byte)
//!      │  │   └─ Delta bytes: 1
//!      │  └─ Type: TINY (00)
//!      └─ Value: 5 (101)
//! ```
//!
//! ```text
//! Value: 256.0, Delta: 10
//! ┌─────────────┬─────────────┬─────────────┬─────────────┐
//! │ Header      │ Delta       │ Value       │ Value       │
//! │ 0b001_10_001│ 0b00001010  │ 0b00000000  │ 0b00000001  │
//! └─────────────┴─────────────┴─────────────┴─────────────┘
//!      │  │   │   │             │             │
//!      │  │   │   └─ Delta: 10  └─ Value LSB  └─ Value MSB
//!      │  │   └─ Delta bytes: 1                  (256 = 0x0100)
//!      │  └─ Type: INT_POS (10)
//!      └─ Value bytes: 1 (001) (ie 2 bytes are used for the value)

use std::io::{IoSlice, Read, Write};

use ffi::t_docId;

use crate::{Decoder, DecoderResult, Delta, Encoder, RSIndexResult, RSResultType};

/// Trait to convert various types to byte representations for numeric encoding
trait ToBytes<const N: usize> {
    /// Packs self into a byte vector.
    fn pack(self) -> [u8; N];
}

impl ToBytes<{ size_of::<usize>() }> for Delta {
    fn pack(self) -> [u8; size_of::<usize>()] {
        let delta = self.0;
        delta.to_le_bytes()
    }
}

pub struct Numeric;

impl Numeric {
    const TINY_TYPE: u8 = 0b00;
    const FLOAT_TYPE: u8 = 0b01;
    const INT_POS_TYPE: u8 = 0b10;
    const INT_NEG_TYPE: u8 = 0b11;
}

impl Encoder for Numeric {
    fn encode<W: Write + std::io::Seek>(
        mut writer: W,
        delta: Delta,
        record: &RSIndexResult,
    ) -> std::io::Result<usize> {
        if matches!(
            record.result_type,
            RSResultType::Union
                | RSResultType::Intersection
                | RSResultType::Term
                | RSResultType::Virtual
                | RSResultType::HybridMetric
        ) {
            panic!("Numeric encoding only supports numeric types")
        }

        // SAFETY: We are guaranteed that the record is a numeric type because of the matches! above
        let num_record = unsafe { &record.data.num };

        let delta = delta.pack();

        // Trim trailing zeros from delta so that we store as little as possible
        let end = delta.iter().rposition(|&b| b != 0).map_or(0, |pos| pos + 1);
        let delta = &delta[..end];
        let delta_bytes = delta.len() as _;

        let bytes_written = match Value::from(num_record.0) {
            Value::TinyInteger(i) => {
                let header = Header {
                    delta_bytes,
                    typ: HeaderType::Tiny(i),
                };

                writer.write_vectored(&[IoSlice::new(&header.pack()), IoSlice::new(delta)])?
            }
            Value::IntegerPositive(i) => {
                let bytes = i.to_le_bytes();

                // Trim trailing zeros from bytes to store as little as possible
                let end = bytes.iter().rposition(|&b| b != 0).map_or(0, |pos| pos + 1);
                let bytes = &bytes[..end];

                let header = Header {
                    delta_bytes,
                    typ: HeaderType::IntegerPositive((end - 1) as _),
                };

                writer.write_vectored(&[
                    IoSlice::new(&header.pack()),
                    IoSlice::new(delta),
                    IoSlice::new(bytes),
                ])?
            }
            Value::IntegerNegative(i) => {
                let bytes = i.to_le_bytes();

                // Trim trailing zeros from bytes to store as little as possible
                let end = bytes.iter().rposition(|&b| b != 0).map_or(0, |pos| pos + 1);
                let bytes = &bytes[..end];

                let header = Header {
                    delta_bytes,
                    typ: HeaderType::IntegerNegative((end - 1) as _),
                };

                writer.write_vectored(&[
                    IoSlice::new(&header.pack()),
                    IoSlice::new(delta),
                    IoSlice::new(bytes),
                ])?
            }
            Value::Float32Positive(value) => {
                let bytes = value.to_le_bytes();

                let header = Header {
                    delta_bytes,
                    typ: HeaderType::Float32Positive,
                };

                writer.write_vectored(&[
                    IoSlice::new(&header.pack()),
                    IoSlice::new(delta),
                    IoSlice::new(&bytes),
                ])?
            }
            Value::Float32Negative(value) => {
                let bytes = value.to_le_bytes();

                let header = Header {
                    delta_bytes,
                    typ: HeaderType::Float32Negative,
                };

                writer.write_vectored(&[
                    IoSlice::new(&header.pack()),
                    IoSlice::new(delta),
                    IoSlice::new(&bytes),
                ])?
            }
            Value::Float64Positive(value) => {
                let bytes = value.to_le_bytes();

                let header = Header {
                    delta_bytes,
                    typ: HeaderType::Float64Positive,
                };

                writer.write_vectored(&[
                    IoSlice::new(&header.pack()),
                    IoSlice::new(delta),
                    IoSlice::new(&bytes),
                ])?
            }
            Value::Float64Negative(value) => {
                let bytes = value.to_le_bytes();

                let header = Header {
                    delta_bytes,
                    typ: HeaderType::Float64Negative,
                };

                writer.write_vectored(&[
                    IoSlice::new(&header.pack()),
                    IoSlice::new(delta),
                    IoSlice::new(&bytes),
                ])?
            }
            Value::FloatInfinity => {
                let header = Header {
                    delta_bytes,
                    typ: HeaderType::FloatInfinite,
                };

                writer.write_vectored(&[IoSlice::new(&header.pack()), IoSlice::new(delta)])?
            }
            Value::FloatNegInfinity => {
                let header = Header {
                    delta_bytes,
                    typ: HeaderType::FloatNegInfinite,
                };

                writer.write_vectored(&[IoSlice::new(&header.pack()), IoSlice::new(delta)])?
            }
        };

        Ok(bytes_written)
    }
}

impl Decoder for Numeric {
    fn decode<R: Read>(
        &self,
        mut reader: R,
        base: t_docId,
    ) -> std::io::Result<Option<DecoderResult>> {
        let mut header = [0; 1];
        reader.read_exact(&mut header)?;

        let header = header[0];
        let delta_bytes = (header & 0b111) as usize;
        let type_bits = (header >> 3) & 0b11;
        let upper_bits = header >> 5;

        let (delta, num) = match type_bits {
            Self::TINY_TYPE => {
                let delta = read_only_u64(&mut reader, delta_bytes)?;
                let num = upper_bits;

                (delta, num as f64)
            }
            Self::FLOAT_TYPE => match upper_bits {
                FLOAT32_POSITIVE => {
                    let (delta, num) = read_u64_and_f32(&mut reader, delta_bytes)?;

                    (delta, num as f64)
                }
                FLOAT32_NEGATIVE => {
                    let (delta, num) = read_u64_and_f32(&mut reader, delta_bytes)?;

                    (delta, num.copysign(-1.0) as f64)
                }
                FLOAT64_POSITIVE => {
                    let (delta, num) = read_u64_and_f64(&mut reader, delta_bytes)?;

                    (delta, num)
                }
                FLOAT64_NEGATIVE => {
                    let (delta, num) = read_u64_and_f64(&mut reader, delta_bytes)?;

                    (delta, num.copysign(-1.0))
                }
                0b101 | FLOAT_INFINITE => {
                    let delta = read_only_u64(&mut reader, delta_bytes)?;

                    (delta, f64::INFINITY)
                }
                0b111 | FLOAT_NEGATIVE_INFINITE => {
                    let delta = read_only_u64(&mut reader, delta_bytes)?;

                    (delta, f64::NEG_INFINITY)
                }
                _ => unreachable!("All upper bits combinations are covered"),
            },
            Self::INT_POS_TYPE => {
                let (delta, num) =
                    read_u64_and_u64(&mut reader, delta_bytes, upper_bits as usize + 1)?;

                (delta, num as f64)
            }
            Self::INT_NEG_TYPE => {
                let (delta, num) =
                    read_u64_and_u64(&mut reader, delta_bytes, upper_bits as usize + 1)?;

                (delta, (num as f64).copysign(-1.0))
            }
            _ => unreachable!("All four possible combinations are covered"),
        };

        let doc_id = base + delta;
        let record = RSIndexResult::numeric(doc_id, num);

        Ok(Some(DecoderResult::Record(record)))
    }
}

fn read_only_u64<R: Read>(reader: &mut R, len: usize) -> std::io::Result<u64> {
    let mut bytes = [0; 8];
    reader.read_exact(&mut bytes[..len])?;
    Ok(u64::from_le_bytes(bytes))
}

fn read_u64_and_u64<R: Read>(
    reader: &mut R,
    first_bytes: usize,
    second_bytes: usize,
) -> std::io::Result<(u64, u64)> {
    let mut buffer = [0; 16];
    let total_bytes = first_bytes + second_bytes;

    // Use one read since it is faster
    reader.read_exact(&mut buffer[..total_bytes])?;

    let mut first = [0; 8];
    first[..first_bytes].copy_from_slice(&buffer[..first_bytes]);
    let first = u64::from_le_bytes(first);

    let mut second = [0; 8];
    second.copy_from_slice(&buffer[first_bytes..first_bytes + 8]);
    let second = u64::from_le_bytes(second);

    Ok((first, second))
}

fn read_u64_and_f32<R: Read>(reader: &mut R, first_bytes: usize) -> std::io::Result<(u64, f32)> {
    let mut buffer = [0; 12];
    let total_bytes = first_bytes + 4;

    // Use one read since it is faster
    reader.read_exact(&mut buffer[..total_bytes])?;

    let mut first = [0; 8];
    first[..first_bytes].copy_from_slice(&buffer[..first_bytes]);
    let first = u64::from_le_bytes(first);

    let mut second = [0; 4];
    second.copy_from_slice(&buffer[first_bytes..first_bytes + 4]);
    let second = f32::from_le_bytes(second);

    Ok((first, second))
}

fn read_u64_and_f64<R: Read>(reader: &mut R, first_bytes: usize) -> std::io::Result<(u64, f64)> {
    let mut buffer = [0; 16];
    let total_bytes = first_bytes + 8;

    // Use one read since it is faster
    reader.read_exact(&mut buffer[..total_bytes])?;

    let mut first = [0; 8];
    first[..first_bytes].copy_from_slice(&buffer[..first_bytes]);
    let first = u64::from_le_bytes(first);

    let mut second = [0; 8];
    second.copy_from_slice(&buffer[first_bytes..first_bytes + 8]);
    let second = f64::from_le_bytes(second);

    Ok((first, second))
}

enum Value {
    TinyInteger(u8),
    IntegerPositive(u64),
    IntegerNegative(u64),
    Float32Positive(f32),
    Float32Negative(f32),
    Float64Positive(f64),
    Float64Negative(f64),
    FloatInfinity,
    FloatNegInfinity,
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        let abs_val = value.abs();
        let u64_val = abs_val as u64;

        if u64_val as f64 == abs_val {
            if u64_val <= 0b111 {
                return Value::TinyInteger(u64_val as u8);
            } else if value.is_sign_positive() {
                return Value::IntegerPositive(u64_val);
            } else {
                return Value::IntegerNegative(u64_val);
            }
        } else {
            match value {
                f64::INFINITY => Value::FloatInfinity,
                f64::NEG_INFINITY => Value::FloatNegInfinity,
                v => {
                    let f32_value = abs_val as f32;
                    let back_to_f64 = f32_value as f64;

                    if back_to_f64 == abs_val {
                        if v.is_sign_positive() {
                            Value::Float32Positive(f32_value)
                        } else {
                            Value::Float32Negative(f32_value)
                        }
                    } else {
                        if v.is_sign_positive() {
                            Value::Float64Positive(abs_val)
                        } else {
                            Value::Float64Negative(abs_val)
                        }
                    }
                }
            }
        }
    }
}

enum HeaderType {
    Tiny(u8),
    Float32Positive,
    Float32Negative,
    Float64Positive,
    Float64Negative,
    FloatInfinite,
    FloatNegInfinite,
    IntegerPositive(u8),
    IntegerNegative(u8),
}

/// Binary header for numeric encoding. See the [header layout](self#header-layout) for the bit layout used.
struct Header {
    delta_bytes: u8,
    typ: HeaderType,
}

const FLOAT32_POSITIVE: u8 = 0b000;
const FLOAT32_NEGATIVE: u8 = 0b010;
const FLOAT64_POSITIVE: u8 = 0b100;
const FLOAT64_NEGATIVE: u8 = 0b110;
const FLOAT_INFINITE: u8 = 0b001;
const FLOAT_NEGATIVE_INFINITE: u8 = 0b011;

impl ToBytes<1> for Header {
    fn pack(self) -> [u8; 1] {
        let mut packed = 0;
        packed |= self.delta_bytes & 0b111; // 3 bits for delta bytes

        match self.typ {
            HeaderType::Tiny(t) => {
                packed |= Numeric::TINY_TYPE << 3; // 2 bits for type
                packed |= (t & 0b111) << 5; // 3 bits for value
            }
            HeaderType::IntegerPositive(b) => {
                packed |= Numeric::INT_POS_TYPE << 3; // 2 bits for type
                packed |= (b & 0b111) << 5; // 3 bits for value bytes
            }
            HeaderType::IntegerNegative(b) => {
                packed |= Numeric::INT_NEG_TYPE << 3; // 2 bits for type
                packed |= (b & 0b111) << 5; // 3 bits for value bytes
            }
            HeaderType::Float32Positive => {
                packed |= Numeric::FLOAT_TYPE << 3; // 2 bits for type
                packed |= FLOAT32_POSITIVE << 5; // 3 bits for small float
            }
            HeaderType::Float32Negative => {
                packed |= Numeric::FLOAT_TYPE << 3; // 2 bits for type
                packed |= FLOAT32_NEGATIVE << 5; // 3 bits for small negative float
            }
            HeaderType::Float64Positive => {
                packed |= Numeric::FLOAT_TYPE << 3; // 2 bits for type
                packed |= FLOAT64_POSITIVE << 5; // 3 bits for big float
            }
            HeaderType::Float64Negative => {
                packed |= Numeric::FLOAT_TYPE << 3; // 2 bits for type
                packed |= FLOAT64_NEGATIVE << 5; // 3 bits for big negative float
            }
            HeaderType::FloatInfinite => {
                packed |= Numeric::FLOAT_TYPE << 3; // 2 bits for type
                packed |= FLOAT_INFINITE << 5; // 3 bits for infinite
            }
            HeaderType::FloatNegInfinite => {
                packed |= Numeric::FLOAT_TYPE << 3; // 2 bits for type
                packed |= FLOAT_NEGATIVE_INFINITE << 5; // 3 bits for negative infinite
            }
        }

        [packed]
    }
}

#[cfg(test)]
mod tests;
