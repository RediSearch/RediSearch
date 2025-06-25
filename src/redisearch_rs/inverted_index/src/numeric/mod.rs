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
//! The text encoding, middle 2 bits of header, can only ever be one of these four values:
//!
//! | Bits | Type     | Description           |
//! |------|----------|-----------------------|
//! | `00` | TINY     | Small integers 0-7    |
//! | `01` | FLOAT    | Floating-point values |
//! | `10` | POS_INT  | Positive integers > 7 |
//! | `11` | NEG_INT  | Negative integers     |
//!
//! #### TINY Type (00) - Bits 5-7 {#tiny-type}
//!
//! ```text
//! Bit:   7   6   5
//!      ┌───┬───┬───┐
//!      │ V │ V │ V │  Value (0-7)
//!      └───┴───┴───┘
//! ```
//! The value is encoded directly in bits 5-7. No additional value bytes
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
//! #### POS_INT Type (10) - Bits 5-7 {#pos-int-type}
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
//! #### NEG_INT Type (11) - Bits 5-7 {#neg-int-type}
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
//!      │  └─ Type: POS_INT (10)
//!      └─ Value bytes: 1 (001) (ie 2 bytes are used for the value)

use std::io::{IoSlice, Read, Write};

use ffi::t_docId;

use crate::{Decoder, DecoderResult, Delta, Encoder, RSIndexResult, RSResultType};

/// Trait to convert various types to and from byte representations for numeric encoding / decoding.
trait ToFromBytes<const N: usize> {
    /// Packs self into a byte vector.
    fn pack(self) -> [u8; N];
}

impl ToFromBytes<{ size_of::<usize>() }> for Delta {
    #[inline(always)]
    fn pack(self) -> [u8; size_of::<usize>()] {
        let delta = self.0;
        delta.to_le_bytes()
    }
}

pub struct Numeric;

impl Numeric {
    const TINY_TYPE: u8 = 0b00;
    const FLOAT_TYPE: u8 = 0b01;
    const POS_INT_TYPE: u8 = 0b10;
    const NEG_INT_TYPE: u8 = 0b11;
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

        let delta = delta.pack();
        let end = delta.iter().rposition(|&b| b != 0).map_or(0, |pos| pos + 1);
        let delta = &delta[..end];

        let num_record = unsafe { &record.data.num };

        let bytes_written = match FloatValue::from(num_record.0) {
            FloatValue::Tiny(i) => {
                let header = Header {
                    delta_bytes: delta.len() as _,
                    typ: HeaderType::Tiny(i),
                };

                writer.write_vectored(&[IoSlice::new(&header.pack()), IoSlice::new(delta)])?
            }
            FloatValue::PosInt(i) => {
                let bytes = i.to_le_bytes();
                let end = bytes.iter().rposition(|&b| b != 0).map_or(0, |pos| pos + 1);

                let bytes = &bytes[..end];

                let header = Header {
                    delta_bytes: delta.len() as _,
                    typ: HeaderType::PositiveInteger((end - 1) as _),
                };

                writer.write_vectored(&[
                    IoSlice::new(&header.pack()),
                    IoSlice::new(delta),
                    IoSlice::new(bytes),
                ])?
            }
            FloatValue::NegInt(i) => {
                let bytes = i.to_le_bytes();
                let end = bytes.iter().rposition(|&b| b != 0).map_or(0, |pos| pos + 1);

                let bytes = &bytes[..end];

                let header = Header {
                    delta_bytes: delta.len() as _,
                    typ: HeaderType::NegativeInteger((end - 1) as _),
                };

                writer.write_vectored(&[
                    IoSlice::new(&header.pack()),
                    IoSlice::new(delta),
                    IoSlice::new(bytes),
                ])?
            }
            FloatValue::F32Pos(value) => {
                let bytes = value.to_le_bytes();

                let header = Header {
                    delta_bytes: delta.len() as _,
                    typ: HeaderType::Float {
                        is_infinite: false,
                        is_negative: false,
                        is_f64: false,
                    },
                };

                writer.write_vectored(&[
                    IoSlice::new(&header.pack()),
                    IoSlice::new(delta),
                    IoSlice::new(&bytes),
                ])?
            }
            FloatValue::F32Neg(value) => {
                let bytes = value.to_le_bytes();

                let header = Header {
                    delta_bytes: delta.len() as _,
                    typ: HeaderType::Float {
                        is_infinite: false,
                        is_negative: true,
                        is_f64: false,
                    },
                };

                writer.write_vectored(&[
                    IoSlice::new(&header.pack()),
                    IoSlice::new(delta),
                    IoSlice::new(&bytes),
                ])?
            }
            FloatValue::F64Pos(value) => {
                let bytes = value.to_le_bytes();

                let header = Header {
                    delta_bytes: delta.len() as _,
                    typ: HeaderType::Float {
                        is_infinite: false,
                        is_negative: false,
                        is_f64: true,
                    },
                };

                writer.write_vectored(&[
                    IoSlice::new(&header.pack()),
                    IoSlice::new(delta),
                    IoSlice::new(&bytes),
                ])?
            }
            FloatValue::F64Neg(value) => {
                let bytes = value.to_le_bytes();

                let header = Header {
                    delta_bytes: delta.len() as _,
                    typ: HeaderType::Float {
                        is_infinite: false,
                        is_negative: true,
                        is_f64: true,
                    },
                };

                writer.write_vectored(&[
                    IoSlice::new(&header.pack()),
                    IoSlice::new(delta),
                    IoSlice::new(&bytes),
                ])?
            }
            FloatValue::Infinity => {
                let header = Header {
                    delta_bytes: delta.len() as _,
                    typ: HeaderType::Float {
                        is_infinite: true,
                        is_negative: false,
                        is_f64: false,
                    },
                };

                writer.write_vectored(&[IoSlice::new(&header.pack()), IoSlice::new(delta)])?
            }
            FloatValue::NegInfinity => {
                let header = Header {
                    delta_bytes: delta.len() as _,
                    typ: HeaderType::Float {
                        is_infinite: true,
                        is_negative: true,
                        is_f64: false,
                    },
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
        let delta_bytes = header & 0b111;
        let type_bits = (header >> 3) & 0b11;
        let upper_bits = header >> 5;

        let delta = read_usize(&mut reader, delta_bytes as _)?;
        let delta = Delta(delta);

        let num = match type_bits {
            Self::TINY_TYPE => upper_bits as f64,
            Self::FLOAT_TYPE => match upper_bits {
                0b000 => read_f32(&mut reader)? as _,
                0b010 => read_f32(&mut reader)?.copysign(-1.0) as _,
                0b100 => read_f64(&mut reader)?,
                0b110 => read_f64(&mut reader)?.copysign(-1.0),
                0b101 | 0b001 => f64::INFINITY,
                0b111 | 0b011 => f64::NEG_INFINITY,
                _ => unreachable!("All upper bits combinations are covered"),
            },
            Self::POS_INT_TYPE => read_u64(&mut reader, upper_bits as usize + 1)? as _,
            Self::NEG_INT_TYPE => {
                let v = read_u64(&mut reader, upper_bits as usize + 1)?;
                (v as f64).copysign(-1.0)
            }
            _ => unreachable!("All four possible combinations are covered"),
        };

        let doc_id = base + (delta.0 as u64);
        let record = RSIndexResult::numeric(doc_id, num);

        Ok(Some(DecoderResult::Record(record)))
    }
}

fn read_usize<R: Read>(reader: &mut R, len: usize) -> std::io::Result<usize> {
    let mut bytes = [0; size_of::<usize>()];
    reader.read_exact(&mut bytes[..len])?;
    Ok(usize::from_le_bytes(bytes))
}

fn read_u64<R: Read>(reader: &mut R, len: usize) -> std::io::Result<u64> {
    let mut bytes = [0; 8];
    reader.read_exact(&mut bytes[..len])?;
    Ok(u64::from_le_bytes(bytes))
}

fn read_f32<R: Read>(reader: &mut R) -> std::io::Result<f32> {
    let mut bytes = [0; 4];
    reader.read_exact(&mut bytes)?;
    Ok(f32::from_le_bytes(bytes))
}

fn read_f64<R: Read>(reader: &mut R) -> std::io::Result<f64> {
    let mut bytes = [0; 8];
    reader.read_exact(&mut bytes)?;
    Ok(f64::from_le_bytes(bytes))
}

enum FloatValue {
    Tiny(u8),
    PosInt(u64),
    NegInt(u64),
    F32Pos(f32),
    F32Neg(f32),
    F64Pos(f64),
    F64Neg(f64),
    Infinity,
    NegInfinity,
}

impl From<f64> for FloatValue {
    fn from(value: f64) -> Self {
        let abs_val = value.abs();
        let u64_val = abs_val as u64;

        if u64_val as f64 == abs_val {
            if u64_val <= 0b111 {
                return FloatValue::Tiny(u64_val as u8);
            } else if value.is_sign_positive() {
                return FloatValue::PosInt(u64_val);
            } else {
                return FloatValue::NegInt(u64_val);
            }
        } else {
            match value {
                f64::INFINITY => FloatValue::Infinity,
                f64::NEG_INFINITY => FloatValue::NegInfinity,
                v => {
                    let f32_value = abs_val as f32;
                    let back_to_f64 = f32_value as f64;

                    if back_to_f64 == abs_val {
                        if v.is_sign_positive() {
                            FloatValue::F32Pos(f32_value)
                        } else {
                            FloatValue::F32Neg(f32_value)
                        }
                    } else {
                        if v.is_sign_positive() {
                            FloatValue::F64Pos(abs_val)
                        } else {
                            FloatValue::F64Neg(abs_val)
                        }
                    }
                }
            }
        }
    }
}

enum HeaderType {
    Tiny(u8),
    Float {
        is_infinite: bool,
        is_negative: bool,
        is_f64: bool,
    },
    PositiveInteger(u8),
    NegativeInteger(u8),
}

/// Binary header for numeric encoding. See the [header layout](self#header-layout) for the bit layout used.
struct Header {
    delta_bytes: u8,
    typ: HeaderType,
}

impl Header {
    const TINY_TYPE: u8 = 0b00;
    const FLOAT_TYPE: u8 = 0b01;
    const POS_INT_TYPE: u8 = 0b10;
    const NEG_INT_TYPE: u8 = 0b11;
}

impl ToFromBytes<1> for Header {
    #[inline(always)]
    fn pack(self) -> [u8; 1] {
        let mut packed = 0;
        packed |= self.delta_bytes & 0b111; // 3 bits for delta bytes

        match self.typ {
            HeaderType::Tiny(t) => {
                packed |= Self::TINY_TYPE << 3; // 2 bits for type
                packed |= (t & 0b111) << 5; // 3 bits for value
            }
            HeaderType::PositiveInteger(b) => {
                packed |= Self::POS_INT_TYPE << 3; // 2 bits for type
                packed |= (b & 0b111) << 5; // 3 bits for value bytes
            }
            HeaderType::NegativeInteger(b) => {
                packed |= Self::NEG_INT_TYPE << 3; // 2 bits for type
                packed |= (b & 0b111) << 5; // 3 bits for value bytes
            }
            HeaderType::Float {
                is_infinite,
                is_negative,
                is_f64,
            } => {
                packed |= Self::FLOAT_TYPE << 3; // 2 bits for type

                if is_infinite {
                    packed |= 1 << 5;
                }
                if is_negative {
                    packed |= 1 << 6;
                }
                if is_f64 {
                    packed |= 1 << 7;
                }
            }
        }

        [packed]
    }
}

#[cfg(test)]
mod tests;
