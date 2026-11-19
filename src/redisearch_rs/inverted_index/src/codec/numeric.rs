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
//! │ (1 byte)    │ (0-7 bytes) │ (0-9 bytes) │
//! └─────────────┴─────────────┴─────────────┘
//! ```
//!
//! 1. **Header byte** (1 byte) - always present, encodes type information and metadata
//! 2. **Delta bytes** (0-7 bytes) - document ID delta from previous record, may be empty
//! 3. **Value bytes** (0-9 bytes) - the numeric value if not encoded in header, may be empty
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
//!        │   │   └─── Infinite flag (decimal when F64 is set too)
//!        │   └─────── Negative flag
//!        └─────────── F64 flag (0=f32, 1=f64)
//! ```
//!
//! These flags allow for the following combinations:
//!
//! | F64 | Neg | Inf | Value Bytes | Description      |
//! |-----|-----|-----|-------------|------------------|
//! | 0   | 0   | 0   | 4           | Positive f32     |
//! | 0   | 1   | 0   | 4           | Negative f32     |
//! | 1   | 0   | 0   | 8           | Positive f64     |
//! | 1   | 1   | 0   | 8           | Negative f64     |
//! | 0   | 0   | 1   | 0           | +∞               |
//! | 0   | 1   | 1   | 0           | -∞               |
//! | 1   | 0   | 1   | 2-9         | Positive decimal |
//! | 1   | 1   | 1   | 2-9         | Negative decimal |
//!
//! Here value bytes is how many bytes follow the delta to represent the value. An f32 or f64
//! writes its IEEE 754 bytes, an infinity writes none, and a decimal writes an info byte
//! followed by its mantissa, laid out [below](#decimal-value).
//!
//! #### Decimal value bytes {#decimal-value}
//!
//! A decimal stores the magnitude as an integer mantissa and a power-of-ten scale, so that
//! `magnitude = mantissa / 10^scale` reproduces the original `f64` bit for bit. The value
//! section starts with one info byte, followed by the mantissa in little-endian order with
//! trailing zero bytes trimmed:
//!
//! ```text
//! Bit:   7   6   5   4   3   2   1   0
//!      ┌───┬───┐ ┌───┬───┬───┐ ┌───┬───┬───┐
//!      │ 0 │ 0 │ │   Scale   │ │ Length - 1│
//!      └───┴───┘ └───┴───┴───┘ └───┴───┴───┘
//! ```
//!
//! The scale is at most [`Decimal::MAX_SCALE`]. An encoder only picks this representation
//! when it is exact and shorter than the float it would otherwise write.
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
//! ```

use std::io::{Cursor, IoSlice, Read, Write};

use rqe_core::DocId;

use crate::{Decoder, Encoder, IdDelta, NumericDecoder};
use index_result::RSIndexResult;

/// Trait to convert various types to byte representations for numeric encoding
trait ToBytes<const N: usize> {
    /// Packs self into a byte vector.
    fn pack(self) -> [u8; N];
}

/// The base numeric decoder/encoder which follows the encoding format described in the module
/// documentation.
#[derive(Debug)]
pub struct Numeric;

impl Numeric {
    const TINY_TYPE: u8 = 0b00;
    const FLOAT_TYPE: u8 = 0b01;
    const INT_POS_TYPE: u8 = 0b10;
    const INT_NEG_TYPE: u8 = 0b11;
}

/// Like the base [`Numeric`] encoder, but attempts to compress float values to f32 when possible.
/// This is done by checking if the float value can be represented as f32 without loss of precision,
/// or if the difference between the f64 and f32 representation is below a certain threshold.
/// An exact [decimal](self#decimal-value) still wins when it is shorter than that f32, so
/// short decimals keep their exact value with compression on.
#[derive(Debug)]
pub struct NumericFloatCompression;

impl NumericFloatCompression {
    const FLOAT_COMPRESSION_THRESHOLD: f64 = 0.01;
}

/// An encoder that decides how it will represent a numeric value before writing it.
///
/// The representation cannot always hold the value it was given —
/// [`NumericFloatCompression`] stores an `f32` when the precision loss is below
/// [its threshold](NumericFloatCompression::FLOAT_COMPRESSION_THRESHOLD), and even
/// [`Numeric`] normalizes `-0.0` to `+0.0` by encoding it as a tiny integer. That
/// decision is made inside [`Encoder::encode`], so anything derived from a value —
/// cardinality, bounds, routing — must be derived from the [`StoredValue`] instead,
/// or it describes a value the index never returns.
///
/// [`prepare`](Self::prepare) exposes the decision on its own and
/// [`encode_prepared`](Self::encode_prepared) takes it back, so a caller that needs
/// the outcome up front does not make the encoder work it out twice.
///
/// Implementers canonicalize: two inputs represented as the same number produce the
/// same [`StoredValue`], so distinct stored values are numerically distinct. Callers
/// that separate values — a cardinality estimate deciding when to split, say — can
/// rely on that instead of second-guessing the representation.
pub trait NumericEncoder: Encoder {
    /// Decide how `value` will be represented. Writes nothing.
    fn prepare(value: f64) -> PreparedValue;

    /// Write a value whose representation has already been decided.
    ///
    /// Equivalent to [`Encoder::encode`] on a record holding the same value, down to
    /// the bytes.
    fn encode_prepared<W: Write + std::io::Seek>(
        writer: W,
        delta: Self::Delta,
        prepared: PreparedValue,
    ) -> std::io::Result<usize>;

    /// The value a reader will decode after `value` is encoded by this encoder.
    ///
    /// Idempotent: the stored form of a stored value is itself.
    fn stored_value(value: f64) -> f64 {
        Self::prepare(value).stored_value().get()
    }
}

impl NumericEncoder for Numeric {
    fn prepare(value: f64) -> PreparedValue {
        PreparedValue(Value::from(value, false))
    }

    fn encode_prepared<W: Write + std::io::Seek>(
        writer: W,
        delta: Self::Delta,
        prepared: PreparedValue,
    ) -> std::io::Result<usize> {
        encode_value(writer, delta, prepared.0)
    }
}

impl NumericEncoder for NumericFloatCompression {
    fn prepare(value: f64) -> PreparedValue {
        PreparedValue(Value::from(value, true))
    }

    fn encode_prepared<W: Write + std::io::Seek>(
        writer: W,
        delta: Self::Delta,
        prepared: PreparedValue,
    ) -> std::io::Result<usize> {
        encode_value(writer, delta, prepared.0)
    }
}

/// How a numeric encoder will represent a value: decided, but not yet written.
///
/// Not parameterized by encoder on purpose: the byte layout records which
/// representation was used, so either numeric encoder can write a decision the other
/// one made, and any numeric decoder reads it back. Preparing with a different
/// encoder than the index uses is therefore harmless, if pointless.
#[derive(Debug, Clone, Copy)]
pub struct PreparedValue(Value);

impl PreparedValue {
    /// The value a reader will decode for this representation.
    pub const fn stored_value(self) -> StoredValue {
        StoredValue(self.0.to_f64())
    }
}

/// A numeric value in the exact form an index stores — and therefore returns — it.
///
/// Distinct from a plain `f64` so that an input value on its way in cannot be
/// mistaken for one the index will hand back. See [`NumericEncoder`] for why that
/// distinction matters.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct StoredValue(f64);

impl StoredValue {
    /// Wrap a value that came out of a numeric index, which is already in stored
    /// form.
    ///
    /// The caller owns that provenance: pass only values read back through a numeric
    /// [`Decoder`], never an input value on its way in.
    pub const fn from_decoded(value: f64) -> Self {
        Self(value)
    }

    /// The wrapped value.
    pub const fn get(self) -> f64 {
        self.0
    }
}

/// The [`Numeric`] encoder only supports encoding deltas that fit within 7 bytes
#[derive(Debug, PartialEq)]
pub struct NumericDelta(u64);

impl ToBytes<8> for NumericDelta {
    fn pack(self) -> [u8; 8] {
        self.0.to_le_bytes()
    }
}

impl IdDelta for NumericDelta {
    fn from_u64(delta: u64) -> Option<Self> {
        if (delta >> (7 * 8)) > 0 {
            // If the delta is larger than 7 bytes (7 * 8), then we cannot encode it with this encoder.
            // The inverted index should create a new block in this case.
            None
        } else {
            Some(Self(delta))
        }
    }

    fn zero() -> Self {
        Self(0)
    }
}

impl NumericDelta {
    /// Get the value this delta type is wrapping
    pub const fn inner(&self) -> u64 {
        self.0
    }
}

impl Encoder for Numeric {
    type Delta = NumericDelta;

    const ALLOW_DUPLICATES: bool = true;

    fn encode<W: Write + std::io::Seek>(
        writer: W,
        delta: Self::Delta,
        record: &RSIndexResult,
    ) -> std::io::Result<usize> {
        encode(writer, delta, record, false)
    }
}

impl Encoder for NumericFloatCompression {
    type Delta = NumericDelta;

    const ALLOW_DUPLICATES: bool = true;

    fn encode<W: Write + std::io::Seek>(
        writer: W,
        delta: Self::Delta,
        record: &RSIndexResult,
    ) -> std::io::Result<usize> {
        encode(writer, delta, record, true)
    }
}

fn encode<W: Write + std::io::Seek>(
    writer: W,
    delta: NumericDelta,
    record: &RSIndexResult,
    compress_floats: bool,
) -> std::io::Result<usize> {
    let num_record = record
        .as_numeric()
        .expect("numeric encoder will only be called for numeric records");

    encode_value(writer, delta, Value::from(num_record, compress_floats))
}

/// Write a value whose representation has already been decided — the single writer
/// behind both [`Encoder::encode`] and [`NumericEncoder::encode_prepared`].
fn encode_value<W: Write + std::io::Seek>(
    mut writer: W,
    delta: NumericDelta,
    value: Value,
) -> std::io::Result<usize> {
    let delta = delta.pack();

    // Trim trailing zeros from delta so that we store as little as possible
    let end = delta.iter().rposition(|&b| b != 0).map_or(0, |pos| pos + 1);
    let delta = &delta[..end];
    let delta_bytes = delta.len() as _;

    let bytes_written = match value {
        Value::TinyInteger(i) => {
            let header = Header {
                delta_bytes,
                typ: HeaderType::Tiny(i),
            };

            write_all_vectored(
                &mut writer,
                [IoSlice::new(&header.pack()), IoSlice::new(delta)],
            )?
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

            write_all_vectored(
                &mut writer,
                [
                    IoSlice::new(&header.pack()),
                    IoSlice::new(delta),
                    IoSlice::new(bytes),
                ],
            )?
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

            write_all_vectored(
                &mut writer,
                [
                    IoSlice::new(&header.pack()),
                    IoSlice::new(delta),
                    IoSlice::new(bytes),
                ],
            )?
        }
        Value::Float32Positive(value) => {
            let bytes = value.to_le_bytes();

            let header = Header {
                delta_bytes,
                typ: HeaderType::Float32Positive,
            };

            write_all_vectored(
                &mut writer,
                [
                    IoSlice::new(&header.pack()),
                    IoSlice::new(delta),
                    IoSlice::new(&bytes),
                ],
            )?
        }
        Value::Float32Negative(value) => {
            let bytes = value.to_le_bytes();

            let header = Header {
                delta_bytes,
                typ: HeaderType::Float32Negative,
            };

            write_all_vectored(
                &mut writer,
                [
                    IoSlice::new(&header.pack()),
                    IoSlice::new(delta),
                    IoSlice::new(&bytes),
                ],
            )?
        }
        Value::Float64Positive(value) => {
            let bytes = value.to_le_bytes();

            let header = Header {
                delta_bytes,
                typ: HeaderType::Float64Positive,
            };

            write_all_vectored(
                &mut writer,
                [
                    IoSlice::new(&header.pack()),
                    IoSlice::new(delta),
                    IoSlice::new(&bytes),
                ],
            )?
        }
        Value::Float64Negative(value) => {
            let bytes = value.to_le_bytes();

            let header = Header {
                delta_bytes,
                typ: HeaderType::Float64Negative,
            };

            write_all_vectored(
                &mut writer,
                [
                    IoSlice::new(&header.pack()),
                    IoSlice::new(delta),
                    IoSlice::new(&bytes),
                ],
            )?
        }
        Value::FloatInfinity => {
            let header = Header {
                delta_bytes,
                typ: HeaderType::FloatInfinite,
            };

            write_all_vectored(
                &mut writer,
                [IoSlice::new(&header.pack()), IoSlice::new(delta)],
            )?
        }
        Value::FloatNegInfinity => {
            let header = Header {
                delta_bytes,
                typ: HeaderType::FloatNegInfinite,
            };

            write_all_vectored(
                &mut writer,
                [IoSlice::new(&header.pack()), IoSlice::new(delta)],
            )?
        }
        Value::DecimalPositive(decimal) | Value::DecimalNegative(decimal) => {
            let typ = if matches!(value, Value::DecimalPositive(_)) {
                HeaderType::DecimalPositive
            } else {
                HeaderType::DecimalNegative
            };
            let header = Header { delta_bytes, typ };
            let mut bytes = [0; 9];
            bytes[0] = decimal.info();
            bytes[1..].copy_from_slice(&decimal.mantissa.to_le_bytes());

            write_all_vectored(
                &mut writer,
                [
                    IoSlice::new(&header.pack()),
                    IoSlice::new(delta),
                    IoSlice::new(&bytes[..1 + decimal.mantissa_bytes()]),
                ],
            )?
        }
    };

    Ok(bytes_written)
}

impl Decoder for Numeric {
    /// Decode a numeric record from the given cursor, using the provided base document ID.
    /// The result is written into the provided `RSIndexResult` instance.
    ///
    /// # Safety
    ///
    /// 1. `result.is_numeric()` must be true to ensure `result` is holding numeric data.
    #[inline(always)]
    fn decode<'index>(
        cursor: &mut Cursor<&'index [u8]>,
        base: DocId,
        result: &mut RSIndexResult<'index>,
    ) -> std::io::Result<()> {
        decode(cursor, base, result)
    }

    fn base_result<'index>() -> RSIndexResult<'index> {
        RSIndexResult::build_numeric(0.0).build()
    }
}

impl Decoder for NumericFloatCompression {
    /// Decode a numeric record from the given cursor, using the provided base document ID.
    /// The result is written into the provided `RSIndexResult` instance.
    ///
    /// # Safety
    ///
    /// 1. `result.is_numeric()` must be true to ensure `result` is holding numeric data.
    #[inline(always)]
    fn decode<'index>(
        cursor: &mut Cursor<&'index [u8]>,
        base: DocId,
        result: &mut RSIndexResult<'index>,
    ) -> std::io::Result<()> {
        decode(cursor, base, result)
    }

    fn base_result<'index>() -> RSIndexResult<'index> {
        RSIndexResult::build_numeric(0.0).build()
    }
}

fn decode<'index>(
    cursor: &mut Cursor<&'index [u8]>,
    base: DocId,
    result: &mut RSIndexResult<'index>,
) -> Result<(), std::io::Error> {
    let mut header = [0; 1];
    cursor.read_exact(&mut header)?;

    let header = header[0];
    let delta_bytes = (header & 0b111) as usize;
    let type_bits = (header >> 3) & 0b11;
    let upper_bits = header >> 5;

    let (delta, num) = match type_bits {
        Numeric::TINY_TYPE => {
            let delta = read_only_u64(cursor, delta_bytes)?;
            let num = upper_bits;

            (delta, num as f64)
        }
        Numeric::INT_POS_TYPE => {
            let (delta, num) = read_u64_and_u64(cursor, delta_bytes, upper_bits as usize + 1)?;

            (delta, num as f64)
        }
        Numeric::INT_NEG_TYPE => {
            let (delta, num) = read_u64_and_u64(cursor, delta_bytes, upper_bits as usize + 1)?;

            (delta, (num as f64).copysign(-1.0))
        }
        Numeric::FLOAT_TYPE => match upper_bits {
            FLOAT32_POSITIVE => {
                let (delta, num) = read_u64_and_f32(cursor, delta_bytes)?;

                (delta, num as f64)
            }
            FLOAT32_NEGATIVE => {
                let (delta, num) = read_u64_and_f32(cursor, delta_bytes)?;

                (delta, num.copysign(-1.0) as f64)
            }
            FLOAT64_POSITIVE => {
                let (delta, num) = read_u64_and_f64(cursor, delta_bytes)?;

                (delta, num)
            }
            FLOAT64_NEGATIVE => {
                let (delta, num) = read_u64_and_f64(cursor, delta_bytes)?;

                (delta, num.copysign(-1.0))
            }
            FLOAT_INFINITE => {
                let delta = read_only_u64(cursor, delta_bytes)?;

                (delta, f64::INFINITY)
            }
            FLOAT_NEGATIVE_INFINITE => {
                let delta = read_only_u64(cursor, delta_bytes)?;

                (delta, f64::NEG_INFINITY)
            }
            DECIMAL_POSITIVE => read_u64_and_decimal(cursor, delta_bytes)?,
            DECIMAL_NEGATIVE => {
                let (delta, num) = read_u64_and_decimal(cursor, delta_bytes)?;

                (delta, num.copysign(-1.0))
            }
            _ => unreachable!("All upper bits combinations are covered"),
        },
        _ => unreachable!("All four possible combinations are covered"),
    };

    let doc_id = base + delta;

    result.doc_id = doc_id;
    // SAFETY: Caller must ensure `result` is numeric
    unsafe {
        *result.as_numeric_unchecked_mut() = num;
    }

    Ok(())
}

#[inline(always)]
fn read_only_u64<R: Read>(reader: &mut R, len: usize) -> std::io::Result<u64> {
    let mut bytes = [0; 8];
    reader.read_exact(&mut bytes[..len])?;
    Ok(u64::from_le_bytes(bytes))
}

#[inline(always)]
fn read_u64_and_u64<R: Read>(
    reader: &mut R,
    first_bytes: usize,
    second_bytes: usize,
) -> std::io::Result<(u64, u64)> {
    let mut buffer = [0; 16];
    let total_bytes = first_bytes + second_bytes;

    // Use one read since it is faster
    reader.read_exact(&mut buffer[..total_bytes])?;

    let first = u64::from_slice(&buffer[..first_bytes]);
    let second = u64::from_slice(&buffer[first_bytes..first_bytes + second_bytes]);

    Ok((first, second))
}

#[inline(always)]
fn read_u64_and_f32<R: Read>(reader: &mut R, first_bytes: usize) -> std::io::Result<(u64, f32)> {
    let mut buffer = [0; 12];
    let total_bytes = first_bytes + 4;

    // Use one read since it is faster
    reader.read_exact(&mut buffer[..total_bytes])?;

    let first = u64::from_slice(&buffer[..first_bytes]);
    let second = f32::from_slice(&buffer[first_bytes..first_bytes + 4]);

    Ok((first, second))
}

#[inline(always)]
fn read_u64_and_f64<R: Read>(reader: &mut R, first_bytes: usize) -> std::io::Result<(u64, f64)> {
    let mut buffer = [0; 16];
    let total_bytes = first_bytes + 8;

    // Use one read since it is faster
    reader.read_exact(&mut buffer[..total_bytes])?;

    let first = u64::from_slice(&buffer[..first_bytes]);
    let second = f64::from_slice(&buffer[first_bytes..first_bytes + 8]);

    Ok((first, second))
}

/// Reads a delta followed by a [decimal value](self#decimal-value), returning its magnitude.
///
/// The info byte decides how many mantissa bytes follow, so reading through [`Read`] would
/// take two variable-length reads. Instead this loads one fixed window straight from the
/// cursor's buffer, which a delta, info byte and mantissa always fit in, and extracts the
/// fields with shifts. Records too close to the end of the buffer for the window, which can
/// be several short ones, take the bounds-checked path instead.
#[inline(always)]
fn read_u64_and_decimal(
    cursor: &mut Cursor<&[u8]>,
    delta_bytes: usize,
) -> std::io::Result<(u64, f64)> {
    const WINDOW: usize = 16;
    let mask = |bytes: usize| (1u128 << (8 * bytes)) - 1;

    let start = cursor.position() as usize;
    let rest = cursor.get_ref().get(start..).unwrap_or_default();

    let (delta, decimal, total) = if let Some(window) = rest.first_chunk::<WINDOW>() {
        let window = u128::from_le_bytes(*window);
        let info = (window >> (8 * delta_bytes)) as u8;
        let mantissa_bytes = (info & 0b111) as usize + 1;
        let mantissa = (window >> (8 * (delta_bytes + 1))) & mask(mantissa_bytes);
        let decimal = Decimal {
            scale: (info >> 3) & 0b111,
            mantissa: mantissa as u64,
        };
        (
            (window & mask(delta_bytes)) as u64,
            decimal,
            delta_bytes + 1 + mantissa_bytes,
        )
    } else {
        let eof = || std::io::Error::from(std::io::ErrorKind::UnexpectedEof);
        let info = *rest.get(delta_bytes).ok_or_else(eof)?;
        let mantissa_bytes = (info & 0b111) as usize + 1;
        let total = delta_bytes + 1 + mantissa_bytes;
        let bytes = rest.get(..total).ok_or_else(eof)?;
        let decimal = Decimal {
            scale: (info >> 3) & 0b111,
            mantissa: u64::from_slice(&bytes[delta_bytes + 1..]),
        };
        (u64::from_slice(&bytes[..delta_bytes]), decimal, total)
    };

    cursor.set_position((start + total) as u64);
    Ok((delta, decimal.to_f64()))
}

/// Helper trait to convert from byte slices to various types
trait FromSlice {
    /// Creates an instance of Self from a byte slice.
    fn from_slice(slice: &[u8]) -> Self;
}

impl FromSlice for u64 {
    #[inline(always)]
    fn from_slice(slice: &[u8]) -> Self {
        debug_assert!(slice.len() <= 8, "Slice length must be at most 8 bytes");

        let mut bytes = [0; 8];
        bytes[..slice.len()].copy_from_slice(slice);
        u64::from_le_bytes(bytes)
    }
}

impl FromSlice for f32 {
    #[inline(always)]
    fn from_slice(slice: &[u8]) -> Self {
        debug_assert!(slice.len() == 4, "Slice length must be exactly 4 bytes");

        let mut bytes = [0; 4];
        bytes.copy_from_slice(slice);
        f32::from_le_bytes(bytes)
    }
}

impl FromSlice for f64 {
    #[inline(always)]
    fn from_slice(slice: &[u8]) -> Self {
        debug_assert!(slice.len() == 8, "Slice length must be exactly 8 bytes");

        let mut bytes = [0; 8];
        bytes.copy_from_slice(slice);
        f64::from_le_bytes(bytes)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
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
    DecimalPositive(Decimal),
    DecimalNegative(Decimal),
}

/// The magnitude of a non-integer value, stored as `mantissa / 10^scale`. See the
/// [decimal layout](self#decimal-value).
#[derive(Debug, Clone, Copy, PartialEq)]
struct Decimal {
    /// Number of decimal places, in `1..=`[`Decimal::MAX_SCALE`].
    scale: u8,
    /// At most [`Decimal::MANTISSA_LIMIT`], so the conversion to `f64` is exact.
    mantissa: u64,
}

impl Decimal {
    /// The largest scale the three scale bits of the info byte can hold.
    const MAX_SCALE: u8 = 7;

    /// Every integer below this is exactly representable as an `f64`.
    const MANTISSA_LIMIT: f64 = (1u64 << f64::MANTISSA_DIGITS) as f64;

    /// Indexed by scale. Every entry is exact in an `f64`, which is what lets
    /// [`Decimal::exact`] and [`Decimal::to_f64`] agree bit for bit.
    const POWERS_OF_TEN: [f64; Self::MAX_SCALE as usize + 1] =
        [1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7];

    /// A decimal that decodes back to exactly `magnitude`, at the smallest scale found.
    ///
    /// Makes a single exactness check, at the largest scale whose mantissa stays below
    /// [`Decimal::MANTISSA_LIMIT`]: a magnitude that is exact at a smaller scale is exact
    /// there too, because both decode the same rational and division rounds it the same
    /// way. Trailing zeros of the mantissa then give the smaller scale back. One check
    /// keeps the cost flat for values with no decimal form, which fail it.
    ///
    /// Close to the limit, rounding `magnitude * 10^scale` can land on the wrong integer and
    /// miss a decimal form the value has; the caller then keeps the float. The check is on
    /// the decoded value itself, so it never accepts an inexact decimal.
    fn exact(magnitude: f64) -> Option<Self> {
        for scale in (1..=Self::MAX_SCALE).rev() {
            let power = Self::POWERS_OF_TEN[scale as usize];
            let scaled = magnitude * power;
            if scaled.is_nan() {
                return None;
            }
            if scaled >= Self::MANTISSA_LIMIT {
                continue;
            }
            // Cheaper than `f64::round`, which is a libm call on baseline x86-64. It can
            // be off by one where adding a half rounds, and the check below then rejects
            // the decimal instead of storing a wrong one.
            let mantissa = (scaled + 0.5) as u64;
            if mantissa as f64 / power != magnitude {
                return None;
            }

            let mut decimal = Self { scale, mantissa };
            while decimal.scale > 1 && decimal.mantissa.is_multiple_of(10) {
                decimal.mantissa /= 10;
                decimal.scale -= 1;
            }
            return Some(decimal);
        }
        None
    }

    /// Shared by [`Value::to_f64`] and [`decode`], so the value an index predicts it
    /// stores is the value a reader gets back.
    const fn to_f64(self) -> f64 {
        self.mantissa as f64 / Self::POWERS_OF_TEN[self.scale as usize]
    }

    const fn mantissa_bytes(self) -> usize {
        let significant = (u64::BITS - self.mantissa.leading_zeros()) as usize;
        let bytes = significant.div_ceil(8);
        if bytes == 0 { 1 } else { bytes }
    }

    /// The info byte, see the [decimal layout](self#decimal-value).
    const fn info(self) -> u8 {
        (self.scale << 3) | (self.mantissa_bytes() as u8 - 1)
    }
}

impl Value {
    fn from(value: f64, compress_floats: bool) -> Self {
        let abs_val = value.abs();
        let u64_val = abs_val as u64;

        if u64_val as f64 == abs_val {
            let tiny_int = u64_val & 0b111;

            if tiny_int as f64 == value {
                Value::TinyInteger(u64_val as u8)
            } else if value.is_sign_negative() {
                Value::IntegerNegative(u64_val)
            } else {
                Value::IntegerPositive(u64_val)
            }
        } else {
            match value {
                f64::INFINITY => Value::FloatInfinity,
                f64::NEG_INFINITY => Value::FloatNegInfinity,
                v => {
                    let float = Float::new(abs_val, compress_floats);

                    match Decimal::exact(abs_val) {
                        Some(decimal) if 1 + decimal.mantissa_bytes() < float.value_bytes() => {
                            if v.is_sign_positive() {
                                Value::DecimalPositive(decimal)
                            } else {
                                Value::DecimalNegative(decimal)
                            }
                        }
                        _ => float.with_sign_of(v),
                    }
                }
            }
        }
    }

    /// The `f64` a reader decodes for this representation.
    ///
    /// Mirrors [`decode`] arm for arm rather than sharing code with it: `decode` is on
    /// the read hot path and rebuilds the number straight from the bytes. The
    /// `numeric_stored_value_matches_encode_decode` proptest keeps the two in step.
    const fn to_f64(self) -> f64 {
        match self {
            Value::TinyInteger(i) => i as f64,
            Value::IntegerPositive(i) => i as f64,
            Value::IntegerNegative(i) => (i as f64).copysign(-1.0),
            Value::Float32Positive(f) => f as f64,
            Value::Float32Negative(f) => f.copysign(-1.0) as f64,
            Value::Float64Positive(f) => f,
            Value::Float64Negative(f) => f.copysign(-1.0),
            Value::FloatInfinity => f64::INFINITY,
            Value::FloatNegInfinity => f64::NEG_INFINITY,
            Value::DecimalPositive(d) => d.to_f64(),
            Value::DecimalNegative(d) => d.to_f64().copysign(-1.0),
        }
    }
}

/// The float representation of a magnitude that is neither an integer nor infinite. A
/// [`Decimal`] replaces it only when shorter.
#[derive(Debug, Clone, Copy)]
enum Float {
    /// Compression rounded the magnitude onto zero.
    Zero,
    F32(f32),
    F64(f64),
}

impl Float {
    const fn new(abs_val: f64, compress_floats: bool) -> Self {
        let f32_value = abs_val as f32;
        let back_to_f64 = f32_value as f64;

        if back_to_f64 == abs_val
            || (compress_floats
                && (abs_val - f32_value as f64).abs()
                    < NumericFloatCompression::FLOAT_COMPRESSION_THRESHOLD)
        {
            // A magnitude that rounds to zero has collapsed *onto* zero, so
            // store the canonical zero — the representation a literal `0.0`
            // gets — rather than a signed f32 zero. Otherwise two inputs this
            // encoder decided to represent as zero become two stored values
            // that compare equal, and every consumer would have to know which
            // stored values are secretly the same value.
            //
            // Only reachable with compression on: without it this branch needs
            // `back_to_f64 == abs_val`, which for a zero `f32_value` means
            // `abs_val` is zero — already claimed by the integer branch of [`Value::from`].
            if f32_value == 0.0 {
                Self::Zero
            } else {
                Self::F32(f32_value)
            }
        } else {
            Self::F64(abs_val)
        }
    }

    /// How many bytes this representation writes after the delta.
    const fn value_bytes(self) -> usize {
        match self {
            Self::Zero => 0,
            Self::F32(_) => size_of::<f32>(),
            Self::F64(_) => size_of::<f64>(),
        }
    }

    /// The [`Value`] holding this magnitude with the sign of `v`.
    const fn with_sign_of(self, v: f64) -> Value {
        match self {
            Self::Zero => Value::TinyInteger(0),
            Self::F32(f) if v.is_sign_positive() => Value::Float32Positive(f),
            Self::F32(f) => Value::Float32Negative(f),
            Self::F64(f) if v.is_sign_positive() => Value::Float64Positive(f),
            Self::F64(f) => Value::Float64Negative(f),
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
    DecimalPositive,
    DecimalNegative,
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
const DECIMAL_POSITIVE: u8 = 0b101;
const DECIMAL_NEGATIVE: u8 = 0b111;

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
            HeaderType::DecimalPositive => {
                packed |= Numeric::FLOAT_TYPE << 3; // 2 bits for type
                packed |= DECIMAL_POSITIVE << 5; // 3 bits for positive decimal
            }
            HeaderType::DecimalNegative => {
                packed |= Numeric::FLOAT_TYPE << 3; // 2 bits for type
                packed |= DECIMAL_NEGATIVE << 5; // 3 bits for negative decimal
            }
        }

        [packed]
    }
}

/// Writes all slices in `bufs` to the writer, advancing the slices as they are written.
#[inline(always)]
fn write_all_vectored<const N: usize, W: Write>(
    writer: &mut W,
    mut bufs: [IoSlice<'_>; N],
) -> std::io::Result<usize> {
    let total_len = bufs.iter().map(|b| b.len()).sum();

    // In theory we only need the code in the `Ok(n)` branch. However, that performs slow when
    // the buffers being written are small (less than 13 bytes). Using a profiler shows that the
    // `write_vectored` call inside the `OK(n)` branch is optimized differently from the one on
    // this match (next line), for reasons that are currently unclear.
    match writer.write_vectored(&bufs) {
        Ok(n) if n == total_len => return Ok(n),
        Ok(0) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "failed to fill whole buffer",
            ));
        }
        Ok(n) => {
            // Partial write, fall back to loop
            let mut bufs = bufs.as_mut_slice();
            IoSlice::advance_slices(&mut bufs, n);

            // Could not write everything in one go, fall back to loop
            while !bufs.is_empty() {
                match writer.write_vectored(bufs) {
                    Ok(0) => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "failed to fill whole buffer",
                        ));
                    }
                    Ok(n) => IoSlice::advance_slices(&mut bufs, n),
                    Err(e) => return Err(e),
                }
            }
        }
        Err(e) => return Err(e),
    }

    Ok(total_len)
}

impl NumericDecoder for Numeric {}

impl NumericDecoder for NumericFloatCompression {}

#[cfg(test)]
mod tests {
    use super::*;

    // Make sure negative zero is encoded as a tiny integer 0
    #[test]
    fn from_negative_zero() {
        let neg_zero = -0.0f64;
        let value = Value::from(neg_zero, false);

        assert_eq!(value, Value::TinyInteger(0));
    }
}
