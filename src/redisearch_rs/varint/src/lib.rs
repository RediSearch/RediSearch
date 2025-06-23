/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Variable-length encoding for integer types, often shortened to "varint encoding".
//!
//! # Usecase
//!
//! Integer types have a fixed size, known upfront—e.g. a `u32` will always be
//! 4 bytes long, no matter the specific value we're working with.
//! Knowing the size upfront unlocks a variety of processing optimizations, but
//! we're trading speed for memory usage.
//!
//! Varint encoding goes in the opposite direction: it uses a value-dependent number of bytes
//! for each integer.
//! This can have a negative impact on processing speed, but it can greatly reduce the
//! storage requirements if most of the integers you're working with are small in magnitude.
//!
//! # Encoding scheme
//!
//! Each integer is represented as a sequence of byte-sized blocks.
//! The most-significant bit in each block is used by the encoding scheme as its
//! **continuation bit**.
//! If the continuation bit is set to 0, the current block is the last block.
//! If the continuation bit is set to 1, we must read the next byte-sized block.
//!
//! # Example
//!
//! All examples will use big-endian representation for bytes.
//!
//! # One block
//!
//! Let's take 10 as an example.
//! As a `u32`, 10 is represented by these four bytes:
//!
//! ```text
//! 00 00 00 00
//! 00 00 00 00
//! 00 00 00 00
//! 00 00 10 10
//! ```
//!
//! When varint-encoded, it only requires one byte:
//!
//! ```text
//! 00 00 10 10
//! ^
//! the continuation bit
//! ```
//!
//! The continuation bit is set to zero, since a single block is enough.
//!
//! # Multi-block
//!
//! Let's now look at 129.
//! As a `u32`, 129 is represented by these four bytes:
//!
//! ```text
//! 00 00 00 00
//! 00 00 00 00
//! 00 00 00 00
//! 10 00 00 01
//! ```
//!
//! When varint-encoded, it requires two bytes:
//!
//! ```text
//! the first continuation bit
//! ∨
//! 10 00 00 00
//! 00 00 00 01
//! ^
//! the second continuation bit
//! ```
//!
//! The first block has its continuation bit set to one, thus signalling that
//! we need to keep reading.

mod vector_writer;
use std::io::{Read, Write};
pub use vector_writer::VectorWriter;

/// A convenient function to read a varint-encoded integer from the given reader.
pub fn read<T, R>(reader: &mut R) -> Result<T, std::io::Error>
where
    R: Read,
    T: VarintEncode,
{
    T::read_as_varint(reader)
}

/// Utilities to varint encode/decode an integer.
pub trait VarintEncode {
    /// Encode an integer in varint format, then write it to the given writer.
    /// It returns the number of bytes written.
    fn write_as_varint<W>(self, writer: W) -> Result<usize, std::io::Error>
    where
        W: Write,
        Self: Sized;

    /// Read a varint-encoded integer from the given reader.
    fn read_as_varint<R>(reader: &mut R) -> Result<Self, std::io::Error>
    where
        R: Read,
        Self: Sized;
}

macro_rules! impl_encode {
    ($ty:ident, $size:literal) => {
        impl VarintEncode for $ty {
            fn write_as_varint<W>(mut self, mut writer: W) -> Result<usize, std::io::Error>
            where
                W: Write,
            {
                // We need an auxiliary buffer to store the encoded blocks while
                // we process the integer.
                // We can't use the writer directly because we process the
                // integer in *reverse order*, starting from its least significant byte.
                // The first block we write will be the last block we read when decoding.
                let mut buffer = [0; $size];
                // We write into the buffer starting from the end, shifting left
                // for every new byte-sized block.
                // By the end of the process, the blocks are stored in the buffer
                // in the expected order and can be sent down to the writer.
                let mut pos = $size - 1;
                // Extract the 7 least significant bits from the current value.
                // The continuation bit is set to zero, since this will be
                // the last block we read when decoding.
                buffer[pos] = (self & 0b0111_1111) as u8;
                // Then shift right to discard the processed bits.
                self >>= 7;
                // If the remaining bits are all zeros, we're done.
                // If not, we need to continue processing.
                while self != 0 {
                    pos -= 1;
                    // A little optimization!
                    // The continuation bit does encode one bit (duh!) of information:
                    // that we have another block coming after the current one.
                    // We can "reuse" that bit of information to compress the value range
                    // further: we subtract 1 from the current value for every extra block
                    // beyond the first.
                    // E.g. this allows us to encode 16511 using two blocks rather than three.
                    self -= 1;
                    buffer[pos] =
                        // Since we're now past the last block, we need to set all continuation
                        // bits to 1.
                        0b1000_0000 |
                        // Extract again the 7 least significant bits..
                        ((self & 0b0111_1111) as u8);
                    // ..then discard them.
                    self >>= 7;
                }

                let encoded = &buffer[pos..];
                writer.write_all(encoded)?;
                Ok(encoded.len())
            }

            fn read_as_varint<R>(reader: &mut R) -> Result<Self, std::io::Error>
            where
                R: Read,
            {
                // We'll use this auxiliary buffer to pull
                // bytes one at a time from the reader,
                // depending on the value of the continuation bit
                // for the current block.
                let mut buffer = [0; 1];
                // First block!
                let mut c: u8 = {
                    reader.read_exact(&mut buffer)?;
                    buffer[0]
                };

                // We extract the 7 least significant bits.
                // We store them directly in a variable of the
                // expected integer size. We'll progressively shift
                // bits to the left to make space for the
                // coming blocks.
                let mut val: Self = (c & 0b0111_1111) as _;
                // Is the continuation bit set?
                while c & 0b1000_0000 != 0 {
                    // Mirror the range optimization
                    // done on the encoding side.
                    val += 1;
                    c = {
                        reader.read_exact(&mut buffer)?;
                        buffer[0]
                    };
                    val =
                        // Shift the bits we have extracted
                        // so far to the left...
                        (val << 7)
                        // To make space for the 7 value bits
                        // from the current block.
                        | ((c & 0b0111_1111) as Self);
                }

                Ok(val)
            }
        }
    };
}

// The maximum number of bytes required to encode a u32 is 5.
impl_encode!(u32, 8);
// The maximum number of bytes required to encode a u64 is 10.
impl_encode!(u64, 16);
// The maximum number of bytes required to encode a u128 is 19.
impl_encode!(u128, 24);
