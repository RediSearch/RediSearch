//! # qint encoding/decoding - From 2 up to 4 integers variable-length encoding scheme
//!
//! The qint encoding scheme is a variable-length encoding scheme for integers. It uses a leading byte to
//! define the number of bytes used to represent the following integers. Based on that leading byte, up to four
//! variable-length integers are encoded. The leading byte encodes each length in a 2-bit field. The first 2 bits 
//! represent the first integer. The next 2 bits represent the second integer, and so on.
//! 
//! As a caller you're interested in the generic [`qint_encode`] and [`qint_decode`] methods. The methods work on [u32] arrays. 
//! To easily invoke [`qint_decode`] see the type aliases for the different sizes: [`QInt2`], [`QInt3`], and [`QInt4`].
//! 
//! ## Usage Example
//! 
//! The following example encodes the two integers `0xFF` and `0x0FF0` into a buffer and then decodes them back. The assertions
//! on the bottom hold.
//! 
//! ```
//! # use std::io::{Cursor, Seek};
//! # use qint::{qint_encode, qint_decode, QInt2};
//! // generate a buffer, cursor and integers
//! let buf = [0u8; 64];
//! let mut cursor = std::io::Cursor::new(buf);
//! let v = [0xFF, 0x0FF0];
//! 
//! // encode and decode the integers
//! let bytes_written = qint_encode(&mut cursor, v).unwrap();
//! cursor.seek(std::io::SeekFrom::Start(0)).unwrap();
//! let (decoded_values, bytes_consumed) = qint_decode::<QInt2, _>(&mut cursor).unwrap();
//! 
//! // these assertions hold
//! assert_eq!(bytes_written, bytes_consumed); 
//! assert_eq!(v, decoded_values);
//! ```
//! 
//! The leading byte leads to two conclusions:
//! 
//! 1. We can only encode up to 4 integers. The leading byte has 8 bits, and each integer takes 2 bits. So the maximum number of integers is 4.
//! 2. The leading byte is interpreted based on the number of integers encoded. That means encode and decode calls must match.
//! 
//! ### Encoding up to four integers
//! 
//! Internally the trait [`AllowedIntegersInQIntEncoding`] is used to restrict the number of integers that can be encoded. The trait
//! is sealed, so you cannot implement it outside of this crate. The trait has a method `ENCODE_SIZE` that returns the number of integers
//! that can be encoded. The trait is implemented for the following types: [QInt2], [QInt3], and [QInt4].
//! 
//! ### Encoding and Decoding must match
//! 
//! A mismatch always means a logical bug. But beside that it can lead to a [std::io::Error] or undefined behavior. Imagine you call with [std::io::Cursor]
//! and you mismatch [QInt2] with [QInt3]. That means the decoding reads a byte more than the encoding. 
//! 
//! - If the buffer ends you get a [std::io::Error] with [std::io::ErrorKind::UnexpectedEof]. 
//! - If the buffer is larger than the encoding, you read random data.
//! 
//! ## Example Encodings
//! 
//! For the following example space separates each four bits for the encoded integers and every two bits for the leading byte.
//! The `|` symbol separates the leading byte and each of the encoded integers.
//! 
//! ### Two integers with a len of 1 byte and 2 bytes
//! 
//! Example values: `a=0xFF, b=0x0FF0`
//! 
//! would have the following bit pattern:
//! 
//! Bit Encoding: `00 01 00 00|1111 1111|0000 1111 1111 0000`
//! 
//! ### Four integers: 1, 2, 3 and 4 bytes
//! 
//! Example values: `a=0xFF, b=0x0FF0, c=0xFF00F0, d=0xFF0000FF`
//!  
//! and 4 bytes for d and has the following bit pattern:
//! 
//! Bit Encoding: `00 01 10 11|1111 1111|0000 1111 1111 0000|1111 1111 0000 0000 1111 0000|1111 1111 0000 0000 0000 0000 1111 1111`


use std::io;

use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write; 

/// type alias for a qint with 2 integers
pub type QInt2 = [u32; 2];

/// type alias for a qint with 3 integers
pub type QInt3 = [u32; 3];

/// type alias for a qint with 4 integers
pub type QInt4 = [u32; 4];

/// Encodes an array of integers into a QInt buffer.
///
/// # Arguments
/// * `cursor` - Must implement [Write] and [Seek], probably a buffer writer
/// * `values` - Array of integers to encode (2, 3, or 4 integers)
///
/// # Returns
/// The number of bytes written to the buffer or an io error
pub fn qint_encode<N: AllowedIntegersInQIntEncoding, W>(
    cursor: &mut W,
    values: N,
) -> Result<usize, std::io::Error>

where
    W: Write + Seek,
{
    let mut leading = 0;
    let mut ret = 0;
    let pos = cursor.stream_position()?;
    ret += cursor.write(b"\0")?; // Write placeholder for leading byte
    for (i, value) in values.into_iter().enumerate() {
        ret += qint_encode_stepwise(&mut leading, cursor, value, i as u32)?;
    }
    cursor.seek(SeekFrom::Start(pos))?;
    cursor.write_all(&[leading])?;
    Ok(ret)
}

/// Decodes a QInt buffer into an array of integers
///
/// # Arguments
/// * `reader` - must implement [Read], probably a buffer reader
/// * `N` - Number of integers to decode (2, 3, or 4)
///
/// # Returns
/// A tuple of (decoded_values as an array, bytes_consumed) or an io error
pub fn qint_decode<N: AllowedIntegersInQIntEncoding, R: Read>(
    reader: &mut R,
) -> Result<(N, usize), std::io::Error> {
    let mut total = 0;

    // Read the leading byte
    let mut leading = [0; 1];
    reader.read_exact(&mut leading)?;
    total += 1;

    // Decode N values based on 2-bit fields in the leading byte
    let mut result = N::zero_initialized();
    for (i, item) in result.iter_mut().enumerate().take(N::ENCODE_SIZE) {
        // Extract 2-bit field for the i-th value
        let bits = (leading[0] >> (i * 2)) & 0x03;
        let (val, bytes) = qint_decode_value(bits, reader)?;
        *item = val;
        total += bytes;
    }

    Ok((result, total))
}

// This macro generates implementations of the AllowedIntegersInQIntEncoding trait for arrays of u32
macro_rules! impl_allowed_integers_in_qint_encoding {
    ($($size:literal),* $(,)?) => {
        $(
            impl AllowedIntegersInQIntEncoding for [u32; $size] {
                const ENCODE_SIZE: usize = $size;

                fn zero_initialized() -> Self {
                    [0; $size]
                }

                fn iter(&self) -> core::slice::Iter<u32> {
                    self[..].iter()
                }

                fn iter_mut(&mut self) -> core::slice::IterMut<u32> {
                    self[..].iter_mut()
                }
            }

            // Also implement Sealed for each array size
            impl private::Sealed for [u32; $size] {}
        )*
    };
}

// Define the trait and private module
mod private {
    pub trait Sealed {}
}

pub trait AllowedIntegersInQIntEncoding: private::Sealed + IntoIterator<Item = u32> {
    const ENCODE_SIZE: usize;
    fn zero_initialized() -> Self;
    fn iter(&self) -> core::slice::Iter<u32>;
    fn iter_mut(&mut self) -> core::slice::IterMut<u32>;
}

// Use the macro to generate impls for [u32; 2], [u32; 3], and [u32; 4]
impl_allowed_integers_in_qint_encoding!(2, 3, 4);

// Internal: Encodes one byte of using qint encoding, called in a loop.
#[inline(always)]
fn qint_encode_stepwise<W>(
    leading: &mut u8,
    cursor: &mut W,
    mut value: u32,
    offset: u32,
) -> Result<usize, io::Error>
where
    W: Write + Seek,
{
    let mut bytes_written: usize = 0;
    loop {
        cursor.write_all(&[value as u8])?;
        bytes_written += 1;
    
        // shift right until we have no more bigger bytes that are non zero
        value >>= 8;
        // do while(value) in c
        if value == 0 {
            break;
        }
    }
    // encode the bit length of our integer into the leading byte.
    // 0 means 1 byte, 1 - 2 bytes, 2 - 3 bytes, 3 - 4 bytes.
    // we encode it at the i*2th place in the leading byte
    *leading |= ((bytes_written-1) as u8) << (offset * 2);
    Ok(bytes_written)
}

/// Internal: Decode an integer value from a buffer based on bit width
///
/// # Parameters
/// * `bit_value` - The number of bits decoded from leading byte (0=1 byte, 1=2 bytes, 2=3 bytes, 3+=4 bytes)
/// * `reader` - The buffer reader
///
/// # Returns
/// A tuple containing (decoded_value, bytes_used)
#[inline(always)]
fn qint_decode_value<R>(bit_value: u8, reader: &mut R) -> Result<(u32, usize), std::io::Error>
where
    R: Read,
{
    match bit_value {
        0 => {
            // 1 byte
            let mut buf = [0; 1];
            reader.read_exact(&mut buf)?;
            Ok((buf[0] as u32, 1))
        }
        1 => {
            // 2 bytes
            let mut buf = [0; 2];
            reader.read_exact(&mut buf)?;
            Ok((u16::from_ne_bytes(buf) as u32, 2))
        }
        2 => {
            // 3 bytes (mask off highest byte)
            let mut buf = [0; 3];
            reader.read_exact(&mut buf)?;
            let bytes = [buf[0], buf[1], buf[2], 0];
            Ok((u32::from_ne_bytes(bytes), 3))
        }
        3 => {
            // 4 bytes
            let mut buf = [0; 4];
            reader.read_exact(&mut buf)?;
            let bytes = [buf[0], buf[1], buf[2], buf[3]];
            Ok((u32::from_ne_bytes(bytes), 4))
        }
        _ => {
            unreachable!("the bit value in the leading byte should never be more than 3 as it's only 2 bits.");
        }
    }
}
