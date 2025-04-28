//! # qint encoding and decoding
//!
//! The qint encoding scheme is a variable-length encoding scheme for integers. It uses a leading byte to
//! define the number of bytes used to represent the following integers. Based on that leading byte, up to four
//! variable-length integers are encoded. The leading byte encodes each length in a 2-bit field. The first 2 bits 
//! represent the first integer. The next 2 bits represent the second integer, and so on.
//!
//! The module provides the same function as the C API, but in Rust: [qint_encode2], [qint_encode3], [qint_encode4] and
//! [qint_decode2], [qint_decode3], [qint_decode4] based on the traits [Read], [Seek] and [Write].
//!
//! These methods build upon the generic [qint_encode] and [qint_decode] methods.


use std::io;

use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;

/// Encodes an array of integers into a QInt buffer.
///
/// # Arguments
/// * `cursor` - Buffer writer
/// * `values` - Array of integers to encode (2, 3, or 4 integers)
///
/// # Returns
/// The number of bytes written to the buffer or an io error
#[inline(always)]
pub(crate) fn qint_encode<const N: usize, W>(
    cursor: &mut W,
    values: [u32; N],
) -> Result<usize, std::io::Error>
where
    W: Write + Seek,
{
    assert!((2..=4).contains(&N), "N must be 2, 3, or 4");

    let mut leading = 0;
    let mut ret = 0;
    let pos = cursor.stream_position()?;
    ret += cursor.write(b"\0")?; // Write placeholder for leading byte
    for (i, &value) in values.iter().enumerate() {
        ret += qint_encode_stepwise(&mut leading, cursor, value, i as u32)?;
    }
    cursor.seek(SeekFrom::Start(pos))?;
    cursor.write_all(&[leading])?;
    Ok(ret)
}

/// Decodes a QInt buffer into an array of integers
///
/// # Arguments
/// * `reader` - Buffer reader
/// * `N` - Number of integers to decode (2, 3, or 4)
///
/// # Returns
/// A tuple of (decoded_values as an array, bytes_consumed) or an io error
pub(crate) fn qint_decode<const N: usize, R: Read>(
    reader: &mut R,
) -> Result<([u32; N], usize), std::io::Error> {
    // Ensure N is valid (2, 3, or 4)
    assert!((2..=4).contains(&N), "N must be 2, 3, or 4");

    let mut total = 0;

    // Read the leading byte
    let mut leading = [0; 1];
    reader.read_exact(&mut leading)?;
    total += 1;

    // Decode N values based on 2-bit fields in the leading byte
    let mut result = [0u32; N];
    for (i, item) in result.iter_mut().enumerate().take(N) {
        // Extract 2-bit field for the i-th value
        let bits = (leading[0] >> (i * 2)) & 0x03;
        let (val, bytes) = qint_decode_value(bits, reader)?;
        *item = val;
        total += bytes;
    }

    Ok((result, total))
}

/// Encodes 4 integers into a QInt buffer
///
/// # Arguments
/// * `cursor` - Buffer writer
/// * `a` - First integer
/// * `b` - Second integer
/// * `c` - Third integer
/// * `d` - Fourth integer
///
/// # Returns
/// The number of bytes written to the buffer or an io error
pub fn qint_encode4<W>(cursor: &mut W, a: u32, b: u32, c: u32, d: u32) -> Result<usize, io::Error>
where
    W: Write + Seek,
{
    qint_encode(cursor, [a, b, c, d])
}

/// Encodes 3 integers into a QInt buffer
///
/// # Arguments
/// * `cursor` - Buffer writer
/// * `a` - First integer
/// * `b` - Second integer
/// * `c` - Third integer
///
/// # Returns
/// The number of bytes written to the buffer or an io error
pub fn qint_encode3<W>(cursor: &mut W, a: u32, b: u32, c: u32) -> Result<usize, io::Error>
where
    W: Write + Seek,
{
    qint_encode(cursor, [a, b, c])
}

/// Encodes 2 integers into a QInt buffer
///
/// # Arguments
/// * `cursor` - Buffer writer
/// * `a` - First integer
/// * `b` - Second integer
///
/// # Returns
/// The number of bytes written to the buffer or an io error
pub fn qint_encode2<W>(cursor: &mut W, a: u32, b: u32) -> Result<usize, io::Error>
where
    W: Write + Seek,
{
    qint_encode(cursor, [a, b])
}

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
    let mut ret: usize = 0;
    let mut num_bytes: i8 = -1;
    loop {
        cursor.write_all(&[value as u8])?;
        ret += &[value as u8].len();
        num_bytes += 1;

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
    *leading |= (num_bytes as u8) << (offset * 2);
    Ok(ret)
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
        _ => {
            // 4 bytes
            let mut buf = [0; 4];
            reader.read_exact(&mut buf)?;
            let bytes = [buf[0], buf[1], buf[2], buf[3]];
            Ok((u32::from_ne_bytes(bytes), 4))
        }
    }
}

/// Decodes 2 integers from a qint buffer
///
/// # Arguments
/// * `reader` - Buffer reader
///
/// # Returns
/// A tuple of (first_value, second_value, bytes_consumed)
pub fn qint_decode2<R: Read>(reader: &mut R) -> Result<(u32, u32, usize), std::io::Error> {
    let (v, bytes) = qint_decode::<2, _>(reader)?;
    Ok((v[0], v[1], bytes))
}

/// Decodes 3 integers from a qint buffer
///
/// # Arguments
/// * `reader` - Buffer reader
///
/// # Returns
/// A tuple of (first_value, second_value, third_value, bytes_consumed)
pub fn qint_decode3<R: Read>(reader: &mut R) -> Result<(u32, u32, u32, usize), std::io::Error> {
    let (v, bytes) = qint_decode::<3, _>(reader)?;
    Ok((v[0], v[1], v[2], bytes))
}

/// Decodes 4 integers from a qint buffer
///     
/// # Arguments
/// * `reader` - Buffer reader
///
/// # Returns
/// A tuple of (first_value, second_value, third_value, fourth_value, bytes_consumed)
pub fn qint_decode4<R: Read>(r: &mut R) -> Result<(u32, u32, u32, u32, usize), io::Error> {
    let (v, bytes) = qint_decode::<4, _>(r)?;
    Ok((v[0], v[1], v[2], v[3], bytes))
}

#[cfg(test)]
mod test {
    use std::io::Cursor;
    use super::qint_decode;
    use super::qint_encode;

    #[test]
    #[should_panic]
    fn test_5_bytes_encode_panics() {
        let mut buf = [0u8; 64];
        let mut write_cursor = Cursor::new(buf.as_mut());
        qint_encode(&mut write_cursor, [1, 2, 3, 4, 5]).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_1_byte_encode_panics() {
        let mut buf = [0u8; 64];
        let mut write_cursor = Cursor::new(buf.as_mut());
        qint_encode(&mut write_cursor, [1]).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_5_bytes_decode_panics() {
        let mut buf = [0u8; 64];
        let mut read_cursor = Cursor::new(buf.as_mut());
        qint_decode::<5, _>(&mut read_cursor).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_1_byte_decode_panics() {
        let mut buf = [0u8; 64];
        let mut read_cursor = Cursor::new(buf.as_mut());
        qint_decode::<1, _>(&mut read_cursor).unwrap();
    }
}
