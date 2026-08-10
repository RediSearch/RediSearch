/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Typed IO primitives for Redis module persistence (RDB save/load).
//!
//! Defines the [`RdbIO`] trait and its impls over `RedisModuleIO` and, for
//! tests, `std::io::Cursor`. The trait is shared substrate: it carries no
//! serialization logic of its own — callers layer their wire format on top of
//! these primitives.

use std::io::{Cursor, Read, Result, Write};

use redis_module::RedisModuleIO;

/// A trait for reading and writing data to Redis module persistence storage.
///
/// This trait provides a type-safe interface for serializing and deserializing
/// data when Redis persists module data to disk (RDB snapshots) or replicates
/// it to other nodes.
///
/// # Examples
///
/// ```
/// # use std::io;
/// # use rdb_io::RdbIO;
/// #
/// fn save_data<T: RdbIO>(io: &mut T) {
///     io.write_u64(42);
///     io.write_buffer(b"hello");
/// }
///
/// fn load_data<T: RdbIO>(io: &mut T) -> io::Result<(u64, Vec<u8>)> {
///     let value = io.read_u64()?;
///     let buffer = io.read_buffer()?;
///     Ok((value, buffer))
/// }
/// ```
pub trait RdbIO {
    /// Reads a 64-bit unsigned integer from the Redis module IO stream.
    ///
    /// # Errors
    ///
    /// Returns an error if the read operation fails or if the data is corrupted.
    fn read_u64(&mut self) -> Result<u64>;

    /// Reads a 64-bit signed integer from the Redis module IO stream.
    ///
    /// # Errors
    /// Returns an error if the read operation fails or if the data is corrupted.
    fn read_i64(&mut self) -> Result<i64>;

    /// Reads a 64-bit float (`double`) from the Redis module IO stream.
    ///
    /// # Errors
    /// Returns an error if the read operation fails or if the data is corrupted.
    fn read_f64(&mut self) -> Result<f64>;

    /// Reads a 32-bit float from the Redis module IO stream.
    ///
    /// # Errors
    /// Returns an error if the read operation fails or if the data is corrupted.
    fn read_f32(&mut self) -> Result<f32>;

    /// Reads a buffer of bytes from the Redis module IO stream.
    ///
    /// The length of the buffer is determined by Redis's internal serialization format.
    ///
    /// # Errors
    ///
    /// Returns an error if the read operation fails or if the data is corrupted.
    fn read_buffer(&mut self) -> Result<Vec<u8>>;

    /// Writes a 64-bit unsigned integer to the Redis module IO stream.
    fn write_u64(&mut self, value: u64);

    /// Writes a 64-bit signed integer to the Redis module IO stream.
    fn write_i64(&mut self, value: i64);

    /// Writes a 64-bit float (`double`) to the Redis module IO stream.
    fn write_f64(&mut self, value: f64);

    /// Writes a 32-bit float to the Redis module IO stream.
    fn write_f32(&mut self, value: f32);

    /// Writes a buffer of bytes to the Redis module IO stream.
    fn write_buffer(&mut self, buffer: &[u8]);
}

impl<T: RdbIO + ?Sized> RdbIO for &mut T {
    fn read_u64(&mut self) -> Result<u64> {
        (**self).read_u64()
    }

    fn read_i64(&mut self) -> Result<i64> {
        (**self).read_i64()
    }

    fn read_f64(&mut self) -> Result<f64> {
        (**self).read_f64()
    }

    fn read_f32(&mut self) -> Result<f32> {
        (**self).read_f32()
    }

    fn read_buffer(&mut self) -> Result<Vec<u8>> {
        (**self).read_buffer()
    }

    fn write_u64(&mut self, value: u64) {
        (**self).write_u64(value);
    }

    fn write_i64(&mut self, value: i64) {
        (**self).write_i64(value);
    }

    fn write_f64(&mut self, value: f64) {
        (**self).write_f64(value);
    }

    fn write_f32(&mut self, value: f32) {
        (**self).write_f32(value);
    }

    fn write_buffer(&mut self, buffer: &[u8]) {
        (**self).write_buffer(buffer);
    }
}

impl RdbIO for RedisModuleIO {
    fn read_u64(&mut self) -> Result<u64> {
        self.read_unsigned().map_err(std::io::Error::other)
    }

    fn read_i64(&mut self) -> Result<i64> {
        self.read_signed().map_err(std::io::Error::other)
    }

    fn read_f64(&mut self) -> Result<f64> {
        self.read_double().map_err(std::io::Error::other)
    }

    fn read_f32(&mut self) -> Result<f32> {
        self.read_float().map_err(std::io::Error::other)
    }

    fn read_buffer(&mut self) -> Result<Vec<u8>> {
        let buffer = self.read_string_buffer().map_err(std::io::Error::other)?;

        Ok(buffer.as_ref().to_vec())
    }

    fn write_u64(&mut self, value: u64) {
        self.write_unsigned(value);
    }

    fn write_i64(&mut self, value: i64) {
        self.write_signed(value);
    }

    fn write_f64(&mut self, value: f64) {
        self.write_double(value);
    }

    fn write_f32(&mut self, value: f32) {
        self.write_float(value);
    }

    fn write_buffer(&mut self, buffer: &[u8]) {
        self.write_slice(buffer);
    }
}

// This implementation allows us to use a Cursor over a Vec<u8> for testing purposes,
impl RdbIO for &mut Cursor<&mut Vec<u8>> {
    fn read_u64(&mut self) -> Result<u64> {
        let mut buf = [0u8; 8];
        self.read_exact(&mut buf)?;
        Ok(u64::from_le_bytes(buf))
    }

    fn read_i64(&mut self) -> Result<i64> {
        let mut buf = [0u8; 8];
        self.read_exact(&mut buf)?;
        Ok(i64::from_le_bytes(buf))
    }

    fn read_f64(&mut self) -> Result<f64> {
        let mut buf = [0u8; 8];
        self.read_exact(&mut buf)?;
        Ok(f64::from_le_bytes(buf))
    }

    fn read_f32(&mut self) -> Result<f32> {
        let mut buf = [0u8; 4];
        self.read_exact(&mut buf)?;
        Ok(f32::from_le_bytes(buf))
    }

    fn read_buffer(&mut self) -> Result<Vec<u8>> {
        let len = self.read_u64()? as usize;
        let mut buffer = vec![0u8; len];
        self.read_exact(&mut buffer)?;
        Ok(buffer)
    }

    fn write_u64(&mut self, value: u64) {
        let bytes = value.to_le_bytes();
        self.write_all(&bytes).unwrap();
    }

    fn write_i64(&mut self, value: i64) {
        let bytes = value.to_le_bytes();
        self.write_all(&bytes).unwrap();
    }

    fn write_f64(&mut self, value: f64) {
        let bytes = value.to_le_bytes();
        self.write_all(&bytes).unwrap();
    }

    fn write_f32(&mut self, value: f32) {
        let bytes = value.to_le_bytes();
        self.write_all(&bytes).unwrap();
    }

    fn write_buffer(&mut self, buffer: &[u8]) {
        let len = buffer.len() as u64;
        self.write_u64(len);
        self.write_all(buffer).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Round-trip every primitive through the `Cursor<&mut Vec<u8>>` impl:
    /// write into the cursor, rewind, then read the same values back out. This
    /// is the sole test-only `Cursor` impl (the read-only `&[u8]` impl was
    /// dropped) and no OSS caller otherwise exercises it — `trie_rdb` uses its
    /// own `RdbMock`.
    #[test]
    fn cursor_round_trip_all_primitives() {
        let mut backing = Vec::new();
        let mut cur = Cursor::new(&mut backing);

        {
            let mut io = &mut cur;
            io.write_u64(42);
            io.write_i64(-7);
            io.write_f64(3.5);
            io.write_f32(1.25);
            io.write_buffer(b"hello");
        }

        cur.set_position(0);

        let mut io = &mut cur;
        assert_eq!(io.read_u64().unwrap(), 42);
        assert_eq!(io.read_i64().unwrap(), -7);
        assert_eq!(io.read_f64().unwrap(), 3.5);
        assert_eq!(io.read_f32().unwrap(), 1.25);
        assert_eq!(io.read_buffer().unwrap(), b"hello");
    }

    /// The blanket `impl RdbIO for &mut T` lets a by-value `impl RdbIO`
    /// function accept a reborrowed `&mut endpoint` — the reborrow pattern
    /// RSE's `save_to_rdb(mut rdb: impl RdbIO)` relies on to delegate.
    #[test]
    fn blanket_impl_accepts_reborrowed_endpoint() {
        fn write_one(mut io: impl RdbIO) {
            io.write_u64(99);
        }

        let mut backing = Vec::new();
        let mut cur = Cursor::new(&mut backing);

        {
            let mut io = &mut cur; // impls RdbIO directly
            write_one(&mut io); // &mut io exercises the &mut T blanket impl
        }

        cur.set_position(0);

        let mut io = &mut cur;
        assert_eq!(io.read_u64().unwrap(), 99);
    }
}
