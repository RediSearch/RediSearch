/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{
    ffi::{c_int, c_void},
    io::{self, Read, Write},
};

// This alias can be removed once it lands in stable Rust
#[allow(non_camel_case_types)]
pub type c_size_t = usize;

/// A writer that calls a C function to write data.
#[repr(C)]
pub struct InvertedIndexGCWriter {
    /// Context pointer passed to the write function.
    pub ctx: *mut c_void,

    /// Function pointer to the write function.
    pub write: extern "C" fn(ctx: *mut c_void, buf: *const c_void, len: c_size_t),
}

impl Write for InvertedIndexGCWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let f = self.write;
        f(
            self.ctx,
            buf.as_ptr() as *const c_void,
            buf.len() as c_size_t,
        );
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// A reader that calls a C function to read data.
#[repr(C)]
pub struct InvertedIndexGCReader {
    /// Context pointer passed to the read function.
    pub ctx: *mut c_void,

    /// Function pointer to the read function.
    pub read: extern "C" fn(ctx: *mut c_void, buf: *mut c_void, len: c_size_t) -> c_int,
}

impl Read for InvertedIndexGCReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let f = self.read;
        let rc = f(
            self.ctx,
            buf.as_mut_ptr() as *mut c_void,
            buf.len() as c_size_t,
        );
        if rc == 0 {
            Ok(buf.len())
        } else {
            Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "reader could not fill the buffer",
            ))
        }
    }
}

/// A callback structure to trigger garbage collection operations.
#[repr(C)]
pub struct InvertedIndexGCCallback {
    /// Context pointer passed to the call function.
    pub ctx: *mut c_void,

    /// Function pointer to the call function.
    pub call: extern "C" fn(ctx: *mut c_void),
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::*;

    extern "C" fn vec_writer(ctx: *mut c_void, buf: *const c_void, len: c_size_t) {
        unsafe {
            let v = &mut *(ctx as *mut Vec<u8>);
            let src = core::slice::from_raw_parts(buf as *const u8, len as usize);
            v.extend_from_slice(src);
        }
    }

    extern "C" fn vec_reader(ctx: *mut c_void, buf: *mut c_void, len: c_size_t) -> c_int {
        unsafe {
            let v = &mut *(ctx as *mut Vec<u8>);
            let want = len as usize;
            if v.len() < want {
                return 1;
            }
            let dst = core::slice::from_raw_parts_mut(buf as *mut u8, want);
            dst.copy_from_slice(&v[..want]);
            v.drain(..want);
            0
        }
    }

    #[test]
    fn serde_round_trip_over_ii_gc() -> Result<(), Box<dyn std::error::Error>> {
        let mut storage: Vec<u8> = Vec::new();

        let mut w = InvertedIndexGCWriter {
            ctx: (&mut storage as *mut Vec<u8>) as *mut c_void,
            write: vec_writer,
        };

        let original = "Test string".to_string();

        original.serialize(&mut rmp_serde::Serializer::new(&mut w))?;

        let mut r = InvertedIndexGCReader {
            ctx: (&mut storage as *mut Vec<u8>) as *mut c_void,
            read: vec_reader,
        };

        let decoded = String::deserialize(&mut rmp_serde::Deserializer::new(&mut r))?;

        assert_eq!(decoded, original);
        Ok(())
    }
}
