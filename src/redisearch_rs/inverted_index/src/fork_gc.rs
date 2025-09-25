// TODO: this needs to move to FFI

use std::{
    ffi::{c_int, c_void},
    io::{self, Read, Write},
};

pub type c_size_t = usize;

#[repr(C)]
pub struct InvertedIndexGCWriter {
    pub ctx: *mut c_void,
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

#[repr(C)]
pub struct InvertedIndexGCReader {
    pub ctx: *mut c_void,
    // read exactly len or return nonzero
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

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use crate::{BlockGcScanResult, GcScanDelta, IndexBlock, RepairType};

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

        let original = GcScanDelta {
            last_block_idx: 4,
            last_block_num_entries: 1,
            deltas: vec![
                BlockGcScanResult {
                    index: 0,
                    repair: RepairType::Delete,
                },
                BlockGcScanResult {
                    index: 1,
                    repair: RepairType::Delete,
                },
                BlockGcScanResult {
                    index: 2,
                    repair: RepairType::Split {
                        blocks: vec![IndexBlock {
                            buffer: vec![0, 0, 0, 0, 0, 0, 0, 1],
                            num_entries: 2,
                            first_doc_id: 21,
                            last_doc_id: 22,
                        }],
                    },
                },
            ],
        };

        original.serialize(&mut rmp_serde::Serializer::new(&mut w))?;

        let mut r = InvertedIndexGCReader {
            ctx: (&mut storage as *mut Vec<u8>) as *mut c_void,
            read: vec_reader,
        };

        let decoded = GcScanDelta::deserialize(&mut rmp_serde::Deserializer::new(&mut r))?;

        assert_eq!(decoded, original);
        Ok(())
    }
}
