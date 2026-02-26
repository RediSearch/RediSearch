use ffi::{sds, sdscatlen};
use std::ffi::c_void;
use std::io::Write;

pub struct SdsWriter {
    sds: sds,
}

impl SdsWriter {
    pub unsafe fn new(sds: sds) -> Self {
        Self { sds }
    }

    pub fn extract_sds(self) -> sds {
        self.sds
    }

    unsafe fn sdscatlen(&mut self, ptr: *const c_void, len: usize) {
        self.sds = unsafe { sdscatlen(self.sds, ptr, len) };
    }

    pub fn write_bytes(&mut self, bytes: &[u8]) {
        unsafe { self.sdscatlen(bytes.as_ptr().cast(), bytes.len()) };
    }

    pub fn write_str(&mut self, str: &str) {
        self.write_bytes(str.as_bytes());
    }
}

impl Write for SdsWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.sds = unsafe { sdscatlen(self.sds, buf.as_ptr().cast(), buf.len()) };
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
