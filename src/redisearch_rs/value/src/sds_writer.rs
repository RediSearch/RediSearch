use ffi::{sds, sdscatlen};
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
