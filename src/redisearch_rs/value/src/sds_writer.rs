/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::{sds, sdscatlen};
use std::io::Write;

pub struct SdsWriter {
    sds: sds,
}

impl SdsWriter {
    pub unsafe fn new(sds: sds) -> Self {
        Self { sds }
    }

    pub fn into_sds(self) -> sds {
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
