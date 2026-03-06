/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::sds;
use std::io::Write;
use value::RsValue;
use value::sds_writer::SdsWriter;

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_DumpSds(value: *const RsValue, sds: sds, obfuscate: bool) -> sds {
    let mut writer = unsafe { SdsWriter::new(sds) };

    match unsafe { value.as_ref() } {
        None => write!(writer, "nil").unwrap(),
        Some(value) => write!(writer, "{:?}", value.debug_formatter(obfuscate)).unwrap(),
    }

    writer.into_sds()
}
