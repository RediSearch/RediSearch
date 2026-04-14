/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::{RSValue, util::try_value};
use ffi::sds;
use std::io::Write;
use value::sds_writer::SdsWriter;

/// Writes the debug representation of an [`RSValue`] into an SDS string.
///
/// If `value` is null, writes `"nil"`. Otherwise, formats the value using
/// [`DebugFormatter`](value::debug::DebugFormatter), optionally obfuscating
/// sensitive data when `obfuscate` is `true`.
///
/// # Safety
///
/// 1. If non-null, `value` must be a [valid] pointer to an [`RSValue`].
/// 2. `sds` must be a [valid], non-null SDS string allocated by the C SDS library.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_DumpSds(value: *const RSValue, sds: sds, obfuscate: bool) -> sds {
    // SAFETY: `sds` is a valid SDS string, guaranteed by the caller.
    let mut writer = unsafe { SdsWriter::new(sds) };

    // SAFETY: If non-null, `value` points to a valid `RSValue`, guaranteed by the caller.
    match unsafe { try_value(value) } {
        None => write!(writer, "nil").unwrap(),
        Some(value) => write!(writer, "{:?}", value.debug_formatter(obfuscate)).unwrap(),
    }

    writer.into_sds()
}
