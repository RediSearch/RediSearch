/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

/// Intentionally trigger a crash in Rust code,
/// to verify the crash handling mechanism.
///
/// Used by the crash result processor.
#[unsafe(no_mangle)]
pub extern "C" fn CrashInRust() {
    panic!("Crash in Rust code");
}
