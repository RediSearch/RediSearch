/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{ffi::c_void, ptr::NonNull};

use rlookup::row::RLookupRow;

// TODO: merge right type here
type RLookupKey = c_void;
type RSValue = c_void;

#[unsafe(no_mangle)]
#[expect(unused, reason = "implemented by later stacked PRs")]
unsafe extern "C" fn RLookup_WriteKey(
    key: *const RLookupKey,
    row: NonNull<RLookupRow>,
    value: RSValue,
) {
    todo!("implement RLookup_WriteKey");
}

#[unsafe(no_mangle)]
#[expect(unused, reason = "implemented by later stacked PRs")]
unsafe extern "C" fn RLookupRow_WriteOwnKey(
    key: *const RLookupKey,
    row: NonNull<RLookupRow>,
    value: RSValue,
) {
    todo!("Implement RLookupRow_WriteOwnKey");
}

/// Wipes a RLookupRow by decrementing all values and resetting the row.
///
/// Safety:
/// 1. The pointer must be a valid pointer to an [`RLookupRow`].
#[unsafe(no_mangle)]
#[expect(unused, reason = "implemented by later stacked PRs")]
unsafe extern "C" fn RLookupRow_Wipe(vec: NonNull<RLookupRow>) {
    todo!("Implement RLookupRow_Wipe");
}

/// Cleanup a RLookupRow by wiping it (see [`RLookupRow_Wipe`]) and deallocating the memory.
///
/// Safety:
/// 1. The pointer must be a valid pointer to an [`RLookupRow`].
#[unsafe(no_mangle)]
#[expect(unused, reason = "implemented by later stacked PRs")]
unsafe extern "C" fn RLookupRow_Cleanup(vec: NonNull<RLookupRow>) {
    todo!("Implement RLookupRow_Cleanup");
}
