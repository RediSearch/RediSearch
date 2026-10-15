/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Exercises the `extern "C"` surface the way the C callers do, so that Miri checks the
//! ownership hand-offs and the iterator C keeps on its stack.

use std::mem::MaybeUninit;
use std::ptr;

use byte_offsets::{ByteOffsets, OpaqueFieldByteOffsets};
use byte_offsets_ffi::*;
use varint::VectorWriter;

/// Every token [`RSByteOffsetIterator_Next`] returns for `field_id`, up to and including
/// the one that ends the iteration. `None` if `offsets` has no such field.
///
/// # Safety
///
/// `offsets` must be a live map from [`NewByteOffsets`] or [`RSByteOffsets_LoadLegacy`].
unsafe fn iterate(offsets: *const ByteOffsets, field_id: u16) -> Option<Vec<TokenByteOffset>> {
    let mut iter = MaybeUninit::<OpaqueFieldByteOffsets>::uninit();
    // SAFETY: `offsets` is live (see the function's safety section) and `iter` is
    // writable.
    if !unsafe { RSByteOffset_Iterate(offsets, field_id, iter.as_mut_ptr()) } {
        return None;
    }
    let mut tokens = Vec::new();
    loop {
        // SAFETY: `iter` was written by `RSByteOffset_Iterate`, and `offsets` is neither
        // mutated nor freed while it is in use.
        let token = unsafe { RSByteOffsetIterator_Next(iter.as_mut_ptr()) };
        tokens.push(token);
        if token.byte_offset == RSBYTEOFFSET_EOF {
            return Some(tokens);
        }
    }
}

const fn token(position: u32, byte_offset: u32) -> TokenByteOffset {
    TokenByteOffset {
        position,
        byte_offset,
    }
}

#[test]
fn indexing_lifecycle() {
    let offsets = NewByteOffsets();
    let mut writer = VectorWriter::new(16);
    for offset in [3, 8, 1, 6] {
        writer.write(offset).expect("writing to a Vec cannot fail");
    }
    // SAFETY: `offsets` is live and nothing else accesses it.
    unsafe { RSByteOffsets_ReserveFields(offsets, 2) };
    // SAFETY: as above.
    unsafe { RSByteOffsets_AddField(offsets, 4, 1, 2) };
    // SAFETY: as above.
    unsafe { RSByteOffsets_AddField(offsets, 5, 3, 1) };
    // SAFETY: as above.
    unsafe { RSByteOffsets_AddField(offsets, 6, 4, 1) };
    // SAFETY: as above, and `writer` is a live `VectorWriter` borrowed exclusively.
    unsafe { RSByteOffsets_TakeFromWriter(offsets, &mut writer) };
    assert!(writer.bytes().is_empty());

    // SAFETY: `offsets` is live until it is freed below.
    let tokens = unsafe { iterate(offsets, 5) };
    assert_eq!(tokens, Some(vec![token(3, 1), token(4, RSBYTEOFFSET_EOF)]));
    // SAFETY: as above.
    assert_eq!(unsafe { iterate(offsets, 7) }, None);

    // SAFETY: `offsets` came from `NewByteOffsets` and is not used afterwards.
    unsafe { RSByteOffsets_Free(offsets) };
}

/// Looking up a field the map lacks writes nothing to `iter`.
#[test]
fn missing_field_leaves_iter_untouched() {
    const FILL: u8 = 0xA5;
    let offsets = NewByteOffsets();
    let mut storage = MaybeUninit::<OpaqueFieldByteOffsets>::uninit();
    // One pointer for every access below, so that none of them invalidates another.
    let iter = storage.as_mut_ptr();
    let bytes = iter.cast::<u8>();
    // SAFETY: `bytes` covers exactly the storage of `iter`.
    unsafe { ptr::write_bytes(bytes, FILL, size_of::<OpaqueFieldByteOffsets>()) };

    // SAFETY: `offsets` is live and `iter` is writable.
    assert!(!unsafe { RSByteOffset_Iterate(offsets, 1, iter) });
    // SAFETY: every byte of `iter` was initialized above.
    let after = unsafe { std::slice::from_raw_parts(bytes, size_of::<OpaqueFieldByteOffsets>()) };
    assert!(after.iter().all(|&byte| byte == FILL));

    // SAFETY: `offsets` came from `NewByteOffsets` and is not used afterwards.
    unsafe { RSByteOffsets_Free(offsets) };
}

#[test]
fn legacy_load_lifecycle() {
    // One field, id 2, at positions 1..=1, then one encoded offset of 7.
    let blob: [u8; _] = [1, 2, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 7];

    // SAFETY: `blob` is readable for its length.
    let offsets = unsafe { RSByteOffsets_LoadLegacy(blob.as_ptr().cast(), blob.len()) };
    assert!(!offsets.is_null());
    // SAFETY: `offsets` is live until it is freed below.
    let tokens = unsafe { iterate(offsets, 2) };
    assert_eq!(tokens, Some(vec![token(1, 7), token(1, RSBYTEOFFSET_EOF)]));
    // SAFETY: `offsets` came from `RSByteOffsets_LoadLegacy` and is not used afterwards.
    unsafe { RSByteOffsets_Free(offsets) };

    // SAFETY: the shorter prefix of `blob` is readable for its length.
    let truncated = unsafe { RSByteOffsets_LoadLegacy(blob.as_ptr().cast(), blob.len() - 1) };
    assert!(truncated.is_null());
    // SAFETY: a NULL `data` is never read.
    let failed_read = unsafe { RSByteOffsets_LoadLegacy(ptr::null(), blob.len()) };
    assert!(failed_read.is_null());
}
