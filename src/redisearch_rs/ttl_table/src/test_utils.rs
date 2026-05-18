/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::num::NonZeroUsize;

use ffi::{FieldExpiration, t_docId, t_expirationTimePoint as timespec};
use thin_vec::ThinVec;

pub const fn ts(sec: i64, nsec: i64) -> timespec {
    // `libc::time_t` is deprecated on musl (musl 1.2 changed it to 64-bit,
    // and the libc crate will follow suit — see libc#1848).
    #[cfg_attr(target_env = "musl", expect(deprecated))]
    let tv_sec = sec as libc::time_t;
    timespec {
        tv_sec,
        tv_nsec: nsec as libc::c_long,
    }
}

pub const DOC_ID_1: t_docId = 1;
pub const DOC_ID_2: t_docId = 4;
pub const FIELD_INDEX_1: u16 = 3;
pub const FIELD_INDEX_2: u16 = 5;
pub const FIELD_INDEX_3: u16 = 6;
pub const FIELD_INDEX_4: u16 = 10;

pub const PAST: timespec = ts(999, 0);
pub const NOW: timespec = ts(1000, 0);
pub const FUTURE: timespec = ts(1000, 1);
pub const FAR_IN_THE_FUTURE: timespec = ts(1001, 0);

// Special value
pub const NEVER: timespec = ts(0, 0);

pub const TEST_MAX_SIZE: NonZeroUsize = NonZeroUsize::new(1024).unwrap();

pub const fn empty_fields() -> ThinVec<FieldExpiration> {
    ThinVec::new()
}

pub const fn fe(index: u16, point: timespec) -> FieldExpiration {
    FieldExpiration { index, point }
}

/// Identity mapping from bit position → field index (bit `i` ↔ field `i`).
pub fn identity_ft_id() -> Vec<u16> {
    (0u16..128).collect()
}

pub fn mask_bit(indexes: &[u16]) -> u32 {
    indexes.iter().map(|index| 1 << index).sum()
}

pub fn mask_bit_u128(indexes: &[u16]) -> u128 {
    indexes.iter().map(|index| 1 << index).sum()
}

pub fn mask_bit_u64(indexes: &[u16]) -> u64 {
    indexes.iter().map(|index| 1u64 << index).sum()
}
