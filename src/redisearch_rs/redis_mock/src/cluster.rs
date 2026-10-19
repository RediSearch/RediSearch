/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ffi::{c_char, c_uint};

/// Number of hash slots in a Redis cluster.
const CLUSTER_SLOTS: u16 = 16384;

/// The part of `key` Redis hashes: the content of its first `{...}` when non-empty, else the
/// whole key.
fn hash_tag(key: &[u8]) -> &[u8] {
    let Some(open) = key.iter().position(|&b| b == b'{') else {
        return key;
    };
    let rest = &key[open + 1..];
    match rest.iter().position(|&b| b == b'}') {
        Some(close) if close > 0 => &rest[..close],
        _ => key,
    }
}

/// CRC16-XMODEM, the checksum Redis uses for key slots.
fn crc16(bytes: &[u8]) -> u16 {
    bytes.iter().fold(0u16, |mut crc, &b| {
        crc ^= u16::from(b) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x1021
            } else {
                crc << 1
            };
        }
        crc
    })
}

/// Mock implementation of `RedisModule_ClusterKeySlotC`, matching Redis' `keyHashSlot`.
///
/// # Safety
/// 1. `key` must point to `len` readable bytes.
#[expect(non_snake_case)]
pub unsafe extern "C" fn RedisModule_ClusterKeySlotC(key: *const c_char, len: usize) -> c_uint {
    // Safety: the caller guarantees `key` points to `len` readable bytes (1.).
    let key = unsafe { std::slice::from_raw_parts(key.cast::<u8>(), len) };
    c_uint::from(crc16(hash_tag(key)) % CLUSTER_SLOTS)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slot(key: &str) -> c_uint {
        // Safety: `key` is a live `&str` of `key.len()` bytes.
        unsafe { RedisModule_ClusterKeySlotC(key.as_ptr().cast(), key.len()) }
    }

    #[test]
    fn matches_redis_cluster_keyslot() {
        // Expected values from `CLUSTER KEYSLOT` on a real Redis.
        assert_eq!(slot("foo"), 12182);
        assert_eq!(slot("bar"), 5061);
        assert_eq!(slot("{user1000}.following"), 3443);
        assert_eq!(slot("foo{}{bar}"), 8363);
    }
}
