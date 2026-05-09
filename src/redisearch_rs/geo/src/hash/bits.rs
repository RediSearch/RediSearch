/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Bit interleaving (Morton code) and neighbor movement operations.
//!
//! The interleaving places latitude bits in even positions and longitude bits
//! in odd positions, producing a Z-order curve over the coordinate space.

use super::types::GeoHashBits;

/// Interleave the lower bits of `x` (latitude) and `y` (longitude) so that
/// x-bits occupy even positions and y-bits occupy odd positions.
///
/// Reference: <https://graphics.stanford.edu/~seander/bithacks.html#InterleaveBMN>
pub(crate) const fn interleave64(xlo: u32, ylo: u32) -> u64 {
    const B: [u64; 5] = [
        0x5555_5555_5555_5555,
        0x3333_3333_3333_3333,
        0x0F0F_0F0F_0F0F_0F0F,
        0x00FF_00FF_00FF_00FF,
        0x0000_FFFF_0000_FFFF,
    ];
    const S: [u32; 5] = [1, 2, 4, 8, 16];

    let mut x = xlo as u64;
    let mut y = ylo as u64;

    x = (x | (x << S[4])) & B[4];
    y = (y | (y << S[4])) & B[4];

    x = (x | (x << S[3])) & B[3];
    y = (y | (y << S[3])) & B[3];

    x = (x | (x << S[2])) & B[2];
    y = (y | (y << S[2])) & B[2];

    x = (x | (x << S[1])) & B[1];
    y = (y | (y << S[1])) & B[1];

    x = (x | (x << S[0])) & B[0];
    y = (y | (y << S[0])) & B[0];

    x | (y << 1)
}

/// Reverse the interleave: extract the x (latitude) and y (longitude)
/// components from an interleaved 64-bit value.
///
/// Reference: <http://stackoverflow.com/questions/4909263>
pub(crate) const fn deinterleave64(interleaved: u64) -> (u32, u32) {
    const B: [u64; 6] = [
        0x5555_5555_5555_5555,
        0x3333_3333_3333_3333,
        0x0F0F_0F0F_0F0F_0F0F,
        0x00FF_00FF_00FF_00FF,
        0x0000_FFFF_0000_FFFF,
        0x0000_0000_FFFF_FFFF,
    ];
    const S: [u32; 6] = [0, 1, 2, 4, 8, 16];

    let mut x = interleaved;
    let mut y = interleaved >> 1;

    x = (x | (x >> S[0])) & B[0];
    y = (y | (y >> S[0])) & B[0];

    x = (x | (x >> S[1])) & B[1];
    y = (y | (y >> S[1])) & B[1];

    x = (x | (x >> S[2])) & B[2];
    y = (y | (y >> S[2])) & B[2];

    x = (x | (x >> S[3])) & B[3];
    y = (y | (y >> S[3])) & B[3];

    x = (x | (x >> S[4])) & B[4];
    y = (y | (y >> S[4])) & B[4];

    x = (x | (x >> S[5])) & B[5];
    y = (y | (y >> S[5])) & B[5];

    (x as u32, y as u32)
}

/// Move a geohash along the x-axis (longitude) by `d` steps (+1 = east,
/// -1 = west).
pub(crate) const fn move_x(hash: &mut GeoHashBits, d: i8) {
    if d == 0 {
        return;
    }

    let step = hash.step.as_u8() as u32;
    let x = hash.bits & 0xAAAA_AAAA_AAAA_AAAA;
    let y = hash.bits & 0x5555_5555_5555_5555;
    let zz = 0x5555_5555_5555_5555u64 >> (64 - step * 2);
    let mask = 0xAAAA_AAAA_AAAA_AAAAu64 >> (64 - step * 2);

    let x = if d > 0 {
        x.wrapping_add(zz.wrapping_add(1)) & mask
    } else {
        (x | zz).wrapping_sub(zz.wrapping_add(1)) & mask
    };

    hash.bits = x | y;
}

/// Move a geohash along the y-axis (latitude) by `d` steps (+1 = north,
/// -1 = south).
pub(crate) const fn move_y(hash: &mut GeoHashBits, d: i8) {
    if d == 0 {
        return;
    }

    let step = hash.step.as_u8() as u32;
    let x = hash.bits & 0xAAAA_AAAA_AAAA_AAAA;
    let y = hash.bits & 0x5555_5555_5555_5555;
    let zz = 0xAAAA_AAAA_AAAA_AAAAu64 >> (64 - step * 2);
    let mask = 0x5555_5555_5555_5555u64 >> (64 - step * 2);

    let y = if d > 0 {
        y.wrapping_add(zz.wrapping_add(1)) & mask
    } else {
        (y | zz).wrapping_sub(zz.wrapping_add(1)) & mask
    };

    hash.bits = x | y;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::types::PrecisionStep;

    #[test]
    fn interleave_roundtrip() {
        for (x, y) in [(0, 0), (1, 0), (0, 1), (0xFFFF, 0xFFFF), (12345, 67890)] {
            let interleaved = interleave64(x, y);
            let (dx, dy) = deinterleave64(interleaved);
            assert_eq!(dx, x, "x mismatch for ({x}, {y})");
            assert_eq!(dy, y, "y mismatch for ({x}, {y})");
        }
    }

    #[test]
    fn interleave_known_values() {
        // All bits set: interleave(0xFFFFFFFF, 0xFFFFFFFF) should be all bits set
        assert_eq!(interleave64(0xFFFF_FFFF, 0xFFFF_FFFF), u64::MAX);
        // Only x bits: interleave(0xFFFFFFFF, 0) should be 0x5555...
        assert_eq!(interleave64(0xFFFF_FFFF, 0), 0x5555_5555_5555_5555);
        // Only y bits: interleave(0, 0xFFFFFFFF) should be 0xAAAA...
        assert_eq!(interleave64(0, 0xFFFF_FFFF), 0xAAAA_AAAA_AAAA_AAAA);
    }

    #[test]
    fn move_x_zero_is_noop() {
        let mut hash = GeoHashBits {
            bits: 0xABCD,
            step: PrecisionStep::new(10).unwrap(),
        };
        let original = hash;
        move_x(&mut hash, 0);
        assert_eq!(hash, original);
    }

    #[test]
    fn move_y_zero_is_noop() {
        let mut hash = GeoHashBits {
            bits: 0xABCD,
            step: PrecisionStep::new(10).unwrap(),
        };
        let original = hash;
        move_y(&mut hash, 0);
        assert_eq!(hash, original);
    }

    #[test]
    fn move_x_east_then_west_roundtrips() {
        let original = GeoHashBits {
            bits: interleave64(100, 200),
            step: PrecisionStep::new(16).unwrap(),
        };
        let mut hash = original;
        move_x(&mut hash, 1);
        assert_ne!(hash, original, "moving east should change the hash");
        move_x(&mut hash, -1);
        assert_eq!(hash, original, "moving east then west should roundtrip");
    }

    #[test]
    fn move_y_north_then_south_roundtrips() {
        let original = GeoHashBits {
            bits: interleave64(100, 200),
            step: PrecisionStep::new(16).unwrap(),
        };
        let mut hash = original;
        move_y(&mut hash, 1);
        assert_ne!(hash, original, "moving north should change the hash");
        move_y(&mut hash, -1);
        assert_eq!(hash, original, "moving north then south should roundtrip");
    }

    #[test]
    fn move_x_and_y_are_independent() {
        let original = GeoHashBits {
            bits: interleave64(50, 75),
            step: PrecisionStep::new(10).unwrap(),
        };

        let mut moved_x = original;
        move_x(&mut moved_x, 1);

        let mut moved_y = original;
        move_y(&mut moved_y, 1);

        // X movement should only change odd-position bits (longitude).
        // Y movement should only change even-position bits (latitude).
        // The y-bits of an x-move should be unchanged, and vice versa.
        let y_mask = 0x5555_5555_5555_5555u64;
        let x_mask = 0xAAAA_AAAA_AAAA_AAAAu64;
        assert_eq!(
            moved_x.bits & y_mask,
            original.bits & y_mask,
            "move_x should not change y (latitude) bits"
        );
        assert_eq!(
            moved_y.bits & x_mask,
            original.bits & x_mask,
            "move_y should not change x (longitude) bits"
        );
    }
}
