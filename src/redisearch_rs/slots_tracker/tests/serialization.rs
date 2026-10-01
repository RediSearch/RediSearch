/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use slots_tracker::serialization::{deserialize, serialize_into};
use slots_tracker::{SlotRange, SlotRangeArray};

fn serialize(ranges: &[SlotRange]) -> Vec<u8> {
    let mut buf = vec![0; SlotRangeArray::size_of(ranges.len())];
    serialize_into(ranges, &mut buf);
    buf
}

fn range(start: u16, end: u16) -> SlotRange {
    SlotRange { start, end }
}

/// The count and both bounds are little-endian, whatever the host.
#[test]
fn format_is_little_endian() {
    let buf = serialize(&[range(0x0102, 0x0304)]);
    assert_eq!(buf, [1, 0, 0, 0, 0x02, 0x01, 0x04, 0x03]);
}

#[test]
fn empty_array_is_just_the_count() {
    let buf = serialize(&[]);
    assert_eq!(buf, [0; 4]);
    assert_eq!(deserialize(&buf).map(Iterator::count), Some(0));
}

/// A buffer is accepted only if its length matches the count it declares.
#[test]
fn rejects_malformed_buffers() {
    let valid = serialize(&[range(0, 10), range(20, 30)]);

    assert!(
        deserialize(&valid[..3]).is_none(),
        "shorter than the header"
    );
    assert!(
        deserialize(&valid[..valid.len() - 1]).is_none(),
        "truncated"
    );
    assert!(
        deserialize(&[valid.as_slice(), &[0]].concat()).is_none(),
        "trailing byte"
    );

    let mut negative = valid.clone();
    negative[..4].copy_from_slice(&(-1i32).to_le_bytes());
    assert!(deserialize(&negative).is_none(), "negative count");
}

#[cfg(not(miri))]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn round_trips(bounds in proptest::collection::vec(any::<(u16, u16)>(), 0..64)) {
            let ranges: Vec<_> = bounds.into_iter().map(|(start, end)| range(start, end)).collect();

            let decoded: Vec<_> = deserialize(&serialize(&ranges)).expect("well-formed").collect();

            prop_assert_eq!(decoded, ranges);
        }
    }
}
