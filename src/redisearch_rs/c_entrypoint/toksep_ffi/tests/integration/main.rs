/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::mem::MaybeUninit;

#[test]
fn scanning_preserves_spans_and_advances_the_cursor() {
    for (name, input, spans) in [
        ("empty", b"\0".as_slice(), vec![(0, 0)]),
        (
            "repeated separators",
            b",,a,\0",
            vec![(0, 0), (1, 0), (2, 1)],
        ),
        ("trailing separators", b"a,,\0", vec![(0, 1), (2, 0)]),
        ("escapes", b"a\\,b,c\\\\,d\0", vec![(0, 4), (5, 3), (9, 1)]),
        ("first NUL", b"ab\0ignored\0", vec![(0, 2)]),
        ("escaped NUL", b"a\\\0ignored\0", vec![(0, 2)]),
        ("trailing escape", b"a\\\0", vec![(0, 2)]),
        ("binary bytes", b"\xff\n_\x80,x\0", vec![(0, 4), (5, 1)]),
    ] {
        let mut storage = input.to_vec();
        let orig = storage.as_mut_ptr().cast::<std::ffi::c_char>();
        let mut cursor = orig;
        for (offset, length) in spans {
            assert!(!cursor.is_null(), "{name}");
            assert_eq!(cursor, orig.wrapping_add(offset), "{name}");
            let mut len = MaybeUninit::uninit();
            // SAFETY: 1. cursor is initialized and writable. 2. storage is NUL-terminated,
            // remains alive and is not mutated. 3. len and cursor are disjoint local slots.
            let token = unsafe { toksep_ffi::toksep(&mut cursor, len.as_mut_ptr()) };
            assert_eq!(token, orig.wrapping_add(offset), "{name}");
            // SAFETY: toksep initializes len on every call.
            assert_eq!(unsafe { len.assume_init() }, length, "{name}");
        }
        assert!(cursor.is_null(), "{name}");
        assert_eq!(storage, input, "{name}");
    }
}

#[cfg(not(miri))]
proptest::proptest! {
    #[test]
    fn scanning_preserves_generated_spans(bytes in proptest::collection::vec(
        proptest::prop_oneof![proptest::prelude::any::<u8>(), proptest::sample::select(b"a\\, \t\0\x80_".to_vec())],
        0..256,
    )) {
        let mut input = bytes;
        input.push(0);
        let mut storage = input.clone();
        let orig = storage.as_mut_ptr().cast::<std::ffi::c_char>();
        let mut cursor = orig;
        let mut offset = 0;
        for (end, &byte) in input.iter().enumerate() {
            let backslashes = input[..end].iter().rev().take_while(|&&b| b == b'\\').count();
            if byte != 0 && (!b" \t,./(){}[]:;~!@#$%^&*-=+|'`\"<>?".contains(&byte) || backslashes % 2 != 0) {
                continue;
            }
            let mut len = MaybeUninit::uninit();
            // SAFETY: 1. cursor is initialized and writable. 2. storage is NUL-terminated
            // and remains alive and unchanged. 3. len and cursor are disjoint local slots.
            let token = unsafe { toksep_ffi::toksep(&mut cursor, len.as_mut_ptr()) };
            proptest::prop_assert_eq!(token, orig.wrapping_add(offset));
            // SAFETY: toksep initializes len on every call.
            proptest::prop_assert_eq!(unsafe { len.assume_init() }, end - offset);
            let finished = byte == 0 || input[end + 1] == 0;
            let next = if finished { std::ptr::null_mut() } else { orig.wrapping_add(end + 1) };
            proptest::prop_assert_eq!(cursor, next);
            if finished {
                break;
            }
            offset = end + 1;
        }
        proptest::prop_assert_eq!(storage, input);
    }
}

#[test]
fn classification_uses_the_low_byte() {
    let separators = b" \t,./(){}[]:;~!@#$%^&*-=+|'`\"<>?";
    for high_bits in [i32::MIN, -512, 0, 256, i32::MAX & !255] {
        for byte in 0..=255 {
            let expected = i32::from(separators.contains(&(byte as u8)));
            assert_eq!(toksep_ffi::istoksep(high_bits | byte), expected);
        }
    }
    #[cfg(not(miri))]
    proptest::proptest!(|(c in proptest::prelude::any::<i32>())| {
        let expected = i32::from(separators.contains(&c.to_le_bytes()[0]));
        proptest::prop_assert_eq!(toksep_ffi::istoksep(c), expected);
    });
}
