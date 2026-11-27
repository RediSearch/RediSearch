/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::mem::MaybeUninit;
use toksep_ffi::{istoksep, toksep};

#[cfg(not(miri))]
proptest::proptest! {
    #[test]
    fn classification_uses_low_byte(high_bits: i32, low_byte: u8) {
        let input = (high_bits & !0xff) | i32::from(low_byte);
        let separators = b" \t!\"#$%&'()*+,-./:;<=>?@[]^`{|}~";
        proptest::prop_assert_eq!(istoksep(input), i32::from(separators.contains(&low_byte)));
    }

    #[test]
    fn scanning_preserves_cursor_progress(
        mut bytes in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..128),
    ) {
        bytes.push(0);
        let original = bytes.clone();
        let start = bytes.as_mut_ptr().cast::<std::ffi::c_char>();
        let mut offset = 0;
        loop {
            let before = start.wrapping_add(offset);
            let mut cursor = before;
            let expected_len = toksep::token_len(bytes[offset..].iter().copied());
            let mut len = MaybeUninit::uninit();
            // SAFETY: 1, 3. Distinct writable locals, disjoint from bytes.
            // 2. offset starts at zero and only advances within the NUL-terminated input.
            let token = unsafe { toksep(&raw mut cursor, len.as_mut_ptr()) };
            proptest::prop_assert_eq!(token, before);
            // SAFETY: toksep initialized the length output.
            proptest::prop_assert_eq!(unsafe { len.assume_init() }, expected_len);
            let end = offset + expected_len;
            if bytes[end] == 0 || bytes[end + 1] == 0 {
                proptest::prop_assert!(cursor.is_null());
                break;
            }
            offset = end + 1;
            proptest::prop_assert_eq!(cursor, start.wrapping_add(offset));
        }
        proptest::prop_assert_eq!(bytes, original);
    }
}

#[test]
fn classification_truncates_signed_integers() {
    assert_eq!(istoksep(i32::MIN), 0);
    assert_eq!(istoksep(i32::MAX), 0);
    assert_eq!(istoksep(-224), 1);
    assert_eq!(istoksep(288), 1);
}

#[test]
fn scanning_preserves_spans() {
    for (name, input, spans) in [
        ("empty", b"\0".as_slice(), vec![(0, 0)]),
        (
            "consecutive and trailing",
            b",a;;b,\0",
            vec![(0, 0), (1, 1), (3, 0), (4, 1)],
        ),
        (
            "escaped separators",
            b"a\\ b \\\\ z\\\0",
            vec![(0, 4), (5, 2), (8, 2)],
        ),
        ("NUL ends input", b"x\0ignored\0", vec![(0, 1)]),
        ("raw bytes", b"\xff\n_\0", vec![(0, 3)]),
    ] {
        let mut bytes = input.to_vec();
        let start = bytes.as_mut_ptr().cast::<std::ffi::c_char>();
        let mut cursor = start;
        for (index, &(offset, expected_len)) in spans.iter().enumerate() {
            assert_eq!(cursor, start.wrapping_add(offset), "{name}");
            let mut len = MaybeUninit::uninit();
            // SAFETY: 1, 3. Distinct writable local outputs, disjoint from bytes.
            // 2. cursor points into the unchanged NUL-terminated bytes allocation.
            let token = unsafe { toksep(&raw mut cursor, len.as_mut_ptr()) };
            assert_eq!(token, start.wrapping_add(offset), "{name}");
            // SAFETY: toksep initialized the length output.
            assert_eq!(unsafe { len.assume_init() }, expected_len, "{name}");
            if index + 1 == spans.len() {
                assert!(cursor.is_null(), "{name}");
            }
        }
        assert_eq!(bytes, input, "{name}");
    }
}
