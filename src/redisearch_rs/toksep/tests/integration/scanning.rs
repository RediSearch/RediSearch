/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use toksep::{is_separator, token_len};

const SEPARATORS: &[u8] = b" \t,./(){}[]:;~!@#$%^&*-=+|'`\"<>?";

#[test]
fn separator_membership_matches_the_tokenizer_alphabet() {
    for byte in 0..=u8::MAX {
        assert_eq!(
            is_separator(byte),
            SEPARATORS.contains(&byte),
            "{byte:#04x}"
        );
    }
}

#[test]
fn scanning_stops_without_reading_the_rest() {
    let mut bytes = b"word,rest".iter().copied();
    assert_eq!(token_len(&mut bytes), 4);
    assert_eq!(bytes.next(), Some(b'r'));
}

#[test]
fn token_lengths_handle_escapes_and_terminators() {
    for (name, input, expected) in [
        ("empty", b"".as_slice(), 0),
        ("leading separator", b",a", 0),
        ("no separator", b"word", 4),
        ("escaped separator", br"a\,b,c", 4),
        ("even backslashes", br"a\\,b", 3),
        ("odd backslashes", br"a\\\,b", 6),
        ("trailing backslash", br"a\", 2),
        ("control and high bytes", b"a\n\r_\x80,", 5),
        ("NUL", b"ab\0cd", 2),
        ("escaped NUL", b"a\\\0b", 2),
    ] {
        assert_eq!(token_len(input.iter().copied()), expected, "{name}");
    }
}

#[cfg(not(miri))]
mod properties {
    use super::SEPARATORS;
    use proptest::prelude::*;
    use toksep::token_len;

    proptest! {
        #[test]
        fn token_length_matches_unescaped_separator_position(bytes in prop::collection::vec(
            prop_oneof![any::<u8>(), prop::sample::select(b"a\\, \t\0\x80_".to_vec())], 0..256,
        )) {
            let expected = bytes.iter().enumerate().position(|(index, byte)| {
                let backslashes = bytes[..index].iter().rev().take_while(|&&b| b == b'\\').count();
                *byte == 0 || (SEPARATORS.contains(byte) && backslashes % 2 == 0)
            }).unwrap_or(bytes.len());
            prop_assert_eq!(token_len(bytes.iter().copied()), expected);
        }
    }
}
