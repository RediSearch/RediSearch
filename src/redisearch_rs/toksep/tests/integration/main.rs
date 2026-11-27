/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use toksep::{is_separator, token_len};

const SEPARATORS: &[u8] = b" \t!\"#$%&'()*+,-./:;<=>?@[]^`{|}~";

#[test]
fn separator_alphabet() {
    for byte in u8::MIN..=u8::MAX {
        assert_eq!(is_separator(byte), SEPARATORS.contains(&byte), "{byte}");
    }
}

#[test]
fn scanning_examples() {
    for (name, input, expected) in [
        ("empty", b"".as_slice(), 0),
        ("unterminated", b"a_b\\", 4),
        ("leading separator", b" a", 0),
        ("odd escapes", b"ab\\ c de", 5),
        ("even escapes", b"ab\\\\ c", 4),
        ("invalid utf8 and controls", b"\xff\x80\n\r\x01 x", 5),
    ] {
        assert_eq!(token_len(input.iter().copied()), expected, "{name}");
    }
}

#[test]
fn scanning_reads_nothing_past_terminator() {
    for (name, input) in [
        ("NUL", b"abc\0".as_slice()),
        ("separator", b"abc "),
        ("escaped NUL", b"abc\\\0"),
    ] {
        let guarded = input.iter().copied().chain(std::iter::once_with(|| {
            panic!("{name}: read past token terminator")
        }));
        assert_eq!(token_len(guarded), input.len() - 1, "{name}");
    }
}

// Proptest uses OS entropy and filesystem persistence, unavailable under Miri isolation.
#[cfg(not(miri))]
proptest::proptest! {
    #[test]
    fn scanning_matches_escape_parity(
        mut input in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..256),
        escape_prefix in 0usize..16,
    ) {
        input.splice(..0, std::iter::repeat_n(b'\\', escape_prefix));
        let expected = input.iter().enumerate().find_map(|(index, byte)| {
            let backslashes = input[..index].iter().rev().take_while(|&&b| b == b'\\').count();
            (*byte == 0 || (SEPARATORS.contains(byte) && backslashes % 2 == 0)).then_some(index)
        }).unwrap_or(input.len());
        proptest::prop_assert_eq!(token_len(input.iter().copied()), expected);
    }
}
