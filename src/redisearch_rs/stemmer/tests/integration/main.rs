/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use stemmer::StemBuffer;

#[test]
fn changed_stems_have_prefix_and_terminator() {
    let mut buffer = StemBuffer::default();
    for (name, original, stem, expected) in [
        (
            "shorter",
            b"worlds".as_slice(),
            b"world".as_slice(),
            b"+world\0".as_slice(),
        ),
        ("empty stem", b"a", b"", b"+\0"),
        ("longer", b"a", b"abc", b"+abc\0"),
        ("same length", b"cat", b"car", b"+car\0"),
        (
            "non-ASCII case",
            "É".as_bytes(),
            "é".as_bytes(),
            "+é\0".as_bytes(),
        ),
        ("NUL mismatch", b"a\0x", b"abx", b"+abx\0"),
        ("different lengths after NUL", b"a\0", b"a\0x", b"+a\0x\0"),
    ] {
        assert_eq!(buffer.update(original, stem), Some(expected), "{name}");
    }
}

#[test]
fn unchanged_stems_are_suppressed() {
    let mut buffer = StemBuffer::default();
    for (name, original, stem) in [
        ("empty", b"".as_slice(), b"".as_slice()),
        ("identical", b"world", b"world"),
        ("ASCII case", b"WoRlD", b"world"),
        ("NUL stops comparison", b"A\0x", b"a\0y"),
        ("invalid UTF-8", b"\xff\x80A", b"\xff\x80a"),
    ] {
        assert_eq!(buffer.update(original, stem), None, "{name}");
    }
}

#[test]
fn repeated_stems_replace_the_buffer() {
    let mut buffer = StemBuffer::default();
    for stem in ["я".repeat(1024), "x".into(), "é".repeat(2048), "".into()] {
        let expected = format!("+{stem}\0").into_bytes();
        assert_eq!(
            buffer.update(b"original", stem.as_bytes()),
            Some(expected.as_slice())
        );
    }
}

#[cfg(not(miri))]
proptest::proptest! {
    #[test]
    fn arbitrary_bytes_follow_length_and_nul_stopping_comparison(
        pairs in proptest::collection::vec((
            proptest::collection::vec(proptest::prelude::any::<u8>(), 0..256),
            proptest::collection::vec(proptest::prelude::any::<u8>(), 0..256),
            0u8..4,
        ), 1..16),
    ) {
        let mut buffer = StemBuffer::default();
        for (mut original, mut stem, kind) in pairs {
            match kind {
                1 => stem = original.to_ascii_uppercase(),
                2 => {
                    original.push(0);
                    original.extend_from_slice(&stem);
                    stem = original.to_ascii_lowercase();
                    if let Some(last) = stem.last_mut() { *last ^= 1; }
                }
                3 => stem.resize(original.len(), b'x'),
                _ => {}
            }
            let visible = |bytes: &[u8]| bytes.iter().copied().take_while(|&b| b != 0)
                .map(|b| b.to_ascii_lowercase()).collect::<Vec<_>>();
            let unchanged = original.len() == stem.len() && visible(&original) == visible(&stem);
            let expected = (!unchanged).then(|| [b"+".as_slice(), &stem, b"\0"].concat());
            proptest::prop_assert_eq!(buffer.update(&original, &stem), expected.as_deref());
        }
    }
}
