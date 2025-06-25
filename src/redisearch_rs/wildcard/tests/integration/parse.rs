/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use memchr::memmem::Finder;
use wildcard::{Token, WildcardPattern};

/// Helper macro that parses the passed pattern and compares it with the expected tokens,
/// forwarding to [`assert_eq!`].
macro_rules! assert_tokens {
    ($pattern:literal, $expected:expr $(,)?) => {
        let tokens = WildcardPattern::parse($pattern);

        assert_eq!(
            tokens.tokens(),
            $expected,
            r#""{}" should be parsed as {:?}"#,
            String::from_utf8_lossy($pattern),
            tokens.tokens()
        );
    };
}

#[test]
fn test_parse_trim() {
    use Token::*;

    assert_tokens!(
        b"foo*bar",
        [Literal(b"foo"), MatchUpTo(Finder::new(b"bar"))]
    );

    assert_tokens!(
        b"*foo*bar",
        [
            MatchUpTo(Finder::new(b"foo")),
            MatchUpTo(Finder::new(b"bar"))
        ]
    );

    assert_tokens!(
        b"foo*bar*",
        [Literal(b"foo"), MatchUpTo(Finder::new(b"bar")), TrailingAny]
    );
    assert_tokens!(
        b"foo*bar*red??*l*bs?",
        [
            Literal(b"foo"),
            MatchUpTo(Finder::new(b"bar")),
            MatchUpTo(Finder::new(b"red")),
            One,
            One,
            MatchUpTo(Finder::new(b"l")),
            MatchUpTo(Finder::new(b"bs")),
            One,
        ],
    );
    assert_tokens!(b"foobar", [Literal(b"foobar")]);

    assert_tokens!(b"*foorbar", [MatchUpTo(Finder::new(b"foorbar"))]);

    assert_tokens!(
        b"foo*bar",
        [Literal(b"foo"), MatchUpTo(Finder::new(b"bar"))]
    );

    assert_tokens!(b"foobar*", [Literal(b"foobar"), TrailingAny]);

    assert_tokens!(b"**foobar", [MatchUpTo(Finder::new(b"foobar"))]);

    assert_tokens!(
        b"foo**bar",
        [Literal(b"foo"), MatchUpTo(Finder::new(b"bar"))]
    );

    assert_tokens!(b"foobar**", [Literal(b"foobar"), TrailingAny]);

    assert_tokens!(b"foo?*", [Literal(b"foo"), One, TrailingAny]);

    assert_tokens!(b"foo*?", [Literal(b"foo"), One, TrailingAny]);

    assert_tokens!(b"foo?**", [Literal(b"foo"), One, TrailingAny]);

    assert_tokens!(b"foo*?*", [Literal(b"foo"), One, TrailingAny]);

    assert_tokens!(b"foo**?", [Literal(b"foo"), One, TrailingAny]);

    assert_tokens!(b"foo**?", [Literal(b"foo"), One, TrailingAny]);

    assert_tokens!(b"***?***?***", [One, One, TrailingAny]);

    assert_tokens!(b"******?", [One, TrailingAny]);

    assert_tokens!(b"*?*?*?*?*", [One, One, One, One, TrailingAny]);
}

#[test]
fn test_parse_escape() {
    use Token::*;

    assert_tokens!(br"foo", [Literal(br"foo")]);

    // beginning of string
    assert_tokens!(br"\foo", [Literal(br"foo")]);

    assert_tokens!(br"\\foo", [Literal(br"\foo")]);

    assert_tokens!(br"'foo", [Literal(br"'foo")]);

    assert_tokens!(br"\'foo", [Literal(br"'foo")]);

    assert_tokens!(br"\\'foo", [Literal(br"\'foo")]);

    // mid string
    assert_tokens!(br"f\oo", [Literal(br"f"), Literal(br"oo")]);

    assert_tokens!(br"f\\oo", [Literal(br"f"), Literal(br"\oo")]);

    assert_tokens!(br"f'oo", [Literal(br"f'oo")]);

    assert_tokens!(br"f\'oo", [Literal(br"f"), Literal(br"'oo")]);

    // end of string
    assert_tokens!(br"foo\", [Literal(br"foo")]);

    assert_tokens!(br"foo\\", [Literal(br"foo"), Literal(br"\")]);

    assert_tokens!(br"foo'", [Literal(br"foo'")]);

    assert_tokens!(br"foo\'", [Literal(br"foo"), Literal(br"'")]);

    assert_tokens!(br"foo\\'", [Literal(br"foo"), Literal(br"\'")]);

    // wildcards
    assert_tokens!(br"foo\*", [Literal(br"foo"), Literal(br"*")]);

    assert_tokens!(br"foo\**", [Literal(br"foo"), Literal(br"*"), TrailingAny]);

    assert_tokens!(br"foo\?", [Literal(br"foo"), Literal(br"?")]);

    assert_tokens!(br"foo\??", [Literal(br"foo"), Literal(br"?"), One]);

    assert_tokens!(br"foo\?*", [Literal(br"foo"), Literal(br"?"), TrailingAny]);

    assert_tokens!(br"foo\*?", [Literal(br"foo"), Literal(br"*"), One]);

    assert_tokens!(b"*?A", [One, MatchUpTo(Finder::new(b"A"))]);
}
