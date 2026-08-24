/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use rqe_wildcard::{Token, WildcardPattern};

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

    assert_tokens!(b"foo*bar", [Literal(b"foo"), Any, Literal(b"bar")]);

    assert_tokens!(b"*foo*bar", [Any, Literal(b"foo"), Any, Literal(b"bar")]);

    assert_tokens!(b"foo*bar*", [Literal(b"foo"), Any, Literal(b"bar"), Any]);

    assert_tokens!(
        b"foo*bar*red??*l*bs?",
        [
            Literal(b"foo"),
            Any,
            Literal(b"bar"),
            Any,
            Literal(b"red"),
            One,
            One,
            Any,
            Literal(b"l"),
            Any,
            Literal(b"bs"),
            One,
        ],
    );
    assert_tokens!(b"foobar", [Literal(b"foobar")]);

    assert_tokens!(b"*foorbar", [Any, Literal(b"foorbar")]);

    assert_tokens!(b"foo*bar", [Literal(b"foo"), Any, Literal(b"bar")]);

    assert_tokens!(b"foobar*", [Literal(b"foobar"), Any]);

    assert_tokens!(b"**foobar", [Any, Literal(b"foobar")]);

    assert_tokens!(b"foo**bar", [Literal(b"foo"), Any, Literal(b"bar")]);

    assert_tokens!(b"foobar**", [Literal(b"foobar"), Any]);

    assert_tokens!(b"foo?*", [Literal(b"foo"), One, Any]);

    assert_tokens!(b"foo*?", [Literal(b"foo"), One, Any]);

    assert_tokens!(b"foo?**", [Literal(b"foo"), One, Any,]);

    assert_tokens!(b"foo*?*", [Literal(b"foo"), One, Any,]);

    assert_tokens!(b"foo**?", [Literal(b"foo"), One, Any]);

    assert_tokens!(b"foo**?", [Literal(b"foo"), One, Any]);

    assert_tokens!(b"***?***?***", [One, One, Any]);

    assert_tokens!(b"******?", [One, Any]);

    assert_tokens!(b"*?*?*?*?*", [One, One, One, One, Any]);
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

    assert_tokens!(br"foo\**", [Literal(br"foo"), Literal(br"*"), Any]);

    assert_tokens!(br"foo\?", [Literal(br"foo"), Literal(br"?")]);

    assert_tokens!(br"foo\??", [Literal(br"foo"), Literal(br"?"), One]);

    assert_tokens!(br"foo\?*", [Literal(br"foo"), Literal(br"?"), Any]);

    assert_tokens!(br"foo\*?", [Literal(br"foo"), Literal(br"*"), One]);

    assert_tokens!(b"*?A", [One, Any, Literal(b"A")]);
}

/// Helper macro mirroring [`assert_tokens`] for [`WildcardPattern::parse_unescaped`].
macro_rules! assert_unescaped_tokens {
    ($pattern:literal, $expected:expr $(,)?) => {
        let tokens = WildcardPattern::parse_unescaped($pattern);

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
fn test_parse_unescaped_treats_backslash_as_a_literal() {
    use Token::*;

    // A backslash is an ordinary byte: it neither disappears nor splits the
    // literal run it sits in, which is what lets a pattern reach a term that
    // holds a backslash of its own.
    assert_unescaped_tokens!(br"f\oo", [Literal(br"f\oo")]);

    assert_unescaped_tokens!(br"\foo", [Literal(br"\foo")]);

    assert_unescaped_tokens!(br"foo\", [Literal(br"foo\")]);

    assert_unescaped_tokens!(br"f\\oo", [Literal(br"f\\oo")]);

    // `*` and `?` keep their meaning; only escaping is gone.
    assert_unescaped_tokens!(br"f\*o", [Literal(br"f\"), Any, Literal(br"o")]);

    assert_unescaped_tokens!(br"f\?o", [Literal(br"f\"), One, Literal(br"o")]);

    // The simplifications still apply.
    assert_unescaped_tokens!(br"a\**b", [Literal(br"a\"), Any, Literal(b"b")]);
}

#[test]
fn test_parse_unescaped_atom_count_counts_the_backslash() {
    // The NFA sizes its bitset from this, so a backslash must occupy a
    // position of its own rather than being folded away.
    assert_eq!(WildcardPattern::parse_unescaped(br"f\oo").atom_count(), 4);
    assert_eq!(WildcardPattern::parse(br"f\oo").atom_count(), 3);
}
