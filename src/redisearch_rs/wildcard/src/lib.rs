//! Wildcard matching functionality.
//! Contains a [`TokenStream`] struct,
//! which can be created by parsing a slice of `i8`s or `u8`s.
//!
//! Implements the syntax described in the [Redis documentation](https://redis.io/docs/latest/develop/interact/search-and-query/advanced-concepts/query_syntax/).
//!
//! [`TokenStream`]: struct.TokenStream.html

#[derive(Clone, PartialEq, Eq)]
/// A pattern token.
pub enum Token<C> {
    /// Matches zero or more characters.
    Any,
    /// Matches exactly one character.
    One,
    /// Matches a literal character.
    Literal(C),
}

/// A parsed stream of tokens.
pub struct TokenStream<C> {
    tokens: Vec<Token<C>>,
    /// The length of the pattern.
    /// Used to short circuit the matching process
    /// in [`Self::matches_fixed_len`].
    pat_len: usize,
}

impl<'pat, C: Copy + CastU8> TokenStream<&'pat [C]> {
    /// Parses a pattern into a stream of tokens,
    /// handling escaped charactes and
    /// trimming the pattern by replacing consecutive * with a single *,
    /// and replacing occurrences of `*?` with `?*`.
    ///
    /// As the tokens may contain references to the original pattern,
    /// patterns with escaped characters may contain more than one token
    /// even if there is no wildcard. E.g. the pattern `br"f\\oo"` is parsed as
    /// `[br"f", br"\oo"]`.
    ///
    /// assert_tokens!(br"f\\oo", [Literal(br"f"), Literal(br"\oo")]);
    pub fn parse(pattern: &'pat [C]) -> Self {
        let mut tokens: Vec<Token<&'pat [C]>> = Vec::new();

        let mut pattern_iter = pattern
            .iter()
            .copied()
            .map(|c| c.cast_u8())
            .enumerate()
            .peekable();

        let mut escape_next = false;
        while let Some((i, curr_char)) = pattern_iter.next() {
            let next_char = pattern_iter.peek().map(|(_, c)| *c);

            match (curr_char, next_char, escape_next) {
                (b'\\', _, false) => {
                    // a '\' means we escape the next character, e.g. force that to be a literal.
                    escape_next = true;
                    continue;
                }
                (b'*', Some(b'*'), false) => {} // ** is equivalent to *
                (b'*', Some(b'?'), false) => {
                    // Replace all occurrences of `*?` with `?*` repetitively,
                    // e.g. `*??` becomes `??*`, `*?*?*` becomes `??*`.
                    loop {
                        match pattern_iter.next().map(|(_, c)| c) {
                            Some(b'?') => tokens.push(Token::One),
                            Some(b'*') => {}
                            _ => break,
                        }
                    }

                    tokens.push(Token::Any);
                    pattern_iter.next();
                }
                (b'*', _, false) => tokens.push(Token::Any),
                (b'?', _, false) => tokens.push(Token::One),
                (_, _, true) => {
                    // Handle escaped characters by starting a new `Literal` token
                    tokens.push(Token::Literal(&pattern[i..][..1]));
                }
                _ => match tokens.last_mut() {
                    // Literal encountered. Either start a new `Literal` token or extend the last one.
                    Some(Token::Literal(chunk)) => {
                        let chunk_len = chunk.len();
                        let chunk_start = i - chunk_len;
                        *chunk = &pattern[chunk_start..][..chunk_len + 1];
                    }
                    _ => tokens.push(Token::Literal(&pattern[i..][..1])),
                },
            }
            escape_next = false;
        }

        Self {
            tokens,
            pat_len: pattern.len(),
        }
    }

    /// Matches a key against the pattern.
    /// See [`Self::matches_fixed_len`] if you're certain
    /// your pattern does not contain any [`Token::Any`].
    pub fn matches(&self, mut key: &[C]) -> bool
    where
        C: PartialEq,
    {
        let mut tokens = self.tokens.iter().peekable();

        while let Some(curr_token) = tokens.next() {
            let next_token = tokens.peek();
            match (curr_token, next_token) {
                (Token::One, _) => {
                    // Skip to the next character
                    if key.is_empty() {
                        return false;
                    }
                    key = &key[1..];
                }
                (Token::Literal(chunk), _) => {
                    let key_chunk = match key.get(..chunk.len()) {
                        Some(slice) => slice,
                        None => return false, // Key does not contain enough characters to match the chunk
                    };
                    if key_chunk != *chunk {
                        return false;
                    }
                    key = &key[chunk.len()..];
                }

                (Token::Any, None) => return true, // Pattern ends with an asterisk, disregard the rest of the key
                (Token::Any, Some(Token::Literal(chunk))) => {
                    // Advance the key until we find the last occurrence
                    // of the literal,
                    // and match that literal too.
                    let Some(i) = key
                        .windows(chunk.len())
                        .enumerate()
                        .rev()
                        .find(|(_, key_chunk)| key_chunk == chunk)
                        .map(|(i, _)| i)
                    else {
                        // Next literal not found in the key
                        return false;
                    };
                    tokens.next();
                    key = &key[(i + chunk.len())..];
                }
                (Token::Any, Some(Token::One)) => {
                    unreachable!(
                        "Any '*?' sequence should have been swapped around during pattern trimming"
                    )
                }
                (Token::Any, Some(Token::Any)) => {
                    unreachable!("All occurences of '**' should have been trimmed to '*'")
                }
            }
        }
        // We should have reached the end of the key by now
        key.is_empty()
    }

    /// Matches the key against a pattern that only contains literal
    /// characters and '?'s. This is more performant than the general
    /// [`matches` method](Self::matches), as it is able to short-
    /// circuit if the length of the key is not equal to the length of
    /// the pattern, and it only needs to look at a single character at
    /// a time.
    ///
    /// Panics in case the pattern contained a '*' wildcard.
    pub fn matches_fixed_len(&self, mut key: &[C]) -> bool
    where
        C: PartialEq,
    {
        if key.len() != self.pat_len {
            return false;
        }

        for token in self.tokens.iter() {
            match token {
                Token::One => {
                    if key.is_empty() {
                        return false;
                    }
                    key = &key[1..];
                }
                Token::Literal(chunk) => {
                    let key_chunk = match key.get(..chunk.len()) {
                        Some(slice) => slice,
                        None => return false,
                    };
                    if key_chunk != *chunk {
                        return false;
                    }
                    key = &key[chunk.len()..];
                }
                Token::Any => panic!(
                    "`matches_fixed_len` must not be called on a token stream that contains a '*' wildcard"
                ),
            }
        }
        // We should have reached the end of the key by now
        key.is_empty()
    }
}

impl<C> TokenStream<C> {
    /// Get the first token in the stream.
    pub fn first(&self) -> Option<&Token<C>> {
        self.tokens.first()
    }
}

mod sealed {
    /// Sealed trait for `CastU8` to ensure that it cannot be implemented outside this module.
    pub trait Sealed {}
}

/// Simple trait that provides a method to cast a value to `u8`.
/// Implemented only for `i8` and `u8` and thus for `std::ffi::c_char`, and sealed
/// so that it cannot be implemented outside this module as the
/// correctness of [`TokenStream`] relies on correct implementation of this trait.
pub trait CastU8: Copy + sealed::Sealed {
    /// Perform a cast to `u8`.
    fn cast_u8(self) -> u8;
}

impl CastU8 for i8 {
    fn cast_u8(self) -> u8 {
        self as u8
    }
}
impl sealed::Sealed for i8 {}

impl CastU8 for u8 {
    fn cast_u8(self) -> u8 {
        self
    }
}
impl sealed::Sealed for u8 {}

#[cfg(test)]
mod tests {
    use super::{CastU8, Token, TokenStream};

    impl<C: CastU8> std::fmt::Debug for Token<&[C]> {
        // `Debug` implementation that formats `Token::Literal` such that
        // it matches the notation we're using in the tests
        // for easy comparison.
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Token::Any => write!(f, "Token::Any"),
                Token::One => write!(f, "Token::One"),
                Token::Literal(chunk) => {
                    write!(
                        f,
                        r#"Token::Literal(br"{}")"#,
                        String::from_utf8_lossy(
                            chunk
                                .iter()
                                .map(|c| c.cast_u8())
                                .collect::<Vec<u8>>()
                                .as_slice()
                        )
                    )
                }
            }
        }
    }

    /// Helper function that forces Rust to have the types of
    /// byte sequence literals (e.g. `b"hello"`) be slices instead of arrays
    /// when wrapped in a `Token`.
    fn coerce_tokens<const N: usize>(t: [Token<&[u8]>; N]) -> [Token<&[u8]>; N] {
        t
    }

    /// Helper function that forces Rust to have the types of
    /// byte sequence literals (e.g. `b"hello"`) be slices instead of arrays.
    fn coerce_literal<const N: usize>(l: [&[u8]; N]) -> [&[u8]; N] {
        l
    }

    /// Helper macro that parses the passed pattern and compares it with the expected tokens,
    /// forwarding to [`assert_eq!`].
    macro_rules! assert_tokens {
        ($pattern:literal, $expected:expr $(, $msg:literal)? $(,)?) => {
            let tokens = TokenStream::parse($pattern);

            assert_eq!(tokens.tokens, coerce_tokens($expected), $(, $msg)?);
        };
    }

    macro_rules! assert_matches {
        ($pattern:literal, $expected_results:expr $(, $msg:literal)? $(,)?) => {
            let tokens = TokenStream::parse($pattern);

            for expected in coerce_literal($expected_results) {
                assert!(tokens.matches(expected), $(, $msg)?);
            }
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

        assert_tokens!(br"\'foo", [Literal(br"'foo")]);

        assert_tokens!(br"\\'foo", [Literal(br"\'foo")]);

        // mid string
        assert_tokens!(br"f\oo", [Literal(br"f"), Literal(br"oo")]);

        assert_tokens!(br"f\\oo", [Literal(br"f"), Literal(br"\oo")]);

        assert_tokens!(br"f'oo", [Literal(br"f'oo")]);

        assert_tokens!(br"f'oo", [Literal(br"f'oo")]);

        assert_tokens!(br"f\'oo", [Literal(br"f"), Literal(br"'oo")]);

        assert_tokens!(br"f'oo", [Literal(br"f'oo")]);

        // end of string
        assert_tokens!(br"foo\", [Literal(br"foo")]);

        assert_tokens!(br"foo\\", [Literal(br"foo"), Literal(br"\")]);

        assert_tokens!(br"foo'", [Literal(br"foo'")]);

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
    }

    #[test]
    fn test_matches() {
        // no wildcard
        assert_matches!(br"foo", [br"foo"]);

        // ? at end
        assert_matches!(b"fo?", [b"foo"]);

        // ? at beginning
        assert_matches!(b"?oo", [b"foo"]);

        // * at end
        assert_matches!(b"fo*", [b"foo", b"fo", b"fooo"]);

        // * at beginning
        assert_matches!(b"*oo", [b"foo", b"fooo"]);
        assert_matches!(b"*", [b"bar", b""]);
        assert_matches!(b"*oo", [b"fofoo", b"foofoo"]);

        // mix
        assert_matches!(b"f?o*bar", [b"foobar", b"fooooobar"]);
    }
}
