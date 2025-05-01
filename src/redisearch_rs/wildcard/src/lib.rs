/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Wildcard matching functionality, as specified in the
//! [RediSearch documentation](https://redis.io/docs/latest/develop/interact/search-and-query/advanced-concepts/query_syntax/#wildcard-matching).
//!
//! All functionality is provided through the [`WildcardPattern`] struct.
//! You can create a [`WildcardPattern`] from a pattern using [`WildcardPattern::parse`] and
//! then rely on [`WildcardPattern::matches`] to determine if a string matches the pattern.

mod fmt;

#[derive(Copy, Clone, PartialEq, Eq)]
/// A pattern token.
pub enum Token<'pattern, C> {
    /// `*`. Matches zero or more characters.
    Any,
    /// `?`. Matches exactly one character.
    One,
    /// One or more literal characters (e.g. `Literal("foo")`).
    ///
    /// It borrows from the original pattern.
    Literal(&'pattern [C]),
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum MatchOutcome {
    /// The pattern matches the input.
    Match,
    /// The input isn't long enough to match the pattern.
    ///
    /// But there is a chance that the pattern matches a longer input
    /// that starts with the current input.
    ///
    /// For example, the pattern `foo*bar` doesn't match `foo`, but it
    /// would match `foobar`.
    PartialMatch,
    /// The pattern does not match the input, nor would it match a longer
    /// input that starts with the current input.
    ///
    /// For example, the pattern `foo*bar` doesn't match `boo`, nor would
    /// it match any other input that starts with `boo`.
    NoMatch,
}

/// A parsed pattern.
#[derive(Clone)]
pub struct WildcardPattern<'pattern, C> {
    tokens: Vec<Token<'pattern, C>>,
    /// The expected length of an input that will match the pattern.
    ///
    /// It is set to `None` if the pattern contains any wildcard tokens, since it will match
    /// inputs of different lengths.
    ///
    /// It is set to `Some` if there are no wildcard tokens, since we can simply count
    /// the number of characters in the pattern to determine the expected length.
    ///
    /// This can be used as an optimization to short-circuit the matching process
    /// early on if the input is longer than the expected length.
    expected_length: Option<usize>,
}

impl<'pattern, C: CharLike> WildcardPattern<'pattern, C> {
    /// Parses a raw pattern.
    ///
    /// Parsing takes care of escaping as well as pattern simplifications.
    ///
    /// # Escaping
    ///
    /// The backslash, `\`, is used to escape symbols that have special meaning in the pattern.
    /// In particular, it is used to escape:
    /// - `*` (wildcard), as `br"\*"`
    /// - `?` (single character wildcard), as `br"\?"`
    /// - `\` (backslash), as `br"\\"`
    ///
    /// There is no validation on escaped characters—whatever comes after the backslash is treated as a literal.
    /// For example, `br"\a"` is parsed as `vec![Token::Literal(b"a")]`, even though it is not a valid escape sequence.
    /// This matches the behaviour of the [original C implementation](https://github.com/RediSearch/RediSearch/blob/d988bde19385cd4e6aeec7987d344819eda66ab4/src/wildcard.c#L136).
    ///
    /// If you wish to reject some of these escaped characters as illegal, you should perform an additional validation step
    /// on top of the parsing process.
    ///
    /// # Simplifications
    ///
    /// The parsing routine tries to simplify the pattern when possible:
    ///
    /// - Consecutive `*` are replaced with a single `*`. `*` matches any number of characters, including none,
    ///   therefore consecutive `*` are equivalent to a single `*`.
    /// - `*?` sequences are replaced with `?*`. `*?` matches one or more characters, just like `?*` matches zero or more characters.
    ///   But the latter allows us to group together multiple `*` characters, which can be simplified further using the previous simplification rule.
    ///   For example, `*?*?*?` becomes `???***`, which is then further simplified to `???*`.
    ///
    /// # Allocations
    ///
    /// Parsing tries to minimize allocations: literal tokens refer to slices of the original pattern.
    ///
    /// As a consequence, patterns with escaped characters may be broken into
    /// more tokens than one might expect at a first glance.
    ///
    /// Let's look at `br"f\\oo"` as an example.
    /// The obvious parsing outcome would be `vec![Token::Literal(br"f\oo")]`, where the escaped backslash is
    /// resolved to a single character (`\`).
    /// But `br"f\oo"` is not a substring of the original pattern. The parsing routine would have to allocate
    /// new memory to store the re-assembled pattern with escaped characters resolved.
    ///
    /// Instead, we split at escape points to maintain zero-copy references. `br"f\\oo"`
    /// is parsed as two tokens rather than one: `vec![Token::Literal(br"f"), Token::Literal(br"\oo")]`.
    /// Both tokens refer to slices of the original pattern and, combined, they give us the correct (resolved)
    /// pattern.
    pub fn parse(pattern: &'pattern [C]) -> Self {
        let mut tokens: Vec<Token<'pattern, C>> = Vec::new();

        let mut expected_length = Some(pattern.len());
        let mut pattern_iter = pattern
            .iter()
            .copied()
            .map(|c| c.as_u8())
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
                        match pattern_iter.peek().map(|(_, c)| c) {
                            Some(b'?') => tokens.push(Token::One),
                            Some(b'*') => {}
                            _ => break,
                        }
                        pattern_iter.next();
                    }

                    tokens.push(Token::Any);
                    expected_length = None;
                }
                (b'*', _, false) => {
                    tokens.push(Token::Any);
                    expected_length = None;
                }
                (b'?', _, false) => tokens.push(Token::One),
                (_, _, true) => {
                    // Handle escaped characters by starting a new `Literal` token
                    tokens.push(Token::Literal(&pattern[i..i + 1]));
                }
                _ => match tokens.last_mut() {
                    // Literal encountered. Either start a new `Literal` token or extend the last one.
                    Some(Token::Literal(chunk)) => {
                        let chunk_len = chunk.len();
                        let chunk_start = i - chunk_len;
                        *chunk = &pattern[chunk_start..chunk_start + chunk_len + 1];
                    }
                    _ => tokens.push(Token::Literal(&pattern[i..i + 1])),
                },
            }
            escape_next = false;
        }

        Self {
            tokens,
            expected_length,
        }
    }

    /// Matches a key against the pattern.
    /// See [`Self::matches_fixed_len`] if you're certain
    /// your pattern does not contain any [`Token::Any`].
    ///
    /// Implementation was adapted from the iterative
    /// algorithm described by [Dogan Kurt]. The major difference
    /// is that literals are not matched per character, but by chunks.
    ///
    /// [Dogan Kurt]: http://dodobyte.com/wildcard.html
    pub fn matches(&self, key: &[C]) -> MatchOutcome {
        if self.tokens.is_empty() {
            return if key.is_empty() {
                MatchOutcome::Match
            } else {
                MatchOutcome::NoMatch
            };
        }

        if let Some(expected_length) = self.expected_length {
            if key.len() > expected_length {
                return MatchOutcome::NoMatch;
            }
        }

        // Backtrack if possible, otherwise return early claiming we can't match.
        macro_rules! try_backtrack {
            ($bt_state:expr, $i_t:ident, $i_k:ident, $label:lifetime) => {
                if let Some((bt_i_t, bt_i_k)) = &mut $bt_state {
                    $i_t = *bt_i_t;
                    $i_k = *bt_i_k + 1;
                    *bt_i_k = $i_k;
                    continue $label;
                } else {
                    return MatchOutcome::NoMatch;
                }
            };
        }

        let mut i_t = 0; // Index in the list of tokens
        let mut i_k = 0; // Index in the key slice
        let mut bt_state = None; // Backtrack state
        'outer: while i_k < key.len() {
            // Obtain the current token
            let Some(curr_token) = self.tokens.get(i_t) else {
                // No more tokens left to match, but we haven't exhausted
                // the key yet. Can we backtrack?
                try_backtrack!(bt_state, i_t, i_k, 'outer);
            };

            match curr_token {
                Token::Any => {
                    i_t += 1;
                    if self.tokens.get(i_t).is_none() {
                        // Pattern ends with a '*' wildcard.
                        // We have a match, no matter what the rest of the key
                        // looks like.
                        return MatchOutcome::Match;
                    }

                    // A wildcard can match zero or more characters.
                    // We start by capturing zero characters—i.e. we don't
                    // increment `i_k`.
                    // We keep track of where the wildcard appears in the pattern
                    // using the backtrack state. In particular, we store the
                    // index of the wildcard in the pattern and the index of the
                    // key token right after the wildcard.
                    // If we have to backtrack, we will then capture exactly one character.
                    // If that doesn't work, we will try again by capturing two characters.
                    // Rinse and repeat until we either find a match or run out of key.
                    bt_state = Some((i_t, i_k));
                }
                Token::Literal(chunk) => {
                    for i in 0..chunk.len() {
                        let Some(key_char) = key.get(i_k + i) else {
                            // It may have matched if we had more characters in the key
                            return MatchOutcome::PartialMatch;
                        };
                        if &chunk[i] != key_char {
                            try_backtrack!(bt_state, i_t, i_k, 'outer);
                        }
                    }

                    i_t += 1;
                    i_k += chunk.len();
                }
                Token::One => {
                    // Advance both indices, since `?` matches exactly one character
                    i_t += 1;
                    i_k += 1;
                }
            }
        }

        debug_assert!(
            i_k >= key.len(),
            "We should have consumed all characters in the key by now"
        );

        if i_t == self.tokens.len() {
            // If there are no more tokens left, we have a match
            MatchOutcome::Match
        } else if i_t + 1 == self.tokens.len() && self.tokens[i_t] == Token::Any {
            // If there's only one token left, and it's a '*' token,
            // we have a match. Even if the key is empty, since '*' matches
            // zero or more characters.
            MatchOutcome::Match
        } else {
            // Otherwise, we would need more key characters to fully match
            // the pattern
            MatchOutcome::PartialMatch
        }
    }
}

impl<'pattern, C> WildcardPattern<'pattern, C> {
    /// The parsed tokens.
    pub fn tokens(&self) -> &[Token<'pattern, C>] {
        &self.tokens
    }
}

/// A character type that can be used for wildcard matching.
///
/// # `c_char`
///
/// We want to provide our wildcard matching functionality for two types of "characters":
/// `u8`s and `std::ffi::c_char`s.
/// The latter is an alias for either `i8` or `u8` depending on the platform, hence
/// our implementation for `i8` below.
///
/// # Sealed
///
/// The trait is [sealed](https://predr.ag/blog/definitive-guide-to-sealed-traits-in-rust/)
/// to ensure that it cannot be implemented outside this module since the correctness of our
/// [`WildcardPattern`] implementation can't be guaranteed
/// for other types.
pub trait CharLike: Copy + PartialEq + sealed::Sealed {
    /// Perform a cast to `u8`.
    fn as_u8(self) -> u8;
}

mod sealed {
    pub trait Sealed {}
}

impl CharLike for i8 {
    fn as_u8(self) -> u8 {
        self as u8
    }
}
impl sealed::Sealed for i8 {}

impl CharLike for u8 {
    fn as_u8(self) -> u8 {
        self
    }
}
impl sealed::Sealed for u8 {}
