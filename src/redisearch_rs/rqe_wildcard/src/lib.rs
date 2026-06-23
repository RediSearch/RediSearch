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

use std::borrow::Cow;

use memchr::arch::all::is_prefix;

mod fmt;

#[derive(Copy, Clone, PartialEq, Eq)]
/// A pattern token.
pub enum Token<'pattern> {
    /// `*`. Matches zero or more characters.
    Any,
    /// `?`. Matches exactly one character.
    One,
    /// One or more literal characters (e.g. `Literal("foo")`).
    ///
    /// It borrows from the original pattern.
    Literal(&'pattern [u8]),
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
pub struct WildcardPattern<'pattern> {
    tokens: Vec<Token<'pattern>>,
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
    /// Number of "atoms" in the pattern, where one atom is the smallest matchable
    /// unit produced by [`Self::parse`]: either a single byte from a `Literal`
    /// token or a `?` / `*` token. Maintained on the fly by `parse` so callers
    /// (e.g. NFA-size dispatchers) don't have to re-walk `tokens`.
    atom_count: usize,
    /// Maximum number of input *bytes* the pattern can consume when treated
    /// as codepoint-aware (i.e. each `?` matches one UTF-8 codepoint, which
    /// is 1 to 4 bytes). `None` iff any [`Token::Any`] is present (variable
    /// length); otherwise [`Self::expected_length`] (byte length assuming
    /// `?` = 1 byte) plus three bytes per `?` for the worst-case
    /// 4-byte codepoint. Consumed by [`Self::matches_utf8`].
    max_utf8_byte_length: Option<usize>,
}

/// UTF-8 lead-byte width table.
///
/// Returns the number of bytes in the encoded codepoint that starts with `b`,
/// or `0` if `b` is a continuation byte or an invalid lead. Lead bytes are
/// `0x00..=0x7F` (1), `0xC2..=0xDF` (2), `0xE0..=0xEF` (3), `0xF0..=0xF4` (4).
/// On valid UTF-8 input, a `0` only occurs when an index lands inside a
/// multi-byte sequence — treated as `NoMatch` by [`WildcardPattern::matches_utf8`].
#[inline]
const fn utf8_char_width(b: u8) -> usize {
    match b {
        0x00..=0x7F => 1,
        0xC2..=0xDF => 2,
        0xE0..=0xEF => 3,
        0xF0..=0xF4 => 4,
        _ => 0,
    }
}

impl<'pattern> WildcardPattern<'pattern> {
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
    pub fn parse(pattern: &'pattern [u8]) -> Self {
        let mut tokens: Vec<Token<'pattern>> = Vec::new();

        let mut expected_length = Some(pattern.len());
        // Maintained alongside `tokens`: every branch below that contributes
        // one matchable unit (`?`, `*`, or a single literal byte — whether
        // it starts a fresh `Literal` or extends an existing one) bumps this
        // by one. `\` and `**` simplifications add nothing.
        let mut atom_count: usize = 0;
        // Count of `?` tokens. Needed to compute `max_utf8_byte_length`
        // below: each `?` can consume a 4-byte codepoint in UTF-8.
        let mut one_count: usize = 0;
        let mut pattern_iter = pattern.iter().copied().enumerate().peekable();

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
                            Some(b'?') => {
                                tokens.push(Token::One);
                                atom_count += 1;
                                one_count += 1;
                            }
                            Some(b'*') => {}
                            _ => break,
                        }
                        pattern_iter.next();
                    }

                    tokens.push(Token::Any);
                    atom_count += 1;
                    expected_length = None;
                }
                (b'*', _, false) => {
                    tokens.push(Token::Any);
                    atom_count += 1;
                    expected_length = None;
                }
                (b'?', _, false) => {
                    tokens.push(Token::One);
                    atom_count += 1;
                    one_count += 1;
                }
                (_, _, true) => {
                    // Handle escaped characters by starting a new `Literal` token
                    tokens.push(Token::Literal(&pattern[i..i + 1]));
                    atom_count += 1;
                }
                _ => {
                    match tokens.last_mut() {
                        // Literal encountered. Either start a new `Literal` token or extend the last one.
                        Some(Token::Literal(chunk)) => {
                            let chunk_len = chunk.len();
                            let chunk_start = i - chunk_len;
                            *chunk = &pattern[chunk_start..chunk_start + chunk_len + 1];
                        }
                        _ => tokens.push(Token::Literal(&pattern[i..i + 1])),
                    }
                    atom_count += 1;
                }
            }
            escape_next = false;
        }

        // With debug assertions on, recompute the count from the final
        // `tokens` and check it against the inline-maintained value.
        // Guards against future edits to the match arms above that
        // forget to bump `atom_count` alongside a push. Stripped in
        // release.
        #[cfg(debug_assertions)]
        {
            let recomputed: usize = tokens
                .iter()
                .map(|t| match t {
                    Token::Literal(bytes) => bytes.len(),
                    Token::One | Token::Any => 1,
                })
                .sum();
            debug_assert_eq!(
                atom_count, recomputed,
                "WildcardPattern::atom_count is out of sync with `tokens`",
            );
        }

        // For codepoint-aware (`matches_utf8`) consumers: each `?` can
        // consume up to a 4-byte UTF-8 codepoint, so the byte cap grows by
        // 3 per `?` above `expected_length` (which assumed 1 byte per `?`).
        // Pinned to `None` whenever `*` is present.
        let max_utf8_byte_length = expected_length.map(|n| n + 3 * one_count);

        Self {
            tokens,
            expected_length,
            atom_count,
            max_utf8_byte_length,
        }
    }

    /// Matches a key against the pattern.
    ///
    /// Implementation was adapted from the iterative
    /// algorithm described by [Dogan Kurt]. The major difference
    /// is that literals are not matched per character, but by chunks.
    ///
    /// [Dogan Kurt]: http://dodobyte.com/wildcard.html
    pub fn matches(&self, key: &[u8]) -> MatchOutcome {
        if self.tokens.is_empty() {
            return if key.is_empty() {
                MatchOutcome::Match
            } else {
                MatchOutcome::NoMatch
            };
        }

        if let Some(expected_length) = self.expected_length
            && key.len() > expected_length
        {
            return MatchOutcome::NoMatch;
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
                    let remaining_key_len = key.len() - i_k;
                    let (min_len, is_partial_match) = if chunk.len() > remaining_key_len {
                        (remaining_key_len, true)
                    } else {
                        (chunk.len(), false)
                    };
                    if !is_prefix(&key[i_k..], &chunk[..min_len]) {
                        try_backtrack!(bt_state, i_t, i_k, 'outer);
                    }
                    if is_partial_match {
                        return MatchOutcome::PartialMatch;
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

    /// Matches a UTF-8 key against the pattern with codepoint-aware
    /// semantics: `?` consumes exactly one codepoint (1 to 4 bytes), not
    /// one byte. Literal byte-prefix matching is unchanged — UTF-8 is
    /// self-synchronizing, so a literal token like `é` (`0xC3 0xA9`) still
    /// byte-matches the prefix of any key whose first codepoint is `é`.
    ///
    /// Callers must supply valid UTF-8: an index landing on a continuation
    /// byte (lead-width 0) returns [`MatchOutcome::NoMatch`] via the
    /// backtrack path.
    ///
    /// Used by the str-keyed wildcard iterator. Keep in sync with
    /// [`Self::matches`] — the two share structure; only the `Token::One`
    /// arm and the early length cap differ.
    pub fn matches_utf8(&self, key: &[u8]) -> MatchOutcome {
        if self.tokens.is_empty() {
            return if key.is_empty() {
                MatchOutcome::Match
            } else {
                MatchOutcome::NoMatch
            };
        }

        if let Some(max_bytes) = self.max_utf8_byte_length
            && key.len() > max_bytes
        {
            return MatchOutcome::NoMatch;
        }

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

        let mut i_t = 0;
        let mut i_k = 0;
        let mut bt_state = None;
        'outer: while i_k < key.len() {
            let Some(curr_token) = self.tokens.get(i_t) else {
                try_backtrack!(bt_state, i_t, i_k, 'outer);
            };

            match curr_token {
                Token::Any => {
                    i_t += 1;
                    if self.tokens.get(i_t).is_none() {
                        return MatchOutcome::Match;
                    }
                    bt_state = Some((i_t, i_k));
                }
                Token::Literal(chunk) => {
                    let remaining_key_len = key.len() - i_k;
                    let (min_len, is_partial_match) = if chunk.len() > remaining_key_len {
                        (remaining_key_len, true)
                    } else {
                        (chunk.len(), false)
                    };
                    if !is_prefix(&key[i_k..], &chunk[..min_len]) {
                        try_backtrack!(bt_state, i_t, i_k, 'outer);
                    }
                    if is_partial_match {
                        return MatchOutcome::PartialMatch;
                    }

                    i_t += 1;
                    i_k += chunk.len();
                }
                Token::One => {
                    // Codepoint-aware: advance by the width of the UTF-8
                    // codepoint that starts at `key[i_k]`. Width 0 means a
                    // continuation byte landed at the boundary — treat as
                    // no match and try to backtrack.
                    let width = utf8_char_width(key[i_k]);
                    if width == 0 || i_k + width > key.len() {
                        try_backtrack!(bt_state, i_t, i_k, 'outer);
                    }
                    i_t += 1;
                    i_k += width;
                }
            }
        }

        debug_assert!(
            i_k >= key.len(),
            "We should have consumed all bytes in the key by now"
        );

        // Match when the pattern is exhausted, or when exactly one token
        // remains and it is a trailing `*` (which can swallow zero chars).
        let trailing_star = i_t + 1 == self.tokens.len() && self.tokens[i_t] == Token::Any;
        if i_t == self.tokens.len() || trailing_star {
            MatchOutcome::Match
        } else {
            MatchOutcome::PartialMatch
        }
    }

    /// The expected length of an input that matches the pattern.
    ///
    /// Returns `None` if the pattern may match inputs of variable length (i.e.
    /// it contains at least one wildcard).
    pub const fn expected_length(&self) -> Option<usize> {
        self.expected_length
    }

    /// Maximum number of bytes an input UTF-8 key can have and still match
    /// the pattern under codepoint-aware semantics ([`Self::matches_utf8`]).
    ///
    /// `None` iff the pattern contains a `*` (variable length); otherwise
    /// `Some(expected_length + 3 * count(?))` — worst case where every `?`
    /// consumes a 4-byte codepoint. Cached during [`Self::parse`].
    pub const fn max_utf8_byte_length(&self) -> Option<usize> {
        self.max_utf8_byte_length
    }

    /// The number of "atoms" in the pattern: every `?`, every `*`, and one
    /// per byte inside any `Literal` token. Cached during [`Self::parse`],
    /// so this is O(1).
    ///
    /// Useful for downstream consumers that need to size data structures
    /// proportionally to the pattern (e.g. picking the right bitset width
    /// for a wildcard NFA).
    pub const fn atom_count(&self) -> usize {
        self.atom_count
    }
}

impl<'pattern> WildcardPattern<'pattern> {
    /// The parsed tokens.
    pub fn tokens(&self) -> &[Token<'pattern>] {
        &self.tokens
    }
}

/// Remove backslash escape sequences from a wildcard pattern.
///
/// Returns [`Cow::Borrowed`] when no escapes are present (zero-copy fast
/// path), or [`Cow::Owned`] with escapes removed.
pub fn remove_escape(s: &str) -> Cow<'_, str> {
    let bytes = s.as_bytes();
    let Some(first_escape) = bytes.iter().position(|&b| b == b'\\') else {
        return Cow::Borrowed(s);
    };

    let mut result = Vec::with_capacity(bytes.len() - 1);
    result.extend_from_slice(&bytes[..first_escape]);

    let mut read = first_escape;
    while read < bytes.len() {
        if bytes[read] == b'\\' {
            read += 1;
            if read >= bytes.len() {
                break;
            }
        }
        result.push(bytes[read]);
        read += 1;
    }

    // SAFETY: removing `\` (0x5C, a single-byte ASCII character) before
    // another byte cannot break a multi-byte UTF-8 sequence, so the
    // output is valid UTF-8 whenever the input is.
    Cow::Owned(String::from_utf8(result).expect("invalid UTF-8 in wildcard pattern"))
}
