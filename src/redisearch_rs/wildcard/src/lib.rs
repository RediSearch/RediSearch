//! Wildcard matching functionality, as specified in the
//! [RediSearch documentation](https://redis.io/docs/latest/develop/interact/search-and-query/advanced-concepts/query_syntax/#wildcard-matching).
//!
//! All functionality is provided through the [`WildcardPattern`] struct.
//! You can create a [`WildcardPattern`] from a pattern using [`WildcardPattern::parse`] and
//! then rely on [`WildcardPattern::matches`] to determine if a string matches the pattern.

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
pub struct WildcardPattern<'pattern, C> {
    tokens: Vec<Token<'pattern, C>>,
    /// The length of the raw pattern that this instance was parsed from.
    ///
    /// Used to short-circuit the matching process
    /// in [`Self::matches_fixed_len`].
    ///
    /// # Implementation Notes
    ///
    /// [`Self::pattern`] is usually going to be greater than the length of [`Self::tokens`].
    /// Parsing may simplify the pattern (e.g. by replacing consecutive `*` with a single `*`)
    /// and consecutive literals will be represented using a single [`Token`] instance.
    ///
    /// For example, `foo*bar` has a pattern length of 7 but it parses into 3 tokens:
    /// `Literal("foo")`, `Any`, and `Literal("bar")`.
    pattern_len: usize,
}

impl<'pattern, C: CharLike> WildcardPattern<'pattern, C> {
    /// Parses a raw pattern.
    ///
    /// It handles escaped characters and tries to trim the pattern
    /// by replacing consecutive * with a single * and
    /// replacing occurrences of `*?` with `?*`.
    ///
    /// # Avoiding allocations
    ///
    /// Parsing tries to avoid allocations: literal tokens refer to slices of the original pattern.
    ///
    /// As a consequence, patterns with escaped characters may be broken into
    /// more tokens than one might expect.
    /// E.g. `br"f\\oo"` is parsed as `[br"f", br"\oo"]`. This allows each token
    /// to reference a slice of the original pattern.
    /// The "obvious" parsing outcome (`[br"f\oo"]`) would require an allocation, since
    /// `foo` is not a substring of the original pattern.
    pub fn parse(pattern: &'pattern [C]) -> Self {
        let mut tokens: Vec<Token<'pattern, C>> = Vec::new();

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
            pattern_len: pattern.len(),
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
            // we have a match
            MatchOutcome::Match
        } else {
            // Otherwise, we would need more key characters to fully match
            // the pattern
            MatchOutcome::PartialMatch
        }
    }

    /// Matches the key against a pattern that contains only literals
    /// and '?'s.
    /// This is simpler and more performant than the general
    /// [`matches` method](Self::matches), since it doesn't have to
    /// perform backtracking.
    ///
    /// # Panics
    ///
    /// Panics in case the pattern contained a '*' wildcard.
    pub fn matches_fixed_len(&self, mut key: &[C]) -> MatchOutcome {
        // It is OK to compare against the length of the original pattern,
        // since there are no possible simplifications when the pattern
        // contains only literals and '?'s.
        if key.len() > self.pattern_len {
            return MatchOutcome::NoMatch;
        }

        for token in self.tokens.iter() {
            match token {
                Token::One => {
                    if key.is_empty() {
                        // It may have matched if we had more characters in the key
                        return MatchOutcome::PartialMatch;
                    }
                    key = &key[1..];
                }
                Token::Literal(chunk) => {
                    for i in 0..chunk.len() {
                        let Some(key_char) = key.get(i) else {
                            // It may have matched if we had more characters in the key
                            return MatchOutcome::PartialMatch;
                        };
                        if &chunk[i] != key_char {
                            return MatchOutcome::NoMatch;
                        }
                    }
                    key = &key[chunk.len()..];
                }
                Token::Any => panic!(
                    "`matches_fixed_len` must not be called on a token stream that contains a '*' wildcard"
                ),
            }
        }
        debug_assert!(
            key.is_empty(),
            "Key should be exhausted after having matched all tokens"
        );
        MatchOutcome::Match
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

impl<C: CharLike> std::fmt::Debug for Token<'_, C> {
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
                    String::from_utf8_lossy(&chunk.iter().map(|c| c.as_u8()).collect::<Vec<u8>>())
                )
            }
        }
    }
}
