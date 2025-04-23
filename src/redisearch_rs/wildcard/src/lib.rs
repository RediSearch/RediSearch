//! Wildcard matching functionality, as specified in the
//! [RediSearch documentation](https://redis.io/docs/latest/develop/interact/search-and-query/advanced-concepts/query_syntax/#wildcard-matching).
//!
//! All functionality is provided through the [`TokenStream`] struct.
//! You can create a [`TokenStream`] from a pattern using [`TokenStream::parse`] and
//! then rely on [`TokenStream::matches`] to determine if a string matches the pattern.

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

/// A parsed stream of tokens.
pub struct TokenStream<'pattern, C> {
    tokens: Vec<Token<'pattern, C>>,
    /// The length of the pattern that this stream was parsed from.
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

impl<'pattern, C: CharLike> TokenStream<'pattern, C> {
    /// Parses a pattern into a stream of tokens.
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
    pub fn matches(&self, key: &[C]) -> bool
    where
        C: PartialEq,
    {
        if self.tokens.is_empty() {
            return key.is_empty();
        }

        let mut i_t = 0; // Index in the list of tokens
        let mut i_k = 0; // Index in the key slice
        let mut bt_state = None; // Backtrack state
        while i_k < key.len() {
            // Obtain the current token
            let Some(curr_token) = self.tokens.get(i_t) else {
                // No more tokens left to match
                let Some((bt_i_t, bt_i_k)) = &mut bt_state else {
                    // There's nowhere to backtrack to
                    break;
                };
                // Backtrack
                i_t = *bt_i_t;
                i_k = *bt_i_k + 1;
                *bt_i_k = i_k;
                continue;
            };

            match curr_token {
                Token::Any => {
                    i_t += 1;
                    // Set backtrack state to the current values
                    // of `i_t`, and `i_k`,
                    // i.e. to the current key character,
                    // and the token right after the '*'
                    // we just encountered.
                    bt_state = Some((i_t, i_k));
                    if self.tokens.get(i_t).is_none() {
                        // Pattern ends with a '*' wildcard.
                        // Disregard the rest of the key
                        return true;
                    }
                }
                Token::Literal(chunk) => {
                    let Some(key_chunk) = key.get(i_k..i_k + chunk.len()) else {
                        // No characters left, so no way to match the chunk
                        return false;
                    };

                    if *chunk != key_chunk {
                        let Some((bt_i_t, bt_i_k)) = &mut bt_state else {
                            // There's nowhere to backtrack to
                            return false;
                        };

                        // Backtrack
                        i_t = *bt_i_t;
                        i_k = *bt_i_k + 1;
                        *bt_i_k = i_k;
                        continue;
                    }
                    i_t += 1;
                    i_k += chunk.len();
                }

                Token::One => {
                    // Simply advance both indices
                    // as with '?' we ignore 1 character
                    i_t += 1;
                    i_k += 1;
                }
            }
        }

        // If there's one token left, and it's a '*' token,
        // we have a match
        if i_t == self.tokens.len() - 1 && self.tokens[i_t] == Token::Any {
            return true;
        }

        // At this point we should have handled all tokens
        i_t == self.tokens.len() && i_k == key.len()
    }

    /// Matches the key against a pattern that only contains literal
    /// characters and '?'s. This is simpler and more performant than the general
    /// [`matches` method](Self::matches), as it is able to short-
    /// circuit if the length of the key is not equal to the length of
    /// the pattern, and doesn't support backtracking.
    ///
    /// Panics in case the pattern contained a '*' wildcard.
    pub fn matches_fixed_len(&self, mut key: &[C]) -> bool
    where
        C: PartialEq,
    {
        if key.len() != self.pattern_len {
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

    /// The parsed tokens.
    pub fn tokens(&self) -> &[Token<'pattern, C>] {
        &self.tokens
    }
}

impl<'pattern, C> TokenStream<'pattern, C> {
    /// Get the first token in the stream.
    pub fn first(&self) -> Option<&Token<'pattern, C>> {
        self.tokens.first()
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
/// [`TokenStream`] implementation can't be guaranteed
/// for other types.
pub trait CharLike: Copy + sealed::Sealed {
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
