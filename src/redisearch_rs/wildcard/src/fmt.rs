/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Implementations of `Debug` and `Display` for our public types.
use crate::{CharLike, Token, WildcardPattern};

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

impl<C: CharLike> std::fmt::Display for Token<'_, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Token::Any => write!(f, "*"),
            Token::One => write!(f, "?"),
            Token::Literal(chunk) => {
                write!(
                    f,
                    r#"{}"#,
                    String::from_utf8_lossy(&chunk.iter().map(|c| c.as_u8()).collect::<Vec<u8>>())
                )
            }
        }
    }
}

impl<C: CharLike> std::fmt::Debug for WildcardPattern<'_, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WildcardPattern")
            .field("tokens", &self.tokens)
            .field("expected_length", &self.expected_length)
            .finish()
    }
}

impl<C: CharLike> std::fmt::Display for WildcardPattern<'_, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut pattern = String::new();
        for token in &self.tokens {
            pattern.push_str(&token.to_string());
        }
        write!(f, "{}", pattern)
    }
}
