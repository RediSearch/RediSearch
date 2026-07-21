/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Incremental UTF-8 decoding for byte-streaming automatons.
//!
//! Trie edge labels can split a multi-byte codepoint across nodes, so an
//! automaton that matches per codepoint must carry the decoder state of an
//! incomplete sequence. [`CodepointDecoder`] is that state: feed it one byte
//! at a time and it hands back a [`char`] whenever a sequence completes.
//!
//! Decoding is delegated to [`utf8parse`], whose table-driven DFA rejects
//! overlong encodings, surrogates, and out-of-range codepoints. Its
//! callback-style [`Receiver`] events are adapted to a pull-style result:
//! at most one event fires per input byte, so a two-field receiver captures
//! the outcome losslessly.

use utf8parse::{Parser, Receiver};

/// The input byte cannot appear at its position in a UTF-8 sequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct InvalidUtf8;

/// Decoder state of an incomplete UTF-8 sequence.
#[derive(Clone, Default)]
pub(crate) struct CodepointDecoder {
    parser: Parser,
}

impl CodepointDecoder {
    /// A decoder sitting on a codepoint boundary.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// `true` when the decoder sits on a codepoint boundary (no bytes pending).
    pub(crate) fn at_boundary(&self) -> bool {
        // A fresh parser is the unique no-pending-bytes state (ground state,
        // empty accumulator); Parser exposes no query API, but derives PartialEq.
        self.parser == Parser::new()
    }

    /// Feed one byte. Returns `Ok(Some(c))` when it completes a codepoint,
    /// `Ok(None)` when more bytes are needed, and `Err(InvalidUtf8)` when the
    /// accumulated bytes cannot be part of a valid UTF-8 sequence.
    pub(crate) fn push(&mut self, byte: u8) -> Result<Option<char>, InvalidUtf8> {
        let mut outcome = ByteOutcome::default();
        self.parser.advance(&mut outcome, byte);
        if outcome.invalid {
            return Err(InvalidUtf8);
        }
        Ok(outcome.codepoint)
    }
}

/// Captures the single event [`Parser::advance`] emits for one input byte.
#[derive(Default)]
struct ByteOutcome {
    codepoint: Option<char>,
    invalid: bool,
}

impl Receiver for ByteOutcome {
    fn codepoint(&mut self, c: char) {
        self.codepoint = Some(c);
    }

    fn invalid_sequence(&mut self) {
        self.invalid = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(bytes: &[u8]) -> Result<Vec<char>, InvalidUtf8> {
        let mut decoder = CodepointDecoder::new();
        let mut out = Vec::new();
        for &b in bytes {
            if let Some(c) = decoder.push(b)? {
                out.push(c);
            }
        }
        Ok(out)
    }

    #[test]
    fn decodes_mixed_width_codepoints() {
        assert_eq!(decode("aé𐍈".as_bytes()).unwrap(), vec!['a', 'é', '𐍈']);
    }

    #[test]
    fn rejects_bare_continuation_byte() {
        assert_eq!(decode(&[0x80]), Err(InvalidUtf8));
    }

    #[test]
    fn rejects_truncated_sequence_followed_by_ascii() {
        // C3 starts a 2-byte sequence; 'x' is not a continuation byte.
        assert_eq!(decode(&[0xC3, b'x']), Err(InvalidUtf8));
    }

    #[test]
    fn rejects_overlong_and_surrogate_encodings() {
        // Overlong 2-byte encoding of '/' (0xC0 0xAF).
        assert_eq!(decode(&[0xC0, 0xAF]), Err(InvalidUtf8));
        // CESU-8 style surrogate half U+D800 (0xED 0xA0 0x80).
        assert_eq!(decode(&[0xED, 0xA0, 0x80]), Err(InvalidUtf8));
    }

    #[test]
    fn boundary_state_is_observable() {
        let mut decoder = CodepointDecoder::new();
        assert!(decoder.at_boundary());
        assert_eq!(decoder.push(0xC3), Ok(None));
        assert!(!decoder.at_boundary());
        assert_eq!(decoder.push(0xA9), Ok(Some('é')));
        assert!(decoder.at_boundary());
    }
}
