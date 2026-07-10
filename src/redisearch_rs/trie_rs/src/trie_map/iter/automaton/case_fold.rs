/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Case-insensitive exact-match automaton.
//!
//! [`CaseFoldExact`] accepts exactly the UTF-8 keys that equal its needle
//! after per-codepoint case folding ([`char::to_lowercase`] applied to each
//! codepoint independently, no locale- or context-dependent rules). The
//! needle is folded once at construction; each stored key is folded
//! incrementally as the driver streams its bytes through [`Automaton::step`].
//!
//! Trie edge labels can split a multi-byte codepoint across nodes, so the
//! state carries the decoder state of an incomplete codepoint (see
//! [`CodepointDecoder`]) until its last byte arrives, then decodes, folds,
//! and matches the folded bytes against the needle. Keys that are not valid
//! UTF-8 never match: an invalid lead or continuation byte kills the state,
//! pruning that subtree.

use super::utf8::CodepointDecoder;
use super::{Automaton, StateClass};

/// Streaming automaton accepting keys equal to a needle up to per-codepoint
/// case folding. See the [module docs](self) for the matching model.
pub struct CaseFoldExact {
    /// The needle with every codepoint folded, as UTF-8 bytes.
    needle: Box<[u8]>,
}

impl CaseFoldExact {
    /// Build an automaton matching `needle` case-insensitively.
    pub fn new(needle: &str) -> Self {
        let folded: String = needle.chars().flat_map(char::to_lowercase).collect();
        Self {
            needle: folded.into_bytes().into_boxed_slice(),
        }
    }

    /// Fold `c` and match its folded UTF-8 bytes against the needle at
    /// `state.pos`, advancing on success.
    fn match_codepoint(&self, mut state: CaseFoldState, c: char) -> Option<CaseFoldState> {
        let mut buf = [0u8; 4];
        for folded in c.to_lowercase() {
            let bytes = folded.encode_utf8(&mut buf).as_bytes();
            let start = state.pos as usize;
            if self.needle.get(start..start + bytes.len())? != bytes {
                return None;
            }
            state.pos += bytes.len() as u32;
        }
        Some(state)
    }
}

/// Progress through the needle, plus the decoder state of a codepoint the
/// driver has only partially delivered (an edge label may end mid-codepoint).
#[derive(Clone)]
pub struct CaseFoldState {
    /// Byte offset into the folded needle matched so far.
    pos: u32,
    /// Decoder state of the incomplete input codepoint.
    partial: CodepointDecoder,
}

impl Automaton for CaseFoldExact {
    type State = CaseFoldState;

    fn start(&self) -> Self::State {
        CaseFoldState {
            pos: 0,
            partial: CodepointDecoder::new(),
        }
    }

    fn step(&mut self, state: &Self::State, byte: u8) -> Option<Self::State> {
        // On a codepoint boundary a full needle can accept no further input.
        if state.partial.at_boundary() && state.pos as usize == self.needle.len() {
            return None;
        }
        let mut state = state.clone();
        match state.partial.push(byte).ok()? {
            Some(c) => self.match_codepoint(state, c),
            None => Some(state),
        }
    }

    fn classify(&self, state: &Self::State) -> StateClass {
        if state.partial.at_boundary() && state.pos as usize == self.needle.len() {
            // Exact match: any descendant extends the key past the needle.
            StateClass::Terminal
        } else {
            StateClass::Live
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn accepts(needle: &str, key: &str) -> bool {
        let mut automaton = CaseFoldExact::new(needle);
        let mut state = automaton.start();
        for &b in key.as_bytes() {
            match automaton.step(&state, b) {
                Some(next) => state = next,
                None => return false,
            }
        }
        automaton.classify(&state).is_accepting()
    }

    #[test]
    fn ascii_case_variants_match() {
        assert!(accepts("hello", "hello"));
        assert!(accepts("hello", "HELLO"));
        assert!(accepts("hello", "hElLo"));
        assert!(accepts("HELLO", "hello"));
    }

    #[test]
    fn non_matches_die() {
        assert!(!accepts("hello", "hellp"));
        assert!(!accepts("hello", "hell"));
        assert!(!accepts("hello", "helloo"));
        assert!(!accepts("hello", ""));
    }

    #[test]
    fn multibyte_case_variants_match() {
        assert!(accepts("über", "Über"));
        assert!(accepts("Über", "über"));
        assert!(accepts("ñandú", "ÑANDÚ"));
        assert!(!accepts("über", "uber"));
    }

    #[test]
    fn one_to_many_folding_matches() {
        // 'İ' (U+0130) folds to "i\u{307}" — the needle must be folded the
        // same way for the two to compare equal.
        assert!(accepts("İstanbul", "İstanbul"));
        assert!(accepts("i\u{307}stanbul", "İstanbul"));
        assert!(!accepts("istanbul", "İstanbul"));
    }

    #[test]
    fn empty_needle_accepts_only_empty_key() {
        assert!(accepts("", ""));
        assert!(!accepts("", "a"));
    }

    #[test]
    fn invalid_utf8_key_dies() {
        let mut automaton = CaseFoldExact::new("a");
        let state = automaton.start();
        // A continuation byte cannot start a codepoint.
        assert!(automaton.step(&state, 0x80).is_none());

        // A lead byte followed by a non-continuation byte fails decoding.
        let state = automaton.step(&automaton.start(), 0xC3).unwrap();
        assert!(automaton.step(&state, b'x').is_none());
    }

    #[test]
    fn state_survives_split_codepoints() {
        // Feed 'é' (C3 A9) one byte at a time, as a trie with an edge split
        // inside the codepoint would.
        let mut automaton = CaseFoldExact::new("É");
        let state = automaton.start();
        let mid = automaton.step(&state, 0xC3).unwrap();
        assert_eq!(automaton.classify(&mid), StateClass::Live);
        let done = automaton.step(&mid, 0xA9).unwrap();
        assert_eq!(automaton.classify(&done), StateClass::Terminal);
    }
}
