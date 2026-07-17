/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Case-insensitive Levenshtein-distance automaton.
//!
//! [`CaseFoldLevenshtein`] accepts the UTF-8 keys whose per-codepoint
//! case-folded form is within a maximum Levenshtein edit distance
//! (insertions, deletions, substitutions — measured in codepoints) of its
//! folded needle. Folding applies [`char::to_lowercase`] to each codepoint
//! independently, with no locale- or context-dependent rules.
//!
//! The state carries one row of the classic edit-distance dynamic-programming
//! matrix; each decoded key codepoint advances the row in place. A subtree is
//! pruned as soon as every cell exceeds the budget — no key below that node
//! can get back within distance, because a row's minimum never decreases.
//! Codepoints split across trie edge labels are reassembled by
//! [`CodepointDecoder`]; keys that are not valid UTF-8 never match.

use super::utf8::CodepointDecoder;
use crate::iter::{Automaton, StateClass};
use string_utils::unicode_tolower_chars;

/// Streaming automaton accepting keys within a Levenshtein distance of a
/// needle, compared case-insensitively. See the [module docs](self) for the
/// matching model.
pub struct CaseFoldLevenshtein {
    /// The needle's codepoints, each case-folded.
    needle: Box<[char]>,
    /// Maximum edit distance accepted.
    max_dist: u32,
}

impl CaseFoldLevenshtein {
    /// Build an automaton matching keys within `max_dist` edits of `needle`.
    pub fn new(needle: &str, max_dist: u32) -> Self {
        Self {
            needle: unicode_tolower_chars(needle).collect(),
            max_dist,
        }
    }

    /// Advance the DP row by one key codepoint, in place. Returns `false`
    /// when every cell exceeds the budget, i.e. the state is dead.
    fn advance_row(&self, row: &mut [u32], c: char) -> bool {
        // `row[j]` is the distance between the key consumed so far and the
        // first `j` needle codepoints. Consuming `c` rebuilds the row from
        // the standard recurrence (substitute / delete / insert).
        let mut prev_diag = row[0];
        row[0] += 1;
        let mut min = row[0];
        for (j, &nc) in self.needle.iter().enumerate() {
            let substitute = prev_diag + u32::from(nc != c);
            prev_diag = row[j + 1];
            row[j + 1] = substitute.min(row[j + 1] + 1).min(row[j] + 1);
            min = min.min(row[j + 1]);
        }
        min <= self.max_dist
    }

    /// Consume one key byte into `state` in place. Returns `false` when the
    /// state died: the byte is invalid UTF-8 or every row cell exceeds the
    /// budget.
    fn step_in_place(&self, state: &mut LevenshteinState, byte: u8) -> bool {
        match state.partial.push(byte) {
            Err(_) => false,
            Ok(None) => true,
            // A codepoint that folds to several (e.g. 'İ' → "i\u{307}") costs
            // one row advance per folded codepoint, mirroring edit distance
            // over the fully folded key.
            Ok(Some(c)) => c
                .to_lowercase()
                .all(|folded| self.advance_row(&mut state.row, folded)),
        }
    }
}

/// One row of the edit-distance matrix, plus the decoder state of a codepoint
/// the driver has only partially delivered (an edge label may end
/// mid-codepoint).
#[derive(Clone)]
pub struct LevenshteinState {
    row: Box<[u32]>,
    partial: CodepointDecoder,
}

impl Automaton for CaseFoldLevenshtein {
    type State = LevenshteinState;

    fn start(&self) -> Self::State {
        // Before any key codepoint, matching the first `j` needle codepoints
        // costs `j` deletions.
        LevenshteinState {
            row: (0..=self.needle.len() as u32).collect(),
            partial: CodepointDecoder::new(),
        }
    }

    fn step(&mut self, state: &Self::State, byte: u8) -> Option<Self::State> {
        let mut state = state.clone();
        self.step_in_place(&mut state, byte).then_some(state)
    }

    /// Cloning the state allocates (the DP row is heap-backed), so unlike the
    /// default implementation — which goes through [`Self::step`] and pays a
    /// clone per byte — this clones once per edge label and advances the row
    /// in place.
    fn step_all(&mut self, state: &Self::State, bytes: &[u8]) -> Option<Self::State> {
        let mut state = state.clone();
        bytes
            .iter()
            .all(|&byte| self.step_in_place(&mut state, byte))
            .then_some(state)
    }

    fn classify(&self, state: &Self::State) -> StateClass {
        // The last cell is the distance between the full (folded) key and
        // the full needle. Descendants may still match — appending a
        // codepoint can lower that distance — so accepting states stay live.
        if state.partial.at_boundary() && state.row[self.needle.len()] <= self.max_dist {
            StateClass::LiveAccepting
        } else {
            StateClass::Live
        }
    }

    /// Accepted keys can differ from the needle by case at any position, and
    /// edits can rewrite the leading codepoints too — no byte prefix is
    /// shared by every match.
    fn literal_prefix(&self) -> Option<&[u8]> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn accepts(needle: &str, max_dist: u32, key: &str) -> bool {
        let mut automaton = CaseFoldLevenshtein::new(needle, max_dist);
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
    fn distance_zero_is_case_insensitive_equality() {
        assert!(accepts("hello", 0, "HELLO"));
        assert!(!accepts("hello", 0, "hellp"));
    }

    #[test]
    fn single_edits_match_at_distance_one() {
        assert!(accepts("hello", 1, "hellp")); // substitution
        assert!(accepts("hello", 1, "hell")); // deletion
        assert!(accepts("hello", 1, "helloo")); // insertion
        assert!(accepts("hello", 1, "HELL")); // edit + case fold
        assert!(!accepts("hello", 1, "help"));
    }

    #[test]
    fn edits_accumulate() {
        // dist("hello", "help") = 2 (substitute, delete); dist("hello", "hp")
        // = 4 (three deletions and a substitution).
        assert!(accepts("hello", 2, "help"));
        assert!(!accepts("hello", 3, "hp"));
        assert!(accepts("hello", 4, "hp"));
    }

    #[test]
    fn dead_subtrees_are_pruned_mid_key() {
        // After "xyz" every row cell exceeds 1, so the state dies before the
        // key ends rather than at classification time.
        let mut automaton = CaseFoldLevenshtein::new("hello", 1);
        let mut state = automaton.start();
        let mut died = false;
        for &b in b"xyzzy" {
            match automaton.step(&state, b) {
                Some(next) => state = next,
                None => {
                    died = true;
                    break;
                }
            }
        }
        assert!(died);
    }

    #[test]
    fn multibyte_edits_count_codepoints_not_bytes() {
        // 'é' is one codepoint (two bytes): substituting it is one edit.
        assert!(accepts("café", 1, "cafe"));
        assert!(accepts("café", 1, "CAFÉ"));
        assert!(!accepts("café", 0, "cafe"));
    }

    #[test]
    fn empty_needle_matches_keys_up_to_the_budget() {
        assert!(accepts("", 0, ""));
        assert!(!accepts("", 0, "a"));
        assert!(accepts("", 2, "ab"));
        assert!(!accepts("", 2, "abc"));
    }

    #[test]
    fn invalid_utf8_key_dies() {
        let mut automaton = CaseFoldLevenshtein::new("hello", 3);
        let state = automaton.start();
        assert!(automaton.step(&state, 0x80).is_none());
    }

    #[test]
    fn step_all_agrees_with_byte_at_a_time_stepping() {
        let mut automaton = CaseFoldLevenshtein::new("café", 1);

        let start = automaton.start();
        let whole = automaton.step_all(&start, "CAFE".as_bytes()).unwrap();

        let mut stepped = automaton.start();
        for &b in "CAFE".as_bytes() {
            stepped = automaton.step(&stepped, b).unwrap();
        }

        assert_eq!(whole.row, stepped.row);
        assert!(automaton.classify(&whole).is_accepting());
    }

    #[test]
    fn step_all_carries_partial_codepoints_across_labels() {
        // Split 'é' (C3 A9) between two step_all calls, as two trie edge
        // labels with the split inside the codepoint would.
        let mut automaton = CaseFoldLevenshtein::new("cliché", 0);
        let start = automaton.start();
        let mid = automaton.step_all(&start, b"clich\xC3").unwrap();
        assert_eq!(automaton.classify(&mid), StateClass::Live);
        let done = automaton.step_all(&mid, b"\xA9").unwrap();
        assert!(automaton.classify(&done).is_accepting());
    }

    #[test]
    fn step_all_dies_mid_label() {
        let mut automaton = CaseFoldLevenshtein::new("hello", 1);
        let start = automaton.start();
        assert!(automaton.step_all(&start, b"xyzzy").is_none());
        assert!(automaton.step_all(&start, b"clich\xC3\x28").is_none()); // invalid UTF-8
    }
}
