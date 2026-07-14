/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Bit-parallel case-insensitive Levenshtein automaton.
//!
//! [`CaseFoldLevenshteinNfa`] accepts exactly the keys
//! [`CaseFoldLevenshtein`](super::CaseFoldLevenshtein) accepts — per-codepoint
//! case-folded form within a maximum Levenshtein edit distance (in
//! codepoints) of the folded needle — but simulates the Levenshtein NFA with
//! bitmasks (Baeza-Yates–Navarro) instead of carrying a dynamic-programming
//! row:
//!
//! - The state is `max_dist + 1` machine words plus a codepoint decoder —
//!   fully inline, so the clone the driver takes at every trie branch point
//!   is a small `memcpy` instead of a heap allocation.
//! - Advancing by one key codepoint is a constant number of shift/mask
//!   operations per word — O(max_dist) — where the DP row walks the whole
//!   needle.
//!
//! Bit *j* of row *i* means: the first `j` codepoints of the needle can be
//! aligned against the folded key consumed so far with at most `i` edits.
//! Consuming a folded key codepoint `c` with per-char match mask `B[c]`
//! (bit `j + 1` set iff `needle[j] == c`) advances every row:
//!
//! ```text
//! new[0] = (old[0] << 1) & B[c]                      // match
//! new[i] = (old[i] << 1) & B[c]                      // match
//!        | old[i-1]                                  // insertion in key
//!        | old[i-1] << 1                             // substitution
//!        | new[i-1] << 1                             // deletion (ε-closure)
//! ```
//!
//! A subtree is pruned when every row is zero — no alignment survives.
//! The key matches when any row has bit `needle_len` set. Codepoints split
//! across trie edge labels are reassembled by [`CodepointDecoder`]; keys
//! that are not valid UTF-8 never match.
//!
//! The word width bounds the needle (`needle_len + 1` bits must fit) and the
//! inline row array bounds the distance ([`MAX_NFA_DIST`]).
//! [`CaseFoldLevenshteinNfa::new`] returns `None` past either bound; callers
//! fall back to the DP-row automaton.

use super::utf8::CodepointDecoder;
use crate::iter::{Automaton, StateClass};
use std::ops::{BitAnd, BitOr};

/// Largest `max_dist` the inline row array holds ([`MAX_ROWS`]` - 1`).
/// Larger distances take the DP-row automaton instead.
pub const MAX_NFA_DIST: u32 = 4;

/// Rows carried in the state: one per edit budget `0..=MAX_NFA_DIST`.
const MAX_ROWS: usize = MAX_NFA_DIST as usize + 1;

/// One NFA row: a bitmask over needle positions `0..=needle_len`.
///
/// The `u64` impl serves needles up to 63 folded codepoints, `u128` up
/// to 127. Mirrors the wildcard NFA's
/// [`NfaBitSet`](crate::iter::NfaBitSet) split so the same automaton code
/// monomorphises against both widths.
pub trait LevRow:
    Copy + Eq + BitAnd<Output = Self> + BitOr<Output = Self> + std::fmt::Debug
{
    /// Number of positions a row can hold; a needle of `m` codepoints
    /// needs `m + 1`.
    const CAPACITY: usize;

    /// Row with no positions set.
    fn zero() -> Self;

    /// Row with positions `0..n` set.
    fn low_bits(n: usize) -> Self;

    /// The row shifted one position up (position `j` moves to `j + 1`).
    fn shl1(self) -> Self;

    /// Whether position `pos` is set.
    fn bit(self, pos: usize) -> bool;

    /// Set position `pos`.
    fn set_bit(&mut self, pos: usize);

    /// Whether no position is set.
    fn is_zero(self) -> bool;
}

impl LevRow for u64 {
    const CAPACITY: usize = 64;

    #[inline]
    fn zero() -> Self {
        0
    }
    #[inline]
    fn low_bits(n: usize) -> Self {
        debug_assert!(n <= 64);
        if n == 64 { !0 } else { (1u64 << n) - 1 }
    }
    #[inline]
    fn shl1(self) -> Self {
        self << 1
    }
    #[inline]
    fn bit(self, pos: usize) -> bool {
        debug_assert!(pos < 64);
        (self >> pos) & 1 == 1
    }
    #[inline]
    fn set_bit(&mut self, pos: usize) {
        debug_assert!(pos < 64);
        *self |= 1u64 << pos;
    }
    #[inline]
    fn is_zero(self) -> bool {
        self == 0
    }
}

impl LevRow for u128 {
    const CAPACITY: usize = 128;

    #[inline]
    fn zero() -> Self {
        0
    }
    #[inline]
    fn low_bits(n: usize) -> Self {
        debug_assert!(n <= 128);
        if n == 128 { !0 } else { (1u128 << n) - 1 }
    }
    #[inline]
    fn shl1(self) -> Self {
        self << 1
    }
    #[inline]
    fn bit(self, pos: usize) -> bool {
        debug_assert!(pos < 128);
        (self >> pos) & 1 == 1
    }
    #[inline]
    fn set_bit(&mut self, pos: usize) {
        debug_assert!(pos < 128);
        *self |= 1u128 << pos;
    }
    #[inline]
    fn is_zero(self) -> bool {
        self == 0
    }
}

/// Streaming bit-parallel automaton accepting keys within a Levenshtein
/// distance of a needle, compared case-insensitively. Accepts the same
/// keys as [`CaseFoldLevenshtein`](super::CaseFoldLevenshtein); see the
/// [module docs](self) for the state encoding.
pub struct CaseFoldLevenshteinNfa<S: LevRow> {
    /// Folded needle length `m`; rows span positions `0..=m`.
    needle_len: usize,
    /// Maximum edit distance accepted; rows `0..=max_dist` are live.
    max_dist: usize,
    /// Mask of the valid positions `0..=m`. Shift/union terms can spill
    /// past position `m`; this clips them so garbage bits can't keep a
    /// dead state alive.
    valid: S,
    /// Per-codepoint match masks for the needle's distinct folded
    /// codepoints, sorted for binary search. Bit `j + 1` is set iff
    /// `needle[j]` equals the codepoint; absent codepoints match nowhere.
    masks: Box<[(char, S)]>,
}

impl<S: LevRow> CaseFoldLevenshteinNfa<S> {
    /// Build an automaton matching keys within `max_dist` edits of
    /// `needle`, or `None` when the needle or distance exceeds what this
    /// word width carries (`needle_len + 1 > CAPACITY` or
    /// `max_dist > MAX_NFA_DIST`) — the caller falls back to the DP-row
    /// automaton.
    pub fn new(needle: &str, max_dist: u32) -> Option<Self> {
        if max_dist > MAX_NFA_DIST {
            return None;
        }
        let folded: Vec<char> = needle.chars().flat_map(char::to_lowercase).collect();
        if folded.len() + 1 > S::CAPACITY {
            return None;
        }
        let mut masks: Vec<(char, S)> = Vec::new();
        for (j, &c) in folded.iter().enumerate() {
            match masks.binary_search_by_key(&c, |&(mc, _)| mc) {
                Ok(idx) => masks[idx].1.set_bit(j + 1),
                Err(idx) => {
                    let mut m = S::zero();
                    m.set_bit(j + 1);
                    masks.insert(idx, (c, m));
                }
            }
        }
        Some(Self {
            needle_len: folded.len(),
            max_dist: max_dist as usize,
            valid: S::low_bits(folded.len() + 1),
            masks: masks.into_boxed_slice(),
        })
    }

    /// Match mask for a folded key codepoint.
    #[inline]
    fn mask(&self, c: char) -> S {
        match self.masks.binary_search_by_key(&c, |&(mc, _)| mc) {
            Ok(idx) => self.masks[idx].1,
            Err(_) => S::zero(),
        }
    }

    /// Advance every row by one folded key codepoint, in place. Returns
    /// `false` when every row is zero, i.e. the state is dead.
    #[inline]
    fn advance_rows(&self, rows: &mut [S; MAX_ROWS], c: char) -> bool {
        let b = self.mask(c);
        let mut prev_old = rows[0];
        let mut prev_new = prev_old.shl1() & b;
        rows[0] = prev_new;
        let mut alive = !prev_new.is_zero();
        for row in rows.iter_mut().take(self.max_dist + 1).skip(1) {
            let old = *row;
            let new =
                ((old.shl1() & b) | prev_old | prev_old.shl1() | prev_new.shl1()) & self.valid;
            *row = new;
            alive |= !new.is_zero();
            prev_old = old;
            prev_new = new;
        }
        alive
    }

    /// Consume one key byte into `state` in place. Returns `false` when
    /// the state died: the byte is invalid UTF-8 or every row is zero.
    fn step_in_place(&self, state: &mut LevenshteinNfaState<S>, byte: u8) -> bool {
        match state.partial.push(byte) {
            Err(_) => false,
            Ok(None) => true,
            // A codepoint that folds to several (e.g. 'İ' → "i\u{307}")
            // costs one advance per folded codepoint, mirroring edit
            // distance over the fully folded key.
            Ok(Some(c)) => c
                .to_lowercase()
                .all(|folded| self.advance_rows(&mut state.rows, folded)),
        }
    }
}

/// The active-position rows (one per edit budget), plus the decoder state
/// of a codepoint the driver has only partially delivered (an edge label
/// may end mid-codepoint). Fully inline: cloning is a small `memcpy`.
#[derive(Clone)]
pub struct LevenshteinNfaState<S> {
    rows: [S; MAX_ROWS],
    partial: CodepointDecoder,
}

impl<S: LevRow> Automaton for CaseFoldLevenshteinNfa<S> {
    type State = LevenshteinNfaState<S>;

    fn start(&self) -> Self::State {
        // Before any key codepoint, the first `j` needle codepoints can
        // be aligned with `j` deletions: row `i` holds positions `0..=i`.
        let mut rows = [S::zero(); MAX_ROWS];
        for (i, row) in rows.iter_mut().take(self.max_dist + 1).enumerate() {
            *row = S::low_bits((i + 1).min(self.needle_len + 1));
        }
        LevenshteinNfaState {
            rows,
            partial: CodepointDecoder::new(),
        }
    }

    fn step(&mut self, state: &Self::State, byte: u8) -> Option<Self::State> {
        let mut state = state.clone();
        self.step_in_place(&mut state, byte).then_some(state)
    }

    fn classify(&self, state: &Self::State) -> StateClass {
        // Bit `needle_len` in any row: the full needle aligns against the
        // full folded key within budget. Descendants may still match —
        // appending a codepoint can complete an alignment — so accepting
        // states stay live.
        let accepting = state.partial.at_boundary()
            && state.rows[..=self.max_dist]
                .iter()
                .any(|row| row.bit(self.needle_len));
        if accepting {
            StateClass::LiveAccepting
        } else {
            StateClass::Live
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::CaseFoldLevenshtein;
    use super::*;

    fn accepts<S: LevRow>(needle: &str, max_dist: u32, key: &str) -> bool {
        let mut automaton =
            CaseFoldLevenshteinNfa::<S>::new(needle, max_dist).expect("within NFA bounds");
        let mut state = automaton.start();
        for &b in key.as_bytes() {
            match automaton.step(&state, b) {
                Some(next) => state = next,
                None => return false,
            }
        }
        automaton.classify(&state).is_accepting()
    }

    fn dp_accepts(needle: &str, max_dist: u32, key: &str) -> bool {
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
        assert!(accepts::<u64>("hello", 0, "HELLO"));
        assert!(!accepts::<u64>("hello", 0, "hellp"));
    }

    #[test]
    fn single_edits_match_at_distance_one() {
        assert!(accepts::<u64>("hello", 1, "hellp")); // substitution
        assert!(accepts::<u64>("hello", 1, "hell")); // deletion
        assert!(accepts::<u64>("hello", 1, "helloo")); // insertion
        assert!(accepts::<u64>("hello", 1, "HELL")); // edit + case fold
        assert!(!accepts::<u64>("hello", 1, "help"));
    }

    #[test]
    fn edits_accumulate() {
        assert!(accepts::<u64>("hello", 2, "help"));
        assert!(!accepts::<u64>("hello", 3, "hp"));
        assert!(accepts::<u64>("hello", 4, "hp"));
    }

    #[test]
    fn dead_subtrees_are_pruned_mid_key() {
        let mut automaton =
            CaseFoldLevenshteinNfa::<u64>::new("hello", 1).expect("within NFA bounds");
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
        assert!(accepts::<u64>("café", 1, "cafe"));
        assert!(accepts::<u64>("café", 1, "CAFÉ"));
        assert!(!accepts::<u64>("café", 0, "cafe"));
    }

    #[test]
    fn empty_needle_matches_keys_up_to_the_budget() {
        assert!(accepts::<u64>("", 0, ""));
        assert!(!accepts::<u64>("", 0, "a"));
        assert!(accepts::<u64>("", 2, "ab"));
        assert!(!accepts::<u64>("", 2, "abc"));
    }

    #[test]
    fn invalid_utf8_key_dies() {
        let mut automaton =
            CaseFoldLevenshteinNfa::<u64>::new("hello", 3).expect("within NFA bounds");
        let state = automaton.start();
        assert!(automaton.step(&state, 0x80).is_none());
    }

    #[test]
    fn rejects_out_of_bounds_construction() {
        let long = "a".repeat(64); // 65 positions > u64 capacity
        assert!(CaseFoldLevenshteinNfa::<u64>::new(&long, 1).is_none());
        assert!(CaseFoldLevenshteinNfa::<u128>::new(&long, 1).is_some());
        let very_long = "a".repeat(128);
        assert!(CaseFoldLevenshteinNfa::<u128>::new(&very_long, 1).is_none());
        assert!(CaseFoldLevenshteinNfa::<u64>::new("abc", MAX_NFA_DIST + 1).is_none());
    }

    #[test]
    fn u128_width_agrees_with_u64() {
        for key in ["hello", "hell", "helloo", "help", "xyzzy"] {
            assert_eq!(
                accepts::<u64>("hello", 1, key),
                accepts::<u128>("hello", 1, key)
            );
        }
    }

    #[test]
    fn step_all_carries_partial_codepoints_across_labels() {
        // Split 'é' (C3 A9) between two step_all calls, as two trie edge
        // labels with the split inside the codepoint would.
        let mut automaton =
            CaseFoldLevenshteinNfa::<u64>::new("cliché", 0).expect("within NFA bounds");
        let start = automaton.start();
        let mid = automaton.step_all(&start, b"clich\xC3").unwrap();
        assert_eq!(automaton.classify(&mid), StateClass::Live);
        let done = automaton.step_all(&mid, b"\xA9").unwrap();
        assert!(automaton.classify(&done).is_accepting());
    }

    /// Exhaustive agreement with the DP-row automaton: every key over a
    /// 3-letter alphabet up to length 6, against several needles and every
    /// in-bounds distance. The alphabet includes a multi-byte codepoint so
    /// codepoint (not byte) semantics are exercised throughout.
    #[test]
    #[cfg_attr(miri, ignore = "exhaustive ~27k-case sweep is too slow under miri")]
    fn agrees_with_dp_row_exhaustively() {
        const ALPHABET: [char; 3] = ['a', 'B', 'é'];
        let needles = ["", "ab", "aBé", "ébba", "aaaa"];

        let mut keys = vec![String::new()];
        let mut frontier = vec![String::new()];
        for _ in 0..6 {
            let mut next = Vec::new();
            for k in &frontier {
                for c in ALPHABET {
                    let mut k = k.clone();
                    k.push(c);
                    next.push(k);
                }
            }
            keys.extend(next.iter().cloned());
            frontier = next;
        }

        for needle in needles {
            for dist in 0..=MAX_NFA_DIST {
                for key in &keys {
                    assert_eq!(
                        accepts::<u64>(needle, dist, key),
                        dp_accepts(needle, dist, key),
                        "needle={needle:?} dist={dist} key={key:?}"
                    );
                }
            }
        }
    }
}
