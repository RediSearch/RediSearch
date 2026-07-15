/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Codepoint-semantics wildcard matching.
//!
//! `?` matches exactly one codepoint (`entr?` matches `entré`), `*` matches
//! any run of codepoints, and literals compare codepoint-for-codepoint. This
//! is the UTF-8 lifting of the byte-level wildcard NFA in
//! [`wildcard`](crate::trie_map::iter::automaton::wildcard), whose `?`
//! consumes a single byte. Matching is case-sensitive; case-insensitive
//! callers fold both sides before matching.
//!
//! [`CodepointWildcard`] is the parsed pattern. It matches flat candidates
//! directly via [`CodepointWildcard::matches`], or drives a trie traversal as
//! the [`CodepointWildcardNfa`] automaton, which reassembles codepoints from
//! the byte stream with a [`CodepointDecoder`] (trie edges may split a
//! codepoint) and advances an NFA position set per *codepoint*. Keys that are
//! not valid UTF-8 never match: an invalid byte kills the state, pruning that
//! subtree.
//!
//! Pattern syntax — escapes (`\*`, `\?`, `\\`) and the `**`/`*?`
//! normalizations — is inherited from [`rqe_wildcard`]'s tokenizer; only the
//! *matching* granularity differs from the byte NFA.

use super::utf8::CodepointDecoder;
use crate::iter::{Automaton, NfaBitSet, StateClass};
use rqe_wildcard::{Token, WildcardPattern};

/// One pattern position, over the codepoint alphabet. The counterpart of the
/// byte NFA's `Atom`, with `Byte(u8)` lifted to `Char(char)`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum CpAtom {
    /// Literal codepoint; the input codepoint must match exactly to advance.
    Char(char),
    /// `?` — matches any single codepoint.
    One,
    /// `*` — matches zero or more codepoints; can self-loop or be skipped.
    Any,
}

/// A parsed codepoint-semantics wildcard pattern. See the [module
/// docs](self) for the matching model.
pub struct CodepointWildcard {
    atoms: Box<[CpAtom]>,
    /// The pattern's leading run of literal codepoints, re-encoded as UTF-8
    /// (escapes resolved). Every match starts with it, so the trie traversal
    /// can jump straight to the matching subtree.
    literal_prefix: Box<str>,
}

impl CodepointWildcard {
    /// Parse `pattern`. Tokenization (escape handling, `**`/`*?`
    /// normalization) is delegated to [`WildcardPattern::parse`]; literal
    /// byte runs are then decoded into codepoint atoms.
    pub fn parse(pattern: &str) -> Self {
        let mut atoms = Vec::new();
        // The tokenizer splits literals at escape points to stay zero-copy,
        // so an escaped multi-byte codepoint can span two adjacent `Literal`
        // tokens. Coalesce adjacent literal runs before decoding: dropping
        // the ASCII `\` from valid UTF-8 leaves valid UTF-8.
        let mut literal_run = Vec::new();
        let flush = |literal_run: &mut Vec<u8>, atoms: &mut Vec<CpAtom>| {
            let run = std::str::from_utf8(literal_run)
                .expect("literal run of a &str pattern with escapes resolved is valid UTF-8");
            atoms.extend(run.chars().map(CpAtom::Char));
            literal_run.clear();
        };
        for token in WildcardPattern::parse(pattern.as_bytes()).tokens() {
            match token {
                Token::Literal(bytes) => literal_run.extend_from_slice(bytes),
                Token::One => {
                    flush(&mut literal_run, &mut atoms);
                    atoms.push(CpAtom::One);
                }
                Token::Any => {
                    flush(&mut literal_run, &mut atoms);
                    atoms.push(CpAtom::Any);
                }
            }
        }
        flush(&mut literal_run, &mut atoms);

        let literal_prefix = atoms
            .iter()
            .map_while(|a| match a {
                CpAtom::Char(c) => Some(*c),
                CpAtom::One | CpAtom::Any => None,
            })
            .collect::<String>()
            .into_boxed_str();

        Self {
            atoms: atoms.into_boxed_slice(),
            literal_prefix,
        }
    }

    /// The number of pattern positions, counted in codepoints (a multi-byte
    /// literal codepoint is one atom). The NFA needs capacity for
    /// `atom_count() + 1` positions.
    pub fn atom_count(&self) -> usize {
        self.atoms.len()
    }

    /// The pattern's leading run of literal codepoints (escapes resolved),
    /// empty if the pattern starts with `?` or `*`. Every match starts with
    /// it.
    pub fn literal_prefix(&self) -> &str {
        &self.literal_prefix
    }

    /// Match a single candidate string against the pattern.
    ///
    /// Greedy two-pointer scan with backtracking to the most recent `*` —
    /// no state-set bookkeeping, no size limit on the pattern. Use this for
    /// flat candidate lists; use the [`CodepointWildcardNfa`] automaton when
    /// traversing a trie, where prefix sharing pays for the NFA.
    pub fn matches(&self, term: &str) -> bool {
        let mut atom_idx = 0;
        let mut rest = term.chars();
        // Most recent `*`: the atom index after it, and the input tail at the
        // point it was reached. On a dead end, resume there with the `*`
        // consuming one more codepoint.
        let mut backtrack: Option<(usize, std::str::Chars<'_>)> = None;

        loop {
            match self.atoms.get(atom_idx) {
                Some(CpAtom::Any) => {
                    // Try skipping the `*` first (it matches zero codepoints);
                    // remember where to resume if that turns out too greedy.
                    atom_idx += 1;
                    backtrack = Some((atom_idx, rest.clone()));
                    continue;
                }
                Some(&CpAtom::Char(expected)) => {
                    if rest.next() == Some(expected) {
                        atom_idx += 1;
                        continue;
                    }
                }
                Some(CpAtom::One) => {
                    if rest.next().is_some() {
                        atom_idx += 1;
                        continue;
                    }
                }
                None => {
                    if rest.clone().next().is_none() {
                        return true;
                    }
                }
            }
            // Dead end: give the most recent `*` one more codepoint and retry.
            let Some((bt_atom, bt_rest)) = &mut backtrack else {
                return false;
            };
            if bt_rest.next().is_none() {
                return false;
            }
            atom_idx = *bt_atom;
            rest = bt_rest.clone();
        }
    }
}

/// [`CodepointWildcard`] as a streaming [`Automaton`] over a trie, backed by
/// an [`NfaBitSet`] of pattern positions. See the [module docs](self) for
/// the matching model and the [byte NFA's module
/// doc](crate::trie_map::iter::automaton::wildcard) for the position/ε-closure
/// primer — the encoding is the same, one transition per *codepoint* instead
/// of per byte.
///
/// Callers must match the bitset width to the pattern:
/// `pattern.atom_count() + 1` positions have to fit in `S`.
pub struct CodepointWildcardNfa<S: NfaBitSet> {
    pattern: CodepointWildcard,
    /// The accept position — `atoms.len()`. A state set containing this
    /// position means the whole pattern has matched.
    accept: usize,
    /// ε-closure of `{0}`. Cloned at the top of every traversal.
    start_positions: S,
    /// Pre-built `{accept}` singleton — the unique terminal state for
    /// patterns with no `*`.
    accept_only: S,
}

impl<S: NfaBitSet> CodepointWildcardNfa<S> {
    /// Wrap a parsed pattern as a trie automaton backed by `S`.
    ///
    /// # Panics
    ///
    /// Panics if `pattern.atom_count() >= S::CAPACITY` — the accept position
    /// lives at index `atom_count`, so the bitset must hold `atom_count + 1`
    /// distinct positions.
    pub fn compile(pattern: CodepointWildcard) -> Self {
        assert!(
            pattern.atom_count() < S::CAPACITY,
            "CodepointWildcardNfa backend has capacity {} but pattern needs {} atoms",
            S::CAPACITY,
            pattern.atom_count(),
        );
        let accept = pattern.atom_count();
        let mut start_positions = S::empty(accept);
        insert_closure(&mut start_positions, &pattern.atoms, 0);
        let accept_only = S::singleton(accept, accept);
        Self {
            pattern,
            accept,
            start_positions,
            accept_only,
        }
    }
}

/// Union the ε-closure of position `pos` into `set`: `pos` itself, plus —
/// for every `*` reached — the position after it (a `*` can be skipped).
fn insert_closure<S: NfaBitSet>(set: &mut S, atoms: &[CpAtom], mut pos: usize) {
    set.insert(pos);
    while let Some(CpAtom::Any) = atoms.get(pos) {
        pos += 1;
        set.insert(pos);
    }
}

/// Active pattern positions, plus the decoder state of a codepoint the
/// driver has only partially delivered (an edge label may end mid-codepoint).
#[derive(Clone)]
pub struct CodepointWildcardState<S> {
    positions: S,
    partial: CodepointDecoder,
}

impl<S: NfaBitSet> Automaton for CodepointWildcardNfa<S> {
    type State = CodepointWildcardState<S>;

    fn start(&self) -> Self::State {
        CodepointWildcardState {
            positions: self.start_positions.clone(),
            partial: CodepointDecoder::new(),
        }
    }

    fn step(&mut self, state: &Self::State, byte: u8) -> Option<Self::State> {
        let mut state = state.clone();
        let Some(c) = state.partial.push(byte).ok()? else {
            return Some(state);
        };
        let atoms = &self.pattern.atoms;
        let mut next = S::empty(self.accept);
        for pos in state.positions.iter() {
            let advances = match atoms.get(pos) {
                Some(&CpAtom::Char(expected)) => expected == c,
                Some(CpAtom::One) => true,
                // `*` self-loops; its ε-closure re-adds the skip targets.
                Some(CpAtom::Any) => {
                    insert_closure(&mut next, atoms, pos);
                    false
                }
                // The accept position has no outgoing transition.
                None => false,
            };
            if advances {
                insert_closure(&mut next, atoms, pos + 1);
            }
        }
        if next.is_empty() {
            return None;
        }
        state.positions = next;
        Some(state)
    }

    fn classify(&self, state: &Self::State) -> StateClass {
        if !state.partial.at_boundary() {
            return StateClass::Live;
        }
        // `{accept}` and only `{accept}` has no outgoing transitions — the
        // unique terminal state for fixed-length patterns.
        //
        // A trailing `*` is deliberately *not* reported as
        // `StateClass::Permanent`: permanent mode stops stepping descendants
        // through the automaton, which would surrender the "keys that are
        // not valid UTF-8 never match" guarantee.
        if state.positions == self.accept_only {
            StateClass::Terminal
        } else if state.positions.contains(self.accept) {
            StateClass::LiveAccepting
        } else {
            StateClass::Live
        }
    }

    fn literal_prefix(&self) -> Option<&[u8]> {
        (!self.pattern.literal_prefix.is_empty()).then_some(self.pattern.literal_prefix.as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn greedy(pattern: &str, term: &str) -> bool {
        CodepointWildcard::parse(pattern).matches(term)
    }

    /// Drive the NFA byte-by-byte, as the trie driver would.
    fn nfa(pattern: &str, term: &str) -> bool {
        let mut automaton =
            CodepointWildcardNfa::<u128>::compile(CodepointWildcard::parse(pattern));
        let mut state = automaton.start();
        for &b in term.as_bytes() {
            match automaton.step(&state, b) {
                Some(next) => state = next,
                None => return false,
            }
        }
        automaton.classify(&state).is_accepting()
    }

    #[rustfmt::skip]
    const CASES: &[(&str, &str, bool)] = &[
        // ? consumes one codepoint, not one byte.
        ("entr?", "entré", true),
        ("entr?", "entre", true),
        ("entr?", "entr", false),
        ("entr??", "entré", false),
        ("?", "é", true),
        ("??", "é", false),
        // Literals compare codepoints, case-sensitively.
        ("café", "café", true),
        ("café", "CAFÉ", false),
        ("clich?", "cliché", true),
        // * matches any run of codepoints, including none.
        ("*", "", true),
        ("*", "日本語", true),
        ("日*", "日本語", true),
        ("*語", "日本語", true),
        ("日*語", "日本語", true),
        ("日*語", "日本", false),
        ("*ab*", "xaab", true),
        ("*ab*", "xba", false),
        // Backtracking: the first * must not swallow the b needed later.
        ("*b?", "abc", true),
        ("a*b*c", "aXbYbZc", true),
        // Empty pattern matches only the empty term.
        ("", "", true),
        ("", "a", false),
        // Escapes force literals.
        (r"\*", "*", true),
        (r"\*", "a", false),
        (r"\?", "?", true),
        (r"\?", "a", false),
    ];

    #[test]
    fn greedy_matches_expected() {
        for &(pattern, term, expected) in CASES {
            assert_eq!(
                greedy(pattern, term),
                expected,
                "greedy: {pattern:?} vs {term:?}"
            );
        }
    }

    #[test]
    fn nfa_agrees_with_greedy() {
        for &(pattern, term, expected) in CASES {
            assert_eq!(nfa(pattern, term), expected, "nfa: {pattern:?} vs {term:?}");
        }
    }

    #[test]
    fn escaped_multibyte_codepoint_is_one_atom() {
        // The tokenizer splits an escaped multi-byte codepoint across two
        // Literal tokens; parsing must reassemble it into a single Char atom.
        let pattern = CodepointWildcard::parse("\\é?");
        assert_eq!(pattern.atom_count(), 2);
        assert!(pattern.matches("éx"));
        assert!(!pattern.matches("é"));
    }

    #[test]
    fn literal_prefix_covers_leading_literals_only() {
        let parsed = CodepointWildcard::parse("café*x");
        let nfa = CodepointWildcardNfa::<u64>::compile(parsed);
        assert_eq!(nfa.literal_prefix(), Some("café".as_bytes()));

        let starts_wild = CodepointWildcardNfa::<u64>::compile(CodepointWildcard::parse("*café"));
        assert_eq!(starts_wild.literal_prefix(), None);
    }

    #[test]
    fn invalid_utf8_key_dies() {
        let mut automaton = CodepointWildcardNfa::<u64>::compile(CodepointWildcard::parse("*"));
        let state = automaton.start();
        assert!(automaton.step(&state, 0x80).is_none());
    }

    #[test]
    fn state_survives_split_codepoints() {
        // Feed 'é' (C3 A9) one byte at a time, as a trie with an edge split
        // inside the codepoint would.
        let mut automaton = CodepointWildcardNfa::<u64>::compile(CodepointWildcard::parse("?"));
        let state = automaton.start();
        let mid = automaton.step(&state, 0xC3).unwrap();
        assert_eq!(automaton.classify(&mid), StateClass::Live);
        let done = automaton.step(&mid, 0xA9).unwrap();
        assert!(automaton.classify(&done).is_accepting());
    }

    #[test]
    fn fixed_length_pattern_terminates() {
        let mut automaton = CodepointWildcardNfa::<u64>::compile(CodepointWildcard::parse("ab"));
        let mut state = automaton.start();
        for &b in b"ab" {
            state = automaton.step(&state, b).unwrap();
        }
        assert_eq!(automaton.classify(&state), StateClass::Terminal);
    }
}
