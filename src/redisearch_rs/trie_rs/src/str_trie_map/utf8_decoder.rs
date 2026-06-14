/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Pure-value UTF-8 decoder FSM, byte-in / state-out.
//!
//! Exists to drive the byte-keyed Levenshtein DFA in [`super::dfa`]: that
//! DFA needs to know, for each consumed trie byte, (a) whether a codepoint
//! just completed, and (b) whether the current position sits on a codepoint
//! boundary (so the DFA can refuse to accept mid-codepoint).
//!
//! The state is [`Copy`] + [`Eq`] + [`Hash`] so it can be used as part of the
//! joint cache key during DFA subset construction without allocation or
//! interior mutability.
//!
//! Byte-class ranges follow the Unicode standard, §3.9 Table 3-7
//! (Well-Formed UTF-8 Byte Sequences). That table has been stable since
//! Unicode 3.1 capped UTF-8 at U+10FFFF, so no version pin is intended.
//! Overlong encodings, lone continuations, surrogate halves, and
//! codepoints above U+10FFFF are all rejected at the byte they become
//! unambiguously invalid.

/// Decoder state. `Between` means "ready for a fresh lead byte"; all other
/// variants hold the partial codepoint accumulator and a constraint on the
/// next byte's allowed range.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DecoderState {
    /// On a codepoint boundary. Next byte must be a lead byte.
    Between,
    /// 2-byte sequence: one continuation byte remaining.
    Cont1Of2 { acc: u32 },
    /// 3-byte sequence: two continuations remaining. The very next byte is
    /// constrained by `next_range` to enforce no-overlong / no-surrogate.
    Cont2Of3 { acc: u32, next_range: ContRange },
    /// 3-byte sequence: one continuation remaining.
    Cont1Of3 { acc: u32 },
    /// 4-byte sequence: three continuations remaining. `next_range`
    /// enforces no-overlong / no-above-U+10FFFF on the second byte.
    Cont3Of4 { acc: u32, next_range: ContRange },
    /// 4-byte sequence: two continuations remaining.
    Cont2Of4 { acc: u32 },
    /// 4-byte sequence: one continuation remaining.
    Cont1Of4 { acc: u32 },
}

/// Constraint on a UTF-8 continuation byte. `Standard` is the usual
/// `0x80..=0xBF` range; the others enforce the lead-byte-specific narrowing
/// from §3.9 Table 3-7 of the Unicode standard.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ContRange {
    /// `0x80..=0xBF` — generic continuation.
    Standard,
    /// `0xA0..=0xBF` — second byte after lead `E0` (excludes overlong
    /// encodings below U+0800).
    HighA0,
    /// `0x80..=0x9F` — second byte after lead `ED` (excludes surrogate
    /// halves U+D800..U+DFFF).
    Low9F,
    /// `0x90..=0xBF` — second byte after lead `F0` (excludes overlong
    /// encodings below U+10000).
    High90,
    /// `0x80..=0x8F` — second byte after lead `F4` (excludes codepoints
    /// above U+10FFFF).
    Low8F,
}

impl ContRange {
    const fn contains(self, b: u8) -> bool {
        match self {
            Self::Standard => 0x80 <= b && b <= 0xBF,
            Self::HighA0 => 0xA0 <= b && b <= 0xBF,
            Self::Low9F => 0x80 <= b && b <= 0x9F,
            Self::High90 => 0x90 <= b && b <= 0xBF,
            Self::Low8F => 0x80 <= b && b <= 0x8F,
        }
    }
}

/// Outcome of feeding one byte to the decoder.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Step {
    /// Byte accepted; codepoint still in progress. State has advanced.
    Pending,
    /// Byte accepted and completed a codepoint. State is back to `Between`.
    Complete(char),
    /// Byte rejected. The caller's input is not valid UTF-8 at this point.
    Invalid,
}

impl DecoderState {
    /// True iff a fresh lead byte is expected — i.e. no codepoint is in
    /// flight. The byte DFA accepts only at positions where this holds.
    pub const fn is_between(self) -> bool {
        matches!(self, Self::Between)
    }
}

/// Feed one byte to the decoder. Returns the next state plus a [`Step`]
/// describing what the byte produced.
///
/// On `Step::Invalid` the returned state is `Between`: the caller may treat
/// the next byte as a fresh lead byte attempt. Whether to do so is up to
/// the caller — the byte DFA treats invalid input as a dead branch.
pub const fn step(state: DecoderState, b: u8) -> (DecoderState, Step) {
    match state {
        DecoderState::Between => step_lead(b),
        DecoderState::Cont1Of2 { acc } => {
            if ContRange::Standard.contains(b) {
                let cp = (acc << 6) | (b & 0x3F) as u32;
                match char::from_u32(cp) {
                    Some(c) => (DecoderState::Between, Step::Complete(c)),
                    None => (DecoderState::Between, Step::Invalid),
                }
            } else {
                (DecoderState::Between, Step::Invalid)
            }
        }
        DecoderState::Cont2Of3 { acc, next_range } => {
            if next_range.contains(b) {
                let new_acc = (acc << 6) | (b & 0x3F) as u32;
                (DecoderState::Cont1Of3 { acc: new_acc }, Step::Pending)
            } else {
                (DecoderState::Between, Step::Invalid)
            }
        }
        DecoderState::Cont1Of3 { acc } => {
            if ContRange::Standard.contains(b) {
                let cp = (acc << 6) | (b & 0x3F) as u32;
                match char::from_u32(cp) {
                    Some(c) => (DecoderState::Between, Step::Complete(c)),
                    None => (DecoderState::Between, Step::Invalid),
                }
            } else {
                (DecoderState::Between, Step::Invalid)
            }
        }
        DecoderState::Cont3Of4 { acc, next_range } => {
            if next_range.contains(b) {
                let new_acc = (acc << 6) | (b & 0x3F) as u32;
                (DecoderState::Cont2Of4 { acc: new_acc }, Step::Pending)
            } else {
                (DecoderState::Between, Step::Invalid)
            }
        }
        DecoderState::Cont2Of4 { acc } => {
            if ContRange::Standard.contains(b) {
                let new_acc = (acc << 6) | (b & 0x3F) as u32;
                (DecoderState::Cont1Of4 { acc: new_acc }, Step::Pending)
            } else {
                (DecoderState::Between, Step::Invalid)
            }
        }
        DecoderState::Cont1Of4 { acc } => {
            if ContRange::Standard.contains(b) {
                let cp = (acc << 6) | (b & 0x3F) as u32;
                match char::from_u32(cp) {
                    Some(c) => (DecoderState::Between, Step::Complete(c)),
                    None => (DecoderState::Between, Step::Invalid),
                }
            } else {
                (DecoderState::Between, Step::Invalid)
            }
        }
    }
}

const fn step_lead(b: u8) -> (DecoderState, Step) {
    match b {
        0x00..=0x7F => (DecoderState::Between, Step::Complete(b as char)),
        0xC2..=0xDF => (
            DecoderState::Cont1Of2 {
                acc: (b & 0x1F) as u32,
            },
            Step::Pending,
        ),
        0xE0 => (
            DecoderState::Cont2Of3 {
                acc: 0,
                next_range: ContRange::HighA0,
            },
            Step::Pending,
        ),
        0xE1..=0xEC | 0xEE..=0xEF => (
            DecoderState::Cont2Of3 {
                acc: (b & 0x0F) as u32,
                next_range: ContRange::Standard,
            },
            Step::Pending,
        ),
        0xED => (
            DecoderState::Cont2Of3 {
                acc: (b & 0x0F) as u32,
                next_range: ContRange::Low9F,
            },
            Step::Pending,
        ),
        0xF0 => (
            DecoderState::Cont3Of4 {
                acc: 0,
                next_range: ContRange::High90,
            },
            Step::Pending,
        ),
        0xF1..=0xF3 => (
            DecoderState::Cont3Of4 {
                acc: (b & 0x07) as u32,
                next_range: ContRange::Standard,
            },
            Step::Pending,
        ),
        0xF4 => (
            DecoderState::Cont3Of4 {
                acc: (b & 0x07) as u32,
                next_range: ContRange::Low8F,
            },
            Step::Pending,
        ),
        // 0x80..=0xBF (lone continuation), 0xC0..=0xC1 (overlong leads),
        // 0xF5..=0xFF (above-U+10FFFF leads), and the rest of the invalid
        // lead bytes all fall here.
        _ => (DecoderState::Between, Step::Invalid),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Feed every byte of `bytes` through the decoder starting at `Between`.
    /// Collect completed codepoints; return the final state and the
    /// sequence of `Step`s observed.
    fn run(bytes: &[u8]) -> (DecoderState, Vec<Step>) {
        let mut state = DecoderState::Between;
        let mut steps = Vec::with_capacity(bytes.len());
        for &b in bytes {
            let (next, s) = step(state, b);
            state = next;
            steps.push(s);
        }
        (state, steps)
    }

    fn completed(steps: &[Step]) -> Vec<char> {
        steps
            .iter()
            .filter_map(|s| match s {
                Step::Complete(c) => Some(*c),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn ascii_each_byte_completes() {
        for b in 0u8..=0x7F {
            let (state, steps) = run(&[b]);
            assert_eq!(state, DecoderState::Between);
            assert_eq!(steps, vec![Step::Complete(b as char)]);
        }
    }

    #[test]
    fn roundtrip_against_str_chars() {
        // Mix of 1, 2, 3, 4 byte codepoints including BMP, supplementary
        // planes, and edge cases near range boundaries.
        let inputs = [
            "",
            "hello",
            "café",
            "naïve résumé",
            "中文",
            "日本語",
            "Ωμέγα",
            "𝄞𝄢",         // U+1D11E G clef + U+1D122 F clef (4-byte)
            "🦀🚀🔥",       // emoji
            "a\u{0800}b",  // smallest 3-byte after ASCII flip-flop
            "a\u{D7FF}b",  // just below surrogate range
            "a\u{E000}b",  // just above surrogate range
            "a\u{FFFF}b",  // BMP max
            "a\u{10000}b", // smallest supplementary
            "a\u{10FFFF}b", // largest codepoint
        ];
        for s in inputs {
            let (state, steps) = run(s.as_bytes());
            assert_eq!(state, DecoderState::Between, "ended mid-codepoint: {s:?}");
            let got: String = completed(&steps).into_iter().collect();
            assert_eq!(got, s, "roundtrip mismatch for {s:?}");
        }
    }

    #[test]
    fn pending_count_matches_continuation_bytes() {
        // For each codepoint c, feeding UTF-8(c) yields N-1 Pending then one
        // Complete(c), where N = c.len_utf8().
        for c in ['a', 'é', '中', '𝄞', '\u{10FFFF}'] {
            let mut buf = [0u8; 4];
            let bytes = c.encode_utf8(&mut buf).as_bytes();
            let (state, steps) = run(bytes);
            assert_eq!(state, DecoderState::Between);
            assert_eq!(steps.len(), c.len_utf8());
            let (last, rest) = steps.split_last().unwrap();
            assert!(rest.iter().all(|s| matches!(s, Step::Pending)), "for {c:?}");
            assert_eq!(*last, Step::Complete(c));
        }
    }

    #[test]
    fn lone_continuation_is_invalid_at_first_byte() {
        for b in 0x80u8..=0xBF {
            let (state, steps) = run(&[b]);
            assert_eq!(state, DecoderState::Between);
            assert_eq!(steps, vec![Step::Invalid], "byte 0x{b:02X}");
        }
    }

    #[test]
    fn overlong_2byte_leads_invalid() {
        // 0xC0 and 0xC1 would only encode codepoints < U+0080, which must
        // be 1-byte instead. Reject at the lead.
        for b in [0xC0u8, 0xC1] {
            let (state, steps) = run(&[b, 0x80]);
            assert_eq!(state, DecoderState::Between);
            // First byte invalid; second byte then re-enters from Between
            // as a lone continuation → also invalid.
            assert_eq!(steps, vec![Step::Invalid, Step::Invalid]);
        }
    }

    #[test]
    fn overlong_3byte_rejected_at_second_byte() {
        // E0 with second byte < A0 would encode U+0000..U+07FF (1/2-byte
        // range) — overlong. Must reject at byte 2.
        let (state, steps) = run(&[0xE0, 0x9F, 0x80]);
        assert_eq!(state, DecoderState::Between);
        assert_eq!(steps[0], Step::Pending);
        assert_eq!(steps[1], Step::Invalid);
    }

    #[test]
    fn surrogate_halves_rejected_at_second_byte() {
        // ED A0..BF would produce U+D800..U+DFFF surrogate range.
        for b2 in 0xA0u8..=0xBF {
            let (state, steps) = run(&[0xED, b2, 0x80]);
            assert_eq!(state, DecoderState::Between);
            assert_eq!(steps[0], Step::Pending);
            assert_eq!(steps[1], Step::Invalid, "b2=0x{b2:02X}");
        }
        // ED 80..9F is fine (U+D000..U+D7FF, just below surrogate range).
        let (state, steps) = run(&[0xED, 0x9F, 0xBF]);
        assert_eq!(state, DecoderState::Between);
        assert_eq!(steps[2], Step::Complete('\u{D7FF}'));
    }

    #[test]
    fn above_max_codepoint_rejected_at_second_byte() {
        // F4 90..BF or F5..FF leads would exceed U+10FFFF.
        // F5..FF rejected at the lead byte.
        for b in [0xF5u8, 0xF6, 0xFE, 0xFF] {
            let (state, steps) = run(&[b]);
            assert_eq!(state, DecoderState::Between);
            assert_eq!(steps, vec![Step::Invalid]);
        }
        // F4 90 → codepoint would be > U+10FFFF.
        let (state, steps) = run(&[0xF4, 0x90, 0x80, 0x80]);
        assert_eq!(state, DecoderState::Between);
        assert_eq!(steps[1], Step::Invalid);
        // F4 8F is fine — reaches U+10FFFF exactly.
        let (state, steps) = run(&[0xF4, 0x8F, 0xBF, 0xBF]);
        assert_eq!(state, DecoderState::Between);
        assert_eq!(steps[3], Step::Complete('\u{10FFFF}'));
    }

    #[test]
    fn overlong_4byte_rejected_at_second_byte() {
        // F0 80..8F would encode codepoints < U+10000 — overlong.
        for b2 in 0x80u8..=0x8F {
            let (state, steps) = run(&[0xF0, b2, 0x80, 0x80]);
            assert_eq!(state, DecoderState::Between);
            assert_eq!(steps[1], Step::Invalid, "b2=0x{b2:02X}");
        }
    }

    #[test]
    fn truncation_leaves_state_mid_codepoint() {
        // 2-byte lead with no continuation.
        let (state, _) = run(&[0xC3]);
        assert!(!state.is_between());
        assert!(matches!(state, DecoderState::Cont1Of2 { .. }));
        // 3-byte lead, one continuation in.
        let (state, _) = run(&[0xE2, 0x82]);
        assert!(!state.is_between());
        assert!(matches!(state, DecoderState::Cont1Of3 { .. }));
        // 4-byte lead, two continuations in.
        let (state, _) = run(&[0xF0, 0x9F, 0xA6]);
        assert!(!state.is_between());
        assert!(matches!(state, DecoderState::Cont1Of4 { .. }));
    }

    #[test]
    fn continuation_byte_inside_sequence_rejected_when_lead_expected() {
        // After completing a codepoint, a stray continuation byte must be
        // rejected — proves we returned to Between cleanly.
        let (state, steps) = run(&[b'a', 0x80]);
        assert_eq!(state, DecoderState::Between);
        assert_eq!(steps, vec![Step::Complete('a'), Step::Invalid]);
    }

    #[test]
    fn invalid_byte_in_continuation_returns_to_between() {
        // ASCII 'A' (0x41) is not a valid continuation. Decoder should
        // reject and reset, so the next byte starts a fresh sequence.
        let (state, steps) = run(&[0xC3, 0x41, b'b']);
        assert_eq!(state, DecoderState::Between);
        assert_eq!(steps[0], Step::Pending);
        assert_eq!(steps[1], Step::Invalid);
        assert_eq!(steps[2], Step::Complete('b'));
    }

    #[test]
    fn state_is_hashable_and_eq() {
        use std::collections::HashSet;
        let mut set: HashSet<DecoderState> = HashSet::new();
        set.insert(DecoderState::Between);
        set.insert(DecoderState::Cont1Of2 { acc: 3 });
        set.insert(DecoderState::Cont1Of2 { acc: 3 });
        assert_eq!(set.len(), 2);
    }
}
