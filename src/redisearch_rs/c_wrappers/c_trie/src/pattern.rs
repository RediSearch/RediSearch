/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The wildcard pattern both trie kinds walk with.

/// A lowercased wildcard pattern, in both encodings the wildcard walks need.
///
/// The lowercasing is the *caller's* job and is not checked here. A pattern
/// built from unfolded runes matches case-sensitively rather than erroring,
/// because the tries store folded keys and nothing on the way in can tell an
/// already-folded rune from one that was never folded.
///
/// The walks read one element *past* the pattern — its value decides the match
/// for a pattern ending in `*` — so the runes carry a zero sentinel past their
/// content, which a plain `Vec` of the converted pattern would not have. This
/// type owns that layout instead of leaving each call site to remember it.
#[derive(Debug)]
pub struct LoweredPattern {
    /// The content runes followed by one zero sentinel, so [`len`] is
    /// `runes.len() - 1`.
    ///
    /// [`len`]: LoweredPattern::len
    pub(crate) runes: Vec<ffi::rune>,
}

impl LoweredPattern {
    /// Build a pattern from its lowercased runes, appending the sentinel.
    ///
    /// `runes` must hold only the content — the sentinel is added here.
    ///
    /// Returns [`None`], which every caller treats as matching nothing, rather
    /// than building a pattern that could not name a stored term:
    ///
    /// - a rune slice longer than [`MAX_RUNE_STR_LEN`](ffi::MAX_RUNE_STR_LEN),
    ///   which term insertion declines the same way. A pattern that *is* built
    ///   therefore fits the `int` length the walks take;
    /// - a slice holding a zero rune, which collides with the sentinel layout
    ///   this type guarantees — a consumer scanning for the zero would see a
    ///   truncated pattern — and which no stored term contains.
    pub fn new(runes: &[ffi::rune]) -> Option<Self> {
        if runes.contains(&0) {
            return None;
        }
        if runes.len() > ffi::MAX_RUNE_STR_LEN as usize {
            return None;
        }

        let mut runes = runes.to_vec();
        runes.push(0);
        Some(Self { runes })
    }

    /// The number of content runes, excluding the sentinel.
    pub const fn len(&self) -> usize {
        self.runes.len() - 1
    }

    /// Whether the pattern has no content.
    pub const fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
