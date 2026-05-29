/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Suffix-index acceleration structure for the `WITHSUFFIXTRIE` tag-field
//! path. Pure-Rust port of the `TrieMap` half of `src/suffix.c`.
//!
//! ## Indexing model
//!
//! Same back-ref model as [`SuffixIndex`](crate::str::suffix::SuffixIndex)
//! for text fields: each indexed tag value `T` of byte length `n` is
//! recorded under its full key and under every proper byte-suffix of
//! length ≥ [`MIN_SUFFIX`]. The full-word entry owns an [`Rc<str>`]
//! back-ref; rotation entries hold clones.
//!
//! ## Byte-space rotation (C parity)
//!
//! Rotation offsets run over **bytes**, not codepoints. On a non-ASCII
//! tag a rotation offset can land inside a multibyte UTF-8 sequence,
//! producing a key that's not valid UTF-8 — but the underlying byte-keyed
//! [`TrieMap`] doesn't care, and lookup at query time does the same byte
//! arithmetic, so the index stays self-consistent.
//!
//! ## Term ownership: `Rc<str>`
//!
//! Shared ownership via [`Rc<str>`]; see [`crate::str::suffix`] for the
//! rationale.

use std::rc::Rc;

use rqe_wildcard::{MatchOutcome, WildcardPattern};

use crate::TrieMap;
use crate::str::suffix::MIN_SUFFIX;

/// Per-node payload stored at each terminal in the tag suffix trie.
///
/// Same fields as [`crate::str::suffix::SuffixData`]; carried as a
/// distinct type so the two trie wrappers stay independently usable.
pub struct TagSuffixData {
    term: Option<Rc<str>>,
    array: Vec<Rc<str>>,
}

impl TagSuffixData {
    fn fresh(term: Rc<str>, is_full_word: bool) -> Self {
        Self {
            term: is_full_word.then(|| Rc::clone(&term)),
            array: vec![term],
        }
    }

    fn promote_to_full_word(&mut self, term: Rc<str>) {
        debug_assert!(
            self.term.is_none(),
            "promote_to_full_word on full-word entry"
        );
        self.term = Some(Rc::clone(&term));
        self.array.push(term);
    }

    fn push_backref(&mut self, term: Rc<str>) {
        self.array.push(term);
    }

    fn remove_backref(&mut self, term: &str) {
        if let Some(pos) = self.array.iter().position(|t| t.as_ref() == term) {
            self.array.swap_remove(pos);
        }
    }
}

/// Suffix-index for a single tag field. See module docs for the
/// byte-space rotation contract.
pub struct TagSuffixIndex {
    inner: TrieMap<TagSuffixData>,
}

impl TagSuffixIndex {
    pub fn new() -> Self {
        Self {
            inner: TrieMap::new(),
        }
    }

    /// Estimated heap bytes held by the suffix index's internal trie.
    pub const fn mem_usage(&self) -> usize {
        self.inner.mem_usage()
    }

    pub const fn len(&self) -> usize {
        self.inner.n_unique_keys()
    }

    pub const fn is_empty(&self) -> bool {
        self.inner.n_unique_keys() == 0
    }

    /// Insert `term` and all of its byte-suffix rotations of length at
    /// least [`MIN_SUFFIX`]. No-op if `term` is already indexed as a
    /// full word. Rotation offsets walk bytes, not codepoints — see
    /// module docs.
    pub fn add(&mut self, term: &str) {
        let bytes = term.as_bytes();
        let len = bytes.len();
        if len == 0 {
            return;
        }

        // Already-indexed full word: no-op. Lookup precedes the `Rc<str>`
        // allocation so the hot dedup path skips the allocation cost.
        if let Some(existing) = self.inner.find(bytes)
            && existing.term.is_some()
        {
            return;
        }

        let owner: Rc<str> = Rc::from(term);

        let owner_for_full_word = Rc::clone(&owner);
        self.inner.insert_with(bytes, |existing| match existing {
            Some(mut data) => {
                data.promote_to_full_word(owner_for_full_word);
                data
            }
            None => TagSuffixData::fresh(owner_for_full_word, true),
        });

        // `j` ranges over `1..=len-MIN_SUFFIX` so the suffix has length
        // ≥ MIN_SUFFIX. `checked_sub` keeps the loop empty (rather than
        // underflowing) when `len < MIN_SUFFIX`.
        let Some(last_j) = len.checked_sub(MIN_SUFFIX) else {
            return;
        };
        for j in 1..=last_j {
            let suffix = &bytes[j..];
            let owner_for_rotation = Rc::clone(&owner);
            self.inner.insert_with(suffix, |existing| match existing {
                Some(mut data) => {
                    data.push_backref(owner_for_rotation);
                    data
                }
                None => TagSuffixData::fresh(owner_for_rotation, false),
            });
        }
    }

    /// Remove `term` and every rotation back-ref it owns. Silently skips
    /// rotations that aren't present so a buggy caller can't bring the
    /// index down.
    pub fn remove(&mut self, term: &str) {
        let bytes = term.as_bytes();
        let len = bytes.len();
        if len == 0 {
            return;
        }

        let last_j = match len.checked_sub(MIN_SUFFIX) {
            Some(v) => v,
            // `len < MIN_SUFFIX` → only the full-word entry was inserted.
            None => {
                let mut drop_node = false;
                if let Some(data) = self.inner.find_mut(bytes) {
                    data.term = None;
                    data.remove_backref(term);
                    if data.array.is_empty() {
                        drop_node = true;
                    }
                }
                if drop_node {
                    self.inner.remove(bytes);
                }
                return;
            }
        };

        for j in 0..=last_j {
            let suffix = &bytes[j..];
            let mut drop_node = false;
            if let Some(data) = self.inner.find_mut(suffix) {
                if j == 0 {
                    data.term = None;
                }
                data.remove_backref(term);
                if data.array.is_empty() {
                    drop_node = true;
                }
            }
            if drop_node {
                self.inner.remove(suffix);
            }
        }
    }

    /// Yield every source term whose byte-suffix is exactly `needle`.
    /// Empty `needle` yields zero matches.
    pub fn iter_suffix(&self, needle: &str) -> TagSuffixHits<'_> {
        if needle.is_empty() {
            return TagSuffixHits::empty();
        }
        match self.inner.find(needle.as_bytes()) {
            Some(data) => TagSuffixHits::from_array(&data.array),
            None => TagSuffixHits::empty(),
        }
    }

    /// Yield every source term that contains `needle` as a byte-substring.
    /// No dedup — a source term may appear under several rotations. Empty
    /// `needle` yields zero matches.
    pub fn iter_contains(&self, needle: &str) -> TagSuffixHits<'_> {
        if needle.is_empty() {
            return TagSuffixHits::empty();
        }
        let collected: Vec<Rc<str>> = self
            .inner
            .prefixed_iter(needle.as_bytes())
            .flat_map(|(_key, data)| data.array.iter().cloned())
            .collect();
        TagSuffixHits::from_vec(collected)
    }

    /// Yield every source term matching the wildcard `pattern`. Returns
    /// `None` if no literal token clears [`MIN_SUFFIX`] — same fallback
    /// signal as the text variant.
    pub fn iter_wildcard(&self, pattern: &str) -> Option<TagSuffixHits<'_>> {
        let chosen = choose_token_bytes(pattern.as_bytes())?;

        // Same dispatch as the text variant — build a sub-pattern from
        // the chosen token plus optional trailing `*` and wildcard-
        // iterate the underlying byte trie.
        let mut sub_pattern: Vec<u8> = chosen.text.to_vec();
        if chosen.followed_by_star {
            sub_pattern.push(b'*');
        }
        let parsed_sub = WildcardPattern::parse(&sub_pattern);
        let parsed_full = WildcardPattern::parse(pattern.as_bytes());

        let candidates: Vec<Rc<str>> = self
            .inner
            .wildcard_iter(parsed_sub)
            .flat_map(|(_key, data)| data.array.iter().cloned())
            .collect();

        let filtered: Vec<Rc<str>> = candidates
            .into_iter()
            .filter(|t| parsed_full.matches(t.as_bytes()) == MatchOutcome::Match)
            .collect();
        Some(TagSuffixHits::from_vec(filtered))
    }
}

impl Default for TagSuffixIndex {
    fn default() -> Self {
        Self::new()
    }
}

/// Owned-iterator view; same shape as
/// [`crate::str::suffix::SuffixHits`].
pub struct TagSuffixHits<'a> {
    items: Vec<Rc<str>>,
    pos: usize,
    _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a> TagSuffixHits<'a> {
    const fn empty() -> Self {
        Self {
            items: Vec::new(),
            pos: 0,
            _marker: std::marker::PhantomData,
        }
    }
    fn from_array(array: &[Rc<str>]) -> Self {
        Self {
            items: array.to_vec(),
            pos: 0,
            _marker: std::marker::PhantomData,
        }
    }
    const fn from_vec(items: Vec<Rc<str>>) -> Self {
        Self {
            items,
            pos: 0,
            _marker: std::marker::PhantomData,
        }
    }
}

impl Iterator for TagSuffixHits<'_> {
    type Item = Rc<str>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.items.len() {
            return None;
        }
        let v = self.items[self.pos].clone();
        self.pos += 1;
        Some(v)
    }
}

struct ChosenTokenBytes<'p> {
    text: &'p [u8],
    followed_by_star: bool,
}

/// Byte-space twin of `crate::str::suffix::choose_token`: split the
/// pattern on `*`, score each non-empty token by byte length plus
/// position bias, penalize trailing `*` and internal `?`, drop tokens
/// shorter than [`MIN_SUFFIX`] bytes.
fn choose_token_bytes(pattern: &[u8]) -> Option<ChosenTokenBytes<'_>> {
    let mut tokens: Vec<(usize, usize)> = Vec::new();
    let mut i = 0;
    while i < pattern.len() {
        if pattern[i] == b'*' {
            i += 1;
            continue;
        }
        let start = i;
        while i < pattern.len() && pattern[i] != b'*' {
            i += 1;
        }
        tokens.push((start, i - start));
    }

    if tokens.is_empty() {
        return None;
    }

    let mut best: Option<(i32, usize)> = None;
    for (idx, &(byte_idx, byte_len)) in tokens.iter().enumerate() {
        if byte_len < MIN_SUFFIX {
            continue;
        }
        let mut score = byte_len as i32 + idx as i32;
        if byte_idx + byte_len < pattern.len() && pattern[byte_idx + byte_len] == b'*' {
            score -= 5;
        }
        for &b in &pattern[byte_idx..byte_idx + byte_len] {
            if b == b'?' {
                score -= 1;
            }
        }
        if best.is_none_or(|(b, _)| score >= b) {
            best = Some((score, idx));
        }
    }

    let (_, idx) = best?;
    let (byte_idx, byte_len) = tokens[idx];
    let followed_by_star =
        byte_idx + byte_len < pattern.len() && pattern[byte_idx + byte_len] == b'*';
    Some(ChosenTokenBytes {
        text: &pattern[byte_idx..byte_idx + byte_len],
        followed_by_star,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn to_set<I: Iterator<Item = Rc<str>>>(it: I) -> HashSet<String> {
        it.map(|r| r.as_ref().to_string()).collect()
    }

    #[test]
    fn add_remove_round_trip() {
        let mut idx = TagSuffixIndex::new();
        idx.add("foobar");
        idx.add("foo");
        idx.remove("foobar");
        idx.remove("foo");
        for needle in ["foobar", "oobar", "obar", "bar", "ar", "foo", "oo"] {
            assert_eq!(idx.iter_contains(needle).count(), 0, "needle={needle}");
            assert_eq!(idx.iter_suffix(needle).count(), 0, "needle={needle}");
        }
        assert!(idx.is_empty());
    }

    #[test]
    fn duplicate_add_is_noop() {
        let mut idx = TagSuffixIndex::new();
        idx.add("apple");
        idx.add("apple");
        let hits: Vec<_> = idx.iter_suffix("apple").collect();
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn min_suffix_byte_boundary() {
        let mut idx = TagSuffixIndex::new();
        idx.add("ab");
        assert_eq!(to_set(idx.iter_suffix("ab")), HashSet::from(["ab".into()]));
        assert_eq!(idx.iter_suffix("b").count(), 0);
    }

    #[test]
    fn promotion_from_rotation_to_full_word() {
        let mut idx = TagSuffixIndex::new();
        idx.add("longer");
        idx.add("ger");
        let hits = to_set(idx.iter_suffix("ger"));
        assert!(hits.contains("longer"));
        assert!(hits.contains("ger"));
        assert_eq!(hits.len(), 2);
    }

    #[test]
    fn delete_unknown_is_noop_in_practice() {
        let mut idx = TagSuffixIndex::new();
        idx.add("present");
        idx.remove("absent");
        assert!(to_set(idx.iter_suffix("present")).contains("present"));
    }

    #[test]
    fn wildcard_short_token_returns_none() {
        let mut idx = TagSuffixIndex::new();
        idx.add("apple");
        assert!(idx.iter_wildcard("*a*").is_none());
    }

    #[test]
    fn wildcard_filters_against_full_pattern() {
        let mut idx = TagSuffixIndex::new();
        for t in ["apple", "happy", "puppet", "banana"] {
            idx.add(t);
        }
        let hits = to_set(idx.iter_wildcard("*pp*").expect("token chosen"));
        assert!(hits.contains("apple"));
        assert!(hits.contains("happy"));
        assert!(hits.contains("puppet"));
        assert!(!hits.contains("banana"));
    }

    #[test]
    fn non_ascii_byte_rotation_lookups_consistent() {
        // Rotation offsets are byte-indexed. A non-ASCII tag ("café")
        // gets rotations at every byte offset ≥ 1, including offsets
        // that fall mid-multibyte-sequence. The exact rotation keys
        // aren't valid UTF-8 — but the trie keys raw bytes, and the
        // query path uses the same byte arithmetic, so lookups stay
        // self-consistent. Pin that the round-trip exact-byte lookup
        // works for the full word and for the well-formed final
        // suffix `"é"` (two bytes, exactly MIN_SUFFIX).
        let mut idx = TagSuffixIndex::new();
        idx.add("café");
        let exact = to_set(idx.iter_suffix("café"));
        assert!(exact.contains("café"));
        // The 2-byte suffix "é" (U+00E9 = 0xC3 0xA9) is exactly
        // MIN_SUFFIX bytes and well-formed UTF-8, so a needle lookup
        // works.
        let hit = to_set(idx.iter_suffix("é"));
        assert!(hit.contains("café"));
    }
}
