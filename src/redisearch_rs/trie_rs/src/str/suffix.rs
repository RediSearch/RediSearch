/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Suffix-index acceleration structure for the `WITHSUFFIXTRIE` text-field
//! path. Pure-Rust port of the `Trie` half of `src/suffix.c`
//! (`addSuffixTrie`, `deleteSuffixTrie`, `Suffix_IterateContains`,
//! `Suffix_IterateWildcard`).
//!
//! ## Indexing model
//!
//! For every indexed term `T` of length `n` codepoints, the suffix index
//! stores:
//!
//!   * a "full-word" entry under the key `T` itself — its [`SuffixData`]
//!     carries `term = Some(Rc<str>)` that owns the term string;
//!   * one "rotation" entry under every proper suffix of `T` whose length is
//!     at least [`MIN_SUFFIX`] codepoints — its [`SuffixData`] carries
//!     `term = None` and an `array` of back-refs to source terms whose
//!     suffix-chain passes through that node.
//!
//! Each entry's `array` accumulates a back-ref (cloned `Rc<str>`) for every
//! source term that shares the suffix. A query for "`*needle*`" walks the
//! subtree rooted at `needle` and yields the union of all those back-refs;
//! a query for "`*needle`" reads a single entry's `array` directly.
//!
//! ## UTF-8 vs rune-space
//!
//! The C implementation operates in BMP UTF-16 rune-space because the
//! underlying `Trie` keys runes (fixed-width per codepoint). Rust's
//! `StrTrieMap` keys raw UTF-8 bytes, so rotation offsets must be derived
//! via [`str::char_indices`] — a naive byte stride would split multibyte
//! sequences and either panic or corrupt subsequent lookups. [`MIN_SUFFIX`]
//! is measured in codepoints, matching C.
//!
//! ## Term ownership: `Rc<str>`
//!
//! C uses one owning `char *` at the full-word entry and weak `char *`
//! back-refs in every rotation entry — sound only because deletion frees
//! the owning copy last. The Rust port translates that contract to
//! [`Rc<str>`]: the full-word entry's `term` field owns the canonical
//! reference, and every rotation entry's `array` clones the `Rc`. Removal
//! decrements the refcount at every visited rotation in turn, and the
//! final drop happens when the full-word entry's `term` is taken last.
//!
//! ## Iteration return type
//!
//! Iterators yield `Rc<str>` clones rather than `&str` borrows. The
//! alternative — borrowing into the [`StrTrieMap`]'s internal node storage
//! — would forbid concurrent mutation of the index even while a query is
//! draining, which is heavier than the actual C contract requires. Cloning
//! the `Rc` is cheap (one atomic increment per yielded back-ref in a
//! single-threaded `Rc`, which is what this port uses).

use std::rc::Rc;

use rqe_wildcard::{MatchOutcome, WildcardPattern};

use crate::str::StrTrieMap;

/// Minimum codepoint length below which a suffix rotation is not indexed.
///
/// Mirrors the `MIN_SUFFIX` macro in `src/suffix.h`. A term of length
/// exactly [`MIN_SUFFIX`] is recorded with a full-word entry only; no
/// proper-suffix rotations are added (the very first proper suffix has
/// length `len - 1 < MIN_SUFFIX`).
pub const MIN_SUFFIX: usize = 2;

/// Per-node payload stored at each terminal in the suffix trie.
///
/// Mirrors the C `suffixData` struct in `src/suffix.h`. The `Option`
/// distinguishes a full-word entry from a rotation entry without an extra
/// flag field: `Some` ⇔ the owning back-ref for the term that has this
/// key as its full form.
pub struct SuffixData {
    /// `Some` iff this entry is the full-word entry for the owning term.
    ///
    /// Holds the canonical strong reference; every rotation entry's
    /// `array` only holds clones, so the term string stays live until the
    /// full-word entry is removed.
    term: Option<Rc<str>>,
    /// Back-refs to source terms whose suffix chain passes through this
    /// node. Includes the term's own back-ref when this is a full-word
    /// entry, and one back-ref per source term whose proper suffix lands
    /// here when this is a rotation entry.
    array: Vec<Rc<str>>,
}

impl SuffixData {
    /// Construct a brand-new payload for a node first visited during the
    /// insertion of `term`.
    ///
    /// `is_full_word = true` populates the owning `Some(term)`; rotation
    /// nodes receive `None`.
    fn fresh(term: Rc<str>, is_full_word: bool) -> Self {
        Self {
            term: is_full_word.then(|| Rc::clone(&term)),
            array: vec![term],
        }
    }

    /// Promote a rotation-only node to full-word status because the term
    /// it carries as a rotation back-ref is now being inserted as a
    /// full-word entry of its own.
    ///
    /// Matches the C branch at `suffix.c:77-80`: the `term` pointer
    /// transitions from `NULL` to the freshly-allocated copy, and the
    /// back-ref array gains one additional entry pointing at that copy.
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

    /// Strip a back-ref equal to `term` from `array`. O(n) over the
    /// back-ref list; uses swap-remove to match the C `array_del_fast`
    /// semantics (order-insensitive removal).
    fn remove_backref(&mut self, term: &str) {
        if let Some(pos) = self.array.iter().position(|t| t.as_ref() == term) {
            self.array.swap_remove(pos);
        }
    }
}

/// Suffix-index for a single text field, port of the `sp->suffix` `Trie`
/// half of `src/suffix.c`. See module docs for the indexing model and the
/// rotation/ownership invariants.
pub struct SuffixIndex {
    inner: StrTrieMap<SuffixData>,
}

impl SuffixIndex {
    pub fn new() -> Self {
        Self {
            inner: StrTrieMap::new(),
        }
    }

    /// Estimated heap bytes held by the suffix index's internal trie.
    ///
    /// Mirrors the contribution `sp->suffix` would make to
    /// `IndexSpec_TotalMemUsage` once the suffix port is wired up. Counts
    /// trie node and key storage; the [`SuffixData`] payloads are included
    /// only insofar as the underlying [`StrTrieMap`] accounts for them.
    pub const fn mem_usage(&self) -> usize {
        self.inner.mem_usage()
    }

    /// Insert `term` and all of its proper suffixes of length at least
    /// [`MIN_SUFFIX`]. No-op if `term` is already indexed as a full word.
    ///
    /// Rotation offsets walk codepoint boundaries via
    /// [`str::char_indices`] so multibyte UTF-8 sequences are never
    /// split. (`suffix.c:46-91`.)
    pub fn add(&mut self, term: &str) {
        if term.is_empty() {
            return;
        }

        // Treat a re-insertion of an already-indexed full word as a no-op
        // (suffix.c:54). Looked up before allocating an `Rc<str>` to avoid
        // the wasted allocation on the hot dedup path.
        if let Some(existing) = self.inner.get(term)
            && existing.term.is_some()
        {
            return;
        }

        let total_chars = term.chars().count();
        let owner: Rc<str> = Rc::from(term);

        // Full-word entry — either fresh insert, or promote an existing
        // rotation-only node.
        self.inner.insert_with(term, {
            let owner = Rc::clone(&owner);
            |existing| match existing {
                Some(mut data) => {
                    data.promote_to_full_word(owner);
                    data
                }
                None => SuffixData::fresh(owner, true),
            }
        });

        // Proper-suffix rotations. Stops once the remaining suffix is
        // shorter than `MIN_SUFFIX` codepoints — matches the C
        // `j + MIN_SUFFIX <= rlen` bound (suffix.c:84).
        for (char_idx, (byte_idx, _)) in term.char_indices().enumerate().skip(1) {
            let suffix_chars = total_chars - char_idx;
            if suffix_chars < MIN_SUFFIX {
                break;
            }
            let suffix = &term[byte_idx..];
            self.inner.insert_with(suffix, {
                let owner = Rc::clone(&owner);
                |existing| match existing {
                    Some(mut data) => {
                        data.push_backref(owner);
                        data
                    }
                    None => SuffixData::fresh(owner, false),
                }
            });
        }
    }

    /// Remove `term` and every rotation back-ref it owns. Silently ignores
    /// terms that aren't currently indexed — the trie may back several
    /// text fields, only some of which inserted the term.
    /// (`suffix.c:147`.)
    pub fn remove(&mut self, term: &str) {
        if term.is_empty() {
            return;
        }

        let total_chars = term.chars().count();

        // Full-word entry. Take ownership of the strong reference held in
        // `term` so the rotation back-refs can be safely dropped (each
        // back-ref is a clone of the same `Rc<str>`; the final
        // `Rc::drop` doesn't need to be the full-word one specifically,
        // but matching C ordering keeps reasoning simple).
        let mut full_word_strong: Option<Rc<str>> = None;
        let mut drop_full_word_node = false;
        if let Some(data) = self.inner.get_mut(term) {
            full_word_strong = data.term.take();
            data.remove_backref(term);
            if data.array.is_empty() {
                drop_full_word_node = true;
            }
        }
        if drop_full_word_node {
            self.inner.remove(term);
        }

        // Rotation entries — same codepoint-aware walk as `add`.
        for (char_idx, (byte_idx, _)) in term.char_indices().enumerate().skip(1) {
            let suffix_chars = total_chars - char_idx;
            if suffix_chars < MIN_SUFFIX {
                break;
            }
            let suffix = &term[byte_idx..];
            let mut drop_node = false;
            if let Some(data) = self.inner.get_mut(suffix) {
                data.remove_backref(term);
                if data.array.is_empty() {
                    drop_node = true;
                }
            }
            if drop_node {
                self.inner.remove(suffix);
            }
        }

        drop(full_word_strong);
    }

    /// Yield every source term whose suffix is exactly `needle`, in
    /// insertion order. Empty `needle` yields zero matches.
    /// (`suffix.c:198-205`, `SUFFIX_TYPE_SUFFIX` branch.)
    pub fn iter_suffix(&self, needle: &str) -> SuffixHits<'_> {
        if needle.is_empty() {
            return SuffixHits::empty();
        }
        match self.inner.get(needle) {
            Some(data) => SuffixHits::from_array(&data.array),
            None => SuffixHits::empty(),
        }
    }

    /// Yield every source term that contains `needle` as a substring.
    /// No deduplication — a source term with multiple rotations under
    /// `needle` is yielded once per rotation. Empty `needle` yields zero
    /// matches.
    /// (`suffix.c:191-197`, `SUFFIX_TYPE_CONTAINS` branch.)
    pub fn iter_contains(&self, needle: &str) -> SuffixHits<'_> {
        if needle.is_empty() {
            return SuffixHits::empty();
        }
        let collected: Vec<Rc<str>> = self
            .inner
            .prefixed_iter(needle)
            .flat_map(|(_key, data)| data.array.iter().cloned())
            .collect();
        SuffixHits::from_vec(collected)
    }

    /// Yield every source term matching the wildcard `pattern`. Returns
    /// `None` if no literal token clears [`MIN_SUFFIX`] codepoints —
    /// caller should fall back to a generic trie iteration.
    ///
    /// Picks the best literal token, sub-iterates the trie at that token,
    /// then filters each candidate against the full pattern with
    /// [`WildcardPattern::matches`]. (`suffix.c:328-345`.)
    pub fn iter_wildcard(&self, pattern: &str) -> Option<SuffixHits<'_>> {
        let chosen = choose_token(pattern)?;

        // Mirror `Suffix_IterateWildcard` (`suffix.c:348-367`): build a
        // sub-pattern from the chosen token text plus the trailing `*`
        // (when present) and wildcard-iterate the trie with it. The
        // sub-pattern can itself contain `?` — a literal `?` from the
        // original pattern carries into the token — so a plain
        // prefix-iter would miss matches whose first `chosen.len`
        // codepoints are any single codepoint (`??*` on a corpus of
        // length-2 terms is the canonical failure mode for a
        // prefix-iter port).
        let sub_pattern: String = if chosen.followed_by_star {
            format!("{}*", chosen.text)
        } else {
            chosen.text.to_string()
        };

        let parsed_full = WildcardPattern::parse(pattern.as_bytes());

        let candidates: Vec<Rc<str>> = self
            .inner
            .wildcard_iter(&sub_pattern)
            .flat_map(|(_key, data)| data.array.iter().cloned())
            .collect();

        // Filter candidates against the full wildcard pattern. The
        // sub-pattern catches the chosen-token's local match; the full
        // pattern enforces the structure of any `*` and other
        // non-chosen tokens.
        let filtered: Vec<Rc<str>> = candidates
            .into_iter()
            .filter(|t| parsed_full.matches(t.as_bytes()) == MatchOutcome::Match)
            .collect();
        Some(SuffixHits::from_vec(filtered))
    }
}

impl Default for SuffixIndex {
    fn default() -> Self {
        Self::new()
    }
}

/// Owned-iterator view over a sequence of suffix-index back-refs.
///
/// Owns its `Rc<str>` clones so the iteration can outlive any borrow into
/// [`SuffixIndex`]'s internal storage — see the module docs for the
/// rationale behind the cloning return type.
pub struct SuffixHits<'a> {
    items: Vec<Rc<str>>,
    pos: usize,
    _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a> SuffixHits<'a> {
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

impl Iterator for SuffixHits<'_> {
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

/// Pattern token chosen by [`choose_token`].
struct ChosenToken<'p> {
    text: &'p str,
    followed_by_star: bool,
}

/// Replicates `Suffix_ChooseToken_rune` in `src/suffix.c` for a UTF-8
/// pattern: split on `*`, score each non-empty token by length plus
/// position bias (later tokens score higher), penalize trailing `*` and
/// internal `?` (one wildcard `?` deducts one point), drop tokens shorter
/// than [`MIN_SUFFIX`] codepoints. Returns `None` if no token clears the
/// minimum-length floor — same sentinel the C code uses to instruct the
/// caller to fall back to a generic walk.
///
/// Mirrors the C codepath at `suffix.c:270-326`. The C variant counts
/// codepoints (BMP runes); the Rust variant counts codepoints via
/// [`str::chars`], yielding identical token boundaries for any pattern
/// whose codepoints fall in the BMP.
fn choose_token(pattern: &str) -> Option<ChosenToken<'_>> {
    let mut tokens: Vec<(usize, usize)> = Vec::new(); // (byte_idx, byte_len)
    let bytes = pattern.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'*' {
            i += 1;
            continue;
        }
        let start = i;
        while i < bytes.len() && bytes[i] != b'*' {
            // Walk by codepoint so multibyte sequences stay grouped.
            let cp_len = utf8_codepoint_len(bytes[i]);
            i += cp_len;
        }
        tokens.push((start, i - start));
    }

    if tokens.is_empty() {
        return None;
    }

    let mut best: Option<(i32, usize)> = None;
    for (idx, &(byte_idx, byte_len)) in tokens.iter().enumerate() {
        let text = &pattern[byte_idx..byte_idx + byte_len];
        let char_count = text.chars().count();
        if char_count < MIN_SUFFIX {
            continue;
        }
        let mut score = char_count as i32 + idx as i32;
        if byte_idx + byte_len < bytes.len() && bytes[byte_idx + byte_len] == b'*' {
            score -= 5;
        }
        for c in text.chars() {
            if c == '?' {
                score -= 1;
            }
        }
        if best.is_none_or(|(b, _)| score >= b) {
            best = Some((score, idx));
        }
    }

    let (_, idx) = best?;
    let (byte_idx, byte_len) = tokens[idx];
    let followed_by_star = byte_idx + byte_len < bytes.len() && bytes[byte_idx + byte_len] == b'*';
    Some(ChosenToken {
        text: &pattern[byte_idx..byte_idx + byte_len],
        followed_by_star,
    })
}

const fn utf8_codepoint_len(b: u8) -> usize {
    if b < 0x80 {
        1
    } else if b < 0xC0 {
        // Continuation byte — shouldn't be entered standalone in
        // well-formed UTF-8, but keep this branch defensive so a
        // malformed pattern doesn't loop forever.
        1
    } else if b < 0xE0 {
        2
    } else if b < 0xF0 {
        3
    } else {
        4
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn to_set<I: Iterator<Item = Rc<str>>>(it: I) -> HashSet<String> {
        it.map(|r| r.as_ref().to_string()).collect()
    }

    #[test]
    fn add_then_remove_returns_to_empty_view() {
        let mut idx = SuffixIndex::new();
        idx.add("banana");
        idx.add("anaconda");
        idx.remove("banana");
        idx.remove("anaconda");

        for needle in [
            "banana", "anana", "nana", "ana", "na", "anaconda", "naconda", "aconda", "conda",
            "onda", "nda", "da", "an",
        ] {
            assert_eq!(idx.iter_contains(needle).count(), 0, "needle={needle}");
            assert_eq!(idx.iter_suffix(needle).count(), 0, "needle={needle}");
        }
    }

    #[test]
    fn add_same_term_twice_is_noop() {
        let mut idx = SuffixIndex::new();
        idx.add("apple");
        idx.add("apple");
        let hits: Vec<_> = idx.iter_contains("apple").collect();
        assert_eq!(hits.len(), 1, "apple inserted twice yields one back-ref");
        assert_eq!(hits[0].as_ref(), "apple");
    }

    #[test]
    fn rotation_then_promote_to_full_word() {
        // "longer" leaves "ger" as a rotation-only node; inserting "ger"
        // must promote it (`suffix.c:77-80`).
        let mut idx = SuffixIndex::new();
        idx.add("longer");
        idx.add("ger");
        let hits = to_set(idx.iter_suffix("ger"));
        assert!(hits.contains("longer"));
        assert!(hits.contains("ger"));
        assert_eq!(hits.len(), 2);
    }

    #[test]
    fn min_suffix_boundary_two_chars_no_rotation() {
        let mut idx = SuffixIndex::new();
        idx.add("ab");
        let exact = to_set(idx.iter_suffix("ab"));
        assert_eq!(exact, HashSet::from(["ab".to_string()]));
        assert_eq!(idx.iter_suffix("b").count(), 0);
    }

    #[test]
    fn multibyte_utf8_terms_round_trip() {
        // Codepoint-aware rotation: "café" has 4 codepoints, so suffix
        // length-2 rotation "fé" is indexed but length-1 "é" is not. A
        // byte-stride port would either panic (`&term[3..]` splits the
        // first byte of "é") or insert a malformed key — neither should
        // happen here.
        let mut idx = SuffixIndex::new();
        for t in ["café", "naïve", "日本語", "🦀rust"] {
            idx.add(t);
        }
        assert!(to_set(idx.iter_suffix("fé")).contains("café"));
        // "é" is a one-codepoint suffix; below MIN_SUFFIX → no entry.
        assert_eq!(idx.iter_suffix("é").count(), 0);
        // "本語" is a two-codepoint suffix of "日本語" — should be present.
        assert!(to_set(idx.iter_suffix("本語")).contains("日本語"));
        // Now round-trip removal.
        for t in ["café", "naïve", "日本語", "🦀rust"] {
            idx.remove(t);
        }
        assert_eq!(idx.iter_contains("café").count(), 0);
        assert_eq!(idx.iter_contains("本語").count(), 0);
        assert_eq!(idx.iter_contains("rust").count(), 0);
    }

    #[test]
    fn delete_unknown_term_is_noop() {
        let mut idx = SuffixIndex::new();
        idx.add("present");
        idx.remove("absent");
        assert!(to_set(idx.iter_suffix("present")).contains("present"));
    }

    #[test]
    fn iter_contains_substring_yields_per_rotation() {
        let mut idx = SuffixIndex::new();
        idx.add("abc");
        idx.add("xabc");
        let hits = to_set(idx.iter_suffix("abc"));
        assert!(hits.contains("abc"));
        assert!(hits.contains("xabc"));
        assert_eq!(hits.len(), 2);
    }

    #[test]
    fn iter_wildcard_returns_none_for_short_pattern() {
        let mut idx = SuffixIndex::new();
        idx.add("apple");
        assert!(idx.iter_wildcard("*a*").is_none());
        assert!(idx.iter_wildcard("*").is_none());
    }

    #[test]
    fn iter_wildcard_filters_against_full_pattern() {
        // "*pp*" picks "pp" as the chosen token (≥ MIN_SUFFIX), then the
        // result is filtered against "*pp*" — "apple", "happy",
        // "puppet" all match; "banana" does not.
        let mut idx = SuffixIndex::new();
        for t in ["apple", "happy", "puppet", "banana"] {
            idx.add(t);
        }
        let hits = to_set(idx.iter_wildcard("*pp*").expect("token chosen"));
        assert!(hits.contains("apple"));
        assert!(hits.contains("happy"));
        assert!(hits.contains("puppet"));
        assert!(!hits.contains("banana"));
    }
}
