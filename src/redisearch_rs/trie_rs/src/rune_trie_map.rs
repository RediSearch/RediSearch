/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! A trie keyed on `&[u16]` rune sequences.
//!
//! [`RuneTrieMap`] is a thin adapter over [`TrieMap`] that packs each rune
//! into two big-endian bytes before delegating to the byte-keyed trie.
//! Big-endian was chosen deliberately: lexicographic order on the packed
//! bytes is identical to lexicographic order on the original `u16`
//! sequence, which is the order the legacy C lex-mode trie produces.
//!
//! ## Wildcard and contains semantics
//!
//! Wildcard (`?`, `*`, `\`) and contains queries operate at the *rune*
//! level, not the byte level. The wrapper translates rune patterns into
//! byte patterns that, when matched against packed keys, yield
//! rune-aligned results, and post-filters byte-level contains hits to
//! reject matches that straddle a rune boundary.

use crate::TrieMap;
use crate::iter::{RangeBoundary, RangeFilter};
use wildcard::WildcardPattern;

/// A trie keyed on `&[u16]` rune sequences.
///
/// Internally wraps a [`TrieMap`] keyed on big-endian-packed bytes. The
/// `T` payload is owned by the trie and dropped when the corresponding
/// key is removed or the trie itself is dropped.
pub struct RuneTrieMap<T> {
    inner: TrieMap<T>,
}

impl<T> Default for RuneTrieMap<T> {
    fn default() -> Self {
        Self {
            inner: TrieMap::new(),
        }
    }
}

impl<T> RuneTrieMap<T> {
    /// Creates an empty trie.
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts a value, replacing any existing entry under the same key.
    ///
    /// Returns the previous value if one was present.
    pub fn insert_replace(&mut self, runes: &[u16], data: T) -> Option<T> {
        let key = pack_runes(runes);
        self.inner.insert(&key, data)
    }

    /// Inserts a value computed from the previous one, if any.
    ///
    /// `f` receives `Some(old)` if the key was already present, or `None`
    /// otherwise, and returns the value to store.
    pub fn insert_with<F>(&mut self, runes: &[u16], f: F)
    where
        F: FnOnce(Option<T>) -> T,
    {
        let key = pack_runes(runes);
        self.inner.insert_with(&key, f);
    }

    /// Removes the entry associated with `runes`, returning the old value
    /// if it was present.
    pub fn remove(&mut self, runes: &[u16]) -> Option<T> {
        let key = pack_runes(runes);
        self.inner.remove(&key)
    }

    /// Returns a reference to the value associated with `runes`.
    pub fn find(&self, runes: &[u16]) -> Option<&T> {
        let key = pack_runes(runes);
        self.inner.find(&key)
    }

    /// Number of unique keys in the trie.
    pub fn len(&self) -> usize {
        self.inner.n_unique_keys()
    }

    /// Whether the trie contains no entries.
    pub fn is_empty(&self) -> bool {
        self.inner.n_unique_keys() == 0
    }

    /// Total memory usage of the trie in bytes (cached, O(1)).
    pub fn mem_usage(&self) -> usize {
        self.inner.mem_usage()
    }

    /// Iterates over all `(runes, &value)` pairs in lexicographic key order.
    pub fn iter(&self) -> impl Iterator<Item = (Vec<u16>, &T)> {
        self.inner
            .iter()
            .map(|(bytes, data)| (unpack_bytes(&bytes), data))
    }

    /// Iterates over entries whose key starts with `prefix`, in
    /// lexicographic key order.
    pub fn prefixed_iter(&self, prefix: &[u16]) -> impl Iterator<Item = (Vec<u16>, &T)> {
        let packed = pack_runes(prefix);
        self.inner
            .prefixed_iter(&packed)
            .map(|(bytes, data)| (unpack_bytes(&bytes), data))
    }

    /// Visits every entry whose key falls within the given rune range, in
    /// lexicographic order, invoking `cb(runes, &value)` per match.
    ///
    /// `min` / `max` set to `None` mean unbounded on that side.
    pub fn range_iter<F>(&self, min: Option<RuneBound<'_>>, max: Option<RuneBound<'_>>, mut cb: F)
    where
        F: FnMut(&[u16], &T),
    {
        let min_packed = min.map(|b| (pack_runes(b.value), b.is_included));
        let max_packed = max.map(|b| (pack_runes(b.value), b.is_included));
        let filter = RangeFilter {
            min: min_packed.as_ref().map(|(v, inc)| RangeBoundary {
                value: v.as_slice(),
                is_included: *inc,
            }),
            max: max_packed.as_ref().map(|(v, inc)| RangeBoundary {
                value: v.as_slice(),
                is_included: *inc,
            }),
        };
        for (bytes, data) in self.inner.range_iter(filter) {
            cb(&unpack_bytes(&bytes), data);
        }
    }

    /// Visits every entry whose key matches the rune-level wildcard
    /// pattern, invoking `cb(runes, &value)` per match.
    ///
    /// In `pattern`, the rune `'?'` (`U+003F`) matches exactly one rune,
    /// `'*'` (`U+002A`) matches any number of runes (including zero), and
    /// `'\\'` (`U+005C`) escapes the next rune so it is treated as a
    /// literal regardless of value.
    pub fn wildcard_iter<F>(&self, pattern: &[u16], mut cb: F)
    where
        F: FnMut(&[u16], &T),
    {
        let translated = translate_wildcard_pattern(pattern);
        let parsed = WildcardPattern::parse(&translated);
        for (bytes, data) in self.inner.wildcard_iter(parsed) {
            cb(&unpack_bytes(&bytes), data);
        }
    }

    /// Visits every entry whose key contains `target` as a rune-aligned
    /// substring, invoking `cb(runes, &value)` per match.
    ///
    /// If `prefix_only` is true, the match must occur at the start of the
    /// key. If `suffix_only` is true, it must occur at the end. Setting
    /// both to true narrows to exact equality, and setting both to false
    /// permits any rune-aligned substring offset.
    pub fn contains_iter<F>(&self, target: &[u16], prefix_only: bool, suffix_only: bool, mut cb: F)
    where
        F: FnMut(&[u16], &T),
    {
        let needle = pack_runes(target);
        for (bytes, data) in self.inner.contains_iter(&needle) {
            if !rune_aligned_match(&bytes, &needle, prefix_only, suffix_only) {
                continue;
            }
            cb(&unpack_bytes(&bytes), data);
        }
    }
}

/// A rune-keyed bound for [`RuneTrieMap::range_iter`].
#[derive(Copy, Clone, Debug)]
pub struct RuneBound<'a> {
    /// The boundary value.
    pub value: &'a [u16],
    /// Whether the boundary value itself is included.
    pub is_included: bool,
}

impl<'a> RuneBound<'a> {
    /// A boundary that includes its value.
    pub const fn included(value: &'a [u16]) -> Self {
        Self {
            value,
            is_included: true,
        }
    }

    /// A boundary that excludes its value.
    pub const fn excluded(value: &'a [u16]) -> Self {
        Self {
            value,
            is_included: false,
        }
    }
}

/// Pack a rune slice into the big-endian byte representation that
/// [`TrieMap`] uses as its key.
fn pack_runes(runes: &[u16]) -> Vec<u8> {
    let mut out = Vec::with_capacity(runes.len() * 2);
    for &r in runes {
        out.extend_from_slice(&r.to_be_bytes());
    }
    out
}

/// Unpack a packed-rune byte sequence back into runes.
///
/// A trailing odd byte (which should never occur for keys produced by
/// [`pack_runes`]) is silently dropped.
fn unpack_bytes(bytes: &[u8]) -> Vec<u16> {
    bytes
        .chunks_exact(2)
        .map(|c| u16::from_be_bytes([c[0], c[1]]))
        .collect()
}

/// Translate a rune-level wildcard pattern into a byte-level pattern
/// suitable for [`WildcardPattern::parse`] running over packed keys.
///
/// Each rune in the input maps to either a single wildcard byte or a
/// (possibly escaped) two-byte literal in the output.
fn translate_wildcard_pattern(pattern: &[u16]) -> Vec<u8> {
    let mut out = Vec::with_capacity(pattern.len() * 4);
    let mut i = 0;
    while i < pattern.len() {
        let r = pattern[i];
        match r {
            // Rune `?` => match one rune (two bytes).
            0x003F => out.extend_from_slice(b"??"),
            // Rune `*` => match any number of runes (any number of bytes).
            // The trie only contains even-length keys, so a `*` will
            // naturally land on rune-aligned offsets.
            0x002A => out.push(b'*'),
            // Rune `\` => next rune is a literal regardless of value.
            0x005C if i + 1 < pattern.len() => {
                i += 1;
                push_literal_rune(&mut out, pattern[i]);
            }
            _ => push_literal_rune(&mut out, r),
        }
        i += 1;
    }
    out
}

/// Append one rune as a two-byte literal, escaping bytes that have
/// special meaning in [`WildcardPattern`].
fn push_literal_rune(out: &mut Vec<u8>, r: u16) {
    for b in r.to_be_bytes() {
        if matches!(b, b'?' | b'*' | b'\\') {
            out.push(b'\\');
        }
        out.push(b);
    }
}

/// Whether a byte-level contains hit corresponds to a rune-aligned match.
///
/// All keys produced by [`pack_runes`] are even-length, so any candidate
/// match offset is constrained to be even. `prefix_only` and `suffix_only`
/// further pin the match to the head or tail respectively.
fn rune_aligned_match(
    key_bytes: &[u8],
    needle_bytes: &[u8],
    prefix_only: bool,
    suffix_only: bool,
) -> bool {
    let needle_len = needle_bytes.len();
    let key_len = key_bytes.len();
    if needle_len > key_len {
        return false;
    }
    let matches_at = |off: usize| {
        off % 2 == 0
            && off + needle_len <= key_len
            && &key_bytes[off..off + needle_len] == needle_bytes
    };
    if prefix_only && suffix_only {
        return needle_len == key_len && matches_at(0);
    }
    if prefix_only {
        return matches_at(0);
    }
    if suffix_only {
        return matches_at(key_len - needle_len);
    }
    (0..=key_len - needle_len).step_by(2).any(matches_at)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(s: &str) -> Vec<u16> {
        s.encode_utf16().collect()
    }

    #[test]
    fn pack_unpack_roundtrip() {
        let runes = vec![0x0041u16, 0x00E9, 0xFEFF, 0x0000, 0xFFFF];
        let packed = pack_runes(&runes);
        assert_eq!(packed.len(), runes.len() * 2);
        assert_eq!(unpack_bytes(&packed), runes);
    }

    #[test]
    fn pack_preserves_lex_order() {
        let a = vec![0x0041u16];
        let b = vec![0x0041u16, 0x0042];
        let c = vec![0x0042u16];
        let d = vec![0x0100u16];
        let e = vec![0x1000u16];
        assert!(pack_runes(&a) < pack_runes(&b));
        assert!(pack_runes(&b) < pack_runes(&c));
        assert!(pack_runes(&c) < pack_runes(&d));
        assert!(pack_runes(&d) < pack_runes(&e));
    }

    #[test]
    fn pack_be_distinguishes_high_byte() {
        // 0x0100 sorts AFTER 0x00FF in u16-order, so the BE pack must too.
        let lo = pack_runes(&[0x00FFu16]);
        let hi = pack_runes(&[0x0100u16]);
        assert!(lo < hi);
        // Sanity: little-endian pack would invert this — confirm we did pick BE.
        let lo_le = 0x00FFu16.to_le_bytes();
        let hi_le = 0x0100u16.to_le_bytes();
        assert!(hi_le.as_slice() < lo_le.as_slice());
    }

    #[test]
    fn insert_find_remove() {
        let mut t: RuneTrieMap<u32> = RuneTrieMap::new();
        let k1 = r("hello");
        let k2 = r("world");

        assert!(t.is_empty());
        assert_eq!(t.insert_replace(&k1, 1), None);
        assert_eq!(t.insert_replace(&k2, 2), None);
        assert_eq!(t.len(), 2);

        assert_eq!(t.find(&k1), Some(&1));
        assert_eq!(t.find(&k2), Some(&2));
        assert_eq!(t.find(&r("missing")), None);

        assert_eq!(t.insert_replace(&k1, 11), Some(1));
        assert_eq!(t.find(&k1), Some(&11));
        assert_eq!(t.len(), 2);

        assert_eq!(t.remove(&k1), Some(11));
        assert_eq!(t.find(&k1), None);
        assert_eq!(t.len(), 1);
    }

    #[test]
    fn insert_with_passes_old_value() {
        let mut t: RuneTrieMap<u32> = RuneTrieMap::new();
        let k = r("counter");

        t.insert_with(&k, |old| {
            assert!(old.is_none());
            1
        });
        t.insert_with(&k, |old| {
            assert_eq!(old, Some(1));
            old.unwrap() + 10
        });
        assert_eq!(t.find(&k), Some(&11));
    }

    #[test]
    fn iter_yields_lex_order() {
        let mut t: RuneTrieMap<u32> = RuneTrieMap::new();
        t.insert_replace(&r("banana"), 2);
        t.insert_replace(&r("apple"), 1);
        t.insert_replace(&r("cherry"), 3);

        let collected: Vec<_> = t.iter().map(|(k, &v)| (k, v)).collect();
        assert_eq!(
            collected,
            vec![
                (r("apple"), 1),
                (r("banana"), 2),
                (r("cherry"), 3),
            ]
        );
    }

    #[test]
    fn iter_lex_order_with_high_runes() {
        // Keys differ only in the high byte of a single rune. BE packing
        // should keep them in u16-numeric order.
        let mut t: RuneTrieMap<u32> = RuneTrieMap::new();
        t.insert_replace(&[0x1000u16], 1);
        t.insert_replace(&[0x0100u16], 2);
        t.insert_replace(&[0x0010u16], 3);
        t.insert_replace(&[0x0001u16], 4);
        let keys: Vec<Vec<u16>> = t.iter().map(|(k, _)| k).collect();
        assert_eq!(
            keys,
            vec![
                vec![0x0001u16],
                vec![0x0010u16],
                vec![0x0100u16],
                vec![0x1000u16],
            ]
        );
    }

    #[test]
    fn prefixed_iter_filters() {
        let mut t: RuneTrieMap<u32> = RuneTrieMap::new();
        t.insert_replace(&r("apple"), 1);
        t.insert_replace(&r("apricot"), 2);
        t.insert_replace(&r("banana"), 3);

        let collected: Vec<_> = t.prefixed_iter(&r("ap")).map(|(k, &v)| (k, v)).collect();
        assert_eq!(collected, vec![(r("apple"), 1), (r("apricot"), 2)]);
    }

    #[test]
    fn range_iter_inclusive_bounds() {
        let mut t: RuneTrieMap<u32> = RuneTrieMap::new();
        for (i, w) in ["alpha", "bravo", "charlie", "delta", "echo"]
            .iter()
            .enumerate()
        {
            t.insert_replace(&r(w), i as u32);
        }

        let mut hits = Vec::new();
        t.range_iter(
            Some(RuneBound::included(&r("bravo"))),
            Some(RuneBound::included(&r("delta"))),
            |k, &v| hits.push((k.to_vec(), v)),
        );
        assert_eq!(
            hits,
            vec![
                (r("bravo"), 1),
                (r("charlie"), 2),
                (r("delta"), 3),
            ]
        );
    }

    #[test]
    fn range_iter_exclusive_bounds() {
        let mut t: RuneTrieMap<u32> = RuneTrieMap::new();
        for (i, w) in ["a", "b", "c", "d", "e"].iter().enumerate() {
            t.insert_replace(&r(w), i as u32);
        }
        let mut hits = Vec::new();
        t.range_iter(
            Some(RuneBound::excluded(&r("b"))),
            Some(RuneBound::excluded(&r("e"))),
            |k, &v| hits.push((k.to_vec(), v)),
        );
        assert_eq!(hits, vec![(r("c"), 2), (r("d"), 3)]);
    }

    #[test]
    fn wildcard_iter_question_mark_matches_one_rune() {
        let mut t: RuneTrieMap<u32> = RuneTrieMap::new();
        t.insert_replace(&r("cat"), 1);
        t.insert_replace(&r("car"), 2);
        t.insert_replace(&r("cab"), 3);
        t.insert_replace(&r("cars"), 4);

        let mut hits = Vec::new();
        t.wildcard_iter(&r("ca?"), |k, &v| hits.push((k.to_vec(), v)));
        hits.sort();
        let mut expected = vec![(r("cab"), 3), (r("car"), 2), (r("cat"), 1)];
        expected.sort();
        assert_eq!(hits, expected);
    }

    #[test]
    fn wildcard_iter_star_matches_any() {
        let mut t: RuneTrieMap<u32> = RuneTrieMap::new();
        t.insert_replace(&r("car"), 1);
        t.insert_replace(&r("cargo"), 2);
        t.insert_replace(&r("cat"), 3);
        t.insert_replace(&r("dog"), 4);

        let mut hits = Vec::new();
        t.wildcard_iter(&r("ca*"), |k, &v| hits.push((k.to_vec(), v)));
        hits.sort();
        let mut expected = vec![(r("car"), 1), (r("cargo"), 2), (r("cat"), 3)];
        expected.sort();
        assert_eq!(hits, expected);
    }

    #[test]
    fn wildcard_iter_high_byte_question() {
        // Confirms that `?` matches exactly one rune even when adjacent
        // runes differ in the high byte. Without 2-byte translation, a
        // single byte `?` would match half a rune.
        let mut t: RuneTrieMap<u32> = RuneTrieMap::new();
        // Two runes 0x0041, 0x0042 ("AB" in UTF-16)
        t.insert_replace(&[0x0041u16, 0x0042], 1);
        // Three runes 0x0041, 0x0042, 0x0043 — `??` rune pattern must NOT
        // match this entry (it has 3 runes).
        t.insert_replace(&[0x0041u16, 0x0042, 0x0043], 2);
        let mut hits = Vec::new();
        t.wildcard_iter(&r("??"), |k, &v| hits.push((k.to_vec(), v)));
        assert_eq!(hits, vec![(vec![0x0041u16, 0x0042], 1)]);
    }

    #[test]
    fn wildcard_iter_escapes_question_rune() {
        // Storing a key whose actual rune content is "a?b". A literal
        // search for "a?b" with escape must match exactly that key, not
        // serve as a wildcard.
        let mut t: RuneTrieMap<u32> = RuneTrieMap::new();
        t.insert_replace(&r("a?b"), 1);
        t.insert_replace(&r("axb"), 2);

        let pattern: Vec<u16> = r("a\\?b");
        let mut hits = Vec::new();
        t.wildcard_iter(&pattern, |k, &v| hits.push((k.to_vec(), v)));
        assert_eq!(hits, vec![(r("a?b"), 1)]);
    }

    #[test]
    fn contains_iter_substring_anywhere() {
        let mut t: RuneTrieMap<u32> = RuneTrieMap::new();
        t.insert_replace(&r("preview"), 1);
        t.insert_replace(&r("review"), 2);
        t.insert_replace(&r("interview"), 3);
        t.insert_replace(&r("preface"), 4);

        let mut hits = Vec::new();
        t.contains_iter(&r("view"), false, false, |k, &v| hits.push((k.to_vec(), v)));
        hits.sort();
        let mut expected = vec![(r("preview"), 1), (r("review"), 2), (r("interview"), 3)];
        expected.sort();
        assert_eq!(hits, expected);
    }

    #[test]
    fn contains_iter_rejects_byte_straddle() {
        // Key runes [0x00AB, 0x00CD] pack to bytes [00 AB 00 CD]. A
        // byte-level needle of [AB 00] would hit at offset 1 (between
        // runes), but rune-level it does NOT contain rune 0xAB00. The
        // post-filter must drop it.
        let mut t: RuneTrieMap<u32> = RuneTrieMap::new();
        t.insert_replace(&[0x00ABu16, 0x00CD], 1);
        let needle = vec![0xAB00u16];

        let mut hits = Vec::new();
        t.contains_iter(&needle, false, false, |k, &v| hits.push((k.to_vec(), v)));
        assert!(hits.is_empty(), "rune-straddling match must be rejected");
    }

    #[test]
    fn contains_iter_prefix_only() {
        let mut t: RuneTrieMap<u32> = RuneTrieMap::new();
        t.insert_replace(&r("foobar"), 1);
        t.insert_replace(&r("barfoo"), 2);
        let mut hits = Vec::new();
        t.contains_iter(&r("foo"), true, false, |k, &v| hits.push((k.to_vec(), v)));
        assert_eq!(hits, vec![(r("foobar"), 1)]);
    }

    #[test]
    fn contains_iter_suffix_only() {
        let mut t: RuneTrieMap<u32> = RuneTrieMap::new();
        t.insert_replace(&r("foobar"), 1);
        t.insert_replace(&r("barfoo"), 2);
        let mut hits = Vec::new();
        t.contains_iter(&r("foo"), false, true, |k, &v| hits.push((k.to_vec(), v)));
        assert_eq!(hits, vec![(r("barfoo"), 2)]);
    }

    #[test]
    fn drop_runs_for_payload() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        static DROPS: AtomicUsize = AtomicUsize::new(0);

        struct Tracked;
        impl Drop for Tracked {
            fn drop(&mut self) {
                DROPS.fetch_add(1, Ordering::SeqCst);
            }
        }

        DROPS.store(0, Ordering::SeqCst);
        {
            let mut t: RuneTrieMap<Tracked> = RuneTrieMap::new();
            t.insert_replace(&r("a"), Tracked);
            t.insert_replace(&r("b"), Tracked);
            assert_eq!(DROPS.load(Ordering::SeqCst), 0);
            let _ = t.remove(&r("a"));
            assert_eq!(DROPS.load(Ordering::SeqCst), 1);
        }
        // `b` dropped on trie drop.
        assert_eq!(DROPS.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn mem_usage_grows_and_shrinks() {
        let mut t: RuneTrieMap<u32> = RuneTrieMap::new();
        let empty = t.mem_usage();
        t.insert_replace(&r("hello"), 1);
        assert!(t.mem_usage() > empty);
        t.remove(&r("hello"));
        assert_eq!(t.mem_usage(), empty);
    }
}
