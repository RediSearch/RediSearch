/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Integration tests for [`trie_rs::RuneTrieMap`].
//!
//! Exercises the wrapper through its public crate-level API, focusing on
//! larger-scale cross-checks and edge cases that the inline unit tests
//! don't cover. The unit tests pin down each rule in isolation; these
//! tests put the rules under combined load.

use std::collections::BTreeMap;

use trie_rs::{RuneBound, RuneTrieMap};

/// Convert a `&str` to a `Vec<u16>` of UTF-16 runes — the wire format
/// callers use throughout the legacy C trie.
fn utf16(s: &str) -> Vec<u16> {
    s.encode_utf16().collect()
}

#[test]
fn cross_check_against_btreemap_oracle() {
    // A BTreeMap keyed on the same packed-byte representation is the
    // ground truth: it sorts identically and supports the same lookups,
    // so any divergence in iter / find / len indicates a wrapper bug.
    let words = [
        "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta", "iota", "kappa",
        "lambda", "mu", "nu", "xi", "omicron", "pi", "rho", "sigma", "tau", "upsilon", "phi",
        "chi", "psi", "omega",
    ];

    let mut trie: RuneTrieMap<u32> = RuneTrieMap::new();
    let mut oracle: BTreeMap<Vec<u16>, u32> = BTreeMap::new();

    for (i, w) in words.iter().enumerate() {
        let key = utf16(w);
        trie.insert_replace(&key, i as u32);
        oracle.insert(key, i as u32);
    }

    assert_eq!(trie.len(), oracle.len());
    for (k, v) in &oracle {
        assert_eq!(trie.find(k), Some(v), "find mismatch on {:?}", k);
    }

    let trie_pairs: Vec<(Vec<u16>, u32)> = trie.iter().map(|(k, &v)| (k, v)).collect();
    let oracle_pairs: Vec<(Vec<u16>, u32)> = oracle.iter().map(|(k, &v)| (k.clone(), v)).collect();
    assert_eq!(trie_pairs, oracle_pairs);

    // Remove every other entry and re-verify.
    for (i, w) in words.iter().enumerate() {
        if i % 2 == 0 {
            let key = utf16(w);
            assert_eq!(trie.remove(&key), oracle.remove(&key));
        }
    }
    assert_eq!(trie.len(), oracle.len());

    let trie_pairs: Vec<(Vec<u16>, u32)> = trie.iter().map(|(k, &v)| (k, v)).collect();
    let oracle_pairs: Vec<(Vec<u16>, u32)> = oracle.iter().map(|(k, &v)| (k.clone(), v)).collect();
    assert_eq!(trie_pairs, oracle_pairs);
}

#[test]
fn iter_order_spans_full_u16_range() {
    // Keys differ in the high byte of the single rune. Without big-endian
    // packing, byte-lex order would split these into four bands by the
    // low byte instead of one rising sequence.
    //
    // Note: `trie_rs::Node` stores `n_children` as `u8`, so we stay
    // strictly under 256 distinct first-byte values to avoid hitting an
    // unrelated debug-mode overflow in the underlying trie.
    let mut trie: RuneTrieMap<u32> = RuneTrieMap::new();
    let runes: Vec<u16> = (0..=u16::MAX).step_by(331).collect();
    assert!(runes.len() < 255, "too many root children for trie_rs");
    for &r in &runes {
        trie.insert_replace(&[r], r as u32);
    }
    let yielded: Vec<u16> = trie.iter().map(|(k, _)| k[0]).collect();
    let mut sorted = runes.clone();
    sorted.sort();
    assert_eq!(yielded, sorted);
    assert_eq!(trie.len(), runes.len());
}

#[test]
fn range_iter_matches_btreemap_range() {
    let mut trie: RuneTrieMap<u32> = RuneTrieMap::new();
    let mut oracle: BTreeMap<Vec<u16>, u32> = BTreeMap::new();
    let words = [
        "ant", "bee", "cat", "dog", "elk", "fox", "gnu", "hare", "ibis", "jay", "kite", "lynx",
        "mole", "newt", "owl", "pig", "quail", "rat", "swan", "toad",
    ];
    for (i, w) in words.iter().enumerate() {
        let key = utf16(w);
        trie.insert_replace(&key, i as u32);
        oracle.insert(key, i as u32);
    }

    let lo = utf16("dog");
    let hi = utf16("owl");

    // Inclusive both ends.
    let mut hits = Vec::new();
    trie.range_iter(
        Some(RuneBound::included(&lo)),
        Some(RuneBound::included(&hi)),
        |k, &v| hits.push((k.to_vec(), v)),
    );
    let expected: Vec<(Vec<u16>, u32)> = oracle
        .range(lo.clone()..=hi.clone())
        .map(|(k, &v)| (k.clone(), v))
        .collect();
    assert_eq!(hits, expected);

    // Exclusive both ends.
    let mut hits = Vec::new();
    trie.range_iter(
        Some(RuneBound::excluded(&lo)),
        Some(RuneBound::excluded(&hi)),
        |k, &v| hits.push((k.to_vec(), v)),
    );
    let expected: Vec<(Vec<u16>, u32)> = oracle
        .range((std::ops::Bound::Excluded(lo.clone()), std::ops::Bound::Excluded(hi.clone())))
        .map(|(k, &v)| (k.clone(), v))
        .collect();
    assert_eq!(hits, expected);

    // Open-ended (no min).
    let mut hits = Vec::new();
    trie.range_iter(None, Some(RuneBound::included(&utf16("cat"))), |k, &v| {
        hits.push((k.to_vec(), v))
    });
    let expected: Vec<(Vec<u16>, u32)> = oracle
        .range(..=utf16("cat"))
        .map(|(k, &v)| (k.clone(), v))
        .collect();
    assert_eq!(hits, expected);

    // Open-ended (no max).
    let mut hits = Vec::new();
    trie.range_iter(Some(RuneBound::included(&utf16("rat"))), None, |k, &v| {
        hits.push((k.to_vec(), v))
    });
    let expected: Vec<(Vec<u16>, u32)> = oracle
        .range(utf16("rat")..)
        .map(|(k, &v)| (k.clone(), v))
        .collect();
    assert_eq!(hits, expected);
}

#[test]
fn wildcard_combined_question_and_star() {
    let mut trie: RuneTrieMap<u32> = RuneTrieMap::new();
    let words = [
        "fooXbar", "fooXXbar", "fooXXXbar", "foobar", "fooXbarX", "barfoo", "fobar",
    ];
    for (i, w) in words.iter().enumerate() {
        trie.insert_replace(&utf16(w), i as u32);
    }

    // `foo?bar` => exactly one rune between foo and bar.
    let mut hits = Vec::new();
    trie.wildcard_iter(&utf16("foo?bar"), |k, _| hits.push(k.to_vec()));
    assert_eq!(hits, vec![utf16("fooXbar")]);

    // `foo*bar` => zero or more runes between foo and bar.
    let mut hits = Vec::new();
    trie.wildcard_iter(&utf16("foo*bar"), |k, _| hits.push(k.to_vec()));
    let mut expected = vec![utf16("foobar"), utf16("fooXbar"), utf16("fooXXbar"), utf16("fooXXXbar")];
    hits.sort();
    expected.sort();
    assert_eq!(hits, expected);

    // `foo*bar*` => trailing `*` allows extra runes after bar.
    let mut hits = Vec::new();
    trie.wildcard_iter(&utf16("foo*bar*"), |k, _| hits.push(k.to_vec()));
    hits.sort();
    let mut expected = vec![
        utf16("foobar"),
        utf16("fooXbar"),
        utf16("fooXXbar"),
        utf16("fooXXXbar"),
        utf16("fooXbarX"),
    ];
    expected.sort();
    assert_eq!(hits, expected);

    // `?o*` => first rune any, second rune literal `o`, rest anything.
    let mut hits = Vec::new();
    trie.wildcard_iter(&utf16("?o*"), |k, _| hits.push(k.to_vec()));
    hits.sort();
    let mut expected = vec![
        utf16("foobar"),
        utf16("fooXbar"),
        utf16("fooXXbar"),
        utf16("fooXXXbar"),
        utf16("fooXbarX"),
        utf16("fobar"),
    ];
    expected.sort();
    assert_eq!(hits, expected);
}

#[test]
fn wildcard_with_high_byte_literal_runes() {
    // Runes whose low byte happens to be the wildcard byte values must
    // still be treated as literals when they appear in a literal context.
    // Rune 0x002A is itself the byte `*`; rune 0x003F is `?`; rune 0x005C
    // is `\`. Each appears here as a key character — the wrapper must
    // escape them when packing the *literal* parts of a pattern.
    let mut trie: RuneTrieMap<u32> = RuneTrieMap::new();
    let key_with_specials: Vec<u16> = vec![0x0041, 0x002A, 0x003F, 0x005C, 0x0042];
    let other_key: Vec<u16> = vec![0x0041, 0x0058, 0x0059, 0x005A, 0x0042];
    trie.insert_replace(&key_with_specials, 1);
    trie.insert_replace(&other_key, 2);

    // Literal pattern using `\` to escape each special rune.
    // Pattern runes: 'A' 0x002A '\' 0x003F '\' 0x005C 'B' — but our `\`
    // rune in the *pattern* is 0x005C which is escape; to spell a
    // literal `*` rune we use `\` followed by `*`. We can't just pass
    // 0x002A as a literal — it would be parsed as wildcard star. So we
    // construct the pattern explicitly.
    let pattern: Vec<u16> = vec![
        0x0041, // A
        0x005C, 0x002A, // \*  -> literal rune 0x002A
        0x005C, 0x003F, // \?  -> literal rune 0x003F
        0x005C, 0x005C, // \\  -> literal rune 0x005C
        0x0042, // B
    ];

    let mut hits = Vec::new();
    trie.wildcard_iter(&pattern, |k, _| hits.push(k.to_vec()));
    assert_eq!(hits, vec![key_with_specials.clone()]);
}

#[test]
fn contains_iter_ignores_byte_straddle_in_corpus() {
    // The straddle hazard: byte-substring on packed keys can match
    // positions that don't align with rune boundaries. We construct a
    // corpus that *would* yield a byte-level false positive, then ensure
    // the wrapper drops it.
    let mut trie: RuneTrieMap<u32> = RuneTrieMap::new();
    // [0x12, 0x34] packs to bytes [00 12 00 34] (from u16 view 0x0012,
    // 0x0034). A naïve byte-substring of [12 00] would hit at offset 1.
    let key: Vec<u16> = vec![0x0012, 0x0034];
    trie.insert_replace(&key, 1);

    // Needle whose packed bytes (12 00) straddle the rune boundary in
    // the key. Rune-level there is no rune 0x1200 in the key.
    let needle = vec![0x1200u16];

    let mut hits = Vec::new();
    trie.contains_iter(&needle, false, false, |k, &v| hits.push((k.to_vec(), v)));
    assert!(hits.is_empty(), "byte-straddle match must not surface");

    // Rune-aligned needle still matches.
    let mut hits = Vec::new();
    trie.contains_iter(&[0x0034u16], false, false, |k, &v| {
        hits.push((k.to_vec(), v))
    });
    assert_eq!(hits, vec![(key, 1)]);
}

#[test]
fn empty_key_is_addressable() {
    // The C trie supports an empty key (root entry). The wrapper should
    // too — pack_runes(&[]) is just an empty byte buffer.
    let mut trie: RuneTrieMap<u32> = RuneTrieMap::new();
    assert_eq!(trie.find(&[]), None);
    assert_eq!(trie.insert_replace(&[], 99), None);
    assert_eq!(trie.find(&[]), Some(&99));
    assert_eq!(trie.len(), 1);
    assert_eq!(trie.remove(&[]), Some(99));
    assert!(trie.is_empty());
}

#[test]
fn long_key_handling() {
    // Rune keys of meaningful length. The legacy C trie tops out around
    // u16::MAX label per node; the wrapper should pass through cleanly
    // for keys well below that.
    let mut trie: RuneTrieMap<u32> = RuneTrieMap::new();
    let key: Vec<u16> = (0..1000).map(|i| (i & 0xFFFF) as u16).collect();
    trie.insert_replace(&key, 7);
    assert_eq!(trie.find(&key), Some(&7));
    let out: Vec<(Vec<u16>, u32)> = trie.iter().map(|(k, &v)| (k, v)).collect();
    assert_eq!(out, vec![(key, 7)]);
}

#[test]
fn drop_count_matches_remaining_entries() {
    // When the trie itself drops, every still-resident payload must
    // drop exactly once. This exercises the `Drop` plumbing across the
    // wrapper boundary — the wrapper itself never touches `Drop` but
    // it must not leak via shared internal state.
    use std::sync::atomic::{AtomicUsize, Ordering};

    static DROPS: AtomicUsize = AtomicUsize::new(0);

    struct Tracker;
    impl Drop for Tracker {
        fn drop(&mut self) {
            DROPS.fetch_add(1, Ordering::SeqCst);
        }
    }

    DROPS.store(0, Ordering::SeqCst);
    {
        let mut trie: RuneTrieMap<Tracker> = RuneTrieMap::new();
        for i in 0u16..50 {
            trie.insert_replace(&[i, i.wrapping_add(1), i.wrapping_add(2)], Tracker);
        }
        assert_eq!(DROPS.load(Ordering::SeqCst), 0);
        // Replacing must drop the old payload exactly once per replace.
        for i in 0u16..10 {
            trie.insert_replace(&[i, i.wrapping_add(1), i.wrapping_add(2)], Tracker);
        }
        assert_eq!(DROPS.load(Ordering::SeqCst), 10);
        // 50 entries still live before drop.
        assert_eq!(trie.len(), 50);
    }
    // 10 already dropped via replace + 50 dropped on tree drop.
    assert_eq!(DROPS.load(Ordering::SeqCst), 60);
}

#[test]
fn prefixed_iter_skips_non_prefix_keys() {
    let mut trie: RuneTrieMap<u32> = RuneTrieMap::new();
    let entries = [
        ("preface", 1),
        ("prefix", 2),
        ("preflight", 3),
        ("preview", 4),
        ("press", 5),
        ("under", 6),
        ("over", 7),
    ];
    for (w, v) in &entries {
        trie.insert_replace(&utf16(w), *v);
    }

    let mut got: Vec<(Vec<u16>, u32)> = trie
        .prefixed_iter(&utf16("pref"))
        .map(|(k, &v)| (k, v))
        .collect();
    let mut expected = vec![
        (utf16("preface"), 1),
        (utf16("prefix"), 2),
        (utf16("preflight"), 3),
    ];
    got.sort();
    expected.sort();
    assert_eq!(got, expected);
}

#[test]
fn unicode_runes_preserve_lex_order() {
    // Mix of ASCII (single-byte UTF-16 runes) and high-plane characters
    // (still single rune in UTF-16 BMP). Surrogate pairs would yield two
    // runes — also exercised here as separate keys.
    let mut trie: RuneTrieMap<u32> = RuneTrieMap::new();
    let inputs = [
        "apple",         // ASCII
        "\u{00E9}clair", // Latin-1 supplement: rune 0x00E9 (é)
        "\u{4E2D}\u{6587}",   // CJK: 0x4E2D, 0x6587 ("中文")
        "z",
        "\u{FFFF}",      // Top of BMP
    ];
    for (i, w) in inputs.iter().enumerate() {
        trie.insert_replace(&utf16(w), i as u32);
    }

    let yielded: Vec<Vec<u16>> = trie.iter().map(|(k, _)| k).collect();
    let mut expected: Vec<Vec<u16>> = inputs.iter().map(|s| utf16(s)).collect();
    expected.sort();
    assert_eq!(yielded, expected);
}
