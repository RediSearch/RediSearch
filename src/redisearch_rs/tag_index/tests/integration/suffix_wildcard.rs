/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Wildcard-query expansion (`TagIndex::suffix_expand` with
//! [`SuffixQuery::Wildcard`]), driven entirely through the public API.
//!
//! The anchor token itself — which literal token [`SuffixWildcardPattern::new`]
//! picks to walk the suffix trie with — is a white-box concern covered by the
//! crate's own unit tests: a wrong choice is invisible in the matched terms, only
//! in how much of the trie is walked.

use tag_index::{InMemoryMode, SuffixQuery, SuffixWildcardPattern, TagIndex};

use crate::util::commit_mem;

const NO_CAP: u64 = u64::MAX;

/// Build an in-memory index with a suffix trie and commit `tags`.
fn indexed(tags: &[&[u8]]) -> TagIndex<InMemoryMode> {
    let mut idx = TagIndex::<InMemoryMode>::new(true);
    commit_mem(&mut idx, tags);
    idx
}

/// Expand a wildcard query and return the matched terms as owned byte vectors
/// with the trailing NUL stripped, sorted for order-independent comparison.
fn matches(idx: &TagIndex<InMemoryMode>, pattern: &[u8], cap: u64) -> Option<Vec<Vec<u8>>> {
    // A pattern with no usable anchor token is a `SuffixWildcardPattern::new`
    // error, surfaced here as `None`.
    let pattern = SuffixWildcardPattern::new(pattern).ok()?;
    let mut out: Vec<Vec<u8>> = idx
        .suffix_expand(
            SuffixQuery::Wildcard {
                pattern: &pattern,
                max_prefix_expansions: cap,
            },
            None,
        )
        .map(|t| t.to_bytes().to_vec())
        .collect();
    out.sort();
    Some(out)
}

#[test]
fn no_usable_token_returns_none() {
    let idx = indexed(&[b"hello"]);
    // Patterns made only of `*` (or empty) have no literal anchor, so the
    // caller must brute-force instead.
    assert_eq!(matches(&idx, b"*", NO_CAP), None);
    assert_eq!(matches(&idx, b"**", NO_CAP), None);
    assert_eq!(matches(&idx, b"", NO_CAP), None);
}

#[test]
fn valid_token_no_match_returns_empty() {
    let idx = indexed(&[b"hello", b"world"]);
    assert_eq!(matches(&idx, b"*zzz", NO_CAP), Some(vec![]));
}

#[test]
fn suffix_match() {
    let idx = indexed(&[b"hello", b"jello", b"world"]);
    assert_eq!(
        matches(&idx, b"*llo", NO_CAP),
        Some(vec![b"hello".to_vec(), b"jello".to_vec()])
    );
}

#[test]
fn prefix_match_via_wildcard() {
    let idx = indexed(&[b"hello", b"hero", b"her", b"world"]);
    // `he*` must include `her` and `hero` (matched through their own full
    // keys, i.e. via `SuffixData::full_term`) as well as `hello`.
    assert_eq!(
        matches(&idx, b"he*", NO_CAP),
        Some(vec![b"hello".to_vec(), b"her".to_vec(), b"hero".to_vec()])
    );
}

#[test]
fn contains_match() {
    let idx = indexed(&[b"abcXYZ", b"XYZabc", b"nomatch"]);
    assert_eq!(
        matches(&idx, b"*abc*", NO_CAP),
        Some(vec![b"XYZabc".to_vec(), b"abcXYZ".to_vec()])
    );
}

/// A `\*` is a literal star, in the anchor walked against the suffix trie as
/// much as in the full-pattern recheck, so a term holding a literal star is
/// reachable. C's escape-blind `Suffix_ChooseToken` anchors this pattern on
/// `foo\*` — a whole-key match no suffix of `xfoo*bar` can satisfy — and
/// returns nothing.
#[test]
fn escaped_star_matches_a_literal_star_in_the_term() {
    let idx = indexed(&[b"xfoo*bar", b"xfoobar"]);
    assert_eq!(
        matches(&idx, br"*foo\**", NO_CAP),
        Some(vec![b"xfoo*bar".to_vec()])
    );
}

#[test]
fn question_mark_matches_single_char() {
    let idx = indexed(&[b"cat", b"cot", b"coat"]);
    // `c?t` matches only the exactly-3-char terms `c_t`, not `coat`.
    assert_eq!(
        matches(&idx, b"c?t", NO_CAP),
        Some(vec![b"cat".to_vec(), b"cot".to_vec()])
    );
}

#[test]
fn max_prefix_expansions_caps_results() {
    let idx = indexed(&[b"aa", b"ba", b"ca", b"da"]);
    // The cap is checked before each match is collected, so a cap of N yields
    // N + 1 entries.
    let pattern = SuffixWildcardPattern::new(b"*a").expect("valid token");
    let got = idx
        .suffix_expand(
            SuffixQuery::Wildcard {
                pattern: &pattern,
                max_prefix_expansions: 1,
            },
            None,
        )
        .count();
    assert_eq!(got, 2);
}

/// The cap truncates one suffix entry's members in registration order, so
/// `eat` — indexed after the longer terms it is a suffix of — falls outside a
/// cap of one. C's `_getWildcardArray` cuts the same array at the same place.
#[test]
fn max_prefix_expansions_cuts_members_in_registration_order() {
    let idx = indexed(&[b"beat", b"heat", b"eat"]);
    let pattern = SuffixWildcardPattern::new(b"*eat").expect("valid token");
    let got: Vec<Vec<u8>> = idx
        .suffix_expand(
            SuffixQuery::Wildcard {
                pattern: &pattern,
                max_prefix_expansions: 1,
            },
            None,
        )
        .map(|t| t.to_bytes().to_vec())
        .collect();

    assert_eq!(got, [b"beat".to_vec(), b"heat".to_vec()]);
}
