/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use stopwords::{MAX_STOPWORDLIST_SIZE, StopWordList};

#[test]
fn normalization_examples() {
    for (name, input, stored) in [
        ("ascii", b"MiXeD".as_slice(), b"mixed".as_slice()),
        ("expansion", "İ".as_bytes(), "i\u{307}".as_bytes()),
        ("cyrillic", "НЕ".as_bytes(), "не".as_bytes()),
        ("embedded nul", b"AB\0CD", b"ab"),
        ("leading nul", b"\0UPPER", b"\0UPPER"),
        ("dangling sequence", b"AB\xe2", b"ab\xe2"),
        ("overlong ascii", b"\xc1\x81", b"a"),
        ("empty", b"", b""),
    ] {
        let list = StopWordList::new([input]);
        assert!(!list.is_empty(), "{name}");
        assert_eq!(list.iter().collect::<Vec<_>>(), [stored], "{name}");
        assert!(list.contains(input), "{name}");
    }
}

#[test]
fn membership_requires_a_whole_word() {
    let list = StopWordList::new([b"xy".as_slice()]);
    assert!(!list.contains(b"x"));
    assert!(!list.contains(b"xyz"));
}

#[test]
fn empty_list_contains_nothing() {
    let empty = StopWordList::new([]);
    assert!(empty.is_empty());
    assert!(!empty.contains(b""));
}

#[test]
fn input_limit_counts_duplicates() {
    let mut words =
        std::iter::repeat_n(b"same".as_slice(), MAX_STOPWORDLIST_SIZE).chain([b"extra".as_slice()]);
    let list = StopWordList::new(&mut words);
    assert_eq!(list.len(), 1);
    assert!(list.contains(b"same"));
    assert!(!list.contains(b"extra"));
    assert_eq!(words.next(), Some(b"extra".as_slice()));
}

#[test]
fn stored_words_are_not_normalized() {
    let list = StopWordList::from_stored([b"UPPER".as_slice(), b"a\0b"]);
    assert_eq!(
        list.iter().collect::<Vec<_>>(),
        [b"UPPER".to_vec(), b"a\0b".to_vec()]
    );
    assert!(!list.contains(b"UPPER"));
}

#[test]
fn key_lengths_use_low_sixteen_bits() {
    for length in [65535, 65536, 65537] {
        // A leading NUL leaves normalization unchanged without scanning a long word under Miri.
        let mut word = vec![b'x'; length];
        word[0] = 0;
        let list = StopWordList::new([word.as_slice()]);
        assert_eq!(list.iter().next().unwrap(), word[..length % 65536]);
        assert!(list.contains(&word));
    }
}

#[test]
fn default_words() {
    let list = StopWordList::default();
    let expected: Vec<_> = concat!(
        "a an and are as at be but by for if in into is it no not of on or such ",
        "that the their then there these they this to was will with",
    )
    .split_ascii_whitespace()
    .map(str::as_bytes)
    .collect();
    assert_eq!(list.len(), expected.len());
    assert_eq!(list.iter().collect::<Vec<_>>(), expected);
    for word in [b"THE".as_slice(), b"a", b"with"] {
        assert!(list.contains(word));
    }
    assert!(!list.contains(b"redis"));
}

#[cfg(not(miri))]
mod properties {
    use super::*;
    use proptest::prelude::*;
    use std::collections::BTreeSet;

    proptest! {
        #[test]
        fn membership_matches_normalized_set(
            words in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..32), 0..50),
            query in prop::collection::vec(any::<u8>(), 0..32),
        ) {
            let list = StopWordList::new(words.iter().map(Vec::as_slice));
            let expected: BTreeSet<_> = words.iter()
                .map(|word| string_utils::unicode::tolower_bytes(word).into_owned()).collect();
            for word in &words {
                prop_assert!(list.contains(word));
            }
            prop_assert_eq!(list.contains(&query), expected.contains(string_utils::unicode::tolower_bytes(&query).as_ref()));
        }

        #[test]
        fn construction_limits_mixed_duplicates(
            words in prop::collection::vec(b'A'..b'Q', MAX_STOPWORDLIST_SIZE - 2..=MAX_STOPWORDLIST_SIZE + 2),
        ) {
            let expected: BTreeSet<_> = words.iter().take(MAX_STOPWORDLIST_SIZE).map(|word| vec![word.to_ascii_lowercase()]).collect();
            let mut input = words.iter();
            let mut consumed = 0;
            let list = StopWordList::new(std::iter::from_fn(|| {
                input.next().map(|word| {
                    consumed += 1;
                    std::slice::from_ref(word)
                })
            }));
            prop_assert_eq!(consumed, words.len().min(MAX_STOPWORDLIST_SIZE));
            prop_assert_eq!(input.copied().collect::<Vec<_>>(), words.iter().skip(MAX_STOPWORDLIST_SIZE).copied().collect::<Vec<_>>());
            prop_assert_eq!(list.len(), expected.len());
            prop_assert_eq!(list.iter().collect::<BTreeSet<_>>(), expected);
        }

        #[test]
        fn iteration_matches_sorted_unique_words(words in prop::collection::vec(
            prop::collection::vec(any::<u8>(), 0..32), 0..50,
        )) {
            let list = StopWordList::from_stored(&words);
            let expected: BTreeSet<_> = words.into_iter().collect();
            prop_assert_eq!(list.len(), expected.len());
            prop_assert_eq!(list.iter().collect::<Vec<_>>(), expected.into_iter().collect::<Vec<_>>());
        }
    }
}
