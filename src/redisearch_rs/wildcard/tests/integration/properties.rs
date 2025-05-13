/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Proptests for [`WildcardPattern`]
//! Adapted from the [`wildcard` crate][wildcard]
//!
//! [wildcard]: https://github.com/cloudflare/wildcard/blob/c560ef01dda595d038e2f46b91cd5804fccb00e0/src/lib.rs#L1170-L1432
use crate::matches;
use proptest::{prelude::*, proptest};
use std::{fmt, ops::Range};
use wildcard::{Token, WildcardPattern};

#[derive(Clone)]
struct PatternAndKeys {
    pattern: Box<[u8]>,
    keys: Vec<Box<[u8]>>,
}

prop_compose! {
    fn pattern_and_keys(
        pat_len: Range<usize>,
        key_len: Range<usize>,
        num_keys: Range<usize>,
    )(
        pattern in pat_len.prop_flat_map(|len| proptest::string::string_regex(format!("([[:alpha:]]|[0-9]|\\*|\\?|\\\\){{{len}}}").as_str()).unwrap()),
        keys in any_with::<Vec<Box<[u8]>>>(
            (
                num_keys.into(),
                (key_len.into(), ()),
            )
        )
    ) -> PatternAndKeys {
            let pattern = pattern.into_bytes().into_boxed_slice();
            PatternAndKeys { pattern, keys }
    }
}

prop_compose! {
    #[expect(clippy::double_parens)]
    fn pattern_and_matching_keys(
        pat_len: Range<usize>,
        num_keys: Range<usize>,
    )(
        p_and_k in ((
            pat_len.prop_map(|pat_len| {
                proptest::string::string_regex(
                    format!("([[:alpha:]]|[0-9]|\\*|\\?|\\\\){{{pat_len}}}").as_str())
                        .unwrap()
            })
            .prop_flat_map(|pat| pat), num_keys)).prop_perturb(|(pat, num_keys), rng| {
            let pattern = pat.into_bytes().into_boxed_slice();
            let keys = generate_matching_keys(&pattern, num_keys, rng);
            PatternAndKeys { pattern, keys }
        }))
     -> PatternAndKeys {
         p_and_k
    }
}

fn generate_matching_keys(pattern: &[u8], num_keys: usize, rng: impl Rng) -> Vec<Box<[u8]>> {
    let rng = std::cell::RefCell::new(rng);
    let tokens = WildcardPattern::parse(pattern);
    let mut keys = Vec::new();
    let mut chars = std::iter::repeat_with(|| {
        proptest::char::select_char(
            &mut *rng.borrow_mut(),
            &[],
            &[],
            &[
                'a'..='z',
                'A'..='Z',
                '0'..='9',
                '*'..='*',
                '?'..='?',
                '\\'..='\\',
            ],
        ) as u8
    });
    for _ in 0..num_keys {
        let mut key = Vec::new();

        for token in tokens.tokens() {
            match token {
                Token::Any => {
                    let num_chars = rng.borrow_mut().gen_range(1..=10);
                    for _ in 0..num_chars {
                        key.push(chars.next().unwrap());
                    }
                }
                Token::One => {
                    key.push(chars.next().unwrap());
                }
                Token::Literal(c) => {
                    key.extend_from_slice(c);
                }
            }
        }
        keys.push(key.into_boxed_slice())
    }
    keys
}

impl fmt::Debug for PatternAndKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let pattern = String::from_utf8_lossy(&self.pattern);
        let keys = Vec::from_iter(self.keys.iter().map(|k| String::from_utf8_lossy(k)));
        f.debug_struct("PatternAndKeys")
            .field("pattern", &pattern)
            .field("keys", &keys)
            .finish()
    }
}

proptest! {
    #[test]
    fn sanity_check_random(input in pattern_and_keys(1..10, 0..10, 1..100)) {
        let Ok(wc_cf) = wildcard_cloudflare::Wildcard::new(&input.pattern) else {
            return Err(TestCaseError::Reject("Pattern rejected by reference implementation".into()));
        };

        let pattern = WildcardPattern::parse(&input.pattern);
        for key in input.keys {
            let outcome = pattern.matches(&key);
            if wc_cf.is_match(&key) {
                assert_eq!(outcome, wildcard::MatchOutcome::Match);
            } else {
                // In this case, our implementation may return
                // either `MatchOutcome::NoMatch` or `MatchOutcome::PartialMatch`.
                assert_ne!(outcome, wildcard::MatchOutcome::Match);
            }
        }
    }

    #[test]
    fn sanity_check_matching(input in pattern_and_matching_keys(5..20, 8..20)) {
        for key in input.keys {
            matches!(&input.pattern, [&key])
        }
    }
}
