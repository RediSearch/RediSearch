/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for the bench data builders.
//!
//! The benches report their corpus shape as parameters, so a builder that
//! quietly produced something else would make every comparison a lie. The RNG is
//! seeded rather than thread-local so the assertions below — several of which are
//! probabilistic in nature — have a fixed outcome instead of a small chance of
//! spurious failure.

use std::collections::HashSet;

use rand::{SeedableRng as _, rngs::StdRng};
use tag_index::SuffixQuery;
use tag_index_bencher::*;

const SEED: u64 = 42;

fn input(unique_tags: usize, tag_len_mean: usize) -> TagCorpusInput {
    TagCorpusInput {
        unique_tags,
        tag_len_mean,
        tag_len_variation: 2,
        shared_prefix_depth: 4,
        prefix_pool: 8,
        alphabet: 26,
    }
}

#[test]
fn corpus_holds_exactly_the_requested_number_of_distinct_tags() {
    let mut rng = StdRng::seed_from_u64(SEED);
    let corpus = TagCorpus::generate(input(500, 12), &mut rng);

    assert_eq!(corpus.len(), 500);
    let distinct: HashSet<&[u8]> = corpus.rust_tags().into_iter().collect();
    assert_eq!(distinct.len(), 500, "generated tags must be distinct");
}

#[test]
fn tags_respect_the_length_bounds_and_are_nul_free() {
    let mut rng = StdRng::seed_from_u64(SEED);
    let corpus = TagCorpus::generate(input(200, 12), &mut rng);

    for tag in corpus.rust_tags() {
        assert!(
            (10..=14).contains(&tag.len()),
            "tag length {} outside mean 12 ± 2",
            tag.len()
        );
        assert!(!tag.contains(&0), "tags must be NUL-free");
    }
}

#[test]
fn a_tag_is_always_longer_than_the_prefix_it_shares() {
    // A short mean against a long shared prefix: without the floor in
    // `generate`, the corpus would collapse onto `prefix_pool` values and the
    // distinctness retry would spin until it gave up.
    let mut rng = StdRng::seed_from_u64(SEED);
    let corpus = TagCorpus::generate(
        TagCorpusInput {
            unique_tags: 100,
            tag_len_mean: 2,
            tag_len_variation: 0,
            shared_prefix_depth: 6,
            prefix_pool: 4,
            alphabet: 26,
        },
        &mut rng,
    );

    assert_eq!(corpus.len(), 100);
    for tag in corpus.rust_tags() {
        assert!(tag.len() > 6, "tag {tag:?} is not longer than its prefix");
    }
}

#[test]
fn shared_prefix_depth_makes_tags_collide_near_the_root() {
    let mut rng = StdRng::seed_from_u64(SEED);
    let shared = TagCorpus::generate(input(200, 12), &mut rng);
    let distinct_prefixes: HashSet<&[u8]> = shared
        .rust_tags()
        .into_iter()
        .map(|tag| &tag[..4])
        .collect();
    assert!(
        distinct_prefixes.len() <= 8,
        "with prefix_pool=8 there can be at most 8 distinct 4-byte prefixes, got {}",
        distinct_prefixes.len()
    );

    let mut rng = StdRng::seed_from_u64(SEED);
    let unshared = TagCorpus::generate(
        TagCorpusInput {
            shared_prefix_depth: 0,
            ..input(200, 12)
        },
        &mut rng,
    );
    let distinct_prefixes: HashSet<&[u8]> = unshared
        .rust_tags()
        .into_iter()
        .map(|tag| &tag[..4])
        .collect();
    assert!(
        distinct_prefixes.len() > 8,
        "without prefix sharing the roots should fan out, got {} distinct prefixes",
        distinct_prefixes.len()
    );
}

#[test]
fn docs_carry_ascending_ids_and_in_range_tags() {
    let mut rng = StdRng::seed_from_u64(SEED);
    let corpus = TagCorpus::generate(input(50, 10), &mut rng);
    let docs = corpus.docs(
        DocsInput {
            count: 20,
            start_doc_id_from: 7,
            tags_per_doc_mean: 3,
            tags_per_doc_variation: 1,
        },
        &mut rng,
    );

    assert_eq!(docs.len(), 20);
    for (offset, doc) in docs.iter().enumerate() {
        assert_eq!(doc.doc_id, 7 + offset as u64);
        assert!(
            (2..=4).contains(&doc.tags.len()),
            "tags per doc {} outside mean 3 ± 1",
            doc.tags.len()
        );
        assert!(doc.tags.iter().all(|&i| i < corpus.len()));
    }
}

#[test]
fn both_projections_describe_the_same_documents() {
    let mut rng = StdRng::seed_from_u64(SEED);
    let corpus = TagCorpus::generate(input(50, 10), &mut rng);
    let docs = corpus.docs(
        DocsInput {
            count: 10,
            start_doc_id_from: 1,
            tags_per_doc_mean: 4,
            tags_per_doc_variation: 0,
        },
        &mut rng,
    );

    let rust_docs = corpus.rust_docs(&docs);
    let c_docs = corpus.c_docs(&docs);

    assert_eq!(rust_docs.len(), c_docs.len());
    for ((rust_id, rust_tags), (c_id, c_tags)) in rust_docs.iter().zip(&c_docs) {
        assert_eq!(rust_id, c_id);
        assert_eq!(rust_tags.len(), c_tags.len());
        for (rust_tag, c_tag) in rust_tags.iter().zip(c_tags) {
            // The C arm gets the very same allocation, one byte of which is the
            // terminator the Rust view excludes. Comparing addresses is the point:
            // equal bytes would not rule out the two arms having diverged onto
            // separate copies.
            assert_eq!(rust_tag.as_ptr() as usize, *c_tag as usize);
        }
    }
}

#[test]
fn a_selective_pattern_matches_no_more_than_a_broad_one() {
    let mut rng = StdRng::seed_from_u64(SEED);
    let corpus = TagCorpus::generate(input(2_000, 12), &mut rng);
    let mut index = build_rust(true);
    commit_rust(&mut index, &corpus.rust_tags());

    for mode in [ExpandMode::Suffix, ExpandMode::Contains] {
        let few = corpus.pattern_for(mode, Selectivity::Few);
        let many = corpus.pattern_for(mode, Selectivity::Many);

        let count = |pattern: &[u8]| {
            let query = match mode {
                ExpandMode::Suffix => SuffixQuery::Suffix(pattern),
                ExpandMode::Contains => SuffixQuery::Contains(pattern),
                ExpandMode::Wildcard => unreachable!("not exercised here"),
            };
            index.suffix_expand(query, None).count()
        };

        let few_count = count(&few);
        let many_count = count(&many);
        assert!(
            few_count > 0,
            "{mode:?}: a pattern carved out of the corpus must match something"
        );
        assert!(
            many_count >= few_count,
            "{mode:?}: the broad pattern matched {many_count}, the selective one {few_count}"
        );
    }
}

#[test]
fn a_wildcard_pattern_always_carries_an_anchor_token() {
    let mut rng = StdRng::seed_from_u64(SEED);
    let corpus = TagCorpus::generate(input(100, 12), &mut rng);

    for selectivity in [Selectivity::Few, Selectivity::Many] {
        let pattern = corpus.pattern_for(ExpandMode::Wildcard, selectivity);
        assert!(
            tag_index::SuffixWildcardPattern::new(&pattern).is_ok(),
            "pattern {pattern:?} has no anchor token"
        );
    }
}

#[cfg(not(miri))]
#[test]
fn both_implementations_index_the_same_tags() {
    let mut rng = StdRng::seed_from_u64(SEED);
    let corpus = TagCorpus::generate(input(300, 10), &mut rng);
    let docs = corpus.docs(
        DocsInput {
            count: 100,
            start_doc_id_from: 1,
            tags_per_doc_mean: 4,
            tags_per_doc_variation: 1,
        },
        &mut rng,
    );

    let rust_docs = corpus.rust_docs(&docs);
    let c_docs = corpus.c_docs(&docs);

    let rust_index = populate_rust(true, &rust_docs);
    // SAFETY: every pointer in `c_docs` addresses a NUL-terminated tag owned by
    // `corpus`, which outlives the index built here.
    let c_index = unsafe { populate_c(true, &c_docs) };

    let tags = corpus.rust_tags();
    let expected: HashSet<&[u8]> = docs
        .iter()
        .flat_map(|doc| doc.tags.iter().map(|&i| tags[i]))
        .collect();

    assert_eq!(rust_index.unique_values(), expected.len());
    // SAFETY: `c_index` is live and memory-mode, which is all this reader needs.
    let c_unique = unsafe { ffi::TagIndex_NUniqueValues(c_index.as_ptr()) };
    assert_eq!(c_unique, expected.len());

    // `c_index` drops here, exercising `TagIndex_Free`.
}

/// The affix benchmark is only meaningful if both arms enumerate the same terms.
/// C materialises them eagerly and Rust yields them lazily, so nothing in the
/// types stops the two from walking different amounts of the trie — which is
/// exactly what happened when the consumption was capped on one side only.
#[cfg(not(miri))]
#[test]
fn both_implementations_expand_an_affix_to_the_same_term_count() {
    let mut rng = StdRng::seed_from_u64(SEED);
    let corpus = TagCorpus::generate(input(500, 12), &mut rng);

    let mut rust_index = build_rust(true);
    commit_rust(&mut rust_index, &corpus.rust_tags());

    let c_index = build_c(true);
    let mut stats = zeroed_stats();
    // SAFETY: the tags are NUL-terminated and owned by `corpus`, which outlives
    // this call; `stats` is a live local.
    unsafe { commit_c(&c_index, &corpus.c_tags(), &mut stats) };

    for mode in [ExpandMode::Suffix, ExpandMode::Contains] {
        for selectivity in [Selectivity::Few, Selectivity::Many] {
            let pattern = corpus.pattern_for(mode, selectivity);

            let query = match mode {
                ExpandMode::Suffix => SuffixQuery::Suffix(&pattern),
                ExpandMode::Contains => SuffixQuery::Contains(&pattern),
                ExpandMode::Wildcard => unreachable!("capped differently; see below"),
            };
            let rust_count = rust_index.suffix_expand(query, None).count();

            // SAFETY: `c_index` is live, `pattern` outlives the call, and the
            // returned array is handed to `consume_suffix_matches`, which frees
            // it.
            let arr = unsafe {
                ffi::TagIndex_GetSuffixMatches(
                    c_index.as_ptr(),
                    pattern.as_ptr().cast(),
                    pattern.len() as u32,
                    matches!(mode, ExpandMode::Contains),
                    NO_TIMEOUT,
                    true,
                )
            };
            // SAFETY: `arr` is what the call just returned and is freed here.
            let c_count = unsafe { consume_suffix_matches(arr) };

            assert_eq!(
                rust_count, c_count,
                "{mode:?}/{selectivity:?}: Rust enumerated {rust_count} terms, C {c_count}"
            );
            assert!(rust_count > 0, "{mode:?}/{selectivity:?}: matched nothing");
        }
    }

    // The wildcard form caps during expansion on both sides, overshooting by one,
    // so only compare where the match set is comfortably under the cap.
    let pattern = corpus.pattern_for(ExpandMode::Wildcard, Selectivity::Few);
    let prepared = tag_index::SuffixWildcardPattern::new(&pattern).expect("anchor token");
    let rust_count = rust_index
        .suffix_expand(
            SuffixQuery::Wildcard {
                pattern: &prepared,
                max_prefix_expansions: MAX_PREFIX_EXPANSIONS as u64,
            },
            None,
        )
        .count();
    assert!(
        rust_count < MAX_PREFIX_EXPANSIONS,
        "pick a narrower pattern: {rust_count} matches is at the cap, where the \
         two implementations' overshoot rules diverge"
    );

    // SAFETY: `c_index` is live and `pattern` outlives the call.
    let arr = unsafe {
        ffi::TagIndex_GetSuffixWildcardMatches(
            c_index.as_ptr(),
            pattern.as_ptr().cast(),
            pattern.len() as u32,
            NO_TIMEOUT,
            MAX_PREFIX_EXPANSIONS as i64,
            true,
        )
    };
    assert_ne!(arr as usize, BAD_POINTER, "pattern has no anchor token");
    // SAFETY: `arr` is what the call just returned, is not the sentinel, and is
    // freed here.
    let c_count = unsafe { consume_wildcard_matches(arr) };
    assert_eq!(
        rust_count, c_count,
        "Wildcard: Rust enumerated {rust_count} terms, C {c_count}"
    );
}

#[cfg(not(miri))]
#[test]
fn a_freshly_built_c_index_is_empty_and_frees_cleanly() {
    let index = build_c(true);

    // SAFETY: `build_c` returned a live index.
    assert_eq!(unsafe { ffi::TagIndex_NUniqueValues(index.as_ptr()) }, 0);
    // SAFETY: as above.
    assert!(unsafe { ffi::TagIndex_HasSuffix(index.as_ptr()) });

    // `index` drops here, exercising `TagIndex_Free` on an empty index.
}
