/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for the public [`BuiltInScorer`] API.

use std::ffi::CString;

use query_types::scorers::{
    BuiltInScorer, DEFAULT_SCORER_NAME, UnknownScorer, slop_forces_offsets,
};
use strum::VariantArray;

#[test]
fn default_is_bm25_std() {
    assert_eq!(BuiltInScorer::default(), BuiltInScorer::Bm25Std);
    assert_eq!(BuiltInScorer::default().name(), DEFAULT_SCORER_NAME);
}

#[test]
fn from_str_round_trips_every_scorer() {
    for &scorer in BuiltInScorer::VARIANTS {
        assert_eq!(scorer.name().parse::<BuiltInScorer>(), Ok(scorer));
    }
}

#[test]
fn from_str_rejects_unknown_names() {
    // The rejected name is captured in the error so it can be logged.
    for name in ["", "MY_CUSTOM_SCORER", /* case-sensitive: */ "tfidf"] {
        assert_eq!(
            name.parse::<BuiltInScorer>(),
            Err(UnknownScorer { name: name.into() })
        );
    }
    // The captured name surfaces in the error message.
    let err = "MY_CUSTOM_SCORER".parse::<BuiltInScorer>().unwrap_err();
    assert!(err.to_string().contains("MY_CUSTOM_SCORER"));
}

#[test]
fn from_c_str_matches_from_str() {
    // Built-in names round-trip through the C-string path too.
    for &scorer in BuiltInScorer::VARIANTS {
        let c_name = CString::new(scorer.name()).unwrap();
        assert_eq!(BuiltInScorer::from_c_str(&c_name), Some(scorer));
    }

    // Custom and non-UTF-8 names are not built-ins, so they yield `None`.
    let custom = CString::new("MY_CUSTOM_SCORER").unwrap();
    assert_eq!(BuiltInScorer::from_c_str(&custom), None);
    let non_utf8 = CString::new([0xFF, 0xFE]).unwrap();
    assert_eq!(BuiltInScorer::from_c_str(&non_utf8), None);
}

#[test]
fn proximity_scorers_need_offsets() {
    for scorer in [
        BuiltInScorer::TfIdf,
        BuiltInScorer::TfIdfDocNorm,
        BuiltInScorer::Bm25,
    ] {
        assert!(scorer.needs_offsets(), "{scorer:?} should need offsets");
    }
}

#[test]
fn non_proximity_scorers_skip_offsets() {
    for scorer in [
        BuiltInScorer::DisMax,
        BuiltInScorer::DocScore,
        BuiltInScorer::Hamming,
        BuiltInScorer::Bm25Std,
        BuiltInScorer::Bm25StdTanh,
        BuiltInScorer::Bm25StdNorm,
    ] {
        assert!(!scorer.needs_offsets(), "{scorer:?} should skip offsets");
    }
}

#[test]
fn slop_or_order_forces_offsets() {
    // A non-negative `max_slop` or an `in_order` requirement forces offsets,
    // regardless of the scorer.
    assert!(slop_forces_offsets(0, 0));
    assert!(slop_forces_offsets(5, 0));
    assert!(slop_forces_offsets(-1, 1));
    assert!(slop_forces_offsets(3, 0));
}

#[test]
fn no_slop_or_order_does_not_force_offsets() {
    // With no phrase/slop constraint (`max_slop < 0`, `in_order == 0`) the
    // options impose nothing and the scorer alone decides.
    assert!(!slop_forces_offsets(-1, 0));
}
