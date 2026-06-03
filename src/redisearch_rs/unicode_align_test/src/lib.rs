/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Test utility crate that probes libnu (RediSearch's C case-mapping
//! library) and compares it side-by-side with `icu_casemap` (the library
//! the Rust sorting vector uses).
//!
//! # Two missions, two modules
//!
//! - [`checks`] — libnu probes. Pure libnu interactions: encoder, decoder,
//!   fold/lower table lookups, length-prediction APIs, casemap-table walker.
//!   The `tests/check_*.rs` files consume these helpers to assert
//!   self-consistency invariants of libnu itself (encoder validity, decoder
//!   round-trip, length-prediction agreement, casemap terminator presence,
//!   fold idempotence). **Failures here are gating** — they signal a
//!   regression in `deps/libnu/`.
//!
//! - [`diff`] — libnu-vs-ICU comparison machinery. Pairs each libnu probe
//!   with its `icu_casemap` counterpart and aggregates the differences into
//!   a [`diff::Report`]. The `tests/diff_*.rs` files consume these helpers
//!   to characterise where the two libraries disagree (sharp s, final
//!   sigma, Turkish I, ligatures, ...). **Failures here are not gating** —
//!   the divergence is the subject under study, not a regression.
//!
//! The split is also visible at the filesystem level: every test file is
//! prefixed `check_` (gating) or `diff_` (reporting), so
//! `cargo test --test 'check_*'` and `--test 'diff_*'` select one mission
//! at a time.
//!
//! # Production call-site mapping
//!
//! The probes mirror the production C call shapes verbatim so that
//! self-checks here are meaningful for the binary that ships:
//!
//! - `unicode_tolower()` in `src/util/strconv.h` →
//!   [`checks::predict_lower_len`], [`checks::lower_libnu`],
//!   [`checks::predict_bytenlen`], [`checks::write_with_libnu`].
//! - `strToLowerRunes()` / `runesToStr()` /
//!   `strToSingleCodepointFoldedRunes()` in `src/trie/rune_util.c` → same
//!   probes plus [`checks::predict_strlen`], [`checks::fold_libnu`].
//! - The `try_insert_string_normalize()` path in
//!   `src/redisearch_rs/sorting_vector/src/lib.rs` →
//!   [`diff::fold_icu`] (the ICU side of the comparison).

pub mod checks;
pub mod diff;

mod libnu_ffi;
