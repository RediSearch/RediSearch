/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#![expect(clippy::filter_map_bool_then)]

//! Shared setup helpers for the [`ttl_table`] Criterion benches.

use std::num::NonZeroUsize;

use ffi::timespec;
use rand::{
    RngExt,
    distr::{Bernoulli, Distribution},
    rngs::ThreadRng,
    seq::{IteratorRandom, SliceRandom},
};
use rqe_core::DocId;
use ttl_table::{
    FieldExpiration, FieldExpirations, TimeToLiveTable,
    test_utils::{FAR_IN_THE_FUTURE, FUTURE, PAST},
};

// Some of the missing C symbols are actually Rust-provided.
redis_mock::mock_or_stub_missing_redis_c_symbols!();

fn random_around_mean<R: RngExt>(mean: usize, absolute_variance: usize, rng: &mut R) -> usize {
    let min = mean.saturating_sub(absolute_variance);
    let max = mean.saturating_add(absolute_variance);

    rng.random_range(min..=max)
}

#[derive(Clone, Copy)]
pub struct FieldExpirationInput {
    pub count_mean: usize,
    pub count_variation: usize,
    pub fill_probability: f32,
    pub expired_probability: f32,
    pub far_future_percentage: f32,
}

#[derive(Clone, Copy)]
pub struct DocsInput {
    pub count: usize,
    pub start_doc_id_from: DocId,
    pub fill_probability: f32,
    pub field_expiration_input: FieldExpirationInput,
}

pub fn create_field_expiration<R: RngExt>(
    _doc_id: DocId,
    input: &FieldExpirationInput,
    rng: &mut R,
) -> FieldExpirations {
    let real_count = random_around_mean(input.count_mean, input.count_variation, rng) as u16;
    // FieldExpiration array cannot be empty by design
    let real_count = real_count.max(1);

    let mut points: Vec<timespec> = Vec::new();
    points.resize(real_count as usize, PAST);
    let p = (real_count as f32 * input.expired_probability) as usize;
    let in_future =
        ((real_count as usize - p) as f32 * (1.0 - input.far_future_percentage)) as usize;

    points[0..p].fill(PAST);
    points[p..(p + in_future)].fill(FUTURE);
    points[(p + in_future)..].fill(FAR_IN_THE_FUTURE);
    points.shuffle(rng);
    points.shrink_to_fit();

    let pick = PickRandom::new(input.fill_probability);

    // Pre-size to the upper bound (`real_count`) so the `push` loop never
    // reallocates geometrically; the `should_keep` filter then leaves spare
    // capacity, which the `shrink_to_fit` below tightens into a `capacity == len`
    // block — matching the tight, doc-ordered arrays the C builder produces via
    // `staging.shrink_to_fit()` + `array_new_sz(len)`.
    let mut output = FieldExpirations::with_capacity(real_count as usize);
    for ((field_id, point), should_keep) in (0..real_count).zip(points).zip(pick.iter(rng)) {
        if should_keep {
            output.push(FieldExpiration {
                index: field_id,
                point,
            });
        }
    }
    // Tighten to a `capacity == len` block (the filter left spare capacity).
    output.shrink_to_fit();

    output
}

pub fn create_docs(input: DocsInput, mut rng: ThreadRng) -> Vec<(DocId, FieldExpirations)> {
    let pick = PickRandom::new(input.fill_probability);
    let mut docs: Vec<(DocId, FieldExpirations)> = (input.start_doc_id_from
        ..(input.start_doc_id_from + input.count as DocId))
        .filter_map(move |doc_id| {
            pick.should_keep(&mut rng).then(|| {
                (
                    doc_id,
                    create_field_expiration(doc_id, &input.field_expiration_input, &mut rng),
                )
            })
        })
        .filter(|(_, f)| !f.is_empty())
        .collect();
    // `filter_map`/`filter` report a `0` lower size hint, so `collect` over-allocates.
    // Shrink to capacity == len so the dataset is a tight block.
    docs.shrink_to_fit();
    docs
}

/// Create and populate a [`TimeToLiveTable`] with the given inputs.
///
/// Re-allocates every payload into a fresh, tight, doc-ordered block before
/// inserting, so the allocated memory is not sparse (important for benchmarks).
pub fn create_and_populate(
    max_size: NonZeroUsize,
    inputs: Vec<(DocId, FieldExpirations)>,
) -> TimeToLiveTable {
    let mut repacked: Vec<(DocId, FieldExpirations)> = Vec::with_capacity(inputs.len());
    for (doc_id, fields) in &inputs {
        repacked.push((*doc_id, fields.clone()));
    }
    drop(inputs);

    let mut t = TimeToLiveTable::new(max_size);
    for (doc_id, fields) in repacked {
        t.add(doc_id, fields);
    }
    t
}

/// Iterator that yield bools at specified probability
struct PickRandom(Bernoulli);

impl PickRandom {
    fn new(probability: f32) -> Self {
        let dist = Bernoulli::new(probability as f64).unwrap();
        Self(dist)
    }

    fn should_keep<R: RngExt>(&self, rng: &mut R) -> bool {
        self.0.sample(rng)
    }

    const fn iter<'s, 'rand, R: RngExt>(
        &'s self,
        rng: &'rand mut R,
    ) -> PickRandomIter<'s, 'rand, R> {
        PickRandomIter(self, rng)
    }
}

struct PickRandomIter<'pick, 'rand, R: RngExt>(&'pick PickRandom, &'rand mut R);

impl<'pick, 'rand, R: RngExt> Iterator for PickRandomIter<'pick, 'rand, R> {
    type Item = bool;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.0.should_keep(self.1))
    }
}

/// Draws a `u32` mask with popcount in `[1, 3]` and bit positions sampled
/// from `[0, 6)`. See the comment on the bench function for why these
/// bounds avoid every short-circuit in `verify_mask`.
pub fn random_mask(rng: &mut ThreadRng) -> u32 {
    let popcount = rng.random_range(1..=3usize);
    (0u32..6)
        .sample(rng, popcount)
        .into_iter()
        .fold(0u32, |acc, bit| acc | (1u32 << bit))
}
