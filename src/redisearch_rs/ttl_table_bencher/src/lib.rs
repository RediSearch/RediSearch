/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Shared setup helpers for the [`ttl_table`] Criterion benches.

use std::num::NonZeroUsize;

use ffi::t_docId;
use libc::timespec;
use rand::{Rng, distr::{Bernoulli, Distribution}, rngs::ThreadRng, seq::{IteratorRandom, SliceRandom}};
use thin_vec::ThinVec;
use ttl_table::{FieldExpiration, TimeToLiveTable, test_utils::{FAR_IN_THE_FUTURE, FUTURE, PAST}};

fn random_around_mean(
    mean: usize,
    absolute_variance: usize,
    rng: &mut ThreadRng,
) -> usize {
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
    pub start_doc_id_from: t_docId,
    pub fill_probability: f32,
    pub field_expiration_input: FieldExpirationInput,
}

pub fn create_field_expiration(
    _doc_id: t_docId,
    input: &FieldExpirationInput,
    rng: &mut ThreadRng,
) -> ThinVec<FieldExpiration> {
    let real_count = random_around_mean(input.count_mean, input.count_variation, rng) as u16;
    // FieldExpiration array cannot be empty by design
    let real_count = real_count.max(1);

    let mut points: Vec<timespec> = Vec::with_capacity(real_count as usize);
    points.resize(real_count as usize, PAST);
    let p = (real_count as f32 * input.expired_probability) as usize;
    let in_future = ((real_count as usize - p) as f32 * (1.0 - input.far_future_percentage)) as usize;

    points[0..p].fill(PAST);
    points[p..(p + in_future)].fill(FUTURE);
    points[(p + in_future)..].fill(FAR_IN_THE_FUTURE);
    points.shuffle(rng);

    let pick = PickRandom::new(input.fill_probability);

    let mut output = ThinVec::with_capacity(real_count as usize);
    for ((field_id, point), should_keep) in (0..real_count).zip(points).zip(pick.iter(rng)) {
        if should_keep {
            output.push(FieldExpiration { index: field_id, point });
        }
    }

    output
}

pub fn create_docs(
    input: DocsInput,
    mut rng: ThreadRng,
) -> Vec<(t_docId, ThinVec<ttl_table::FieldExpiration>)> {
    let pick = PickRandom::new(input.fill_probability);
    (input.start_doc_id_from..(input.start_doc_id_from + input.count as t_docId)).filter_map(
        move |doc_id| {
            pick.should_keep(&mut rng).then(|| {
                (
                    doc_id,
                    create_field_expiration(doc_id, &input.field_expiration_input, &mut rng),
                )
            })
        },
    ).collect()
}

/// Create and populate TimeToLiveTable
/// 
/// # Safety
/// 
/// The same of `TimeToLiveTable::add`
pub unsafe fn create_and_populate(
    max_size: NonZeroUsize,
    inputs: Vec<(t_docId, ThinVec<FieldExpiration>)>,
) -> TimeToLiveTable {
    let mut t = TimeToLiveTable::new(max_size);
    for (doc_id, fields) in inputs {
        unsafe { t.add(doc_id, fields) };
    }
    t
}

/// Iterator that yield bools at specified probability
struct PickRandom(Bernoulli);

impl PickRandom {
    fn new(
        probability: f32,
    ) -> Self {
        let dist = Bernoulli::new(probability as f64).unwrap();
        Self(dist)
    }

    fn should_keep(&self, rng: &mut ThreadRng) -> bool {
        self.0.sample(rng)
    }

    fn iter<'s, 'rand>(&'s self, rng: &'rand mut ThreadRng) -> PickRandomIter<'s, 'rand> {
        PickRandomIter(self, rng)
    }
}

struct PickRandomIter<'pick, 'rand>(&'pick PickRandom, &'rand mut ThreadRng);

impl<'pick, 'rand> Iterator for PickRandomIter<'pick, 'rand> {
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
        .choose_multiple(rng, popcount)
        .into_iter()
        .fold(0u32, |acc, bit| acc | (1u32 << bit))
}


#[cfg(test)]
mod tests {
    use std::num::NonZero;

    use super::*;

    #[test]
    fn test_create_field_expiration() {
        let mut rng = rand::rng();

        // All items in FAR_IN_THE_FUTURE
        let output = create_field_expiration(
            0,
            &FieldExpirationInput { count_mean: 10, count_variation: 0, fill_probability: 1., expired_probability: 0., far_future_percentage: 1. },
            &mut rng,
        );
        assert_eq!(output.len(), 10);
        for (index, field) in output.into_iter().enumerate() {
            assert_eq!(index as u16, field.index);
            assert_eq!(field.point, FAR_IN_THE_FUTURE);
        }

        // All items in FUTURE
        let output = create_field_expiration(
            0,
            &FieldExpirationInput { count_mean: 10, count_variation: 0, fill_probability: 1., expired_probability: 0., far_future_percentage: 0. },
            &mut rng,
        );
        assert_eq!(output.len(), 10);
        for (index, field) in output.into_iter().enumerate() {
            assert_eq!(index as u16, field.index);
            assert_eq!(field.point, FUTURE);
        }

        // All items in PAST
        let output = create_field_expiration(
            0,
            &FieldExpirationInput { count_mean: 10, count_variation: 0, fill_probability: 1., expired_probability: 1., far_future_percentage: 0. },
            &mut rng,
        );
        assert_eq!(output.len(), 10);
        for (index, field) in output.into_iter().enumerate() {
            assert_eq!(index as u16, field.index);
            assert_eq!(field.point, PAST);
        }

        // Not all are present
        let output = create_field_expiration(
            0,
            &FieldExpirationInput { count_mean: 10, count_variation: 0, fill_probability: 0.5, expired_probability: 1., far_future_percentage: 0. },
            &mut rng,
        );
        assert_ne!(output.len(), 10);

        // Count variation
        let output = create_field_expiration(
            0,
            &FieldExpirationInput { count_mean: 10, count_variation: 5, fill_probability: 1., expired_probability: 1., far_future_percentage: 0. },
            &mut rng,
        );
        assert!(output.len() <= 15 && output.len() >= 5);

        // 0 is cast to 1
        let output = create_field_expiration(
            0,
            &FieldExpirationInput { count_mean: 0, count_variation: 0, fill_probability: 1., expired_probability: 0., far_future_percentage: 1. },
            &mut rng,
        );
        assert_eq!(output.len(), 1);
    }

    #[test]
    fn test_create_docs() {
        let field_expiration_input = FieldExpirationInput { count_mean: 10, count_variation: 0, fill_probability: 1., expired_probability: 1., far_future_percentage: 0. };

        let output: Vec<_> = create_docs(
            DocsInput {
                count: 10,
                start_doc_id_from: 0,
                field_expiration_input,
                fill_probability: 1.0,
            },
            rand::rng(),
        );
        assert_eq!(output.len(), 10);
        for i in 0..10 {
            assert_eq!(output[i].0, i as t_docId);
            assert!(!output[i].1.is_empty());
        }

        let output: Vec<_> = create_docs(
            DocsInput {
                count: 10,
                start_doc_id_from: 5,
                field_expiration_input,
                fill_probability: 1.0,
            },
            rand::rng(),
        );
        assert_eq!(output.len(), 10);
        for (i, (doc_id, fields)) in (5..15).zip(output) {
            assert_eq!(doc_id, i as t_docId);
            assert!(!fields.is_empty());
        }
    }


    #[test]
    fn test_create_and_populate() {
        // SAFETY: inputs is empty
        let table = unsafe { create_and_populate(
            NonZero::try_from(2).unwrap(),
            vec![],
        ) };

        assert!(table.is_empty());
    }
}