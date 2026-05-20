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

// Some of the missing C symbols are actually Rust-provided.
extern crate redisearch_rs;
redis_mock::mock_or_stub_missing_redis_c_symbols!();

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

/// C analogue of [`create_field_expiration`]: same randomization plan,
/// emitted into an `arrayof(FieldExpiration)` allocated through `arr.h`.
///
/// The returned pointer is a fat-pointer head — `array_len`/`array_free`
/// from `arr.h` operate on it. Ownership transfers to the caller, who must
/// either pass it to [`ffi::TimeToLiveTable_Add`] (which adopts it) or call
/// [`ffi::array_free`].
pub fn create_field_expiration_c(
    _doc_id: t_docId,
    input: &FieldExpirationInput,
    rng: &mut ThreadRng,
) -> *mut ffi::FieldExpiration {
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

    let mut staging: Vec<ffi::FieldExpiration> = Vec::with_capacity(real_count as usize);
    for ((field_id, point), should_keep) in (0..real_count).zip(points).zip(pick.iter(rng)) {
        if should_keep {
            staging.push(ffi::FieldExpiration {
                index: field_id,
                point: ffi::timespec { tv_sec: point.tv_sec as _, tv_nsec: point.tv_nsec as _ },
            });
        }
    }

    // SAFETY: `array_new_sz` is FFI but otherwise pure — given valid args it
    // returns a freshly allocated buffer with `array_len(buf) == staging.len()`.
    let raw = unsafe {
        ffi::array_new_sz(
            std::mem::size_of::<ffi::FieldExpiration>() as u16,
            0,
            staging.len() as u32,
        )
    }
    .cast::<ffi::FieldExpiration>();
    if !staging.is_empty() {
        // SAFETY: `raw` was sized for exactly `staging.len()` elements and is
        // disjoint from `staging`, so the copy is in-bounds and non-overlapping.
        unsafe {
            std::ptr::copy_nonoverlapping(staging.as_ptr(), raw, staging.len());
        }
    }
    raw
}

/// C analogue of [`create_docs`]: same per-doc Bernoulli skip and doc-id
/// range, but each kept document carries an `arrayof(FieldExpiration)`
/// instead of a [`ThinVec`].
pub fn create_docs_c(
    input: DocsInput,
    mut rng: ThreadRng,
) -> Vec<(t_docId, *mut ffi::FieldExpiration)> {
    let pick = PickRandom::new(input.fill_probability);
    (input.start_doc_id_from..(input.start_doc_id_from + input.count as t_docId)).filter_map(
        move |doc_id| {
            pick.should_keep(&mut rng).then(|| {
                (
                    doc_id,
                    create_field_expiration_c(doc_id, &input.field_expiration_input, &mut rng),
                )
            })
        },
    ).collect()
}

/// RAII handle around the C `TimeToLiveTable*`. Calls
/// `TimeToLiveTable_Destroy` on drop so bench iterations don't leak the
/// bucket array (or its per-doc FieldExpiration payloads) across millions
/// of repetitions.
pub struct CTimeToLiveTable(*mut ffi::TimeToLiveTable);

impl CTimeToLiveTable {
    /// Borrow the raw pointer for passing to other `TimeToLiveTable_*`
    /// functions. The pointer is valid for as long as `self` is alive.
    pub const fn as_ptr(&self) -> *mut ffi::TimeToLiveTable {
        self.0
    }
}

impl Drop for CTimeToLiveTable {
    fn drop(&mut self) {
        // SAFETY: `self.0` was produced by `TimeToLiveTable_VerifyInit` in
        // `create_and_populate_c` and is not aliased; `Destroy` accepts
        // either a NULL slot or a live table and clears the slot.
        unsafe { ffi::TimeToLiveTable_Destroy(&mut self.0) };
    }
}

/// C analogue of [`create_and_populate`]: lazy-inits the C
/// `TimeToLiveTable` with `max_size` as the slot modulus and feeds every
/// input through `TimeToLiveTable_Add`.
///
/// # Safety
///
/// Same preconditions as [`ffi::TimeToLiveTable_Add`]: each payload must be
/// non-empty and sorted ascending by `index`. [`create_field_expiration_c`]
/// builds payloads in `0..real_count` order, satisfying the sort invariant.
pub unsafe fn create_and_populate_c(
    max_size: NonZeroUsize,
    inputs: Vec<(t_docId, *mut ffi::FieldExpiration)>,
) -> CTimeToLiveTable {
    let mut t: *mut ffi::TimeToLiveTable = std::ptr::null_mut();
    // SAFETY: `&mut t` points at a writable `*mut TimeToLiveTable`;
    // `TimeToLiveTable_VerifyInit` writes a freshly allocated table to it.
    unsafe {
        ffi::TimeToLiveTable_VerifyInit(&mut t, max_size.get());
    }
    for (doc_id, fields) in inputs {
        // SAFETY: caller's invariant — `fields` is non-empty, sorted by
        // index, and unique per `doc_id`. Ownership transfers to the table.
        unsafe {
            ffi::TimeToLiveTable_Add(t, doc_id, fields);
        }
    }
    CTimeToLiveTable(t)
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

    /// Returns the `array_len` reported by `arr.h` for a payload created
    /// by [`create_field_expiration_c`].
    fn c_array_len<T>(arr: *mut T) -> u32 {
        // SAFETY: `arr` points at an `array_t` allocated by `array_new_sz`,
        // so its `array_hdr_t` is intact.
        unsafe { ffi::array_len_func(arr.cast()) }
    }

    /// Reads element `i` of an `arrayof(FieldExpiration)` produced by
    /// `create_field_expiration_c`.
    fn c_array_read(arr: *mut ffi::FieldExpiration, i: usize) -> ffi::FieldExpiration {
        // SAFETY: caller guarantees `i < array_len(arr)`.
        unsafe { *arr.add(i) }
    }

    #[test]
    fn test_create_field_expiration_c() {
        let mut rng = rand::rng();

        // All items in FAR_IN_THE_FUTURE
        let output = create_field_expiration_c(
            0,
            &FieldExpirationInput { count_mean: 10, count_variation: 0, fill_probability: 1., expired_probability: 0., far_future_percentage: 1. },
            &mut rng,
        );
        assert_eq!(c_array_len(output), 10);
        for index in 0..10 {
            let field = c_array_read(output, index);
            assert_eq!(index as u16, field.index);
            assert_eq!(field.point.tv_sec as i64, FAR_IN_THE_FUTURE.tv_sec as i64);
            assert_eq!(field.point.tv_nsec as i64, FAR_IN_THE_FUTURE.tv_nsec as i64);
        }
        // SAFETY: allocated by `array_new_sz`, no other owner.
        unsafe { ffi::array_free(output.cast()) };

        // Count variation
        let output = create_field_expiration_c(
            0,
            &FieldExpirationInput { count_mean: 10, count_variation: 5, fill_probability: 1., expired_probability: 1., far_future_percentage: 0. },
            &mut rng,
        );
        let len = c_array_len(output);
        assert!((5..=15).contains(&len));
        unsafe { ffi::array_free(output.cast()) };

        // 0 is cast to 1
        let output = create_field_expiration_c(
            0,
            &FieldExpirationInput { count_mean: 0, count_variation: 0, fill_probability: 1., expired_probability: 0., far_future_percentage: 1. },
            &mut rng,
        );
        assert_eq!(c_array_len(output), 1);
        unsafe { ffi::array_free(output.cast()) };
    }

    #[test]
    fn test_create_docs_c() {
        let field_expiration_input = FieldExpirationInput { count_mean: 10, count_variation: 0, fill_probability: 1., expired_probability: 1., far_future_percentage: 0. };

        let output = create_docs_c(
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
            assert!(c_array_len(fields) > 0);
            // SAFETY: each `fields` was just produced by `create_field_expiration_c`.
            unsafe { ffi::array_free(fields.cast()) };
        }
    }

    #[test]
    fn test_create_and_populate_c() {
        // SAFETY: inputs is empty, so no array ownership is transferred.
        let table = unsafe { create_and_populate_c(
            NonZero::try_from(2).unwrap(),
            vec![],
        ) };

        // SAFETY: `VerifyInit` ran, so `as_ptr()` is non-null.
        assert!(unsafe { ffi::TimeToLiveTable_IsEmpty(table.as_ptr()) });
        // `table` drops here, exercising `TimeToLiveTable_Destroy`.
    }

    #[test]
    fn test_create_and_populate_c_with_entries() {
        let field_expiration_input = FieldExpirationInput { count_mean: 3, count_variation: 0, fill_probability: 1., expired_probability: 0., far_future_percentage: 1. };
        let inputs = create_docs_c(
            DocsInput {
                count: 4,
                start_doc_id_from: 1,
                field_expiration_input,
                fill_probability: 1.0,
            },
            rand::rng(),
        );

        // SAFETY: inputs come from `create_docs_c` with `fill_probability=1.0`,
        // so each payload is non-empty and sorted by index.
        let table = unsafe { create_and_populate_c(
            NonZero::try_from(16).unwrap(),
            inputs,
        ) };

        // SAFETY: table is live.
        assert!(!unsafe { ffi::TimeToLiveTable_IsEmpty(table.as_ptr()) });
        // `table` drops here, freeing the bucket array and the
        // per-doc FieldExpiration arrays adopted by `_Add`.
    }
}