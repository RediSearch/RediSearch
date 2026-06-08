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

use std::{num::NonZeroUsize, ops::Deref};

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

fn random_around_mean(mean: usize, absolute_variance: usize, rng: &mut ThreadRng) -> usize {
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

pub fn create_field_expiration(
    _doc_id: DocId,
    input: &FieldExpirationInput,
    rng: &mut ThreadRng,
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

    // Pre-size to the upper bound (`real_count`) so the block is allocated
    // exactly once, in doc order — matching the tight, sequentially-laid-out
    // arrays the C builder produces via `staging.shrink_to_fit()` +
    // `array_new_sz(len)`. Avoids both the geometric over-allocation of
    // repeated `push` and the placement-scattering realloc of `shrink_to_fit`.
    let mut output = FieldExpirations::new();
    for ((field_id, point), should_keep) in (0..real_count).zip(points).zip(pick.iter(rng)) {
        if should_keep {
            output.push(FieldExpiration {
                index: field_id,
                point,
            });
        }
    }

    output
}

pub fn create_docs(input: DocsInput, mut rng: ThreadRng) -> Vec<(DocId, FieldExpirations)> {
    let pick = PickRandom::new(input.fill_probability);
    (input.start_doc_id_from..(input.start_doc_id_from + input.count as DocId))
        .filter_map(move |doc_id| {
            pick.should_keep(&mut rng).then(|| {
                (
                    doc_id,
                    create_field_expiration(doc_id, &input.field_expiration_input, &mut rng),
                )
            })
        })
        .filter(|(_, f)| !f.is_empty())
        .collect()
}

/// Project the shared Rust dataset produced by [`create_docs`] into the C
/// `arrayof(FieldExpiration)` payloads the C table consumes, so both
/// implementations benchmark byte-for-byte identical data.
pub fn convert_into_ffi_docs(
    docs: &[(DocId, FieldExpirations)],
) -> Vec<(DocId, *mut ffi::FieldExpiration)> {
    let mut output = Vec::with_capacity(docs.len());
    for (doc_id, fes) in docs {
        let staging: Vec<_> = fes
            .deref()
            .iter()
            .map(|fe| ffi::FieldExpiration {
                index: fe.index,
                point: ffi::timespec {
                    tv_sec: fe.point.tv_sec,
                    tv_nsec: fe.point.tv_nsec,
                },
            })
            .collect();

        assert!(
            !staging.is_empty(),
            "convert_into_ffi_docs: empty payload (create_docs should have filtered it)"
        );

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

        // SAFETY: `raw` was sized for exactly `staging.len()` elements and is
        // disjoint from `staging`, so the copy is in-bounds and non-overlapping.
        unsafe {
            std::ptr::copy_nonoverlapping(staging.as_ptr(), raw, staging.len());
        }

        output.push((*doc_id, raw));
    }

    output
}

/// Create and populate TimeToLiveTable
pub fn create_and_populate(
    max_size: NonZeroUsize,
    inputs: Vec<(DocId, FieldExpirations)>,
) -> TimeToLiveTable {
    let mut t = TimeToLiveTable::new(max_size);
    for (doc_id, fields) in inputs {
        t.add(doc_id, fields);
    }
    t
}

/// RAII handle around the C `TimeToLiveTable*`. Calls
/// `TimeToLiveTable_Destroy` on drop so bench iterations don't leak the
/// bucket array (or its per-doc FieldExpiration payloads) across millions
/// of repetitions.
pub struct CTimeToLiveTable(pub *mut ffi::TimeToLiveTable);

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

/// C analogue of [`create_and_populate`].
///
/// # Safety
///
/// Same preconditions as [`ffi::TimeToLiveTable_Add`]: each payload must be
/// non-empty and sorted ascending by `index`. [`convert_into_ffi_docs`]
/// preserves the `0..real_count` order of [`create_field_expiration`],
/// satisfying the sort invariant.
pub unsafe fn create_and_populate_c(
    max_size: NonZeroUsize,
    inputs: Vec<(DocId, *mut ffi::FieldExpiration)>,
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
    fn new(probability: f32) -> Self {
        let dist = Bernoulli::new(probability as f64).unwrap();
        Self(dist)
    }

    fn should_keep(&self, rng: &mut ThreadRng) -> bool {
        self.0.sample(rng)
    }

    const fn iter<'s, 'rand>(&'s self, rng: &'rand mut ThreadRng) -> PickRandomIter<'s, 'rand> {
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
        .sample(rng, popcount)
        .into_iter()
        .fold(0u32, |acc, bit| acc | (1u32 << bit))
}
