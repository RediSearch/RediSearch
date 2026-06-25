/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Bounded storage for the COLLECT reducer, split along two axes. The SORTBY
//! axis is the [`Storage`] enum ([`Unranked`][Storage::Unranked] arrival order vs
//! [`Ranked`][Storage::Ranked] top-K); the orthogonal DISTINCT axis is a
//! `Plain` / `Distinct` variant *inside* each family, so callers branch only on
//! SORTBY and only the ranked path builds a sort-key snapshot.
//!
//! The DISTINCT variants dedup on the [`ProjectedRow`] itself — its
//! [`Hash`]/[`Eq`] are the identity.

use std::cmp::Reverse;
use std::hash::{Hash, Hasher};
use std::mem;

use indexmap::IndexSet;
use itertools::Either;
use min_max_heap::MinMaxHeap;
use priority_queue::PriorityQueue;
use rlookup::RLookupRow;
use value::SharedValue;
use value::comparison::compare_on_equality_only;
use value::hash::hash_value;

use super::heap::{HeapEntry, RankingKey};

/// `SORTBY` result count when no explicit `LIMIT` is given, matching the C
/// implementation's `DEFAULT_LIMIT`.
pub const DEFAULT_LIMIT: u64 = 10;

/// Caps only the up-front `with_capacity`, not the retained-row count, so a huge
/// `offset + count` doesn't force a huge initial allocation.
const INITIAL_CAPACITY_CAP: usize = 16_384;

/// The projected (collected) fields, in a type distinct from [`RankingKey`] so
/// the ranking axis (order) and the value axis (content, and the DISTINCT
/// identity) stay separate.
pub struct ProjectedRow(RLookupRow<'static>);

impl ProjectedRow {
    pub const fn new(row: RLookupRow<'static>) -> Self {
        Self(row)
    }

    pub const fn row(&self) -> &RLookupRow<'static> {
        &self.0
    }

    pub fn into_row(self) -> RLookupRow<'static> {
        self.0
    }

    /// The collected values with trailing `None` slots trimmed off.
    ///
    /// Trailing `None`s only reflect how far the highest-index written key
    /// reached, not content, so they carry no meaning for [`Hash`]/[`Eq`].
    fn effective_values(&self) -> &[Option<SharedValue>] {
        let slots = self.0.dyn_values();
        let end = slots.iter().rposition(Option::is_some).map_or(0, |i| i + 1);
        &slots[..end]
    }
}

/// Compares the trailing-trimmed slots (see [`ProjectedRow::effective_values`])
/// position-by-position with [`compare_on_equality_only`].
///
/// This can disagree with the [`Hash`] impl's [`hash_value`], which distinguishes
/// some values the comparator equates (num↔string, maps, NaN) — a pre-existing
/// comparator quirk, not addressed here.
impl PartialEq for ProjectedRow {
    fn eq(&self, other: &Self) -> bool {
        let (a, b) = (self.effective_values(), other.effective_values());
        a.len() == b.len()
            && a.iter().zip(b).all(|(x, y)| match (x, y) {
                (None, None) => true,
                (Some(p), Some(q)) => compare_on_equality_only(p, q),
                _ => false,
            })
    }
}

impl Eq for ProjectedRow {}

impl Hash for ProjectedRow {
    /// Hashes the trailing-trimmed slots (see [`ProjectedRow::effective_values`])
    /// via [`hash_value`]. A per-slot discriminant keeps an empty slot distinct
    /// from a present value; see the [`PartialEq`] impl for the eq/hash
    /// consistency caveat.
    fn hash<H: Hasher>(&self, state: &mut H) {
        for slot in self.effective_values() {
            mem::discriminant(slot).hash(state);
            if let Some(v) = slot {
                hash_value(v, state);
            }
        }
    }
}

/// `D` is the doc-id tie-breaker carried by the ranked family.
pub enum Storage<D: Ord> {
    Unranked(UnrankedStorage),
    Ranked(RankedStorage<D>),
}

impl<D: Ord> Storage<D> {
    pub fn new(sortby: bool, distinct: bool, limit: Option<(u64, u64)>, sort_asc_map: u64) -> Self {
        let (offset, count) = match (sortby, limit) {
            (_, Some((o, c))) => (o as usize, c as usize),
            // Without an explicit LIMIT the unranked path falls back to the
            // global cap, the ranked path to DEFAULT_LIMIT.
            // SAFETY: `RSGlobalConfig` is the module-global config initialised
            // once at load; we only read a single `usize` field.
            (false, None) => (0, unsafe { ffi::RSGlobalConfig.maxAggregateResults }),
            (true, None) => (0, DEFAULT_LIMIT as usize),
        };
        if sortby {
            Self::Ranked(RankedStorage::new(distinct, sort_asc_map, offset, count))
        } else {
            Self::Unranked(UnrankedStorage::new(distinct, offset, count))
        }
    }

    /// Yields the stored values best→worst (ranked) or in arrival order
    /// (unranked); the ranked arm's [`RankingKey`]s are discarded.
    pub fn drain(&mut self) -> impl ExactSizeIterator<Item = ProjectedRow> {
        match self {
            Self::Unranked(u) => Either::Left(u.drain()),
            Self::Ranked(r) => Either::Right(r.drain().map(HeapEntry::into_projected)),
        }
    }
}

/// Non-`SORTBY` path. Both variants retain at most `offset + count` rows;
/// [`Distinct`][Self::Distinct] dedups on the [`ProjectedRow`] identity, keeping
/// the first arrival. `IndexSet` (over `HashSet`) preserves arrival order without
/// a second buffer, since [`ProjectedRow`] is not `Clone`.
pub enum UnrankedStorage {
    Plain {
        buf: Vec<ProjectedRow>,
        offset: usize,
        count: usize,
    },
    Distinct {
        set: IndexSet<ProjectedRow>,
        offset: usize,
        count: usize,
    },
}

impl UnrankedStorage {
    fn new(distinct: bool, offset: usize, count: usize) -> Self {
        let initial_capacity = offset.saturating_add(count).min(INITIAL_CAPACITY_CAP);
        if distinct {
            Self::Distinct {
                set: IndexSet::with_capacity(initial_capacity),
                offset,
                count,
            }
        } else {
            Self::Plain {
                buf: Vec::with_capacity(initial_capacity),
                offset,
                count,
            }
        }
    }

    /// `Plain` projects only retained rows; `Distinct` projects any row below the
    /// cap (the identity needs the materialised row), then inserts as a no-op if
    /// it's a duplicate.
    pub fn push(&mut self, project: impl FnOnce() -> ProjectedRow) {
        match self {
            Self::Plain { buf, offset, count } => {
                if buf.len() < offset.saturating_add(*count) {
                    buf.push(project());
                }
            }
            Self::Distinct { set, offset, count } => {
                if set.len() < offset.saturating_add(*count) {
                    set.insert(project());
                }
            }
        }
    }

    pub fn drain(&mut self) -> impl ExactSizeIterator<Item = ProjectedRow> {
        match self {
            Self::Plain { buf, offset, count } => {
                Either::Left(std::mem::take(buf).into_iter().skip(*offset).take(*count))
            }
            Self::Distinct { set, offset, count } => {
                Either::Right(std::mem::take(set).into_iter().skip(*offset).take(*count))
            }
        }
    }
}

/// `SORTBY` path, top-K by [`RankingKey`], draining best→worst.
///
/// [`Plain`][Self::Plain] defers projection: a candidate is compared against the
/// worst survivor before `project` runs. [`Distinct`][Self::Distinct] keys a
/// [`PriorityQueue`] on the [`ProjectedRow`] (so identical rows collapse), with a
/// [`Reverse`]'d [`RankingKey`] as priority so the queue's single exposed end is
/// the *worst* survivor — the one evicted to stay bounded. It cannot defer
/// projection (the row keys the queue).
pub enum RankedStorage<D: Ord> {
    Plain {
        heap: MinMaxHeap<HeapEntry<D, ProjectedRow>>,
        sort_asc_map: u64,
        offset: usize,
        count: usize,
    },
    Distinct {
        pq: PriorityQueue<ProjectedRow, Reverse<RankingKey<D>>>,
        sort_asc_map: u64,
        offset: usize,
        count: usize,
    },
}

impl<D: Ord> RankedStorage<D> {
    fn new(distinct: bool, sort_asc_map: u64, offset: usize, count: usize) -> Self {
        let initial_capacity = offset.saturating_add(count).min(INITIAL_CAPACITY_CAP);
        if distinct {
            Self::Distinct {
                pq: PriorityQueue::with_capacity(initial_capacity),
                sort_asc_map,
                offset,
                count,
            }
        } else {
            Self::Plain {
                heap: MinMaxHeap::with_capacity(initial_capacity),
                sort_asc_map,
                offset,
                count,
            }
        }
    }

    /// `doc_id` breaks ties when sort keys compare equal (see [`RankingKey`]).
    pub fn consider(
        &mut self,
        sort_vals: Box<[Option<SharedValue>]>,
        doc_id: D,
        project: impl FnOnce() -> ProjectedRow,
    ) {
        match self {
            Self::Plain {
                heap,
                sort_asc_map,
                offset,
                count,
            } => {
                let max_size = offset.saturating_add(*count);
                if max_size == 0 {
                    return;
                }
                let cand_key = RankingKey::new(sort_vals, *sort_asc_map, doc_id);
                if heap.len() < max_size {
                    heap.push(HeapEntry::new(cand_key, project()));
                } else {
                    // `peek_min` is the worst survivor (best = max, see
                    // `super::heap`); a full heap with `max_size > 0` is non-empty.
                    let worst = heap.peek_min().expect("heap at cap is non-empty");
                    if cand_key > *worst.key() {
                        heap.push_pop_min(HeapEntry::new(cand_key, project()));
                    }
                }
            }
            Self::Distinct {
                pq,
                sort_asc_map,
                offset,
                count,
            } => {
                let max_size = offset.saturating_add(*count);
                if max_size == 0 {
                    return;
                }
                let ranking_key = RankingKey::new(sort_vals, *sort_asc_map, doc_id);
                let row = project();
                // Priority is `Reverse<RankingKey>`, so `push_decrease` keeps the
                // better (higher) `RankingKey` per identity, and the queue's max —
                // what `pop` removes — is the worst survivor. `push_decrease`
                // returns `None` only on a *new* identity, the sole case that can
                // exceed the cap.
                if pq.push_decrease(row, Reverse(ranking_key)).is_none() && pq.len() > max_size {
                    pq.pop();
                }
            }
        }
    }

    /// Each [`HeapEntry`] keeps its [`RankingKey`] so the remote reducer can read
    /// [`RankingKey::sort_vals`] (via [`HeapEntry::into_parts`]) to rebuild the
    /// wire row; the local reducer just takes [`HeapEntry::into_projected`].
    pub fn drain(&mut self) -> impl ExactSizeIterator<Item = HeapEntry<D, ProjectedRow>> {
        match self {
            Self::Plain {
                heap,
                offset,
                count,
                ..
            } => std::mem::take(heap)
                .into_vec_desc()
                .into_iter()
                .skip(*offset)
                .take(*count),
            Self::Distinct {
                pq, offset, count, ..
            } => {
                // `into_sorted_iter` yields highest-priority first; with a
                // `Reverse<RankingKey>` priority that is worst→best, so reverse it.
                let mut best_first: Vec<HeapEntry<D, ProjectedRow>> = std::mem::take(pq)
                    .into_sorted_iter()
                    .map(|(row, Reverse(ranking_key))| HeapEntry::new(ranking_key, row))
                    .collect();
                best_first.reverse();
                best_first.into_iter().skip(*offset).take(*count)
            }
        }
    }
}
