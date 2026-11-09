/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The set of gone documents a GC cycle scans against, for both policies.

use std::ops::RangeInclusive;

use index_spec::IndexSpecReadGuard;
use inverted_index::DocLiveness;
use roaring::RoaringTreemap;
use rqe_core::DocId;

/// The most bytes a [`GoneIds::Bitset`] spends; a wider window keeps its set.
pub const MAX_BITMAP_BYTES: usize = 16 << 20;

/// How many times the size of a forked child's live IDs their complement may take
/// and still be kept instead, for its faster checks.
const DEAD_SET_SIZE_FACTOR: usize = 2;

/// The IDs one roaring container spans.
const CONTAINER_IDS: DocId = 1 << 16;

/// About what a roaring container holding one run costs, its key and store
/// included: the least a complement spends on each container of its window.
const EMPTY_CONTAINER_BYTES: u64 = 32;

/// The documents gone when a cycle began, as the cycle's only source of liveness.
///
/// A document's ID is never reused, and an update gives the document a new one, so
/// the IDs that matter sit in a window that moves up over time. Each policy starts
/// from the set it has: an in-process cycle from the IDs its doc table recorded as
/// deleted, a forked child from the live IDs of its frozen table. A window that
/// fits [`MAX_BITMAP_BYTES`] becomes a [`GoneIds::Bitset`], where a check is one
/// load; a wider one keeps its set, a few dependent searches per check but sized
/// by its contents.
///
/// An ID above the window is live in every form: in a forked child the table holds
/// nothing newer, and an in-process cycle leaves documents deleted after it began
/// for the next one.
#[derive(Debug)]
pub enum GoneIds {
    /// Gone IDs listed one by one: the IDs deleted when an in-process cycle began,
    /// or a forked child's complement of its live IDs when that is no bigger.
    DeadSet(DeadSet),
    /// A forked child's live IDs, sized by the index rather than by how far its IDs
    /// have run.
    LiveSet(LiveSet),
    /// One bit per ID of a window, set when the ID is gone.
    Bitset(Bitset),
}

/// The gone IDs of a [`GoneIds::DeadSet`].
#[derive(Debug)]
pub struct DeadSet {
    /// Every gone ID at or above `dead_below`.
    dead: RoaringTreemap,
    /// Every ID below this is gone: a forked child's dead prefix, which a roaring set
    /// would spend a container per 2^16 IDs on. 0 for an in-process cycle.
    dead_below: DocId,
}

/// The live IDs of a [`GoneIds::LiveSet`].
#[derive(Debug)]
pub struct LiveSet {
    live: RoaringTreemap,
    /// The table's largest ID: every ID up to it that `live` lacks is gone.
    max: DocId,
}

/// What the IDs below a [`Bitset`]'s window are.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BelowWindow {
    /// An in-process cycle's IDs below its first deleted one.
    Live,
    /// A forked child's IDs below its first live one.
    Gone,
}

/// The window of a [`GoneIds::Bitset`].
#[derive(Debug)]
pub struct Bitset {
    /// The ID of bit 0 of `words`.
    first: DocId,
    words: Box<[u64]>,
    below: BelowWindow,
}

impl GoneIds {
    /// The documents in `dead`, as an in-process cycle copies them from the doc
    /// table's deleted set.
    pub fn from_set(dead: RoaringTreemap) -> Self {
        Self::from_set_with_cap(dead, MAX_BITMAP_BYTES)
    }

    /// Like [`GoneIds::from_set`], with bits only for a window of at most
    /// `max_bitmap_bytes`.
    pub fn from_set_with_cap(dead: RoaringTreemap, max_bitmap_bytes: usize) -> Self {
        let set = |dead| {
            Self::DeadSet(DeadSet {
                dead,
                dead_below: 0,
            })
        };
        let (Some(first), Some(last)) = (dead.min(), dead.max()) else {
            return set(dead);
        };
        let Some(len) = window_words(first, last, max_bitmap_bytes) else {
            return set(dead);
        };
        let mut words = vec![0u64; len].into_boxed_slice();
        for id in &dead {
            let bit = id - first;
            words[(bit / 64) as usize] |= 1 << (bit % 64);
        }
        Self::Bitset(Bitset {
            first,
            words,
            below: BelowWindow::Live,
        })
    }

    /// The documents a forked child's frozen table does not hold.
    pub fn of_table(spec: &IndexSpecReadGuard) -> Self {
        Self::of_table_with_cap(spec, MAX_BITMAP_BYTES)
    }

    /// Like [`GoneIds::of_table`], with bits only for a window of at most
    /// `max_bitmap_bytes`.
    pub fn of_table_with_cap(spec: &IndexSpecReadGuard, max_bitmap_bytes: usize) -> Self {
        let mut live = RoaringTreemap::new();
        spec.for_each_live_doc(|id| {
            live.insert(id);
        });
        Self::from_live_set(live, spec.doc_table().maxDocId, max_bitmap_bytes)
    }

    /// The gone IDs of a table whose live IDs are `live`, none of them above
    /// `max_doc_id`: every other ID up to `max_doc_id`.
    pub fn from_live_set(live: RoaringTreemap, max_doc_id: DocId, max_bitmap_bytes: usize) -> Self {
        debug_assert!(live.max().is_none_or(|last| last <= max_doc_id));
        // Starting on a word boundary is harmless: every ID below the first live
        // one is gone either way. With nothing live the window is empty, and every
        // ID up to the table's maximum is below it.
        let first = live
            .min()
            .map_or(max_doc_id.saturating_add(1), |id| id / 64 * 64);
        let Some(len) = window_words(first, max_doc_id, max_bitmap_bytes) else {
            return Self::from_wide_live_set(live, max_doc_id);
        };
        let mut words = vec![!0u64; len].into_boxed_slice();
        for id in &live {
            let bit = id - first;
            words[(bit / 64) as usize] &= !(1 << (bit % 64));
        }
        // The table holds no ID above its maximum, so those bits stay live.
        let past_max = (max_doc_id % 64) + 1;
        if past_max < 64
            && let Some(word) = words.last_mut()
        {
            *word &= (1 << past_max) - 1;
        }
        Self::Bitset(Bitset {
            first,
            words,
            below: BelowWindow::Gone,
        })
    }

    /// [`GoneIds::from_live_set`] for a window too wide for bits: the complement of
    /// `live` from its first ID to `max_doc_id`, unless that is more than
    /// [`DEAD_SET_SIZE_FACTOR`] times the size of `live`, in which case `live`.
    ///
    /// The complement is preferred at a size near `live`'s because its containers are
    /// the denser ones: a container past 4096 IDs is a bitmap, checked with one load,
    /// where a sparser one is a sorted array, checked with a binary search.
    fn from_wide_live_set(mut live: RoaringTreemap, max_doc_id: DocId) -> Self {
        let first_live = live
            .min()
            .expect("a window too wide for bits holds a live ID");
        // Documents added in sequence and never deleted leave long live stretches,
        // which runs hold far smaller than arrays or bitmaps.
        live.optimize();
        let live_bytes = live.serialized_size();
        // Each container of the window costs the complement one even when it holds no
        // live ID, so a window too wide for that cannot beat `live` and is not built.
        let containers = (max_doc_id - first_live) / CONTAINER_IDS + 1;
        let budget = live_bytes.saturating_mul(DEAD_SET_SIZE_FACTOR);
        if containers.saturating_mul(EMPTY_CONTAINER_BYTES) <= budget as u64 {
            let mut dead = RoaringTreemap::new();
            dead.insert_range(first_live..=max_doc_id);
            dead -= &live;
            // Subtracting from a range leaves each container a list of runs, which a
            // check searches; this turns a mixed one back into a bitmap, and leaves a
            // stretch with no live ID the single run it is.
            dead.optimize();
            if dead.serialized_size() <= budget {
                return Self::DeadSet(DeadSet {
                    dead,
                    dead_below: first_live,
                });
            }
        }
        Self::LiveSet(LiveSet {
            live,
            max: max_doc_id,
        })
    }

    /// Every gone ID, for an in-process cycle to prune what it collected.
    ///
    /// Only an in-process cycle calls this, on what [`GoneIds::from_set`] built; a
    /// forked child's dead prefix, or a [`GoneIds::LiveSet`]'s complement, would come
    /// back as ranges up to the table's maximum.
    pub fn into_set(self) -> RoaringTreemap {
        match self {
            Self::DeadSet(set) => {
                let mut dead = set.dead;
                if set.dead_below > 0 {
                    dead.insert_range(..set.dead_below);
                }
                dead
            }
            Self::LiveSet(set) => {
                let mut dead = RoaringTreemap::new();
                dead.insert_range(..=set.max);
                dead -= set.live;
                dead
            }
            Self::Bitset(bits) => {
                let mut dead = RoaringTreemap::new();
                if bits.below == BelowWindow::Gone {
                    dead.insert_range(..bits.first);
                }
                for (i, &word) in bits.words.iter().enumerate() {
                    let mut set = word;
                    while set != 0 {
                        dead.insert(
                            bits.first + i as DocId * 64 + DocId::from(set.trailing_zeros()),
                        );
                        set &= set - 1;
                    }
                }
                dead
            }
        }
    }

    /// Add the gone IDs in `range` to `into`, at a cost set by how many there are
    /// rather than by the width of `range`. Only an in-process cycle calls this; a
    /// [`GoneIds::LiveSet`] adds the range less its live IDs.
    pub fn insert_gone_in(&self, range: RangeInclusive<DocId>, into: &mut RoaringTreemap) {
        let (first, last) = range.into_inner();
        if first > last {
            return;
        }
        match self {
            Self::DeadSet(set) => {
                if first < set.dead_below {
                    into.insert_range(first..=last.min(set.dead_below - 1));
                }
                let mut ids = set.dead.iter();
                ids.advance_to(first);
                into.extend(ids.take_while(|&id| id <= last));
            }
            Self::LiveSet(set) => {
                let last = last.min(set.max);
                if first > last {
                    return;
                }
                let mut dead = RoaringTreemap::new();
                dead.insert_range(first..=last);
                dead -= &set.live;
                *into |= dead;
            }
            Self::Bitset(bits) => {
                if bits.below == BelowWindow::Gone && first < bits.first {
                    into.insert_range(first..=last.min(bits.first - 1));
                }
                let Some(hi) = last.checked_sub(bits.first) else {
                    return;
                };
                let lo = first.saturating_sub(bits.first);
                let Some(last_word) = (bits.words.len() as DocId).checked_sub(1) else {
                    return;
                };
                for w in lo / 64..=(hi / 64).min(last_word) {
                    let mut set = bits.words[w as usize];
                    while set != 0 {
                        let bit = w * 64 + DocId::from(set.trailing_zeros());
                        if (lo..=hi).contains(&bit) {
                            into.insert(bits.first + bit);
                        }
                        set &= set - 1;
                    }
                }
            }
        }
    }
}

/// The words a bitmap from `first` to `last` takes, when it fits `max_bytes`.
fn window_words(first: DocId, last: DocId, max_bytes: usize) -> Option<usize> {
    let Some(span) = last.checked_sub(first) else {
        return Some(0);
    };
    let words = span / 64 + 1;
    (words * 8 <= max_bytes as u64).then_some(words as usize)
}

impl Bitset {
    #[inline]
    fn gone(&self, id: DocId) -> bool {
        let Some(bit) = id.checked_sub(self.first) else {
            return self.below == BelowWindow::Gone;
        };
        self.words
            .get((bit / 64) as usize)
            .is_some_and(|word| word >> (bit % 64) & 1 != 0)
    }

    fn any_gone(&self, first: DocId, last: DocId) -> bool {
        if first < self.first && self.below == BelowWindow::Gone {
            return true;
        }
        let Some(window_last) = (self.words.len() as DocId * 64).checked_sub(1) else {
            return false;
        };
        let lo = first.max(self.first) - self.first;
        let Some(hi) = last.checked_sub(self.first) else {
            // Wholly below the window, where IDs are live (`Gone` returned above).
            return false;
        };
        let hi = hi.min(window_last);
        if lo > hi {
            // Wholly above the window, where no ID is gone.
            return false;
        }
        let (lo_word, hi_word) = ((lo / 64) as usize, (hi / 64) as usize);
        let lo_mask = !0u64 << (lo % 64);
        let hi_mask = !0u64 >> (63 - hi % 64);
        if lo_word == hi_word {
            return self.words[lo_word] & lo_mask & hi_mask != 0;
        }
        self.words[lo_word] & lo_mask != 0
            || self.words[lo_word + 1..hi_word]
                .iter()
                .any(|&word| word != 0)
            || self.words[hi_word] & hi_mask != 0
    }
}

impl LiveSet {
    fn any_gone(&self, first: DocId, last: DocId) -> bool {
        let last = last.min(self.max);
        if first > last {
            return false;
        }
        self.live.range_cardinality(first..=last) < last - first + 1
    }
}

impl DocLiveness for &GoneIds {
    #[inline]
    fn exists(&self, id: DocId) -> bool {
        match self {
            GoneIds::Bitset(bits) => !bits.gone(id),
            GoneIds::DeadSet(set) => !(id < set.dead_below || contains(&set.dead, id)),
            GoneIds::LiveSet(set) => id > set.max || contains(&set.live, id),
        }
    }

    fn any_gone(&self, first: DocId, last: DocId) -> bool {
        match self {
            GoneIds::Bitset(bits) => bits.any_gone(first, last),
            GoneIds::DeadSet(set) => first < set.dead_below || any_in(&set.dead, first, last),
            GoneIds::LiveSet(set) => set.any_gone(first, last),
        }
    }
}

/// Whether `ids` holds `id`, for a [`GoneIds::DeadSet`] or [`GoneIds::LiveSet`]. Kept out of line so that the
/// [`DocLiveness::exists`] of [`GoneIds`], with only the bitmap check inline, stays
/// small enough to inline into the scanners' loops.
#[inline(never)]
fn contains(ids: &RoaringTreemap, id: DocId) -> bool {
    ids.contains(id)
}

/// Whether `ids` holds an ID in `first..=last`, for a [`GoneIds::DeadSet`].
fn any_in(ids: &RoaringTreemap, first: DocId, last: DocId) -> bool {
    // Seek to the first ID at or after `first` rather than count the range: a count
    // ranks inside every container it touches, which on a dense bitmap container is
    // a scan of its words.
    let mut ids = ids.iter();
    ids.advance_to(first);
    ids.next().is_some_and(|id| id <= last)
}
