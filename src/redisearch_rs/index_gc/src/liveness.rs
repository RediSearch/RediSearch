/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The set of gone documents a GC cycle scans against, for both policies.

use index_spec::IndexSpecReadGuard;
use inverted_index::DocLiveness;
use roaring::RoaringTreemap;
use rqe_core::DocId;

/// The most bytes [`GoneIds`] spends on its bitmap; a wider window goes without one.
pub const MAX_BITMAP_BYTES: usize = 32 << 20;

/// The documents gone when a cycle began, as the cycle's only source of liveness.
///
/// A document's ID is never reused, and an update gives the document a new one, so
/// the live IDs sit in a band that moves up over time and everything below the band
/// is gone. The set keeps that prefix as one bound, the gone IDs above it in a
/// roaring set for range checks, and, when it fits [`MAX_BITMAP_BYTES`], the same
/// IDs as a bitmap over just their span, so a per-record check is one load into a
/// table small enough to stay in cache.
///
/// An ID above the gone ones is live: in a forked child the table holds nothing
/// newer, and an in-process cycle leaves documents deleted after it began for the
/// next one.
#[derive(Debug, Default)]
pub struct GoneIds {
    /// Every ID below this is gone.
    gone_below: DocId,
    /// The gone IDs from `gone_below` up.
    gone: RoaringTreemap,
    /// `gone` as bits, bit `id - first` per ID from the first gone ID to the last.
    bits: Option<Bits>,
}

#[derive(Debug)]
struct Bits {
    first: DocId,
    words: Box<[u64]>,
}

impl GoneIds {
    /// The documents in `gone`, as an in-process cycle copies them from the doc
    /// table's deleted set.
    pub fn from_set(gone: RoaringTreemap) -> Self {
        Self::new(0, gone, MAX_BITMAP_BYTES)
    }

    /// The documents a forked child's frozen table does not hold.
    pub fn of_table(spec: &IndexSpecReadGuard) -> Self {
        let max_doc_id = spec.doc_table().maxDocId;
        // The C fill is the fastest walk of the table; past its size cap the walk
        // collects the live IDs instead.
        let (gone_below, gone) = match spec.live_doc_bitmap() {
            Some(live) => gone_from_live_bitmap(&live, max_doc_id),
            None => {
                let mut live = RoaringTreemap::new();
                spec.for_each_live_doc(|id| {
                    live.insert(id);
                });
                gone_from_live_set(&live, max_doc_id)
            }
        };
        Self::new(gone_below, gone, MAX_BITMAP_BYTES)
    }

    /// Like [`GoneIds::from_set`], with a bitmap of at most `max_bitmap_bytes`, and
    /// every ID below `gone_below` gone.
    pub fn new(gone_below: DocId, mut gone: RoaringTreemap, max_bitmap_bytes: usize) -> Self {
        gone.remove_range(..gone_below);
        let bits = match (gone.min(), gone.max()) {
            (Some(first), Some(last)) if (last - first) / 8 < max_bitmap_bytes as u64 => {
                let mut words = vec![0u64; ((last - first) / 64 + 1) as usize].into_boxed_slice();
                for id in &gone {
                    let bit = id - first;
                    words[(bit / 64) as usize] |= 1 << (bit % 64);
                }
                Some(Bits { first, words })
            }
            _ => None,
        };
        Self {
            gone_below,
            gone,
            bits,
        }
    }

    /// The gone IDs from the prefix bound up, for the caller to prune once collected.
    pub fn into_set(self) -> RoaringTreemap {
        self.gone
    }

    /// Whether no document is gone.
    pub fn is_empty(&self) -> bool {
        self.gone_below <= 1 && self.gone.is_empty()
    }
}

impl DocLiveness for &GoneIds {
    #[inline]
    fn exists(&self, id: DocId) -> bool {
        if id < self.gone_below {
            return false;
        }
        match &self.bits {
            Some(bits) => {
                let Some(bit) = id.checked_sub(bits.first) else {
                    return true;
                };
                usize::try_from(bit / 64)
                    .ok()
                    .and_then(|i| bits.words.get(i))
                    .is_none_or(|word| word & (1 << (bit % 64)) == 0)
            }
            None => !contains(&self.gone, id),
        }
    }

    fn any_gone(&self, first: DocId, last: DocId) -> bool {
        if first < self.gone_below {
            return true;
        }
        // Seek to the first gone ID at or after `first` rather than count the range: a
        // count ranks inside every container it touches, which on a dense bitmap
        // container is a scan of its words.
        let mut gone = self.gone.iter();
        gone.advance_to(first);
        gone.next().is_some_and(|id| id <= last)
    }
}

/// Kept out of line so that [`GoneIds`]' [`DocLiveness::exists`], with only the bitmap check inline,
/// stays small enough to inline into the scanners' loops.
#[inline(never)]
fn contains(ids: &RoaringTreemap, id: DocId) -> bool {
    ids.contains(id)
}

/// The prefix bound and the gone IDs above it, from a bitmap of the live IDs.
fn gone_from_live_bitmap(live: &[u64], max_doc_id: DocId) -> (DocId, RoaringTreemap) {
    let Some(first_live) = live
        .iter()
        .position(|&word| word != 0)
        .map(|i| i as DocId * 64 + DocId::from(live[i].trailing_zeros()))
    else {
        return (max_doc_id.saturating_add(1), RoaringTreemap::new());
    };
    let unset = live.iter().enumerate().flat_map(|(i, &word)| {
        let base = i as DocId * 64;
        let mut unset = !word;
        std::iter::from_fn(move || {
            (unset != 0).then(|| {
                let bit = unset.trailing_zeros();
                unset &= unset - 1;
                base + DocId::from(bit)
            })
        })
    });
    let gone = RoaringTreemap::from_sorted_iter(
        unset
            .skip_while(|&id| id < first_live)
            .take_while(|&id| id <= max_doc_id),
    )
    .expect("bit positions are visited in increasing order");
    (first_live, gone)
}

/// The prefix bound and the gone IDs above it, from the set of live IDs.
fn gone_from_live_set(live: &RoaringTreemap, max_doc_id: DocId) -> (DocId, RoaringTreemap) {
    let Some(first_live) = live.min() else {
        return (max_doc_id.saturating_add(1), RoaringTreemap::new());
    };
    let mut gone = RoaringTreemap::new();
    gone.insert_range(first_live..=max_doc_id);
    gone -= live;
    (first_live, gone)
}
