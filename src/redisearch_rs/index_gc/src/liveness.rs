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

/// The most bytes [`GoneIds::Bits`] spends; a wider window is kept as a [`GoneIds::Set`].
pub const MAX_BITMAP_BYTES: usize = 16 << 20;

/// The documents gone when a cycle began, as the cycle's only source of liveness.
///
/// A document's ID is never reused, and an update gives the document a new one, so
/// the IDs that matter sit in a window that moves up over time. A window that fits
/// [`MAX_BITMAP_BYTES`] is kept as bits, where a check is one load; a wider one as
/// a roaring set, a few dependent searches per check but sized by its contents.
///
/// An ID above the window is live: in a forked child the table holds nothing newer,
/// and an in-process cycle leaves documents deleted after it began for the next one.
#[derive(Debug)]
pub enum GoneIds {
    /// One bit per ID of the window, set when the ID is gone.
    Bits(Bits),
    /// Every gone ID.
    Set(RoaringTreemap),
}

/// The window of a [`GoneIds::Bits`].
#[derive(Debug)]
pub struct Bits {
    /// The ID of bit 0 of `words`.
    first: DocId,
    words: Box<[u64]>,
    /// Whether the IDs below `first` are gone (a forked child's dead prefix) or
    /// live (an in-process cycle's IDs below its first deleted one).
    gone_below: bool,
}

impl GoneIds {
    /// The documents in `gone`, as an in-process cycle copies them from the doc
    /// table's deleted set.
    pub fn from_set(gone: RoaringTreemap) -> Self {
        Self::from_set_with_cap(gone, MAX_BITMAP_BYTES)
    }

    /// Like [`GoneIds::from_set`], with bits only for a window of at most
    /// `max_bitmap_bytes`.
    pub fn from_set_with_cap(gone: RoaringTreemap, max_bitmap_bytes: usize) -> Self {
        let (Some(first), Some(last)) = (gone.min(), gone.max()) else {
            return Self::Set(gone);
        };
        let Some(len) = window_words(first, last, max_bitmap_bytes) else {
            return Self::Set(gone);
        };
        let mut words = vec![0u64; len].into_boxed_slice();
        for id in &gone {
            let bit = id - first;
            words[(bit / 64) as usize] |= 1 << (bit % 64);
        }
        Self::Bits(Bits {
            first,
            words,
            gone_below: false,
        })
    }

    /// The documents a forked child's frozen table does not hold.
    pub fn of_table(spec: &IndexSpecReadGuard) -> Self {
        Self::of_table_with_cap(spec, MAX_BITMAP_BYTES)
    }

    /// Like [`GoneIds::of_table`], with bits only for a window of at most
    /// `max_bitmap_bytes`.
    pub fn of_table_with_cap(spec: &IndexSpecReadGuard, max_bitmap_bytes: usize) -> Self {
        let max_doc_id = spec.doc_table().maxDocId;
        // The C fill is the fastest walk of the table; past its own size cap the
        // walk collects the live IDs instead.
        let Some(live) = spec.live_doc_bitmap() else {
            let mut live = RoaringTreemap::new();
            spec.for_each_live_doc(|id| {
                live.insert(id);
            });
            let mut gone = RoaringTreemap::new();
            gone.insert_range(0..=max_doc_id);
            gone -= live;
            return Self::Set(gone);
        };
        Self::from_live_words(&live, max_doc_id, max_bitmap_bytes)
    }

    /// The gone IDs of a table whose live IDs are the set bits of `live`.
    fn from_live_words(live: &[u64], max_doc_id: DocId, max_bitmap_bytes: usize) -> Self {
        // Words past the table's maximum hold no live ID.
        let words = (max_doc_id / 64 + 1).min(live.len() as u64) as usize;
        let live = &live[..words];
        let Some(first_word) = live.iter().position(|&word| word != 0) else {
            // Nothing is live: everything up to the table's maximum is gone.
            return Self::Bits(Bits {
                first: max_doc_id.saturating_add(1),
                words: Box::default(),
                gone_below: true,
            });
        };
        let window = &live[first_word..];
        // Starting on a word boundary is harmless: every ID below the first live
        // one is gone either way.
        let first = first_word as DocId * 64;
        if window.len().saturating_mul(8) <= max_bitmap_bytes {
            let mut words: Box<[u64]> = window.iter().map(|&word| !word).collect();
            // The table holds no ID above its maximum, so those bits stay live.
            let past_max = (max_doc_id % 64) + 1;
            if past_max < 64
                && let Some(word) = words.last_mut()
            {
                *word &= (1 << past_max) - 1;
            }
            return Self::Bits(Bits {
                first,
                words,
                gone_below: true,
            });
        }
        // A sparse table's gaps can span hundreds of millions of IDs, so a stretch of
        // words with no live ID goes in as one range; only the words that have one are
        // walked bit by bit.
        let mut gone = RoaringTreemap::new();
        let mut dead_from = 0;
        for (i, &word) in window.iter().enumerate().filter(|&(_, &word)| word != 0) {
            let base = first + i as DocId * 64;
            gone.insert_range(dead_from..base);
            let mut unset = !word;
            while unset != 0 {
                gone.insert(base + DocId::from(unset.trailing_zeros()));
                unset &= unset - 1;
            }
            dead_from = base + 64;
        }
        gone.insert_range(dead_from..=max_doc_id);
        gone.remove_range(max_doc_id.saturating_add(1)..);
        Self::Set(gone)
    }

    /// Every gone ID, for an in-process cycle to prune what it collected.
    ///
    /// A forked child's dead prefix comes back as the range it is.
    pub fn into_set(self) -> RoaringTreemap {
        match self {
            Self::Set(gone) => gone,
            Self::Bits(bits) => {
                let mut gone = RoaringTreemap::new();
                if bits.gone_below {
                    gone.insert_range(..bits.first);
                }
                for (i, &word) in bits.words.iter().enumerate() {
                    let mut set = word;
                    while set != 0 {
                        gone.insert(
                            bits.first + i as DocId * 64 + DocId::from(set.trailing_zeros()),
                        );
                        set &= set - 1;
                    }
                }
                gone
            }
        }
    }
}

/// The words a bitmap from `first` to `last` takes, when it fits `max_bytes`.
fn window_words(first: DocId, last: DocId, max_bytes: usize) -> Option<usize> {
    let words = (last - first) / 64 + 1;
    (words * 8 <= max_bytes as u64).then_some(words as usize)
}

impl Bits {
    #[inline]
    fn gone(&self, id: DocId) -> bool {
        let Some(bit) = id.checked_sub(self.first) else {
            return self.gone_below;
        };
        self.words
            .get((bit / 64) as usize)
            .is_some_and(|word| word >> (bit % 64) & 1 != 0)
    }

    fn any_gone(&self, first: DocId, last: DocId) -> bool {
        if first < self.first && self.gone_below {
            return true;
        }
        let Some(window_last) = (self.words.len() as DocId * 64).checked_sub(1) else {
            return false;
        };
        let lo = first.max(self.first) - self.first;
        let Some(hi) = last.checked_sub(self.first) else {
            // Wholly below the window, where IDs are live (`gone_below` returned above).
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

impl DocLiveness for &GoneIds {
    #[inline]
    fn exists(&self, id: DocId) -> bool {
        match self {
            GoneIds::Bits(bits) => !bits.gone(id),
            GoneIds::Set(gone) => !contains(gone, id),
        }
    }

    fn any_gone(&self, first: DocId, last: DocId) -> bool {
        match self {
            GoneIds::Bits(bits) => bits.any_gone(first, last),
            GoneIds::Set(gone) => any_in(gone, first, last),
        }
    }
}

/// Whether `ids` holds `id`, for a [`GoneIds::Set`]. Kept out of line so that the
/// [`DocLiveness::exists`] of [`GoneIds`], with only the bitmap check inline, stays
/// small enough to inline into the scanners' loops.
#[inline(never)]
fn contains(ids: &RoaringTreemap, id: DocId) -> bool {
    ids.contains(id)
}

/// Whether `ids` holds an ID in `first..=last`, for a [`GoneIds::Set`].
fn any_in(ids: &RoaringTreemap, first: DocId, last: DocId) -> bool {
    // Seek to the first ID at or after `first` rather than count the range: a count
    // ranks inside every container it touches, which on a dense bitmap container is
    // a scan of its words.
    let mut ids = ids.iter();
    ids.advance_to(first);
    ids.next().is_some_and(|id| id <= last)
}
