/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Per-entry payload for [`TermSuffixIndex`](super::TermSuffixIndex).
//!
//! A trie key `K` can play two independent roles at the same time:
//!
//! - `K` may itself be an indexed member of the set.
//! - `K` may be a proper suffix of one or more longer members.
//!
//! Both, either, or neither role may hold at any moment. Tracking
//! them in separate fields (`full_term` and `longer_terms`) lets
//! [`add`](super::TermSuffixIndex::add) and
//! [`remove`](super::TermSuffixIndex::remove) distinguish updating
//! an indexed member from updating back-reference bookkeeping for
//! a longer source.

use std::rc::Rc;

/// [`Rc<str>`](std::rc::Rc) shares one heap allocation across all of a term's
/// trie entries — one full-term entry plus its `N - 1` proper-suffix
/// entries — keeping per-term memory `O(N)` instead of `O(N²)`.
/// Bytes drop with the last reference.
#[derive(Debug, Default)]
pub(super) struct TermRefs {
    full_term: Option<Rc<str>>,
    longer_terms: Vec<Rc<str>>,
}

impl TermRefs {
    /// Record `term` as the full-term occupant of an entry, given the
    /// entry's current payload (`None` if the key is new). Shaped to be
    /// passed straight to [`StrTrieMap::insert_with`](trie_rs::str_trie_map::StrTrieMap::insert_with),
    /// which constructs-or-mutates in a single trie descent.
    pub(super) fn upsert_full_term(slot: Option<Self>, term: Rc<str>) -> Self {
        match slot {
            Some(mut data) => {
                debug_assert!(
                    data.full_term.is_none(),
                    "upsert_full_term on full-term entry"
                );
                data.full_term = Some(term);
                data
            }
            None => Self {
                full_term: Some(term),
                longer_terms: Vec::new(),
            },
        }
    }

    /// Record a back-reference from a suffix entry to a longer `term` it
    /// is a proper suffix of, given the entry's current payload (`None`
    /// if the key is new). Same construct-or-mutate shape as
    /// [`upsert_full_term`](Self::upsert_full_term).
    pub(super) fn upsert_longer_term(slot: Option<Self>, term: Rc<str>) -> Self {
        match slot {
            Some(mut data) => {
                data.longer_terms.push(term);
                data
            }
            None => Self {
                full_term: None,
                longer_terms: vec![term],
            },
        }
    }

    /// Whether the entry's key is itself an indexed member, as opposed to
    /// being only a suffix of longer members.
    pub(super) const fn has_full_term(&self) -> bool {
        self.full_term.is_some()
    }

    /// Drop the full-term role from the entry, reporting via [`Outcome`]
    /// whether any back-references remain.
    pub(super) fn clear_full_term(&mut self) -> Outcome {
        debug_assert!(
            self.full_term.is_some(),
            "clear_full_term on suffix-only entry"
        );
        self.full_term = None;
        self.outcome()
    }

    /// Drop one back-reference to `term`, reporting via [`Outcome`]
    /// whether the entry still serves any role. A missing `term` is a
    /// no-op: callers walk every suffix of a removed term, but some of
    /// those entries may already have been evicted.
    pub(super) fn remove_longer_term(&mut self, term: &str) -> Outcome {
        if let Some(pos) = self.longer_terms.iter().position(|t| t.as_ref() == term) {
            self.longer_terms.swap_remove(pos);
        }
        self.outcome()
    }

    /// Classify the entry's current state for the caller's evict decision.
    const fn outcome(&self) -> Outcome {
        if self.is_empty() {
            Outcome::Drained
        } else {
            Outcome::Retained
        }
    }

    const fn is_empty(&self) -> bool {
        self.full_term.is_none() && self.longer_terms.is_empty()
    }

    /// Every member term reachable through this entry: the full term (if
    /// the key is one) followed by the longer terms it is a suffix of.
    pub(super) fn terms(&self) -> impl Iterator<Item = &Rc<str>> {
        self.full_term.iter().chain(self.longer_terms.iter())
    }
}

/// Whether a mutating operation on [`TermRefs`] left the entry empty.
#[derive(Debug, PartialEq, Eq)]
#[must_use = "evict the entry from the trie on Outcome::Drained"]
pub(super) enum Outcome {
    Drained,
    Retained,
}
