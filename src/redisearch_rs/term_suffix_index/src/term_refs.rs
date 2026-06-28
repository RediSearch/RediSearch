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

    pub(super) const fn has_full_term(&self) -> bool {
        self.full_term.is_some()
    }

    pub(super) fn clear_full_term(&mut self) -> Outcome {
        debug_assert!(
            self.full_term.is_some(),
            "clear_full_term on suffix-only entry"
        );
        self.full_term = None;
        self.outcome()
    }

    pub(super) fn remove_longer_term(&mut self, term: &str) -> Outcome {
        if let Some(pos) = self.longer_terms.iter().position(|t| t.as_ref() == term) {
            self.longer_terms.swap_remove(pos);
        }
        self.outcome()
    }

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

    pub(super) fn terms(&self) -> impl Iterator<Item = &Rc<str>> {
        self.full_term.iter().chain(self.longer_terms.iter())
    }
}

/// Whether a mutating operation on `TermRefs` left the entry empty.
#[derive(Debug, PartialEq, Eq)]
#[must_use = "evict the entry from the trie on Outcome::Drained"]
pub(super) enum Outcome {
    Drained,
    Retained,
}
