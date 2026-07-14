/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::{
    TrieMap,
    iter::{self, AutomatonIter, filter},
    str_trie_map::{
        automaton::{CodepointWildcard, CodepointWildcardNfa},
        iter::unfiltered::key_to_string,
    },
};

/// Iterator over the entries of a
/// [`StrTrieMap`](crate::str_trie_map::StrTrieMap) whose key matches a
/// wildcard pattern with codepoint semantics, in lexicographical key order.
///
/// See [`CodepointWildcard`] for the matching model. The backend is selected
/// by pattern size, mirroring the byte-level
/// [`WildcardIter`](crate::iter::WildcardIter): an NFA driven over the trie
/// while the position set fits a `u64`/`u128` bitset, and a full-scan
/// [`CodepointWildcard::matches`] filter beyond that.
pub struct WildcardIter<'tm, Data: 'tm>(Backend<'tm, Data>);

enum Backend<'tm, Data: 'tm> {
    /// `u64`-backed NFA — pattern has ≤ 63 atoms.
    Nfa64(AutomatonIter<'tm, Data, CodepointWildcardNfa<u64>>),
    /// `u128`-backed NFA — pattern has 64..=127 atoms.
    Nfa128(AutomatonIter<'tm, Data, CodepointWildcardNfa<u128>>),
    /// Per-key filter fallback — pattern has ≥ 128 atoms.
    Filter {
        pattern: CodepointWildcard,
        candidates: iter::Iter<'tm, Data, filter::VisitAll>,
    },
}

impl<'tm, Data: 'tm> WildcardIter<'tm, Data> {
    pub(crate) fn new(trie: &'tm TrieMap<Data>, pattern: &str) -> Self {
        let parsed = CodepointWildcard::parse(pattern);
        // The accept position lives at index `atom_count`, so the bitset
        // must hold `atom_count + 1` positions.
        let positions_needed = parsed.atom_count() + 1;
        let backend = if positions_needed <= 64 {
            Backend::Nfa64(trie.automaton_iter(CodepointWildcardNfa::compile(parsed)))
        } else if positions_needed <= 128 {
            Backend::Nfa128(trie.automaton_iter(CodepointWildcardNfa::compile(parsed)))
        } else {
            // Every match starts with the literal prefix, so even the scan
            // fallback only visits that subtree.
            let candidates = trie.prefixed_iter(parsed.literal_prefix().as_bytes());
            Backend::Filter {
                pattern: parsed,
                candidates,
            }
        };
        Self(backend)
    }
}

impl<'tm, Data: 'tm> Iterator for WildcardIter<'tm, Data> {
    type Item = (String, &'tm Data);

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.0 {
            Backend::Nfa64(iter) => iter.next().map(|(k, v)| (key_to_string(k), v)),
            Backend::Nfa128(iter) => iter.next().map(|(k, v)| (key_to_string(k), v)),
            Backend::Filter {
                pattern,
                candidates,
            } => candidates.find_map(|(k, v)| {
                let key = key_to_string(k);
                pattern.matches(&key).then_some((key, v))
            }),
        }
    }
}
