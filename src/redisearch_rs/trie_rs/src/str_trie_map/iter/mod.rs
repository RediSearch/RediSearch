/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Different iterators to traverse a [`StrTrieMap`](crate::str_trie_map::StrTrieMap).

mod case_insensitive;
mod contains;
mod fuzzy;
mod prefixed;
mod prefixed_values;
mod range;
mod suffixed;
mod unfiltered;
mod wildcard;

pub use case_insensitive::CaseInsensitiveIter;
pub use contains::ContainsIter;
pub use fuzzy::FuzzyIter;
pub use prefixed::PrefixedIter;
pub use prefixed_values::PrefixedValues;
pub use range::{RangeBoundary, RangeFilter, RangeIter};
pub use suffixed::SuffixedIter;
pub use unfiltered::Iter;
pub use wildcard::WildcardIter;
