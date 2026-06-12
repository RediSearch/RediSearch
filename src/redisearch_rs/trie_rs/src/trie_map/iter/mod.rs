/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Different iterators to traverse a [`TrieMap`](crate::TrieMap).
mod contains;
pub mod filter;
mod into_values;
mod lending;
mod lending_contains;
mod lending_range;
mod prefixes;
mod range;
mod unfiltered;
mod values;
mod wildcard;

pub use contains::ContainsIter;
pub use into_values::IntoValues;
pub use lending::LendingIter;
pub use lending_contains::ContainsLendingIter;
pub use lending_range::RangeLendingIter;
pub use prefixes::PrefixesIter;
pub use range::{RangeBoundary, RangeFilter, RangeIter};
pub use unfiltered::Iter;
pub use values::Values;
pub use wildcard::{WildcardFilter, WildcardIter};
