/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

/// What a query asks of one routing level.
///
/// [`CompoundIndex::collect_terminals`](crate::CompoundIndex::collect_terminals) takes one
/// constraint per level.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Constraint<'a> {
    /// Every child of the level.
    Any,
    /// The children whose tag value is in the set.
    TagIn(TagSet<'a>),
    /// The children whose tag value is not in the set.
    TagNotIn(TagSet<'a>),
    /// The children whose tag value starts with the prefix.
    TagPrefix(&'a [u8]),
}

/// A set of tag values, kept sorted and without repeats.
///
/// A walk looks up each value of a [`Constraint::TagIn`] once, so a value listed twice would
/// return its terminal twice; and it binary-searches a [`Constraint::TagNotIn`]. Holding the set
/// in this form makes both hold by construction.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TagSet<'a>(Box<[&'a [u8]]>);

impl<'a> TagSet<'a> {
    /// The set of `values`, in any order, repeats counting once.
    pub fn new(values: impl IntoIterator<Item = &'a [u8]>) -> Self {
        let mut values: Vec<&'a [u8]> = values.into_iter().collect();
        values.sort_unstable();
        values.dedup();
        Self(values.into_boxed_slice())
    }

    /// Whether `value` is in the set.
    pub fn contains(&self, value: &[u8]) -> bool {
        self.0.binary_search(&value).is_ok()
    }

    /// The values, in increasing byte order.
    pub fn values(&self) -> &[&'a [u8]] {
        &self.0
    }
}
