/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Shared fixtures. Terminals are plain lists of doc ids, standing in for the posting lists
//! the real terminals hold.

use std::num::NonZeroUsize;

use compound_index::{BucketLevel, CompoundIndex, Constraint, Key, Level, TagLevel};
use timeout::{TimeoutCheckResult, TimeoutChecker};

pub type Index = CompoundIndex<Vec<u32>>;

/// A [`TimeoutChecker`] that passes its first `n` checks and times out from then on, so a walk
/// that checks exactly `n` times succeeds and one that checks once more fails.
pub struct TimesOutAfter(pub usize);

impl TimeoutChecker for TimesOutAfter {
    fn check_timeout(&mut self) -> TimeoutCheckResult {
        match self.0.checked_sub(1) {
            Some(left) => {
                self.0 = left;
                TimeoutCheckResult::Ok
            }
            None => TimeoutCheckResult::TimedOut,
        }
    }

    fn reset_counter(&mut self) {}
}

pub const fn tag(value: &str) -> Key<'_> {
    Key::Tag(value.as_bytes())
}

pub const fn tag_level() -> Level {
    Level::Tag(TagLevel::new())
}

pub const fn tag_level_max(max: usize) -> Level {
    Level::Tag(TagLevel::with_max_cardinality(
        NonZeroUsize::new(max).unwrap(),
    ))
}

pub fn bucket_level(boundaries: &[f64]) -> Level {
    Level::Buckets(BucketLevel::new(boundaries.to_vec()).unwrap())
}

pub fn tag_in<'a>(values: &[&'a str]) -> Constraint<'a> {
    Constraint::TagIn(values.iter().map(|v| v.as_bytes()).collect())
}

/// Posts `doc` in the terminal at `path`, creating it if needed.
pub fn post(index: &mut Index, path: &[Key<'_>], doc: u32) {
    index.get_or_insert_with(path, Vec::new).unwrap().push(doc);
}

/// The distinct doc ids found in `terminals`, sorted: what a union of their posting lists
/// would return.
pub fn union(terminals: &[&Vec<u32>]) -> Vec<u32> {
    let mut docs: Vec<u32> = terminals.iter().flat_map(|t| t.iter().copied()).collect();
    docs.sort_unstable();
    docs.dedup();
    docs
}

/// Two tag levels, `color` then `size`, holding these order lines:
///
/// | doc | items                      |
/// |-----|----------------------------|
/// | 1   | (red, M), (blue, L)        |
/// | 2   | (red, L)                   |
/// | 3   | (red, M), (red, M)         |
/// | 4   | (blue, M)                  |
pub fn orders() -> Index {
    let mut index = Index::new(vec![tag_level(), tag_level()]).unwrap();
    for (doc, color, size) in [
        (1, "red", "M"),
        (1, "blue", "L"),
        (2, "red", "L"),
        (3, "red", "M"),
        (3, "red", "M"),
        (4, "blue", "M"),
    ] {
        post(&mut index, &[tag(color), tag(size)], doc);
    }
    index
}
