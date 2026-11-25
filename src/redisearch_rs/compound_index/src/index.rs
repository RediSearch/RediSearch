/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroUsize,
    ops::{Bound, Range},
};

use lending_iterator::LendingIterator;
use timeout::{TimeoutCheckResult, TimeoutChecker};
use trie_rs::TrieMap;

use crate::{
    BucketLevel, BucketRange, CollectError, Constraint, InsertError, Key, Level, PathError,
    SchemaError,
};

/// The most routing levels a [`CompoundIndex`] accepts.
///
/// Together with the terminal attribute, an index covers at most one more attribute than this.
/// The bound keeps the recursion of a walk shallow and the fan-out of a query predictable.
pub const MAX_LEVELS: usize = 3;

/// The routing levels of a compound index, with a terminal of type `T` at the end of each path.
///
/// See the [crate documentation](crate) for what a path means. Terminals are created on first
/// insert, by [`CompoundIndex::get_or_insert_with`].
pub struct CompoundIndex<T> {
    levels: Box<[Level]>,
    root: Node<T>,
    num_terminals: usize,
}

/// A node at some depth of the index. Nodes above the last level are [`Node::Inner`], with
/// children of their level's kind. Nodes at depth [`CompoundIndex::levels`]`.len()` are
/// [`Node::Terminal`].
enum Node<T> {
    Inner(Children<T>),
    Terminal(T),
}

enum Children<T> {
    Tag(TrieMap<Box<Node<T>>>),
    /// One slot per bucket of the level.
    Buckets(Box<[Option<Box<Node<T>>>]>),
}

/// The child a key selects on one level, once checked against that level.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Step<'k> {
    Tag(&'k [u8]),
    Bucket(usize),
}

/// A [`Constraint`] checked against its level.
enum LevelPlan<'c> {
    Any,
    /// Sorted and deduplicated.
    TagIn(Vec<&'c [u8]>),
    /// Sorted, for binary search.
    TagNotIn(Vec<&'c [u8]>),
    TagPrefix(&'c [u8]),
    Buckets(Range<usize>),
}

const INVARIANT: &str = "a node's kind matches its depth, checked when the path was routed";

impl<T> Node<T> {
    /// The children of an inner node. Only called on nodes above the terminal depth.
    fn children(&self) -> &Children<T> {
        match self {
            Self::Inner(children) => children,
            Self::Terminal(_) => unreachable!("{INVARIANT}"),
        }
    }
}

impl<T> Children<T> {
    /// The child `step` selects, if it exists. `step` was routed against this node's level.
    fn child(&self, step: &Step<'_>) -> Option<&Node<T>> {
        match (self, step) {
            (Self::Tag(map), Step::Tag(value)) => map.find(value).map(|child| &**child),
            (Self::Buckets(slots), Step::Bucket(bucket)) => slots[*bucket].as_deref(),
            _ => unreachable!("{INVARIANT}"),
        }
    }

    /// The number of children.
    fn len(&self) -> usize {
        match self {
            Self::Tag(map) => map.n_unique_keys(),
            Self::Buckets(slots) => slots.iter().filter(|slot| slot.is_some()).count(),
        }
    }

    fn for_level(level: &Level) -> Self {
        match level {
            Level::Tag(_) => Self::Tag(TrieMap::new()),
            Level::Buckets(buckets) => {
                Self::Buckets((0..buckets.num_buckets()).map(|_| None).collect())
            }
        }
    }
}

impl<T> CompoundIndex<T> {
    /// An empty index with the given routing levels, outermost first.
    pub fn new(levels: Vec<Level>) -> Result<Self, SchemaError> {
        if levels.is_empty() {
            return Err(SchemaError::NoLevels);
        }
        if levels.len() > MAX_LEVELS {
            return Err(SchemaError::TooManyLevels {
                max: MAX_LEVELS,
                got: levels.len(),
            });
        }
        let root = Node::Inner(Children::for_level(&levels[0]));
        Ok(Self {
            levels: levels.into_boxed_slice(),
            root,
            num_terminals: 0,
        })
    }

    /// The routing levels, outermost first.
    pub fn levels(&self) -> &[Level] {
        &self.levels
    }

    /// The number of terminals, which is the number of distinct paths inserted and not removed.
    pub const fn num_terminals(&self) -> usize {
        self.num_terminals
    }

    /// Whether the index holds no terminal.
    pub const fn is_empty(&self) -> bool {
        self.num_terminals == 0
    }

    /// The terminal at the end of `path`, if that path was inserted.
    pub fn get(&self, path: &[Key<'_>]) -> Result<Option<&T>, PathError> {
        let steps = self.route(path)?;
        Ok(self.find(&steps))
    }

    /// The terminal at the end of `path`, created with `init` if the path is new: the route step
    /// of [route, then post](crate#route-then-post).
    ///
    /// Creating the path adds every missing node on the way. The path is checked in full
    /// before anything is added, so an error leaves the index unchanged.
    ///
    /// # Examples
    ///
    /// ```
    /// use compound_index::{CompoundIndex, Key, Level, TagLevel};
    ///
    /// // Terminals are doc-id lists; levels are `color`, then `size`.
    /// let mut index: CompoundIndex<Vec<u32>> =
    ///     CompoundIndex::new(vec![Level::Tag(TagLevel::new()), Level::Tag(TagLevel::new())])
    ///         .unwrap();
    /// let path = [Key::Tag(b"red"), Key::Tag(b"M")];
    ///
    /// // Route, then post.
    /// index.get_or_insert_with(&path, Vec::new).unwrap().push(7);
    /// // Same path, so the same terminal: `init` does not run again.
    /// index.get_or_insert_with(&path, Vec::new).unwrap().push(9);
    ///
    /// assert_eq!(index.get(&path).unwrap(), Some(&vec![7, 9]));
    /// assert_eq!(index.num_terminals(), 1);
    /// ```
    pub fn get_or_insert_with(
        &mut self,
        path: &[Key<'_>],
        init: impl FnOnce() -> T,
    ) -> Result<&mut T, InsertError> {
        let steps = self.route(path)?;
        // The same check as `check_paths`, so a batch it accepts always inserts here.
        self.check_routed(std::slice::from_ref(&steps))?;
        let is_new = self.find(&steps).is_none();
        // Built before anything is inserted, so a panicking `init` leaves the index unchanged.
        let mut terminal = is_new.then(init);

        let levels = &self.levels;
        let mut new_node = |depth: usize| -> Box<Node<T>> {
            Box::new(match levels.get(depth) {
                Some(level) => Node::Inner(Children::for_level(level)),
                None => Node::Terminal(
                    terminal
                        .take()
                        .expect("a new path reaches the terminal depth once"),
                ),
            })
        };

        let mut node = &mut self.root;
        for (depth, step) in steps.iter().enumerate() {
            let Node::Inner(children) = node else {
                unreachable!("{INVARIANT}");
            };
            node = match (children, step) {
                (Children::Tag(map), Step::Tag(value)) => {
                    if map.find(value).is_none() {
                        map.insert(value, new_node(depth + 1));
                    }
                    &mut **map.find_mut(value).expect("present or inserted above")
                }
                (Children::Buckets(slots), Step::Bucket(bucket)) => {
                    &mut **slots[*bucket].get_or_insert_with(|| new_node(depth + 1))
                }
                _ => unreachable!("{INVARIANT}"),
            };
        }
        if is_new {
            self.num_terminals += 1;
        }
        match node {
            Node::Terminal(terminal) => Ok(terminal),
            Node::Inner(_) => unreachable!("{INVARIANT}"),
        }
    }

    /// Checks that every path of `paths` can be inserted together, without inserting anything.
    ///
    /// Each path passing [`CompoundIndex::get_or_insert_with`]'s checks on its own is not
    /// enough for a batch: two new values under the same parent may each fit a cardinality limit
    /// alone and break it together. This counts the new children every parent would receive from
    /// the whole batch, including parents the batch itself creates. When it returns `Ok`, inserting
    /// every path, in any order, succeeds. A caller indexing all the elements of one document calls
    /// it first, so a document is indexed whole or not at all.
    ///
    /// A path error is reported for the first invalid path; cardinality is checked once every path
    /// has been routed.
    pub fn check_paths(&self, paths: &[&[Key<'_>]]) -> Result<(), InsertError> {
        let routed = paths
            .iter()
            .map(|path| self.route(path))
            .collect::<Result<Vec<_>, _>>()?;
        self.check_routed(&routed)
    }

    /// The terminals whose path satisfies every constraint, one constraint per level.
    ///
    /// Terminals come out in a deterministic order: tag children by byte order of their value,
    /// buckets in increasing order. Each terminal is returned at most once. The walk never returns
    /// a partial result: it fails with [`CollectError::TooManyTerminals`] as soon as it would
    /// return more than `max_terminals`, and with [`CollectError::TimedOut`] as soon as `timeout`
    /// reports a timeout.
    ///
    /// `max_terminals` bounds the result, so the memory of whatever the caller opens per terminal.
    /// It does not bound the work: a level the query leaves open is entered through every child,
    /// even when nothing matches below it. `timeout` bounds that, and is checked on every node
    /// entered. On a bucket level the walk also reads the empty slots in its range;
    /// [`MAX_BUCKETS`](crate::MAX_BUCKETS) bounds those per node entered.
    pub fn collect_terminals<'s>(
        &'s self,
        constraints: &[Constraint<'_>],
        max_terminals: usize,
        timeout: &mut impl TimeoutChecker,
    ) -> Result<Vec<&'s T>, CollectError> {
        let plan = self.plan(constraints)?;
        let mut walk = Walk {
            max_terminals,
            timeout,
            out: Vec::new(),
        };
        walk.visit(&self.root, &plan)?;
        Ok(walk.out)
    }

    fn route<'k>(&self, path: &[Key<'k>]) -> Result<Vec<Step<'k>>, PathError> {
        if path.len() != self.levels.len() {
            return Err(PathError::WrongLength {
                expected: self.levels.len(),
                got: path.len(),
            });
        }
        self.levels
            .iter()
            .zip(path)
            .enumerate()
            .map(|(level_index, (level, key))| match (level, key) {
                (Level::Tag(_), Key::Tag(value)) => Ok(Step::Tag(value)),
                (Level::Buckets(buckets), Key::Numeric(value)) => buckets
                    .bucket_of(*value)
                    .map(Step::Bucket)
                    .ok_or(PathError::OutsideBuckets { level: level_index }),
                _ => Err(PathError::KeyMismatch { level: level_index }),
            })
            .collect()
    }

    /// The terminal at the end of `steps`, if that path was inserted.
    fn find(&self, steps: &[Step<'_>]) -> Option<&T> {
        let mut node = &self.root;
        for step in steps {
            node = node.children().child(step)?;
        }
        match node {
            Node::Terminal(terminal) => Some(terminal),
            Node::Inner(_) => unreachable!("{INVARIANT}"),
        }
    }

    /// Fails if inserting every path of `paths` would give a node more children than its level's
    /// cardinality limit. The one write-side check: [`CompoundIndex::check_paths`] runs it on a
    /// batch and [`CompoundIndex::get_or_insert_with`] on its single path, so they cannot
    /// disagree.
    fn check_routed(&self, paths: &[Vec<Step<'_>>]) -> Result<(), InsertError> {
        // A parent on a level with a limit → its existing children and the new ones the batch
        // adds. A level without a limit accepts any child, so it is not tracked.
        let mut new_children: BTreeMap<&[Step<'_>], (usize, BTreeSet<Step<'_>>)> = BTreeMap::new();
        for steps in paths {
            // `None` once the walk has left the existing nodes: everything below is new.
            let mut node = Some(&self.root);
            for (depth, step) in steps.iter().enumerate() {
                let children = node.map(Node::children);
                node = children.and_then(|children| children.child(step));
                if node.is_none() && self.max_cardinality(depth).is_some() {
                    new_children
                        .entry(&steps[..depth])
                        .or_insert_with(|| (children.map_or(0, Children::len), BTreeSet::new()))
                        .1
                        .insert(*step);
                }
            }
        }
        for (parent, (existing, added)) in &new_children {
            let level = parent.len();
            let max = self
                .max_cardinality(level)
                .expect("only levels with a limit are tracked");
            if existing + added.len() > max.get() {
                return Err(InsertError::CardinalityExceeded {
                    level,
                    max: max.get(),
                });
            }
        }
        Ok(())
    }

    /// The cardinality limit of the level at `level_index`, if it has one.
    fn max_cardinality(&self, level_index: usize) -> Option<NonZeroUsize> {
        match &self.levels[level_index] {
            Level::Tag(tag) => tag.max_cardinality(),
            Level::Buckets(_) => None,
        }
    }

    fn plan<'c>(&self, constraints: &[Constraint<'c>]) -> Result<Vec<LevelPlan<'c>>, CollectError> {
        if constraints.len() != self.levels.len() {
            return Err(CollectError::WrongLength {
                expected: self.levels.len(),
                got: constraints.len(),
            });
        }
        self.levels
            .iter()
            .zip(constraints)
            .enumerate()
            .map(|(level_index, (level, constraint))| {
                let mismatch = CollectError::ConstraintMismatch { level: level_index };
                match (level, constraint) {
                    (_, Constraint::Any) => Ok(LevelPlan::Any),
                    (Level::Tag(_), Constraint::TagIn(values)) => {
                        let mut values = values.clone();
                        values.sort_unstable();
                        values.dedup();
                        Ok(LevelPlan::TagIn(values))
                    }
                    (Level::Tag(_), Constraint::TagNotIn(values)) => {
                        let mut values = values.clone();
                        values.sort_unstable();
                        Ok(LevelPlan::TagNotIn(values))
                    }
                    (Level::Tag(_), Constraint::TagPrefix(prefix)) => {
                        Ok(LevelPlan::TagPrefix(prefix))
                    }
                    (Level::Buckets(buckets), Constraint::BucketRange(range)) => {
                        bucket_span(buckets, *range)
                            .map(LevelPlan::Buckets)
                            .ok_or(CollectError::UnalignedBucketRange { level: level_index })
                    }
                    (Level::Tag(_), Constraint::BucketRange(_))
                    | (
                        Level::Buckets(_),
                        Constraint::TagIn(_) | Constraint::TagNotIn(_) | Constraint::TagPrefix(_),
                    ) => Err(mismatch),
                }
            })
            .collect()
    }
}

/// The buckets lying wholly inside `range`, or [`None`] if a bucket is only partly inside it or
/// an end is NaN.
fn bucket_span(buckets: &BucketLevel, range: BucketRange) -> Option<Range<usize>> {
    let is_nan =
        |bound: Bound<f64>| matches!(bound, Bound::Included(v) | Bound::Excluded(v) if v.is_nan());
    if is_nan(range.start) || is_nan(range.end) {
        return None;
    }
    // An empty range holds no value, so no bucket is even partly inside it.
    let is_empty = match (range.start, range.end) {
        (Bound::Included(start), Bound::Included(end)) => start > end,
        (Bound::Included(start), Bound::Excluded(end))
        | (Bound::Excluded(start), Bound::Included(end)) => start >= end,
        // No float lies strictly between two adjacent floats.
        (Bound::Excluded(start), Bound::Excluded(end)) => start.next_up() >= end,
        (Bound::Unbounded, _) | (_, Bound::Unbounded) => false,
    };
    if is_empty {
        return Some(0..0);
    }
    let mut span: Option<Range<usize>> = None;
    for (bucket, bounds) in buckets.boundaries().windows(2).enumerate() {
        match overlap(bounds[0], bounds[1], range) {
            Overlap::Inside => {
                let start = span.map_or(bucket, |span| span.start);
                span = Some(start..bucket + 1);
            }
            Overlap::Outside => {}
            Overlap::Partial => return None,
        }
    }
    // A range is an interval, so the buckets inside it are contiguous.
    Some(span.unwrap_or(0..0))
}

/// How the bucket `[lo, hi)` sits relative to a range.
enum Overlap {
    /// Every value of the bucket is in the range.
    Inside,
    /// No value of the bucket is in the range.
    Outside,
    /// Some values of the bucket are in the range and some are not.
    Partial,
}

/// Classifies the bucket `[lo, hi)` against `range`, whose ends are not NaN and which holds at
/// least one value.
fn overlap(lo: f64, hi: f64, range: BucketRange) -> Overlap {
    // The largest value of the bucket is the float just below `hi`. Comparing it, rather than
    // `hi`, keeps the classification exact for ends one float away from a boundary.
    let top = hi.next_down();
    let all_above_start = match range.start {
        Bound::Unbounded => true,
        Bound::Included(start) => lo >= start,
        Bound::Excluded(start) => lo > start,
    };
    let all_below_end = match range.end {
        Bound::Unbounded => true,
        Bound::Included(end) => top <= end,
        Bound::Excluded(end) => top < end,
    };
    let all_before_start = match range.start {
        Bound::Unbounded => false,
        Bound::Included(start) => top < start,
        Bound::Excluded(start) => top <= start,
    };
    let all_after_end = match range.end {
        Bound::Unbounded => false,
        Bound::Included(end) => lo > end,
        Bound::Excluded(end) => lo >= end,
    };
    if all_before_start || all_after_end {
        Overlap::Outside
    } else if all_above_start && all_below_end {
        Overlap::Inside
    } else {
        Overlap::Partial
    }
}

/// The state of one [`CompoundIndex::collect_terminals`] walk.
struct Walk<'s, 't, T, C> {
    max_terminals: usize,
    timeout: &'t mut C,
    out: Vec<&'s T>,
}

impl<'s, T, C: TimeoutChecker> Walk<'s, '_, T, C> {
    /// Visits a child, after checking the timeout. The root is visited without a check, so
    /// every node entered below it, terminals included, costs one check.
    fn enter(&mut self, node: &'s Node<T>, plan: &[LevelPlan<'_>]) -> Result<(), CollectError> {
        if let TimeoutCheckResult::TimedOut = self.timeout.check_timeout() {
            return Err(CollectError::TimedOut);
        }
        self.visit(node, plan)
    }

    fn visit(&mut self, node: &'s Node<T>, plan: &[LevelPlan<'_>]) -> Result<(), CollectError> {
        let children = match node {
            Node::Terminal(terminal) => {
                if self.out.len() == self.max_terminals {
                    return Err(CollectError::TooManyTerminals {
                        max: self.max_terminals,
                    });
                }
                self.out.push(terminal);
                return Ok(());
            }
            Node::Inner(children) => children,
        };
        let (level_plan, rest) = plan.split_first().expect(INVARIANT);
        match (children, level_plan) {
            (Children::Tag(map), LevelPlan::Any) => {
                for child in map.values() {
                    self.enter(child, rest)?;
                }
            }
            (Children::Tag(map), LevelPlan::TagIn(values)) => {
                for child in values.iter().filter_map(|value| map.find(value)) {
                    self.enter(child, rest)?;
                }
            }
            (Children::Tag(map), LevelPlan::TagNotIn(values)) => {
                // Borrows each key instead of copying it, as `TrieMap::iter` would.
                let mut entries = map.lending_iter();
                while let Some((value, child)) = entries.next() {
                    if values.binary_search(&value).is_err() {
                        self.enter(child, rest)?;
                    }
                }
            }
            (Children::Tag(map), LevelPlan::TagPrefix(prefix)) => {
                for child in map.prefixed_values(prefix) {
                    self.enter(child, rest)?;
                }
            }
            (Children::Buckets(slots), LevelPlan::Any) => {
                for child in slots.iter().flatten() {
                    self.enter(child, rest)?;
                }
            }
            (Children::Buckets(slots), LevelPlan::Buckets(span)) => {
                for child in slots[span.clone()].iter().flatten() {
                    self.enter(child, rest)?;
                }
            }
            _ => unreachable!("constraints were checked against their levels"),
        }
        Ok(())
    }
}
