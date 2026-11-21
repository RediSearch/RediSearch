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
    ops::{Bound, Range},
};

use lending_iterator::LendingIterator;
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

/// Bounds on the work of one [`CompoundIndex::collect_terminals`] walk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalkLimits {
    /// The most terminals the walk may return.
    pub max_terminals: usize,
    /// The most nodes the walk may enter, terminals included. A level a query leaves open is
    /// entered through every child, even when few terminals match below it, so this bounds the
    /// work of a walk that returns little. On a bucket level the walk also reads the empty slots
    /// in its range; those are not counted, and [`MAX_BUCKETS`](crate::MAX_BUCKETS) bounds them
    /// per node entered.
    pub max_nodes: usize,
}

impl WalkLimits {
    /// No bound on either count.
    pub const UNLIMITED: Self = Self {
        max_terminals: usize::MAX,
        max_nodes: usize::MAX,
    };
}

/// The routing levels of a compound index, with a terminal of type `T` at the end of each path.
///
/// See the [crate documentation](crate) for what a path means. Terminals are created on first
/// insert, by [`CompoundIndex::terminal_or_insert_with`], and removed by
/// [`CompoundIndex::retain_terminals`], which also drops the nodes left without children.
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

/// Whether a subtree survives [`CompoundIndex::retain_terminals`].
enum Fate {
    Keep,
    Drop,
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

    fn is_empty(&self) -> bool {
        match self {
            Self::Tag(map) => map.n_unique_keys() == 0,
            Self::Buckets(slots) => slots.iter().all(Option::is_none),
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
    pub fn terminal(&self, path: &[Key<'_>]) -> Result<Option<&T>, PathError> {
        let steps = self.route(path)?;
        let mut node = &self.root;
        for step in &steps {
            match node.children().child(step) {
                Some(child) => node = child,
                None => return Ok(None),
            }
        }
        match node {
            Node::Terminal(terminal) => Ok(Some(terminal)),
            Node::Inner(_) => unreachable!("{INVARIANT}"),
        }
    }

    /// Checks that `path` routes through every level, without reading the tree.
    ///
    /// The result depends on the levels alone, so it holds whatever has been inserted.
    pub fn check_path(&self, path: &[Key<'_>]) -> Result<(), PathError> {
        self.route(path).map(drop)
    }

    /// The terminal at the end of `path`, created with `init` if the path is new.
    ///
    /// Creating the path adds every missing node on the way. The path is checked in full
    /// before anything is added, so an error leaves the index unchanged.
    pub fn terminal_or_insert_with(
        &mut self,
        path: &[Key<'_>],
        init: impl FnOnce() -> T,
    ) -> Result<&mut T, InsertError> {
        let steps = self.route(path)?;
        let is_new = self.check_new_path(&steps)?;
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
    /// Each path passing [`CompoundIndex::terminal_or_insert_with`]'s checks on its own is not
    /// enough for a batch: two new values under the same parent may each fit a cardinality limit
    /// alone and break it together. This counts the new children every parent would receive from
    /// the whole batch, including parents the batch itself creates. When it returns `Ok`, inserting
    /// every path, in any order, succeeds. A caller indexing all the elements of one document calls
    /// it first, so a document is indexed whole or not at all.
    ///
    /// A path error is reported for the first invalid path; cardinality is checked once every path
    /// has been routed. Without any cardinality limit the tree is not read, as in
    /// [`CompoundIndex::check_path`].
    pub fn check_paths<'k>(&self, paths: &[&[Key<'k>]]) -> Result<(), InsertError> {
        let limited = self
            .levels
            .iter()
            .any(|level| matches!(level, Level::Tag(tag) if tag.max_cardinality().is_some()));
        if !limited {
            for path in paths {
                self.check_path(path)?;
            }
            return Ok(());
        }
        // The parent's path → its existing children and the new ones the batch adds.
        let mut new_children: BTreeMap<Vec<Step<'k>>, (usize, BTreeSet<Step<'k>>)> =
            BTreeMap::new();
        for path in paths {
            let steps = self.route(path)?;
            // `None` once the walk has left the existing nodes: everything below is new.
            let mut node = Some(&self.root);
            for (depth, step) in steps.iter().enumerate() {
                let children = node.map(Node::children);
                node = children.and_then(|children| children.child(step));
                if node.is_none() {
                    new_children
                        .entry(steps[..depth].to_vec())
                        .or_insert_with(|| (children.map_or(0, Children::len), BTreeSet::new()))
                        .1
                        .insert(*step);
                }
            }
        }
        for (parent, (existing, added)) in &new_children {
            self.check_room(parent.len(), *existing, added.len())?;
        }
        Ok(())
    }

    /// The terminals whose path satisfies every constraint, one constraint per level.
    ///
    /// Terminals come out in a deterministic order: tag children by byte order of their value,
    /// buckets in increasing order. Each terminal is returned at most once. The walk fails as soon
    /// as it would exceed one of `limits`, with [`CollectError::TooManyTerminals`] or
    /// [`CollectError::TooManyNodes`]; it never returns a partial result.
    pub fn collect_terminals<'s>(
        &'s self,
        constraints: &[Constraint<'_>],
        limits: WalkLimits,
    ) -> Result<Vec<&'s T>, CollectError> {
        let plan = self.plan(constraints)?;
        let mut walk = Walk {
            limits,
            nodes: 0,
            out: Vec::new(),
        };
        walk.visit(&self.root, &plan)?;
        Ok(walk.out)
    }

    /// Removes the terminals for which `keep` returns `false`, then every node left without
    /// children. Returns how many terminals were removed.
    ///
    /// `keep` receives each terminal mutably, so a garbage collector can compact it and drop it
    /// in the same pass when nothing is left in it.
    ///
    /// If `keep` panics, the terminals it rejected so far are removed and
    /// [`CompoundIndex::num_terminals`] counts the rest exactly. Nodes the interrupted pass left
    /// without children are dropped by the next pass.
    pub fn retain_terminals(&mut self, mut keep: impl FnMut(&mut T) -> bool) -> usize {
        let before = self.num_terminals;
        let Node::Inner(children) = &mut self.root else {
            unreachable!("{INVARIANT}");
        };
        retain_children(children, &mut keep, &mut self.num_terminals);
        before - self.num_terminals
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

    /// Whether `steps` leads to a path not inserted yet. Fails if inserting it would add a child
    /// beyond a tag level's cardinality limit.
    ///
    /// Only the first missing node can break a limit: every node below it is new, so it starts
    /// empty and each limit is at least one.
    fn check_new_path(&self, steps: &[Step<'_>]) -> Result<bool, InsertError> {
        let mut node = &self.root;
        for (level_index, step) in steps.iter().enumerate() {
            let children = node.children();
            match children.child(step) {
                Some(child) => node = child,
                None => {
                    self.check_room(level_index, children.len(), 1)?;
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Fails if a node on `level_index` holding `existing` children cannot take `added` more.
    fn check_room(
        &self,
        level_index: usize,
        existing: usize,
        added: usize,
    ) -> Result<(), InsertError> {
        if let Level::Tag(tag) = &self.levels[level_index]
            && let Some(max) = tag.max_cardinality()
            && existing + added > max.get()
        {
            return Err(InsertError::CardinalityExceeded {
                level: level_index,
                max: max.get(),
            });
        }
        Ok(())
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
struct Walk<'s, T> {
    limits: WalkLimits,
    /// Nodes entered so far, terminals included, the root excluded.
    nodes: usize,
    out: Vec<&'s T>,
}

impl<'s, T> Walk<'s, T> {
    fn enter(&mut self, node: &'s Node<T>, plan: &[LevelPlan<'_>]) -> Result<(), CollectError> {
        if self.nodes == self.limits.max_nodes {
            return Err(CollectError::TooManyNodes {
                max: self.limits.max_nodes,
            });
        }
        self.nodes += 1;
        self.visit(node, plan)
    }

    fn visit(&mut self, node: &'s Node<T>, plan: &[LevelPlan<'_>]) -> Result<(), CollectError> {
        let children = match node {
            Node::Terminal(terminal) => {
                if self.out.len() == self.limits.max_terminals {
                    return Err(CollectError::TooManyTerminals {
                        max: self.limits.max_terminals,
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

/// Removes the rejected terminals below `children`, decrementing `num_terminals` for each as
/// it goes, so the count stays exact if `keep` panics.
fn retain_children<T>(
    children: &mut Children<T>,
    keep: &mut impl FnMut(&mut T) -> bool,
    num_terminals: &mut usize,
) {
    match children {
        Children::Tag(map) => {
            // The trie has no mutable iterator, so walk its keys and look each one up.
            let keys: Vec<Vec<u8>> = map.iter().map(|(key, _)| key).collect();
            for key in keys {
                let child = map.find_mut(&key).expect("the key was just listed");
                if let Fate::Drop = retain_node(child, keep, num_terminals) {
                    map.remove(&key);
                }
            }
        }
        Children::Buckets(slots) => {
            for slot in slots.iter_mut() {
                if let Some(child) = slot
                    && let Fate::Drop = retain_node(child, keep, num_terminals)
                {
                    *slot = None;
                }
            }
        }
    }
}

fn retain_node<T>(
    node: &mut Node<T>,
    keep: &mut impl FnMut(&mut T) -> bool,
    num_terminals: &mut usize,
) -> Fate {
    match node {
        Node::Terminal(terminal) => {
            if keep(terminal) {
                Fate::Keep
            } else {
                *num_terminals -= 1;
                Fate::Drop
            }
        }
        Node::Inner(children) => {
            retain_children(children, keep, num_terminals);
            if children.is_empty() {
                Fate::Drop
            } else {
                Fate::Keep
            }
        }
    }
}
