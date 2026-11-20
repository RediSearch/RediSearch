/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ops::Range;

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
#[derive(Clone, Copy)]
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

impl<T> Children<T> {
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
            let Node::Inner(children) = node else {
                unreachable!("{INVARIANT}");
            };
            let child = match (children, step) {
                (Children::Tag(map), Step::Tag(value)) => map.find(value),
                (Children::Buckets(slots), Step::Bucket(bucket)) => slots[*bucket].as_ref(),
                _ => unreachable!("{INVARIANT}"),
            };
            match child {
                Some(child) => node = &**child,
                None => return Ok(None),
            }
        }
        match node {
            Node::Terminal(terminal) => Ok(Some(terminal)),
            Node::Inner(_) => unreachable!("{INVARIANT}"),
        }
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
        self.check_cardinality(&steps)?;

        let levels = &self.levels;
        let mut init = Some(init);
        let mut created = false;
        let mut new_node = |depth: usize| -> Box<Node<T>> {
            Box::new(match levels.get(depth) {
                Some(level) => Node::Inner(Children::for_level(level)),
                None => {
                    created = true;
                    let init = init
                        .take()
                        .expect("only the terminal depth consumes `init`");
                    Node::Terminal(init())
                }
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
        if created {
            self.num_terminals += 1;
        }
        match node {
            Node::Terminal(terminal) => Ok(terminal),
            Node::Inner(_) => unreachable!("{INVARIANT}"),
        }
    }

    /// The terminals whose path satisfies every constraint, one constraint per level.
    ///
    /// Terminals come out in a deterministic order: tag children by byte order of their value,
    /// buckets in increasing order. Each terminal is returned at most once. The walk fails with
    /// [`CollectError::TooManyTerminals`] as soon as it would return more than `max_terminals`;
    /// it never returns a partial result.
    pub fn collect_terminals<'s>(
        &'s self,
        constraints: &[Constraint<'_>],
        max_terminals: usize,
    ) -> Result<Vec<&'s T>, CollectError> {
        let plan = self.plan(constraints)?;
        let mut out = Vec::new();
        collect(&self.root, &plan, max_terminals, &mut out)?;
        Ok(out)
    }

    /// Removes the terminals for which `keep` returns `false`, then every node left without
    /// children. Returns how many terminals were removed.
    ///
    /// `keep` receives each terminal mutably, so a garbage collector can compact it and drop it
    /// in the same pass when nothing is left in it.
    pub fn retain_terminals(&mut self, mut keep: impl FnMut(&mut T) -> bool) -> usize {
        let Node::Inner(children) = &mut self.root else {
            unreachable!("{INVARIANT}");
        };
        let removed = retain_children(children, &mut keep);
        self.num_terminals -= removed;
        removed
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

    /// Fails if inserting `steps` would add a child beyond a tag level's cardinality limit.
    ///
    /// Only the first missing node can break a limit: every node below it is new, so it starts
    /// empty and each limit is at least one.
    fn check_cardinality(&self, steps: &[Step<'_>]) -> Result<(), InsertError> {
        let mut node = &self.root;
        for (level_index, step) in steps.iter().enumerate() {
            let Node::Inner(children) = node else {
                unreachable!("{INVARIANT}");
            };
            let child = match (children, step) {
                (Children::Tag(map), Step::Tag(value)) => match map.find(value) {
                    Some(child) => child,
                    None => {
                        let Level::Tag(tag) = &self.levels[level_index] else {
                            unreachable!("{INVARIANT}");
                        };
                        if let Some(max) = tag.max_cardinality()
                            && map.n_unique_keys() >= max.get()
                        {
                            return Err(InsertError::CardinalityExceeded {
                                level: level_index,
                                max: max.get(),
                            });
                        }
                        return Ok(());
                    }
                },
                (Children::Buckets(slots), Step::Bucket(bucket)) => match &slots[*bucket] {
                    Some(child) => child,
                    None => return Ok(()),
                },
                _ => unreachable!("{INVARIANT}"),
            };
            node = &**child;
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

/// The buckets covered by `range`, or [`None`] if an end is not aligned on a boundary.
fn bucket_span(buckets: &BucketLevel, range: BucketRange) -> Option<Range<usize>> {
    let boundaries = buckets.boundaries();
    let first = boundaries[0];
    let last = boundaries[boundaries.len() - 1];
    // Either end may lie outside the buckets: below the first boundary or above the last one.
    let position = |value: f64| {
        if value <= first {
            Some(0)
        } else if value >= last {
            Some(buckets.num_buckets())
        } else {
            buckets.boundary_position(value)
        }
    };
    let start = position(range.start)?;
    let end = position(range.end)?;
    // An empty or inverted range covers no bucket.
    Some(start..end.max(start))
}

fn collect<'s, T>(
    node: &'s Node<T>,
    plan: &[LevelPlan<'_>],
    max_terminals: usize,
    out: &mut Vec<&'s T>,
) -> Result<(), CollectError> {
    let children = match node {
        Node::Terminal(terminal) => {
            if out.len() == max_terminals {
                return Err(CollectError::TooManyTerminals { max: max_terminals });
            }
            out.push(terminal);
            return Ok(());
        }
        Node::Inner(children) => children,
    };
    let (level_plan, rest) = plan.split_first().expect(INVARIANT);
    let mut visit = |child: &'s Node<T>| collect(child, rest, max_terminals, out);
    match (children, level_plan) {
        (Children::Tag(map), LevelPlan::Any) => {
            for (_, child) in map.iter() {
                visit(child)?;
            }
        }
        (Children::Tag(map), LevelPlan::TagIn(values)) => {
            for child in values.iter().filter_map(|value| map.find(value)) {
                visit(child)?;
            }
        }
        (Children::Tag(map), LevelPlan::TagNotIn(values)) => {
            for (value, child) in map.iter() {
                if values.binary_search(&value.as_slice()).is_err() {
                    visit(child)?;
                }
            }
        }
        (Children::Tag(map), LevelPlan::TagPrefix(prefix)) => {
            for (_, child) in map.prefixed_iter(prefix) {
                visit(child)?;
            }
        }
        (Children::Buckets(slots), LevelPlan::Any) => {
            for child in slots.iter().flatten() {
                visit(child)?;
            }
        }
        (Children::Buckets(slots), LevelPlan::Buckets(span)) => {
            for child in slots[span.clone()].iter().flatten() {
                visit(child)?;
            }
        }
        _ => unreachable!("constraints were checked against their levels"),
    }
    Ok(())
}

fn retain_children<T>(children: &mut Children<T>, keep: &mut impl FnMut(&mut T) -> bool) -> usize {
    let mut removed = 0;
    match children {
        Children::Tag(map) => {
            // The trie has no mutable iterator, so walk its keys and look each one up.
            let keys: Vec<Vec<u8>> = map.iter().map(|(key, _)| key).collect();
            for key in keys {
                let child = map.find_mut(&key).expect("the key was just listed");
                let (fate, removed_below) = retain_node(child, keep);
                removed += removed_below;
                if let Fate::Drop = fate {
                    map.remove(&key);
                }
            }
        }
        Children::Buckets(slots) => {
            for slot in slots.iter_mut() {
                if let Some(child) = slot {
                    let (fate, removed_below) = retain_node(child, keep);
                    removed += removed_below;
                    if let Fate::Drop = fate {
                        *slot = None;
                    }
                }
            }
        }
    }
    removed
}

fn retain_node<T>(node: &mut Node<T>, keep: &mut impl FnMut(&mut T) -> bool) -> (Fate, usize) {
    match node {
        Node::Terminal(terminal) => {
            if keep(terminal) {
                (Fate::Keep, 0)
            } else {
                (Fate::Drop, 1)
            }
        }
        Node::Inner(children) => {
            let removed = retain_children(children, keep);
            let fate = if children.is_empty() {
                Fate::Drop
            } else {
                Fate::Keep
            };
            (fate, removed)
        }
    }
}
