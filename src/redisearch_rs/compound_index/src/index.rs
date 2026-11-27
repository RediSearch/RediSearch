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
};

use lending_iterator::LendingIterator;
use timeout::{TimeoutCheckResult, TimeoutChecker};
use trie_rs::TrieMap;

use crate::{CardinalityExceeded, CollectError, Constraint, SchemaError, TagLevel};

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
    levels: Box<[TagLevel]>,
    root: Node<T>,
    num_terminals: usize,
}

/// A node at some depth of the index. Nodes above the last level are [`Node::Inner`], with their
/// children keyed by tag value. Nodes at depth [`CompoundIndex::levels`]`.len()` are
/// [`Node::Terminal`].
enum Node<T> {
    Inner(TrieMap<Box<Node<T>>>),
    Terminal(T),
}

const INVARIANT: &str = "a node's kind matches its depth, and every path has one key per level";

impl<T> Node<T> {
    /// The children of an inner node. Only called on nodes above the terminal depth.
    fn children(&self) -> &TrieMap<Box<Self>> {
        match self {
            Self::Inner(children) => children,
            Self::Terminal(_) => unreachable!("{INVARIANT}"),
        }
    }

    /// The child keyed by `value`, if it exists.
    fn child(&self, value: &[u8]) -> Option<&Self> {
        self.children().find(value).map(|child| &**child)
    }
}

impl<T> CompoundIndex<T> {
    /// An empty index with the given routing levels, outermost first.
    pub fn new(levels: Vec<TagLevel>) -> Result<Self, SchemaError> {
        if levels.is_empty() {
            return Err(SchemaError::NoLevels);
        }
        if levels.len() > MAX_LEVELS {
            return Err(SchemaError::TooManyLevels {
                max: MAX_LEVELS,
                got: levels.len(),
            });
        }
        Ok(Self {
            levels: levels.into_boxed_slice(),
            root: Node::Inner(TrieMap::new()),
            num_terminals: 0,
        })
    }

    /// The routing levels, outermost first.
    pub fn levels(&self) -> &[TagLevel] {
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
    ///
    /// # Panics
    ///
    /// If `path` does not have one value per level.
    pub fn get(&self, path: &[&[u8]]) -> Option<&T> {
        self.assert_path(path);
        self.find(path)
    }

    /// The terminal at the end of `path`, created with `init` if the path is new: the route step
    /// of [route, then post](crate#route-then-post).
    ///
    /// Creating the path adds every missing node on the way. The cardinality limits are checked
    /// before anything is added, so an error leaves the index unchanged.
    ///
    /// # Panics
    ///
    /// If `path` does not have one value per level.
    ///
    /// # Examples
    ///
    /// ```
    /// use compound_index::{CompoundIndex, TagLevel};
    ///
    /// // Terminals are doc-id lists; levels are `color`, then `size`.
    /// let mut index: CompoundIndex<Vec<u32>> =
    ///     CompoundIndex::new(vec![TagLevel::new(), TagLevel::new()]).unwrap();
    /// let path: [&[u8]; 2] = [b"red", b"M"];
    ///
    /// // Route, then post.
    /// index.get_or_insert_with(&path, Vec::new).unwrap().push(7);
    /// // Same path, so the same terminal: `init` does not run again.
    /// index.get_or_insert_with(&path, Vec::new).unwrap().push(9);
    ///
    /// assert_eq!(index.get(&path), Some(&vec![7, 9]));
    /// assert_eq!(index.num_terminals(), 1);
    /// ```
    pub fn get_or_insert_with(
        &mut self,
        path: &[&[u8]],
        init: impl FnOnce() -> T,
    ) -> Result<&mut T, CardinalityExceeded> {
        self.assert_path(path);
        // The same check as `check_paths`, so a batch it accepts always inserts here.
        if let Some(exceeded) = self.exceeded_cardinality(&[path]) {
            return Err(exceeded);
        }
        let is_new = self.find(path).is_none();
        // Built before anything is inserted, so a panicking `init` leaves the index unchanged.
        let mut terminal = is_new.then(init);

        let depth_of_terminals = self.levels.len();
        let mut node = &mut self.root;
        for (depth, value) in path.iter().enumerate() {
            let Node::Inner(children) = node else {
                unreachable!("{INVARIANT}");
            };
            if children.find(value).is_none() {
                let child = if depth + 1 == depth_of_terminals {
                    Node::Terminal(
                        terminal
                            .take()
                            .expect("a new path reaches the terminal depth once"),
                    )
                } else {
                    Node::Inner(TrieMap::new())
                };
                children.insert(value, Box::new(child));
            }
            node = &mut **children.find_mut(value).expect("present or inserted above");
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
    /// Without any cardinality limit the tree is not read: the outcome depends on the levels
    /// alone, so it holds whatever has been inserted.
    ///
    /// # Panics
    ///
    /// If a path does not have one value per level.
    pub fn check_paths(&self, paths: &[&[&[u8]]]) -> Result<(), CardinalityExceeded> {
        for path in paths {
            self.assert_path(path);
        }
        self.exceeded_cardinality(paths).map_or(Ok(()), Err)
    }

    /// The terminals whose path satisfies every constraint, one constraint per level.
    ///
    /// Terminals come out in byte order of their path. Each terminal is returned at most once.
    /// The walk never returns a partial result: it fails with [`CollectError::TooManyTerminals`]
    /// as soon as it would return more than `max_terminals`, and with [`CollectError::TimedOut`]
    /// as soon as `timeout` reports a timeout.
    ///
    /// `max_terminals` bounds the result, so the memory of whatever the caller opens per terminal.
    /// It does not bound the work: a level the query leaves open is entered through every child,
    /// even when nothing matches below it. `timeout` bounds that, and is checked once per child
    /// the walk considers: every node entered, and every tag value looked up or skipped without
    /// entering a node.
    ///
    /// # Panics
    ///
    /// If `constraints` does not have one constraint per level.
    pub fn collect_terminals<'s>(
        &'s self,
        constraints: &[Constraint<'_>],
        max_terminals: usize,
        timeout: &mut impl TimeoutChecker,
    ) -> Result<Vec<&'s T>, CollectError> {
        assert_eq!(
            constraints.len(),
            self.levels.len(),
            "a walk takes one constraint per level"
        );
        let mut walk = Walk {
            max_terminals,
            timeout,
            out: Vec::new(),
        };
        walk.visit(&self.root, constraints)?;
        Ok(walk.out)
    }

    /// A path of another length would stop above the terminals or run past them, so it is a
    /// caller's bug rather than data to reject.
    fn assert_path(&self, path: &[&[u8]]) {
        assert_eq!(
            path.len(),
            self.levels.len(),
            "a path has one value per level"
        );
    }

    /// The terminal at the end of `path`, if that path was inserted.
    fn find(&self, path: &[&[u8]]) -> Option<&T> {
        let mut node = &self.root;
        for value in path {
            node = node.child(value)?;
        }
        match node {
            Node::Terminal(terminal) => Some(terminal),
            Node::Inner(_) => unreachable!("{INVARIANT}"),
        }
    }

    /// The level and its limit, if inserting every path of `paths` would give a node more
    /// children than that level's cardinality limit. The one write-side check:
    /// [`CompoundIndex::check_paths`] runs it on a batch and
    /// [`CompoundIndex::get_or_insert_with`] on its single path, so they cannot disagree.
    fn exceeded_cardinality(&self, paths: &[&[&[u8]]]) -> Option<CardinalityExceeded> {
        // `check_paths` promises not to read the tree when no level has a limit.
        if (0..self.levels.len()).all(|level| self.max_cardinality(level).is_none()) {
            return None;
        }
        // A parent on a level with a limit → its existing children and the new ones the batch
        // adds. A level without a limit accepts any child, so it is not tracked.
        let mut new_children: BTreeMap<&[&[u8]], NewChildren<'_>> = BTreeMap::new();
        for path in paths {
            // `None` once the walk has left the existing nodes: everything below is new.
            let mut node = Some(&self.root);
            for (depth, value) in path.iter().enumerate() {
                let parent = node;
                node = parent.and_then(|parent| parent.child(value));
                if node.is_none() && self.max_cardinality(depth).is_some() {
                    new_children
                        .entry(&path[..depth])
                        .or_insert_with(|| {
                            let existing =
                                parent.map_or(0, |parent| parent.children().n_unique_keys());
                            (existing, BTreeSet::new())
                        })
                        .1
                        .insert(value);
                }
            }
        }
        for (parent, (existing, added)) in &new_children {
            let level = parent.len();
            let max = self
                .max_cardinality(level)
                .expect("only levels with a limit are tracked");
            if existing + added.len() > max.get() {
                return Some(CardinalityExceeded {
                    level,
                    max: max.get(),
                });
            }
        }
        None
    }

    /// The cardinality limit of the level at `level_index`, if it has one.
    fn max_cardinality(&self, level_index: usize) -> Option<NonZeroUsize> {
        self.levels[level_index].max_cardinality()
    }
}

/// A parent's existing number of children, and the new values a batch adds under it.
type NewChildren<'k> = (usize, BTreeSet<&'k [u8]>);

/// The state of one [`CompoundIndex::collect_terminals`] walk.
struct Walk<'s, 't, T, C> {
    max_terminals: usize,
    timeout: &'t mut C,
    out: Vec<&'s T>,
}

impl<'s, T, C: TimeoutChecker> Walk<'s, '_, T, C> {
    /// Counts one child considered against the timeout. A tag value that matches no child, or a
    /// child a negation skips, costs a check as an entered node does, so a walk that enters
    /// nothing still checks once per candidate.
    fn consider(&mut self) -> Result<(), CollectError> {
        match self.timeout.check_timeout() {
            TimeoutCheckResult::TimedOut => Err(CollectError::TimedOut),
            TimeoutCheckResult::Ok => Ok(()),
        }
    }

    /// Visits a child, after checking the timeout. The root is visited without a check, so
    /// every node entered below it, terminals included, costs one check.
    fn enter(&mut self, node: &'s Node<T>, rest: &[Constraint<'_>]) -> Result<(), CollectError> {
        self.consider()?;
        self.visit(node, rest)
    }

    fn visit(
        &mut self,
        node: &'s Node<T>,
        constraints: &[Constraint<'_>],
    ) -> Result<(), CollectError> {
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
        let (constraint, rest) = constraints.split_first().expect(INVARIANT);
        match constraint {
            Constraint::Any => {
                for child in children.values() {
                    self.enter(child, rest)?;
                }
            }
            Constraint::TagIn(values) => {
                for value in values.values() {
                    match children.find(value) {
                        Some(child) => self.enter(child, rest)?,
                        None => self.consider()?,
                    }
                }
            }
            Constraint::TagNotIn(values) => {
                // Borrows each key instead of copying it, as `TrieMap::iter` would.
                let mut entries = children.lending_iter();
                while let Some((value, child)) = entries.next() {
                    if values.contains(value) {
                        self.consider()?;
                    } else {
                        self.enter(child, rest)?;
                    }
                }
            }
            Constraint::TagPrefix(prefix) => {
                for child in children.prefixed_values(prefix) {
                    self.enter(child, rest)?;
                }
            }
        }
        Ok(())
    }
}
