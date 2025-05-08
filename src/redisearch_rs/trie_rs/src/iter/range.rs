/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{cmp::Ordering, fmt::Write};

use crate::{node::Node, utils::longest_common_prefix};

/// Iterates over the entries of a [`TrieMap`](crate::TrieMap) between the specified `min` and `max`,
/// in lexicographical order.
///
/// Invoke [`TrieMap::range_iter`](crate::TrieMap::iter) to create an instance of this iterator.
pub struct RangeIter<'tm, Data> {
    /// Stack of nodes and whether they have been visited.
    stack: Vec<StackEntry<'tm, Data>>,
    /// Concatenation of the labels of current node and its ancestors,
    /// i.e. the key of the current node.
    key: Vec<u8>,
}

#[derive(Clone, Copy, Debug)]
pub struct Boundary<'a> {
    pub value: &'a [u8],
    pub is_included: bool,
}

#[derive(Clone, Copy)]
pub struct RangeFilter<'a> {
    pub min: Option<Boundary<'a>>,
    pub max: Option<Boundary<'a>>,
}

impl std::fmt::Debug for RangeFilter<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(min) = self.min {
            write!(f, "{} <", String::from_utf8_lossy(min.value))?;
            if min.is_included {
                f.write_char('=')?;
            }
        }
        f.write_str(" .. ")?;
        if let Some(max) = self.max {
            let equal = if max.is_included { "=" } else { "" };
            write!(f, "<{equal} {}", String::from_utf8_lossy(max.value))?;
        }
        Ok(())
    }
}

struct StackEntry<'a, Data> {
    node: &'a Node<Data>,
    was_visited: bool,
    range: RangeFilter<'a>,
}

impl<'tm, Data> RangeIter<'tm, Data> {
    /// Creates a new iterator over the entries of a [`TrieMap`](crate::TrieMap).
    pub(crate) fn new(
        root: Option<&'tm Node<Data>>,
        prefix: Vec<u8>,
        range: RangeFilter<'tm>,
    ) -> Self {
        Self::filtered(root, prefix, range)
    }

    /// Creates a new empty iterator, that yields no entries.
    pub(crate) fn empty() -> Self {
        Self::filtered(
            None,
            vec![],
            RangeFilter {
                min: None,
                max: None,
            },
        )
    }
}

impl<'tm, Data> RangeIter<'tm, Data> {
    /// Creates a new iterator over the entries of a [`TrieMap`](crate::TrieMap).
    pub(crate) fn filtered(
        root: Option<&'tm Node<Data>>,
        prefix: Vec<u8>,
        range: RangeFilter<'tm>,
    ) -> Self {
        Self {
            stack: root
                .into_iter()
                .map(|node| StackEntry {
                    node,
                    was_visited: false,
                    range,
                })
                .collect(),
            key: prefix,
        }
    }

    /// The current key, obtained by concatenating the labels of the nodes
    /// between the root and the current node.
    pub(crate) fn key(&self) -> &[u8] {
        &self.key
    }

    /// Advance this iterator to the next node, and set the
    /// key to the one matching that node's entry
    pub(crate) fn advance(&mut self) -> Option<&'tm Data> {
        loop {
            let StackEntry {
                node,
                was_visited,
                range,
            } = self.stack.pop()?;

            if !was_visited {
                self.stack.push(StackEntry {
                    node,
                    was_visited: true,
                    range,
                });
                self.key.extend(node.label());

                let mut child_range = RangeFilter {
                    min: None,
                    max: None,
                };

                let mut yield_current = true;
                let mut visit_descendants = true;

                if let Some(min_boundary) = range.min {
                    match longest_common_prefix(min_boundary.value, node.label()) {
                        Some((_, ord)) => match ord {
                            Ordering::Less => {
                                // This node and all its descendants are greater than the minimum
                            }
                            Ordering::Greater => {
                                // This node and all its descendants are smaller than the minimum,
                                // hence they can be skipped.
                                continue;
                            }
                            Ordering::Equal => unreachable!(),
                        },
                        None => match min_boundary.value.len().cmp(&(node.label_len() as usize)) {
                            Ordering::Less => {
                                // The minimum is a prefix of the current label.
                                // This node and all its descendants are greater than the minimum.
                            }
                            Ordering::Equal => {
                                // The minimum is identical to the current label.
                                // All the descendants of this node are greater than the minimum.
                                if !min_boundary.is_included {
                                    yield_current = false
                                }
                            }
                            Ordering::Greater => {
                                // The current label is a prefix of the minimum.
                                yield_current = false;
                                // We need to compare the label of the descendants against the
                                // remaining suffix.
                                child_range.min = Some(Boundary {
                                    value: &min_boundary.value[node.label_len() as usize..],
                                    is_included: min_boundary.is_included,
                                })
                            }
                        },
                    }
                }

                if let Some(max_boundary) = range.max {
                    match longest_common_prefix(max_boundary.value, node.label()) {
                        Some((_, ord)) => match ord {
                            Ordering::Less => {
                                // This node and all its descendants are greater than the maximum
                                // hence they can be skipped.
                                continue;
                            }
                            Ordering::Greater => {
                                // This node and all its descendants are smaller than the maximum,
                            }
                            Ordering::Equal => unreachable!(),
                        },
                        None => match max_boundary.value.len().cmp(&(node.label_len() as usize)) {
                            Ordering::Less => {
                                // The maximum is a prefix of the current label.
                                // This node and all its descendants are greater than the maximum.
                                continue;
                            }
                            Ordering::Equal => {
                                // The maximum is identical to the current label.
                                // All the descendants of this node are greater than the maximum.
                                if !max_boundary.is_included {
                                    yield_current = false;
                                }
                                visit_descendants = false;
                            }
                            Ordering::Greater => {
                                // The current label is a prefix of the maximum.
                                // We need to compare the descendants against the remaining prefix.
                                child_range.max = Some(Boundary {
                                    value: &max_boundary.value[node.label_len() as usize..],
                                    is_included: max_boundary.is_included,
                                })
                            }
                        },
                    }
                }

                if visit_descendants {
                    for child in node.children().iter().rev() {
                        self.stack.push(StackEntry {
                            node: child,
                            was_visited: false,
                            range: child_range,
                        });
                    }
                }

                if yield_current {
                    if let Some(data) = node.data() {
                        return Some(data);
                    }
                }
            } else {
                self.key
                    .truncate(self.key.len() - node.label_len() as usize);
            }
        }
    }
}

impl<'tm, Data> Iterator for RangeIter<'tm, Data> {
    type Item = (Vec<u8>, &'tm Data);

    fn next(&mut self) -> Option<Self::Item> {
        self.advance().map(|d| (self.key.clone(), d))
    }
}
