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
    /// Whether to include the minimum term in the result set, if the trie contains it.
    ///
    /// It is only taken into account if the range specifies a minimum boundary.
    is_min_included: bool,
    /// Whether to include the maximum term in the result set, if the trie contains it.
    ///
    /// It is only taken into account if the range specifies a maximum boundary.
    is_max_included: bool,
}

#[derive(Clone, Copy, Debug)]
/// One of the bounds for a [`RangeFilter`].
pub struct RangeBoundary<'a> {
    pub value: &'a [u8],
    pub is_included: bool,
}

impl<'a> RangeBoundary<'a> {
    /// Create a new range boundary that includes its boundary value.
    pub fn included(value: &'a [u8]) -> Self {
        Self {
            value,
            is_included: true,
        }
    }

    /// Create a new range boundary that doesn't include its boundary value.
    pub fn excluded(value: &'a [u8]) -> Self {
        Self {
            value,
            is_included: false,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct RangeFilter<'a> {
    pub min: Option<RangeBoundary<'a>>,
    pub max: Option<RangeBoundary<'a>>,
}

impl RangeFilter<'_> {
    /// A filter that matches all entries.
    pub fn all() -> Self {
        Self {
            min: None,
            max: None,
        }
    }
}

impl std::fmt::Display for RangeFilter<'_> {
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
    min: Option<&'a [u8]>,
    max: Option<&'a [u8]>,
}

impl<'tm, Data> RangeIter<'tm, Data> {
    /// Creates a new iterator over the entries of a [`TrieMap`](crate::TrieMap).
    pub(crate) fn new(root: Option<&'tm Node<Data>>, filter: RangeFilter<'tm>) -> Self {
        let Some(root) = root else {
            return Self::empty();
        };
        let (Some(min), Some(max)) = (filter.min, filter.max) else {
            // If the range is only bounded on one side, we need to start from the root.
            return RangeIter::filtered(Some(root), vec![], filter);
        };
        // If the minimum and the maximum share a prefix, we can skip directly
        // to the subtree of the terms under that prefix.
        let prefix = match longest_common_prefix(min.value, max.value) {
            Some((0, _)) => {
                // No common prefix between the boundaries of the range,
                // therefore we start from the root.
                return RangeIter::filtered(Some(root), vec![], filter);
            }
            Some((equal_up_to, _)) => &min.value[..equal_up_to],
            None => {
                if min.value.len() > max.value.len() {
                    // The maximum is a prefix of the minimum!
                    // Nothing to find here, the result set is empty.
                    return RangeIter::empty();
                } else {
                    min.value
                }
            }
        };
        let Some((subroot, subroot_prefix)) = root.find_root_for_prefix(prefix) else {
            // No term in the trie has that prefix. The result set is empty.
            return RangeIter::empty();
        };

        // Shorten the boundaries. The minimum may be gone entirely.
        let filter = RangeFilter {
            min: (subroot_prefix.len() != min.value.len()).then(|| RangeBoundary {
                value: &min.value[subroot_prefix.len()..],
                is_included: min.is_included,
            }),
            max: Some(RangeBoundary {
                value: &max.value[subroot_prefix.len()..],
                is_included: max.is_included,
            }),
        };
        RangeIter::filtered(Some(subroot), subroot_prefix, filter)
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
    fn filtered(root: Option<&'tm Node<Data>>, prefix: Vec<u8>, range: RangeFilter<'tm>) -> Self {
        Self {
            stack: root
                .into_iter()
                .map(|node| StackEntry {
                    node,
                    was_visited: false,
                    min: range.min.map(|m| m.value),
                    max: range.max.map(|m| m.value),
                })
                .collect(),
            key: prefix,
            is_min_included: range.min.map(|m| m.is_included).unwrap_or(false),
            is_max_included: range.max.map(|m| m.is_included).unwrap_or(false),
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
                min,
                max,
            } = self.stack.pop()?;

            if !was_visited {
                self.stack.push(StackEntry {
                    node,
                    was_visited: true,
                    min,
                    max,
                });
                self.key.extend(node.label());

                let mut child_min = None;
                let mut child_max = None;
                let mut yield_current = true;
                let mut visit_descendants = true;

                if let Some(min) = min {
                    match longest_common_prefix(min, node.label()) {
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
                        None => match min.len().cmp(&(node.label_len() as usize)) {
                            Ordering::Less => {
                                // The minimum is a prefix of the current label.
                                // This node and all its descendants are greater than the minimum.
                            }
                            Ordering::Equal => {
                                // The minimum is identical to the current label.
                                // All the descendants of this node are greater than the minimum.
                                if !self.is_min_included {
                                    yield_current = false
                                }
                            }
                            Ordering::Greater => {
                                // The current label is a prefix of the minimum.
                                yield_current = false;
                                // We need to compare the label of the descendants against the
                                // remaining suffix.
                                child_min = Some(&min[node.label_len() as usize..]);
                            }
                        },
                    }
                }

                if let Some(max) = max {
                    match longest_common_prefix(max, node.label()) {
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
                        None => match max.len().cmp(&(node.label_len() as usize)) {
                            Ordering::Less => {
                                // The maximum is a prefix of the current label.
                                // This node and all its descendants are greater than the maximum.
                                continue;
                            }
                            Ordering::Equal => {
                                // The maximum is identical to the current label.
                                // All the descendants of this node are greater than the maximum.
                                if !self.is_max_included {
                                    yield_current = false;
                                }
                                visit_descendants = false;
                            }
                            Ordering::Greater => {
                                // The current label is a prefix of the maximum.
                                // We need to compare the descendants against the remaining prefix.
                                child_max = Some(&max[node.label_len() as usize..]);
                            }
                        },
                    }
                }

                if visit_descendants {
                    self.stack.reserve(node.children().len());

                    let mut max_index = node.children().len();
                    if let Some(max) = child_max {
                        if let Some(first) = max.first() {
                            let i = match node.children_first_bytes().binary_search(first) {
                                Ok(i) => {
                                    self.stack.push(StackEntry {
                                        node: &node.children()[i],
                                        was_visited: false,
                                        min: child_min,
                                        max: child_max,
                                    });
                                    i
                                }
                                Err(i) => i,
                            };
                            max_index = i;
                        }
                    }

                    let mut min_index = 0;

                    let mut min_entry = None;
                    if let Some(min) = child_min {
                        if let Some(first) = min.first() {
                            match node.children_first_bytes()[..max_index].binary_search(first) {
                                Ok(i) => {
                                    min_entry = Some(StackEntry {
                                        node: &node.children()[i],
                                        was_visited: false,
                                        min: child_min,
                                        max: None,
                                    });
                                    min_index = i + 1;
                                }
                                Err(i) => {
                                    min_index = i;
                                }
                            }
                        }
                    }

                    for child in node
                        .children()
                        .iter()
                        .skip(min_index)
                        .rev()
                        .skip(node.children().len() - max_index)
                    {
                        self.stack.push(StackEntry {
                            node: child,
                            was_visited: false,
                            min: None,
                            max: None,
                        });
                    }

                    if let Some(min_child) = min_entry {
                        self.stack.push(min_child);
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
