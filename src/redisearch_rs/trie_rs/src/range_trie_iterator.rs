use crate::trie::Node;
use crate::trie::Trie;

use crate::trie_iter::TrieIterator;

#[derive(Clone)]
struct RangeBound<'trie> {
    val: &'trie [u8],
    include_val: bool,
}

#[derive(Clone)]
struct Range<'trie> {
    min: Option<RangeBound<'trie>>,
    max: Option<RangeBound<'trie>>,
}

enum CompareResult<'trie> {
    Inside((Option<RangeBound<'trie>>, Option<RangeBound<'trie>>)),
    OutsideContinue((Option<RangeBound<'trie>>, Option<RangeBound<'trie>>)),
    Outside,
}

trait Comparable<'trie> {
    fn compare(self, val: &[u8]) -> CompareResult<'trie>;
}

impl<'trie> Comparable<'trie> for Option<Range<'trie>> {
    fn compare(self, val: &[u8]) -> CompareResult<'trie> {
        if self.is_none() {
            return CompareResult::Inside((None, None));
        }
        let range = self.unwrap();

        let (min_bound, in_min_range) = if let Some(min) = range.min {
            let min_slice = min.val;
            if min_slice > val {
                if min_slice.starts_with(val) {
                    (Some(min), false)
                } else {
                    return CompareResult::Outside;
                }
            } else {
                (
                    None,
                    if min_slice == val {
                        min.include_val
                    } else {
                        true
                    },
                )
            }
        } else {
            (None, true)
        };

        let (max_bound, in_max_range) = if let Some(max) = range.max {
            let max_slice = max.val;
            if max_slice < val {
                return CompareResult::Outside;
            } else if max_slice.starts_with(val) {
                let in_max_range = max.include_val;
                (
                    Some(max),
                    if max_slice.len() == val.len() {
                        in_max_range
                    } else {
                        true
                    },
                )
            } else {
                (None, true)
            }
        } else {
            (None, true)
        };

        // must be in range
        if in_min_range && in_max_range {
            CompareResult::Inside((min_bound, max_bound))
        } else {
            CompareResult::OutsideContinue((min_bound, max_bound))
        }
    }
}

impl<'trie> Range<'trie> {
    fn new(min: Option<RangeBound<'trie>>, max: Option<RangeBound<'trie>>) -> Range<'trie> {
        Range { min, max }
    }
}

pub struct RangeTrieIterator<'trie, Data> {
    iters: Vec<Box<dyn Iterator<Item = (&'trie Node<Data>, Option<Range<'trie>>)> + 'trie>>, // the box can not outlive the trie lifetime
    prefix_buffer: Vec<u8>,
    processed_nodes: Vec<&'trie Node<Data>>,
}

impl<'trie, Data> RangeTrieIterator<'trie, Data> {
    pub(crate) fn new(
        trie: &'trie Trie<Data>,
        min: Option<&'trie [u8]>,
        include_min: bool,
        max: Option<&'trie [u8]>,
        include_max: bool,
    ) -> RangeTrieIterator<'trie, Data> {
        let range = if min.is_none() && max.is_none() {
            None
        } else {
            Some(Range {
                min: min.map(|v| RangeBound {
                    val: v,
                    include_val: include_min,
                }),
                max: max.map(|v| RangeBound {
                    val: v,
                    include_val: include_max,
                }),
            })
        };

        RangeTrieIterator {
            iters: trie
                .root
                .as_ref()
                .map(
                    move |r| -> Vec<Box<dyn Iterator<Item = (&Node<Data>, Option<Range<'_>>)>>> {
                        vec![Box::new(
                            vec![r].into_iter().map(move |v| (v, range.clone())),
                        )]
                    },
                )
                .unwrap_or_else(|| Vec::new()),
            prefix_buffer: Vec::new(),
            processed_nodes: Vec::new(),
        }
    }
}

impl<'trie, Data> TrieIterator for RangeTrieIterator<'trie, Data> {
    type Item<'a> = (&'a [u8], &'trie Data) where Self: 'a;

    fn next<'a>(&'a mut self) -> Option<Self::Item<'a>> {
        loop {
            let last_iter = self.iters.last_mut()?;
            if let Some((curr_node, range)) = last_iter.next() {
                self.prefix_buffer.extend(curr_node.val.iter());
                match range.compare(self.prefix_buffer.as_slice()) {
                    CompareResult::Inside((min_bound, max_bound)) => {
                        // still in range, lets continue
                        let new_range = if min_bound.is_some() || max_bound.is_some() {
                            Some(Range::new(min_bound, max_bound))
                        } else {
                            None
                        };
                        self.iters.push(Box::new(
                            curr_node
                                .children
                                .values()
                                .map(move |v| (v, new_range.clone())),
                        ));
                        self.processed_nodes.push(curr_node);
                        if let Some(data) = curr_node.data.as_ref() {
                            return Some((self.prefix_buffer.as_ref(), data));
                        }
                    }
                    CompareResult::OutsideContinue((min_bound, max_bound)) => {
                        let new_range = if min_bound.is_some() || max_bound.is_some() {
                            Some(Range::new(min_bound, max_bound))
                        } else {
                            None
                        };
                        self.iters.push(Box::new(
                            curr_node
                                .children
                                .values()
                                .map(move |v| (v, new_range.clone())),
                        ));
                        self.processed_nodes.push(curr_node);
                    }
                    CompareResult::Outside => {
                        // we did not continue to the node children
                        let new_size = self.prefix_buffer.len() - curr_node.val.len();
                        self.prefix_buffer.truncate(new_size);
                    }
                }
            } else {
                // no more elements in the iterator, pop the iterator and the prefix.
                self.iters.pop();
                if let Some(last_node) = self.processed_nodes.pop() {
                    let new_size = self.prefix_buffer.len() - last_node.val.len();
                    self.prefix_buffer.truncate(new_size);
                }
            }
        }
    }
}

pub struct RangeTrieRustIterator<'trie, Data> {
    inner_iter: RangeTrieIterator<'trie, Data>,
}

impl<'trie, Data> IntoIterator for RangeTrieIterator<'trie, Data> {
    type Item = (Vec<u8>, &'trie Data);
    type IntoIter = RangeTrieRustIterator<'trie, Data>;

    fn into_iter(self) -> Self::IntoIter {
        RangeTrieRustIterator { inner_iter: self }
    }
}

impl<'trie, Data> Iterator for RangeTrieRustIterator<'trie, Data> {
    type Item = (Vec<u8>, &'trie Data);

    fn next(&mut self) -> Option<Self::Item> {
        let (key, data) = self.inner_iter.next()?;
        Some((key.to_vec(), data))
    }
}
