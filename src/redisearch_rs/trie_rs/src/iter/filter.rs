//! Utilities to control which nodes are visited during iteration.
use std::ffi::c_char;

#[derive(Clone, Copy)]
/// The outcome of [`TraversalFilter::filter`].
pub struct FilterOutcome {
    /// If `false`, the key and value associated with the current
    /// node won't be yielded by the iterator.
    pub yield_current: bool,
    /// If `false`, the entire subtree rooted in the current node
    /// will be skipped.
    pub visit_descendants: bool,
}

/// A mechanism to control which nodes are visited during iteration.
pub trait TraversalFilter {
    /// Determine whether the current node should be yielded
    /// and whether its descendants should be visited.
    ///
    /// The filter takes as an input the key associated with
    /// the current node—i.e. the concatenation of the
    /// labels associated with every node between the
    /// root of the trie and the current one.
    fn filter(&self, key: &[c_char]) -> FilterOutcome;
}

/// The simplest filter: visit all nodes, no exceptions.
pub struct VisitAll;

impl TraversalFilter for VisitAll {
    fn filter(&self, _key: &[c_char]) -> FilterOutcome {
        FilterOutcome {
            yield_current: true,
            visit_descendants: true,
        }
    }
}
