//! Mock iterator for testing purposes and for starting the work on several iterators without blocking if we scale up the team
//!
//! See iterator_util.h of Guy for a Mock implementation in C, impl trait QueryIterator.

pub struct MockIterator {
    pub doc_ids: Vec<DocId>,
    pub current_index: usize,
    pub read_count: usize,
    pub when_done: IteratorStatus,
}