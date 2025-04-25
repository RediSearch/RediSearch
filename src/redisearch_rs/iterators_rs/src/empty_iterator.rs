use crate::{DocId, IteratorStatus, QueryIterator};

// Create a static instance
pub const EMPTY_ITERATOR: EmptyIterator = EmptyIterator {};

pub struct EmptyIterator;

impl QueryIterator for EmptyIterator {
    type Item = ();

    fn read(&mut self) -> IteratorStatus {
        IteratorStatus::IteratorEof
    }

    fn num_estimated(&self) -> usize {
        0
    }

    fn skip_to(&mut self, _doc_id: DocId) -> IteratorStatus {
        IteratorStatus::IteratorEof
    }

    fn rewind(&mut self) {}

    fn current(&self) -> Option<Self::Item> {
        None
    }
}
