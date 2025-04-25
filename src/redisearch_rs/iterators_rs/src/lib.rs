//! This module defines the iterator types and traits for the Redisearch library.

mod empty_iterator;
mod id_list_iterator;

pub type DocId = u64;

pub enum IteratorType {
    ReadIterator,       // 3
    HybridIterator,
    UnionIterator,
    IntersectIterator,
    NotIterator,
    OptionalIterator,
    WildcardIterator,
    EmptyIterator,      // 1
    IdListIterator,     // 2
    MetricIterator,
    ProfileIterator,
    OptimusIterator,
    MaxIterator,
}

pub enum IteratorStatus {
    IteratorOk,
    IteratorNotfound,
    IteratorEof,
    IteratorTimeout,
}

pub trait QueryIterator  {
    type Item; // RSIndexResult in C or later ported type in Rust

    // operations from v2 api of Guy
    fn read(&mut self) -> IteratorStatus;
    fn num_estimated(&self) -> usize;
    fn skip_to(&mut self, doc_id: DocId) -> IteratorStatus;
    fn rewind(&mut self);

    /// access the RSIndexResult or None
    fn current(&self) -> Option<Self::Item>;
}

pub struct RustifiedIterator<I: QueryIterator> {
    inner: I,
}

impl<I: QueryIterator> RustifiedIterator<I> {
    pub fn current(&self) -> Option<I::Item> {
        self.inner.current()
    }
}

impl<I: QueryIterator> Iterator for RustifiedIterator<I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.read() {
            IteratorStatus::IteratorOk => self.current(),
            _ => None,
        }
    }
}
