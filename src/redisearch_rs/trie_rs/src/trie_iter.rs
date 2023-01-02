pub(crate) trait TrieIterator {
    type Item<'a>: Clone
    where
        Self: 'a;

    fn next<'a>(&'a mut self) -> Option<Self::Item<'a>>;
}
