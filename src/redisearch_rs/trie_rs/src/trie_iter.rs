pub(crate) trait TrieIterator {
    type Item<'a>: Clone
    where
        Self: 'a;

    fn next(&mut self) -> Option<Self::Item<'_>>;
}
