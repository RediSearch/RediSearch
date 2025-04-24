use crate::node::Node;

/// Iterate over the values stored in a [`TrieMap`](crate::TrieMap), in lexicographical order.
///
/// It only yields the values attached to the nodes, without reconstructing
/// the corresponding keys.
///
/// It can be instantiated by calling [`TrieMap::values`](crate::TrieMap::values).
pub struct Values<'tm, Data> {
    stack: Vec<&'tm Node<Data>>,
}

impl<'tm, Data> Values<'tm, Data> {
    /// Create a new [`Values`] iterator.
    pub(crate) fn new(root: Option<&'tm Node<Data>>) -> Self {
        Self {
            stack: root.into_iter().collect(),
        }
    }
}

impl<'tm, Data> Iterator for Values<'tm, Data> {
    type Item = &'tm Data;

    fn next(&mut self) -> Option<Self::Item> {
        let node = self.stack.pop()?;

        for child in node.children().iter().rev() {
            self.stack.push(child);
        }

        if let Some(data) = node.data() {
            return Some(data);
        }

        self.next()
    }
}
