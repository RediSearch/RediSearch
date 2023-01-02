use crate::trie::Node;

use crate::trie_iter::TrieIterator;

pub struct SubTrieIterator<'trie, Data> {
    iters: Vec<Box<dyn Iterator<Item = &'trie Node<Data>> + 'trie>>, // the box can not outlive the trie lifetime
    prefix_buffer: Vec<u8>,
    processed_nodes: Vec<&'trie Node<Data>>,
}

impl<'trie, Data> SubTrieIterator<'trie, Data> {
    pub(crate) fn empty() -> SubTrieIterator<'trie, Data> {
        SubTrieIterator {
            iters: Vec::new(),
            prefix_buffer: Vec::new(),
            processed_nodes: Vec::new(),
        }
    }

    pub(crate) fn from_node_and_prefixes(
        n: &'trie Node<Data>,
        prefixes: Vec<&'trie [u8]>,
    ) -> SubTrieIterator<'trie, Data> {
        SubTrieIterator {
            iters: vec![Box::new(vec![n].into_iter())],
            prefix_buffer: prefixes.into_iter().fold(Vec::new(), |mut a, v| {
                a.extend(v);
                a
            }),
            processed_nodes: Vec::new(),
        }
    }
}

pub struct SubTrieRustIterator<'trie, Data> {
    inner_iter: SubTrieIterator<'trie, Data>,
}

impl<'trie, Data> SubTrieIterator<'trie, Data> {
    pub fn into_iter(self) -> SubTrieRustIterator<'trie, Data> {
        SubTrieRustIterator { inner_iter: self }
    }
}

impl<'trie, Data> Iterator for SubTrieRustIterator<'trie, Data> {
    type Item = (Vec<u8>, &'trie Data);

    fn next(&mut self) -> Option<Self::Item> {
        let (key, data) = self.inner_iter.next()?;
        Some((key.to_vec(), data))
    }
}

impl<'trie, Data> TrieIterator for SubTrieIterator<'trie, Data> {
    type Item<'a> = (&'a [u8], &'trie Data) where Self: 'a;

    fn next<'a>(&'a mut self) -> Option<Self::Item<'a>> {
        loop {
            let last_iter = self.iters.last_mut()?;
            if let Some(curr_node) = last_iter.next() {
                self.prefix_buffer.extend(&curr_node.val);
                self.iters.push(
                    curr_node
                        .children
                        .as_ref()
                        .map(|v| -> Box<dyn Iterator<Item = &'trie Node<Data>>> {
                            let values = v.values();
                            Box::new(values)
                        })
                        .unwrap_or(Box::new(Vec::new().into_iter())),
                );
                self.processed_nodes.push(curr_node);
                if let Some(data) = curr_node.data.as_ref() {
                    return Some((self.prefix_buffer.as_ref(), data));
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
