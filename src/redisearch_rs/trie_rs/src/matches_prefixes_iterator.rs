use crate::trie::{Node, Trie};

use crate::trie_iter::TrieIterator;

pub struct MatchesPrefixesIterator<'trie, Data> {
    curr_term: &'trie [u8],
    curr_node: Option<&'trie Node<Data>>,
    curr_prefix: Vec<u8>,
}

impl<'trie, Data> MatchesPrefixesIterator<'trie, Data> {
    pub(crate) fn from_trie(
        t: &'trie Trie<Data>,
        term: &'trie [u8],
    ) -> MatchesPrefixesIterator<'trie, Data> {
        MatchesPrefixesIterator {
            curr_term: term,
            curr_node: t.root.as_ref(),
            curr_prefix: Vec::new(),
        }
    }
}

pub struct MatchesPrefixesRustIterator<'trie, Data> {
    inner_iter: MatchesPrefixesIterator<'trie, Data>,
}

impl<'trie, Data> MatchesPrefixesIterator<'trie, Data> {
    pub fn into_iter(self) -> MatchesPrefixesRustIterator<'trie, Data> {
        MatchesPrefixesRustIterator { inner_iter: self }
    }
}

impl<'trie, Data> Iterator for MatchesPrefixesRustIterator<'trie, Data> {
    type Item = (Vec<u8>, &'trie Data);

    fn next(&mut self) -> Option<Self::Item> {
        let (key, data) = self.inner_iter.next()?;
        Some((key.to_vec(), data))
    }
}

impl<'trie, Data> TrieIterator for MatchesPrefixesIterator<'trie, Data> {
    type Item<'a> = (&'a [u8], &'trie Data) where Self: 'a;

    fn next<'a>(&'a mut self) -> Option<Self::Item<'a>> {
        loop {
            let curr_node = self.curr_node?;
            if self.curr_term.starts_with(&curr_node.val) {
                // node matches, we should try to progress
                self.curr_prefix.extend(&curr_node.val);
                self.curr_term = &self.curr_term[curr_node.val.len()..];
                self.curr_node = match self.curr_term.get(0) {
                    Some(c) => curr_node
                        .children
                        .as_ref()
                        .map(|v| v.get(c))
                        .unwrap_or(None),
                    None => None,
                };
                if let Some(data) = &curr_node.data {
                    return Some((self.curr_prefix.as_ref(), data));
                }
            } else {
                break;
            }
        }
        None
    }
}
