use crate::trie::Node;

use crate::trie_iter::TrieIterator;

#[derive(Clone)]
struct VirtualNode<'trie, Data> {
    index: usize,
    node: &'trie Node<Data>,
}

impl<'trie, Data> VirtualNode<'trie, Data> {
    fn new(node: &'trie Node<Data>) -> VirtualNode<'trie, Data> {
        VirtualNode{index: 0, node: node}
    }

    fn get_current_byte(&mut self) -> u8 {
        *self.node.val.get(self.index as u32).unwrap()
    }

    fn get_children(&self) -> Box<dyn Iterator<Item = VirtualNode<'trie, Data>> + 'trie> {
        if self.index < self.node.val.len() - 1 {
            return Box::new(vec![VirtualNode{index: self.index + 1, node: self.node}].into_iter());
        }
        Box::new(self.node.children.values().map(|e| VirtualNode{index: 0, node: e}))
    }

    fn data(&self) -> Option<&'trie Data> {
        if self.index == self.node.val.len() - 1 {
            self.node.data.as_ref()
        } else {
            None
        }
    }
}

pub struct WildcardTrieIterator<'trie, Data> {
    iters: Vec<Box<dyn Iterator<Item = (Vec<&'trie [u8]>, VirtualNode<'trie, Data>)> + 'trie>>, // the box can not outlive the trie lifetime
    prefix_buffer: Vec<u8>,
}

impl<'trie, Data> WildcardTrieIterator<'trie, Data> {
    pub(crate) fn new(val: &'trie [u8], node: Option<&'trie Node<Data>>) -> WildcardTrieIterator<'trie, Data> {
        let mut wildcards = vec![val];
        if val.len() > 1 && val[0] == b'*' {
            wildcards.push(&val[1..])
        }
        let res = {
            node.map(|e| -> Vec<Box<dyn Iterator<Item = (Vec<&'trie [u8]>, VirtualNode<'trie, Data>)>>> {
                vec![
                    if e.val.len() > 0 {
                        Box::new(vec![
                            (wildcards, VirtualNode::new(e))
                        ].into_iter())
                    } else {
                        Box::new(e.children.values().map(move|c| (wildcards.clone(), VirtualNode::new(c))))
                    }
                ]
            }).unwrap_or_else(Vec::new)
        };
        WildcardTrieIterator{
            iters: res,
            prefix_buffer: Vec::new(),
        }
    }
} 

fn get_matches_wildcards(wildcards: Vec<&[u8]>, c: u8) -> (bool, Vec<&[u8]>) {
    let mut res = Vec::new();
    let mut exact_match = false;
    for wildcard in wildcards {
        let possible_exact_match = wildcard.len() == 1 || (wildcard.len() > 1 && wildcard[1] == b'*');
        if wildcard[0] == b'*' {
            if possible_exact_match {
                exact_match = true;
            } else {
                res.push(&wildcard[1 ..]);
            }
            res.push(wildcard);
        } else if wildcard[0] == c || wildcard[0] == b'?' {
            if possible_exact_match {
                exact_match = true;
            }
            if wildcard.len() > 1 {
                res.push(&wildcard[1 ..]);
            }
        }
    }
    (exact_match, res)
}

impl<'trie, Data> TrieIterator for WildcardTrieIterator<'trie, Data> {
    type Item<'a> = (&'a [u8], &'trie Data) where Self: 'a;

    fn next<'a>(&'a mut self) -> Option<Self::Item<'a>> {
        loop {
            let last_iter = self.iters.last_mut()?;
            if let Some((curr_wildcards, mut curr_node)) = last_iter.next() {
                self.prefix_buffer.push(curr_node.get_current_byte());
                let (exact_match, match_wildcards) = get_matches_wildcards(curr_wildcards, self.prefix_buffer[self.prefix_buffer.len() - 1]);
                if match_wildcards.len() == 0 {
                    self.iters.push(Box::new(vec![].into_iter())); // push dummy iterator that will pop from the prefix buffer
                } else {
                    self.iters.push(Box::new(curr_node.get_children().map(move |e| (match_wildcards.clone(), e))));
                }
                if exact_match {
                    if let Some(data) = curr_node.data() {
                        return Some((&self.prefix_buffer, data));
                    }
                }
            } else {
                self.iters.pop();
                self.prefix_buffer.pop();
            }
        }
    }
}

pub struct WildcardTrieRustIterator<'trie, Data> {
    inner_iter: WildcardTrieIterator<'trie, Data>,
}

impl<'trie, Data> IntoIterator for WildcardTrieIterator<'trie, Data> {
    type Item = (Vec<u8>, &'trie Data);
    type IntoIter = WildcardTrieRustIterator<'trie, Data>;

    fn into_iter(self) -> Self::IntoIter {
        WildcardTrieRustIterator { inner_iter: self }
    }
}

impl<'trie, Data> Iterator for WildcardTrieRustIterator<'trie, Data> {
    type Item = (Vec<u8>, &'trie Data);

    fn next(&mut self) -> Option<Self::Item> {
        let (key, data) = self.inner_iter.next()?;
        Some((key.to_vec(), data))
    }
}