use crate::matches_prefixes_iterator::MatchesPrefixesIterator;
use crate::sub_trie_iterator::SubTrieIterator;
use std::collections::HashMap;

#[derive(Debug)]
pub(crate) struct Node<Data> {
    pub(crate) val: Vec<u8>,
    pub(crate) children: Option<HashMap<u8, Node<Data>>>,
    pub(crate) data: Option<Data>, // only termina nodes will have data
}

impl<Data> Node<Data> {
    fn split(&mut self, index: usize) {
        let second_val = self.val[index..].to_vec();
        let mut new_children = HashMap::new();
        new_children.insert(
            second_val[0],
            Node {
                val: second_val,
                children: self.children.take(),
                data: self.data.take(),
            },
        );
        self.val.truncate(index);
        self.children = Some(new_children);
        self.data = None;
    }

    fn add(&mut self, val: &[u8], data: Data) -> Option<Data> {
        if self.val == val {
            // todo: add to current
            let ret = self.data.take();
            self.data = Some(data);
            return ret;
        }

        if self.val.starts_with(val) {
            self.split(val.len());
            return self.add(val, data);
        }

        if val.starts_with(&self.val) {
            let val = &val[self.val.len()..];
            // find children to progress with or create one if no children exists
            let children = self.children.get_or_insert(HashMap::new());
            let child = children.entry(val[0]).or_insert(Node {
                val: val.to_vec(),
                children: None,
                data: None,
            });
            return child.add(val, data);
        }

        let mut common_prefix_index = 0;
        for (i, c) in self.val.iter().enumerate() {
            if val[i] != *c {
                common_prefix_index = i;
                break;
            }
        }

        self.split(common_prefix_index);
        self.add(val, data)
    }

    fn get(&self, val: &[u8]) -> Option<&Data> {
        if val == self.val {
            return self.data.as_ref();
        }

        if val.starts_with(&self.val) {
            let val = &val[self.val.len()..];
            let child = self.children.as_ref()?.get(&val[0])?;
            return child.get(val);
        }

        None
    }

    fn get_mut(&mut self, val: &[u8]) -> Option<&mut Data> {
        if val == self.val {
            return self.data.as_mut();
        }

        if val.starts_with(&self.val) {
            let val = &val[self.val.len()..];
            let child = self.children.as_mut()?.get_mut(&val[0])?;
            return child.get_mut(val);
        }

        None
    }

    fn try_join_with_single_child(&mut self) {
        let children = self.children.as_mut();
        if children.is_none() {
            return;
        }
        let children = children.unwrap();
        if children.len() == 1 && self.data.is_none() {
            // if we have a single child and we do not hold data, join ourself with out only child.
            let (_c, mut child) = children.drain().next().unwrap(); // the single child must exists
            self.data = child.data.take();
            self.children = child.children.take();
            self.val.append(&mut child.val);
        }
    }

    fn del(&mut self, val: &[u8]) -> Option<(Data, bool)> {
        if val == self.val {
            let data = self.data.take();
            self.try_join_with_single_child();
            return data.map(|v| {
                (
                    v,
                    self.children.as_ref().map(|v| v.is_empty()).unwrap_or(true),
                )
            });
        }

        if val.starts_with(&self.val) {
            let val = &val[self.val.len()..];
            let children = self.children.as_mut()?;
            let child = children.get_mut(&val[0])?;
            let (data, should_delete) = child.del(val)?;
            if should_delete {
                children.remove(&val[0]);
                self.try_join_with_single_child();
            };
            return Some((
                data,
                self.children.as_ref().map(|v| v.len()).unwrap_or(0) == 0,
            ));
        }

        None
    }

    fn find<'trie>(
        &'trie self,
        val: &'trie [u8],
        mut prefixes: Vec<&'trie [u8]>,
    ) -> SubTrieIterator<'trie, Data> {
        if self.val.starts_with(val) {
            return SubTrieIterator::from_node_and_prefixes(self, prefixes);
        }

        prefixes.push(&self.val);

        if val.starts_with(&self.val) {
            let val = &val[self.val.len()..];
            if let Some(children) = self.children.as_ref() {
                if let Some(child) = children.get(&val[0]) {
                    return child.find(val, prefixes);
                }
            }
        }

        SubTrieIterator::empty()
    }
}

#[derive(Debug)]
pub struct Trie<Data> {
    pub(crate) root: Option<Node<Data>>,
    len: usize,
}

impl<Data> Trie<Data> {
    pub fn new() -> Self {
        Trie { root: None, len: 0 }
    }

    pub fn add(&mut self, key: &[u8], data: Data) -> Option<Data> {
        let res = match self.root.as_mut() {
            Some(root) => root.add(key, data),
            None => {
                self.root = Some(Node {
                    val: key.to_vec(),
                    children: None,
                    data: Some(data),
                });
                None
            }
        };
        if res.is_none() {
            // new node was added
            self.len += 1;
        }
        res
    }

    pub fn add_str(&mut self, key: &str, data: Data) -> Option<Data> {
        self.add(key.as_bytes(), data)
    }

    pub fn get(&self, key: &[u8]) -> Option<&Data> {
        self.root.as_ref()?.get(key)
    }

    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut Data> {
        self.root.as_mut()?.get_mut(key)
    }

    pub fn get_str(&self, key: &str) -> Option<&Data> {
        self.get(key.as_bytes())
    }

    pub fn get_str_mut(&mut self, key: &str) -> Option<&mut Data> {
        self.get_mut(key.as_bytes())
    }

    pub fn del(&mut self, key: &[u8]) -> Option<Data> {
        let root = self.root.as_mut()?;
        let (data, should_delete) = root.del(key)?;
        if should_delete && root.data.is_none() {
            self.root = None;
        }
        // something was actually deleted
        self.len -= 1;
        Some(data)
    }

    pub fn del_str(&mut self, key: &str) -> Option<Data> {
        self.del(key.as_bytes())
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn find<'trie>(&'trie mut self, key: &'trie [u8]) -> SubTrieIterator<'trie, Data> {
        self.root
            .as_ref()
            .map(|v| v.find(key, Vec::new()))
            .unwrap_or_else(SubTrieIterator::empty)
    }

    pub fn find_str<'trie>(&'trie mut self, key: &'trie str) -> SubTrieIterator<'trie, Data> {
        self.find(key.as_bytes())
    }

    pub fn find_matches_prefixes<'trie>(
        &'trie mut self,
        key: &'trie [u8],
    ) -> MatchesPrefixesIterator<'trie, Data> {
        MatchesPrefixesIterator::from_trie(self, key)
    }

    pub fn find_matches_prefixes_str<'trie>(
        &'trie mut self,
        key: &'trie str,
    ) -> MatchesPrefixesIterator<'trie, Data> {
        self.find_matches_prefixes(key.as_bytes())
    }
}

impl<Data> Default for Trie<Data> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Data> IntoIterator for Trie<Data> {
    type Item = Data;
    type IntoIter = TrieDataIterator<Data>;

    fn into_iter(self) -> Self::IntoIter {
        TrieDataIterator {
            nodes: self.root.map(|v| vec![v]).unwrap_or_else(Vec::new),
        }
    }
}

pub struct TrieDataIterator<Data> {
    nodes: Vec<Node<Data>>,
}

impl<Data> Iterator for TrieDataIterator<Data> {
    type Item = Data;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(n) = self.nodes.pop() {
            if let Some(children) = n.children {
                self.nodes.append(&mut children.into_iter().map(|(_, v)| v).collect());
            }
            if let Some(data) = n.data {
                return Some(data);
            }
        }
        None
    }
}
