use crate::matches_prefixes_iterator::MatchesPrefixesIterator;
use crate::sub_trie_iterator::SubTrieIterator;
use crate::ordered_u8_map::OrderedU8Map;

#[derive(Debug)]
pub(crate) struct Node<Data> {
    pub(crate) val: Vec<u8>,
    pub(crate) children: OrderedU8Map<Node<Data>>,
    pub(crate) data: Option<Data>, // only termina nodes will have data
}

impl<Data> Node<Data> {
    fn split(&mut self, index: usize) {
        let second_val = self.val[index..].to_vec();
        let mut new_children = OrderedU8Map::new();
        new_children.insert(
            second_val[0],
            Node {
                val: second_val,
                children: self.children.take(),
                data: self.data.take(),
            },
        );
        self.val.truncate(index);
        self.children = new_children;
        self.data = None;
    }

    fn add(&mut self, val: &[u8], data: Data) -> (Option<Data>, usize) {
        if self.val == val {
            // todo: add to current
            let ret = self.data.take();
            self.data = Some(data);
            return (ret, 0);
        }

        if self.val.starts_with(val) {
            self.split(val.len());
            let mut res = self.add(val, data);
            res.1 += 1; // we splited one node here so we must have add one node.
            return res
        }

        if val.starts_with(&self.val) {
            let val = &val[self.val.len()..];
            // find children to progress with or create one if no children exists
            let mut n_nodes_added = 0;
            let child = self.children.get_or_create(val[0], || {
                n_nodes_added += 1;
                Node { val: val.to_vec(), children: OrderedU8Map::new(), data: None }
            });
            let mut res = child.add(val, data);
            res.1 += n_nodes_added;
            return res
        }

        let mut common_prefix_index = 0;
        for (i, c) in self.val.iter().enumerate() {
            if val[i] != *c {
                common_prefix_index = i;
                break;
            }
        }

        self.split(common_prefix_index);
        let mut res = self.add(val, data);
        res.1 += 1; // we splited one node here so we must have add one node.
        return res
    }

    fn get(&self, val: &[u8]) -> Option<&Data> {
        if val == self.val {
            return self.data.as_ref();
        }

        if val.starts_with(&self.val) {
            let val = &val[self.val.len()..];
            let child = self.children.get(val[0])?;
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
            let child = self.children.get_mut(val[0])?;
            return child.get_mut(val);
        }

        None
    }

    fn try_join_with_single_child(&mut self) -> bool {
        if self.children.is_empty() {
            return false;
        }
        if self.children.len() == 1 && self.data.is_none() {
            // if we have a single child and we do not hold data, join ourself with our only child.
            let (_c, mut child) = self.children.take().into_iter().next().unwrap(); // the single child must exists
            self.data = child.data.take();
            self.children = child.children;
            self.val.append(&mut child.val);
            true
        } else {
            false
        }
    }

    fn del(&mut self, val: &[u8]) -> Option<(Data, bool, usize)> {
        if val == self.val {
            let data = self.data.take();
            let node_deleted = self.try_join_with_single_child();
            return data.map(|v| {
                (
                    v,
                    self.children.is_empty() && self.data.is_none(),
                    if node_deleted {1} else {0},
                )
            });
        }

        if val.starts_with(&self.val) {
            if self.children.is_empty() {
                return None;
            }
            let val = &val[self.val.len()..];
            let child = self.children.get_mut(val[0])?;
            let (data, should_delete, mut n_nodes_deleted) = child.del(val)?;
            if should_delete {
                self.children.remove(val[0]); // one child deleted
                n_nodes_deleted += 1;
                n_nodes_deleted += if self.try_join_with_single_child() {1} else {0};
            };
            return Some((
                data,
                self.children.is_empty() && self.data.is_none(),
                n_nodes_deleted
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
            if let Some(child) = self.children.get(val[0]) {
                return child.find(val, prefixes);
            }
        }

        SubTrieIterator::empty()
    }
}

#[derive(Debug)]
pub struct Trie<Data> {
    pub(crate) root: Option<Node<Data>>,
    len: usize,
    n_nodes: usize,
}

impl<Data> Trie<Data> {
    pub fn new() -> Self {
        Trie { root: None, len: 0, n_nodes: 0 }
    }

    pub fn add(&mut self, key: &[u8], data: Data) -> Option<Data> {
        let res = match self.root.as_mut() {
            Some(root) => {
                let (old_data, nodes_added) = root.add(key, data);
                self.n_nodes += nodes_added;
                old_data
            }
            None => {
                self.root = Some(Node {
                    val: key.to_vec(),
                    children: OrderedU8Map::new(),
                    data: Some(data),
                });
                self.n_nodes += 1;
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
        let (data, should_delete, mut n_nodes_deleted) = root.del(key)?;
        if should_delete && root.data.is_none() {
            self.root = None;
            n_nodes_deleted += 1;
        }
        // something was actually deleted
        self.len -= 1;
        self.n_nodes -= n_nodes_deleted;
        Some(data)
    }

    pub fn del_str(&mut self, key: &str) -> Option<Data> {
        self.del(key.as_bytes())
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn n_nodes(&self) -> usize {
        self.n_nodes
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
            self.nodes.append(&mut n.children.into_iter().map(|(_, v)| v).collect());
            if let Some(data) = n.data {
                return Some(data);
            }
        }
        None
    }
}
