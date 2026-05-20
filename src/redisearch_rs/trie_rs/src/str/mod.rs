use itertools::Itertools;

use crate::{
    TrieMap,
    iter::{self, filter},
};

type Byte = u8;

pub struct StrTrieMap<Data> {
    inner: TrieMap<Data>,
}

impl<Data> StrTrieMap<Data> {
    pub fn new() -> Self {
        Self {
            inner: TrieMap::new(),
        }
    }

    pub fn insert(&mut self, key: &str, data: Data) -> Option<Data> {
        self.inner.insert(&split_key(key), data)
    }

    pub fn insert_with<F>(&mut self, key: &str, f: F)
    where
        F: FnOnce(Option<Data>) -> Data,
    {
        self.inner.insert_with(&split_key(key), f);
    }

    pub fn remove(&mut self, key: &str) -> Option<Data> {
        self.inner.remove(&split_key(key))
    }

    pub fn get(&mut self, key: &str) -> Option<&Data> {
        self.inner.find(&split_key(key))
    }

    pub fn len(&self) -> usize {
        self.inner.iter().count()
    }

    pub fn iter(&self) -> RuneTrieMapIter<'_, Data> {
        RuneTrieMapIter(self.inner.iter())
    }
}

pub struct RuneTrieMapIter<'a, Data>(iter::Iter<'a, Data, filter::VisitAll>);

impl<'a, Data> Iterator for RuneTrieMapIter<'a, Data> {
    type Item = (String, &'a Data);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(k, v)| (join_key(k), v))
    }
}

fn split_key(key: &str) -> Vec<Byte> {
    key.bytes().collect_vec()
}

fn join_key(key: Vec<Byte>) -> String {
    String::from_utf8(key).unwrap()
}
