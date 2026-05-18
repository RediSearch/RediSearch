//   ┌───────────┬───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
//   │   Layer   │                                                                 Functions                                                                 │
//   ├───────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
//   │ Core node │ radix node with str: SmallVec<u16>, children: Vec<RuneNode>, first_runes: Vec<u16> (parallel array for binary search by leading rune),    │
//   │           │ payload: Option<Box<[u8]>>, score: f32, num_docs: u64, flags (Terminal/Deleted)                                                           │
//   ├───────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
//   │ Trie      │ Trie { root, size, freecb } — no sort_mode field needed (Lex implicit)                                                                    │
//   │ wrapper   │                                                                                                                                           │
//   ├───────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
//   │ Insert    │ Trie_InsertRune, Trie_InsertStringBuffer, Trie_Insert, Trie_InsertRuneNoSize; both ADD_REPLACE + ADD_INCR                                 │
//   ├───────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
//   │ Delete    │ Trie_Delete, Trie_DeleteRunes, Trie_DecrementNumDocs                                                                                      │
//   ├───────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
//   │ Lookup    │ Trie_GetNode(runes, exact, *offsetOut)                                                                                                    │
//   ├───────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
//   │ Iterate   │ Trie_IterateAll, Trie_Iterate (DFA), Trie_IterateRange, Trie_IterateContains, Trie_IterateWildcard                                        │
//   ├───────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
//   │ Misc      │ Trie_Size, Trie_RandomKey                                                                                                                 │
//   ├───────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
//   │ RDB       │ TrieType_GenericSave / GenericLoad (call back into C for RedisModule_Save* — wrap as FFI fn pointers)                                     │
//   └───────────┴───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

use itertools::Itertools;

use crate::{
    TrieMap,
    iter::{self, filter},
};

pub type Rune = u16;

type Byte = u8;

pub struct RuneTrieMap<Data> {
    inner: TrieMap<Data>,
}

impl<Data> RuneTrieMap<Data> {
    pub fn new() -> Self {
        Self {
            inner: TrieMap::new(),
        }
    }

    pub fn insert(&mut self, key: &[Rune], data: Data) -> Option<Data> {
        self.inner.insert(&split_key(key), data)
    }

    pub fn insert_with<F>(&mut self, key: &[Rune], f: F)
    where
        F: FnOnce(Option<Data>) -> Data,
    {
        self.inner.insert_with(&split_key(key), f);
    }

    pub fn remove(&mut self, key: &[Rune]) -> Option<Data> {
        self.inner.remove(&split_key(key))
    }

    pub fn get(&mut self, key: &[Rune]) -> Option<&Data> {
        self.inner.find(&split_key(key))
    }

    pub fn len(&self) -> usize {
        self.inner.iter().count()
    }

    pub fn iter(&self) -> RuneTrieMapIter<'_, Data> {
        RuneTrieMapIter(self.inner.iter())
    }

    pub fn decrement_num_docs(&self, term: &[u16], delta: usize) -> DecrResult {
        todo!("lets not put this on the type")
    }
}

pub enum DecrResult {
    NotFound,
    Updated,
    Deleted,
}

pub struct RuneTrieMapIter<'a, Data>(iter::Iter<'a, Data, filter::VisitAll>);

impl<'a, Data> Iterator for RuneTrieMapIter<'a, Data> {
    type Item = (Vec<Rune>, &'a Data);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(k, v)| (join_key(&k), v))
    }
}

fn split_key(key: &[Rune]) -> Vec<Byte> {
    key.into_iter().flat_map(|x| x.to_be_bytes()).collect_vec()
}

fn join_key(key: &[Byte]) -> Vec<Rune> {
    key.as_chunks()
        .0
        .iter()
        .map(|&[x, y]| Rune::from_be_bytes([x, y]))
        .collect_vec()
}
