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
use rqe_wildcard::WildcardPattern;

use crate::{
    TrieMap,
    iter::{self, RangeBoundary, RangeFilter, filter},
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
        self.inner.insert(&rune_to_bytes(key), data)
    }

    pub fn insert_with<F>(&mut self, key: &[Rune], f: F)
    where
        F: FnOnce(Option<Data>) -> Data,
    {
        self.inner.insert_with(&rune_to_bytes(key), f);
    }

    pub fn remove(&mut self, key: &[Rune]) -> Option<Data> {
        self.inner.remove(&rune_to_bytes(key))
    }

    pub fn get(&mut self, key: &[Rune]) -> Option<&Data> {
        self.inner.find(&rune_to_bytes(key))
    }

    pub fn len(&self) -> usize {
        self.inner.iter().count()
    }

    pub fn iter(&self) -> RuneTrieMapIter<'_, Data> {
        RuneTrieMapIter(self.inner.iter())
    }

    pub fn iterate_contains<'a>(
        &'a self,
        target: &[u16],
        _prefix: bool,
        _suffix: bool,
    ) -> RuneTrieMapContainsIter<'a, Data> {
        let target_bytes: Box<[u8]> = rune_to_bytes(target).into_boxed_slice();

        RuneTrieMapContainsIterBuilder {
            target_bytes,
            inner_builder: |t| self.inner.contains_iter(t.as_ref()),
        }
        .build()
    }

    pub fn iterate_range<'a>(
        &'a self,
        min: Option<&[Rune]>,
        include_min: bool,
        max: Option<&[Rune]>,
        include_max: bool,
    ) -> RuneTrieMapRangeIter<'a, Data> {
        let min_bytes: Option<Box<[u8]>> = min.map(|m| rune_to_bytes(m).into_boxed_slice());
        let max_bytes: Option<Box<[u8]>> = max.map(|m| rune_to_bytes(m).into_boxed_slice());

        RuneTrieMapRangeIterBuilder {
            bytes: (min_bytes, max_bytes),
            inner_builder: |(mn, mx)| {
                let filter = RangeFilter {
                    min: mn.as_ref().map(|b| RangeBoundary {
                        value: b.as_ref(),
                        is_included: include_min,
                    }),
                    max: mx.as_ref().map(|b| RangeBoundary {
                        value: b.as_ref(),
                        is_included: include_max,
                    }),
                };
                self.inner.range_iter(filter)
            },
        }
        .build()
    }

    pub fn iterate_wildcard(&self, _buf: &[u16]) -> RuneTrieMapIter<'_, Data> {
        // self.inner
        //     .wildcard_iter(WildcardPattern::parse(&rune_to_bytes(buf)))
        todo!()
    }
}

pub struct RuneTrieMapIter<'a, Data>(iter::Iter<'a, Data, filter::VisitAll>);

impl<'a, Data> Iterator for RuneTrieMapIter<'a, Data> {
    type Item = (Vec<Rune>, &'a Data);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(k, v)| (bytes_to_rune(&k), v))
    }
}

#[ouroboros::self_referencing]
pub struct RuneTrieMapContainsIter<'tm, Data: 'tm> {
    target_bytes: Box<[u8]>,
    #[borrows(target_bytes)]
    #[covariant]
    inner: iter::ContainsIter<'tm, 'this, Data>,
}

impl<'tm, Data: 'tm> Iterator for RuneTrieMapContainsIter<'tm, Data> {
    type Item = (Vec<Rune>, &'tm Data);

    fn next(&mut self) -> Option<Self::Item> {
        self.with_inner_mut(|inner| inner.next())
            .map(|(k, v)| (bytes_to_rune(&k), v))
    }
}

#[ouroboros::self_referencing]
pub struct RuneTrieMapRangeIter<'tm, Data: 'tm> {
    bytes: (Option<Box<[u8]>>, Option<Box<[u8]>>),
    #[borrows(bytes)]
    #[covariant]
    inner: iter::RangeIter<'tm, 'this, Data>,
}

impl<'tm, Data: 'tm> Iterator for RuneTrieMapRangeIter<'tm, Data> {
    type Item = (Vec<Rune>, &'tm Data);

    fn next(&mut self) -> Option<Self::Item> {
        self.with_inner_mut(|inner| inner.next())
            .map(|(k, v)| (bytes_to_rune(&k), v))
    }
}

fn rune_to_bytes(rune: &[Rune]) -> Vec<Byte> {
    rune.iter()
        .map(|x| x.to_be_bytes())
        .collect_vec()
        .into_flattened()
}

fn bytes_to_rune(key: &[u8]) -> Vec<u16> {
    key.as_chunks()
        .0
        .iter()
        .map(|&c| u16::from_be_bytes(c))
        .collect()
}
