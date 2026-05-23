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
        _target: &'a [u16],
        _prefix: bool,
        _suffix: bool,
    ) -> RuneTrieMapContainsIter<'a, Data> {
        // RuneTrieMapContainsIter(self.inner.contains_iter(&split_key(_target)))
        todo!()
    }

    // pub fn range_iter<F>(&self, min: Option<RuneBound<'_>>, max: Option<RuneBound<'_>>, mut cb: F)
    // where
    //     F: FnMut(&[u16], &T),
    // {
    //     let min_packed = min.map(|b| (pack_runes(b.value), b.is_included));
    //     let max_packed = max.map(|b| (pack_runes(b.value), b.is_included));
    //     let filter = RangeFilter {
    //         min: min_packed.as_ref().map(|(v, inc)| RangeBoundary {
    //             value: v.as_slice(),
    //             is_included: *inc,
    //         }),
    //         max: max_packed.as_ref().map(|(v, inc)| RangeBoundary {
    //             value: v.as_slice(),
    //             is_included: *inc,
    //         }),
    //     };
    //     for (bytes, data) in self.inner.range_iter(filter) {
    //         cb(&unpack_bytes(&bytes), data);
    //     }
    // }

    //     pub fn range_iter<'a>(&'a self, filter: RangeFilter<'a>) -> RangeIter<'a, Data> {

    pub fn iterate_range<'a>(
        &'a self,
        _as_deref_1: Option<&[u16]>,
        _include_min: bool,
        _as_deref_2: Option<&[u16]>,
        _include_max: bool,
    ) -> RuneTrieMapRangeIter<'a, Data> {
        let rf = RangeFilter {
            min: Some(RangeBoundary::included(&rune_to_bytes(
                _as_deref_1.unwrap(),
            ))),
            max: Some(RangeBoundary::included(&rune_to_bytes(
                _as_deref_2.unwrap(),
            ))),
        };

        RuneTrieMapRangeIter(self.inner.range_iter(rf))
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

pub struct RuneTrieMapContainsIter<'a, Data>(iter::ContainsIter<'a, Data>);

impl<'a, Data> Iterator for RuneTrieMapContainsIter<'a, Data> {
    type Item = (Vec<Rune>, &'a Data);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(k, v)| (bytes_to_rune(&k), v))
    }
}

pub struct RuneTrieMapRangeIter<'a, Data>(iter::RangeIter<'a, Data>);

impl<'a, Data> Iterator for RuneTrieMapRangeIter<'a, Data> {
    type Item = (Vec<Rune>, &'a Data);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(k, v)| (bytes_to_rune(&k), v))
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
