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

    pub fn get(&self, key: &[Rune]) -> Option<&Data> {
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
        prefix: bool,
        suffix: bool,
    ) -> RuneTrieMapContainsIter<'a, Data> {
        // C rune-trie contract: empty pattern yields zero matches (see
        // `TrieNode_Get(root, str, 0, ...)` returning NULL). The byte-level
        // ContainsIter would otherwise match every term (memchr semantics).
        if target.is_empty() {
            return RuneTrieMapContainsIter(RuneTrieMapContainsIterKind::Empty);
        }

        // Exact mode (`!prefix && !suffix`) mirrors `TrieNode_IterateContains`
        // at src/trie/trie_node.c:1048: a single `TrieNode_Get(..., exact=true)`
        // emitting one callback iff the node is terminal. `TrieMap::find`
        // already returns `None` for non-terminal internal nodes.
        if !prefix && !suffix {
            let value = self.inner.find(&rune_to_bytes(target));
            return RuneTrieMapContainsIter(RuneTrieMapContainsIterKind::Single(
                value.map(|v| (target.to_vec(), v)),
            ));
        }

        // Pure-prefix mode (`prefix && !suffix`) mirrors the C branch at
        // src/trie/trie_node.c:1066: locate the subtree root for the prefix
        // and iterate the whole subtree. `TrieMap::prefixed_iter` already
        // performs that subtree descent via `find_root_for_prefix`.
        if prefix && !suffix {
            return RuneTrieMapContainsIter(RuneTrieMapContainsIterKind::Prefix(
                self.inner.prefixed_iter(&rune_to_bytes(target)),
            ));
        }

        // Pure-suffix mode (`!prefix && suffix`): filter-based — walk the
        // entire trie and keep terms whose byte representation ends with the
        // target. Correct but O(N) regardless of selectivity. A future
        // optimization would port C's `containsIterate` walker
        // (src/trie/trie_node.c:1077-1082) as a new byte-level iterator on
        // `TrieMap` and reuse it for both contains and suffix modes via a
        // flag, matching the C perf characteristics.
        if !prefix && suffix {
            return RuneTrieMapContainsIter(RuneTrieMapContainsIterKind::Suffix {
                target_bytes: rune_to_bytes(target).into_boxed_slice(),
                iter: self.inner.iter(),
            });
        }

        let target_bytes: Box<[u8]> = rune_to_bytes(target).into_boxed_slice();

        RuneTrieMapContainsIter(RuneTrieMapContainsIterKind::Substring(
            RuneTrieMapContainsIterInnerBuilder {
                target_bytes,
                inner_builder: |t| self.inner.contains_iter(t.as_ref()),
            }
            .build(),
        ))
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
struct RuneTrieMapContainsIterInner<'tm, Data: 'tm> {
    target_bytes: Box<[u8]>,
    #[borrows(target_bytes)]
    #[covariant]
    inner: iter::ContainsIter<'tm, 'this, Data>,
}

pub struct RuneTrieMapContainsIter<'tm, Data: 'tm>(RuneTrieMapContainsIterKind<'tm, Data>);

enum RuneTrieMapContainsIterKind<'tm, Data: 'tm> {
    Empty,
    Single(Option<(Vec<Rune>, &'tm Data)>),
    Prefix(iter::Iter<'tm, Data, filter::VisitAll>),
    Suffix {
        target_bytes: Box<[u8]>,
        iter: iter::Iter<'tm, Data, filter::VisitAll>,
    },
    Substring(RuneTrieMapContainsIterInner<'tm, Data>),
}

impl<'tm, Data: 'tm> Iterator for RuneTrieMapContainsIter<'tm, Data> {
    type Item = (Vec<Rune>, &'tm Data);

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.0 {
            RuneTrieMapContainsIterKind::Empty => None,
            RuneTrieMapContainsIterKind::Single(slot) => slot.take(),
            RuneTrieMapContainsIterKind::Prefix(inner) => {
                inner.next().map(|(k, v)| (bytes_to_rune(&k), v))
            }
            RuneTrieMapContainsIterKind::Suffix { target_bytes, iter } => loop {
                let (k, v) = iter.next()?;
                if k.ends_with(target_bytes) {
                    return Some((bytes_to_rune(&k), v));
                }
            },
            RuneTrieMapContainsIterKind::Substring(inner) => inner
                .with_inner_mut(|i| i.next())
                .map(|(k, v)| (bytes_to_rune(&k), v)),
        }
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
