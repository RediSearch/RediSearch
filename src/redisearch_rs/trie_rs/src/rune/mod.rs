use itertools::Itertools;

use crate::TrieMap;

pub mod iter;

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

    pub fn iter(&self) -> iter::Iter<'_, Data> {
        iter::Iter::new(&self.inner)
    }

    /// Yield every terminal whose key starts with `prefix`.
    ///
    /// Mirrors the C `Trie_IterateContains(..., prefix=true, suffix=false)`
    /// branch at `src/trie/trie_node.c:1066`: locate the subtree root for
    /// the prefix and walk the whole subtree. Empty `prefix` yields zero
    /// matches — matches the C contract (`TrieNode_Get(root, _, 0, ...)`
    /// returns NULL at `src/trie/trie_node.c:411`).
    pub fn prefixed_iter(&self, prefix: &[Rune]) -> iter::PrefixedIter<'_, Data> {
        iter::PrefixedIter::new(&self.inner, prefix)
    }

    /// Yield every terminal whose key ends with `suffix`.
    ///
    /// Mirrors the C `Trie_IterateContains(..., prefix=false, suffix=true)`
    /// branch. Current implementation walks the entire trie and filters by
    /// `ends_with` — correct but O(N) regardless of selectivity. A future
    /// optimization would port C's `containsIterate` walker
    /// (`src/trie/trie_node.c:1077-1082`) as a byte-level `suffixed_iter`
    /// on `TrieMap`. Empty `suffix` yields zero matches (C contract).
    pub fn suffixed_iter(&self, suffix: &[Rune]) -> iter::SuffixedIter<'_, Data> {
        iter::SuffixedIter::new(&self.inner, suffix)
    }

    /// Yield every terminal whose key contains `target` as a substring.
    ///
    /// Mirrors the C `Trie_IterateContains(..., prefix=true, suffix=true)`
    /// branch and delegates to [`TrieMap::contains_iter`]. Empty `target`
    /// yields zero matches — without this short-circuit the byte-level
    /// `contains_iter` would match every term (memchr semantics on an
    /// empty needle).
    pub fn contains_iter(&self, target: &[Rune]) -> iter::ContainsIter<'_, Data> {
        iter::ContainsIter::new(&self.inner, target)
    }

    pub fn range_iter<'a>(
        &'a self,
        min: Option<&[Rune]>,
        include_min: bool,
        max: Option<&[Rune]>,
        include_max: bool,
    ) -> iter::RangeIter<'a, Data> {
        iter::RangeIter::build_from(&self.inner, min, include_min, max, include_max)
    }

    pub fn wildcard_iter(&self, buf: &[u16]) -> iter::WildcardIter<'_, Data> {
        iter::WildcardIter::new(&self.inner, buf)
    }
}

pub(super) fn rune_to_bytes(rune: &[Rune]) -> Vec<Byte> {
    rune.iter()
        .map(|x| x.to_be_bytes())
        .collect_vec()
        .into_flattened()
}

pub(super) fn bytes_to_rune(key: &[u8]) -> Vec<u16> {
    key.as_chunks()
        .0
        .iter()
        .map(|&c| u16::from_be_bytes(c))
        .collect()
}
