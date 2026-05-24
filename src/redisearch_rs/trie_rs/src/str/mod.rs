//   ┌───────────┬───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
//   │   Layer   │                                                                 Functions                                                                 │
//   ├───────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
//   │ Core node │ radix node with str: SmallVec<u8>, children: Vec<StrNode>, first_bytes: Vec<u8> (parallel array for binary search by leading byte),       │
//   │           │ payload: Option<Box<[u8]>>, score: f32, num_docs: u64, flags (Terminal/Deleted)                                                            │
//   ├───────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
//   │ Trie      │ Trie { root, size, freecb } — no sort_mode field needed (Lex implicit)                                                                    │
//   │ wrapper   │                                                                                                                                           │
//   ├───────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
//   │ Insert    │ Trie_InsertStr, Trie_InsertStringBuffer, Trie_Insert; both ADD_REPLACE + ADD_INCR                                                          │
//   ├───────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
//   │ Delete    │ Trie_Delete, Trie_DecrementNumDocs                                                                                                         │
//   ├───────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
//   │ Lookup    │ Trie_GetNode(s, exact, *offsetOut)                                                                                                        │
//   ├───────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
//   │ Iterate   │ Trie_IterateAll, Trie_Iterate (DFA), Trie_IterateRange, Trie_IterateContains, Trie_IterateWildcard                                        │
//   ├───────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
//   │ Misc      │ Trie_Size, Trie_RandomKey                                                                                                                 │
//   ├───────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
//   │ RDB       │ TrieType_GenericSave / GenericLoad (call back into C for RedisModule_Save* — wrap as FFI fn pointers)                                     │
//   └───────────┴───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

use crate::TrieMap;

pub mod iter;

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
        self.inner.insert(key.as_bytes(), data)
    }

    pub fn insert_with<F>(&mut self, key: &str, f: F)
    where
        F: FnOnce(Option<Data>) -> Data,
    {
        self.inner.insert_with(key.as_bytes(), f);
    }

    pub fn remove(&mut self, key: &str) -> Option<Data> {
        self.inner.remove(key.as_bytes())
    }

    pub fn get(&self, key: &str) -> Option<&Data> {
        self.inner.find(key.as_bytes())
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
    pub fn prefixed_iter(&self, prefix: &str) -> iter::PrefixedIter<'_, Data> {
        iter::PrefixedIter::new(&self.inner, prefix)
    }

    /// Yield every terminal whose key ends with `suffix`.
    ///
    /// Mirrors the C `Trie_IterateContains(..., prefix=false, suffix=true)`
    /// branch. Current implementation walks the entire trie and filters by
    /// byte `ends_with` — correct because UTF-8 is self-synchronizing
    /// (a multibyte sequence cannot be a suffix of another codepoint).
    /// Empty `suffix` yields zero matches (C contract).
    pub fn suffixed_iter(&self, suffix: &str) -> iter::SuffixedIter<'_, Data> {
        iter::SuffixedIter::new(&self.inner, suffix)
    }

    /// Yield every terminal whose key contains `target` as a substring.
    ///
    /// Mirrors the C `Trie_IterateContains(..., prefix=true, suffix=true)`
    /// branch and delegates to [`TrieMap::contains_iter`]. Empty `target`
    /// yields zero matches — without this short-circuit the byte-level
    /// `contains_iter` would match every term (memchr semantics on an
    /// empty needle).
    pub fn contains_iter<'tm, 'p>(&'tm self, target: &'p str) -> iter::ContainsIter<'tm, 'p, Data> {
        iter::ContainsIter::new(&self.inner, target)
    }

    pub fn range_iter<'tm, 'p>(
        &'tm self,
        min: Option<&'p str>,
        include_min: bool,
        max: Option<&'p str>,
        include_max: bool,
    ) -> iter::RangeIter<'tm, 'p, Data> {
        iter::RangeIter::build_from(&self.inner, min, include_min, max, include_max)
    }

    /// Wildcard iteration over UTF-8 keys.
    ///
    /// Pattern syntax is byte-level (delegated to `rqe_wildcard`): `?`
    /// matches a single byte and `*` matches zero or more bytes. For ASCII
    /// patterns over ASCII keys this is identical to "one char / any chars",
    /// but for multibyte codepoints `?` matches one byte of a UTF-8 sequence,
    /// not one logical character. Test snapshots that exercise non-ASCII
    /// wildcards are expected to diverge from the rune-keyed oracle.
    pub fn wildcard_iter<'tm, 'p>(&'tm self, pattern: &'p str) -> iter::WildcardIter<'tm, 'p, Data> {
        iter::WildcardIter::new(&self.inner, pattern)
    }
}
