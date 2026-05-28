use crate::TrieMap;

pub mod dfa;
pub mod iter;
pub mod suffix;
pub mod tag_suffix;
pub mod term_dict;

pub struct StrTrieMap<Data> {
    inner: TrieMap<Data>,
}

impl<Data> Default for StrTrieMap<Data> {
    fn default() -> Self {
        Self::new()
    }
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

    pub fn get_mut(&mut self, key: &str) -> Option<&mut Data> {
        self.inner.find_mut(key.as_bytes())
    }

    pub const fn len(&self) -> usize {
        self.inner.n_unique_keys()
    }

    pub const fn is_empty(&self) -> bool {
        self.inner.n_unique_keys() == 0
    }

    /// Estimated heap memory currently held by this map. Mirrors the cached
    /// counter on the underlying [`TrieMap`] — O(1).
    pub const fn mem_usage(&self) -> usize {
        self.inner.mem_usage()
    }

    /// Reach the inner byte-keyed [`TrieMap`].
    ///
    /// Crate-internal — exists so the [`term_dict`] wrapper can hand a
    /// folded `Cow<'_, str>` to the `*_cow` / `build_from_cow` iterator
    /// constructors without re-routing the owned-buffer ownership
    /// through the public `StrTrieMap` API.
    ///
    /// [`term_dict`]: crate::str::term_dict
    pub(crate) const fn byte_trie(&self) -> &TrieMap<Data> {
        &self.inner
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

    /// Wildcard iteration over UTF-8 keys with codepoint-aware semantics.
    ///
    /// Routes through [`TrieMap::wildcard_iter_utf8`]: `?` matches exactly
    /// one Unicode codepoint (1 to 4 bytes), `*` matches zero or more
    /// codepoints. Literal pattern bytes are matched verbatim against the
    /// key bytes — safe because UTF-8 is self-synchronizing, so a literal
    /// like `é` (`0xC3 0xA9`) byte-prefix-matches every key whose leading
    /// codepoint is `é`.
    pub fn wildcard_iter<'tm, 'p>(
        &'tm self,
        pattern: &'p str,
    ) -> iter::WildcardIter<'tm, 'p, Data> {
        iter::WildcardIter::new(&self.inner, pattern)
    }
}
