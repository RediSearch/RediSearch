/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! A trie-based structure for accumulating counts per key.
//!
//! [`TrieCount`] wraps a [`TrieMap<u64>`] to efficiently track counts associated
//! with byte-string keys. It is memory-efficient for keys with shared prefixes.

use crate::TrieMap;

/// A trie structure for accumulating counts per key.
///
/// This structure efficiently tracks counts for byte-string keys using a trie,
/// which is memory-efficient when keys share common prefixes.
///
/// # Example
///
/// ```
/// use trie_rs::TrieCount;
///
/// let mut counts = TrieCount::new();
///
/// // Increment counts for various keys
/// counts.increment(b"hello", 1);
/// counts.increment(b"world", 1);
///
/// // Increment the same key again
/// counts.increment(b"hello", 1);
///
/// assert_eq!(counts.get(b"hello"), Some(2));
/// assert_eq!(counts.get(b"world"), Some(1));
/// assert_eq!(counts.get(b"unknown"), None);
/// ```
#[derive(Debug, Clone, Default)]
pub struct TrieCount {
    inner: TrieMap<u64>,
}

impl TrieCount {
    /// Create a new empty [`TrieCount`].
    ///
    /// No allocation is performed on creation.
    /// Memory is allocated only when the first key is added.
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment the count for a key.
    ///
    /// If the key is not present, it is inserted with the given delta.
    /// If the key is already present, the delta is added to the existing count.
    /// Uses saturating addition to prevent overflow.
    ///
    /// # Arguments
    ///
    /// * `key` - The key as a byte slice
    /// * `delta` - The count to add
    pub fn increment(&mut self, key: &[u8], delta: u64) {
        self.inner.insert_with(key, |existing| {
            existing.unwrap_or(0).saturating_add(delta)
        });
    }

    /// Get the current count for a key.
    ///
    /// Returns `None` if the key is not present.
    pub fn get(&self, key: &[u8]) -> Option<u64> {
        self.inner.find(key).copied()
    }

    /// Returns the number of unique keys tracked.
    pub fn len(&self) -> usize {
        self.inner.n_unique_keys()
    }

    /// Returns `true` if no keys are being tracked.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the memory usage of this structure in bytes.
    pub fn mem_usage(&self) -> usize {
        self.inner.mem_usage()
    }

    /// Iterate over all (key, count) pairs in lexicographical order.
    pub fn iter(&self) -> impl Iterator<Item = (Vec<u8>, u64)> + '_ {
        self.inner.iter().map(|(key, &count)| (key, count))
    }

    /// Clear all entries, resetting the structure to empty.
    pub fn clear(&mut self) {
        self.inner = TrieMap::new();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_is_empty() {
        let counts = TrieCount::new();
        assert!(counts.is_empty());
        assert_eq!(counts.len(), 0);
    }

    #[test]
    fn test_single_increment() {
        let mut counts = TrieCount::new();
        counts.increment(b"hello", 1);

        assert_eq!(counts.get(b"hello"), Some(1));
        assert_eq!(counts.len(), 1);
        assert!(!counts.is_empty());
    }

    #[test]
    fn test_multiple_increments_same_key() {
        let mut counts = TrieCount::new();
        counts.increment(b"hello", 1);
        counts.increment(b"hello", 1);
        counts.increment(b"hello", 3);

        assert_eq!(counts.get(b"hello"), Some(5));
        assert_eq!(counts.len(), 1);
    }

    #[test]
    fn test_multiple_keys() {
        let mut counts = TrieCount::new();
        counts.increment(b"apple", 10);
        counts.increment(b"banana", 5);
        counts.increment(b"cherry", 3);

        assert_eq!(counts.get(b"apple"), Some(10));
        assert_eq!(counts.get(b"banana"), Some(5));
        assert_eq!(counts.get(b"cherry"), Some(3));
        assert_eq!(counts.get(b"date"), None);
        assert_eq!(counts.len(), 3);
    }

    #[test]
    fn test_shared_prefix_keys() {
        let mut counts = TrieCount::new();
        counts.increment(b"help", 10);
        counts.increment(b"helper", 5);
        counts.increment(b"helping", 3);
        counts.increment(b"hello", 7);

        assert_eq!(counts.get(b"help"), Some(10));
        assert_eq!(counts.get(b"helper"), Some(5));
        assert_eq!(counts.get(b"helping"), Some(3));
        assert_eq!(counts.get(b"hello"), Some(7));
        assert_eq!(counts.len(), 4);

        // Increment existing keys
        counts.increment(b"help", 2);
        counts.increment(b"helper", 3);

        assert_eq!(counts.get(b"help"), Some(12));
        assert_eq!(counts.get(b"helper"), Some(8));
        assert_eq!(counts.len(), 4); // Still 4 unique keys
    }

    #[test]
    fn test_unicode_keys() {
        let mut counts = TrieCount::new();

        // café (UTF-8: 0x63 0x61 0x66 0xC3 0xA9)
        let cafe = "café".as_bytes();
        // naïve (UTF-8 with ï)
        let naive = "naïve".as_bytes();
        // 日本 (Japanese: Japan)
        let nihon = "日本".as_bytes();
        // 日本語 (Japanese: Japanese language)
        let nihongo = "日本語".as_bytes();
        // München (German: Munich)
        let munchen = "München".as_bytes();

        counts.increment(cafe, 100);
        counts.increment(naive, 50);
        counts.increment(nihon, 200);
        counts.increment(nihongo, 150);
        counts.increment(munchen, 75);

        assert_eq!(counts.get(cafe), Some(100));
        assert_eq!(counts.get(naive), Some(50));
        assert_eq!(counts.get(nihon), Some(200));
        assert_eq!(counts.get(nihongo), Some(150));
        assert_eq!(counts.get(munchen), Some(75));
        assert_eq!(counts.len(), 5);

        // Increment Unicode keys
        counts.increment(cafe, 20);
        counts.increment(nihon, 30);

        assert_eq!(counts.get(cafe), Some(120));
        assert_eq!(counts.get(nihon), Some(230));
    }

    #[test]
    fn test_iteration_lexicographic_order() {
        let mut counts = TrieCount::new();
        counts.increment(b"cherry", 3);
        counts.increment(b"apple", 10);
        counts.increment(b"banana", 5);

        let entries: Vec<_> = counts.iter().collect();

        // Should be in lexicographic order
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0], (b"apple".to_vec(), 10));
        assert_eq!(entries[1], (b"banana".to_vec(), 5));
        assert_eq!(entries[2], (b"cherry".to_vec(), 3));
    }

    #[test]
    fn test_clear() {
        let mut counts = TrieCount::new();
        counts.increment(b"hello", 10);
        counts.increment(b"world", 20);

        assert_eq!(counts.len(), 2);
        assert!(!counts.is_empty());

        counts.clear();

        assert_eq!(counts.len(), 0);
        assert!(counts.is_empty());
        assert_eq!(counts.get(b"hello"), None);
        assert_eq!(counts.get(b"world"), None);
    }

    #[test]
    fn test_mem_usage_increases() {
        let mut counts = TrieCount::new();
        let initial_mem = counts.mem_usage();

        counts.increment(b"hello", 1);
        let after_one = counts.mem_usage();
        assert!(after_one > initial_mem);

        counts.increment(b"world", 1);
        let after_two = counts.mem_usage();
        assert!(after_two > after_one);
    }

    #[test]
    fn test_saturating_add() {
        let mut counts = TrieCount::new();

        // Start with a large value
        counts.increment(b"overflow", u64::MAX - 10);
        assert_eq!(counts.get(b"overflow"), Some(u64::MAX - 10));

        // Adding more should saturate at u64::MAX, not overflow
        counts.increment(b"overflow", 100);
        assert_eq!(counts.get(b"overflow"), Some(u64::MAX));
    }

    #[test]
    fn test_accumulation() {
        // Test accumulating counts across multiple operations
        let mut counts = TrieCount::new();

        // First batch of increments
        for key in [b"redis".as_slice(), b"search", b"database"] {
            counts.increment(key, 1);
        }

        // Second batch
        for key in [b"redis".as_slice(), b"cache"] {
            counts.increment(key, 1);
        }

        // Third batch
        for key in [b"redis".as_slice(), b"search", b"index"] {
            counts.increment(key, 1);
        }

        // Verify accumulated counts
        assert_eq!(counts.get(b"redis"), Some(3));
        assert_eq!(counts.get(b"search"), Some(2));
        assert_eq!(counts.get(b"database"), Some(1));
        assert_eq!(counts.get(b"cache"), Some(1));
        assert_eq!(counts.get(b"index"), Some(1));
        assert_eq!(counts.len(), 5);

        // Verify total via iteration
        let total: u64 = counts.iter().map(|(_, count)| count).sum();
        assert_eq!(total, 8); // 3 + 2 + 1 + 1 + 1
    }
}

/// Tests demonstrating how TrieCount can be used to update a C Trie via FFI callbacks.
///
/// These tests simulate the pattern described in the GC design where:
/// 1. Rust accumulates term deletion counts in a TrieCount
/// 2. The TrieCount is iterated, calling a C callback for each (term, delta)
/// 3. The C callback updates the serving Trie's numDocs field
#[cfg(test)]
mod ffi_callback_tests {
    use super::*;
    use std::collections::HashMap;
    use std::ffi::c_void;

    /// Simulates the C Trie's TrieNode structure.
    /// In the real C code, this is a complex packed struct with rune-based keys.
    /// Here we simplify to just track the numDocs per term (using String keys).
    #[derive(Debug, Default)]
    struct MockCTrie {
        /// Maps term -> numDocs count
        nodes: HashMap<Vec<u8>, u64>,
        /// Tracks terms that dropped to zero
        zeroed_terms: Vec<Vec<u8>>,
    }

    impl MockCTrie {
        fn new() -> Self {
            Self::default()
        }

        /// Insert a term with initial numDocs (simulates Trie_InsertStringBuffer)
        fn insert(&mut self, term: &[u8], num_docs: u64) {
            self.nodes.insert(term.to_vec(), num_docs);
        }

        /// Get numDocs for a term (simulates TrieNode_Get + reading numDocs)
        fn get_num_docs(&self, term: &[u8]) -> Option<u64> {
            self.nodes.get(term).copied()
        }

        /// Decrement numDocs for a term, returns true if it dropped to zero
        /// (simulates the callback logic from gc-trie-c-rust-analysis.md)
        fn decrement_num_docs(&mut self, term: &[u8], delta: u64) -> bool {
            if let Some(count) = self.nodes.get_mut(term) {
                *count = count.saturating_sub(delta);
                if *count == 0 {
                    self.zeroed_terms.push(term.to_vec());
                    return true;
                }
            }
            false
        }
    }

    /// The callback type matching the design document:
    /// ```c
    /// typedef bool (*TrieUpdateCallback)(
    ///     void *trie,
    ///     const uint8_t *term,
    ///     size_t term_len,
    ///     uint64_t delta
    /// );
    /// ```
    type TrieUpdateCallback =
        unsafe extern "C" fn(trie: *mut c_void, term: *const u8, term_len: usize, delta: u64)
            -> bool;

    /// The callback implementation that updates the MockCTrie.
    /// In real C code, this would:
    /// 1. Convert UTF-8 bytes to runes via runeBufFill()
    /// 2. Look up the TrieNode via TrieNode_Get()
    /// 3. Decrement node->numDocs
    ///
    /// # Safety
    /// - `trie` must be a valid pointer to a `MockCTrie`
    /// - `term` must be a valid pointer to `term_len` bytes
    unsafe extern "C" fn mock_update_callback(
        trie: *mut c_void,
        term: *const u8,
        term_len: usize,
        delta: u64,
    ) -> bool {
        // SAFETY: Caller guarantees `trie` is a valid `*mut MockCTrie`
        let trie = unsafe { &mut *(trie as *mut MockCTrie) };
        // SAFETY: Caller guarantees `term` points to `term_len` valid bytes
        let term_slice = unsafe { std::slice::from_raw_parts(term, term_len) };

        trie.decrement_num_docs(term_slice, delta)
    }

    /// Simulates `TermDeltaTrie_ApplyToServingTrie` from the design document.
    /// Iterates over all (term, delta) pairs and calls the callback for each.
    /// Returns the count of terms that dropped to zero.
    fn apply_deltas_to_trie(
        deltas: &TrieCount,
        trie: *mut c_void,
        callback: TrieUpdateCallback,
    ) -> u64 {
        let mut zeroed_count = 0u64;

        for (term, delta) in deltas.iter() {
            // SAFETY: We pass valid pointers matching the callback's expectations
            let dropped_to_zero =
                unsafe { callback(trie, term.as_ptr(), term.len(), delta) };

            if dropped_to_zero {
                zeroed_count += 1;
            }
        }

        zeroed_count
    }

    #[test]
    fn test_apply_deltas_basic() {
        // Setup: Create a "C Trie" with some terms
        let mut c_trie = MockCTrie::new();
        c_trie.insert(b"hello", 100);
        c_trie.insert(b"world", 50);
        c_trie.insert(b"foo", 10);

        // Collect some deletion deltas in TrieCount
        let mut deltas = TrieCount::new();
        deltas.increment(b"hello", 30); // Delete 30 docs containing "hello"
        deltas.increment(b"world", 20); // Delete 20 docs containing "world"

        // Apply deltas via callback
        let zeroed = apply_deltas_to_trie(
            &deltas,
            &mut c_trie as *mut MockCTrie as *mut c_void,
            mock_update_callback,
        );

        // Verify results
        assert_eq!(zeroed, 0); // No terms dropped to zero
        assert_eq!(c_trie.get_num_docs(b"hello"), Some(70)); // 100 - 30
        assert_eq!(c_trie.get_num_docs(b"world"), Some(30)); // 50 - 20
        assert_eq!(c_trie.get_num_docs(b"foo"), Some(10)); // Unchanged
    }

    #[test]
    fn test_apply_deltas_term_drops_to_zero() {
        let mut c_trie = MockCTrie::new();
        c_trie.insert(b"temporary", 5);
        c_trie.insert(b"permanent", 100);

        let mut deltas = TrieCount::new();
        deltas.increment(b"temporary", 5); // Exactly removes all docs
        deltas.increment(b"permanent", 10);

        let zeroed = apply_deltas_to_trie(
            &deltas,
            &mut c_trie as *mut MockCTrie as *mut c_void,
            mock_update_callback,
        );

        assert_eq!(zeroed, 1); // "temporary" dropped to zero
        assert_eq!(c_trie.get_num_docs(b"temporary"), Some(0));
        assert_eq!(c_trie.get_num_docs(b"permanent"), Some(90));
        assert_eq!(c_trie.zeroed_terms, vec![b"temporary".to_vec()]);
    }

    #[test]
    fn test_apply_deltas_multiple_terms_drop_to_zero() {
        let mut c_trie = MockCTrie::new();
        c_trie.insert(b"alpha", 10);
        c_trie.insert(b"beta", 20);
        c_trie.insert(b"gamma", 30);
        c_trie.insert(b"delta", 40);

        let mut deltas = TrieCount::new();
        deltas.increment(b"alpha", 10); // Drops to zero
        deltas.increment(b"beta", 5); // Still has 15
        deltas.increment(b"gamma", 30); // Drops to zero
        deltas.increment(b"delta", 50); // Saturates to zero

        let zeroed = apply_deltas_to_trie(
            &deltas,
            &mut c_trie as *mut MockCTrie as *mut c_void,
            mock_update_callback,
        );

        assert_eq!(zeroed, 3); // alpha, gamma, delta dropped to zero
        assert_eq!(c_trie.get_num_docs(b"alpha"), Some(0));
        assert_eq!(c_trie.get_num_docs(b"beta"), Some(15));
        assert_eq!(c_trie.get_num_docs(b"gamma"), Some(0));
        assert_eq!(c_trie.get_num_docs(b"delta"), Some(0));
    }

    #[test]
    fn test_apply_deltas_accumulated_increments() {
        // Simulate multiple GC rounds accumulating into the same TrieCount
        let mut c_trie = MockCTrie::new();
        c_trie.insert(b"term1", 100);
        c_trie.insert(b"term2", 100);

        let mut deltas = TrieCount::new();

        // First "GC round" - accumulate some deltas
        deltas.increment(b"term1", 10);
        deltas.increment(b"term2", 20);

        // Second "GC round" - accumulate more
        deltas.increment(b"term1", 15);
        deltas.increment(b"term2", 30);

        // Third "GC round"
        deltas.increment(b"term1", 5);

        // Apply accumulated deltas
        let zeroed = apply_deltas_to_trie(
            &deltas,
            &mut c_trie as *mut MockCTrie as *mut c_void,
            mock_update_callback,
        );

        assert_eq!(zeroed, 0);
        assert_eq!(c_trie.get_num_docs(b"term1"), Some(70)); // 100 - (10+15+5)
        assert_eq!(c_trie.get_num_docs(b"term2"), Some(50)); // 100 - (20+30)
    }

    #[test]
    fn test_apply_deltas_unicode_terms() {
        // Test that UTF-8 encoded terms work correctly through the callback
        let mut c_trie = MockCTrie::new();

        // Insert Unicode terms (these would be converted to runes in real C code)
        let cafe = "café".as_bytes();
        let nihon = "日本".as_bytes();

        c_trie.insert(cafe, 50);
        c_trie.insert(nihon, 100);

        let mut deltas = TrieCount::new();
        deltas.increment(cafe, 10);
        deltas.increment(nihon, 25);

        let zeroed = apply_deltas_to_trie(
            &deltas,
            &mut c_trie as *mut MockCTrie as *mut c_void,
            mock_update_callback,
        );

        assert_eq!(zeroed, 0);
        assert_eq!(c_trie.get_num_docs(cafe), Some(40));
        assert_eq!(c_trie.get_num_docs(nihon), Some(75));
    }

    #[test]
    fn test_apply_deltas_missing_term_in_trie() {
        // If a term exists in deltas but not in the C Trie, nothing happens
        let mut c_trie = MockCTrie::new();
        c_trie.insert(b"exists", 100);

        let mut deltas = TrieCount::new();
        deltas.increment(b"exists", 10);
        deltas.increment(b"missing", 50); // Not in c_trie

        let zeroed = apply_deltas_to_trie(
            &deltas,
            &mut c_trie as *mut MockCTrie as *mut c_void,
            mock_update_callback,
        );

        assert_eq!(zeroed, 0);
        assert_eq!(c_trie.get_num_docs(b"exists"), Some(90));
        assert_eq!(c_trie.get_num_docs(b"missing"), None);
    }

    #[test]
    fn test_apply_deltas_empty() {
        let mut c_trie = MockCTrie::new();
        c_trie.insert(b"unchanged", 100);

        let deltas = TrieCount::new(); // Empty

        let zeroed = apply_deltas_to_trie(
            &deltas,
            &mut c_trie as *mut MockCTrie as *mut c_void,
            mock_update_callback,
        );

        assert_eq!(zeroed, 0);
        assert_eq!(c_trie.get_num_docs(b"unchanged"), Some(100));
    }

    #[test]
    fn test_apply_deltas_lexicographic_iteration_order() {
        // Verify that iteration happens in lexicographic order
        // This is important for cache-friendly access patterns
        let mut c_trie = MockCTrie::new();
        c_trie.insert(b"zebra", 10);
        c_trie.insert(b"alpha", 10);
        c_trie.insert(b"middle", 10);

        let mut deltas = TrieCount::new();
        deltas.increment(b"zebra", 10);
        deltas.increment(b"alpha", 10);
        deltas.increment(b"middle", 10);

        // All drop to zero
        let zeroed = apply_deltas_to_trie(
            &deltas,
            &mut c_trie as *mut MockCTrie as *mut c_void,
            mock_update_callback,
        );

        assert_eq!(zeroed, 3);

        // zeroed_terms should be in lexicographic order (order of iteration)
        assert_eq!(
            c_trie.zeroed_terms,
            vec![b"alpha".to_vec(), b"middle".to_vec(), b"zebra".to_vec()]
        );
    }
}
