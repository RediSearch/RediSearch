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
