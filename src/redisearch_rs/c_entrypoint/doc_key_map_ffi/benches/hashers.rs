/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Paired hasher comparison with the document map's bucket and comparison model.
//!
//! This isolates hashing, table access, and borrowed-key comparison. It excludes
//! Redis allocation, C/FFI calls, DMD chain traversal, locking, and query iteration.

use hashbrown::HashTable;
use std::{collections::hash_map::RandomState, hash::BuildHasher, hint::black_box, time::Instant};

const ROUNDS: usize = 11;
const WARMUPS: usize = 2;
const QUERY_COUNT: usize = 900 * 64;
const READ_REPEATS: usize = 2;
const HASH_REPEATS: usize = 8;

#[derive(Debug)]
struct Entry {
    hash: u64,
    id: u64,
}

// Both hashers use exactly the same payload, growth policy, ID indirection, and
// full-key comparison. Neither fixture owns another copy of the indexed keys.
struct Fixture<B> {
    entries: HashTable<Entry>,
    hasher: B,
}

impl<B: BuildHasher + Clone> Fixture<B> {
    fn build(keys: &[Box<[u8]>], hasher: &B) -> Self {
        let mut fixture = Self {
            entries: HashTable::new(),
            hasher: hasher.clone(),
        };
        for (i, key) in keys.iter().enumerate() {
            let hash = fixture.hasher.hash_one(key.as_ref());
            fixture.entries.insert_unique(
                hash,
                Entry {
                    hash,
                    id: i as u64 + 1,
                },
                |e| e.hash,
            );
        }
        fixture
    }

    fn find(&self, keys: &[Box<[u8]>], key: &[u8]) -> u64 {
        let hash = self.hasher.hash_one(key);
        self.entries
            .find(hash, |e| {
                e.hash == hash && keys[e.id as usize - 1].as_ref() == key
            })
            .map_or(0, |e| e.id)
    }

    fn replace(&mut self, key: &[u8], id: u64) {
        let hash = self.hasher.hash_one(black_box(key));
        self.entries
            .find_entry(hash, |e| e.hash == hash && e.id == id)
            .expect("existing document")
            .remove();
        let hash = self.hasher.hash_one(black_box(key));
        self.entries
            .insert_unique(hash, Entry { hash, id }, |e| e.hash);
    }
}

const fn mixed(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e3779b97f4a7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

fn key(i: usize, len: usize) -> Box<[u8]> {
    let mut bytes = match len {
        16 => format!("doc:{i:012}").into_bytes(),
        64 => format!(
            "tenant:{:08}:catalog:product:{:016x}",
            i % 1024,
            mixed(i as u64)
        )
        .into_bytes(),
        256 => {
            let mut bytes = vec![b'k'];
            bytes.extend_from_slice(&(i as u64).to_le_bytes());
            for j in 0..31 {
                bytes.extend_from_slice(&mixed(i as u64 ^ (j * 0x100000000)).to_le_bytes());
            }
            bytes
        }
        _ => unreachable!(),
    };
    bytes.resize(len, b':');
    bytes.truncate(len);
    bytes.into_boxed_slice()
}

fn timed<T>(f: impl FnOnce() -> T) -> u128 {
    let start = Instant::now();
    let result = f();
    let elapsed = start.elapsed().as_nanos();
    // Table destruction and result disposal are outside the measurement.
    black_box(result);
    elapsed
}

fn paired<A, B, RA, RB>(
    len: usize,
    n: usize,
    operation: &str,
    operations: usize,
    mut a: A,
    mut b: B,
) where
    A: FnMut() -> RA,
    B: FnMut() -> RB,
{
    for round in 0..WARMUPS + ROUNDS {
        let (std, ahash) = if round % 2 == 0 {
            (timed(&mut a), timed(&mut b))
        } else {
            let ahash = timed(&mut b);
            (timed(&mut a), ahash)
        };
        if round >= WARMUPS {
            println!(
                "{len},{n},{operation},{},{:.4},{:.4}",
                round - WARMUPS,
                std as f64 / operations as f64,
                ahash as f64 / operations as f64,
            );
        }
    }
}

fn hash_batch<B: BuildHasher>(hasher: &B, queries: &[Box<[u8]>]) {
    let mut checksum = 0;
    for _ in 0..HASH_REPEATS {
        for key in queries {
            checksum ^= hasher.hash_one(black_box(key.as_ref()));
        }
    }
    black_box(checksum);
}

fn read_batch<B: BuildHasher + Clone>(
    fixture: &Fixture<B>,
    keys: &[Box<[u8]>],
    queries: &[Box<[u8]>],
) {
    let mut checksum = 0;
    for _ in 0..READ_REPEATS {
        for key in queries {
            checksum ^= fixture.find(black_box(keys), black_box(key.as_ref()));
        }
    }
    black_box(checksum);
}

fn dataset(n: usize, len: usize) {
    let keys: Vec<_> = (0..n).map(|i| key(i, len)).collect();
    let ids: Vec<_> = (0..QUERY_COUNT)
        .map(|i| mixed(i as u64) as usize % n)
        .collect();
    // Query strings deliberately have different addresses from indexed keys,
    // preventing pointer identity from bypassing full-key comparison.
    let hits: Vec<_> = ids.iter().map(|&i| keys[i].clone()).collect();
    let misses: Vec<_> = hits
        .iter()
        .map(|key| {
            let mut key = key.clone();
            key[0] ^= 0x80;
            key
        })
        .collect();
    let std_hasher = RandomState::new();
    let ahash_hasher = ahash::RandomState::new();
    let mut std = Fixture::build(&keys, &std_hasher);
    let mut ahash = Fixture::build(&keys, &ahash_hasher);
    eprintln!(
        "dataset bytes={len} docs={n} buckets={} std_alloc={} ahash_alloc={}",
        std.entries.num_buckets(),
        size_of_val(&std) + std.entries.allocation_size(),
        size_of_val(&ahash) + ahash.entries.allocation_size(),
    );
    for (i, hit) in hits.iter().enumerate() {
        assert_eq!(std.find(&keys, hit), ids[i] as u64 + 1);
        assert_eq!(ahash.find(&keys, hit), ids[i] as u64 + 1);
        assert_eq!(std.find(&keys, &misses[i]), 0);
        assert_eq!(ahash.find(&keys, &misses[i]), 0);
    }
    paired(
        len,
        n,
        "hash",
        QUERY_COUNT * HASH_REPEATS,
        || hash_batch(&std_hasher, &hits),
        || hash_batch(&ahash_hasher, &hits),
    );
    paired(
        len,
        n,
        "hit",
        QUERY_COUNT * READ_REPEATS,
        || read_batch(&std, &keys, &hits),
        || read_batch(&ahash, &keys, &hits),
    );
    paired(
        len,
        n,
        "miss",
        QUERY_COUNT * READ_REPEATS,
        || read_batch(&std, &keys, &misses),
        || read_batch(&ahash, &keys, &misses),
    );
    paired(
        len,
        n,
        "replace",
        QUERY_COUNT,
        || {
            for (&i, key) in ids.iter().zip(&hits) {
                std.replace(black_box(key), i as u64 + 1);
            }
        },
        || {
            for (&i, key) in ids.iter().zip(&hits) {
                ahash.replace(black_box(key), i as u64 + 1);
            }
        },
    );
    paired(
        len,
        n,
        "build",
        n,
        || Fixture::build(black_box(&keys), &std_hasher),
        || Fixture::build(black_box(&keys), &ahash_hasher),
    );
    for (i, hit) in hits.iter().enumerate() {
        assert_eq!(std.find(&keys, hit), ids[i] as u64 + 1);
        assert_eq!(ahash.find(&keys, hit), ids[i] as u64 + 1);
    }
}

fn main() {
    eprintln!(
        "aes_target_feature={} rounds={ROUNDS} warmups={WARMUPS}",
        cfg!(target_feature = "aes")
    );
    println!("key_bytes,documents,operation,round,std_ns_per_op,ahash_ns_per_op");
    for n in [100_000, 1_600_000] {
        for len in [16, 64, 256] {
            dataset(n, len);
        }
    }
}
