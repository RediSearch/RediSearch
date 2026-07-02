/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Compares four implementations of the ranked `COLLECT … SORTBY` insert step
//! (`RankedStorage::consider`), isolating the sort-value handling, on **both**
//! query paths:
//!
//! - **remote** (shard): fetching one sort value is an O(1) borrow (a slice
//!   index, like `RLookupRow::get`). Large stream, `N ≫ K`.
//! - **local** (coordinator): the reducer merges `SHARDS` shards' already-top-K'd
//!   payloads, so `N = SHARDS · K`. Each value is fetched by `Map::get` — a
//!   linear scan + byte-compare per field (like `get_field`), i.e. NOT O(1).
//!
//! All four strategies **materialise (clone) a candidate's values at most once**
//! — only when it is kept. They differ in how the borrowed values reach the
//! comparison: how many times the source is iterated, and whether a buffer is
//! allocated.
//!
//! - **`master_eager`** — build the owned [`RankingKey`] snapshot (one allocation
//!   + an `Arc` bump per value) for *every* candidate, then compare
//!   owned-vs-owned. Clones losers too.
//! - **`A_iterator`** — the shipped optimisation: pass the borrows as a lazy,
//!   cloneable iterator; rank against the worst survivor *by borrow*
//!   ([`RankingKey::ranks_below`]) and materialise only on a win. No buffer.
//! - **`Bprime_stackarray`** — collect the borrows once into a stack
//!   `SmallVec<[_; 8]>` (8 = `SORTASCMAP_MAXFIELDS`), compare from it, clone on win.
//! - **`B_box`** — the reviewer's suggestion: same, but the buffer is a heap
//!   `Box<[Option<&SharedValue>]>` — one heap allocation per candidate.

extern crate redisearch_rs;

use std::hint::black_box;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use min_max_heap::MinMaxHeap;
use reducers::collect::ranking::{RankedEntry, RankingKey};
use smallvec::SmallVec;
use value::{SharedValue, Value};

redis_mock::mock_or_stub_missing_redis_c_symbols!();

/// All fields DESC (`SORTASCMAP_INIT` clears to 0 for descending): larger value
/// ranks better, so an ascending input stream is accept-heavy.
const MAP: u64 = 0;
/// `SORTASCMAP_MAXFIELDS` — the parse-time cap on SORTBY fields. Doubles as the
/// high end of the field-count sweep and the `Bprime_stackarray` inline capacity.
const MAX_SORT_KEYS: usize = 8;

/// Remote/shard path: large stream into a small top-K heap (`N ≫ K`).
const K_REMOTE: usize = 10;
const N_REMOTE: usize = 100_000;
/// Local/coordinator path: merges `SHARDS` shards' top-`K_LOCAL` payloads.
const SHARDS: usize = 12;
const K_LOCAL: usize = 100;
const N_LOCAL: usize = SHARDS * K_LOCAL;

#[derive(Clone, Copy)]
enum Order {
    /// Descending stream: after the heap fills, every candidate loses.
    Reject,
    /// Random stream: accepts front-loaded, then mostly losers.
    Shuffle,
    /// Ascending stream: every candidate beats the worst survivor and evicts.
    Accept,
}

impl Order {
    fn name(self) -> &'static str {
        match self {
            Order::Reject => "reject",
            Order::Shuffle => "shuffle",
            Order::Accept => "accept",
        }
    }
}

/// Per-candidate input to `consider`: its doc id and a cheap, cloneable iterator
/// of its borrowed sort-key values. The two impls differ only in fetch cost —
/// O(1) slice index (remote) vs `Map::get` linear scan (local) — which is the
/// variable that decides whether avoiding a redundant fetch is worth a buffer.
trait SortSource {
    fn doc(&self) -> u32;
    fn sort_vals(&self) -> impl Iterator<Item = Option<&SharedValue>> + Clone;
}

/// Shard/remote row: sort values fetched by O(1) slice index (like `RLookupRow::get`).
struct RemoteRow {
    doc: u32,
    vals: Vec<Option<SharedValue>>,
}

impl SortSource for RemoteRow {
    fn doc(&self) -> u32 {
        self.doc
    }
    fn sort_vals(&self) -> impl Iterator<Item = Option<&SharedValue>> + Clone {
        self.vals.iter().map(Option::as_ref)
    }
}

/// Coordinator/local row: a `Value::Map` payload; each sort value is fetched by
/// `Map::get`, a linear scan + byte-compare per field (like the reducer's `get_field`).
struct LocalRow {
    doc: u32,
    item: SharedValue,
    names: Vec<Vec<u8>>,
}

impl SortSource for LocalRow {
    fn doc(&self) -> u32 {
        self.doc
    }
    fn sort_vals(&self) -> impl Iterator<Item = Option<&SharedValue>> + Clone {
        let map = match &*self.item {
            Value::Map(m) => Some(m),
            _ => None,
        };
        self.names.iter().map(move |n| map.and_then(|m| m.get(n)))
    }
}

type Heap = MinMaxHeap<RankedEntry<RankingKey<u32>, u32>>;

/// master — build the owned key for every candidate, then compare owned-vs-owned.
fn run_master<S: SortSource>(rows: &[S], k: usize, map: u64) -> Heap {
    let mut heap: Heap = MinMaxHeap::with_capacity(k);
    for row in rows {
        let doc = row.doc();
        let cand_key = RankingKey::new(row.sort_vals().map(|v| v.cloned()).collect(), map, doc);
        if heap.len() < k {
            heap.push(RankedEntry::new(cand_key, doc));
        } else {
            let worst = heap.peek_min().expect("heap at cap is non-empty");
            if cand_key > *worst.key() {
                heap.push_pop_min(RankedEntry::new(cand_key, doc));
            }
        }
    }
    heap
}

/// A — shipped: compare by borrow, clone the snapshot only on a win.
fn run_a<S: SortSource>(rows: &[S], k: usize, map: u64) -> Heap {
    let mut heap: Heap = MinMaxHeap::with_capacity(k);
    for row in rows {
        let doc = row.doc();
        let sort_vals = row.sort_vals();
        if heap.len() < k {
            let key = RankingKey::new(sort_vals.map(|v| v.cloned()).collect(), map, doc);
            heap.push(RankedEntry::new(key, doc));
        } else {
            let worst = heap.peek_min().expect("heap at cap is non-empty");
            if worst.key().ranks_below(sort_vals.clone(), &doc) {
                let key = RankingKey::new(sort_vals.map(|v| v.cloned()).collect(), map, doc);
                heap.push_pop_min(RankedEntry::new(key, doc));
            }
        }
    }
    heap
}

/// B′ — collect borrows into a stack `SmallVec` once, compare from it, clone on win.
fn run_bprime<S: SortSource>(rows: &[S], k: usize, map: u64) -> Heap {
    let mut heap: Heap = MinMaxHeap::with_capacity(k);
    for row in rows {
        let doc = row.doc();
        let buf: SmallVec<[Option<&SharedValue>; MAX_SORT_KEYS]> = row.sort_vals().collect();
        if heap.len() < k {
            let key = RankingKey::new(buf.iter().copied().map(|v| v.cloned()).collect(), map, doc);
            heap.push(RankedEntry::new(key, doc));
        } else {
            let worst = heap.peek_min().expect("heap at cap is non-empty");
            if worst.key().ranks_below(buf.iter().copied(), &doc) {
                let key =
                    RankingKey::new(buf.iter().copied().map(|v| v.cloned()).collect(), map, doc);
                heap.push_pop_min(RankedEntry::new(key, doc));
            }
        }
    }
    heap
}

/// B — collect borrows into a heap `Box` once, compare from it, clone on win.
fn run_b<S: SortSource>(rows: &[S], k: usize, map: u64) -> Heap {
    let mut heap: Heap = MinMaxHeap::with_capacity(k);
    for row in rows {
        let doc = row.doc();
        let buf: Box<[Option<&SharedValue>]> = row.sort_vals().collect();
        if heap.len() < k {
            let key = RankingKey::new(buf.iter().copied().map(|v| v.cloned()).collect(), map, doc);
            heap.push(RankedEntry::new(key, doc));
        } else {
            let worst = heap.peek_min().expect("heap at cap is non-empty");
            if worst.key().ranks_below(buf.iter().copied(), &doc) {
                let key =
                    RankingKey::new(buf.iter().copied().map(|v| v.cloned()).collect(), map, doc);
                heap.push_pop_min(RankedEntry::new(key, doc));
            }
        }
    }
    heap
}

/// Deterministic Fisher-Yates permutation of `0..n` (fixed seed → identical
/// stream for every strategy).
fn shuffled(n: usize) -> Vec<u64> {
    let mut idx: Vec<u64> = (0..n as u64).collect();
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    for i in (1..n).rev() {
        // xorshift64
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let j = (state % (i as u64 + 1)) as usize;
        idx.swap(i, j);
    }
    idx
}

/// Field-0 (ranking) values for the stream, in the order dictated by `order`.
fn field0_seq(n: usize, order: Order) -> Vec<u64> {
    match order {
        Order::Accept => (0..n as u64).collect(),
        Order::Reject => (0..n as u64).rev().collect(),
        Order::Shuffle => shuffled(n),
    }
}

/// Where the distinct (discriminating) sort field sits — decides how far the
/// comparison must read before it can rank two candidates.
#[derive(Clone, Copy, PartialEq)]
enum Disc {
    /// Field 0 is unique → the compare decides on the first field and
    /// short-circuits (best case for the lazy iterator).
    Head,
    /// Only the *last* field is unique; fields `0..F-1` are identical across all
    /// rows, so the compare must read every field before deciding (defeats the
    /// short-circuit — worst case for the iterator, forces all F fetches).
    Tail,
}

/// Value of sort field `j` for row `i` (ranking value `rank`), per layout.
fn field_val(disc: Disc, f: usize, i: usize, j: usize, rank: u64) -> f64 {
    let disc_idx = match disc {
        Disc::Head => 0,
        Disc::Tail => f - 1,
    };
    if j == disc_idx {
        rank as f64
    } else {
        match disc {
            // Distinct filler; never compared (the discriminator already decided).
            Disc::Head => (i * f + j) as f64,
            // Identical across all rows so the compare ties through to the last field.
            Disc::Tail => 0.0,
        }
    }
}

fn gen_remote(n: usize, f: usize, order: Order, disc: Disc) -> Vec<RemoteRow> {
    field0_seq(n, order)
        .into_iter()
        .enumerate()
        .map(|(i, rank)| {
            let vals = (0..f)
                .map(|j| Some(SharedValue::new_num(field_val(disc, f, i, j, rank))))
                .collect();
            RemoteRow {
                doc: i as u32,
                vals,
            }
        })
        .collect()
}

fn gen_local(n: usize, f: usize, order: Order, disc: Disc) -> Vec<LocalRow> {
    field0_seq(n, order)
        .into_iter()
        .enumerate()
        .map(|(i, rank)| {
            let names: Vec<Vec<u8>> = (0..f).map(|j| format!("s{j}").into_bytes()).collect();
            let pairs = (0..f).map(|j| {
                let key = SharedValue::new_string(format!("s{j}").into_bytes());
                let val = SharedValue::new_num(field_val(disc, f, i, j, rank));
                (key, val)
            });
            LocalRow {
                doc: i as u32,
                item: SharedValue::new_map(pairs),
                names,
            }
        })
        .collect()
}

fn bench_path<S: SortSource>(
    c: &mut Criterion,
    path: &str,
    k: usize,
    n: usize,
    gen_rows: impl Fn(usize, Order, Disc) -> Vec<S>,
) {
    // (fields, layout): F1 head, F8 head (short-circuits), F8 tail (no short-circuit).
    for &(f, disc) in &[
        (1usize, Disc::Head),
        (MAX_SORT_KEYS, Disc::Head),
        (MAX_SORT_KEYS, Disc::Tail),
    ] {
        for order in [Order::Reject, Order::Shuffle, Order::Accept] {
            let rows = gen_rows(f, order, disc);
            let suffix = if disc == Disc::Tail { "_tail" } else { "" };
            let mut group = c.benchmark_group(format!("{path}/{}/F{f}{suffix}", order.name()));
            group.throughput(Throughput::Elements(n as u64));
            group.bench_function("master_eager", |b| {
                b.iter(|| black_box(run_master(black_box(&rows), k, MAP)));
            });
            group.bench_function("A_iterator", |b| {
                b.iter(|| black_box(run_a(black_box(&rows), k, MAP)));
            });
            group.bench_function("Bprime_stackarray", |b| {
                b.iter(|| black_box(run_bprime(black_box(&rows), k, MAP)));
            });
            group.bench_function("B_box", |b| {
                b.iter(|| black_box(run_b(black_box(&rows), k, MAP)));
            });
            group.finish();
        }
    }
}

fn bench_consider(c: &mut Criterion) {
    bench_path(
        c,
        "collect_consider_remote",
        K_REMOTE,
        N_REMOTE,
        |f, order, disc| gen_remote(N_REMOTE, f, order, disc),
    );
    bench_path(
        c,
        "collect_consider_local",
        K_LOCAL,
        N_LOCAL,
        |f, order, disc| gen_local(N_LOCAL, f, order, disc),
    );
}

criterion_group!(benches, bench_consider);
criterion_main!(benches);
