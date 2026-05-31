/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Microbenchmark: does reusing a scratch buffer in [`RdbWrite::save_bytes_nul_terminated`]
//! actually help, compared to allocating a fresh `Vec<u8>` per call?
//!
//! Both writer impls below share an identical sink (`Vec<u8>`) and identical
//! framing (length-prefixed bytes, fixed-width primitives). The only delta is
//! how the per-entry NUL-terminated buffer is assembled.
//!
//! Run with: `cargo bench -p trie_bencher --bench rdb_save`.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use trie_bencher::corpus::CorpusType;
use trie_rs::rdb::{RdbOpts, RdbWrite, TrieEntry};
use trie_rs::str::StrTrieMap;
use trie_rs::str::rdb as str_rdb;

/// Writer that reuses a single scratch buffer for every NUL-terminated record.
/// Matches the production `RmIoWriter` in `triemap_ffi/src/rdb.rs`.
struct ScratchWriter<'a> {
    sink: &'a mut Vec<u8>,
    scratch: Vec<u8>,
}

impl<'a> ScratchWriter<'a> {
    fn new(sink: &'a mut Vec<u8>) -> Self {
        Self {
            sink,
            scratch: Vec::new(),
        }
    }
}

impl RdbWrite for ScratchWriter<'_> {
    fn save_u64(&mut self, v: u64) {
        self.sink.extend_from_slice(&v.to_le_bytes());
    }

    fn save_f64(&mut self, v: f64) {
        self.sink.extend_from_slice(&v.to_le_bytes());
    }

    fn save_bytes_nul_terminated(&mut self, b: &[u8]) {
        self.scratch.clear();
        self.scratch.reserve(b.len() + 1);
        self.scratch.extend_from_slice(b);
        self.scratch.push(0);
        // Length-prefix the contiguous NUL-terminated record into the sink,
        // mirroring the shape of `RedisModule_SaveStringBuffer`.
        self.sink
            .extend_from_slice(&(self.scratch.len() as u64).to_le_bytes());
        self.sink.extend_from_slice(&self.scratch);
    }
}

/// Writer that allocates a fresh `Vec<u8>` for every NUL-terminated record.
struct FreshWriter<'a> {
    sink: &'a mut Vec<u8>,
}

impl<'a> FreshWriter<'a> {
    fn new(sink: &'a mut Vec<u8>) -> Self {
        Self { sink }
    }
}

impl RdbWrite for FreshWriter<'_> {
    fn save_u64(&mut self, v: u64) {
        self.sink.extend_from_slice(&v.to_le_bytes());
    }

    fn save_f64(&mut self, v: f64) {
        self.sink.extend_from_slice(&v.to_le_bytes());
    }

    fn save_bytes_nul_terminated(&mut self, b: &[u8]) {
        let mut buf = Vec::with_capacity(b.len() + 1);
        buf.extend_from_slice(b);
        buf.push(0);
        self.sink
            .extend_from_slice(&(buf.len() as u64).to_le_bytes());
        self.sink.extend_from_slice(&buf);
    }
}

/// Build a trie of `TrieEntry` values from the Wiki-1K corpus keys.
///
/// `payload_size` controls the optional payload length (when payloads are
/// persisted at save time). `0` yields `payload: None`.
fn build_map(keys: &[String], payload_size: usize) -> StrTrieMap<TrieEntry> {
    let payload = if payload_size == 0 {
        None
    } else {
        Some(vec![b'p'; payload_size])
    };
    let mut map = StrTrieMap::new();
    for (i, k) in keys.iter().enumerate() {
        map.insert(
            k,
            TrieEntry {
                score: i as f64,
                payload: payload.clone(),
                num_docs: i as u64,
            },
        );
    }
    map
}

/// Conservative upper bound on the bytes emitted by `str_rdb::save` for `map`.
///
/// Used to pre-size the sink so allocator growth doesn't dominate either arm.
fn estimate_sink_capacity(
    map: &StrTrieMap<TrieEntry>,
    opts: RdbOpts,
    payload_size: usize,
) -> usize {
    let n = map.len();
    // Rough per-entry size: 8 (length prefix) + key + 1 (NUL) + 8 (score)
    //                    + [8 + payload + 1] when payloads
    //                    + [8] when num_docs
    // Wiki-1K average key length is small; over-estimate to be safe.
    let avg_key_len = 32;
    let mut per_entry = 8 + avg_key_len + 1 + 8;
    if opts.payloads {
        per_entry += 8 + payload_size + 1;
    }
    if opts.num_docs {
        per_entry += 8;
    }
    8 + n * per_entry
}

fn bench_save(c: &mut Criterion, label: &str, opts: RdbOpts, payload_size: usize) {
    let corpus = CorpusType::RedisBench1kWiki;
    let keys = corpus.create_terms(false);
    let map = build_map(&keys, payload_size);

    let cap = estimate_sink_capacity(&map, opts, payload_size);
    let mut group = c.benchmark_group(format!("rdb_save/{label}"));

    // Throughput in number of trie entries serialized per iteration. Lets
    // Criterion report a per-entry rate that's comparable across configs.
    group.throughput(Throughput::Elements(map.len() as u64));

    group.bench_function(BenchmarkId::new("scratch", "reused"), |b| {
        b.iter_batched(
            || Vec::with_capacity(cap),
            |mut sink| {
                let mut w = ScratchWriter::new(&mut sink);
                str_rdb::save(black_box(&map), &mut w, opts);
                black_box(sink);
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.bench_function(BenchmarkId::new("scratch", "fresh-per-call"), |b| {
        b.iter_batched(
            || Vec::with_capacity(cap),
            |mut sink| {
                let mut w = FreshWriter::new(&mut sink);
                str_rdb::save(black_box(&map), &mut w, opts);
                black_box(sink);
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn benches(c: &mut Criterion) {
    // Keys-only path: 1 bytes call per entry — worst case for scratch
    // amortization (least benefit per saved allocation).
    bench_save(
        c,
        "keys_numdocs",
        RdbOpts {
            payloads: false,
            num_docs: true,
        },
        0,
    );

    // Full TrieType_RdbSave shape: 2 bytes calls per entry.
    bench_save(
        c,
        "keys_payloads_numdocs_empty",
        RdbOpts {
            payloads: true,
            num_docs: true,
        },
        0,
    );

    // Size axis: larger payloads make the per-call alloc cheaper relatively,
    // but the scratch reuse still saves the allocation itself.
    bench_save(
        c,
        "keys_payloads64_numdocs",
        RdbOpts {
            payloads: true,
            num_docs: true,
        },
        64,
    );

    bench_save(
        c,
        "keys_payloads512_numdocs",
        RdbOpts {
            payloads: true,
            num_docs: true,
        },
        512,
    );
}

criterion_group!(rdb_save, benches);
criterion_main!(rdb_save);
