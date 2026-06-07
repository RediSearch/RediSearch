/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Side-by-side Rust `TermDictionary` vs C `Trie` (`Trie_Sort_Lex`) benches.
//!
//! Every `*_group` helper here produces a single Criterion benchmark group
//! containing one `Rust` bar and (where a faithful C equivalent exists) one
//! `C` bar. The group label embeds the corpus prefix and the case-folding
//! mode so a Criterion run produces self-explanatory charts.
//!
//! ## Folding modes
//!
//! [`FoldMode::Raw`] feeds untouched corpus to both sides — each side folds
//! internally (ICU for Rust, libnu inside `Trie_InsertStringBuffer` for C),
//! so the measured cost mirrors the realistic call-site. ICU folds differ
//! from libnu on a few codepoints (e.g. `ß → ss`, `ς → σ`), so the two
//! tries store slightly different key sets when the corpus contains those
//! codepoints — flagged here, not blocking the comparison.
//!
//! [`FoldMode::PreFolded`] ICU-folds the corpus once at bencher
//! construction and feeds the folded bytes to both sides. The Rust side
//! still runs ICU on each insert, but on already-folded input
//! `CaseMapper::fold_string` returns `Cow::Borrowed` — i.e. no
//! allocation — so the trie work dominates. The C side's libnu pass still
//! runs unchanged; on ASCII input that is a near-no-op.
//!
//! ## Per-bench shape
//!
//! - Immutable benches reuse a pre-built map held on `self`.
//! - Mutable benches use `iter_batched(_, _, PerIteration)` because
//!   `TermDictionary` and `CTrie` are not `Clone` — each iteration rebuilds
//!   a fresh map from the cached terms via the bench setup closure.
//!   Criterion excludes the setup cost from the measured window, so the
//!   "rebuild N keys" overhead is not double-counted as part of the
//!   operation under test.

use std::time::Duration;

use criterion::{BatchSize, BenchmarkGroup, Criterion, measurement::WallTime};
use icu_casemap::CaseMapper;

use trie_rs::str::term_dict::TermDictionary;

use crate::c_trie::CTrie;

/// Which case-folding input regime the bencher feeds to both sides.
///
/// See the module docs for the trade-off; the default is to run both
/// modes from the bench entry points so charts compare directly.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum FoldMode {
    /// Feed the corpus as it arrived — both sides fold internally.
    Raw,
    /// ICU-fold the corpus once at construction so both sides see
    /// already-folded input.
    PreFolded,
}

impl FoldMode {
    /// Short tag included in the Criterion group label so charts stay
    /// self-describing.
    pub const fn tag(self) -> &'static str {
        match self {
            FoldMode::Raw => "raw",
            FoldMode::PreFolded => "pre-folded",
        }
    }
}

/// Helper that drives one (corpus, folding-mode) sweep across the bench
/// surface, mirroring every Rust call with the equivalent C call where
/// one exists.
pub struct OperationBencher {
    /// Pre-built Rust dict — used directly by immutable benches.
    rust_map: TermDictionary,
    /// Pre-built C trie — used directly by immutable benches.
    c_map: CTrie,
    /// Original (post-fold) terms used to rebuild fresh maps in mutation
    /// bench setup closures.
    keys: Vec<String>,
    /// Cached byte view of `keys` for `CTrie::insert` (avoids re-borrowing
    /// every iteration).
    key_bytes: Vec<Vec<u8>>,
    measurement_times: CorpusMeasurementTime,
    /// Already includes the corpus prefix and the [`FoldMode`] tag.
    prefix: String,
}

/// Per-corpus measurement durations.
pub struct CorpusMeasurementTime {
    immutable: Duration,
    mutable: Duration,
}

impl CorpusMeasurementTime {
    /// Approximate immutable measurement time at 20% of the mutable time.
    /// Holds for trie ops where read is ~5× faster than write.
    pub fn from_mutable_trie(mutable_measurement_time: Duration) -> Self {
        Self {
            immutable: mutable_measurement_time.mul_f32(0.2),
            mutable: mutable_measurement_time,
        }
    }
}

impl Default for CorpusMeasurementTime {
    fn default() -> Self {
        Self {
            immutable: Duration::from_secs(5),
            mutable: Duration::from_secs(5),
        }
    }
}

impl OperationBencher {
    /// Build a bencher over `terms` for a given `fold_mode`.
    ///
    /// - `prefix` identifies the corpus in the bench group label.
    /// - `fold_mode` decides whether `terms` are ICU-folded at construction
    ///   (see [`FoldMode`]).
    /// - `mutable_measurement_time` overrides Criterion's default 5s for
    ///   mutable ops; defaults to 5s if `None`.
    pub fn new(
        prefix: String,
        terms: Vec<String>,
        fold_mode: FoldMode,
        mutable_measurement_time: Option<Duration>,
    ) -> Self {
        let keys = match fold_mode {
            FoldMode::Raw => terms,
            FoldMode::PreFolded => {
                let cm = CaseMapper::new();
                terms
                    .into_iter()
                    .map(|t| cm.fold_string(&t).into_owned())
                    .collect()
            }
        };
        let key_bytes: Vec<Vec<u8>> = keys.iter().map(|s| s.as_bytes().to_vec()).collect();

        let rust_map = build_rust_map(&keys);
        let c_map = build_c_map(&key_bytes);

        let measurement_time = mutable_measurement_time.unwrap_or(Duration::from_secs(5));
        Self {
            prefix: format!("{prefix}|{}", fold_mode.tag()),
            rust_map,
            c_map,
            keys,
            key_bytes,
            measurement_times: CorpusMeasurementTime::from_mutable_trie(measurement_time),
        }
    }

    fn benchmark_group_mutable<'a>(
        &self,
        c: &'a mut Criterion,
        label: &str,
    ) -> BenchmarkGroup<'a, WallTime> {
        let mut group = c.benchmark_group(format!("{}|{}", self.prefix, label));
        group.measurement_time(self.measurement_times.mutable);
        group
    }

    fn benchmark_group_immutable<'a>(
        &self,
        c: &'a mut Criterion,
        label: &str,
    ) -> BenchmarkGroup<'a, WallTime> {
        let mut group = c.benchmark_group(format!("{}|{}", self.prefix, label));
        group.measurement_time(self.measurement_times.immutable);
        group
    }

    /// `get` on a pre-built dict. C-side mirror omitted: `sp->terms` has
    /// no `Trie_Find` primitive — `Trie_InsertStringBuffer` does the
    /// read+modify atomically, so the realistic comparison lives in
    /// [`Self::insert_group`].
    pub fn find_group(&self, c: &mut Criterion, word: &str, label: &str) {
        let mut group = self.benchmark_group_immutable(c, label);
        group.bench_function("Rust", |b| {
            b.iter(|| self.rust_map.get(std::hint::black_box(word)).is_some())
        });
        group.finish();
    }

    /// `ADD_INCR` insert — mirrors `Trie_InsertStringBuffer` in
    /// `src/spec.c:1928`. Mutation bench: each iteration rebuilds a fresh
    /// dict from cached terms via `iter_batched(..., PerIteration)`.
    pub fn insert_group(&self, c: &mut Criterion, word: &str, label: &str) {
        let mut group = self.benchmark_group_mutable(c, label);
        let word_bytes = word.as_bytes();
        group.bench_function("Rust", |b| {
            b.iter_batched_ref(
                || build_rust_map(&self.keys),
                |dict| {
                    dict.add_term(std::hint::black_box(word), 1.0, 1);
                },
                BatchSize::PerIteration,
            )
        });
        group.bench_function("C", |b| {
            b.iter_batched_ref(
                || build_c_map(&self.key_bytes),
                |trie| trie.insert(std::hint::black_box(word_bytes)),
                BatchSize::PerIteration,
            )
        });
        group.finish();
    }

    pub fn remove_group(&self, c: &mut Criterion, word: &str, label: &str) {
        let mut group = self.benchmark_group_mutable(c, label);
        let word_bytes = word.as_bytes();
        group.bench_function("Rust", |b| {
            b.iter_batched_ref(
                || build_rust_map(&self.keys),
                |dict| dict.remove(std::hint::black_box(word)).is_some(),
                BatchSize::PerIteration,
            )
        });
        group.bench_function("C", |b| {
            b.iter_batched_ref(
                || build_c_map(&self.key_bytes),
                |trie| trie.remove(std::hint::black_box(word_bytes)),
                BatchSize::PerIteration,
            )
        });
        group.finish();
    }

    /// Bulk-load the whole corpus from scratch.
    pub fn load_group(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group_mutable(c, "Load");
        group.bench_function("Rust", |b| {
            b.iter_batched(
                || self.keys.clone(),
                |keys| build_rust_map(&keys),
                BatchSize::LargeInput,
            )
        });
        let key_bytes_view: Vec<&[u8]> =
            self.key_bytes.iter().map(|v| v.as_slice()).collect();
        group.bench_function("C", |b| {
            b.iter_batched(
                || key_bytes_view.clone(),
                |keys| {
                    let mut t = CTrie::new();
                    for k in &keys {
                        t.insert(k);
                    }
                    t
                },
                BatchSize::LargeInput,
            )
        });
        group.finish();
    }

    /// Full lex walk — `TermDictionary::iter()` vs `Trie_IterateAll`.
    /// Hot path for fork-GC terms walk (`src/fork_gc/terms.c:20`).
    pub fn iter_all_group(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group_immutable(c, "IterAll");
        group.bench_function("Rust", |b| {
            b.iter(|| {
                let mut count = 0usize;
                for _ in self.rust_map.iter() {
                    count += 1;
                }
                std::hint::black_box(count)
            })
        });
        group.bench_function("C", |b| {
            b.iter(|| std::hint::black_box(self.c_map.iterate_all()))
        });
        group.finish();
    }

    /// DFA-filtered walk — `TermDictionary::iterate_dfa` vs `Trie_Iterate`.
    /// Hot path for FT.SEARCH prefix/fuzzy queries (`src/query.c:617`).
    /// We sum the per-match distance on both sides so the running-min
    /// bookkeeping is included — it is load-bearing for FT.SUGGET FUZZY
    /// ranking (memory `project_dfa_filter_dist_semantics`).
    pub fn dfa_group(&self, c: &mut Criterion, prefix: &str, max_dist: u32, prefix_mode: bool) {
        let label = format!("DFA[prefix={prefix:?}, dist={max_dist}, pm={prefix_mode}]");
        let mut group = self.benchmark_group_immutable(c, &label);
        group.bench_function("Rust", |b| {
            b.iter(|| {
                let mut count = 0usize;
                let mut dist_sum: u64 = 0;
                for (_, _, dist) in self.rust_map.iterate_dfa(prefix, max_dist, prefix_mode) {
                    count += 1;
                    dist_sum += u64::from(dist);
                }
                std::hint::black_box((count, dist_sum))
            })
        });
        let prefix_bytes = prefix.as_bytes();
        group.bench_function("C", |b| {
            b.iter(|| {
                std::hint::black_box(self.c_map.iterate_dfa(
                    prefix_bytes,
                    max_dist as i32,
                    prefix_mode,
                ))
            })
        });
        group.finish();
    }

    /// Wildcard walk — `TermDictionary::wildcard_iter` vs `Trie_IterateWildcard`.
    pub fn wildcard_group(&self, c: &mut Criterion, pattern: &str) {
        let label = format!("Wildcard[{pattern}]");
        let mut group = self.benchmark_group_immutable(c, &label);
        group.bench_function("Rust", |b| {
            b.iter(|| {
                let mut count = 0usize;
                for entry in self.rust_map.wildcard_iter(std::hint::black_box(pattern)) {
                    std::hint::black_box(entry);
                    count += 1;
                }
                std::hint::black_box(count)
            })
        });
        group.bench_function("C", |b| {
            b.iter(|| std::hint::black_box(self.c_map.iterate_wildcard(pattern)))
        });
        group.finish();
    }

    /// Lex-range walk. `None` bound disables that side.
    pub fn range_group(
        &self,
        c: &mut Criterion,
        min: Option<&str>,
        include_min: bool,
        max: Option<&str>,
        include_max: bool,
    ) {
        let label = format!(
            "Range[min={:?}({}), max={:?}({})]",
            min,
            if include_min { "incl" } else { "excl" },
            max,
            if include_max { "incl" } else { "excl" },
        );
        let mut group = self.benchmark_group_immutable(c, &label);
        group.bench_function("Rust", |b| {
            b.iter(|| {
                let mut count = 0usize;
                for entry in self
                    .rust_map
                    .range_iter(min, include_min, max, include_max)
                {
                    std::hint::black_box(entry);
                    count += 1;
                }
                std::hint::black_box(count)
            })
        });
        group.bench_function("C", |b| {
            b.iter(|| {
                std::hint::black_box(self.c_map.iterate_range(
                    min,
                    include_min,
                    max,
                    include_max,
                ))
            })
        });
        group.finish();
    }

    /// "Contains anywhere" walk — `TermDictionary::contains_iter` vs
    /// `Trie_IterateContains(prefix=false, suffix=false)`.
    pub fn contains_group(&self, c: &mut Criterion, target: &str) {
        let label = format!("Contains[{target}]");
        let mut group = self.benchmark_group_immutable(c, &label);
        group.bench_function("Rust", |b| {
            b.iter(|| {
                let mut count = 0usize;
                for entry in self.rust_map.contains_iter(std::hint::black_box(target)) {
                    std::hint::black_box(entry);
                    count += 1;
                }
                std::hint::black_box(count)
            })
        });
        group.bench_function("C", |b| {
            b.iter(|| std::hint::black_box(self.c_map.iterate_contains(target)))
        });
        group.finish();
    }
}

/// Build a fresh [`TermDictionary`] from the cached corpus. Used as the
/// setup closure for `iter_batched` mutation benches and as the
/// one-shot construction for immutable benches.
pub fn build_rust_map(keys: &[String]) -> TermDictionary {
    let mut dict = TermDictionary::new();
    for k in keys {
        dict.add_term(k, 1.0, 1);
    }
    dict
}

/// Build a fresh [`CTrie`] from the cached corpus.
pub fn build_c_map(keys: &[Vec<u8>]) -> CTrie {
    let mut t = CTrie::new();
    for k in keys {
        t.insert(k);
    }
    t
}
