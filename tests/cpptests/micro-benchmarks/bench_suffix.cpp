/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// A/B micro-benchmark: the legacy C TEXT suffix trie (`suffix.c`, rune-keyed
// `Trie`) versus the Rust `TermSuffixIndex` reached through its FFI. Both APIs
// are C-callable and both are still compiled in (the C TEXT half of `suffix.c`
// is dead after the query/index switch but not yet removed), so they can be
// driven head-to-head over the same corpus.
//
// LOCAL-ONLY (`bench_*`, not `benchmark_*`): it exercises soon-to-be-removed C
// code and is not part of the nightly CI benchmark set.
//
// CAVEAT: cross-language C<->Rust LTO is OFF on local dev builds (ON in
// prod/CI), so the FFI-call overhead on the Rust rows is OVER-estimated here.
// Treat FFI-dominated rows as pessimistic until rerun on an LTO build.

#include "benchmark/benchmark.h"
#include "redismock/redismock.h"
#include "rmalloc.h"

#include "suffix.h"
#include "trie/trie.h"
#include "trie/rune_util.h"
#include "term_suffix_index_ffi.h"

#include <malloc/malloc.h>

#include <cstdint>
#include <random>
#include <string>
#include <unordered_set>
#include <vector>

namespace {

constexpr size_t kNumWords = 5000;

// Deterministic lowercase-ASCII corpus. Lowercase because the C suffix trie is
// fed already-folded tokens by the tokenizer, while the Rust side folds on
// insert; feeding lowercase keeps the stored content identical on both sides.
std::vector<std::string> MakeCorpus(uint32_t seed) {
  std::mt19937 rng(seed);
  std::uniform_int_distribution<int> len_dist(3, 12);
  std::uniform_int_distribution<int> letter_dist(0, 25);
  std::unordered_set<std::string> seen;
  std::vector<std::string> out;
  out.reserve(kNumWords);
  while (out.size() < kNumWords) {
    int n = len_dist(rng);
    std::string s;
    s.reserve(n);
    for (int i = 0; i < n; ++i) s.push_back(static_cast<char>('a' + letter_dist(rng)));
    if (seen.insert(s).second) out.push_back(std::move(s));
  }
  return out;
}

// A handful of short needles taken from corpus words, so they actually match.
std::vector<std::string> MakeNeedles(const std::vector<std::string>& corpus) {
  std::vector<std::string> needles;
  for (size_t i = 0; i < corpus.size() && needles.size() < 16; i += 313) {
    const std::string& w = corpus[i];
    if (w.size() >= 4) needles.push_back(w.substr(1, 2));  // interior 2-gram
  }
  return needles;
}

// Live bytes in the default malloc zone. Both the C trie (via `rm_malloc`) and
// the Rust index (via its `RedisAlloc` global allocator) bottom out at
// `RedisModule_Alloc` -> redismock -> libc malloc, so a single default-zone
// snapshot captures both. The delta around one build is the real footprint —
// unlike `TrieType_MemUsage`, which only estimates node structure and omits the
// `suffixData` payloads (term-pointer arrays + duplicated term strings).
size_t HeapInUse() {
  malloc_statistics_t s = {};
  malloc_zone_statistics(malloc_default_zone(), &s);
  return s.size_in_use;
}

// Counting callback, shared by both APIs (identical signature). C linkage so it
// is a valid `TrieSuffixCallback` / `TermSuffixIterateCallback`.
extern "C" int count_cb(const char* s, size_t n, void* ctx, void* payload) {
  (void)s;
  (void)n;
  (void)payload;
  ++*static_cast<size_t*>(ctx);
  return 0;  // continue iterating
}

// ---- C suffix trie helpers ----

Trie* BuildC(const std::vector<std::string>& corpus) {
  Trie* t = NewTrie(suffixTrie_freeCallback, Trie_Sort_Lex);
  for (const auto& w : corpus) {
    addSuffixTrie(t, w.c_str(), static_cast<uint32_t>(w.size()));
  }
  return t;
}

// ---- Rust suffix index helpers ----

TermSuffixIndex* BuildRust(const std::vector<std::string>& corpus) {
  TermSuffixIndex* t = TermSuffixIndex_New();
  for (const auto& w : corpus) {
    TermSuffixIndex_Add(t, w.c_str(), w.size());
  }
  return t;
}

// ===================== Insert (+ resulting memory) =====================

void BM_Suffix_Insert_C(benchmark::State& state) {
  auto corpus = MakeCorpus(42);

  // One-shot real footprint via the allocator (not timed).
  size_t before = HeapInUse();
  Trie* probe = BuildC(corpus);
  double footprint = static_cast<double>(HeapInUse() - before);
  double self_report = static_cast<double>(TrieType_MemUsage(probe));
  TrieType_Free(probe);

  for (auto _ : state) {
    Trie* t = BuildC(corpus);
    benchmark::DoNotOptimize(t);
    state.PauseTiming();
    TrieType_Free(t);
    state.ResumeTiming();
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * corpus.size());
  state.counters["heap_KiB"] = footprint / 1024.0;
  state.counters["self_KiB"] = self_report / 1024.0;
}

void BM_Suffix_Insert_Rust(benchmark::State& state) {
  auto corpus = MakeCorpus(42);

  // One-shot real footprint via the allocator (not timed).
  size_t before = HeapInUse();
  TermSuffixIndex* probe = BuildRust(corpus);
  double footprint = static_cast<double>(HeapInUse() - before);
  double self_report = static_cast<double>(TermSuffixIndex_MemUsage(probe));
  TermSuffixIndex_Free(probe);

  for (auto _ : state) {
    TermSuffixIndex* t = BuildRust(corpus);
    benchmark::DoNotOptimize(t);
    state.PauseTiming();
    TermSuffixIndex_Free(t);
    state.ResumeTiming();
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * corpus.size());
  state.counters["heap_KiB"] = footprint / 1024.0;
  state.counters["self_KiB"] = self_report / 1024.0;
}

// ===================== Contains / Suffix queries =====================

// `c_type` is SUFFIX_TYPE_CONTAINS or SUFFIX_TYPE_SUFFIX.
void RunQueryC(benchmark::State& state, SuffixType c_type) {
  auto corpus = MakeCorpus(42);
  auto needles = MakeNeedles(corpus);
  Trie* t = BuildC(corpus);

  // Pre-convert needles to (lowercased) runes, as the query path does.
  std::vector<std::pair<rune*, size_t>> runes;
  for (const auto& nd : needles) {
    size_t rl = 0;
    rune* r = strToLowerRunes(nd.c_str(), nd.size(), &rl);
    runes.emplace_back(r, rl);
  }

  size_t count = 0;
  for (auto _ : state) {
    for (auto& [r, rl] : runes) {
      SuffixCtx sc = {};
      sc.trie = t;
      sc.rune = r;
      sc.runelen = rl;
      sc.type = c_type;
      sc.callback = count_cb;
      sc.cbCtx = &count;
      Suffix_IterateContains(&sc);
    }
  }
  benchmark::DoNotOptimize(count);
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * needles.size());

  for (auto& [r, rl] : runes) rm_free(r);
  TrieType_Free(t);
}

void RunQueryRust(benchmark::State& state, bool contains) {
  auto corpus = MakeCorpus(42);
  auto needles = MakeNeedles(corpus);
  TermSuffixIndex* t = BuildRust(corpus);

  size_t count = 0;
  for (auto _ : state) {
    for (const auto& nd : needles) {
      if (contains) {
        TermSuffixIndex_IterateContains(t, nd.c_str(), nd.size(), count_cb, &count);
      } else {
        TermSuffixIndex_IterateSuffix(t, nd.c_str(), nd.size(), count_cb, &count);
      }
    }
  }
  benchmark::DoNotOptimize(count);
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * needles.size());

  TermSuffixIndex_Free(t);
}

void BM_Suffix_Contains_C(benchmark::State& state) { RunQueryC(state, SUFFIX_TYPE_CONTAINS); }
void BM_Suffix_Contains_Rust(benchmark::State& state) { RunQueryRust(state, /*contains=*/true); }
void BM_Suffix_Suffix_C(benchmark::State& state) { RunQueryC(state, SUFFIX_TYPE_SUFFIX); }
void BM_Suffix_Suffix_Rust(benchmark::State& state) { RunQueryRust(state, /*contains=*/false); }

// ===================== Wildcard query =====================

// `*ab*`-style anchored wildcard patterns built from corpus 2-grams.
std::vector<std::string> MakeWildcards(const std::vector<std::string>& corpus) {
  std::vector<std::string> pats;
  for (size_t i = 0; i < corpus.size() && pats.size() < 16; i += 313) {
    const std::string& w = corpus[i];
    if (w.size() >= 4) pats.push_back("*" + w.substr(1, 2) + "*");
  }
  return pats;
}

void BM_Suffix_Wildcard_C(benchmark::State& state) {
  auto corpus = MakeCorpus(42);
  auto pats = MakeWildcards(corpus);
  Trie* t = BuildC(corpus);

  // C wildcard needs both the rune form (token selection) and the lowercased
  // UTF-8 form (the `Wildcard_MatchChar` recheck) — same as the query path.
  struct Prep {
    rune* r;
    size_t rl;
    std::string cstr;
  };
  std::vector<Prep> preps;
  for (const auto& p : pats) {
    size_t rl = 0;
    rune* r = strToLowerRunes(p.c_str(), p.size(), &rl);
    size_t cl = 0;
    char* c = runesToStr(r, rl, &cl);
    preps.push_back({r, rl, std::string(c, cl)});
    rm_free(c);
  }
  struct timespec never;
  never.tv_sec = static_cast<time_t>(1) << 40;
  never.tv_nsec = 0;

  size_t count = 0;
  for (auto _ : state) {
    for (auto& pr : preps) {
      SuffixCtx sc = {};
      sc.trie = t;
      sc.rune = pr.r;
      sc.runelen = pr.rl;
      sc.cstr = pr.cstr.c_str();
      sc.cstrlen = pr.cstr.size();
      sc.type = SUFFIX_TYPE_WILDCARD;
      sc.callback = count_cb;
      sc.cbCtx = &count;
      sc.timeout = &never;
      sc.skipTimeoutChecks = true;
      Suffix_IterateWildcard(&sc);
    }
  }
  benchmark::DoNotOptimize(count);
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * pats.size());

  for (auto& pr : preps) rm_free(pr.r);
  TrieType_Free(t);
}

void BM_Suffix_Wildcard_Rust(benchmark::State& state) {
  auto corpus = MakeCorpus(42);
  auto pats = MakeWildcards(corpus);
  TermSuffixIndex* t = BuildRust(corpus);

  size_t count = 0;
  for (auto _ : state) {
    for (const auto& p : pats) {
      TermSuffixIndex_IterateWildcard(t, p.c_str(), p.size(), count_cb, &count);
    }
  }
  benchmark::DoNotOptimize(count);
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * pats.size());

  TermSuffixIndex_Free(t);
}

// ===================== Delete (GC path) =====================
// Build is rebuilt (untimed) each iteration; only the delete loop is timed.

void BM_Suffix_Delete_C(benchmark::State& state) {
  auto corpus = MakeCorpus(42);
  for (auto _ : state) {
    state.PauseTiming();
    Trie* t = BuildC(corpus);
    state.ResumeTiming();
    for (const auto& w : corpus) {
      deleteSuffixTrie(t, w.c_str(), static_cast<uint32_t>(w.size()));
    }
    state.PauseTiming();
    TrieType_Free(t);
    state.ResumeTiming();
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * corpus.size());
}

void BM_Suffix_Delete_Rust(benchmark::State& state) {
  auto corpus = MakeCorpus(42);
  for (auto _ : state) {
    state.PauseTiming();
    TermSuffixIndex* t = BuildRust(corpus);
    state.ResumeTiming();
    for (const auto& w : corpus) {
      TermSuffixIndex_Remove(t, w.c_str(), w.size());
    }
    state.PauseTiming();
    TermSuffixIndex_Free(t);
    state.ResumeTiming();
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * corpus.size());
}

// ===================== Dump-all (FT.DEBUG DUMP_SUFFIX_TRIE) =====================
// Walk every key (each term plus its suffix entries). The C path decodes each
// rune key to UTF-8 (as DumpSuffix does); the Rust path yields UTF-8 directly.

void BM_Suffix_DumpAll_C(benchmark::State& state) {
  auto corpus = MakeCorpus(42);
  Trie* t = BuildC(corpus);
  size_t total = 0, keys = 0;
  for (auto _ : state) {
    TrieIterator* it = Trie_IterateAll(t);
    rune* rstr;
    t_len len;
    float score;
    while (TrieIterator_Next(it, &rstr, &len, NULL, &score, NULL, NULL)) {
      size_t slen = 0;
      char* s = runesToStr(rstr, len, &slen);
      total += slen;
      ++keys;
      rm_free(s);
    }
    TrieIterator_Free(it);
  }
  benchmark::DoNotOptimize(total);
  state.SetItemsProcessed(static_cast<int64_t>(keys));
  TrieType_Free(t);
}

void BM_Suffix_DumpAll_Rust(benchmark::State& state) {
  auto corpus = MakeCorpus(42);
  TermSuffixIndex* t = BuildRust(corpus);
  size_t total = 0, keys = 0;
  for (auto _ : state) {
    TermSuffixIndexIterator* it = TermSuffixIndex_IterateAll(t);
    const char* s;
    size_t len;
    while (TermSuffixIndexIterator_Next(it, &s, &len)) {
      total += len;
      ++keys;
    }
    TermSuffixIndexIterator_Free(it);
  }
  benchmark::DoNotOptimize(total);
  state.SetItemsProcessed(static_cast<int64_t>(keys));
  TermSuffixIndex_Free(t);
}

}  // namespace

BENCHMARK(BM_Suffix_Insert_C)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_Suffix_Insert_Rust)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_Suffix_Contains_C)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_Suffix_Contains_Rust)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_Suffix_Suffix_C)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_Suffix_Suffix_Rust)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_Suffix_Wildcard_C)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_Suffix_Wildcard_Rust)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_Suffix_Delete_C)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_Suffix_Delete_Rust)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_Suffix_DumpAll_C)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_Suffix_DumpAll_Rust)->Unit(benchmark::kMicrosecond);

// Minimal module bootstrap: populate the redismock `RedisModule_*` table so the
// C suffix trie's allocator calls resolve. The Rust side uses its own allocator
// and needs nothing here.
static int bench_OnLoad(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  (void)argv;
  (void)argc;
  return RedisModule_Init(ctx, "bench_suffix", 1, REDISMODULE_APIVER_1);
}

int main(int argc, char** argv) {
  RMCK_Bootstrap(bench_OnLoad, nullptr, 0);
  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  RMCK_Shutdown();
  return 0;
}
