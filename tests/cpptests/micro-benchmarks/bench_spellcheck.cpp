/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// A/B micro-benchmark: the C spell-check dictionary (a lex-sorted rune-keyed
// `Trie`, driven exactly as `dictionary.c` and `spell_check.c` drive it)
// versus the Rust `SpellCheckDictionary` reached through its FFI. Both APIs
// are C-callable, so they can be driven head-to-head over the same corpus.
//
// LOCAL-ONLY (`bench_*`, not `benchmark_*`): A/B exploration for the port,
// not part of the nightly CI benchmark set.
//
// The corpus size is the argument axis (`->Arg(N)`): the C fuzzy path is a
// DFA-pruned trie descent while the Rust path is a full-corpus Levenshtein
// scan, so the gap is expected to widen with dictionary size.
//
// CAVEAT: cross-language C<->Rust LTO is OFF on local dev builds (ON in
// prod/CI), so the FFI-call overhead on the Rust rows is OVER-estimated here.
// Treat FFI-dominated rows as pessimistic until rerun on an LTO build.

#include "benchmark/benchmark.h"
#include "redismock/redismock.h"
#include "rmalloc.h"

#include "trie/trie.h"
#include "trie/trie_node.h"
#include "trie/levenshtein.h"
#include "trie/rune_util.h"
#include "spellcheck_dictionary_ffi.h"

#include <malloc/malloc.h>

#include <cstdint>
#include <random>
#include <string>
#include <unordered_set>
#include <vector>

namespace {

constexpr size_t kNumQueries = 16;

// Deterministic lowercase-ASCII corpus. Lowercase keeps the two sides'
// case-folding work symmetric: the C DFA folds node runes during descent,
// the Rust side lowercases needle and keys.
std::vector<std::string> MakeCorpus(uint32_t seed, size_t num_words) {
  std::mt19937 rng(seed);
  std::uniform_int_distribution<int> len_dist(3, 12);
  std::uniform_int_distribution<int> letter_dist(0, 25);
  std::unordered_set<std::string> seen;
  std::vector<std::string> out;
  out.reserve(num_words);
  while (out.size() < num_words) {
    int n = len_dist(rng);
    std::string s;
    s.reserve(n);
    for (int i = 0; i < n; ++i) s.push_back(static_cast<char>('a' + letter_dist(rng)));
    if (seen.insert(s).second) out.push_back(std::move(s));
  }
  return out;
}

// Exact-hit queries: corpus words, evenly sampled.
std::vector<std::string> MakeHits(const std::vector<std::string>& corpus) {
  std::vector<std::string> hits;
  size_t step = corpus.size() / kNumQueries;
  for (size_t i = 0; i < corpus.size() && hits.size() < kNumQueries; i += step) {
    hits.push_back(corpus[i]);
  }
  return hits;
}

// Miss queries: guaranteed absent (digits are outside the corpus alphabet).
std::vector<std::string> MakeMisses(const std::vector<std::string>& corpus) {
  std::vector<std::string> misses = MakeHits(corpus);
  for (auto& m : misses) m[m.size() / 2] = '0';
  return misses;
}

// Fuzzy queries: corpus words with one substitution to a character outside
// the corpus alphabet — exactly distance 1 from their source word.
std::vector<std::string> MakeFuzzyQueries(const std::vector<std::string>& corpus) {
  return MakeMisses(corpus);
}

// Live bytes in the default malloc zone. Both the C trie (via `rm_malloc`)
// and the Rust dictionary (via its `RedisAlloc` global allocator) bottom out
// at `RedisModule_Alloc` -> redismock -> libc malloc, so a single
// default-zone snapshot captures both.
size_t HeapInUse() {
  malloc_statistics_t s = {};
  malloc_zone_statistics(malloc_default_zone(), &s);
  return s.size_in_use;
}

// ---- C dictionary helpers (as dictionary.c / spell_check.c drive the Trie) ----

Trie* BuildC(const std::vector<std::string>& corpus) {
  Trie* t = NewTrie(NULL, Trie_Sort_Lex);
  for (const auto& w : corpus) {
    // Dictionary_Add: score 1, incr, no payload.
    Trie_InsertStringBuffer(t, w.c_str(), w.size(), 1, 1, NULL, 0);
  }
  return t;
}

// SpellCheck_IsTermExistsInTrie: fuzzy iterate at distance 0, first match wins.
bool ContainsC(Trie* t, const std::string& term) {
  rune* rstr = NULL;
  t_len slen = 0;
  float score = 0;
  int dist = 0;
  TrieIterator* it = Trie_IterateFuzzy(t, term.c_str(), term.size(), 0, TRIE_MATCH_EDIT_DISTANCE);
  if (it == NULL) return false;
  bool found = TrieIterator_Next(it, &rstr, &slen, NULL, &score, NULL, &dist) != 0;
  TrieIterator_Free(it);
  return found;
}

// SpellCheck_FindSuggestions' trie walk: fuzzy iterate at `max_dist`, decode
// every match to UTF-8 (the real path hands each decoded term to the scorer).
size_t FuzzyDrainC(Trie* t, const std::string& term, int max_dist) {
  rune* rstr = NULL;
  t_len slen = 0;
  float score = 0;
  int dist = 0;
  size_t matches = 0;
  TrieIterator* it =
      Trie_IterateFuzzy(t, term.c_str(), term.size(), max_dist, TRIE_MATCH_EDIT_DISTANCE);
  if (it == NULL) return 0;
  while (TrieIterator_Next(it, &rstr, &slen, NULL, &score, NULL, &dist)) {
    size_t len = 0;
    char* s = runesToStr(rstr, slen, &len);
    benchmark::DoNotOptimize(s);
    rm_free(s);
    ++matches;
  }
  TrieIterator_Free(it);
  return matches;
}

// ---- Rust dictionary helpers ----

SpellCheckDictionary* BuildRust(const std::vector<std::string>& corpus) {
  SpellCheckDictionary* d = SpellCheckDictionary_New();
  for (const auto& w : corpus) {
    SpellCheckDictionary_Add(d, w.c_str(), w.size());
  }
  return d;
}

size_t FuzzyDrainRust(SpellCheckDictionary* d, const std::string& term, uint32_t max_dist) {
  size_t matches = 0;
  SpellCheckDictionaryIterator* it =
      SpellCheckDictionary_IterateFuzzy(d, term.c_str(), term.size(), max_dist);
  const char* s;
  size_t len;
  while (SpellCheckDictionaryIterator_Next(it, &s, &len)) {
    benchmark::DoNotOptimize(s);
    ++matches;
  }
  SpellCheckDictionaryIterator_Free(it);
  return matches;
}

// ===================== Insert (+ resulting memory) =====================

void BM_SpellCheck_Insert_C(benchmark::State& state) {
  auto corpus = MakeCorpus(42, state.range(0));

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

void BM_SpellCheck_Insert_Rust(benchmark::State& state) {
  auto corpus = MakeCorpus(42, state.range(0));

  // One-shot real footprint via the allocator (not timed).
  size_t before = HeapInUse();
  SpellCheckDictionary* probe = BuildRust(corpus);
  double footprint = static_cast<double>(HeapInUse() - before);
  SpellCheckDictionary_Free(probe);

  for (auto _ : state) {
    SpellCheckDictionary* d = BuildRust(corpus);
    benchmark::DoNotOptimize(d);
    state.PauseTiming();
    SpellCheckDictionary_Free(d);
    state.ResumeTiming();
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * corpus.size());
  state.counters["heap_KiB"] = footprint / 1024.0;
}

// ===================== Contains (exclude-dict membership check) =====================
// Half hits, half misses, interleaved: the C path short-circuits on the first
// DFA match, the Rust path scans the sorted keys until a fold-equal one.

void BM_SpellCheck_Contains_C(benchmark::State& state) {
  auto corpus = MakeCorpus(42, state.range(0));
  auto hits = MakeHits(corpus);
  auto misses = MakeMisses(corpus);
  Trie* t = BuildC(corpus);

  size_t found = 0;
  for (auto _ : state) {
    for (size_t i = 0; i < hits.size(); ++i) {
      found += ContainsC(t, hits[i]);
      found += ContainsC(t, misses[i]);
    }
  }
  benchmark::DoNotOptimize(found);
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * hits.size() * 2);
  TrieType_Free(t);
}

void BM_SpellCheck_Contains_Rust(benchmark::State& state) {
  auto corpus = MakeCorpus(42, state.range(0));
  auto hits = MakeHits(corpus);
  auto misses = MakeMisses(corpus);
  SpellCheckDictionary* d = BuildRust(corpus);

  size_t found = 0;
  for (auto _ : state) {
    for (size_t i = 0; i < hits.size(); ++i) {
      found += SpellCheckDictionary_Contains(d, hits[i].c_str(), hits[i].size());
      found += SpellCheckDictionary_Contains(d, misses[i].c_str(), misses[i].size());
    }
  }
  benchmark::DoNotOptimize(found);
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * hits.size() * 2);
  SpellCheckDictionary_Free(d);
}

// ===================== Fuzzy (include-dict suggestion lookup) =====================
// Queries are distance 1 from a corpus word; drained fully, like
// SpellCheck_FindSuggestions. Distance is the second argument.

void BM_SpellCheck_Fuzzy_C(benchmark::State& state) {
  auto corpus = MakeCorpus(42, state.range(0));
  auto queries = MakeFuzzyQueries(corpus);
  int max_dist = static_cast<int>(state.range(1));
  Trie* t = BuildC(corpus);

  size_t matches = 0;
  for (auto _ : state) {
    for (const auto& q : queries) {
      matches += FuzzyDrainC(t, q, max_dist);
    }
  }
  benchmark::DoNotOptimize(matches);
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * queries.size());
  TrieType_Free(t);
}

void BM_SpellCheck_Fuzzy_Rust(benchmark::State& state) {
  auto corpus = MakeCorpus(42, state.range(0));
  auto queries = MakeFuzzyQueries(corpus);
  uint32_t max_dist = static_cast<uint32_t>(state.range(1));
  SpellCheckDictionary* d = BuildRust(corpus);

  size_t matches = 0;
  for (auto _ : state) {
    for (const auto& q : queries) {
      matches += FuzzyDrainRust(d, q, max_dist);
    }
  }
  benchmark::DoNotOptimize(matches);
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * queries.size());
  SpellCheckDictionary_Free(d);
}

// ===================== Delete =====================
// Build is rebuilt (untimed) each iteration; only the delete loop is timed.

void BM_SpellCheck_Delete_C(benchmark::State& state) {
  auto corpus = MakeCorpus(42, state.range(0));
  for (auto _ : state) {
    state.PauseTiming();
    Trie* t = BuildC(corpus);
    state.ResumeTiming();
    for (const auto& w : corpus) {
      Trie_Delete(t, w.c_str(), w.size());
    }
    state.PauseTiming();
    TrieType_Free(t);
    state.ResumeTiming();
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * corpus.size());
}

void BM_SpellCheck_Delete_Rust(benchmark::State& state) {
  auto corpus = MakeCorpus(42, state.range(0));
  for (auto _ : state) {
    state.PauseTiming();
    SpellCheckDictionary* d = BuildRust(corpus);
    state.ResumeTiming();
    for (const auto& w : corpus) {
      SpellCheckDictionary_Remove(d, w.c_str(), w.size());
    }
    state.PauseTiming();
    SpellCheckDictionary_Free(d);
    state.ResumeTiming();
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * corpus.size());
}

// ===================== Dump-all (FT.DICTDUMP) =====================
// Walk every term. The C path decodes each rune key to UTF-8 (as
// Dictionary_Dump does); the Rust path yields UTF-8 directly.

void BM_SpellCheck_DumpAll_C(benchmark::State& state) {
  auto corpus = MakeCorpus(42, state.range(0));
  Trie* t = BuildC(corpus);
  size_t total = 0, terms = 0;
  for (auto _ : state) {
    TrieIterator* it = Trie_IterateAll(t);
    rune* rstr;
    t_len len;
    float score;
    while (TrieIterator_Next(it, &rstr, &len, NULL, &score, NULL, NULL)) {
      size_t slen = 0;
      char* s = runesToStr(rstr, len, &slen);
      total += slen;
      ++terms;
      rm_free(s);
    }
    TrieIterator_Free(it);
  }
  benchmark::DoNotOptimize(total);
  state.SetItemsProcessed(static_cast<int64_t>(terms));
  TrieType_Free(t);
}

void BM_SpellCheck_DumpAll_Rust(benchmark::State& state) {
  auto corpus = MakeCorpus(42, state.range(0));
  SpellCheckDictionary* d = BuildRust(corpus);
  size_t total = 0, terms = 0;
  for (auto _ : state) {
    SpellCheckDictionaryIterator* it = SpellCheckDictionary_IterateAll(d);
    const char* s;
    size_t len;
    while (SpellCheckDictionaryIterator_Next(it, &s, &len)) {
      total += len;
      ++terms;
    }
    SpellCheckDictionaryIterator_Free(it);
  }
  benchmark::DoNotOptimize(total);
  state.SetItemsProcessed(static_cast<int64_t>(terms));
  SpellCheckDictionary_Free(d);
}

// ===================== RDB save / load =====================
// The dict wire format (`TrieType_GenericSave/Load(…, false, false)`, score
// constant 1) driven through both aux-callback implementations:
// `TrieType_Generic*` on the C side, `SpellCheckDictionary_Rdb*` on the Rust
// side. The two emit byte-identical streams, so the load rows parse one
// C-emitted buffer and the A/B input is identical.
//
// Save: BGSAVE / replica-sync serialize, runs in the fork child.
// Load: server restart / replica full-sync rebuild — the critical path.

void BM_SpellCheck_RdbSave_C(benchmark::State& state) {
  auto corpus = MakeCorpus(42, state.range(0));
  Trie* t = BuildC(corpus);
  RedisModuleIO* io = RMCK_CreateRdbIO();

  for (auto _ : state) {
    io->buffer.clear();  // keeps capacity: steady-state serialize, no regrowth
    TrieType_GenericSave(io, t, /*savePayloads=*/false, /*saveNumDocs=*/false);
    benchmark::DoNotOptimize(io->buffer.data());
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * corpus.size());
  state.counters["rdb_KiB"] = static_cast<double>(io->buffer.size()) / 1024.0;

  RMCK_FreeRdbIO(io);
  TrieType_Free(t);
}

void BM_SpellCheck_RdbSave_Rust(benchmark::State& state) {
  auto corpus = MakeCorpus(42, state.range(0));
  SpellCheckDictionary* d = BuildRust(corpus);
  RedisModuleIO* io = RMCK_CreateRdbIO();

  for (auto _ : state) {
    io->buffer.clear();  // keeps capacity: steady-state serialize, no regrowth
    SpellCheckDictionary_RdbSave(io, d);
    benchmark::DoNotOptimize(io->buffer.data());
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * corpus.size());
  state.counters["rdb_KiB"] = static_cast<double>(io->buffer.size()) / 1024.0;

  RMCK_FreeRdbIO(io);
  SpellCheckDictionary_Free(d);
}

void BM_SpellCheck_RdbLoad_C(benchmark::State& state) {
  auto corpus = MakeCorpus(42, state.range(0));
  Trie* t = BuildC(corpus);
  RedisModuleIO* io = RMCK_CreateRdbIO();
  TrieType_GenericSave(io, t, false, false);
  TrieType_Free(t);

  for (auto _ : state) {
    io->read_pos = 0;
    // SpellCheckDictAuxLoad loads dict tries in lex mode.
    Trie* loaded = static_cast<Trie*>(
        TrieType_GenericLoad(io, /*loadPayloads=*/false, /*loadNumDocs=*/false, Trie_Sort_Lex));
    benchmark::DoNotOptimize(loaded);
    state.PauseTiming();
    TrieType_Free(loaded);
    state.ResumeTiming();
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * corpus.size());

  RMCK_FreeRdbIO(io);
}

void BM_SpellCheck_RdbLoad_Rust(benchmark::State& state) {
  auto corpus = MakeCorpus(42, state.range(0));
  Trie* t = BuildC(corpus);
  RedisModuleIO* io = RMCK_CreateRdbIO();
  TrieType_GenericSave(io, t, false, false);
  TrieType_Free(t);

  for (auto _ : state) {
    io->read_pos = 0;
    SpellCheckDictionary* loaded = SpellCheckDictionary_RdbLoad(io);
    benchmark::DoNotOptimize(loaded);
    state.PauseTiming();
    SpellCheckDictionary_Free(loaded);
    state.ResumeTiming();
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * corpus.size());

  RMCK_FreeRdbIO(io);
}

}  // namespace

BENCHMARK(BM_SpellCheck_Insert_C)->Arg(1000)->Arg(10000)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_SpellCheck_Insert_Rust)->Arg(1000)->Arg(10000)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_SpellCheck_Contains_C)->Arg(1000)->Arg(10000)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_SpellCheck_Contains_Rust)->Arg(1000)->Arg(10000)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_SpellCheck_Fuzzy_C)
    ->Args({1000, 1})
    ->Args({1000, 2})
    ->Args({10000, 1})
    ->Args({10000, 2})
    ->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_SpellCheck_Fuzzy_Rust)
    ->Args({1000, 1})
    ->Args({1000, 2})
    ->Args({10000, 1})
    ->Args({10000, 2})
    ->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_SpellCheck_Delete_C)->Arg(1000)->Arg(10000)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_SpellCheck_Delete_Rust)->Arg(1000)->Arg(10000)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_SpellCheck_DumpAll_C)->Arg(1000)->Arg(10000)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_SpellCheck_DumpAll_Rust)->Arg(1000)->Arg(10000)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_SpellCheck_RdbSave_C)->Arg(1000)->Arg(10000)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_SpellCheck_RdbSave_Rust)->Arg(1000)->Arg(10000)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_SpellCheck_RdbLoad_C)->Arg(1000)->Arg(10000)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_SpellCheck_RdbLoad_Rust)->Arg(1000)->Arg(10000)->Unit(benchmark::kMicrosecond);

// Minimal module bootstrap: populate the redismock `RedisModule_*` table so
// the C trie's allocator calls resolve. The Rust side routes through its own
// global allocator, which bottoms out in the same table.
static int bench_OnLoad(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  (void)argv;
  (void)argc;
  return RedisModule_Init(ctx, "bench_spellcheck", 1, REDISMODULE_APIVER_1);
}

int main(int argc, char** argv) {
  RMCK_Bootstrap(bench_OnLoad, nullptr, 0);
  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  RMCK_Shutdown();
  return 0;
}
