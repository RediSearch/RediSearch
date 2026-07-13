/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

// A/B micro-benchmark: the C FT.SEARCH terms dictionary (a lex-sorted
// rune-keyed `Trie`, driven exactly as spec.c indexing and query.c term
// expansion drive it) versus the Rust `TermDictionary` reached through its
// FFI. Both APIs are C-callable, so they can be driven head-to-head over the
// same corpus.
//
// Covered paths and their production analogs:
//   Insert     — IndexSpec_AddTerm on every document write (indexing hot path)
//   Prefix     — Query_EvalPrefixNode expansion (`foo*`)
//   Fuzzy      — Query_EvalFuzzyNode / iterateExpandedTerms (`%foo%`)
//   Wildcard   — Query_EvalWildcardQueryNode (`f?o*`)
//   IterateAll — fork-GC term collection / FT.DEBUG DUMP_TERMS
//   Delete     — fork-GC empty-term removal
//
// The C query callbacks decode each rune key back to UTF-8 (runesToStr) before
// opening a reader, so the C rows include that decode; the Rust iterator hands
// out `const char*` directly. That asymmetry is the production difference, not
// a benchmark artifact.
//
// Match-count parity: prefix/wildcard/iterate counts must agree and are
// asserted at setup. Fuzzy counts may legitimately differ: the C matcher also
// admits terms whose all-but-last-rune form is within budget (accept-state
// check one rune early), which the Rust automaton — enforcing true
// Levenshtein distance — does not. Each fuzzy row reports its own `matches`
// counter instead.
//
// LOCAL-ONLY (`bench_*`, not `benchmark_*`): A/B exploration for the port,
// not part of the nightly CI benchmark set.
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
#include "term_dictionary_ffi.h"

#include <malloc/malloc.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <random>
#include <string>
#include <unordered_set>
#include <vector>

namespace {

constexpr size_t kNumQueries = 16;

size_t HeapInUse() {
  malloc_statistics_t s = {};
  malloc_zone_statistics(malloc_default_zone(), &s);
  return s.size_in_use;
}

// Deterministic lowercase-ASCII corpus. The terms dictionary stores
// already-folded terms (the tokenizer folds before insert), so lowercase
// keeps the two sides' fold work symmetric.
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

// Prefix queries: leading 2-3 chars of corpus words, evenly sampled — short
// prefixes make the expansion walk a real subtree.
std::vector<std::string> MakePrefixQueries(const std::vector<std::string>& corpus) {
  std::vector<std::string> out;
  size_t step = corpus.size() / kNumQueries;
  for (size_t i = 0; i < corpus.size() && out.size() < kNumQueries; i += step) {
    out.push_back(corpus[i].substr(0, 2 + i % 2));
  }
  return out;
}

// Fuzzy queries: corpus words with one substitution to a character outside
// the corpus alphabet — exactly distance 1 from their source word.
std::vector<std::string> MakeFuzzyQueries(const std::vector<std::string>& corpus) {
  std::vector<std::string> out;
  size_t step = corpus.size() / kNumQueries;
  for (size_t i = 0; i < corpus.size() && out.size() < kNumQueries; i += step) {
    std::string w = corpus[i];
    w[w.size() / 2] = '0';
    out.push_back(std::move(w));
  }
  return out;
}

// Wildcard queries: first two + last two chars of corpus words around a `*`,
// with a `?` for the third char when the word is long enough (`ab?*yz`).
std::vector<std::string> MakeWildcardQueries(const std::vector<std::string>& corpus) {
  std::vector<std::string> out;
  size_t step = corpus.size() / kNumQueries;
  for (size_t i = 0; i < corpus.size() && out.size() < kNumQueries; i += step) {
    const std::string& w = corpus[i];
    std::string pat = w.substr(0, 2);
    if (w.size() >= 6) pat += '?';
    pat += '*';
    pat += w.substr(w.size() - 2);
    out.push_back(std::move(pat));
  }
  return out;
}

// ---- C terms trie helpers (as spec.c / query.c drive the Trie) ----

Trie* BuildC(const std::vector<std::string>& corpus) {
  Trie* t = NewTrie(NULL, Trie_Sort_Lex);
  for (const auto& w : corpus) {
    // IndexSpec_AddTerm: score 1, incr, no payload, one doc.
    Trie_InsertStringBuffer(t, w.c_str(), w.size(), 1, 1, NULL, 1);
  }
  return t;
}

// runeIterCb's term handling: decode the rune key to UTF-8, then hand it on.
// The reader open is out of scope; the decode is not.
int CountingRuneCb(const rune* r, size_t n, void* ctx, void* payload, size_t numDocsInTerm) {
  (void)payload;
  (void)numDocsInTerm;
  size_t len = 0;
  char* s = runesToStr(r, n, &len);
  benchmark::DoNotOptimize(s);
  rm_free(s);
  ++*static_cast<size_t*>(ctx);
  return REDISEARCH_OK;
}

size_t PrefixDrainC(const Trie* t, const std::string& q) {
  size_t nstr = 0;
  rune* str = strToLowerRunes(q.c_str(), q.size(), &nstr);
  size_t matches = 0;
  struct timespec timeout = {0, 0};
  Trie_IterateContains(t, str, nstr, /*prefix=*/true, /*suffix=*/false, CountingRuneCb, &matches,
                       &timeout, /*skipTimeoutChecks=*/true);
  rm_free(str);
  return matches;
}

size_t WildcardDrainC(const Trie* t, const std::string& q) {
  size_t nstr = 0;
  rune* str = strToLowerRunes(q.c_str(), q.size(), &nstr);
  size_t matches = 0;
  struct timespec timeout = {0, 0};
  Trie_IterateWildcard(t, str, nstr, CountingRuneCb, &matches, &timeout,
                       /*skipTimeoutChecks=*/true);
  rm_free(str);
  return matches;
}

// iterateExpandedTerms' walk: fuzzy iterate at `max_dist`, decode every match.
size_t FuzzyDrainC(Trie* t, const std::string& q, int max_dist) {
  rune* rstr = NULL;
  t_len slen = 0;
  float score = 0;
  int dist = 0;
  size_t matches = 0;
  size_t numDocs = 0;
  TrieIterator* it = Trie_IterateFuzzy(t, q.c_str(), q.size(), max_dist, TRIE_MATCH_EDIT_DISTANCE);
  if (it == NULL) return 0;
  while (TrieIterator_Next(it, &rstr, &slen, NULL, &score, &numDocs, &dist)) {
    size_t len = 0;
    char* s = runesToStr(rstr, slen, &len);
    benchmark::DoNotOptimize(s);
    rm_free(s);
    ++matches;
  }
  TrieIterator_Free(it);
  return matches;
}

// FGC_childCollectTerms' walk: iterate every term, decode to UTF-8.
size_t IterateAllDrainC(Trie* t) {
  rune* rstr = NULL;
  t_len slen = 0;
  float score = 0;
  size_t matches = 0;
  TrieIterator* it = Trie_IterateAll(t);
  while (TrieIterator_Next(it, &rstr, &slen, NULL, &score, NULL, NULL)) {
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

TermDictionary* BuildRust(const std::vector<std::string>& corpus) {
  TermDictionary* d = NewTermDictionary();
  for (const auto& w : corpus) {
    TermDictionary_AddTerm(d, w.c_str(), w.size(), 1, 1);
  }
  return d;
}

size_t DrainRust(TermDictionaryIterator* it) {
  const char* s;
  size_t len;
  float score;
  size_t num_docs;
  size_t matches = 0;
  while (TermDictionaryIterator_Next(it, &s, &len, &score, &num_docs)) {
    benchmark::DoNotOptimize(s);
    ++matches;
  }
  TermDictionaryIterator_Free(it);
  return matches;
}

size_t PrefixDrainRust(const TermDictionary* d, const std::string& q) {
  return DrainRust(TermDictionary_IteratePrefix(d, q.c_str(), q.size()));
}

size_t WildcardDrainRust(const TermDictionary* d, const std::string& q) {
  return DrainRust(TermDictionary_IterateWildcard(d, q.c_str(), q.size()));
}

size_t FuzzyDrainRust(const TermDictionary* d, const std::string& q, uint32_t max_dist) {
  return DrainRust(TermDictionary_IterateFuzzy(d, q.c_str(), q.size(), max_dist));
}

size_t IterateAllDrainRust(const TermDictionary* d) {
  return DrainRust(TermDictionary_Iterate(d));
}

// ===================== Insert (+ resulting memory) =====================

void BM_TermDict_Insert_C(benchmark::State& state) {
  auto corpus = MakeCorpus(42, state.range(0));

  size_t before = HeapInUse();
  Trie* probe = BuildC(corpus);
  double footprint = static_cast<double>(HeapInUse() - before);
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
}

void BM_TermDict_Insert_Rust(benchmark::State& state) {
  auto corpus = MakeCorpus(42, state.range(0));

  size_t before = HeapInUse();
  TermDictionary* probe = BuildRust(corpus);
  double footprint = static_cast<double>(HeapInUse() - before);
  TermDictionary_Free(probe);

  for (auto _ : state) {
    TermDictionary* d = BuildRust(corpus);
    benchmark::DoNotOptimize(d);
    state.PauseTiming();
    TermDictionary_Free(d);
    state.ResumeTiming();
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * corpus.size());
  state.counters["heap_KiB"] = footprint / 1024.0;
}

// ===================== Query expansion =====================

void BM_TermDict_Prefix_C(benchmark::State& state) {
  auto corpus = MakeCorpus(42, state.range(0));
  auto queries = MakePrefixQueries(corpus);
  Trie* t = BuildC(corpus);

  size_t matches = 0;
  for (auto _ : state) {
    for (const auto& q : queries) matches += PrefixDrainC(t, q);
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * queries.size());
  state.counters["matches"] = static_cast<double>(matches) / state.iterations();

  TrieType_Free(t);
}

void BM_TermDict_Prefix_Rust(benchmark::State& state) {
  auto corpus = MakeCorpus(42, state.range(0));
  auto queries = MakePrefixQueries(corpus);
  TermDictionary* d = BuildRust(corpus);

  size_t matches = 0;
  for (auto _ : state) {
    for (const auto& q : queries) matches += PrefixDrainRust(d, q);
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * queries.size());
  state.counters["matches"] = static_cast<double>(matches) / state.iterations();

  TermDictionary_Free(d);
}

void BM_TermDict_Fuzzy_C(benchmark::State& state) {
  auto corpus = MakeCorpus(42, state.range(0));
  auto queries = MakeFuzzyQueries(corpus);
  Trie* t = BuildC(corpus);
  int max_dist = static_cast<int>(state.range(1));

  size_t matches = 0;
  for (auto _ : state) {
    for (const auto& q : queries) matches += FuzzyDrainC(t, q, max_dist);
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * queries.size());
  state.counters["matches"] = static_cast<double>(matches) / state.iterations();

  TrieType_Free(t);
}

void BM_TermDict_Fuzzy_Rust(benchmark::State& state) {
  auto corpus = MakeCorpus(42, state.range(0));
  auto queries = MakeFuzzyQueries(corpus);
  TermDictionary* d = BuildRust(corpus);
  uint32_t max_dist = static_cast<uint32_t>(state.range(1));

  size_t matches = 0;
  for (auto _ : state) {
    for (const auto& q : queries) matches += FuzzyDrainRust(d, q, max_dist);
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * queries.size());
  state.counters["matches"] = static_cast<double>(matches) / state.iterations();

  TermDictionary_Free(d);
}

void BM_TermDict_Wildcard_C(benchmark::State& state) {
  auto corpus = MakeCorpus(42, state.range(0));
  auto queries = MakeWildcardQueries(corpus);
  Trie* t = BuildC(corpus);

  size_t matches = 0;
  for (auto _ : state) {
    for (const auto& q : queries) matches += WildcardDrainC(t, q);
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * queries.size());
  state.counters["matches"] = static_cast<double>(matches) / state.iterations();

  TrieType_Free(t);
}

void BM_TermDict_Wildcard_Rust(benchmark::State& state) {
  auto corpus = MakeCorpus(42, state.range(0));
  auto queries = MakeWildcardQueries(corpus);
  TermDictionary* d = BuildRust(corpus);

  size_t matches = 0;
  for (auto _ : state) {
    for (const auto& q : queries) matches += WildcardDrainRust(d, q);
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * queries.size());
  state.counters["matches"] = static_cast<double>(matches) / state.iterations();

  TermDictionary_Free(d);
}

// ===================== Fork-GC paths =====================

void BM_TermDict_IterateAll_C(benchmark::State& state) {
  auto corpus = MakeCorpus(42, state.range(0));
  Trie* t = BuildC(corpus);

  for (auto _ : state) {
    size_t n = IterateAllDrainC(t);
    benchmark::DoNotOptimize(n);
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * corpus.size());

  TrieType_Free(t);
}

void BM_TermDict_IterateAll_Rust(benchmark::State& state) {
  auto corpus = MakeCorpus(42, state.range(0));
  TermDictionary* d = BuildRust(corpus);

  for (auto _ : state) {
    size_t n = IterateAllDrainRust(d);
    benchmark::DoNotOptimize(n);
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * corpus.size());

  TermDictionary_Free(d);
}

void BM_TermDict_Delete_C(benchmark::State& state) {
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

void BM_TermDict_Delete_Rust(benchmark::State& state) {
  auto corpus = MakeCorpus(42, state.range(0));

  for (auto _ : state) {
    state.PauseTiming();
    TermDictionary* d = BuildRust(corpus);
    state.ResumeTiming();
    for (const auto& w : corpus) {
      TermDictionary_Remove(d, w.c_str(), w.size());
    }
    state.PauseTiming();
    TermDictionary_Free(d);
    state.ResumeTiming();
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * corpus.size());
}

// Bootstraps the redismock module-API table so the C trie's allocator calls
// resolve. The Rust side routes through its own global allocator, which
// bottoms out in the same table.
static int bench_OnLoad(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  (void)argv;
  (void)argc;
  return RedisModule_Init(ctx, "bench_term_dict", 1, REDISMODULE_APIVER_1);
}

// Prefix/wildcard/iterate expansions must agree between the two sides on this
// ASCII corpus; abort loudly rather than publish numbers over divergent work.
// Fuzzy is exempt: the C matcher's early accept-state check admits extra
// terms by design difference (see file comment).
void RequireEqual(size_t c, size_t rust, const char* what, const std::string& q) {
  if (c != rust) {
    fprintf(stderr, "match parity violated: %s '%s': C=%zu Rust=%zu\n", what, q.c_str(), c, rust);
    abort();
  }
}

void VerifyMatchParity() {
  auto corpus = MakeCorpus(42, 10000);
  Trie* t = BuildC(corpus);
  TermDictionary* d = BuildRust(corpus);
  for (const auto& q : MakePrefixQueries(corpus)) {
    RequireEqual(PrefixDrainC(t, q), PrefixDrainRust(d, q), "prefix", q);
  }
  for (const auto& q : MakeWildcardQueries(corpus)) {
    RequireEqual(WildcardDrainC(t, q), WildcardDrainRust(d, q), "wildcard", q);
  }
  RequireEqual(IterateAllDrainC(t), IterateAllDrainRust(d), "iterate-all", "");
  TrieType_Free(t);
  TermDictionary_Free(d);
}

}  // namespace

BENCHMARK(BM_TermDict_Insert_C)->Arg(10000)->Arg(100000)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_TermDict_Insert_Rust)->Arg(10000)->Arg(100000)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_TermDict_Prefix_C)->Arg(10000)->Arg(100000)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_TermDict_Prefix_Rust)->Arg(10000)->Arg(100000)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_TermDict_Fuzzy_C)
    ->Args({10000, 1})
    ->Args({10000, 2})
    ->Args({100000, 1})
    ->Args({100000, 2})
    ->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_TermDict_Fuzzy_Rust)
    ->Args({10000, 1})
    ->Args({10000, 2})
    ->Args({100000, 1})
    ->Args({100000, 2})
    ->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_TermDict_Wildcard_C)->Arg(10000)->Arg(100000)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_TermDict_Wildcard_Rust)->Arg(10000)->Arg(100000)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_TermDict_IterateAll_C)->Arg(10000)->Arg(100000)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_TermDict_IterateAll_Rust)->Arg(10000)->Arg(100000)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_TermDict_Delete_C)->Arg(10000)->Arg(100000)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_TermDict_Delete_Rust)->Arg(10000)->Arg(100000)->Unit(benchmark::kMillisecond);

int main(int argc, char** argv) {
  RMCK_Bootstrap(bench_OnLoad, nullptr, 0);
  VerifyMatchParity();
  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  RMCK_Shutdown();
  return 0;
}
