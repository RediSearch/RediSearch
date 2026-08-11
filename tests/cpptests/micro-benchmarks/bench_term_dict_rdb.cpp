/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

// A/B micro-benchmark: RDB persistence of the FT.SEARCH terms dictionary —
// the C rune Trie driven as spec.c drives it (`TrieType_GenericSave/Load`,
// per-entry score and num_docs, no payloads) versus the Rust `TermDictionary`
// through its production entry points (`TermDictionary_RdbSave/RdbLoad`).
// Both sides speak the same wire format, so each load row also parses the
// other side's bytes correctly; the C-emitted buffer feeds both loads.
//
// This wire path is load-bearing on the disk/SST save+load flow and the
// pre-2.0 legacy-upgrade load, not on plain in-memory RDB (which rebuilds
// the index by keyspace scan).
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
#include "term_dictionary_ffi.h"

#include <cstdint>
#include <random>
#include <string>
#include <unordered_set>
#include <vector>

namespace {

// Deterministic lowercase-ASCII corpus. The terms dictionary stores
// already-folded terms (the tokenizer folds before insert), so lowercase
// keeps the two sides' load-time fold work symmetric.
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

// Per-term score / num_docs with some spread, deterministic per index.
float ScoreFor(size_t i) {
  return 1.0f + static_cast<float>(i % 5);
}
size_t NumDocsFor(size_t i) {
  return 1 + i % 7;
}

Trie* BuildC(const std::vector<std::string>& corpus) {
  Trie* t = NewTrie(NULL, Trie_Sort_Lex);
  for (size_t i = 0; i < corpus.size(); ++i) {
    Trie_InsertStringBuffer(t, corpus[i].c_str(), corpus[i].size(), ScoreFor(i), 0, NULL,
                            NumDocsFor(i));
  }
  return t;
}

TermDictionary* BuildRust(const std::vector<std::string>& corpus) {
  TermDictionary* d = NewTermDictionary();
  for (size_t i = 0; i < corpus.size(); ++i) {
    TermDictionary_AddTerm(d, corpus[i].c_str(), corpus[i].size(), ScoreFor(i), NumDocsFor(i));
  }
  return d;
}

// ---- RDB save / load ----
//
// Save: fork-child serialize on the disk/SST flow.
// Load: restart / replica full-sync rebuild — the critical path.

void BM_TermDict_RdbSave_C(benchmark::State& state) {
  auto corpus = MakeCorpus(42, state.range(0));
  Trie* t = BuildC(corpus);
  RedisModuleIO* io = RMCK_CreateRdbIO();

  for (auto _ : state) {
    io->buffer.clear();  // keeps capacity: steady-state serialize, no regrowth
    TrieType_GenericSave(io, t, /*savePayloads=*/false, /*saveNumDocs=*/true);
    benchmark::DoNotOptimize(io->buffer.data());
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * corpus.size());
  state.counters["rdb_KiB"] = static_cast<double>(io->buffer.size()) / 1024.0;

  RMCK_FreeRdbIO(io);
  TrieType_Free(t);
}

void BM_TermDict_RdbSave_Rust(benchmark::State& state) {
  auto corpus = MakeCorpus(42, state.range(0));
  TermDictionary* d = BuildRust(corpus);
  RedisModuleIO* io = RMCK_CreateRdbIO();

  for (auto _ : state) {
    io->buffer.clear();  // keeps capacity: steady-state serialize, no regrowth
    TermDictionary_RdbSave(io, d);
    benchmark::DoNotOptimize(io->buffer.data());
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * corpus.size());
  state.counters["rdb_KiB"] = static_cast<double>(io->buffer.size()) / 1024.0;

  RMCK_FreeRdbIO(io);
  TermDictionary_Free(d);
}

void BM_TermDict_RdbLoad_C(benchmark::State& state) {
  auto corpus = MakeCorpus(42, state.range(0));
  Trie* t = BuildC(corpus);
  RedisModuleIO* io = RMCK_CreateRdbIO();
  TrieType_GenericSave(io, t, false, true);
  TrieType_Free(t);

  for (auto _ : state) {
    io->read_pos = 0;
    // spec.c loads sp->terms in lex mode.
    Trie* loaded = static_cast<Trie*>(
        TrieType_GenericLoad(io, /*loadPayloads=*/false, /*loadNumDocs=*/true, Trie_Sort_Lex));
    benchmark::DoNotOptimize(loaded);
    state.PauseTiming();
    TrieType_Free(loaded);
    state.ResumeTiming();
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * corpus.size());

  RMCK_FreeRdbIO(io);
}

void BM_TermDict_RdbLoad_Rust(benchmark::State& state) {
  auto corpus = MakeCorpus(42, state.range(0));
  Trie* t = BuildC(corpus);
  RedisModuleIO* io = RMCK_CreateRdbIO();
  TrieType_GenericSave(io, t, false, true);
  TrieType_Free(t);

  for (auto _ : state) {
    io->read_pos = 0;
    TermDictionary* loaded = TermDictionary_RdbLoad(io);
    benchmark::DoNotOptimize(loaded);
    state.PauseTiming();
    TermDictionary_Free(loaded);
    state.ResumeTiming();
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * corpus.size());

  RMCK_FreeRdbIO(io);
}

// Bootstraps the redismock module-API table so the C trie's allocator calls
// resolve. The Rust side routes through its own global allocator, which
// bottoms out in the same table.
static int bench_OnLoad(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  (void)argv;
  (void)argc;
  return RedisModule_Init(ctx, "bench_term_dict_rdb", 1, REDISMODULE_APIVER_1);
}

}  // namespace

BENCHMARK(BM_TermDict_RdbSave_C)->Arg(10000)->Arg(100000)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_TermDict_RdbSave_Rust)->Arg(10000)->Arg(100000)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_TermDict_RdbLoad_C)->Arg(10000)->Arg(100000)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_TermDict_RdbLoad_Rust)->Arg(10000)->Arg(100000)->Unit(benchmark::kMillisecond);

int main(int argc, char** argv) {
  RMCK_Bootstrap(bench_OnLoad, nullptr, 0);
  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  RMCK_Shutdown();
  return 0;
}
