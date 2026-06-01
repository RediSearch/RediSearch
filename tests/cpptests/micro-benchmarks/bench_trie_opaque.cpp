/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

/* Microbenchmark for the external call sites of the newly-out-of-line
 * TrieNode/TriePayload accessors. Exercises every non-test caller:
 *   - suffix.c recursiveAdd:        TrieNode_NumChildren, TrieNode_ChildAt,
 *                                   TrieNode_GetPayloadData
 *   - suffix.c Suffix_CB_Wildcard:  TriePayload_Data
 *   - query.c  per-token expansion: TrieNode_NumDocs
 *
 * Loads a word list from BENCH_WORDS_PATH (default /usr/share/dict/words),
 * builds a terms trie + a suffix trie once, then runs three bench groups
 * against fixed probe sets so master vs. branch comparisons are apples-to-
 * apples. */

#include "benchmark/benchmark.h"
#include "redismock/util.h"

#include "trie/trie.h"
#include "trie/trie_node.h"
#include "trie/rune_util.h"
#include "suffix.h"
#include "redismodule.h"

/* On the encapsulation branch TrieNode is opaque and TrieNode_NumDocs exists.
 * On master the struct is visible and there's no function for numDocs - readers
 * dereference node->numDocs directly. Detect via the internal header that the
 * branch adds. */
#if defined(__has_include) && __has_include("trie/trie_node_internal.h")
#  define BENCH_TRIE_NODE_OPAQUE 1
#else
#  define BENCH_TRIE_NODE_OPAQUE 0
#endif

#include <cstdlib>
#include <fstream>
#include <random>
#include <string>
#include <vector>

namespace {

constexpr size_t kMaxWords = 10000;

const char *wordsPath() {
  if (const char *env = std::getenv("BENCH_WORDS_PATH")) return env;
  return "/usr/share/dict/words";
}

std::vector<std::string> loadCorpus() {
  std::vector<std::string> words;
  std::ifstream in(wordsPath());
  if (!in) return words;
  std::string line;
  while (words.size() < kMaxWords && std::getline(in, line)) {
    if (line.size() >= 2 && line.size() <= 32) {
      words.push_back(std::move(line));
    }
  }
  return words;
}

struct Corpus {
  Trie *terms = nullptr;
  Trie *suffix = nullptr;
  std::vector<std::string> words;
  std::vector<std::string> probes;
  std::vector<std::string> wildcards;

  bool valid() const { return terms != nullptr && !words.empty(); }

  void Build() {
    words = loadCorpus();
    if (words.empty()) return;

    terms = NewTrie(NULL, Trie_Sort_Score);
    suffix = NewTrie(suffixTrie_freeCallback, Trie_Sort_Lex);
    for (const auto &w : words) {
      Trie_InsertStringBuffer(terms, w.c_str(), w.size(), 1, 1, NULL, 0);
      addSuffixTrie(suffix, w.c_str(), w.size());
    }

    probes = {"ing", "tion", "er", "ly", "ous", "ent"};
    wildcards = {"*ing*", "*tion*", "*ous*", "*ent*"};
  }
};

Corpus *gCorpus = nullptr;

bool ensureInit(benchmark::State &state) {
  static bool initialized = false;
  if (!initialized) {
    RMCK::init();
    gCorpus = new Corpus();
    gCorpus->Build();
    initialized = true;
  }
  if (!gCorpus->valid()) {
    state.SkipWithError("Corpus empty: set BENCH_WORDS_PATH or install /usr/share/dict/words");
    return false;
  }
  return true;
}

int countingCallback(const char *s, size_t n, void *ctx, void *payload) {
  (*static_cast<size_t *>(ctx))++;
  return REDISMODULE_OK;
}

}  // namespace

static void BM_Suffix_IterateContains(benchmark::State &state) {
  if (!ensureInit(state)) return;
  size_t probeIdx = 0;
  size_t totalHits = 0;
  for (auto _ : state) {
    const std::string &p = gCorpus->probes[probeIdx++ % gCorpus->probes.size()];
    runeBuf buf;
    size_t rlen = 0;
    rune *runes = runeBufFill(p.c_str(), p.size(), &buf, &rlen);
    size_t hits = 0;
    SuffixCtx ctx{};
    ctx.trie = gCorpus->suffix;
    ctx.rune = runes;
    ctx.runelen = rlen;
    ctx.cstr = p.c_str();
    ctx.cstrlen = p.size();
    ctx.type = SUFFIX_TYPE_CONTAINS;
    ctx.callback = countingCallback;
    ctx.cbCtx = &hits;
    ctx.timeout = NULL;
    ctx.skipTimeoutChecks = true;
    Suffix_IterateContains(&ctx);
    runeBufFree(&buf);
    totalHits += hits;
  }
  benchmark::DoNotOptimize(totalHits);
}

static void BM_Suffix_IterateWildcard(benchmark::State &state) {
  if (!ensureInit(state)) return;
  size_t probeIdx = 0;
  size_t totalHits = 0;
  for (auto _ : state) {
    const std::string &p = gCorpus->wildcards[probeIdx++ % gCorpus->wildcards.size()];
    runeBuf buf;
    size_t rlen = 0;
    rune *runes = runeBufFill(p.c_str(), p.size(), &buf, &rlen);
    size_t hits = 0;
    SuffixCtx ctx{};
    ctx.trie = gCorpus->suffix;
    ctx.rune = runes;
    ctx.runelen = rlen;
    ctx.cstr = p.c_str();
    ctx.cstrlen = p.size();
    ctx.type = SUFFIX_TYPE_WILDCARD;
    ctx.callback = countingCallback;
    ctx.cbCtx = &hits;
    ctx.timeout = NULL;
    ctx.skipTimeoutChecks = true;
    Suffix_IterateWildcard(&ctx);
    runeBufFree(&buf);
    totalHits += hits;
  }
  benchmark::DoNotOptimize(totalHits);
}

static void BM_Trie_NumDocs_Lookup(benchmark::State &state) {
  if (!ensureInit(state)) return;
  std::mt19937 rng(42);
  std::uniform_int_distribution<size_t> dist(0, gCorpus->words.size() - 1);
  std::vector<size_t> indices(1024);
  for (auto &i : indices) i = dist(rng);

  size_t k = 0;
  size_t acc = 0;
  for (auto _ : state) {
    const std::string &w = gCorpus->words[indices[k++ % indices.size()]];
    runeBuf buf;
    size_t rlen = 0;
    rune *runes = runeBufFill(w.c_str(), w.size(), &buf, &rlen);
    TrieNode *node = Trie_GetNode(gCorpus->terms, runes, rlen, true, NULL);
#if BENCH_TRIE_NODE_OPAQUE
    acc += node ? TrieNode_NumDocs(node) : 0;
#else
    acc += node ? node->numDocs : 0;
#endif
    runeBufFree(&buf);
  }
  benchmark::DoNotOptimize(acc);
}

BENCHMARK(BM_Suffix_IterateContains);
BENCHMARK(BM_Suffix_IterateWildcard);
BENCHMARK(BM_Trie_NumDocs_Lookup);

BENCHMARK_MAIN();
