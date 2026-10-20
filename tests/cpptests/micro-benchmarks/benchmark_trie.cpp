/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "benchmark/benchmark.h"
#include "redismock/util.h"
#include "trie/trie.h"

#include <array>
#include <cstdint>
#include <random>
#include <set>
#include <string>
#include <vector>

namespace {

constexpr rune kPrefix = 0x41;
constexpr rune kChildBase = 0x20;
constexpr size_t kBatchSize = 64;

using TrieKey = std::array<rune, 2>;

TrieKey MakeKey(size_t child) {
  return {kPrefix, static_cast<rune>(kChildBase + child)};
}

std::vector<TrieKey> MakeKeys(size_t childCount) {
  std::vector<TrieKey> keys;
  keys.reserve(childCount);
  for (size_t i = 0; i < childCount; ++i) {
    keys.push_back(MakeKey(i));
  }
  return keys;
}

Trie *MakeWideTrie(size_t childCount) {
  Trie *trie = NewTrie(nullptr, Trie_Sort_Score);
  Trie_InsertRune(trie, &kPrefix, 1, 1.0, 0, nullptr, 0);

  for (size_t i = 0; i < childCount; ++i) {
    TrieKey key = MakeKey(i);
    Trie_InsertRune(trie, key.data(), key.size(), static_cast<double>(i + 1), 0, nullptr, 0);
  }

  return trie;
}

void FreeTries(std::vector<Trie *> &tries) {
  for (Trie *trie : tries) {
    TrieType_Free(trie);
  }
  tries.clear();
}

void BM_TrieLookupWideNode(benchmark::State &state) {
  RMCK::init();
  const size_t childCount = static_cast<size_t>(state.range(0));
  const std::vector<TrieKey> keys = MakeKeys(childCount);

  Trie *trie = MakeWideTrie(childCount);

  size_t idx = 0;
  for (auto _ : state) {
    TrieNode *node = Trie_GetNode(trie, keys[idx].data(), keys[idx].size(), true, nullptr);
    benchmark::DoNotOptimize(node);
    idx = (idx + 1) % childCount;
  }

  TrieType_Free(trie);
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
}

void BM_TrieDeleteWideNode(benchmark::State &state) {
  RMCK::init();
  const size_t childCount = static_cast<size_t>(state.range(0));
  const std::vector<TrieKey> keys = MakeKeys(childCount);
  std::vector<Trie *> tries;
  tries.reserve(kBatchSize);

  size_t idx = 0;
  for (auto _ : state) {
    state.PauseTiming();
    for (size_t i = 0; i < kBatchSize; ++i) {
      tries.push_back(MakeWideTrie(childCount));
    }
    state.ResumeTiming();

    for (Trie *trie : tries) {
      int deleted = Trie_DeleteRunes(trie, keys[idx].data(), keys[idx].size());
      benchmark::DoNotOptimize(deleted);
      idx = (idx + 1) % childCount;
    }

    state.PauseTiming();
    FreeTries(tries);
    state.ResumeTiming();
  }

  state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * kBatchSize));
}

void BM_TrieInsertWideNode(benchmark::State &state) {
  RMCK::init();
  const size_t childCount = static_cast<size_t>(state.range(0));
  std::vector<Trie *> tries;
  tries.reserve(kBatchSize);

  for (auto _ : state) {
    state.PauseTiming();
    for (size_t i = 0; i < kBatchSize; ++i) {
      tries.push_back(MakeWideTrie(childCount));
    }
    state.ResumeTiming();

    for (size_t i = 0; i < tries.size(); ++i) {
      TrieKey key = MakeKey(childCount + i);
      int inserted = Trie_InsertRune(tries[i], key.data(), key.size(), static_cast<double>(i + 1),
                                     0, nullptr, 0);
      benchmark::DoNotOptimize(inserted);
    }

    state.PauseTiming();
    FreeTries(tries);
    state.ResumeTiming();
  }

  state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * kBatchSize));
}

void AppendUtf8(std::string &out, uint32_t cp) {
  if (cp < 0x80) {
    out.push_back(static_cast<char>(cp));
  } else if (cp < 0x800) {
    out.push_back(static_cast<char>(0xC0 | (cp >> 6)));
    out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
  } else {
    out.push_back(static_cast<char>(0xE0 | (cp >> 12)));
    out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
    out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
  }
}

// Distinct random words in lexicographic order, shaped like an index's terms trie
// over web text: thousands of distinct first characters make the root very wide.
// Deleted in the order the fork GC walks the trie.
std::vector<std::string> MakeVocabulary(size_t count) {
  constexpr uint32_t kFirstRuneBase = 0x100;
  constexpr uint32_t kFirstRuneSpan = 0x3000;  // stays below the surrogate range
  std::mt19937 rng(42);  // NOSONAR: fixed-seed benchmark input, not a security context
  std::set<std::string> words;
  while (words.size() < count) {
    std::string word;
    AppendUtf8(word, kFirstRuneBase + rng() % kFirstRuneSpan);
    for (size_t len = 2 + rng() % 8; len > 0; len--) {
      word.push_back(static_cast<char>('a' + rng() % 26));
    }
    words.insert(word);
  }
  return {words.begin(), words.end()};
}

void BM_TrieDeleteLexVocabulary(benchmark::State &state) {
  RMCK::init();
  const std::vector<std::string> words = MakeVocabulary(static_cast<size_t>(state.range(0)));

  for (auto _ : state) {
    state.PauseTiming();
    Trie *trie = NewTrie(nullptr, Trie_Sort_Lex);
    for (const std::string &word : words) {
      Trie_InsertStringBuffer(trie, word.data(), word.size(), 1, 1, nullptr, 1);
    }
    state.ResumeTiming();

    for (const std::string &word : words) {
      benchmark::DoNotOptimize(Trie_Delete(trie, word.data(), word.size()));
    }

    state.PauseTiming();
    TrieType_Free(trie);
    state.ResumeTiming();
  }

  state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * words.size()));
}

}  // namespace

BENCHMARK(BM_TrieLookupWideNode)
    ->Name("LookupWideNode")
    ->ArgName("children")
    ->Arg(16)
    ->Arg(64)
    ->Arg(128);
BENCHMARK(BM_TrieDeleteWideNode)
    ->Name("DeleteWideNode")
    ->ArgName("children")
    ->Arg(16)
    ->Arg(64)
    ->Arg(128);
BENCHMARK(BM_TrieInsertWideNode)
    ->Name("InsertWideNode")
    ->ArgName("children")
    ->Arg(16)
    ->Arg(64)
    ->Arg(128);

BENCHMARK(BM_TrieDeleteLexVocabulary)
    ->Name("DeleteLexVocabulary")
    ->ArgName("words")
    ->Arg(100000)
    ->Arg(1000000)
    ->Unit(benchmark::kMillisecond);

BENCHMARK_MAIN();
