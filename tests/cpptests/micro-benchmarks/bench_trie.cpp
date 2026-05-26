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

#include <cstdio>
#include <random>
#include <string>
#include <vector>

namespace {

std::vector<std::string> GenerateWords(size_t count, size_t minLen, size_t maxLen,
                                       uint32_t seed) {
    std::mt19937 rng(seed);
    std::uniform_int_distribution<size_t> lenDist(minLen, maxLen);
    std::uniform_int_distribution<int> charDist('a', 'z');
    std::vector<std::string> out;
    out.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        size_t len = lenDist(rng);
        std::string s;
        s.reserve(len);
        for (size_t j = 0; j < len; ++j) {
            s.push_back(static_cast<char>(charDist(rng)));
        }
        out.push_back(std::move(s));
    }
    return out;
}

int CountingRangeCb(const rune *, size_t, void *ctx, void *, size_t) {
    ++*static_cast<size_t *>(ctx);
    return REDISEARCH_OK;
}

void *triePayload(Trie *t, const char *s, size_t len, bool exact) {
    if (len > TRIE_INITIAL_STRING_LEN * sizeof(rune)) {
        return nullptr;
    }
    runeBuf buf;
    rune *runes = runeBufFill(s, len, &buf, &len);
    TrieNode *node = Trie_GetNode(t, runes, len, exact, NULL);
    runeBufFree(&buf);
    return (node && node->payload) ? node->payload->data : nullptr;
}

}  // namespace

template <TrieSortMode SortMode>
class BM_Trie : public benchmark::Fixture {
public:
    static bool initialized;

    Trie *trie = nullptr;
    std::vector<std::string> corpus;
    std::vector<std::string> misses;
    size_t insertCounter = 0;

    void SetUp(::benchmark::State &state) {
        if (!initialized) {
            RMCK::init();
            initialized = true;
        }
        const auto numWords = static_cast<size_t>(state.range(0));
        // Corpus: words inserted into the trie at startup. Seeded for reproducibility.
        corpus = GenerateWords(numWords, 4, 12, 0xC0FFEE);
        // Disjoint probe pool used to measure find-misses. Different seed → different words.
        misses = GenerateWords(8'192, 4, 12, 0xBADCAFE);

        trie = NewTrie(NULL, SortMode);
        for (const auto &w : corpus) {
            Trie_InsertStringBuffer(trie, w.c_str(), w.size(), 1.0f, 1, NULL, 0);
        }
        insertCounter = 0;
    }

    void TearDown(::benchmark::State &) {
        TrieType_Free(trie);
        trie = nullptr;
        corpus.clear();
        misses.clear();
    }
};

template <TrieSortMode SortMode>
bool BM_Trie<SortMode>::initialized = false;

// Pre-population sizes: 1K (small autocomplete dict), 10K (suggestion-list scale),
// 100K (large term dictionary). Same shape as the Rust trie_bencher corpora.
#define TRIE_SCENARIOS() RangeMultiplier(10)->Range(1'000, 100'000)

// Insert: synthesize fresh strings each iteration so we always hit the
// "new node" path instead of degenerating into score-bumping on existing keys.
// Uppercase 'Z' prefix keeps inserts disjoint from the lowercase corpus.
BENCHMARK_TEMPLATE1_DEFINE_F(BM_Trie, InsertLex, Trie_Sort_Lex)(benchmark::State &state) {
    char buf[24];
    for (auto _ : state) {
        int n = std::snprintf(buf, sizeof(buf), "Z%zu", insertCounter++);
        Trie_InsertStringBuffer(trie, buf, static_cast<size_t>(n), 1.0f, 1, NULL, 0);
    }
}

BENCHMARK_TEMPLATE1_DEFINE_F(BM_Trie, InsertScore, Trie_Sort_Score)(benchmark::State &state) {
    char buf[24];
    for (auto _ : state) {
        int n = std::snprintf(buf, sizeof(buf), "Z%zu", insertCounter++);
        Trie_InsertStringBuffer(trie, buf, static_cast<size_t>(n), 1.0f, 1, NULL, 0);
    }
}

BENCHMARK_TEMPLATE1_DEFINE_F(BM_Trie, FindHit, Trie_Sort_Lex)(benchmark::State &state) {
    size_t i = 0;
    for (auto _ : state) {
        const auto &w = corpus[i];
        benchmark::DoNotOptimize(
            triePayload(trie, w.c_str(), w.size(), true));
        if (++i == corpus.size()) i = 0;
    }
}

BENCHMARK_TEMPLATE1_DEFINE_F(BM_Trie, FindMiss, Trie_Sort_Lex)(benchmark::State &state) {
    size_t i = 0;
    for (auto _ : state) {
        const auto &w = misses[i];
        benchmark::DoNotOptimize(
            triePayload(trie, w.c_str(), w.size(), true));
        if (++i == misses.size()) i = 0;
    }
}

BENCHMARK_TEMPLATE1_DEFINE_F(BM_Trie, RangeAll, Trie_Sort_Lex)(benchmark::State &state) {
    for (auto _ : state) {
        size_t hits = 0;
        Trie_IterateRange(trie, NULL, -1, true, NULL, -1, false,
                          CountingRangeCb, &hits);
        benchmark::DoNotOptimize(hits);
    }
}

BENCHMARK_TEMPLATE1_DEFINE_F(BM_Trie, RangeNarrow, Trie_Sort_Lex)(benchmark::State &state) {
    rune lo[8] = {0};
    rune hi[8] = {0};
    size_t loN = strToRunesN("aa", 2, lo);
    size_t hiN = strToRunesN("ab", 2, hi);
    for (auto _ : state) {
        size_t hits = 0;
        Trie_IterateRange(trie, lo, loN, true, hi, hiN, false,
                          CountingRangeCb, &hits);
        benchmark::DoNotOptimize(hits);
    }
}

BENCHMARK_TEMPLATE1_DEFINE_F(BM_Trie, ContainsPrefix, Trie_Sort_Lex)(benchmark::State &state) {
    rune p[8] = {0};
    size_t pN = strToRunesN("ab", 2, p);
    for (auto _ : state) {
        size_t hits = 0;
        Trie_IterateContains(trie, p, pN, /*prefix=*/true, /*suffix=*/false,
                             CountingRangeCb, &hits, NULL, /*skipTimeoutChecks=*/true);
        benchmark::DoNotOptimize(hits);
    }
}

BENCHMARK_TEMPLATE1_DEFINE_F(BM_Trie, ContainsSuffix, Trie_Sort_Lex)(benchmark::State &state) {
    rune s[8] = {0};
    size_t sN = strToRunesN("ab", 2, s);
    for (auto _ : state) {
        size_t hits = 0;
        Trie_IterateContains(trie, s, sN, /*prefix=*/false, /*suffix=*/true,
                             CountingRangeCb, &hits, NULL, /*skipTimeoutChecks=*/true);
        benchmark::DoNotOptimize(hits);
    }
}

BENCHMARK_TEMPLATE1_DEFINE_F(BM_Trie, Wildcard, Trie_Sort_Lex)(benchmark::State &state) {
    rune w[16] = {0};
    size_t wN = strToRunesN("a*z", 3, w);
    for (auto _ : state) {
        size_t hits = 0;
        Trie_IterateWildcard(trie, w, wN, CountingRangeCb, &hits, NULL,
                             /*skipTimeoutChecks=*/true);
        benchmark::DoNotOptimize(hits);
    }
}

BENCHMARK_TEMPLATE1_DEFINE_F(BM_Trie, IterateAll, Trie_Sort_Lex)(benchmark::State &state) {
    for (auto _ : state) {
        TrieIterator *it = Trie_Iterate(trie, "", 0, 0, /*prefixMode=*/1);
        rune *rstr;
        t_len rlen;
        float score;
        RSPayload payload;
        size_t cnt = 0;
        while (TrieIterator_Next(it, &rstr, &rlen, &payload, &score, NULL, NULL)) {
            ++cnt;
        }
        benchmark::DoNotOptimize(cnt);
        TrieIterator_Free(it);
    }
}

BENCHMARK_TEMPLATE1_DEFINE_F(BM_Trie, FuzzyDist1, Trie_Sort_Score)(benchmark::State &state) {
    size_t i = 0;
    for (auto _ : state) {
        const auto &w = corpus[i];
        TrieIterator *it = Trie_Iterate(trie, w.c_str(), w.size(), /*maxDist=*/1,
                                        /*prefixMode=*/0);
        rune *rstr;
        t_len rlen;
        float score;
        RSPayload payload;
        size_t cnt = 0;
        while (TrieIterator_Next(it, &rstr, &rlen, &payload, &score, NULL, NULL)) {
            ++cnt;
        }
        benchmark::DoNotOptimize(cnt);
        TrieIterator_Free(it);
        if (++i == corpus.size()) i = 0;
    }
}

// Fuzzy at edit-distance 2: DFA size and branching grow non-linearly with maxDist,
// so refactors to __ti_step or the DFAFilter matchCtx tend to show up here, not at dist=1.
BENCHMARK_TEMPLATE1_DEFINE_F(BM_Trie, FuzzyDist2, Trie_Sort_Score)(benchmark::State &state) {
    size_t i = 0;
    for (auto _ : state) {
        const auto &w = corpus[i];
        TrieIterator *it = Trie_Iterate(trie, w.c_str(), w.size(), /*maxDist=*/2,
                                        /*prefixMode=*/0);
        rune *rstr;
        t_len rlen;
        float score;
        RSPayload payload;
        size_t cnt = 0;
        while (TrieIterator_Next(it, &rstr, &rlen, &payload, &score, NULL, NULL)) {
            ++cnt;
        }
        benchmark::DoNotOptimize(cnt);
        TrieIterator_Free(it);
        if (++i == corpus.size()) i = 0;
    }
}

// Delete: walks the same recursion as __trieNode_Add and exercises the
// child-collapse path. Re-insert under PauseTiming so the trie stays at
// state.range(0) during measurement. Pause/resume costs a few hundred ns, but
// Delete itself walks O(word_length) nodes and is microsecond-scale at these
// corpus sizes, so signal dominates.
BENCHMARK_TEMPLATE1_DEFINE_F(BM_Trie, DeleteHit, Trie_Sort_Lex)(benchmark::State &state) {
    size_t i = 0;
    for (auto _ : state) {
        const auto &w = corpus[i];
        Trie_Delete(trie, w.c_str(), w.size());
        state.PauseTiming();
        Trie_InsertStringBuffer(trie, w.c_str(), w.size(), 1.0f, 1, NULL, 0);
        state.ResumeTiming();
        if (++i == corpus.size()) i = 0;
    }
}

// Insert with incr=1 on an existing key: hits the score-bump branch inside
// __trieNode_Add and the maxChildScore bubble-up. The InsertLex/InsertScore
// benches above synthesize fresh 'Z'-prefixed keys, which only exercises the
// "new node" path.
BENCHMARK_TEMPLATE1_DEFINE_F(BM_Trie, InsertIncr, Trie_Sort_Score)(benchmark::State &state) {
    size_t i = 0;
    for (auto _ : state) {
        const auto &w = corpus[i];
        Trie_InsertStringBuffer(trie, w.c_str(), w.size(), 1.0f, /*incr=*/1, NULL, 0);
        if (++i == corpus.size()) i = 0;
    }
}

BENCHMARK_REGISTER_F(BM_Trie, InsertLex)->TRIE_SCENARIOS();
BENCHMARK_REGISTER_F(BM_Trie, InsertScore)->TRIE_SCENARIOS();
BENCHMARK_REGISTER_F(BM_Trie, InsertIncr)->TRIE_SCENARIOS();
BENCHMARK_REGISTER_F(BM_Trie, DeleteHit)->TRIE_SCENARIOS();
BENCHMARK_REGISTER_F(BM_Trie, FindHit)->TRIE_SCENARIOS();
BENCHMARK_REGISTER_F(BM_Trie, FindMiss)->TRIE_SCENARIOS();
BENCHMARK_REGISTER_F(BM_Trie, RangeAll)->TRIE_SCENARIOS();
BENCHMARK_REGISTER_F(BM_Trie, RangeNarrow)->TRIE_SCENARIOS();
BENCHMARK_REGISTER_F(BM_Trie, ContainsPrefix)->TRIE_SCENARIOS();
BENCHMARK_REGISTER_F(BM_Trie, ContainsSuffix)->TRIE_SCENARIOS();
BENCHMARK_REGISTER_F(BM_Trie, Wildcard)->TRIE_SCENARIOS();
BENCHMARK_REGISTER_F(BM_Trie, IterateAll)->TRIE_SCENARIOS();
BENCHMARK_REGISTER_F(BM_Trie, FuzzyDist1)->TRIE_SCENARIOS();
BENCHMARK_REGISTER_F(BM_Trie, FuzzyDist2)->TRIE_SCENARIOS();

BENCHMARK_MAIN();
