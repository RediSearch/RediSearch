/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// Candidate flavor: drives the Rust TermSuffixIndex FFI surface exactly the
// way the post-port call sites do (indexer.c, fork_gc/terms.c, query.c).
// A baseline flavor with identical benchmark names lives on the
// memark-bench-suffix-baseline branch and drives the C suffix trie the way
// the pre-port call sites did. Identical names let Google Benchmark's
// compare.py pair the runs.

#include "benchmark/benchmark.h"
#include "redismock/util.h"

#include "trie/trie.h"
#include "trie/rune_util.h"
#include "triemap_ffi.h"
#include "rmalloc.h"

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

int CountingRuneCb(const rune *, size_t, void *ctx, void *, size_t) {
    ++*static_cast<size_t *>(ctx);
    return REDISEARCH_OK;
}

int CountingCharCb(const char *, size_t, void *ctx, void *) {
    ++*static_cast<size_t *>(ctx);
    return REDISEARCH_OK;
}

// Call-site-faithful contains/suffix iteration, mirroring
// Query_EvalPrefixNode: the query string arrives as chars, is lowered to
// runes by the shared prefix-node code, and the suffix block then folds it
// back to UTF-8 for the char-keyed Rust index.
size_t IterateNeedle(TermSuffixIndex *suffix, const char *needle, size_t needleLen,
                     bool contains) {
    size_t nstr;
    rune *str = strToLowerRunes(needle, needleLen, &nstr);
    size_t foldedLen;
    char *folded = runesToStr(str, nstr, &foldedLen);
    size_t hits = 0;
    if (contains) {
        TermSuffixIndex_IterateContains(suffix, folded, foldedLen, CountingCharCb, &hits);
    } else {
        TermSuffixIndex_IterateSuffix(suffix, folded, foldedLen, CountingCharCb, &hits);
    }
    rm_free(folded);
    rm_free(str);
    return hits;
}

// Call-site-faithful wildcard, mirroring Query_EvalWildcardQueryNode after
// the port: the folded pattern is offered to the suffix index first; a NULL
// iterator means no token can anchor the search and the terms trie is
// brute-force scanned instead.
size_t IterateWildcardPattern(TermSuffixIndex *suffix, Trie *terms, const char *pattern,
                              size_t patternLen) {
    size_t nstr;
    rune *str = strToLowerRunes(pattern, patternLen, &nstr);
    size_t hits = 0;
    size_t foldedLen;
    char *folded = runesToStr(str, nstr, &foldedLen);
    TermSuffixIndexIterator *it = TermSuffixIndex_IterateWildcard(suffix, folded, foldedLen);
    if (it) {
        const char *term;
        size_t termLen;
        while (TermSuffixIndexIterator_Next(it, &term, &termLen)) {
            ++hits;
        }
        TermSuffixIndexIterator_Free(it);
    } else {
        Trie_IterateWildcard(terms, str, nstr, CountingRuneCb, &hits, NULL,
                             /*skipTimeoutChecks=*/true);
    }
    rm_free(folded);
    rm_free(str);
    return hits;
}

}  // namespace

class BM_Suffix : public benchmark::Fixture {
public:
    static bool initialized;

    TermSuffixIndex *suffix = nullptr;
    Trie *terms = nullptr;  // wildcard queries scan the terms trie
    std::vector<std::string> corpus;
    size_t insertCounter = 0;

    void SetUp(::benchmark::State &state) {
        if (!initialized) {
            RMCK::init();
            initialized = true;
        }
        const auto numWords = static_cast<size_t>(state.range(0));
        // Same corpus shape and seed as bench_trie so absolute numbers are
        // comparable across the two benches.
        corpus = GenerateWords(numWords, 4, 12, 0xC0FFEE);

        suffix = NewTermSuffixIndex();
        terms = NewTrie(NULL, Trie_Sort_Lex);
        for (const auto &w : corpus) {
            TermSuffixIndex_Add(suffix, w.c_str(), w.size());
            Trie_InsertStringBuffer(terms, w.c_str(), w.size(), 1.0f, 1, NULL, 0);
        }
        insertCounter = 0;
    }

    void TearDown(::benchmark::State &) {
        TermSuffixIndex_Free(suffix);
        suffix = nullptr;
        TrieType_Free(terms);
        terms = nullptr;
        corpus.clear();
    }
};

bool BM_Suffix::initialized = false;

// Iterate benches sweep 1K / 10K / 100K pre-populated terms; write benches
// use one mid-size point (same rationale as bench_trie).
#define SUFFIX_SCENARIOS() RangeMultiplier(10)->Range(1'000, 100'000)
#define SUFFIX_WRITE_SCENARIO() Arg(10'000)

// Add: indexer.c path. Fresh fixed-length words each iteration so every Add
// expands and inserts the full suffix chain instead of no-op'ing on an
// existing term. 'z' prefix + digits stay disjoint from the corpus tail
// distribution closely enough while keeping length constant.
BENCHMARK_DEFINE_F(BM_Suffix, Add)(benchmark::State &state) {
    char buf[24];
    for (auto _ : state) {
        int n = std::snprintf(buf, sizeof(buf), "z%08zu", insertCounter++);
        TermSuffixIndex_Add(suffix, buf, static_cast<size_t>(n));
    }
}

// Remove: fork-GC path. Re-add under PauseTiming so the index stays at
// state.range(0) during measurement.
BENCHMARK_DEFINE_F(BM_Suffix, Remove)(benchmark::State &state) {
    size_t i = 0;
    for (auto _ : state) {
        const auto &w = corpus[i];
        TermSuffixIndex_Remove(suffix, w.c_str(), w.size());
        state.PauseTiming();
        TermSuffixIndex_Add(suffix, w.c_str(), w.size());
        state.ResumeTiming();
        if (++i == corpus.size()) i = 0;
    }
}

// Contains query: *ab*
BENCHMARK_DEFINE_F(BM_Suffix, IterateContains)(benchmark::State &state) {
    size_t hits = 0;
    for (auto _ : state) {
        hits = IterateNeedle(suffix, "ab", 2, /*contains=*/true);
        benchmark::DoNotOptimize(hits);
    }
    state.counters["hits"] = static_cast<double>(hits);
}

// Suffix query: *ab
BENCHMARK_DEFINE_F(BM_Suffix, IterateSuffix)(benchmark::State &state) {
    size_t hits = 0;
    for (auto _ : state) {
        hits = IterateNeedle(suffix, "ab", 2, /*contains=*/false);
        benchmark::DoNotOptimize(hits);
    }
    state.counters["hits"] = static_cast<double>(hits);
}

// Wildcard with a usable token ("cd", len >= MIN_SUFFIX): the
// suffix-assisted path on both flavors.
BENCHMARK_DEFINE_F(BM_Suffix, WildcardTokenized)(benchmark::State &state) {
    size_t hits = 0;
    for (auto _ : state) {
        hits = IterateWildcardPattern(suffix, terms, "ab*cd", 5);
        benchmark::DoNotOptimize(hits);
    }
    state.counters["hits"] = static_cast<double>(hits);
}

// Wildcard whose tokens are all shorter than MIN_SUFFIX: both flavors
// brute-force the terms trie, so this pair should compare flat. Sanity
// anchor for the harness.
BENCHMARK_DEFINE_F(BM_Suffix, WildcardFallback)(benchmark::State &state) {
    size_t hits = 0;
    for (auto _ : state) {
        hits = IterateWildcardPattern(suffix, terms, "a*z", 3);
        benchmark::DoNotOptimize(hits);
    }
    state.counters["hits"] = static_cast<double>(hits);
}

BENCHMARK_REGISTER_F(BM_Suffix, Add)->SUFFIX_WRITE_SCENARIO();
BENCHMARK_REGISTER_F(BM_Suffix, Remove)->SUFFIX_WRITE_SCENARIO();
BENCHMARK_REGISTER_F(BM_Suffix, IterateContains)->SUFFIX_SCENARIOS();
BENCHMARK_REGISTER_F(BM_Suffix, IterateSuffix)->SUFFIX_SCENARIOS();
BENCHMARK_REGISTER_F(BM_Suffix, WildcardTokenized)->SUFFIX_SCENARIOS();
BENCHMARK_REGISTER_F(BM_Suffix, WildcardFallback)->SUFFIX_SCENARIOS();

BENCHMARK_MAIN();
