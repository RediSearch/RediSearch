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
#include "util/strconv.h"
#include "rmalloc.h"

#include <cstring>
#include <random>
#include <string>
#include <vector>

// Token corpora for the unicode_tolower benchmark.
//
// MS MARCO and other English document/query workloads feed the tokenizer with
// short, mixed-case ASCII tokens. We synthesize equivalents here (deterministic
// PRNG) so the benchmark is self-contained and reproducible:
//   - ascii:   ~7-byte uppercase/mixed-case ASCII words ([A-Za-z]+)
//   - cyrillic: ~12-byte Cyrillic words (each codepoint 2 bytes UTF-8)
//   - mixed:    ASCII tokens with one Latin-1 accented codepoint mid-token
//
// The bench measures the per-token cost of unicode_tolower in a tight loop,
// re-copying the input each iteration since unicode_tolower mutates in place.

namespace {

constexpr size_t kNumTokens = 4096;

std::vector<std::string> MakeAsciiCorpus(uint32_t seed) {
  std::mt19937 rng(seed);
  std::uniform_int_distribution<int> len_dist(3, 12);
  std::uniform_int_distribution<int> case_dist(0, 1);
  std::uniform_int_distribution<int> letter_dist(0, 25);
  std::vector<std::string> out;
  out.reserve(kNumTokens);
  for (size_t i = 0; i < kNumTokens; ++i) {
    int n = len_dist(rng);
    std::string s;
    s.reserve(n);
    for (int j = 0; j < n; ++j) {
      char base = case_dist(rng) ? 'A' : 'a';
      s.push_back(base + letter_dist(rng));
    }
    out.emplace_back(std::move(s));
  }
  return out;
}

std::vector<std::string> MakeCyrillicCorpus(uint32_t seed) {
  std::mt19937 rng(seed);
  std::uniform_int_distribution<int> len_dist(3, 12);
  // Cyrillic uppercase range U+0410..U+042F (32 letters).
  std::uniform_int_distribution<uint32_t> cp_dist(0x0410, 0x042F);
  std::vector<std::string> out;
  out.reserve(kNumTokens);
  for (size_t i = 0; i < kNumTokens; ++i) {
    int n = len_dist(rng);
    std::string s;
    s.reserve(n * 2);
    for (int j = 0; j < n; ++j) {
      uint32_t cp = cp_dist(rng);
      // Encode 2-byte UTF-8: 110xxxxx 10xxxxxx
      s.push_back(static_cast<char>(0xC0 | (cp >> 6)));
      s.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
    }
    out.emplace_back(std::move(s));
  }
  return out;
}

std::vector<std::string> MakeMixedCorpus(uint32_t seed) {
  // ASCII token with a single Latin-1 accented codepoint (e.g. U+00C9 É)
  // inserted at a random position. Models the realistic "mostly-ASCII corpus
  // with occasional accented word" pattern.
  std::mt19937 rng(seed);
  std::uniform_int_distribution<int> len_dist(3, 12);
  std::uniform_int_distribution<int> case_dist(0, 1);
  std::uniform_int_distribution<int> letter_dist(0, 25);
  // U+00C0..U+00DE without U+00D7 (Latin-1 uppercase block).
  std::uniform_int_distribution<uint32_t> cp_dist(0x00C0, 0x00DE);
  std::vector<std::string> out;
  out.reserve(kNumTokens);
  for (size_t i = 0; i < kNumTokens; ++i) {
    int n = len_dist(rng);
    std::string s;
    s.reserve(n + 2);
    int accent_pos = std::uniform_int_distribution<int>(0, n - 1)(rng);
    for (int j = 0; j < n; ++j) {
      if (j == accent_pos) {
        uint32_t cp = cp_dist(rng);
        if (cp == 0x00D7) cp = 0x00C0; // skip multiplication sign
        s.push_back(static_cast<char>(0xC0 | (cp >> 6)));
        s.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
      } else {
        char base = case_dist(rng) ? 'A' : 'a';
        s.push_back(base + letter_dist(rng));
      }
    }
    out.emplace_back(std::move(s));
  }
  return out;
}

enum class Corpus { kAscii, kCyrillic, kMixed };

const std::vector<std::string>& GetCorpus(Corpus c) {
  static const std::vector<std::string> ascii = MakeAsciiCorpus(0xA5C11A11);
  static const std::vector<std::string> cyr = MakeCyrillicCorpus(0xC7B1110C);
  static const std::vector<std::string> mix = MakeMixedCorpus(0x111E4ED1);
  switch (c) {
    case Corpus::kAscii: return ascii;
    case Corpus::kCyrillic: return cyr;
    case Corpus::kMixed: return mix;
  }
  return ascii;
}

void RunCorpus(benchmark::State& state, Corpus c) {
  RMCK::init();
  const auto& tokens = GetCorpus(c);
  // Compute total bytes processed per iteration for throughput reporting.
  size_t total_bytes = 0;
  for (const auto& t : tokens) total_bytes += t.size();

  // Pre-allocate a working buffer large enough for the longest token plus one
  // (unicode_tolower may write up to the same byte length on its in-place path).
  size_t max_len = 0;
  for (const auto& t : tokens) max_len = std::max(max_len, t.size());
  std::vector<char> buf(max_len + 1);

  for (auto _ : state) {
    for (const auto& t : tokens) {
      std::memcpy(buf.data(), t.data(), t.size());
      size_t len = t.size();
      char* longer = unicode_tolower(buf.data(), &len);
      if (longer) {
        benchmark::DoNotOptimize(longer);
        rm_free(longer);
      } else {
        benchmark::DoNotOptimize(buf.data());
      }
    }
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * total_bytes);
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * tokens.size());
}

void BM_UnicodeToLower_Ascii(benchmark::State& state) { RunCorpus(state, Corpus::kAscii); }
void BM_UnicodeToLower_Cyrillic(benchmark::State& state) { RunCorpus(state, Corpus::kCyrillic); }
void BM_UnicodeToLower_Mixed(benchmark::State& state) { RunCorpus(state, Corpus::kMixed); }

}  // namespace

BENCHMARK(BM_UnicodeToLower_Ascii)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_UnicodeToLower_Cyrillic)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_UnicodeToLower_Mixed)->Unit(benchmark::kMicrosecond);

BENCHMARK_MAIN();
