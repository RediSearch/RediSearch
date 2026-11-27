/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "filter_quick_check.h"
#include "gtest/gtest.h"

#include <array>
#include <cmath>
#include <cstring>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace {
using Check = std::unique_ptr<FilterQuickCheck, decltype(&FilterQuickCheck_Free)>;

Check compile(const FilterQuickCheckPredicate& predicate) {
  return Check(FilterQuickCheck_Compile(&predicate), FilterQuickCheck_Free);
}

FilterQuickCheckPredicate term(std::string_view value, uint16_t field = 1,
                               FilterQuickCheckTermKind kind = FilterQuickCheck_TAG) {
  FilterQuickCheckPredicate result = {};
  result.type = FilterQuickCheck_TERM;
  result.term = {kind, field, value.data(), value.size()};
  return result;
}

template <size_t N>
FilterQuickCheckPredicate term(const char (&value)[N], uint16_t field = 1,
                               FilterQuickCheckTermKind kind = FilterQuickCheck_TAG) {
  return term(std::string_view(value, N - 1), field, kind);
}

FilterQuickCheckPredicate compound(FilterQuickCheckPredicateType type,
                                   const FilterQuickCheckPredicate* children, size_t len) {
  FilterQuickCheckPredicate result = {};
  result.type = type;
  result.operands = {children, len};
  return result;
}

FilterQuickCheckSignature signature(std::initializer_list<FilterQuickCheckPredicate> terms) {
  FilterQuickCheckSignature result = {};
  for (const auto& t : terms) {
    FilterQuickCheckSignature_Add(&result, &t.term);
  }
  return result;
}

struct Provider {
  std::unordered_map<uint64_t, FilterQuickCheckSignature> documents;
  std::vector<uint64_t> requests;

  static bool lookup(void* opaque, uint64_t id, FilterQuickCheckSignature* out) {
    auto& self = *static_cast<Provider*>(opaque);
    self.requests.push_back(id);
    auto found = self.documents.find(id);
    if (found == self.documents.end()) {
      return false;
    }
    *out = found->second;
    return true;
  }
};

FilterQuickCheckPredicate numeric(double min, double max, bool minInclusive = true,
                                  bool maxInclusive = true, uint16_t field = 2) {
  FilterQuickCheckPredicate result = {};
  result.type = FilterQuickCheck_NUMERIC;
  result.numeric = {field, min, max, minInclusive, maxInclusive};
  return result;
}

FilterQuickCheckNumericSummary numericSummary(std::initializer_list<double> values) {
  FilterQuickCheckNumericSummary result;
  FilterQuickCheckNumericSummary_Init(&result);
  for (double value : values) {
    FilterQuickCheckNumericSummary_Add(&result, value);
  }
  return result;
}

struct NumericProvider {
  std::unordered_map<uint64_t, std::unordered_map<uint16_t, FilterQuickCheckNumericSummary>>
      documents;
  std::vector<std::pair<uint64_t, uint16_t>> requests;

  static bool lookup(void* opaque, uint64_t id, uint16_t field,
                     FilterQuickCheckNumericSummary* out) {
    auto& self = *static_cast<NumericProvider*>(opaque);
    self.requests.emplace_back(id, field);
    auto doc = self.documents.find(id);
    if (doc == self.documents.end()) {
      return false;
    }
    auto summary = doc->second.find(field);
    if (summary == doc->second.end()) {
      return false;
    }
    *out = summary->second;
    return true;
  }
};
}  // namespace

TEST(FilterQuickCheckTest, rejectsMissingTermButDoesNotCertifyPresence) {
  auto predicate = term("electronics");
  auto check = compile(predicate);
  auto present = signature({predicate});
  FilterQuickCheckSignature empty = {};
  EXPECT_EQ(FilterQuickCheck_Evaluate(check.get(), &present), FilterQuickCheck_MAYBE);
  EXPECT_EQ(FilterQuickCheck_Evaluate(check.get(), &empty), FilterQuickCheck_NO);

  FilterQuickCheckSignature saturated;
  memset(&saturated, 0xff, sizeof(saturated));
  EXPECT_EQ(FilterQuickCheck_Evaluate(check.get(), &saturated), FilterQuickCheck_MAYBE);
}

TEST(FilterQuickCheckTest, separatesFieldsAndTermKinds) {
  auto document = signature({term("redis", 1, FilterQuickCheck_TAG)});
  auto otherField = compile(term("redis", 2, FilterQuickCheck_TAG));
  auto otherKind = compile(term("redis", 1, FilterQuickCheck_TEXT));
  EXPECT_EQ(FilterQuickCheck_Evaluate(otherField.get(), &document), FilterQuickCheck_NO);
  EXPECT_EQ(FilterQuickCheck_Evaluate(otherKind.get(), &document), FilterQuickCheck_NO);
}

TEST(FilterQuickCheckTest, supportsEmptyAndLengthDelimitedIndexedTerms) {
  auto emptyTerm = term("");
  auto emptyCheck = compile(emptyTerm);
  auto document = signature({emptyTerm});
  EXPECT_EQ(FilterQuickCheck_Evaluate(emptyCheck.get(), &document), FilterQuickCheck_MAYBE);

  const char binary[] = {'a', '\0', 'b'};
  auto binaryTerm = term("unused");
  binaryTerm.term.value = binary;
  binaryTerm.term.len = sizeof(binary);
  auto binaryCheck = compile(binaryTerm);
  document = signature({binaryTerm});
  EXPECT_EQ(FilterQuickCheck_Evaluate(binaryCheck.get(), &document), FilterQuickCheck_MAYBE);
  auto prefixCheck = compile(term("a"));
  EXPECT_EQ(FilterQuickCheck_Evaluate(prefixCheck.get(), &document), FilterQuickCheck_NO);
}

TEST(FilterQuickCheckTest, compiledPlanOwnsItsMasksAndStructure) {
  char value[] = "hello";
  auto original = term(value);
  auto check = compile(original);
  value[0] = 'j';
  original = {};
  auto document = signature({term("hello")});
  EXPECT_EQ(FilterQuickCheck_Evaluate(check.get(), &document), FilterQuickCheck_MAYBE);
}

TEST(FilterQuickCheckTest, unknownAndNegatedConditionsRemainConservative) {
  FilterQuickCheckSignature empty = {};
  auto negative = term("absent");
  FilterQuickCheckPredicate unknown = {};
  std::array<FilterQuickCheckPredicate, 2> children = {negative, unknown};
  auto conjunction = compile(compound(FilterQuickCheck_AND, children.data(), children.size()));
  auto disjunction = compile(compound(FilterQuickCheck_OR, children.data(), children.size()));
  EXPECT_EQ(FilterQuickCheck_Evaluate(conjunction.get(), &empty), FilterQuickCheck_NO);
  EXPECT_EQ(FilterQuickCheck_Evaluate(disjunction.get(), &empty), FilterQuickCheck_MAYBE);

  auto negation = compile(compound(FilterQuickCheck_NOT, &negative, 1));
  auto matching = signature({negative});
  EXPECT_EQ(FilterQuickCheck_Evaluate(negation.get(), &empty), FilterQuickCheck_MAYBE);
  EXPECT_EQ(FilterQuickCheck_Evaluate(negation.get(), &matching), FilterQuickCheck_MAYBE);

  auto nestedNot = compound(FilterQuickCheck_NOT, &negative, 1);
  auto doubleNot = compile(compound(FilterQuickCheck_NOT, &nestedNot, 1));
  EXPECT_EQ(FilterQuickCheck_Evaluate(doubleNot.get(), &empty), FilterQuickCheck_MAYBE);
}

TEST(FilterQuickCheckTest, respectsNestedBooleanExpressionsForAllDocumentSubsets) {
  // Exhaust the exact truth table for (a OR b) AND c, across different term hashes.
  for (size_t seed = 0; seed < 100; ++seed) {
    std::string a = "a" + std::to_string(seed);
    std::string b = "b" + std::to_string(seed);
    std::string c = "c" + std::to_string(seed);
    std::array<FilterQuickCheckPredicate, 3> atoms = {term(a, 1), term(b, 2),
                                                      term(c, 3, FilterQuickCheck_TEXT)};
    std::array<FilterQuickCheckPredicate, 2> children = {
        compound(FilterQuickCheck_OR, atoms.data(), 2), atoms[2]};
    auto check = compile(compound(FilterQuickCheck_AND, children.data(), children.size()));
    for (unsigned subset = 0; subset < 8; ++subset) {
      FilterQuickCheckSignature document = {};
      for (unsigned bit = 0; bit < atoms.size(); ++bit) {
        if (subset & (1U << bit)) {
          FilterQuickCheckSignature_Add(&document, &atoms[bit].term);
        }
      }
      bool exact = (subset & 3) && (subset & 4);
      if (exact) {
        EXPECT_EQ(FilterQuickCheck_Evaluate(check.get(), &document), FilterQuickCheck_MAYBE)
            << "seed=" << seed << " subset=" << subset;
      }
      if (subset == 0) {
        EXPECT_EQ(FilterQuickCheck_Evaluate(check.get(), &document), FilterQuickCheck_NO);
      }
    }
  }
}

TEST(FilterQuickCheckTest, emptyBooleanOperandsHaveTheirLogicalIdentities) {
  FilterQuickCheckSignature empty = {};
  auto all = compile(compound(FilterQuickCheck_AND, nullptr, 0));
  auto any = compile(compound(FilterQuickCheck_OR, nullptr, 0));
  EXPECT_EQ(FilterQuickCheck_Evaluate(all.get(), &empty), FilterQuickCheck_MAYBE);
  EXPECT_EQ(FilterQuickCheck_Evaluate(any.get(), &empty), FilterQuickCheck_NO);
}

TEST(FilterQuickCheckTest, invalidAndMissingInputsCannotReject) {
  auto valid = compile(term("x"));
  FilterQuickCheckSignature empty = {};
  EXPECT_EQ(FilterQuickCheck_Evaluate(nullptr, &empty), FilterQuickCheck_MAYBE);
  EXPECT_EQ(FilterQuickCheck_Evaluate(valid.get(), nullptr), FilterQuickCheck_MAYBE);
  Check nullPlan(FilterQuickCheck_Compile(nullptr), FilterQuickCheck_Free);
  EXPECT_EQ(FilterQuickCheck_Evaluate(nullPlan.get(), &empty), FilterQuickCheck_MAYBE);
  auto invalid = term("x");
  invalid.term.value = nullptr;
  auto invalidPlan = compile(invalid);
  EXPECT_EQ(FilterQuickCheck_Evaluate(invalidPlan.get(), &empty), FilterQuickCheck_MAYBE);
  FilterQuickCheckSignature_Add(&empty, &invalid.term);
  EXPECT_EQ(FilterQuickCheck_Evaluate(valid.get(), &empty), FilterQuickCheck_MAYBE);
  FilterQuickCheckSignature_Add(nullptr, &invalid.term);
  FilterQuickCheck_Free(nullptr);

  auto malformed = compile(compound(FilterQuickCheck_AND, nullptr, 1));
  EXPECT_EQ(FilterQuickCheck_Evaluate(malformed.get(), &empty), FilterQuickCheck_MAYBE);
  EXPECT_EQ(FilterQuickCheck_MayMatch(nullptr, 1), 1);
  FilterQuickCheckContext missingLookup = {valid.get(), nullptr, nullptr, nullptr, nullptr};
  EXPECT_EQ(FilterQuickCheck_MayMatch(&missingLookup, 1), 1);
}

TEST(FilterQuickCheckTest, budgetExhaustionNeverDropsARequiredDisjunct) {
  FilterQuickCheckSignature empty = {};
  // The final UNKNOWN could match any document, even if every earlier term is absent.
  for (size_t count : {size_t{255}, size_t{256}, size_t{257}, size_t{1000}}) {
    std::vector<FilterQuickCheckPredicate> children(count, term("missing"));
    children.back() = {};
    auto check = compile(compound(FilterQuickCheck_OR, children.data(), children.size()));
    EXPECT_EQ(FilterQuickCheck_Evaluate(check.get(), &empty), FilterQuickCheck_MAYBE) << count;
  }
  // A pathological cyclic input is bounded too; it must not recurse indefinitely.
  FilterQuickCheckPredicate cyclic = {};
  cyclic = compound(FilterQuickCheck_AND, &cyclic, 1);
  auto check = compile(cyclic);
  EXPECT_EQ(FilterQuickCheck_Evaluate(check.get(), &empty), FilterQuickCheck_MAYBE);
}

TEST(FilterQuickCheckTest, callbackAndBatchesAllowRandomAccessAndMissingSummaries) {
  auto predicate = term("electronics");
  auto check = compile(predicate);
  Provider provider;
  provider.documents[10] = signature({predicate});
  provider.documents[3] = {};
  FilterQuickCheckContext context = {check.get(), Provider::lookup, &provider, nullptr, nullptr};
  const std::array<uint64_t, 5> ids = {10, 3, 999, 3, 10};
  std::array<FilterQuickCheckResult, ids.size()> results;
  FilterQuickCheck_CheckBatch(&context, ids.data(), ids.size(), results.data());
  EXPECT_EQ(results, (std::array<FilterQuickCheckResult, 5>{
                         FilterQuickCheck_MAYBE, FilterQuickCheck_NO, FilterQuickCheck_MAYBE,
                         FilterQuickCheck_NO, FilterQuickCheck_MAYBE}));
  EXPECT_EQ(provider.requests, (std::vector<uint64_t>(ids.begin(), ids.end())));
  EXPECT_EQ(FilterQuickCheck_MayMatch(&context, 3), 0);
  EXPECT_EQ(FilterQuickCheck_MayMatch(&context, 10), 1);
  EXPECT_EQ(FilterQuickCheck_MayMatch(&context, 999), 1);
  FilterQuickCheck_CheckBatch(nullptr, nullptr, 0, nullptr);

  // A provider may invalidate a summary across an update/reload and publish the replacement.
  provider.documents.erase(3);
  EXPECT_EQ(FilterQuickCheck_MayMatch(&context, 3), 1);
  provider.documents[3] = signature({predicate});
  EXPECT_EQ(FilterQuickCheck_MayMatch(&context, 3), 1);
}

TEST(FilterQuickCheckTest, unsupportedPlanDoesNotFetchDocumentSummaries) {
  FilterQuickCheckPredicate unknown = {};
  auto check = compile(unknown);
  Provider provider;
  FilterQuickCheckContext context = {check.get(), Provider::lookup, &provider, nullptr, nullptr};
  EXPECT_EQ(FilterQuickCheck_MayMatch(&context, 42), 1);
  EXPECT_TRUE(provider.requests.empty());
}

TEST(FilterQuickCheckTest, exactRecheckRemovesFalsePositivesWithoutLosingMatches) {
  auto predicate = term("electronics");
  auto check = compile(predicate);
  Provider provider;
  provider.documents[1] = signature({predicate});
  provider.documents[2] = {};
  // Model a collision/saturated signature for a nonmatching document.
  memset(&provider.documents[3], 0xff, sizeof(FilterQuickCheckSignature));
  // Document 4 matches but has no resident summary; it must reach the exact filter.
  FilterQuickCheckContext context = {check.get(), Provider::lookup, &provider, nullptr, nullptr};
  std::vector<uint64_t> verified, rechecked;
  for (uint64_t id : {4, 3, 2, 1}) {
    if (FilterQuickCheck_MayMatch(&context, id)) {
      rechecked.push_back(id);
      if (id == 1 || id == 4) {
        verified.push_back(id);
      }
    }
  }
  EXPECT_EQ(rechecked, (std::vector<uint64_t>{4, 3, 1}));
  EXPECT_EQ(verified, (std::vector<uint64_t>{4, 1}));
}

TEST(FilterQuickCheckTest, numericBuilderRequiresInitializationAndPoisonsInvalidInput) {
  FilterQuickCheckNumericSummary summary = {};
  FilterQuickCheckNumericSummary_Add(&summary, 5);
  EXPECT_EQ(summary.state, FilterQuickCheckNumeric_UNKNOWN);
  FilterQuickCheckNumericSummary_Init(&summary);
  EXPECT_EQ(summary.state, FilterQuickCheckNumeric_EMPTY);
  for (double value : {5.0, -2.0, 9.0, 5.0}) {
    FilterQuickCheckNumericSummary_Add(&summary, value);
  }
  EXPECT_EQ(summary.state, FilterQuickCheckNumeric_BOUNDS);
  EXPECT_EQ(summary.min, -2);
  EXPECT_EQ(summary.max, 9);
  FilterQuickCheckNumericSummary_Add(&summary, std::numeric_limits<double>::quiet_NaN());
  FilterQuickCheckNumericSummary_Add(&summary, 7);
  EXPECT_EQ(summary.state, FilterQuickCheckNumeric_UNKNOWN);
  FilterQuickCheckNumericSummary_Init(&summary);
  FilterQuickCheckNumericSummary_Add(&summary, 7);
  EXPECT_EQ(summary.min, 7);
  EXPECT_EQ(summary.max, 7);
  summary.min = 8;
  FilterQuickCheckNumericSummary_Add(&summary, 10);
  EXPECT_EQ(summary.state, FilterQuickCheckNumeric_UNKNOWN);
  FilterQuickCheckNumericSummary_Init(nullptr);
  FilterQuickCheckNumericSummary_Add(nullptr, 1);
}

TEST(FilterQuickCheckTest, numericScalarChecksRespectEndpointsInfinityAndSignedZero) {
  const double inf = std::numeric_limits<double>::infinity();
  const std::array<double, 7> values = {-inf, -1, -0.0, 0.0, 1, 2, inf};
  NumericProvider provider;
  FilterQuickCheckContext context = {nullptr, nullptr, nullptr, NumericProvider::lookup, &provider};
  for (double min : values) {
    for (double max : values) {
      if (min > max) {
        continue;
      }
      for (unsigned inclusive = 0; inclusive < 4; ++inclusive) {
        auto predicate = numeric(min, max, inclusive & 1, inclusive & 2);
        auto check = compile(predicate);
        context.check = check.get();
        for (double value : values) {
          provider.documents[1][2] = numericSummary({value});
          bool exact = (predicate.numeric.minInclusive ? value >= min : value > min) &&
                       (predicate.numeric.maxInclusive ? value <= max : value < max);
          EXPECT_EQ(FilterQuickCheck_MayMatch(&context, 1), exact)
              << "value=" << value << " min=" << min << " max=" << max
              << " inclusive=" << inclusive;
        }
        provider.documents[1][2] = numericSummary({});
        EXPECT_EQ(FilterQuickCheck_MayMatch(&context, 1), 0);
      }
    }
  }
}

TEST(FilterQuickCheckTest, numericMultivalueBoundsNeverRejectAnExactMatch) {
  const double inf = std::numeric_limits<double>::infinity();
  const std::array<double, 6> values = {-inf, -10, -0.0, 0.0, 10, inf};
  const std::array<double, 7> endpoints = {-inf, -11, -1, 0, 1, 11, inf};
  NumericProvider provider;
  FilterQuickCheckContext context = {nullptr, nullptr, nullptr, NumericProvider::lookup, &provider};
  for (double min : endpoints) {
    for (double max : endpoints) {
      if (min > max) {
        continue;
      }
      for (unsigned inclusive = 0; inclusive < 4; ++inclusive) {
        auto predicate = numeric(min, max, inclusive & 1, inclusive & 2);
        auto check = compile(predicate);
        context.check = check.get();
        for (unsigned subset = 0; subset < (1U << values.size()); ++subset) {
          auto summary = numericSummary({});
          bool exact = false;
          for (size_t j = 0; j < values.size(); ++j) {
            if (subset & (1U << j)) {
              FilterQuickCheckNumericSummary_Add(&summary, values[j]);
              exact |= (predicate.numeric.minInclusive ? values[j] >= min : values[j] > min) &&
                       (predicate.numeric.maxInclusive ? values[j] <= max : values[j] < max);
            }
          }
          provider.documents[1][2] = summary;
          const bool maybe = FilterQuickCheck_MayMatch(&context, 1);
          EXPECT_FALSE(exact && !maybe) << "subset=" << subset << " min=" << min << " max=" << max
                                        << " inclusive=" << inclusive;
          if (!subset) {
            EXPECT_FALSE(maybe);
          }
        }
      }
    }
  }
}

TEST(FilterQuickCheckTest, numericGapsAndIndependentArrayPredicatesRequireExactRechecks) {
  NumericProvider provider;
  provider.documents[1][2] = numericSummary({10, 100});
  auto gap = compile(numeric(40, 60));
  FilterQuickCheckContext context = {gap.get(), nullptr, nullptr, NumericProvider::lookup,
                                     &provider};
  EXPECT_EQ(FilterQuickCheck_MayMatch(&context, 1), 1);
  // Different array elements can satisfy different conjuncts on the same field.
  std::array<FilterQuickCheckPredicate, 2> children = {numeric(10, 10), numeric(100, 100)};
  auto both = compile(compound(FilterQuickCheck_AND, children.data(), children.size()));
  context.check = both.get();
  EXPECT_EQ(FilterQuickCheck_MayMatch(&context, 1), 1);
}

TEST(FilterQuickCheckTest, numericUnknownEmptyInvalidAndUncoveredFieldsAreDistinct) {
  NumericProvider provider;
  auto check = compile(numeric(20, 30));
  FilterQuickCheckContext context = {check.get(), nullptr, nullptr, NumericProvider::lookup,
                                     &provider};
  EXPECT_EQ(FilterQuickCheck_MayMatch(&context, 1), 1);
  provider.documents[1][3] = numericSummary({});
  EXPECT_EQ(FilterQuickCheck_MayMatch(&context, 1), 1);
  auto& summary = provider.documents[1][2];
  EXPECT_EQ(FilterQuickCheck_MayMatch(&context, 1), 1);
  summary = numericSummary({});
  EXPECT_EQ(FilterQuickCheck_MayMatch(&context, 1), 0);
  const double nan = std::numeric_limits<double>::quiet_NaN();
  for (auto malformed : {FilterQuickCheckNumericSummary{FilterQuickCheckNumeric_BOUNDS, 30, 20},
                         FilterQuickCheckNumericSummary{FilterQuickCheckNumeric_BOUNDS, nan, 20},
                         FilterQuickCheckNumericSummary{FilterQuickCheckNumeric_BOUNDS, 20, nan}}) {
    summary = malformed;
    EXPECT_EQ(FilterQuickCheck_MayMatch(&context, 1), 1);
  }
  summary = numericSummary({0});
  for (const auto& invalid : {numeric(nan, 1), numeric(0, nan), numeric(2, 1)}) {
    auto invalidCheck = compile(invalid);
    context.check = invalidCheck.get();
    provider.requests.clear();
    EXPECT_EQ(FilterQuickCheck_MayMatch(&context, 1), 1);
    EXPECT_TRUE(provider.requests.empty());
  }
}

TEST(FilterQuickCheckTest, numericAndTermProvidersFailOpenIndependently) {
  Provider terms;
  NumericProvider numbers;
  terms.documents[1] = {};
  numbers.documents[1][2] = numericSummary({100});
  std::array<FilterQuickCheckPredicate, 2> children = {term("book"), numeric(10, 20)};
  auto both = compile(compound(FilterQuickCheck_AND, children.data(), children.size()));
  auto either = compile(compound(FilterQuickCheck_OR, children.data(), children.size()));
  FilterQuickCheckContext context = {both.get(), Provider::lookup, &terms, NumericProvider::lookup,
                                     &numbers};
  EXPECT_EQ(FilterQuickCheck_MayMatch(&context, 1), 0);
  context.lookup = nullptr;
  EXPECT_EQ(FilterQuickCheck_MayMatch(&context, 1), 0);
  context.check = either.get();
  EXPECT_EQ(FilterQuickCheck_MayMatch(&context, 1), 1);
  context.lookup = Provider::lookup;
  EXPECT_EQ(FilterQuickCheck_MayMatch(&context, 1), 0);
  context.numericLookup = nullptr;
  EXPECT_EQ(FilterQuickCheck_MayMatch(&context, 1), 1);
  context.check = both.get();
  EXPECT_EQ(FilterQuickCheck_MayMatch(&context, 1), 0);
  context.numericLookup = NumericProvider::lookup;
  terms.documents.erase(1);
  EXPECT_EQ(FilterQuickCheck_MayMatch(&context, 1), 0);
  terms.documents[1] = {};
  numbers.documents.erase(1);
  EXPECT_EQ(FilterQuickCheck_MayMatch(&context, 1), 0);
  terms.documents.erase(1);
  EXPECT_EQ(FilterQuickCheck_MayMatch(&context, 1), 1);
}

TEST(FilterQuickCheckTest, numericOnlySkipsSignatureLookupAndSupportsBatchReplacement) {
  Provider terms;
  NumericProvider numbers;
  auto predicate = numeric(10, 20);
  auto check = compile(predicate);
  predicate.numeric = {3, 100, 200, true, true};
  numbers.documents[5][2] = numericSummary({15});
  numbers.documents[2][2] = numericSummary({30});
  FilterQuickCheckContext context = {check.get(), Provider::lookup, &terms, NumericProvider::lookup,
                                     &numbers};
  const std::array<uint64_t, 5> ids = {5, 2, 999, 2, 5};
  std::array<FilterQuickCheckResult, ids.size()> results;
  FilterQuickCheck_CheckBatch(&context, ids.data(), ids.size(), results.data());
  EXPECT_EQ(results, (std::array<FilterQuickCheckResult, 5>{
                         FilterQuickCheck_MAYBE, FilterQuickCheck_NO, FilterQuickCheck_MAYBE,
                         FilterQuickCheck_NO, FilterQuickCheck_MAYBE}));
  EXPECT_TRUE(terms.requests.empty());
  EXPECT_EQ(numbers.requests,
            (std::vector<std::pair<uint64_t, uint16_t>>{{5, 2}, {2, 2}, {999, 2}, {2, 2}, {5, 2}}));
  numbers.documents.erase(2);
  EXPECT_EQ(FilterQuickCheck_MayMatch(&context, 2), 1);
  numbers.documents[2][2] = numericSummary({12});
  EXPECT_EQ(FilterQuickCheck_MayMatch(&context, 2), 1);
}

TEST(FilterQuickCheckTest, signatureOnlyEvaluationAndNegatedNumericLeavesStayConservative) {
  auto range = numeric(10, 20);
  auto check = compile(range);
  FilterQuickCheckSignature empty = {};
  EXPECT_EQ(FilterQuickCheck_Evaluate(check.get(), &empty), FilterQuickCheck_MAYBE);
  FilterQuickCheckContext absent = {check.get(), nullptr, nullptr, nullptr, nullptr};
  EXPECT_EQ(FilterQuickCheck_MayMatch(&absent, 1), 1);
  NumericProvider provider;
  provider.documents[1][2] = numericSummary({30});
  auto negated = compile(compound(FilterQuickCheck_NOT, &range, 1));
  FilterQuickCheckContext context = {negated.get(), nullptr, nullptr, NumericProvider::lookup,
                                     &provider};
  EXPECT_EQ(FilterQuickCheck_MayMatch(&context, 1), 1);
  EXPECT_TRUE(provider.requests.empty());
  auto termCheck = compile(term("book"));
  context.check = termCheck.get();
  EXPECT_EQ(FilterQuickCheck_MayMatch(&context, 1), 1);
  EXPECT_TRUE(provider.requests.empty());
}
