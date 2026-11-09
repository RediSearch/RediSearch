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
#include <cstring>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace {
using Check = std::unique_ptr<FilterQuickCheck, decltype(&FilterQuickCheck_Free)>;

Check compile(const FilterQuickCheckPredicate& predicate) {
  return Check(FilterQuickCheck_Compile(&predicate), FilterQuickCheck_Free);
}

FilterQuickCheckPredicate term(const char* value, uint16_t field = 1,
                               FilterQuickCheckTermKind kind = FilterQuickCheck_TAG) {
  FilterQuickCheckPredicate result = {};
  result.type = FilterQuickCheck_TERM;
  result.term = {kind, field, value, strlen(value)};
  return result;
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
    std::array<FilterQuickCheckPredicate, 3> atoms = {term(a.c_str(), 1), term(b.c_str(), 2),
                                                      term(c.c_str(), 3, FilterQuickCheck_TEXT)};
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
  FilterQuickCheckContext missingLookup = {valid.get(), nullptr, nullptr};
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
  FilterQuickCheckContext context = {check.get(), Provider::lookup, &provider};
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
  FilterQuickCheckContext context = {check.get(), Provider::lookup, &provider};
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
  FilterQuickCheckContext context = {check.get(), Provider::lookup, &provider};
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
