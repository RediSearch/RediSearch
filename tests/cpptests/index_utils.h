/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include "query_ctx.h"
#include "inverted_index.h"
#include "numeric_index.h"
#include "ttl_table.h"
#include <string>
#include <vector>
#include <algorithm>
#include <cstring>

/** returns a string object containing @param id as a string */
std::string numToDocStr(unsigned id);

/** Adds a document to a given index.
 * Returns the memory added to the index */
size_t addDocumentWrapper(RedisModuleCtx *ctx, RSIndex *index, const char *docid, const char *field, const char *value);

InvertedIndex *createPopulateTermsInvIndex(int size, int idStep, int start_with=0);

/** Returns a reference manager object to new spec.
 * To get the spec object (not safe), call get_spec(ism);
 * To free the spec and its resources, call freeSpec;
 */
RefManager *createSpec(RedisModuleCtx *ctx, const std::vector<const char*>& prefixes = {});

void freeSpec(RefManager *ism);

/**
 * Iterates the inverted indices in a the numeric tree and calculates the memory used by them.
 * This memory includes memory allocated for data and blocks metadata.
 * NOTE: the returned memory doesn't not include the memory used by the tree itself.
 *
 * If @param rt is NULL, the function will return 0.
 *
 * this function also verifies that the memory counter of each range is equal to its actual memory.
 * if not, if will set @param failed_range to point to the range that failed the check.
 * Then, you can get the range memory by calling InvertedIndex_MemUsage(failed_range);
 * NOTE: Upon early bail out, the returned value will **not** include the memory used by the failed range.
 */
size_t CalculateNumericInvertedIndexMemory(NumericRangeTree *rt, NumericRangeNode **failed_range);

NumericRangeTree *getNumericTree(IndexSpec *spec, const char *field);

class MockQueryEvalCtx {
public:
  QueryEvalCtx qctx;
  RedisSearchCtx sctx;
  IndexSpec spec;
  SchemaRule rule;

  MockQueryEvalCtx(t_docId maxDocId = 0, size_t numDocs = 0) {
    // Initialize SchemaRule
    std::memset(&rule, 0, sizeof(rule));
    rule.index_all = false;

    // Initialize IndexSpec
    spec = {0};
    spec.rule = &rule;
    spec.existingDocs = nullptr;
    spec.monitorDocumentExpiration = true; // Only depends on API availability, so always true
    spec.monitorFieldExpiration = true; // Only depends on API availability, so always true
    spec.docs.maxDocId = maxDocId;
    spec.docs.size = numDocs ?: maxDocId;
    spec.stats.numDocuments = spec.docs.size;

    // Initialize RedisSearchCtx
    sctx = {0};
    sctx.spec = &spec;

    // Initialize QueryEvalCtx
    qctx = {0};
    qctx.sctx = &sctx;
    qctx.docTable = &spec.docs;
  }

  MockQueryEvalCtx(std::vector<t_docId> &docs) : MockQueryEvalCtx() {
    std::sort(docs.begin(), docs.end());
    docs.erase(std::unique(docs.begin(), docs.end()), docs.end());
    spec.docs.maxDocId = docs.empty() ? 0 : docs.back();
    spec.docs.size = docs.size();
    spec.stats.numDocuments = docs.size();
    rule.index_all = true; // Enable index_all for wildcard iterator tests
    spec.existingDocs = NewInvertedIndex(Index_DocIdsOnly, &spec.stats.invertedSize);
    for (t_docId docId : docs) {
      RSIndexResult rec = {.docId = docId, .data = {.tag = RSResultData_Virtual}};
      InvertedIndex_WriteEntryGeneric(spec.existingDocs, &rec);
    }
  }

  ~MockQueryEvalCtx() noexcept {
    if (spec.existingDocs) {
      InvertedIndex_Free(spec.existingDocs);
    }
    TimeToLiveTable_Destroy(&spec.docs.ttl);
    array_free(spec.fieldIdToIndex);
  }

  void TTL_Add(t_docId docId, t_expirationTimePoint expiration = {LONG_MAX, LONG_MAX}) {
    VerifyTTLInit();
    TimeToLiveTable_Add(spec.docs.ttl, docId, expiration, NULL);
  }

  void TTL_Add(t_docId docId, t_fieldIndex field, t_expirationTimePoint expiration = {LONG_MAX, LONG_MAX}) {
    VerifyTTLInit();
    arrayof(FieldExpiration) fe = array_new(FieldExpiration, 1);
    FieldExpiration fe_entry = {field, expiration};
    array_append(fe, fe_entry);
    TimeToLiveTable_Add(spec.docs.ttl, docId, {LONG_MAX, LONG_MAX}, fe);
  }
  void TTL_Add(t_docId docId, t_fieldMask fieldMask, t_expirationTimePoint expiration = {LONG_MAX, LONG_MAX}) {
    VerifyTTLInit();
    arrayof(FieldExpiration) fe = array_new(FieldExpiration, __builtin_popcountll(fieldMask));
    for (t_fieldIndex i = 0; i < sizeof(fieldMask) * 8; ++i) {
      if (fieldMask & (1ULL << i)) {
        FieldExpiration fe_entry = {i, expiration};
        array_append(fe, fe_entry);
      }
    }
    TimeToLiveTable_Add(spec.docs.ttl, docId, {LONG_MAX, LONG_MAX}, fe);
  }

private:
  void VerifyTTLInit() {
    if (!spec.fieldIdToIndex) {
      // By default, set a max-length array (128 text fields) with fieldId(i) -> index(i)
      spec.fieldIdToIndex = array_new(t_fieldIndex, 128);
      for (t_fieldIndex i = 0; i < 128; ++i) {
        array_append(spec.fieldIdToIndex, i);
      }
    }
    TimeToLiveTable_VerifyInit(&spec.docs.ttl);
  }
};
