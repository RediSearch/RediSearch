/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "index.h"
#include "inverted_index.h"
#include "numeric_index.h"
#include <string>
#include <vector>
#include <algorithm>

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
RefManager *createSpec(RedisModuleCtx *ctx);

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
 * Then, you can get the range memory by calling NumericRangeGetMemory(failed_range);
 * NOTE: Upon early bail out, the returned value will **not** include the memory used by the failed range.
 */
size_t CalculateNumericInvertedIndexMemory(NumericRangeTree *rt, NumericRangeNode **failed_range);

/**
 * Returns the total memory consumed by the inverted index of a numeric tree node.
 */
size_t NumericRangeGetMemory(const NumericRangeNode *Node);

NumericRangeTree *getNumericTree(IndexSpec *spec, const char *field);

class MockQueryEvalCtx {
public:
  QueryEvalCtx qctx;
  RedisSearchCtx sctx;
  IndexSpec spec;
  DocTable docTable;
  SchemaRule rule;

  MockQueryEvalCtx(t_docId maxDocId = 0, size_t numDocs = 0) {
    // Initialize DocTable
    docTable.maxDocId = maxDocId;
    docTable.size = numDocs ?: maxDocId;

    // Initialize SchemaRule
    rule.index_all = false;

    // Initialize IndexSpec
    spec.rule = &rule;
    spec.existingDocs = nullptr;

    // Initialize RedisSearchCtx
    sctx.spec = &spec;

    // Initialize QueryEvalCtx
    qctx.sctx = &sctx;
    qctx.docTable = &docTable;
  }

  MockQueryEvalCtx(std::vector<t_docId> &docs) : MockQueryEvalCtx() {
    std::sort(docs.begin(), docs.end());
    docs.erase(std::unique(docs.begin(), docs.end()), docs.end());
    docTable.maxDocId = docs.empty() ? 0 : docs.back();
    docTable.size = docs.size();
    rule.index_all = true; // Enable index_all for wildcard iterator tests
    spec.existingDocs = NewInvertedIndex(Index_DocIdsOnly, 1, &spec.stats.invertedSize);
    IndexEncoder enc = InvertedIndex_GetEncoder(spec.existingDocs->flags);
    for (t_docId docId : docs) {
      InvertedIndex_WriteEntryGeneric(spec.existingDocs, enc, docId, nullptr);
    }
  }

  ~MockQueryEvalCtx() noexcept {
    if (spec.existingDocs) {
      InvertedIndex_Free(spec.existingDocs);
    }
  }
};
