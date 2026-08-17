/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// Subset of the former low-level C API (`src/redisearch_api.{h,c}`) that the
// C++ test suite still relies on, kept here so the module no longer has to
// export these symbols. Names and signatures match what `redisearch_api.h`
// used to declare, so the test sources can stay almost untouched.
#ifndef TESTS_CPPTESTS_LLAPI_TEST_HELPERS_H_
#define TESTS_CPPTESTS_LLAPI_TEST_HELPERS_H_

#include "redismodule.h"
#include "obfuscation/hidden.h"
#include <limits.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct RefManager RSIndex;
typedef size_t RSFieldID;
#define RSFIELD_INVALID SIZE_MAX

typedef struct Document RSDoc;
typedef struct RSQueryNode RSQNode;
typedef struct RS_ApiIter RSResultsIterator;
typedef struct RSIdxOptions RSIndexOptions;

#define RSFLDTYPE_DEFAULT 0x00
#define RSFLDTYPE_FULLTEXT 0x01
#define RSFLDTYPE_NUMERIC 0x02
#define RSFLDTYPE_GEO 0x04
#define RSFLDTYPE_TAG 0x08
#define RSFLDTYPE_VECTOR 0x10

#define RSFLDOPT_NONE 0x00
#define RSFLDOPT_SORTABLE 0x01
#define RSFLDOPT_NOINDEX 0x02
#define RSFLDOPT_TXTNOSTEM 0x04
#define RSFLDOPT_TXTPHONETIC 0x08
#define RSFLDOPT_WITHSUFFIXTRIE 0x10

#define RSIDXOPT_DOCTBLSIZE_UNLIMITED 0x01

#define GC_POLICY_NONE -1
#define GC_POLICY_FORK 0

typedef int (*RSGetValueCallback)(void* ctx, const char* fieldName, const void* id, char** strVal,
                                  double* doubleVal);

struct RSIdxOptions {
  RSGetValueCallback gvcb;
  void* gvcbData;
  uint32_t flags;
  int gcPolicy;
  char **stopwords;
  int stopwordsLen;
  double score;
  const char *lang;
};

RSIndex* RediSearch_CreateIndex(const char *name, const RSIndexOptions* options);

RSFieldID RediSearch_CreateField(RSIndex* idx, const char* name, unsigned ftype, unsigned fopt);

#define RediSearch_CreateNumericField(idx, name) \
  RediSearch_CreateField(idx, name, RSFLDTYPE_NUMERIC, RSFLDOPT_NONE)
#define RediSearch_CreateTextField(idx, name) \
  RediSearch_CreateField(idx, name, RSFLDTYPE_FULLTEXT, RSFLDOPT_NONE)
#define RediSearch_CreateTagField(idx, name) \
  RediSearch_CreateField(idx, name, RSFLDTYPE_TAG, RSFLDOPT_NONE)
#define RediSearch_CreateGeoField(idx, name) \
  RediSearch_CreateField(idx, name, RSFLDTYPE_GEO, RSFLDOPT_NONE)
#define RediSearch_CreateVectorField(idx, name) \
  RediSearch_CreateField(idx, name, RSFLDTYPE_VECTOR, RSFLDOPT_NONE)

int RediSearch_DeleteDocument(RSIndex* sp, const void* docKey, size_t len);

RSResultsIterator* RediSearch_GetResultsIterator(RSQNode* qn, RSIndex* sp);

RSResultsIterator* RediSearch_IterateQuery(RSIndex* sp, const char* s, size_t n, char** err);

const void* RediSearch_ResultsIteratorNext(RSResultsIterator* iter, RSIndex* sp, size_t* len);

void RediSearch_ResultsIteratorFree(RSResultsIterator* iter);

const char* RediSearch_HiddenStringGet(const HiddenString* hs);

#ifdef __cplusplus
}
#endif

#endif /* TESTS_CPPTESTS_LLAPI_TEST_HELPERS_H_ */
