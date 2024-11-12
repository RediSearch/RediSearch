/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "inverted_index.h"
#include "numeric_index.h"
#include <string>

/** returns a string object containg @param id as a string */
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

InvertedIndex *createIndex(int size, int idStep, int start_with=0);
