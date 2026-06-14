/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#ifndef TERM_STORE_H__
#define TERM_STORE_H__

#include "trie/trie.h"

#include "redismodule.h"

#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

Trie *TermStore_NewTermsTrie(void);

bool TermStore_AddTerm(Trie *terms, const char *term, size_t len, size_t *addedBytes);

bool TermStore_DecrTerm(Trie *terms, const char *term, size_t len, size_t decr);

size_t TermStore_TermsMemUsage(const Trie *terms);

void TermStore_RdbSaveTerms(RedisModuleIO *rdb, Trie *terms);

Trie *TermStore_RdbLoadTerms(RedisModuleIO *rdb, bool withNumDocs);

#ifdef __cplusplus
}
#endif

#endif  // TERM_STORE_H__
