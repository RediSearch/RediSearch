/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "term_store.h"

#include "rmutil/rm_assert.h"

Trie *TermStore_NewTermsTrie(void) {
  return NewTrie(NULL, Trie_Sort_Lex);
}

bool TermStore_AddTerm(Trie *terms, const char *term, size_t len, size_t *addedBytes) {
  // Payload is NULL so TRIE_ERR_PAYLOAD_OVERFLOW cannot occur
  int rc = Trie_InsertStringBuffer(terms, (char *)term, len, 1, 1, NULL, 1);
  if (rc == TRIE_OK_NEW) {
    *addedBytes = len;
    return true;
  }
  return false;
}

bool TermStore_DecrTerm(Trie *terms, const char *term, size_t len, size_t decr) {
  if (!terms || decr == 0) {
    return false;
  }
  // Decrement the numDocs count for this term in the trie
  // If numDocs reaches 0, the node will be deleted
  TrieDecrResult result = Trie_DecrementNumDocs(terms, term, len, decr);
  RS_ASSERT(result != TRIE_DECR_NOT_FOUND);
  return result == TRIE_DECR_DELETED;
}

size_t TermStore_TermsMemUsage(const Trie *terms) {
  return TrieType_MemUsage(terms);
}

void TermStore_RdbSaveTerms(RedisModuleIO *rdb, Trie *terms) {
  TrieType_GenericSave(rdb, terms, false, true);
}

Trie *TermStore_RdbLoadTerms(RedisModuleIO *rdb, bool withNumDocs) {
  return TrieType_GenericLoad(rdb, false, withNumDocs, Trie_Sort_Lex);
}
