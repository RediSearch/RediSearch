/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "llapi_test_helpers.h"

#include "spec.h"
#include "doc_table.h"
#include "gc.h"
#include "config.h"
#include "query.h"
#include "search_ctx.h"
#include "search_options.h"
#include "iterators/iterator_api.h"
#include "obfuscation/hidden.h"
#include "util/dict.h"

extern "C" {
#include "rwlock.h"
#include "rmutil/rm_assert.h"
}

#define get_spec(x) ((IndexSpec *)__RefManager_Get_Object(x))

bool RS::deleteDocument(RedisModuleCtx *ctx, RSIndex *index, const char *docid) {
  RWLOCK_ACQUIRE_WRITE();
  IndexSpec *sp = get_spec(index);
  bool ok = false;
  const size_t len = strlen(docid);
  t_docId id = DocTable_GetId(&sp->docs, docid, len);
  if (id != 0) {
    RSDocumentMetadata *md = DocTable_Pop(&sp->docs, docid, len);
    if (md) {
      RS_LOG_ASSERT(sp->stats.scoring.numDocuments > 0, "numDocuments cannot be negative");
      sp->stats.scoring.numDocuments--;
      RS_LOG_ASSERT(sp->stats.scoring.totalDocsLen >= md->docLen,
                    "totalDocsLen is smaller than md->docLen");
      sp->stats.scoring.totalDocsLen -= md->docLen;
      DMD_Return(md);
      if (sp->gc) {
        GCContext_OnDelete(sp->gc);
      }
      ok = true;
    }
  }
  RWLOCK_RELEASE();
  return ok;
}

std::vector<std::string> RS::search(RSIndex *index, const char *s) {
  std::vector<std::string> ret;
  IndexSpec *sp = get_spec(index);

  RWLOCK_ACQUIRE_READ();
  pthread_rwlock_rdlock(&sp->rwlock);
  dictPauseRehashing(sp->keysDict);

  RSSearchOptions options = {0};
  RSSearchOptions_Init(&options);
  if (sp->rule != NULL && sp->rule->lang_default != DEFAULT_LANGUAGE) {
    options.language = sp->rule->lang_default;
  }

  RedisSearchCtx sctx = SEARCH_CTX_STATIC(NULL, sp);
  QueryAST ast = {0};
  QueryError status = QueryError_Default();

  if (QAST_Parse(&ast, &sctx, &options, s, strlen(s), 1, &status) != REDISMODULE_OK) {
    goto cleanup;
  }
  iteratorsConfig_init(&ast.config);
  if (QAST_Expand(&ast, NULL, &options, &sctx, &status) != REDISMODULE_OK) {
    goto cleanup;
  }
  {
    QueryIterator *it = QAST_Iterate(&ast, &options, &sctx, 0, &status);
    if (!it) {
      goto cleanup;
    }
    while (it->Read(it) == ITERATOR_OK) {
      const RSDocumentMetadata *md = DocTable_Borrow(&sp->docs, it->current->docId);
      if (md == NULL) {
        continue;
      }
      ret.emplace_back(md->keyPtr, sdslen(md->keyPtr));
      DMD_Return(md);
    }
    it->Free(it);
  }

cleanup:
  QAST_Destroy(&ast);
  QueryError_ClearError(&status);
  dictResumeRehashing(sp->keysDict);
  pthread_rwlock_unlock(&sp->rwlock);
  RWLOCK_RELEASE();
  return ret;
}

RefManager *createEmptySpec(const char *name) {
  IndexSpec *spec = NewIndexSpec(NewHiddenString(name, strlen(name), true));
  spec->own_ref = StrongRef_New(spec, (RefManager_Free)IndexSpec_Free);
  IndexSpec_MakeKeyless(spec);
  // Temporary so we don't start any background workers we don't need.
  spec->flags = (IndexFlags)(spec->flags | Index_Temporary);
  IndexSpec_StartGCFromSpec(spec->own_ref, spec, GCPolicy_Fork);
  return spec->own_ref.rm;
}

void addTagField(RefManager *ism, const char *name) {
  RWLOCK_ACQUIRE_WRITE();
  IndexSpec *sp = get_spec(ism);
  FieldSpec *fs = IndexSpec_CreateField(sp, name, NULL);
  RS_LOG_ASSERT(fs, "Failed to create tag field");
  fs->types = (FieldType)(fs->types | INDEXFLD_T_TAG);
  RWLOCK_RELEASE();
}

void addNumericField(RefManager *ism, const char *name) {
  RWLOCK_ACQUIRE_WRITE();
  IndexSpec *sp = get_spec(ism);
  FieldSpec *fs = IndexSpec_CreateField(sp, name, NULL);
  RS_LOG_ASSERT(fs, "Failed to create numeric field");
  fs->types = (FieldType)(fs->types | INDEXFLD_T_NUMERIC);
  RWLOCK_RELEASE();
}
