/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// Implementations of the `RediSearch_*` helpers still needed by C++ tests.
// Lifted verbatim from the deleted `src/redisearch_api.c`, with only the
// references to the now-removed `Index_FromLLAPI` flag and
// `IndexSpec::getValue`/`getValueCtx` callback fields dropped.

#include "llapi_test_helpers.h"
#include "common.h"

#include "spec.h"
#include "field_spec.h"
#include "document.h"
#include "rmutil/rm_assert.h"
#include "util/dict.h"
#include "util/references.h"
#include "query_node.h"
#include "search_options.h"
#include "query_internal.h"
#include "suffix.h"
#include "triemap_ffi.h"
#include "query.h"
#include "extension.h"
#include "fork_gc.h"
#include "iterators/iterator_api.h"
#include "config.h"
#include "doc_table.h"
#include "gc.h"
#include "obfuscation/hidden.h"

#include <float.h>

extern "C" {
#include "search_disk.h"
}

RefManager* RediSearch_CreateIndex(const char* name, const RSIndexOptions* options) {
  RSIndexOptions opts_s = {.gcPolicy = GC_POLICY_FORK, .stopwordsLen = -1};
  if (!options) {
    options = &opts_s;
  }
  IndexSpec* spec = NewIndexSpec(NewHiddenString(name, strlen(name), true));
  spec->own_ref = StrongRef_New(spec, (RefManager_Free)IndexSpec_Free);
  IndexSpec_MakeKeyless(spec);
  //TODO: Should not be supported for SearchDisk, but no way to return error in programmatic API
  spec->flags = (IndexFlags)(spec->flags | Index_Temporary);  // temporary is so that we will not use threads!!

  if (options->flags & RSIDXOPT_DOCTBLSIZE_UNLIMITED) {
    spec->docs.maxSize = DOCID_MAX;
  }
  if (options->gcPolicy != GC_POLICY_NONE) {
    spec->gc = GCContext_CreateGC(spec->own_ref, options->gcPolicy);
    GCContext_Start(spec->gc);
  }
  if (options->stopwordsLen != -1) {
    // replace default list which is a global so no need to free anything.
    spec->stopwords = NewStopWordListCStr((const char **)options->stopwords,
                                                         options->stopwordsLen);
  }
  return spec->own_ref.rm;
}

RSFieldID RediSearch_CreateField(RefManager* rm, const char* name, unsigned types,
                                 unsigned options) {
  RS_LOG_ASSERT(types, "types should not be RSFLDTYPE_DEFAULT");
  IndexSpec *sp = get_spec(rm);

  // TODO: add a function which can take both path and name
  FieldSpec* fs = IndexSpec_CreateField(sp, name, NULL);
  RS_LOG_ASSERT_FMT(fs, "Failed to create field %s", name);

  int numTypes = 0;
  if (types & RSFLDTYPE_FULLTEXT) {
    numTypes++;
    int txtId = IndexSpec_CreateTextId(sp, fs->index);
    if (txtId < 0) {
      return RSFIELD_INVALID;
    }
    fs->ftId = txtId;
    fs->types = (FieldType)(fs->types | INDEXFLD_T_FULLTEXT);
  }

  if (types & RSFLDTYPE_NUMERIC) {
    numTypes++;
    fs->types = (FieldType)(fs->types | INDEXFLD_T_NUMERIC);
  }
  if (types & RSFLDTYPE_GEO) {
    fs->types = (FieldType)(fs->types | INDEXFLD_T_GEO);
    numTypes++;
  }
  if (types & RSFLDTYPE_VECTOR) {
    fs->types = (FieldType)(fs->types | INDEXFLD_T_VECTOR);
    numTypes++;
  }
  if (types & RSFLDTYPE_TAG) {
    fs->types = (FieldType)(fs->types | INDEXFLD_T_TAG);
    numTypes++;
  }

  if (numTypes > 1) {
    fs->options = (FieldSpecOptions)(fs->options | FieldSpec_Dynamic);
  }

  if (options & RSFLDOPT_NOINDEX) {
    fs->options = (FieldSpecOptions)(fs->options | FieldSpec_NotIndexable);
  }
  if (options & RSFLDOPT_SORTABLE) {
    fs->options = (FieldSpecOptions)(fs->options | FieldSpec_Sortable);
    fs->sortIdx = sp->numSortableFields++;
  }
  if (options & RSFLDOPT_TXTNOSTEM) {
    fs->options = (FieldSpecOptions)(fs->options | FieldSpec_NoStemming);
  }
  if (options & RSFLDOPT_TXTPHONETIC) {
    fs->options = (FieldSpecOptions)(fs->options | FieldSpec_Phonetics);
    sp->flags = (IndexFlags)(sp->flags | Index_HasPhonetic);
  }
  if (options & RSFLDOPT_WITHSUFFIXTRIE) {
    fs->options = (FieldSpecOptions)(fs->options | FieldSpec_WithSuffixTrie);
    if (fs->types == INDEXFLD_T_FULLTEXT) {
      sp->suffixMask |= FIELD_BIT(fs);
      if (!sp->suffix) {
        sp->suffix = NewTermSuffixIndex();
        sp->flags = (IndexFlags)(sp->flags | Index_HasSuffixTrie);
      }
    }
  }

  return fs->index;
}

int RediSearch_DeleteDocument(RefManager* rm, const void* docKey, size_t len) {
  RS_ASSERT(!SearchDisk_IsEnabled());
  IndexSpec* sp = get_spec(rm);
  int rc = REDISMODULE_OK;
  t_docId id = DocTable_GetId(&sp->docs, (const char *)docKey, len);
  if (id == 0) {
    rc = REDISMODULE_ERR;
  } else {
    RSDocumentMetadata* md = DocTable_Pop(&sp->docs, (const char *)docKey, len);
    if (md) {
      // Delete returns true/false, not RM_{OK,ERR}
      RS_LOG_ASSERT(sp->stats.scoring.numDocuments > 0, "numDocuments cannot be negative");
      sp->stats.scoring.numDocuments--;
      RS_LOG_ASSERT(sp->stats.scoring.totalDocsLen >= md->docLen, "totalDocsLen is smaller than md->docLen");
      sp->stats.scoring.totalDocsLen -= md->docLen;
      DMD_Return(md);

      if (sp->gc) {
        GCContext_OnDelete(sp->gc);
      }
    } else {
      rc = REDISMODULE_ERR;
    }
  }

  return rc;
}

struct RS_ApiIter {
  QueryIterator* internal;
  RedisSearchCtx sctx;
  RSIndexResult* res;
  const RSDocumentMetadata* lastmd;
  ScoringFunctionArgs scargs;
  RSScoringFunction scorer;
  RSFreeFunction scorerFree;
  double minscore;  // Used for scoring
  QueryAST qast;    // Used for string queries..
  IndexSpec* sp;
};

#define QUERY_INPUT_STRING 1
#define QUERY_INPUT_NODE 2

typedef struct {
  int qtype;
  union {
    struct {
      const char* qs;
      size_t n;
      unsigned int dialect;
    } s;
    QueryNode* qn;
  } u;
} QueryInput;

static RS_ApiIter* handleIterCommon(IndexSpec* sp, QueryInput* input, char** error) {
  /* The per-spec rwlock is acquired here and released in RediSearch_ResultsIteratorFree.
   * On error, cleanup is done here if iter->sp wasn't set, otherwise in Free. */
  pthread_rwlock_rdlock(&sp->rwlock);
  /* We might have multiple readers that reads from the index,
   * Avoid rehashing the terms dictionary */
  dictPauseRehashing(sp->keysDict);

  RSSearchOptions options = {0};
  QueryError status = QueryError_Default();
  RSSearchOptions_Init(&options);
  if (sp->rule != NULL && sp->rule->lang_default != DEFAULT_LANGUAGE) {
    options.language = sp->rule->lang_default;
  }

  const char *defaultScorer = NULL;
  ExtScoringFunctionCtx* scoreCtx = NULL;

  RS_ApiIter* it = (RS_ApiIter *)rm_calloc(1, sizeof(*it));
  it->sctx = SEARCH_CTX_STATIC(NULL, sp);

  if (input->qtype == QUERY_INPUT_STRING) {
    if (QAST_Parse(&it->qast, &it->sctx, &options, input->u.s.qs, input->u.s.n, input->u.s.dialect, &status) !=
        REDISMODULE_OK) {
      goto end;
    }
  } else {
    it->qast.root = input->u.qn;
  }

  // set queryAST configuration parameters
  iteratorsConfig_init(&it->qast.config);

  if (QAST_Expand(&it->qast, NULL, &options, &it->sctx, &status) != REDISMODULE_OK) {
    goto end;
  }

  it->internal = QAST_Iterate(&it->qast, &options, &it->sctx, 0, NULL, &status);
  RS_ASSERT(it->internal);

  IndexSpec_GetStats(sp, &it->scargs.indexStats);
  defaultScorer = RSGlobalConfig.defaultScorer;
  RS_LOG_ASSERT(defaultScorer, "No default scorer");
  scoreCtx = Extensions_GetScoringFunction(&it->scargs, defaultScorer);
  RS_LOG_ASSERT(scoreCtx, "GetScoringFunction failed");
  it->scorer = scoreCtx->sf;
  it->scorerFree = scoreCtx->ff;
  it->minscore = DBL_MAX;
  it->sp = sp;

end:

  if (QueryError_HasError(&status)) {
    /* Resume rehashing and release locks only if iter->sp was not set,
     * since RediSearch_ResultsIteratorFree won't do it in that case.
     * If iter->sp is set, RediSearch_ResultsIteratorFree will handle cleanup. */
    if (!it->sp) {
      dictResumeRehashing(sp->keysDict);
      pthread_rwlock_unlock(&sp->rwlock);
    }
    RediSearch_ResultsIteratorFree(it);
    it = NULL;
    if (error) {
      *error = rm_strdup(QueryError_GetUserError(&status));
    }
  }
  QueryError_ClearError(&status);
  return it;
}

RS_ApiIter* RediSearch_IterateQuery(RefManager* rm, const char* s, size_t n, char** error) {
  QueryInput input = {QUERY_INPUT_STRING, {.s = {s, n, 1}}};
  return handleIterCommon(get_spec(rm), &input, error);
}

RS_ApiIter* RediSearch_GetResultsIterator(QueryNode* qn, RefManager* rm) {
  QueryInput input = {QUERY_INPUT_NODE, {.qn = qn}};
  return handleIterCommon(get_spec(rm), &input, NULL);
}

const void* RediSearch_ResultsIteratorNext(RS_ApiIter* iter, RefManager* rm, size_t* len) {
  IndexSpec *sp = get_spec(rm);
  while (iter->internal->Read(iter->internal) == ITERATOR_OK) {
    iter->res = iter->internal->current;
    const RSDocumentMetadata* md = DocTable_Borrow(&sp->docs, iter->res->docId);
    if (md == NULL) {
      continue;
    }
    DMD_Return(iter->lastmd);
    iter->lastmd = md;
    if (len) {
      *len = sdslen(md->keyPtr);
    }
    return md->keyPtr;
  }
  return NULL;
}

void RediSearch_ResultsIteratorFree(RS_ApiIter* iter) {
  if (iter->internal) {
    iter->internal->Free(iter->internal);
  }
  if (iter->scorerFree) {
    iter->scorerFree(iter->scargs.extdata);
  }
  QAST_Destroy(&iter->qast);
  DMD_Return(iter->lastmd);
  /* Release the per-spec lock only if iter->sp is set. On error paths in
   * handleIterCommon, if iter->sp is NULL, the lock was already released there. */
  if (iter->sp) {
    if (iter->sp->keysDict) {
      dictResumeRehashing(iter->sp->keysDict);
    }
    pthread_rwlock_unlock(&iter->sp->rwlock);
  }
  rm_free(iter);
}

const char *RediSearch_HiddenStringGet(const HiddenString* value) {
  return HiddenString_GetUnsafe(value, NULL);
}
