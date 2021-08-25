#ifndef CPPTESTS_COMMON_H
#define CPPTESTS_COMMON_H

#include "redismock/redismock.h"
#include "redismock/util.h"
#include "spec.h"
#include "document.h"

#ifdef __cplusplus
extern "C" {
#endif
#include "rwlock.h"
#ifdef __cplusplus
}
#endif

namespace RS {

static void donecb(RSAddDocumentCtx *aCtx, RedisModuleCtx *, void *) {
  // printf("Finished indexing document. Status: %s\n", QueryError_GetError(&aCtx->status));
}

template <typename... Ts>
bool addDocument(RedisModuleCtx *ctx, IndexSpec *sp, const char *docid, Ts... args) {
  RWLOCK_ACQUIRE_WRITE();
  RMCK::ArgvList argv(ctx, args...);
  AddDocumentOptions options = {0};
  options.numFieldElems = argv.size();
  options.fieldsArray = argv;
  options.donecb = donecb;
  options.keyStr = RedisModule_CreateString(ctx, docid, strlen(docid));
  options.score = 1.0;
  options.options = DOCUMENT_ADD_REPLACE;

  QueryError status = {QueryErrorCode(0)};
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, sp);
  int rv = RS_AddDocument(&sctx, RMCK::RString(docid), &options, &status);
  RedisModule_FreeString(ctx, options.keyStr);
  RWLOCK_RELEASE();
  return rv == REDISMODULE_OK;
}

bool deleteDocument(RedisModuleCtx *ctx, IndexSpec *sp, const char *docid);

template <typename... Ts>
IndexSpec *createIndex(RedisModuleCtx *ctx, const char *name, Ts... args) {
  RMCK::ArgvList argv("FT.CREATE", name, args...);
  QueryError err{QueryErrorCode(0)};
  IndexSpec *sp = IndexSpec_CreateNew(ctx, argv, argv.size(), &err);
  if (!sp) {
    abort();
  }
  return sp;
}

std::vector<std::string> search(RSIndex *index, RSQueryNode *qn);
std::vector<std::string> search(RSIndex *index, const char *s);

}  // namespace RS

#endif
