#ifndef CPPTESTS_COMMON_H
#define CPPTESTS_COMMON_H

#include "redismock/redismock.h"
#include "redismock/util.h"
#include "spec.h"
#include "document.h"
#include "rwlock.h"

namespace RS {

template <typename... Ts>
bool addDocument(RedisModuleCtx *ctx, IndexSpec *sp, const char *docid, Ts... args) {
  RWLOCK_ACQUIRE_WRITE();
  RMCK::ArgvList argv(ctx, args...);
  AddDocumentOptions options = {0};
  options.numFieldElems = argv.size();
  options.fieldsArray = argv;

  QueryError status = {QueryErrorCode(0)};
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, sp);
  int rv = RS_AddDocument(&sctx, RMCK::RString(docid), &options, &status);
  RWLOCK_RELEASE();
  return rv == REDISMODULE_OK;
}

bool deleteDocument(RedisModuleCtx *ctx, IndexSpec *sp, const char *docid);

static IndexSpec *createIndex(RedisModuleCtx *ctx, const char *name, RMCK::ArgvList &l) {
  QueryError err{QueryErrorCode(0)};
  ArgsCursor ac = {0};
  ArgsCursor_InitRString(&ac, l, l.size());
  AC_Seek(&ac, 2);
  IndexSpec *sp = IndexSpec_ParseArgs(name, &ac, NULL, &err);
  if (!sp) {
    abort();
  }
  return sp;
}

template <typename... Ts>
IndexSpec *createIndex(RedisModuleCtx *ctx, const char *name, Ts... args) {
  RMCK::ArgvList argv(ctx, "FT.CREATE", name, args...);
  return createIndex(ctx, name, argv);
}

static IndexSpec *createIndex(const char *name, const char **argv, size_t argc,
                              QueryError *err = NULL) {
  ArgsCursor ac = {0};
  ArgsCursor_InitCString(&ac, argv, argc);
  QueryError err_s = {QUERY_OK};
  if (err == NULL) {
    err = &err_s;
  }
  return IndexSpec_ParseArgs(name, &ac, NULL, err);
}

std::vector<std::string> search(RSIndex *index, RSQueryNode *qn);
std::vector<std::string> search(RSIndex *index, const char *s);

}  // namespace RS

#endif
