#include <gtest/gtest.h>
#include <aggregate/aggregate.h>
#include "redismock/redismock.h"
#include "redismock/util.h"
#include "redismock/internal.h"
#include "spec.h"
#include <module.h>
#include <version.h>
#include <vector>
#include <array>
#include <iostream>
#include <cstdarg>

class AggTest : public ::testing::Test {};

static void donecb(RSAddDocumentCtx *aCtx, RedisModuleCtx *, void *) {
  printf("Finished indexing document. Status: %s\n", QueryError_GetError(&aCtx->status));
}

template <typename... Ts>
void addDocument(RedisModuleCtx *ctx, IndexSpec *sp, const char *docid, Ts... args) {
  RMCK::ArgvList argv(ctx, args...);
  AddDocumentOptions options = {0};
  options.options |= DOCUMENT_ADD_CURTHREAD;
  options.numFieldElems = argv.size();
  options.fieldsArray = argv;
  options.donecb = donecb;

  QueryError status = {QueryErrorCode(0)};
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, sp);
  RS_AddDocument(&sctx, RedisModule_CreateString(ctx, docid, strlen(docid)), &options, &status);
}

TEST_F(AggTest, testBasic) {
  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
  QueryError qerr = {QueryErrorCode(0)};

  RMCK::ArgvList args(ctx, "FT.CREATE", "idx", "SCHEMA", "t1", "TEXT", "SORTABLE", "t2", "NUMERIC",
                      "sortable", "t3", "TEXT");
  auto spec = IndexSpec_CreateNew(ctx, args, args.size(), &qerr);
  ASSERT_TRUE(spec);

  // Try to create a document...
  addDocument(ctx, spec, "doc1", "t1", "value one", (const char *)NULL);
  addDocument(ctx, spec, "doc2", "t1", "value two", (const char *)NULL);
  addDocument(ctx, spec, "doc3", "t1", "value three", (const char *)NULL);

  RedisModuleKey *kk = RedisModule_OpenKey(
      ctx, RedisModule_CreateString(ctx, "doc1", strlen("doc1")), REDISMODULE_READ);
  ASSERT_FALSE(kk == NULL);
  // Ensure the key has the correct properties
  RedisModuleString *vtmp = NULL;
  int rv = RedisModule_HashGet(kk, REDISMODULE_HASH_CFIELDS, "t1", &vtmp, NULL);
  ASSERT_EQ(REDISMODULE_OK, rv);
  ASSERT_STREQ("value one", RedisModule_StringPtrLen(vtmp, NULL));
  RedisModule_CloseKey(kk);

  AREQ *rr = AREQ_New();
  RMCK::ArgvList aggArgs(ctx, "*");
  rv = AREQ_Compile(rr, aggArgs, aggArgs.size(), &qerr);
  ASSERT_EQ(REDISMODULE_OK, rv) << QueryError_GetError(&qerr);
  ASSERT_FALSE(QueryError_HasError(&qerr));

  RedisSearchCtx *sctx =
      NewSearchCtx(ctx, RedisModule_CreateString(ctx, spec->name, strlen(spec->name)));
  ASSERT_FALSE(sctx == NULL);
  rv = AREQ_ApplyContext(rr, sctx, &qerr);
  ASSERT_EQ(REDISMODULE_OK, rv);

  rv = AREQ_BuildPipeline(rr, &qerr);
  ASSERT_EQ(REDISMODULE_OK, rv) << QueryError_GetError(&qerr);

  auto rp = AREQ_RP(rr);
  ASSERT_FALSE(rp == NULL);

  SearchResult res = {0};
  RLookup *lk = AGPLN_GetLookup(&rr->ap, NULL, AGPLN_GETLOOKUP_LAST);
  while ((rv = rp->Next(rp, &res)) == RS_RESULT_OK) {
    std::cerr << "Doc ID: " << res.docId << std::endl;
    for (auto kk = lk->head; kk; kk = kk->next) {
      RSValue *vv = RLookup_GetItem(kk, &res.rowdata);
      if (vv != NULL) {
        std::cerr << "  " << kk->name << ": ";
        RSValue_Print(vv);
        std::cerr << std::endl;
      }
    }
    SearchResult_Clear(&res);
  }
  ASSERT_EQ(RS_RESULT_EOF, rv);

  SearchResult_Destroy(&res);
  AREQ_Free(rr);
  IndexSpec_Free(spec);
}