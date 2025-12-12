/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/


#include "gtest/gtest.h"
#include "aggregate/aggregate.h"
#include "redismock/redismock.h"
#include "redismock/util.h"
#include "redismock/internal.h"
#include "spec.h"
#include "common.h"
#include "module.h"
#include "version.h"
#include "search_result_rs.h"

#include <vector>
#include <array>
#include <iostream>
#include <cstdarg>

class AggTest : public ::testing::Test {};
using RS::addDocument;

#ifdef HAVE_RM_SCANCURSOR_CREATE
//@@ TODO: avoid background indexing so cursor won't be needed

TEST_F(AggTest, testBasic) {
  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
  QueryError qerr = QueryError_Default();

  RMCK::ArgvList args(ctx, "FT.CREATE", "idx", "ON", "HASH",
                      "SCHEMA", "t1", "TEXT", "SORTABLE", "t2", "NUMERIC",
                      "sortable", "t3", "TEXT");
  auto spec = IndexSpec_CreateNew(ctx, args, args.size(), &qerr);
  ASSERT_TRUE(spec);

  // Try to create a document...
  addDocument(ctx, spec, "doc1", "t1", "value one", (const char *)NULL);
  addDocument(ctx, spec, "doc2", "t1", "value two", (const char *)NULL);
  addDocument(ctx, spec, "doc3", "t1", "value three", (const char *)NULL);
  RedisModuleKey *kk = RedisModule_OpenKey(ctx, RMCK::RString("doc1"), REDISMODULE_READ);
  ASSERT_FALSE(kk == NULL);

  // Ensure the key has the correct properties
  RedisModuleString *vtmp = NULL;
  int rv = RedisModule_HashGet(kk, REDISMODULE_HASH_CFIELDS, "t1", &vtmp, NULL);
  ASSERT_EQ(REDISMODULE_OK, rv);
  ASSERT_STREQ("value one", RedisModule_StringPtrLen(vtmp, NULL));
  RedisModule_CloseKey(kk);
  RedisModule_FreeString(ctx, vtmp);

  AREQ *rr = AREQ_New();
  RMCK::ArgvList aggArgs(ctx, "*");
  rv = AREQ_Compile(rr, aggArgs, aggArgs.size(), &qerr);
  ASSERT_EQ(REDISMODULE_OK, rv) << QueryError_GetUserError(&qerr);
  ASSERT_FALSE(QueryError_HasError(&qerr));
  RedisSearchCtx *sctx = NewSearchCtxC(ctx, spec->specName, true);
  ASSERT_FALSE(sctx == NULL);
  rv = AREQ_ApplyContext(rr, sctx, &qerr);
  ASSERT_EQ(REDISMODULE_OK, rv);

  rv = AREQ_BuildPipeline(rr, &qerr);
  ASSERT_EQ(REDISMODULE_OK, rv) << QueryError_GetUserError(&qerr);

  auto rp = AREQ_RP(rr);
  ASSERT_FALSE(rp == NULL);

  SearchResult res = SearchResult_New();
  RLookup *lk = AGPLN_GetLookup(AREQ_AGGPlan(rr), NULL, AGPLN_GETLOOKUP_LAST);
  size_t count = 0;
  while ((rv = rp->Next(rp, &res)) == RS_RESULT_OK) {
    count++;
    // std::cerr << "Doc ID: " << res.docId << std::endl;
    // for (auto kk = lk->head; kk; kk = kk->next) {
    //   RSValue *vv = RLookup_GetItem(kk, &res.rowdata);
    //   if (vv != NULL) {
    //     std::cerr << "  " << kk->name << ": ";
    //     sds s = RSValue_DumpSds(vv);
    //     std::cerr << s << std::endl;
    //     sdsfree(s)
    //   }
    // }
    SearchResult_Clear(&res);
  }
  ASSERT_EQ(RS_RESULT_EOF, rv);
  ASSERT_EQ(3, count);

  SearchResult_Destroy(&res);
  AREQ_Free(rr);
  IndexSpec_Free(spec);
  args.clear();
  aggArgs.clear();
  RedisModule_FreeThreadSafeContext(ctx);
}

#endif // HAVE_RM_SCANCURSOR_CREATE

class RPMock : public ResultProcessor {
 public:
  size_t counter;
  const char **values;
  size_t numvals;
  RLookupKey *rkscore;
  RLookupKey *rkvalue;

  RPMock() {
    memset(this, 0, sizeof(*this));
  }
};

#define NUM_RESULTS 300000

class ReducerOptionsCXX : public ReducerOptions {
 private:
  std::vector<const char *> m_args;
  ArgsCursor m_ac;
  QueryError m_status;

 public:
  template <typename... T>
  ReducerOptionsCXX(const char *name, RLookup *lk, T... args) {
    memset((void *)this, 0, sizeof(*this));
    std::vector<T...> tmpvec{args...};
    m_args = std::move(tmpvec);
    ArgsCursor_InitCString(&m_ac, &m_args[0], m_args.size());
    this->name = name;
    this->args = &m_ac;
    this->status = &m_status;
    this->srclookup = lk;
  }
};

TEST_F(AggTest, testGroupBy) {
  QueryProcessingCtx qitr = {0};
  RPMock ctx;
  RLookup rk_in = {0};
  const char *values[] = {"foo", "bar", "baz", "foo"};
  ctx.values = values;
  ctx.numvals = sizeof(values) / sizeof(values[0]);
  ctx.rkscore = RLookup_GetKey_Write(&rk_in, "score", RLOOKUP_F_NOFLAGS);
  ctx.rkvalue = RLookup_GetKey_Write(&rk_in, "value", RLOOKUP_F_NOFLAGS);
  ctx.Next = [](ResultProcessor *rp, SearchResult *res) -> int {
    RPMock *p = (RPMock *)rp;
    if (p->counter >= NUM_RESULTS) {
      return RS_RESULT_EOF;
    }
    SearchResult_SetDocId(res, ++p->counter);

    RSValue *sval = RSValue_NewConstCString((char *)p->values[p->counter % p->numvals]);
    RSValue *scoreval = RSValue_NewNumber(p->counter);
    RLookup_WriteOwnKey(p->rkvalue, SearchResult_GetRowDataMut(res), sval);
    RLookup_WriteOwnKey(p->rkscore, SearchResult_GetRowDataMut(res), scoreval);
    //* res = * p->res;
    return RS_RESULT_OK;
  };

  QITR_PushRP(&qitr, &ctx);

  RLookup rk_out = {0};
  RLookupKey *v_out = RLookup_GetKey_Write(&rk_out, "value", RLOOKUP_F_NOFLAGS);
  RLookupKey *score_out = RLookup_GetKey_Write(&rk_out, "SCORE", RLOOKUP_F_NOFLAGS);
  RLookupKey *count_out = RLookup_GetKey_Write(&rk_out, "COUNT", RLOOKUP_F_NOFLAGS);

  Grouper *gr = Grouper_New((const RLookupKey **)&ctx.rkvalue, (const RLookupKey **)&v_out, 1);
  ASSERT_TRUE(gr != NULL);

  ArgsCursor args = {0};
  ReducerOptions opt = {0};
  opt.args = &args;
  Grouper_AddReducer(gr, RDCRCount_New(&opt), count_out);
  ReducerOptionsCXX sumOptions("SUM", &rk_in, "score");
  auto sumReducer = RDCRSum_New(&sumOptions);
  ASSERT_TRUE(sumReducer != NULL) << QueryError_GetUserError(sumOptions.status);
  Grouper_AddReducer(gr, sumReducer, score_out);
  SearchResult res = SearchResult_New();
  ResultProcessor *gp = Grouper_GetRP(gr);
  QITR_PushRP(&qitr, gp);

  while (gp->Next(gp, &res) == RS_RESULT_OK) {
    SearchResult_Clear(&res);
  }
  SearchResult_Destroy(&res);
  gp->Free(gp);
  RLookup_Cleanup(&rk_out);
  RLookup_Cleanup(&rk_in);
}

class ArrayGenerator : public ResultProcessor {
 public:
  RLookupKey *kvalue = NULL;
  std::vector<const char *> values = {"foo", "bar", "baz"};
  const size_t nvalues = 3;
  size_t counter = 0;

  ArrayGenerator() {
    memset(static_cast<ResultProcessor *>(this), 0, sizeof(ResultProcessor));
  }
};

TEST_F(AggTest, testGroupSplit) {
  QueryProcessingCtx qitr = {0};
  ArrayGenerator gen;
  RLookup lk_in = {0};
  RLookup lk_out = {0};
  gen.kvalue = RLookup_GetKey_Write(&lk_in, "value", RLOOKUP_F_NOFLAGS);
  RLookupKey *val_out = RLookup_GetKey_Write(&lk_out, "value", RLOOKUP_F_NOFLAGS);
  RLookupKey *count_out = RLookup_GetKey_Write(&lk_out, "COUNT", RLOOKUP_F_NOFLAGS);
  Grouper *gr = Grouper_New((const RLookupKey **)&gen.kvalue, (const RLookupKey **)&val_out, 1);
  ArgsCursor args = {0};
  ReducerOptions opt = {0};
  opt.args = &args;
  Grouper_AddReducer(gr, RDCRCount_New(&opt), count_out);
  SearchResult res = SearchResult_New();
  size_t ii = 0;

  QITR_PushRP(&qitr, &gen);

  ResultProcessor *gp = Grouper_GetRP(gr);
  gen.Next = [](ResultProcessor *rp, SearchResult *res) -> int {
    ArrayGenerator *p = static_cast<ArrayGenerator *>(rp);
    if (p->counter >= NUM_RESULTS) return RS_RESULT_EOF;
    SearchResult_SetDocId(res, ++p->counter);
    RLookup_WriteOwnKey(p->kvalue, SearchResult_GetRowDataMut(res),
                        RSValue_NewConstStringArray((char **)&p->values[0], p->values.size()));
    //* res = * p->res;
    return RS_RESULT_OK;
  };

  QITR_PushRP(&qitr, gp);

  while (gp->Next(gp, &res) == RS_RESULT_OK) {
    RSValue *rv = RLookup_GetItem(val_out, SearchResult_GetRowData(&res));
    ASSERT_FALSE(NULL == rv);
    ASSERT_FALSE(RSValue_IsNull(rv));
    ASSERT_TRUE(RSValue_IsString(rv));
    bool foundValue = false;
    for (auto s : gen.values) {
      if (!strcmp(RSValue_String_Get(rv, NULL), s)) {
        foundValue = true;
        break;
      }
    }
    ASSERT_TRUE(foundValue);
    SearchResult_Clear(&res);
  }
  SearchResult_Destroy(&res);
  gp->Free(gp);
  RLookup_Cleanup(&lk_in);
  RLookup_Cleanup(&lk_out);
}

#if 0
int testAggregatePlan() {
  CmdString *argv = CmdParser_NewArgListV(
      39, "FT.AGGREGATE", "idx", "foo bar", "APPLY", "@foo", "AS", "@bar", "GROUPBY", "2", "@foo",
      "@bar", "REDUCE", "count_distinct", "1", "@foo", "REDUCE", "count", "0", "AS", "num",
      "SORTBY", "4", "@foo", "ASC", "@bar", "DESC", "MAX", "5", "APPLY", "@num/3", "AS", "subnum",
      "APPLY", "timefmt(@subnum)", "AS", "datenum", "LIMIT", "0", "100");

  CmdArg *cmd = NULL;
  char *err;
  Aggregate_BuildSchema();
  CmdParser_ParseCmd(GetAggregateRequestSchema(), &cmd, argv, 39, &err, 1);
  ASSERT(!err);
  ASSERT(cmd);

  CmdArg_Print(cmd, 0);

  AggregatePlan plan;
  int rc = AggregatePlan_Build(&plan, cmd, &err);
  if (err) printf("%s\n", err);
  ASSERT(rc);

  ASSERT(!err);

  AggregatePlan_Print(&plan);

  AggregateSchema sc = AggregatePlan_GetSchema(&plan, NULL);
  for (size_t i = 0; i < array_len(sc); i++) {
    printf("%s => %d (%d)\n", sc[i].property, sc[i].type, sc[i].kind);
  }

  RETURN_TEST_SUCCESS
}
#endif

TEST_F(AggTest, AvoidingCompleteResultStructOpt) {
  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);

  auto scenario = [&](QEFlags flags, auto... args) -> bool {
    QueryError qerr = QueryError_Default();
    AREQ *rr = AREQ_New();
    AREQ_AddRequestFlags(rr, flags);
    RMCK::ArgvList aggArgs(ctx, "*", args...);
    int rv = AREQ_Compile(rr, aggArgs, aggArgs.size(), &qerr);
    EXPECT_EQ(REDISMODULE_OK, rv) << QueryError_GetUserError(&qerr);
    bool res = rr->searchopts.flags & Search_CanSkipRichResults;
    QueryError_ClearError(&qerr);
    AREQ_Free(rr);
    return res;
  };

  // Default search command, we have an implicit sorter by scores
  EXPECT_FALSE(scenario(QEXEC_F_IS_SEARCH, "LIMIT", "0", "100"));

  // Explicit sorting, no need for scores
  EXPECT_TRUE(scenario(QEXEC_F_IS_SEARCH, "SORTBY", "foo", "ASC"));
  // Explicit sorting, with explicit request for scores
  EXPECT_FALSE(scenario(QEXEC_F_IS_SEARCH, "WITHSCORES", "SORTBY", "foo", "ASC"));
  // Explicit sorting, with explicit request for scores in a different order
  EXPECT_FALSE(scenario(QEXEC_F_IS_SEARCH, "SORTBY", "foo", "ASC", "WITHSCORES"));
  // Requesting HIGHLIGHT, which requires rich results
  EXPECT_FALSE(scenario(QEXEC_F_IS_SEARCH, "SORTBY", "foo", "HIGHLIGHT", "FIELDS", "1", "foo"));

  // Default aggregate command, no need for scores
  EXPECT_TRUE(scenario(QEXEC_F_IS_AGGREGATE, "LIMIT", "0", "100"));
  // Explicit request for scores
  EXPECT_FALSE(scenario(QEXEC_F_IS_AGGREGATE, "ADDSCORES"));

  RedisModule_FreeThreadSafeContext(ctx);
}
