#include <gtest/gtest.h>
#include <aggregate/aggregate.h>
#include "redismock/redismock.h"
#include "redismock/util.h"
#include "redismock/internal.h"
#include "spec.h"
#include "common.h"
#include <module.h>
#include <version.h>
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
  QueryError qerr = {QueryErrorCode(0)};

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
  ASSERT_EQ(REDISMODULE_OK, rv) << QueryError_GetError(&qerr);
  ASSERT_FALSE(QueryError_HasError(&qerr));
  RedisSearchCtx *sctx = NewSearchCtxC(ctx, spec->name, true);
  ASSERT_FALSE(sctx == NULL);
  rv = AREQ_ApplyContext(rr, sctx, &qerr);
  ASSERT_EQ(REDISMODULE_OK, rv);

  rv = AREQ_BuildPipeline(rr, 0, &qerr);
  ASSERT_EQ(REDISMODULE_OK, rv) << QueryError_GetError(&qerr);

  auto rp = AREQ_RP(rr);
  ASSERT_FALSE(rp == NULL);

  SearchResult res = {0};
  RLookup *lk = AGPLN_GetLookup(&rr->ap, NULL, AGPLN_GETLOOKUP_LAST);
  size_t count = 0;
  while ((rv = rp->Next(rp, &res)) == RS_RESULT_OK) {
    count++;
    // std::cerr << "Doc ID: " << res.docId << std::endl;
    // for (auto kk = lk->head; kk; kk = kk->next) {
    //   RSValue *vv = RLookup_GetItem(kk, &res.rowdata);
    //   if (vv != NULL) {
    //     std::cerr << "  " << kk->name << ": ";
    //     RSValue_Print(vv);
    //     std::cerr << std::endl;
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
  QueryIterator qitr = {0};
  RPMock ctx;
  RLookup rk_in = {0};
  const char *values[] = {"foo", "bar", "baz", "foo"};
  ctx.values = values;
  ctx.numvals = sizeof(values) / sizeof(values[0]);
  ctx.rkscore = RLookup_GetKey(&rk_in, "score", RLOOKUP_F_OCREAT);
  ctx.rkvalue = RLookup_GetKey(&rk_in, "value", RLOOKUP_F_OCREAT);
  ctx.Next = [](ResultProcessor *rp, SearchResult *res) -> int {
    RPMock *p = (RPMock *)rp;
    if (p->counter >= NUM_RESULTS) {
      return RS_RESULT_EOF;
    }
    res->docId = ++p->counter;

    RSValue *sval = RS_ConstStringValC((char *)p->values[p->counter % p->numvals]);
    RSValue *scoreval = RS_NumVal(p->counter);
    RLookup_WriteOwnKey(p->rkvalue, &res->rowdata, sval);
    RLookup_WriteOwnKey(p->rkscore, &res->rowdata, scoreval);
    //* res = * p->res;
    return RS_RESULT_OK;
  };

  QITR_PushRP(&qitr, &ctx);

  RLookup rk_out = {0};
  RLookupKey *v_out = RLookup_GetKey(&rk_out, "value", RLOOKUP_F_OCREAT);
  RLookupKey *score_out = RLookup_GetKey(&rk_out, "SCORE", RLOOKUP_F_OCREAT);
  RLookupKey *count_out = RLookup_GetKey(&rk_out, "COUNT", RLOOKUP_F_OCREAT);

  Grouper *gr = Grouper_New((const RLookupKey **)&ctx.rkvalue, (const RLookupKey **)&v_out, 1);
  ASSERT_TRUE(gr != NULL);

  ArgsCursor args = {0};
  ReducerOptions opt = {0};
  opt.args = &args;
  Grouper_AddReducer(gr, RDCRCount_New(&opt), count_out);
  ReducerOptionsCXX sumOptions("SUM", &rk_in, "score");
  auto sumReducer = RDCRSum_New(&sumOptions);
  ASSERT_TRUE(sumReducer != NULL) << QueryError_GetError(sumOptions.status);
  Grouper_AddReducer(gr, sumReducer, score_out);
  SearchResult res = {0};
  ResultProcessor *gp = Grouper_GetRP(gr);
  QITR_PushRP(&qitr, gp);

  while (gp->Next(gp, &res) == RS_RESULT_OK) {
    // RLookupRow_Dump(&res.rowdata);
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
  QueryIterator qitr = {0};
  ArrayGenerator gen;
  RLookup lk_in = {0};
  RLookup lk_out = {0};
  gen.kvalue = RLookup_GetKey(&lk_in, "value", RLOOKUP_F_OCREAT);
  RLookupKey *val_out = RLookup_GetKey(&lk_out, "value", RLOOKUP_F_OCREAT);
  RLookupKey *count_out = RLookup_GetKey(&lk_out, "COUNT", RLOOKUP_F_OCREAT);
  Grouper *gr = Grouper_New((const RLookupKey **)&gen.kvalue, (const RLookupKey **)&val_out, 1);
  ArgsCursor args = {0};
  ReducerOptions opt = {0};
  opt.args = &args;
  Grouper_AddReducer(gr, RDCRCount_New(&opt), count_out);
  SearchResult res = {0};
  size_t ii = 0;

  QITR_PushRP(&qitr, &gen);

  ResultProcessor *gp = Grouper_GetRP(gr);
  gen.Next = [](ResultProcessor *rp, SearchResult *res) -> int {
    ArrayGenerator *p = static_cast<ArrayGenerator *>(rp);
    if (p->counter >= NUM_RESULTS) return RS_RESULT_EOF;
    res->docId = ++p->counter;
    RLookup_WriteOwnKey(p->kvalue, &res->rowdata,
                        RS_StringArrayT((char **)&p->values[0], p->values.size(), RSString_Const));
    //* res = * p->res;
    return RS_RESULT_OK;
  };

  QITR_PushRP(&qitr, gp);

  while (gp->Next(gp, &res) == RS_RESULT_OK) {
    // RLookupRow_Dump(&res.rowdata);
    RSValue *rv = RLookup_GetItem(val_out, &res.rowdata);
    ASSERT_FALSE(NULL == rv);
    ASSERT_FALSE(RSValue_IsNull(rv));
    ASSERT_TRUE(RSValue_IsString(rv));
    bool foundValue = false;
    for (auto s : gen.values) {
      if (!strcmp(rv->strval.str, s)) {
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
