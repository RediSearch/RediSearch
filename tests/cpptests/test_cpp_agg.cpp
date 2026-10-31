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
#include "search_result_ffi.h"
#include "redismock/redismock.h"
#include "redismock/util.h"
#include "redismock/internal.h"
#include "spec.h"
#include "common.h"
#include "module.h"
#include "version.h"
#include "search_result.h"

#include <vector>
#include <array>
#include <iostream>
#include <cstdarg>
#include <pthread.h>
#include <string>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

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
  auto spec = Indexes_CreateNewSpec(ctx, args, args.size(), &qerr);
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

  RMCK::ArgvList aggArgs(ctx, "*");
  AREQ *rr = AREQ_New(aggArgs, aggArgs.size());
  rv = AREQ_Compile(rr, ctx, 0, false, &qerr);
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

TEST_F(AggTest, GroupBySealsEveryLookup) {
  RMCK::Context ctx;
  RMCK::ArgvList args(ctx, "*", "APPLY", "1", "AS", "first", "GROUPBY", "1", "@first", "REDUCE",
                      "COUNT", "0", "AS", "count", "APPLY", "@count + 1", "AS", "count", "GROUPBY",
                      "0", "REDUCE", "SUM", "1", "@count", "AS", "total");
  QueryError status = QueryError_Default();
  AREQ *req = AREQ_New(args, args.size());
  ASSERT_EQ(AREQ_Compile(req, ctx, 0, false, &status), REDISMODULE_OK);
  AREQ_AddRequestFlags(req, QEXEC_F_IS_COORDINATOR);
  ASSERT_EQ(AREQ_BuildPipeline(req, &status), REDISMODULE_OK) << QueryError_GetUserError(&status);

  AGGPlan *plan = AREQ_AGGPlan(req);
  size_t count = 0;
  for (const DLLIST_node *node = plan->steps.next; node != &plan->steps; node = node->next) {
    const PLN_BaseStep *step = DLLIST_ITEM(node, PLN_BaseStep, llnodePln);
    if (step->type != PLN_T_GROUP) {
      continue;
    }
    RLookup *lookup = AGPLN_GetLookup(plan, step, AGPLN_GETLOOKUP_PREV);
    EXPECT_DEATH(RLookup_SetCache(lookup, nullptr), "sealed");
    ++count;
  }
  EXPECT_EQ(count, 2);
  RLookup *last = AGPLN_GetLookup(plan, nullptr, AGPLN_GETLOOKUP_LAST);
  EXPECT_DEATH(RLookup_SetCache(last, nullptr), "sealed");
  // The seal protects existing keys, but coordinator replies can still add fields.
  EXPECT_NE(RLookup_GetKey_Write(last, "late", RLOOKUP_F_NOFLAGS), nullptr);
  AREQ_Free(req);
  QueryError_ClearError(&status);
}

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
  RLookup rk_in = RLookup_New();
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

    const char *str = p->values[p->counter % p->numvals];
    RSValue *sval = RSValue_NewBorrowedString(str, strlen(str));
    RSValue *scoreval = RSValue_NewNumber(p->counter);
    RLookup_WriteOwnKey(p->rkvalue, SearchResult_GetRowDataMut(res), sval);
    RLookup_WriteOwnKey(p->rkscore, SearchResult_GetRowDataMut(res), scoreval);
    //* res = * p->res;
    return RS_RESULT_OK;
  };

  QITR_PushRP(&qitr, &ctx);

  RLookup rk_out = RLookup_New();
  RLookupKey *v_out = RLookup_GetKey_Write(&rk_out, "value", RLOOKUP_F_NOFLAGS);
  RLookupKey *score_out = RLookup_GetKey_Write(&rk_out, "SCORE", RLOOKUP_F_NOFLAGS);
  RLookupKey *count_out = RLookup_GetKey_Write(&rk_out, "COUNT", RLOOKUP_F_NOFLAGS);

  Grouper *gr = Grouper_New((const RLookupKey **)&ctx.rkvalue, (const RLookupKey **)&v_out, 1,
                            GroupByLimits_Default(DEFAULT_MAX_AGGREGATE_GROUPS));
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
  RLookup lk_in = RLookup_New();
  RLookup lk_out = RLookup_New();
  gen.kvalue = RLookup_GetKey_Write(&lk_in, "value", RLOOKUP_F_NOFLAGS);
  RLookupKey *val_out = RLookup_GetKey_Write(&lk_out, "value", RLOOKUP_F_NOFLAGS);
  RLookupKey *count_out = RLookup_GetKey_Write(&lk_out, "COUNT", RLOOKUP_F_NOFLAGS);
  Grouper *gr = Grouper_New((const RLookupKey **)&gen.kvalue, (const RLookupKey **)&val_out, 1,
                            GroupByLimits_Default(DEFAULT_MAX_AGGREGATE_GROUPS));
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
    uint32_t sz = p->values.size();
    char **strs = (char **)&p->values[0];
    RSValue **arr = RSValue_NewArrayBuilder(sz);
    for (uint32_t i = 0; i < sz; i++) {
      arr[i] = RSValue_NewBorrowedString(strs[i], strlen(strs[i]));
    }
    RSValue *array = RSValue_NewArrayFromBuilder(arr, sz);
    RLookup_WriteOwnKey(p->kvalue, SearchResult_GetRowDataMut(res), array);
    //* res = * p->res;
    return RS_RESULT_OK;
  };

  QITR_PushRP(&qitr, gp);

  while (gp->Next(gp, &res) == RS_RESULT_OK) {
    RSValue *rv = RLookupRow_Get(val_out, SearchResult_GetRowData(&res));
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

// The widest GROUPBY the argument parser accepts (MAX_GROUPBY_PROPERTIES).
static constexpr size_t WIDEST_GROUPBY = (1 << 16) - 1;
// The default secondary-thread stack size on macOS; musl's default is smaller.
static constexpr size_t SMALL_STACK = 512 * 1024;
// Inaccessible memory right below the small stack. It is wide enough that a
// frame running past the stack faults in it instead of skipping a page-sized
// guard into whatever is mapped further down.
static constexpr size_t STACK_GUARD = 64 << 20;

/** Run `fn(arg)` on a thread with a SMALL_STACK-byte stack. Exits on setup failure. */
static void runOnSmallStack(void *(*fn)(void *), void *arg) {
  char *map = static_cast<char *>(mmap(NULL, STACK_GUARD + SMALL_STACK, PROT_READ | PROT_WRITE,
                                       MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0));
  pthread_attr_t attr;
  pthread_t thread;
  if (map == MAP_FAILED || mprotect(map, STACK_GUARD, PROT_NONE) != 0 ||
      pthread_attr_init(&attr) != 0 ||
      pthread_attr_setstack(&attr, map + STACK_GUARD, SMALL_STACK) != 0 ||
      pthread_create(&thread, &attr, fn, arg) != 0 || pthread_join(thread, NULL) != 0) {
    fprintf(stderr, "could not run a thread on a %zu KiB stack\n", SMALL_STACK / 1024);
    _exit(2);
  }
  pthread_attr_destroy(&attr);
  munmap(map, STACK_GUARD + SMALL_STACK);
}

struct WidePipelineBuild {
  AREQ *req;
  QueryError status = QueryError_Default();
  int rc = REDISMODULE_OK;
};

static void *buildWidePipeline(void *arg) {
  auto *build = static_cast<WidePipelineBuild *>(arg);
  build->rc = AREQ_BuildPipeline(build->req, &build->status);
  return NULL;
}

/** Build the pipeline of the widest GROUPBY on a small stack. Exits 0 on the expected result. */
static void buildWidestPipelineOnSmallStack() {
  RMCK::Context ctx;
  // Repeating one property passes argument parsing; the repetition is only
  // rejected while the grouper is built, after its per-property storage exists.
  std::vector<std::string> argv = {"*", "APPLY", "1", "AS", "f", "GROUPBY",
                                   std::to_string(WIDEST_GROUPBY)};
  argv.insert(argv.end(), WIDEST_GROUPBY, "@f");
  argv.insert(argv.end(), {"REDUCE", "COUNT", "0", "AS", "n"});
  RMCK::ArgvList args(ctx, argv);
  WidePipelineBuild build{AREQ_New(args, args.size())};
  if (AREQ_Compile(build.req, ctx, 0, false, &build.status) != REDISMODULE_OK) {
    fprintf(stderr, "compile failed: %s\n", QueryError_GetUserError(&build.status));
    _exit(1);
  }
  AREQ_AddRequestFlags(build.req, QEXEC_F_IS_COORDINATOR);
  runOnSmallStack(buildWidePipeline, &build);
  if (build.rc != REDISMODULE_ERR ||
      QueryError_GetCode(&build.status) != QUERY_ERROR_CODE_DUP_FIELD) {
    fprintf(stderr, "unexpected build result %d: %s\n", build.rc,
            QueryError_GetUserError(&build.status));
    _exit(1);
  }
  _exit(0);
}

class WideRowGenerator : public ResultProcessor {
 public:
  RLookupKey *multi = NULL;
  RLookupKey *single = NULL;
  size_t counter = 0;

  WideRowGenerator() {
    memset(static_cast<ResultProcessor *>(this), 0, sizeof(ResultProcessor));
  }
};

struct WideGroupByRun {
  Grouper *grouper;
  size_t groups = 0;
};

static void *runWideGroupBy(void *arg) {
  auto *run = static_cast<WideGroupByRun *>(arg);
  ResultProcessor *gp = Grouper_GetRP(run->grouper);
  SearchResult res = SearchResult_New();
  while (gp->Next(gp, &res) == RS_RESULT_OK) {
    ++run->groups;
    SearchResult_Clear(&res);
  }
  SearchResult_Destroy(&res);
  return NULL;
}

/** Group one row by the widest GROUPBY on a small stack. Exits 0 on the expected result. */
static void groupWidestRowOnSmallStack() {
  QueryProcessingCtx qitr = {0};
  WideRowGenerator gen;
  RLookup lk_in = RLookup_New();
  RLookup lk_out = RLookup_New();
  gen.multi = RLookup_GetKey_Write(&lk_in, "multi", RLOOKUP_F_NOFLAGS);
  gen.single = RLookup_GetKey_Write(&lk_in, "single", RLOOKUP_F_NOFLAGS);
  RLookupKey *multi_out = RLookup_GetKey_Write(&lk_out, "multi", RLOOKUP_F_NOFLAGS);
  RLookupKey *single_out = RLookup_GetKey_Write(&lk_out, "single", RLOOKUP_F_NOFLAGS);

  // One multi-valued key followed by scalar ones, so the row is added to two groups.
  std::vector<const RLookupKey *> srckeys(WIDEST_GROUPBY, gen.single);
  std::vector<const RLookupKey *> dstkeys(WIDEST_GROUPBY, single_out);
  srckeys[0] = gen.multi;
  dstkeys[0] = multi_out;

  gen.Next = [](ResultProcessor *rp, SearchResult *res) -> int {
    auto *p = static_cast<WideRowGenerator *>(rp);
    if (p->counter++ > 0) return RS_RESULT_EOF;
    SearchResult_SetDocId(res, p->counter);
    RSValue **arr = RSValue_NewArrayBuilder(2);
    arr[0] = RSValue_NewBorrowedString("x", 1);
    arr[1] = RSValue_NewBorrowedString("y", 1);
    RLookup_WriteOwnKey(p->multi, SearchResult_GetRowDataMut(res),
                        RSValue_NewArrayFromBuilder(arr, 2));
    RLookup_WriteOwnKey(p->single, SearchResult_GetRowDataMut(res),
                        RSValue_NewBorrowedString("s", 1));
    return RS_RESULT_OK;
  };
  QITR_PushRP(&qitr, &gen);

  Grouper *gr = Grouper_New(srckeys.data(), dstkeys.data(), WIDEST_GROUPBY,
                            GroupByLimits_Default(DEFAULT_MAX_AGGREGATE_GROUPS));
  ArgsCursor args = {0};
  ReducerOptions opt = {0};
  opt.args = &args;
  RLookupKey *count_out = RLookup_GetKey_Write(&lk_out, "COUNT", RLOOKUP_F_NOFLAGS);
  Grouper_AddReducer(gr, RDCRCount_New(&opt), count_out);
  QITR_PushRP(&qitr, Grouper_GetRP(gr));

  WideGroupByRun run{gr};
  runOnSmallStack(runWideGroupBy, &run);
  if (run.groups != 2) {
    fprintf(stderr, "expected 2 groups, got %zu\n", run.groups);
    _exit(1);
  }
  _exit(0);
}

// How the last small-stack child ended, recorded by `exitedCleanly`.
static int smallStackChildStatus;

static bool exitedCleanly(int status) {
  smallStackChildStatus = status;
  return WIFEXITED(status) && WEXITSTATUS(status) == 0;
}

static std::string describeSmallStackChild(const char *stage) {
  int status = smallStackChildStatus;
  if (WIFSIGNALED(status)) {
    return "GROUPBY over " + std::to_string(WIDEST_GROUPBY) + " properties overflowed a " +
           std::to_string(SMALL_STACK / 1024) + " KiB thread stack while " + stage +
           " (killed by signal " + std::to_string(WTERMSIG(status)) + ")";
  }
  return std::string("GROUPBY over ") + std::to_string(WIDEST_GROUPBY) + " properties " +
         "gave an unexpected result while " + stage + " (exit code " +
         std::to_string(WEXITSTATUS(status)) + ", see the child's output above)";
}

// The stack must not grow with the number of GROUPBY properties, neither while
// the pipeline is built nor while rows are added to groups. Each stage runs in a
// child process, so an overflow is reported as a failure instead of a crash.
TEST_F(AggTest, testGroupByWidestOnSmallStack) {
  const std::string style = GTEST_FLAG_GET(death_test_style);
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  EXPECT_EXIT(buildWidestPipelineOnSmallStack(), exitedCleanly, "")
      << describeSmallStackChild("building the pipeline");
  EXPECT_EXIT(groupWidestRowOnSmallStack(), exitedCleanly, "")
      << describeSmallStackChild("grouping a row");
  GTEST_FLAG_SET(death_test_style, style);
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
    RMCK::ArgvList aggArgs(ctx, "*", args...);
    AREQ *rr = AREQ_New(aggArgs, aggArgs.size());
    AREQ_AddRequestFlags(rr, flags);
    int rv = AREQ_Compile(rr, ctx, 0, false, &qerr);
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
