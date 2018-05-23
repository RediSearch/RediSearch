#include <stdio.h>
#include <assert.h>
#include <aggregate/aggregate.h>
#include <aggregate/reducer.h>
#include "test_util.h"
#include "time_sample.h"
#include <util/arr.h>
#include <rmutil/alloc.h>

struct mockProcessorCtx {
  int counter;
  char **values;
  int numvals;
  SearchResult *res;
};

#define NUM_RESULTS 300000

int mock_Next(ResultProcessorCtx *ctx, SearchResult *res) {

  struct mockProcessorCtx *p = ctx->privdata;
  if (p->counter >= NUM_RESULTS) return RS_RESULT_EOF;

  res->docId = ++p->counter;

  // printf("%s\n", p->values[p->counter % p->numvals]);
  RSFieldMap_Set(&res->fields, "value", RS_ConstStringValC(p->values[p->counter % p->numvals]));
  RSFieldMap_Set(&res->fields, "score", RS_NumVal((double)p->counter));
  //* res = * p->res;
  return RS_RESULT_OK;
}

int testGroupBy() {
  char *values[] = {"foo", "bar", "baz"};
  struct mockProcessorCtx ctx = {
      0,
      values,
      3,
      NewSearchResult(),
  };

  ResultProcessor *mp = NewResultProcessor(NULL, &ctx);
  mp->Next = mock_Next;
  mp->Free = NULL;
  RSMultiKey *keys = RS_NewMultiKeyVariadic(2, "value", "val");

  Grouper *gr = NewGrouper(keys, NULL);
  Grouper_AddReducer(gr, NewCount(NULL, "countie"));
  Grouper_AddReducer(gr, NewSum(NULL, "score", NULL));

  ResultProcessor *gp = NewGrouperProcessor(gr, mp);
  SearchResult *res = NewSearchResult();
  res->fields = NULL;
  TimeSample ts;
  TimeSampler_Start(&ts);
  while (ResultProcessor_Next(gp, res, 0) != RS_RESULT_EOF) {
    RSFieldMap_Print(res->fields);
    RSFieldMap_Reset(res->fields);
    // res->fields->len = 0;
    // res->fields = NULL;
    printf("\n");
  }
  SearchResult_Free(res);
  // res = NewSearchResult();
  TimeSampler_End(&ts);
  printf("%d iterations in %fms, %fns/iter", NUM_RESULTS, TimeSampler_DurationSec(&ts) * 1000,
         (double)(TimeSampler_DurationNS(&ts)) / (double)NUM_RESULTS);
  gp->Free(gp);
  RETURN_TEST_SUCCESS;
}

int mock_Next_Arr(ResultProcessorCtx *ctx, SearchResult *res) {

  struct mockProcessorCtx *p = ctx->privdata;
  if (p->counter >= NUM_RESULTS) return RS_RESULT_EOF;

  res->docId = ++p->counter;
  res->fields = NULL;
  // printf("%s\n", p->values[p->counter % p->numvals]);
  RSFieldMap_Set(&res->fields, "value", RS_StringArrayT(p->values, p->numvals, RSString_Const));
  //* res = * p->res;
  return RS_RESULT_OK;
}

int testPlanSchema() {

  RSSortingTable *tbl = NewSortingTable();
  RSSortingTable_Add(tbl, "txt", RSValue_String);
  RSSortingTable_Add(tbl, "num", RSValue_Number);

  const char *args[] = {"FT.AGGREGATE", "idx",    "*",       "VERBATIM",    "APPLY", "@txt",
                        "AS",           "txt2",   "APPLY",   "upper(@txt)", "AS",    "upper",
                        "APPLY",        "@num/2", "AS",      "halfnum",     "APPLY", "sqrt(@num)",
                        "AS",           "sqrt",   "GROUPBY", "2",           "@txt",  "@num",
                        "reduce",       "count",  "0",       "as",          "count", "reduce",
                        "tolist",       "1",      "@txt",    "as",          "list"};
  int len = sizeof(args) / sizeof(char *);
  CmdString *argv = CmdParser_NewArgListC(args, len);
  CmdArg *cmd = NULL;
  char *err;
  Aggregate_BuildSchema();
  CmdParser_ParseCmd(GetAggregateRequestSchema(), &cmd, argv, len, &err, 1);
  if (err) puts(err);
  ASSERT(!err);
  ASSERT(cmd);

  AggregatePlan plan;
  int rc = AggregatePlan_Build(&plan, cmd, &err);
  if (err) puts(err);
  ASSERT(rc);
  ASSERT(!err);

  AggregateSchema sc = AggregatePlan_GetSchema(&plan, tbl);

  AggregateProperty expected[] = {
      {"txt", RSValue_String, Property_Field},
      {"txt2", RSValue_String, Property_Projection},
      {"upper", RSValue_String, Property_Projection},
      {"num", RSValue_Number, Property_Field},
      {"halfnum", RSValue_Number, Property_Projection},
      {"sqrt", RSValue_Number, Property_Projection},
      {"count", RSValue_Number, Property_Aggregate},
      {"list", RSValue_Array, Property_Aggregate},

  };
  for (int i = 0; i < array_len(sc); i++) {
    ASSERT_STRING_EQ(expected[i].property, sc[i].property);
    ASSERT_EQUAL(expected[i].kind, sc[i].kind);
    ASSERT_EQUAL(expected[i].type, sc[i].type);

    printf("%s, %s, %d\n", sc[i].property, RSValue_TypeName(sc[i].type), sc[i].kind);
  }
  array_free(sc);
  RETURN_TEST_SUCCESS;
}
int testGroupSplit() {

  char *values[] = {("foo"), ("bar"), ("baz")};
  struct mockProcessorCtx ctx = {
      0,
      values,
      3,
      NewSearchResult(),
  };

  ResultProcessor *mp = NewResultProcessor(NULL, &ctx);
  mp->Next = mock_Next_Arr;
  mp->Free = NULL;
  RSMultiKey *keys = RS_NewMultiKeyVariadic(1, "value");

  Grouper *gr = NewGrouper(keys, NULL);
  Grouper_AddReducer(gr, NewCount(NULL, "countie"));

  ResultProcessor *gp = NewGrouperProcessor(gr, mp);
  SearchResult *res = NewSearchResult();
  res->fields = NULL;
  TimeSample ts;
  TimeSampler_Start(&ts);
  int i = 0;
  while (ResultProcessor_Next(gp, res, 0) != RS_RESULT_EOF) {
    RSFieldMap_Print(res->fields);
    RSValue *rv = RSFieldMap_Get(res->fields, "value");
    ASSERT(!RSValue_IsNull(rv));
    ASSERT(RSValue_IsString(rv));
    ASSERT((!strcmp(rv->strval.str, values[0]) || !strcmp(rv->strval.str, values[1]) ||
            !strcmp(rv->strval.str, values[2])))
    ASSERT_EQUAL(NUM_RESULTS, RSFieldMap_Get(res->fields, "countie")->numval);
    RSFieldMap_Reset(res->fields);
  }
  SearchResult_Free(res);
  // res = NewSearchResult();
  TimeSampler_End(&ts);
  printf("%d iterations in %fms, %fns/iter", NUM_RESULTS, TimeSampler_DurationSec(&ts) * 1000,
         (double)(TimeSampler_DurationNS(&ts)) / (double)NUM_RESULTS);
  gp->Free(gp);
  RETURN_TEST_SUCCESS;
}

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

/*
int testDistribute() {
  CmdString *argv = CmdParser_NewArgListV(
      22, "FT.AGGREGATE", "idx", "foo", "GROUPBY", "1", "@bar", "REDUCE", "AVG", "1", "@foo",
"AS", "num", "REDUCE", "MAX", "1", "@bar", "AS", "sum_bar", "SORTBY", "2", "@num", "DESC");

  CmdArg *cmd = NULL;
  char *err;
  Aggregate_BuildSchema();
  CmdParser_ParseCmd(GetAggregateRequestSchema(), &cmd, argv, 22, &err, 1);
  printf("%s\n", err);
  ASSERT(!err);
  ASSERT(cmd);

  CmdArg_Print(cmd, 0);

  AggregatePlan plan;
  int rc = AggregatePlan_Build(&plan, cmd, &err);
  if (err) printf("%s\n", err);
  ASSERT(rc);

  ASSERT(!err);

  AggregatePlan_Print(&plan);

  printf("----------------\n");
  printf("----------------\n");

  AggregatePlan distro;
  rc = AggregatePlan_MakeDistributed(&plan, &distro);
  ASSERT(rc);
  printf("----------------\n");
  AggregatePlan_Print(&plan);
  printf("----------------\n");

  AggregatePlan_Print(&distro);
  AggregatePlan_Free(&plan);

  RETURN_TEST_SUCCESS;
}

int testRevertToBasic() {
  CmdString *argv =
      CmdParser_NewArgListV(22, "FT.AGGREGATE", "idx", "foo", "GROUPBY", "1", "@bar", "REDUCE",
                            "COUNT_DISTINCT", "1", "@foo", "AS", "num", "REDUCE", "MAX", "1",
                            "@bar", "AS", "sum_bar", "SORTBY", "2", "@num", "DESC");

  CmdArg *cmd = NULL;
  char *err;
  Aggregate_BuildSchema();
  CmdParser_ParseCmd(GetAggregateRequestSchema(), &cmd, argv, 22, &err, 1);
  printf("%s\n", err);
  ASSERT(!err);
  ASSERT(cmd);

  CmdArg_Print(cmd, 0);

  AggregatePlan plan;
  int rc = AggregatePlan_Build(&plan, cmd, &err);
  if (err) printf("%s\n", err);
  ASSERT(rc);

  ASSERT(!err);

  AggregatePlan_Print(&plan);

  printf("----------------\n");
  printf("----------------\n");

  AggregatePlan distro;
  rc = AggregatePlan_MakeDistributed(&plan, &distro);
  ASSERT(rc);
  printf("----------------\n");
  AggregatePlan_Print(&plan);
  printf("----------------\n");

  AggregatePlan_Print(&distro);
  AggregatePlan_Free(&plan);

  RETURN_TEST_SUCCESS;
}
*/
TEST_MAIN({
  RMUTil_InitAlloc();

  // TESTFUNC(testRevertToBasic);
  TESTFUNC(testGroupSplit);
  TESTFUNC(testGroupBy);
  TESTFUNC(testAggregatePlan);
  TESTFUNC(testPlanSchema);
  // TESTFUNC(testDistribute);
})
