#include <stdio.h>
#include <assert.h>
#include <aggregate/aggregate.h>
#include <aggregate/reducer.h>
#include "test_util.h"
#include "time_sample.h"
#include <util/arr.h>

struct mockProcessorCtx {
  int counter;
  char **values;
  int numvals;
  SearchResult *res;
};

#define NUM_RESULTS 3000000

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
      22, "FT.AGGREGATE", "idx", "foo", "GROUPBY", "1", "@bar", "REDUCE", "AVG", "1", "@foo", "AS",
      "num", "REDUCE", "MAX", "1", "@bar", "AS", "sum_bar", "SORTBY", "2", "@num", "DESC");

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
  // TESTFUNC(testRevertToBasic);
  TESTFUNC(testGroupBy);
  TESTFUNC(testAggregatePlan);
  // TESTFUNC(testDistribute);
})
