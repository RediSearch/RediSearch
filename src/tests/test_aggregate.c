#include <stdio.h>
#include <assert.h>
#include <aggregate/aggregate.h>
#include <aggregate/reducer.h>
#include "test_util.h"
#include "time_sample.h"
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

TEST_MAIN({

  TESTFUNC(testGroupBy);

})
