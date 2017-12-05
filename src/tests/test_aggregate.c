#include <stdio.h>
#include <assert.h>
#include <aggregate/aggregate.h>
#include <aggregate/reducer.h>
#include "test_util.h"

struct mockProcessorCtx {
  int counter;
  char **values;
  int numvals;
  SearchResult *res;
};

#define NUM_RESULTS 1000

int mock_Next(ResultProcessorCtx *ctx, SearchResult *res) {

  struct mockProcessorCtx *p = ctx->privdata;
  if (p->counter >= NUM_RESULTS) return RS_RESULT_EOF;

  p->res->docId = ++p->counter;
  // printf("%s\n", p->values[p->counter % p->numvals]);
  RSFieldMap_Set(&p->res->fields, "value", RS_CStringValStatic(p->values[p->counter % p->numvals]));
  *res = *p->res;
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

  ResultProcessor *gr = NewGrouper(mp, "value", NewCounter(), NULL);

  SearchResult *res = NewSearchResult();
  while (ResultProcessor_Next(gr, res, 0) != RS_RESULT_EOF) {
    RSFieldMap_Print(res->fields);
    printf("\n");
    SearchResult_Free(res);
    res = NewSearchResult();
  }
  gr->Free(gr);
  RETURN_TEST_SUCCESS;
}

TEST_MAIN({

  TESTFUNC(testGroupBy);

})
