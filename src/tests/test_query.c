#include <stdio.h>
#include "test_util.h"
#include "../query.h"
#include "../query_parser/tokenizer.h"
#include "time_sample.h"

void __queryStage_Print(QueryStage *qs, int depth);

int testQueryParser() {
  char *err = NULL;
  char *qt = strdup("(hello|world) \"another world\" sdsd wa");
  RedisSearchCtx ctx;
  Query *q = NewQuery(NULL, qt, strlen(qt), 0, 1, 0xff, 0, "en");

  QueryStage *n = Query_Parse(q, &err);

  if (err) 
    FAIL("Error parsing query: %s", err);
  __queryStage_Print(n, 0);
  ASSERT(err == NULL);
  ASSERT(n != NULL);
  ASSERT(n->op == Q_INTERSECT);
  ASSERT(n->nchildren == 2);
  ASSERT(n->children[0]->op == Q_UNION);
  ASSERT(n->children[1]->op == Q_EXACT);
  
  

 TIME_SAMPLE_RUN_LOOP(10000, { Query_Parse(q, &err); });
  return 0;
}

int main(int argc, char **argv) {

  // LOGGING_INIT(L_INFO);
  TESTFUNC(testQueryParser);
}