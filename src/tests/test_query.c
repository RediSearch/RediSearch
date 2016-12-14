#include <stdio.h>
#include "test_util.h"
#include "../query.h"
#include "../query_parser/tokenizer.h"
#include "time_sample.h"

void __queryStage_Print(QueryStage *qs, int depth);

int testQueryParser() {
  char *err = NULL;
  char *qt = strdup("(hello|world) \"another world\"");
  RedisSearchCtx ctx;
  Query *q = NewQuery(NULL, qt, strlen(qt), 0, 1, 0xff, 0, "en", DEFAULT_STOPWORDS);

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


// int testQueryTokenize() {
//   char *text = strdup("hello \"world wat\" wat");
//   QueryTokenizer qt = NewQueryTokenizer(text, strlen(text), DEFAULT_STOPWORDS);

//   char *expected[] = {"hello", NULL, "world", "wat", NULL, "wat"};
//   QueryTokenType etypes[] = {T_WORD,  T_QUOTE, T_WORD, T_WORD,
//                              T_QUOTE, T_WORD,  T_END};
//   int i = 0;
//   while (QueryTokenizer_HasNext(&qt)) {
//     QueryToken t = QueryTokenizer_Next(&qt);
//     printf("%d Token text: %.*s, token type %d\n", i, (int)t.len, t.s, t.type);

//     // ASSERT((t.s == NULL && expected[i] == NULL) ||  (t.s != NULL &&
//     // expected[i] &&
//     // !strcmp(t.s, expected[i])))
//     if (t.s != NULL) {
//       ASSERT(expected[i] != NULL)
//       ASSERT(!strncmp(expected[i], t.s, t.len))
//     }
//     ASSERT(t.type == etypes[i])
//     i++;
//     free((char *)t.s);

//     if (t.type == T_END) {
//       break;
//     }
//   }
//   free(text);
//   return 0;
// }


int main(int argc, char **argv) {

  // LOGGING_INIT(L_INFO);
  TESTFUNC(testQueryParser);
}