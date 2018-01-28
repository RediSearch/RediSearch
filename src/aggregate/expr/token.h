#ifndef RS_AGGREGATE_TOKEN_H_
#define RS_AGGREGATE_TOKEN_H_
#include <stdlib.h>

/* A query-specific tokenizer, that reads symbols like quots, pipes, etc */
typedef struct {
  const char *text;
  size_t len;
  char *pos;
} ExprParseCtx;

/* A token in the process of parsing a query. Unlike the document tokenizer,  it
works iteratively and is not callback based.  */
typedef struct {
  char *s;
  int len;
  int pos;
  double numval;
} RSExprToken;
#endif