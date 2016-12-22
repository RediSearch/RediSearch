#ifndef __QUERY_PARSER_PARSE_H__
#define __QUERY_PARSER_PARSE_H__

#include "tokenizer.h"
#include "../query.h"
//#include "../rmutil/alloc.h"

typedef struct {
  Query *q;
  QueryNode *root;
  int ok;
  char *errorMsg;
} parseCtx;

#endif // !__QUERY_PARSER_PARSE_H__