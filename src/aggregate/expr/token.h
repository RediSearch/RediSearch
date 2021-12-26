#pragma once

#include "expression.h"

#include <stdlib.h>

// A query-specific tokenizer, that reads symbols like quots, pipes, etc

struct RSExprParseCtx {
  const char *raw;
  size_t len;
  char *pos;

  char *errorMsg;

  RSExpr *root;
  int ok;
};

// A token in the process of parsing a query.
// Unlike the document tokenizer,  it works iteratively and is not callback based.

struct RSExprToken {
  const char *s;
  int len;
  int pos;
  double numval;
};
