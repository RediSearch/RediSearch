
#pragma once

#include "tokenize.h"

#include <stdlib.h>

// A query-specific tokenizer, that reads symbols like quots, pipes, etc

struct QueryTokenizer {
  const char *text;
  size_t len;
  char *pos;
  const char *separators;
  NormalizeFunc normalize;
  const char **stopwords;
};

// A token in the process of parsing a query.
// Unlike the document tokenizer, it works iteratively and is not callback based.

struct QueryToken {
  const char *s;
  int len;
  int pos;
  char *field;
  double numval;
};

struct RangeNumber {
  double num;
  int inclusive;
};

// #define QUERY_STOPWORDS DEFAULT_STOPWORDS;
