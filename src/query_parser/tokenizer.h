#ifndef __QUERY_TOKENIZER_H__
#define __QUERY_TOKENIZER_H__

#include <stdlib.h>
#include <stdbool.h>
#include "../tokenize.h"
#include "VecSim/vec_sim_common.h"

/* A query-specific tokenizer, that reads symbols like quots, pipes, etc */
typedef struct {
  const char *text;
  size_t len;
  char *pos;
  const char *separators;
  NormalizeFunc normalize;
  const char **stopwords;

} QueryTokenizer;


typedef enum {
  // Concrete types
  QT_TERM,
  QT_TERM_CASE,
  QT_NUMERIC,
  QT_SIZE,
  // Parameterized types
  QT_PARAM_ANY,
  QT_PARAM_TERM,
  QT_PARAM_TERM_CASE,
  QT_PARAM_NUMERIC,
  QT_PARAM_SIZE,
  QT_PARAM_NUMERIC_MIN_RANGE,
  QT_PARAM_NUMERIC_MAX_RANGE,
  QT_PARAM_GEO_COORD,
  QT_PARAM_GEO_UNIT,
  QT_PARAM_VEC
} QueryTokenType;

/* A token in the process of parsing a query. Unlike the document tokenizer,  it
works iteratively and is not callback based.  */
typedef struct {
  const char *s;
  int len;
  int pos;
  double numval;
  int inclusive;
  QueryTokenType type;
} QueryToken;

typedef struct {
  double num;
  int inclusive;
} RangeNumber;

typedef struct {
  VecSimRawParam param;
  bool needResolve;
} SingleVectorQueryParam;

#define QUERY_STOPWORDS DEFAULT_STOPWORDS;

#endif