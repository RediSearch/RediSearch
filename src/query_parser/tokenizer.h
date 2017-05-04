#ifndef __QUERY_TOKENIZER_H__
#define __QUERY_TOKENIZER_H__

#include <stdlib.h>
#include "../tokenize.h"

/* A query-specific tokenizer, that reads symbols like quots, pipes, etc */
typedef struct {
  const char *text;
  size_t len;
  char *pos;
  const char *separators;
  NormalizeFunc normalize;
  const char **stopwords;

} QueryTokenizer;

/* Quer tokenizer token type */
// typedef enum { T_WORD, T_QUOTE, T_AND, T_OR, T_END, T_STOPWORD }
// QueryTokenType;

/* A token in the process of parsing a query. Unlike the document tokenizer,  it
works iteratively and is not callback based.  */
typedef struct {
  const char *s;
  int len;
  int pos;
  char *field;
  double numval;
  // QueryTokenType ;
} QueryToken;

typedef struct {
  double num;
  int inclusive;
} RangeNumber;

#define QUERY_SEPARATORS " \t,/{};/\\!#$%^&=+<>?";

#define QUERY_STOPWORDS DEFAULT_STOPWORDS;

/* Create a new query tokenizer. There is no need to free anything in the object
 */
QueryTokenizer NewQueryTokenizer(char *text, size_t len, const char **stopwords);

/* Read the next token from the tokenizer. If tit has reached the end of the
query text, it will return a token with type T_END and null content.

Note: The token's text might not be null terminated, so use the len variable */
int QueryTokenizer_Next(QueryTokenizer *t, QueryToken *tok);

/* Returns 1 if the tokenizer can read more tokens from the query text */
int QueryTokenizer_HasNext(QueryTokenizer *);

#endif