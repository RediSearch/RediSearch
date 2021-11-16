#ifndef __TOKEN_H__
#define __TOKEN_H__
#include <stdlib.h>

typedef struct {
  int64_t intval;
  double dval;
  char *strval;
  char *s;  // token string
  size_t len; //token string lenght
  int pos;  // position in the query
} Token;

extern Token tok;

#endif