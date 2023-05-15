/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

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