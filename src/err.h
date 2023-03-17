/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef RS_ERR_H_
#define RS_ERR_H_
#include <stdio.h>

#define FMT_ERR(e, fmt, ...)          \
  ({                                  \
    rm_asprintf(e, fmt, __VA_ARGS__); \
    NULL;                             \
  })

#define SET_ERR(e, msg)                \
  ({                                   \
    if (e && !*e) *e = rm_strdup(msg); \
    NULL;                              \
  })

#define ERR_FREE(e) \
  if (e) {          \
    rm_free(e);     \
  }

#endif
