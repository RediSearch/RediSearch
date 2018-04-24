#ifndef RS_ERR_H_
#define RS_ERR_H_
#include <stdio.h>

#define FMT_ERR(e, fmt, ...)       \
  ({                               \
    asprintf(e, fmt, __VA_ARGS__); \
    NULL;                          \
  })

#define SET_ERR(e, msg)             \
  ({                                \
    if (e && !*e) *e = strdup(msg); \
    NULL;                           \
  })

#define ERR_FREE(e) \
  if (e) {          \
    free(e);        \
  }

#endif