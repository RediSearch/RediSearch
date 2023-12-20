/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef __REDISEARCH_ALLOC__
#define __REDISEARCH_ALLOC__

#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include "redismodule.h"

#ifdef REDIS_MODULE_TARGET /* Set this when compiling your code as a module */

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus
void *rm_malloc(size_t n) ;
void *rm_calloc(size_t nelem, size_t elemsz) ;
void *rm_realloc(void *p, size_t n) ;
void rm_free(void *p) ;
char *rm_strdup(const char *s) ;

static char *rm_strndup(const char *s, size_t n) {
  char *ret = (char *)rm_malloc(n + 1);

  if (ret) {
    ret[n] = '\0';
    memcpy(ret, s, n);
  }
  return ret;
}

static int rm_vasprintf(char **__restrict __ptr, const char *__restrict __fmt, va_list __arg) {
  va_list args_copy;
  va_copy(args_copy, __arg);

  size_t needed = vsnprintf(NULL, 0, __fmt, __arg) + 1;
  *__ptr = (char *)rm_malloc(needed);

  int res = vsprintf(*__ptr, __fmt, args_copy);

  va_end(args_copy);

  return res;
}

static int rm_asprintf(char **__ptr, const char *__restrict __fmt, ...) {
  va_list ap;
  va_start(ap, __fmt);

  int res = rm_vasprintf(__ptr, __fmt, ap);

  va_end(ap);

  return res;
}
#ifdef __cplusplus
} // extern "C"
#endif // __cplusplus
#endif
#ifndef REDIS_MODULE_TARGET
/* for non redis module targets */
#define rm_malloc malloc
#define rm_free free
#define rm_calloc calloc
#define rm_realloc realloc
#define rm_strdup strdup
#define rm_strndup strndup
#define rm_asprintf asprintf
#define rm_vasprintf vasprintf
#endif

#define rm_new(x) rm_malloc(sizeof(x))

#endif /* __RMUTIL_ALLOC__ */
