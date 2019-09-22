#ifndef __REDISEARCH_ALLOC__
#define __REDISEARCH_ALLOC__

#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include "redismodule.h"

#ifdef REDIS_MODULE_TARGET /* Set this when compiling your code as a module */

static inline void *rm_malloc(size_t n) {
  return RedisModule_Alloc(n);
}
static inline void *rm_calloc(size_t nelem, size_t elemsz) {
  return RedisModule_Calloc(nelem, elemsz);
}
static inline void *rm_realloc(void *p, size_t n) {
  if (n == 0) {
    RedisModule_Free(p);
    return NULL;
  }
  return RedisModule_Realloc(p, n);
}
static inline void rm_free(void *p) {
  RedisModule_Free(p);
}
static inline char *rm_strdup(const char *s) {
  return RedisModule_Strdup(s);
}

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
