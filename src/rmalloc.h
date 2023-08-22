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
#include "redisearch_api.h"

typedef struct {
  IndexStats *stats;
} alloc_context;

#ifdef REDIS_MODULE_TARGET /* Set this when compiling your code as a module */
#ifdef COUNT_MEM_USAGE /* Set this to 1 to enable memory usage counting */
static inline void *rm_malloc(alloc_context *ctx, size_t n) {
  if(ctx) {
    ctx->stats->invertedCap += n;
  }
  return RedisModule_Alloc(n);
}

static inline void *rm_calloc(alloc_context *ctx, size_t nelem, size_t elemsz) {
  if(ctx) {
    ctx->stats->invertedCap += nelem * elemsz;
  }
  return RedisModule_Calloc(nelem, elemsz);
}

static inline void *rm_realloc(alloc_context *ctx, void *p, size_t n) {
  if(ctx) {
    ctx->stats->invertedCap -= RedisModule_MallocSize(p);
    ctx->stats->invertedCap += n;
  }
  if (n == 0) {
    RedisModule_Free(p);
    return NULL;
  }
  return RedisModule_Realloc(p, n);
}

static inline void rm_free(alloc_context *ctx, void *p) {
  if(ctx) {
    ctx->stats->invertedCap -= RedisModule_MallocSize(p);
  }
  RedisModule_Free(p);
}

static inline char *rm_strdup(alloc_context *ctx, const char *s) {
  if(ctx) {
    ctx->stats->invertedCap += (strlen(s) + 1)*sizeof(char);
  }
  return RedisModule_Strdup(s);
}

static char *rm_strndup(alloc_context *ctx, const char *s, size_t n) {
  char *ret = (char *)rm_malloc(ctx, n + 1);

  if (ret) {
    ret[n] = '\0';
    memcpy(ret, s, n);
  }
  return ret;
}

static int rm_vasprintf(alloc_context *ctx, char **__restrict __ptr, const char *__restrict __fmt, va_list __arg) {
  va_list args_copy;
  va_copy(args_copy, __arg);

  size_t needed = vsnprintf(NULL, 0, __fmt, __arg) + 1;
  *__ptr = (char *)rm_malloc(ctx, needed);

  int res = vsprintf(*__ptr, __fmt, args_copy);

  va_end(args_copy);

  return res;
}

static int rm_asprintf(alloc_context *ctx, char **__ptr, const char *__restrict __fmt, ...) {
  va_list ap;
  va_start(ap, __fmt);

  int res = rm_vasprintf(ctx, __ptr, __fmt, ap);

  va_end(ap);

  return res;
}

#else

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
#endif /* COUNT_MEM_USAGE */
#endif
#ifndef REDIS_MODULE_TARGET
/* for non redis module targets */

#ifdef COUNT_MEM_USAGE /* Set this to 1 to enable memory usage counting */
#define rm_malloc(ctx, size)        \
({                                  \
  void *ret = malloc(size)          \
  if(ctx) {                          \
    ctx->stats->invertedCap += size;   \
  }                                 \
  ret;                              \
})
#define rm_free(ctx, ptr)                                  \
({                                                         \
  if(ctx) {                          \
    ctx->stats->invertedCap -= malloc_size(ptr);   \
  }                                                       \
  free(ptr);                                               \
})

#define rm_calloc(ctx, nitems, size)         \
({                                           \
  void *ret = calloc(nitems, size)           \
  if(ctx) {                          \
    ctx->stats->invertedCap += nitems*size;     \
  }                                          \
  ret;                                       \
})

#define rm_realloc(ctx, ptr, size)            \
({                                            \
  if(ctx) {                          \
    ctx->stats->invertedCap -= malloc_size(ptr);   \
    ctx->stats->invertedCap += size;             \
  }                                          \
  void *ret = realloc(ptr, size);             \
  ret;                                        \
})

#define rm_strdup(ctx, s)                           \
({                                                  \
  if(ctx) {                          \
    ctx->stats->invertedCap += (strlen(s)+1)*sizeof(char); \
  }                                                 \
  char *ret = strdup(s);                            \
  ret;                                              \
})

#define rm_strndup(ctx, s, n)                 \
({                                            \
  ctx->stats->invertedCap += (n+1)*sizeof(char);   \
  char *ret = strndup(s, n);                  \
  ret;                                        \
})

#define rm_asprintf(ctx, strp, fmt, ...)      \
({                                            \
  int ret = asprintf(strp, __VA_ARGS__);      \
  if(ctx) {                          \
    ctx->stats->invertedCap += ret + 1;          \
  }                                          \
  ret;                                        \
})

#define rm_vasprintf(ctx, strp, fmt, ap)      \
({                                            \
  int ret = vasprintf(strp, fmt, ap);         \
  if(ctx) {                          \
    ctx->stats->invertedCap += ret + 1;          \
  }                                          \
  ret;                                        \
})

#else
#define rm_malloc(ctx, ...) malloc(__VA_ARGS__)
#define rm_free(ctx, ...) free(__VA_ARGS__)
#define rm_calloc(ctx, ...) calloc(__VA_ARGS__)
#define rm_realloc(ctx, ...) realloc(__VA_ARGS__)
#define rm_strdup(ctx, ...) strdup(__VA_ARGS__)
#define rm_strndup(ctx, ...) strndup(__VA_ARGS__)
#define rm_asprintf(ctx, ...) asprintf(__VA_ARGS__)
#define rm_vasprintf(ctx, ...) vasprintf(__VA_ARGS__)
#endif /* COUNT_MEM_USAGE */
#endif

#define rm_new(ctx, x) rm_malloc(ctx, sizeof(x))

#endif /* __RMUTIL_ALLOC__ */
