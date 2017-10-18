#ifndef __REDISEARCH_ALLOC__
#define __REDISEARCH_ALLOC__

#include <stdlib.h>
#include <string.h>
#include "redismodule.h"

#ifdef REDIS_MODULE_TARGET /* Set this when compiling your code as a module */

#define rm_malloc(size) RedisModule_Alloc(size)
#define rm_calloc(count, size) RedisModule_Calloc(count, size)
#define rm_realloc(ptr, size) RedisModule_Realloc(ptr, size)
#define rm_free(ptr) RedisModule_Free(ptr)
#define rm_strdup(s) RedisModule_Strdup(s)

static char *rm_strndup(const char *s, size_t n) {
  char *ret = rm_malloc(n + 1);

  if (ret) {
    ret[n] = '\0';
    memcpy(ret, s, n);
  }
  return ret;
}
#endif
#ifndef REDIS_MODULE_TARGET
/* for non redis module targets */
#define rm_alloc malloc
#define rm_free free
#define rm_realloc realloc
#define rm_free free
#define rm_strdup strdup
#define rm_strndup strndup
#endif

#define rm_new(x) rm_malloc(sizeof(x))

#endif /* __RMUTIL_ALLOC__ */
