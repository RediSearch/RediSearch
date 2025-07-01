#include <string.h>
#include <stdint.h>

#include "libnu/libnu.h"

#include "normalize.h"

#ifndef rm_calloc 
#ifndef REDIS_MODULE_TARGET
#include <stdlib.h>
#define rm_calloc calloc
#else
#include "redismodule.h"
static inline void *rm_calloc(size_t nelem, size_t elemsz) {
    return RedisModule_Calloc(nelem, elemsz);
}
#endif
#endif

/* Normalize sorting string for storage. This folds everything to unicode equivalent strings. The
 * allocated return string needs to be freed later */
char *normalizeStr(const char *str) {

    size_t buflen = 2 * strlen(str) + 1;
    char *lower_buffer = rm_calloc(buflen, 1);
    char *lower = lower_buffer;
    char *end = lower + buflen;
  
    const char *p = str;
    size_t off = 0;
    while (*p != 0 && lower < end) {
      uint32_t in = 0;
      p = nu_utf8_read(p, &in);
      const char *lo = nu_tofold(in);
  
      if (lo != 0) {
        uint32_t u = 0;
        do {
          lo = nu_casemap_read(lo, &u);
          if (u == 0) break;
          lower = nu_utf8_write(u, lower);
        } while (u != 0 && lower < end);
      } else {
        lower = nu_utf8_write(in, lower);
      }
    }
  
    return lower_buffer;
  }
  